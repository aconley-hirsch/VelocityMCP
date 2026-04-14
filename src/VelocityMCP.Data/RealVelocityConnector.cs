#if VELOCITY_REAL
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VelocityAdapter;
using VelocityAdapter.Devices.Doors;
using VelocityAdapter.People;
using VelocityMCP.Data.Models;

namespace VelocityMCP.Data;

/// <summary>
/// Real Velocity SDK connector. Wraps <c>VelocityAdapter.dll</c> on Windows,
/// translating SDK types into our allow-listed DTOs. PII fields (PIN, CODE,
/// ROMID) are dropped at the mapper boundary and never reach DuckDB.
///
/// Threading: the SDK's calls are synchronous and block on internal SQL
/// round-trips. Each public method does <c>Task.Run</c> so callers can await
/// without pinning a thread-pool thread.
///
/// Clearance methods bypass the SDK's per-credential enumerators (which would
/// be O(door_groups × credentials) round-trips on a real site) and run three
/// bulk SELECTs through the SDK's connection pool — one for the clearance
/// dimension, one for reader↔clearance edges, one for person↔clearance edges.
/// </summary>
public sealed class RealVelocityConnector : IVelocityClient
{
    private readonly VelocityOptions _options;
    private readonly ILogger<RealVelocityConnector> _logger;
    private readonly object _connectLock = new();
    private VelocityServer? _sdk;
    private bool _disposed;

    public RealVelocityConnector(IOptions<VelocityOptions> options, ILogger<RealVelocityConnector> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public bool IsConnected => _sdk?.IsConnected == true;

    public Task ConnectAsync(CancellationToken ct = default)
    {
        if (IsConnected) return Task.CompletedTask;

        return Task.Run(() =>
        {
            lock (_connectLock)
            {
                if (IsConnected) return;

                var sdk = new VelocityServer();

                // Wire failure events BEFORE Connect — the starter template warns
                // events fired between Connect and the first += would be lost.
                sdk.OnDisconnect += forced =>
                    _logger.LogWarning("Velocity SDK disconnected (forced={Forced})", forced);
                sdk.ConnectionFailure += msg =>
                    _logger.LogError("Velocity SDK connection failure: {Message}", msg);

                _logger.LogInformation("Connecting to Velocity at {Server}/{Database}",
                    _options.SqlServer, _options.Database);

                if (string.IsNullOrEmpty(_options.AppRolePassword))
                {
                    sdk.Connect(_options.SqlServer, _options.Database);
                }
                else
                {
                    sdk.Connect(_options.SqlServer, _options.Database, _options.AppRolePassword);
                }

                _logger.LogInformation(
                    "Connected: operator={OperatorId} workstation={WorkstationId} server={ServerName} velocity={Release}",
                    sdk.OperatorID, sdk.WorkstationID, sdk.ServerName, sdk.VelocityRelease);

                _sdk = sdk;
            }
        }, ct);
    }

    // ── Fact streams ────────────────────────────────────────────────────

    public Task<List<TransactionRecord>> GetLogTransactionsAsync(DateTime sinceDate, CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var rows = sdk.GetLogTransactions(sinceDate);
            var result = new List<TransactionRecord>(rows.Count);
            foreach (var t in rows)
            {
                ct.ThrowIfCancellationRequested();
                result.Add(MapTransaction(t));
            }
            return result;
        }, ct);

    public Task<List<AlarmRecord>> GetAlarmLogAsync(DateTime sinceDate, CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var rows = sdk.GetAlarmLog(sinceDate);
            var result = new List<AlarmRecord>(rows.Count);
            foreach (var a in rows)
            {
                ct.ThrowIfCancellationRequested();
                result.Add(MapAlarm(a));
            }
            return result;
        }, ct);

    // Operator audit trail. Log_Software.Description is already human-readable,
    // so we don't need an event-code lookup — we keep EventCode only for the
    // `search_admin_actions` tool's scoped filtering. OperatorId is populated
    // but NOT the operator name; that's denormalized at the mirror boundary
    // from dim_operators after the fact.
    public Task<List<SoftwareEventRecord>> GetSoftwareEventsAsync(DateTime sinceDate, CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var rows = sdk.GetSoftwareEvents(sinceDate);
            var result = new List<SoftwareEventRecord>(rows.Count);
            foreach (var s in rows)
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new SoftwareEventRecord
                {
                    LogId = s.LogID,
                    DtDate = s.EventTime,
                    PcDateTime = s.PCDateTime,
                    EventCode = s.Event,
                    Description = s.Description ?? "",
                    OperatorId = s.OperatorId,
                    NetAddress = s.NetAddress,
                    SecurityDomainId = s.SecurityDomainID,
                });
            }
            return result;
        }, ct);

    // ── Dimension getters ───────────────────────────────────────────────

    public Task<List<DoorRecord>> GetDoorsAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<DoorRecord>();
            // Doors live on the RPC facade, not VelocityServer directly.
            var en = sdk.RPC.GetDoors();
            while (en.MoveNext())
            {
                ct.ThrowIfCancellationRequested();
                IDoor d = en.Current;
                result.Add(new DoorRecord
                {
                    Id = d.ID,
                    Name = d.Name ?? "",
                    Index = d.Index,
                    ControllerId = d.ControllerId,
                    Address = d.Address,
                });
            }
            return result;
        }, ct);

    public Task<List<ReaderRecord>> GetReadersAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var rows = sdk.getReaders();
            var result = new List<ReaderRecord>(rows.Count);
            foreach (var r in rows)
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new ReaderRecord
                {
                    Id = r.id,
                    Name = r.name ?? "",
                    FromZone = r.FromZone,
                    ToZone = r.ToZone,
                    // The SDK's getReaders() projection doesn't expose a door id.
                    // dim_readers.door_id stays null until a future enrichment
                    // pass joins via the controller→door tables.
                    DoorId = null,
                });
            }
            return result;
        }, ct);

    public Task<List<PersonRecord>> GetPersonsAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<PersonRecord>();
            var en = sdk.GetPersons();
            while (en.MoveNext())
            {
                ct.ThrowIfCancellationRequested();
                IPerson p = en.Current;
                result.Add(new PersonRecord
                {
                    PersonId = p.PersonID,
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                });
            }
            return result;
        }, ct);

    // Credential→person mapping. Velocity's fact_transactions.UID1 is a
    // CredentialId (from UserCredentials), NOT a HostUserId, so any tool that
    // wants to filter events by a real person has to expand person → list of
    // credentials first. One row per non-template credential.
    // Activation/expiration dates come from HostActivationDate/HostExpirationDate;
    // both are nullable (credentials can be open-ended / perpetual).
    public Task<List<UserCredentialRecord>> GetUserCredentialsAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<UserCredentialRecord>();

            using var lease = SqlLease.From(sdk);
            using var cmd = lease.CreateCommand();
            cmd.CommandText = @"
                SELECT CredentialId, HostUserId,
                       HostActivationDate, HostExpirationDate,
                       HostIsActivated, HostExpirationUsed
                FROM UserCredentials WITH (NOLOCK)
                WHERE IsTemplate = 0 AND HostUserId IS NOT NULL AND HostUserId > 0";
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new UserCredentialRecord
                {
                    CredentialId = rd.GetInt32(0),
                    PersonId = rd.GetInt32(1),
                    ActivationDate = rd.IsDBNull(2) ? null : rd.GetDateTime(2),
                    ExpirationDate = rd.IsDBNull(3) ? null : rd.GetDateTime(3),
                    IsActivated = !rd.IsDBNull(4) && rd.GetBoolean(4),
                    ExpirationUsed = !rd.IsDBNull(5) && rd.GetBoolean(5),
                });
            }
            return result;
        }, ct);

    // Operator (admin user) directory. Small table on every real site
    // (<20 rows typical). Used to resolve fact_software_events.operator_id
    // to a human name and to power the find_operators tool.
    public Task<List<OperatorRecord>> GetOperatorsAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<OperatorRecord>();

            using var lease = SqlLease.From(sdk);
            using var cmd = lease.CreateCommand();
            cmd.CommandText = @"
                SELECT OperatorID, Name, FullName, Description, Enabled
                FROM Operators WITH (NOLOCK)
                ORDER BY OperatorID";
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new OperatorRecord
                {
                    OperatorId = rd.GetInt32(0),
                    Name = rd.IsDBNull(1) ? "" : rd.GetString(1),
                    FullName = rd.IsDBNull(2) ? null : rd.GetString(2),
                    Description = rd.IsDBNull(3) ? null : rd.GetString(3),
                    Enabled = !rd.IsDBNull(4) && rd.GetBoolean(4),
                });
            }
            return result;
        }, ct);

    // ── Clearance bulk SQL ──────────────────────────────────────────────
    //
    // Three bulk SELECTs through the SDK's connection pool. The N+1 alternative
    // (loop door groups, call GetCredentialsByDoorGroupID per group) explodes on
    // real sites with ~hundreds of door groups × 10k+ credentials.
    //
    // Master door groups are NOT mirrored as standalone clearances — they're
    // a Velocity UI grouping, not an access-decision unit. Master assignments
    // are expanded into their constituent door groups in Query C so the
    // person↔clearance graph matches what the panel actually enforces.

    public Task<List<ClearanceRecord>> GetClearancesAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<ClearanceRecord>();

            using var lease = SqlLease.From(sdk);
            using var cmd = lease.CreateCommand();
            cmd.CommandText = @"
                SELECT DoorGroupID, DoorGroupName
                FROM DoorGroups WITH (NOLOCK)
                ORDER BY DoorGroupID";
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new ClearanceRecord
                {
                    Id = rd.GetInt32(0),
                    Name = rd.IsDBNull(1) ? "" : rd.GetString(1),
                    Active = true,
                });
            }
            return result;
        }, ct);

    public Task<List<ReaderClearanceRecord>> GetReaderClearancesAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<ReaderClearanceRecord>();

            // Zone-level join: a clearance grants a reader iff the reader's
            // FromZone or ToZone matches an access zone the door group maps to,
            // ON THE SAME CONTROLLER. This matches Velocity's actual access
            // decision (panel checks reader↔zone, not just controller membership).
            //
            // Master groups expand inline: any door group reachable via
            // MasterDoorGroupsToDoorGroups gets emitted under the constituent
            // door group's id, since the mirror only knows door groups.
            using var lease = SqlLease.From(sdk);
            using var cmd = lease.CreateCommand();
            cmd.CommandText = @"
                SELECT DISTINCT dg.DoorGroupID AS clearance_id, r.ReaderID
                FROM DoorGroups dg WITH (NOLOCK)
                JOIN DoorGroupsToAccessZones dgaz WITH (NOLOCK)
                    ON dgaz.DoorGroupID = dg.DoorGroupID
                JOIN ControllerSTDAccessZones csaz WITH (NOLOCK)
                    ON csaz.AccessZoneID = dgaz.AccessZoneID
                JOIN Readers r WITH (NOLOCK)
                    ON r.ControllerID = csaz.ControllerID
                   AND (r.FromZone = csaz.AccessZoneIndex
                        OR r.ToZone = csaz.AccessZoneIndex)";
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new ReaderClearanceRecord
                {
                    ClearanceId = rd.GetInt32(0),
                    ReaderId = rd.GetInt32(1),
                });
            }
            return result;
        }, ct);

    public Task<List<PersonClearanceRecord>> GetPersonClearancesAsync(CancellationToken ct = default) =>
        Task.Run(() =>
        {
            var sdk = RequireConnected();
            var result = new List<PersonClearanceRecord>();

            // One bulk SELECT replaces N+1 GetCredentialsByDoorGroupID calls.
            //
            // Schema (verified against the SDK's own SELECTs at decompiled.cs:24506,
            // :24648, :25524):
            //   UserCredentials.HostUserId          → person id (NOT "PersonID")
            //   UserCredentials.IsTemplate = 0      → exclude template credentials
            //   UserCredentialFunctions.Function    → access-function type code
            //   UserCredentialFunctions.HostZone    → door group id, OR -1 sentinel
            //                                         meaning "look at MasterDoorGroupID"
            //   UserCredentialFunctions.MasterDoorGroupID → master door group id
            //                                               when HostZone = -1
            //
            // Function IN (1,2,3,34,20..27) is the exact set the SDK treats as
            // access functions (line 24648). Function=32 is excluded — that's a
            // function-group assignment, not a door-group assignment. Function
            // values outside the set are control functions (relays, etc).
            //
            // Master door groups are expanded inline through
            // MasterDoorGroupsToDoorGroups so the mirror only stores leaf door
            // group ids, matching what the panel actually enforces.
            using var lease = SqlLease.From(sdk);
            using var cmd = lease.CreateCommand();
            cmd.CommandText = @"
                ;WITH access_functions AS (
                    SELECT uc.HostUserId AS PersonId,
                           CASE WHEN ucf.HostZone = -1 THEN ucf.MasterDoorGroupID
                                ELSE ucf.HostZone
                           END AS GroupId,
                           CASE WHEN ucf.HostZone = -1 THEN 1 ELSE 0 END AS IsMaster
                    FROM UserCredentials uc WITH (NOLOCK)
                    JOIN UserCredentialFunctions ucf WITH (NOLOCK)
                        ON ucf.CredentialId = uc.CredentialId
                    WHERE uc.IsTemplate = 0
                      AND ucf.[Function] IN (1, 2, 3, 34, 20, 21, 22, 23, 24, 25, 26, 27)
                ),
                expanded AS (
                    SELECT PersonId, GroupId AS DoorGroupId
                    FROM access_functions
                    WHERE IsMaster = 0

                    UNION

                    SELECT af.PersonId, mdgtdg.DoorGroupID
                    FROM access_functions af
                    JOIN MasterDoorGroupsToDoorGroups mdgtdg WITH (NOLOCK)
                        ON mdgtdg.MasterDoorGroupID = af.GroupId
                    WHERE af.IsMaster = 1
                )
                SELECT DISTINCT PersonId, DoorGroupId
                FROM expanded
                WHERE PersonId IS NOT NULL AND PersonId > 0";
            using var rd = cmd.ExecuteReader();
            var grantedAt = DateTime.UtcNow;
            while (rd.Read())
            {
                ct.ThrowIfCancellationRequested();
                result.Add(new PersonClearanceRecord
                {
                    PersonId = rd.GetInt32(0),
                    ClearanceId = rd.GetInt32(1),
                    GrantedAt = grantedAt,
                    ExpiresAt = null,
                });
            }
            return result;
        }, ct);

    // ── Mappers ─────────────────────────────────────────────────────────

    private static TransactionRecord MapTransaction(LogTransaction t) => new()
    {
        // PII fields t.PIN, t.CODE, t.ROMID are intentionally dropped here.
        LogId = t.LogID,
        DtDate = t.dtDate,
        PcDateTime = t.PCDateTime,
        EventCode = t.Event,
        Description = t.Description,
        Disposition = t.Disposition,
        TransactionType = t.TransactionType,
        ReportAsAlarm = t.ReportAsAlarm,
        AlarmLevelPriority = t.AlarmLevelPriority,
        PortAddr = t.PortAddr,
        DtAddr = t.DTAddr,
        XAddr = t.XAddr,
        NetAddress = t.NetAddress,
        DoorOrExpansion = t.DoorOrExpansion,
        Reader = t.Reader,
        ReaderName = t.ReaderName,
        FromZone = t.FromZone,
        ToZone = t.ToZone,
        // NOTE: t.UID1 is a CredentialId (from UserCredentials), NOT a HostUserId
        // (from Users). Do NOT join fact_transactions.uid1 against dim_people.person_id —
        // they are two different ID spaces and any numeric overlap is coincidence.
        // Velocity denormalizes the credential owner's name at log-insert time into
        // UID1FirstName/UID1LastName; those are the authoritative display names.
        Uid1 = t.UID1,
        Uid1Name = JoinName(t.UID1FirstName, t.UID1LastName),
        Uid2 = t.UID2,
        Uid2Name = JoinName(t.UID2FirstName, t.UID2LastName),
        ServerID = t.ServerID,
        SecurityDomainID = t.SecurityDomainID,
    };

    private static AlarmRecord MapAlarm(AlarmLog a) => new()
    {
        AlarmId = a.AlarmID,
        DtDate = a.dtDate,
        DbDate = a.dbDate,
        AkDate = a.akDate,
        ClDate = a.clDate,
        EventId = a.EventID,
        AlarmLevelPriority = a.AlarmLevelPriority,
        Status = a.Status,
        Description = a.Description,
        PortAddr = a.PortAddr,
        DtAddr = a.DTAddr,
        XAddr = a.XAddr,
        NetAddress = a.NetAddress,
        AkOperator = a.akOperator,
        ClOperator = a.clOperator,
        WorkstationName = a.WorkstationName,
        Uid1 = a.UID1,
        Uid1Name = JoinName(a.UID1FirstName, a.UID1LastName),
        Uid2 = a.UID2,
        Uid2Name = JoinName(a.UID2FirstName, a.UID2LastName),
        Parm1 = a.Parm1,
        Parm2 = a.Parm2,
        TransactionType = a.TransactionType,
        ServerID = a.ServerID,
        SiteID = a.SiteID,
    };

    private static string? JoinName(string? first, string? last)
    {
        if (string.IsNullOrWhiteSpace(first) && string.IsNullOrWhiteSpace(last)) return null;
        return $"{first} {last}".Trim();
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private VelocityServer RequireConnected()
    {
        var sdk = _sdk;
        if (sdk is null || !sdk.IsConnected)
            throw new InvalidOperationException("Velocity SDK is not connected. Call ConnectAsync first.");
        return sdk;
    }

    /// <summary>
    /// RAII checkout from the SDK's connection pool. The SDK exposes
    /// <c>getConn()</c>/<c>releaseConn()</c> on VelocityServer; this struct
    /// guarantees we always release even if the caller throws mid-query.
    /// </summary>
    private readonly struct SqlLease : IDisposable
    {
        private readonly VelocityServer _sdk;
        private readonly VelocityAdapter.ISQLConnect _conn;

        private SqlLease(VelocityServer sdk, VelocityAdapter.ISQLConnect conn)
        {
            _sdk = sdk;
            _conn = conn;
        }

        public static SqlLease From(VelocityServer sdk) => new(sdk, sdk.getConn());

        // Returns DbCommand (the System.Data.Common base) rather than the concrete
        // SqlCommand from System.Data.SqlClient so we don't tag every caller site
        // with the CS0618 obsoletion warning. The runtime object is still a
        // SqlCommand from whichever SqlClient the SDK was built against.
        public DbCommand CreateCommand()
        {
            DbConnection conn = _conn.Connection;
            var cmd = conn.CreateCommand();
            cmd.CommandTimeout = 60;
            return cmd;
        }

        public void Dispose() => _sdk.releaseConn(_conn);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            _sdk?.Disconnect();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Velocity SDK Disconnect threw — swallowing during dispose");
        }
        _sdk = null;
    }
}
#endif
