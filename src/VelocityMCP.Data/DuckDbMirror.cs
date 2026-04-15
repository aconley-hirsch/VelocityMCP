using System.Data;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using VelocityMCP.Data.Models;

namespace VelocityMCP.Data;

/// <summary>
/// Read/write access to the DuckDB analytical mirror.
/// Ingest methods use parameterized INSERT with ON CONFLICT for de-duplication.
/// Query methods are used by MCP tool handlers.
/// </summary>
public sealed class DuckDbMirror : IDisposable
{
    private readonly DuckDBConnection _conn;
    private readonly ILogger<DuckDbMirror> _logger;

    public DuckDbMirror(string connectionString, ILogger<DuckDbMirror> logger)
    {
        _conn = new DuckDBConnection(connectionString);
        _conn.Open();
        _logger = logger;
    }

    // ── Ingest ──────────────────────────────────────────────────────────

    public void IngestTransactions(IReadOnlyList<TransactionRecord> records)
    {
        if (records.Count == 0) return;

        using var tx = _conn.BeginTransaction();
        try
        {
            foreach (var r in records)
            {
                using var cmd = _conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    INSERT OR IGNORE INTO fact_transactions (
                        log_id, dt_date, pc_date_time, event_code, description,
                        disposition, transaction_type, report_as_alarm, alarm_level_priority,
                        port_addr, dt_addr, x_addr, net_address,
                        door_or_expansion, reader, reader_name,
                        from_zone, to_zone,
                        uid1, uid1_name, uid2, uid2_name,
                        server_id, security_domain_id
                    ) VALUES (
                        $log_id, $dt_date, $pc_date_time, $event_code, $description,
                        $disposition, $transaction_type, $report_as_alarm, $alarm_level_priority,
                        $port_addr, $dt_addr, $x_addr, $net_address,
                        $door_or_expansion, $reader, $reader_name,
                        $from_zone, $to_zone,
                        $uid1, $uid1_name, $uid2, $uid2_name,
                        $server_id, $security_domain_id
                    )
                    """;
                cmd.Parameters.Add(new DuckDBParameter("log_id", r.LogId));
                cmd.Parameters.Add(new DuckDBParameter("dt_date", r.DtDate));
                cmd.Parameters.Add(new DuckDBParameter("pc_date_time", (object?)r.PcDateTime ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("event_code", r.EventCode));
                cmd.Parameters.Add(new DuckDBParameter("description", (object?)r.Description ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("disposition", r.Disposition));
                cmd.Parameters.Add(new DuckDBParameter("transaction_type", r.TransactionType));
                cmd.Parameters.Add(new DuckDBParameter("report_as_alarm", r.ReportAsAlarm));
                cmd.Parameters.Add(new DuckDBParameter("alarm_level_priority", r.AlarmLevelPriority));
                cmd.Parameters.Add(new DuckDBParameter("port_addr", (object?)r.PortAddr ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("dt_addr", (object?)r.DtAddr ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("x_addr", (object?)r.XAddr ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("net_address", (object?)r.NetAddress ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("door_or_expansion", (short)r.DoorOrExpansion));
                cmd.Parameters.Add(new DuckDBParameter("reader", (short)r.Reader));
                cmd.Parameters.Add(new DuckDBParameter("reader_name", (object?)r.ReaderName ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("from_zone", (object?)r.FromZone ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("to_zone", (object?)r.ToZone ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid1", r.Uid1));
                cmd.Parameters.Add(new DuckDBParameter("uid1_name", (object?)r.Uid1Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid2", r.Uid2));
                cmd.Parameters.Add(new DuckDBParameter("uid2_name", (object?)r.Uid2Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("server_id", r.ServerID));
                cmd.Parameters.Add(new DuckDBParameter("security_domain_id", r.SecurityDomainID));
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
            _logger.LogDebug("Ingested {Count} transactions", records.Count);
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    public void IngestAlarms(IReadOnlyList<AlarmRecord> records)
    {
        if (records.Count == 0) return;

        using var tx = _conn.BeginTransaction();
        try
        {
            foreach (var r in records)
            {
                using var cmd = _conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    INSERT OR IGNORE INTO fact_alarms (
                        alarm_id, dt_date, db_date, ak_date, cl_date,
                        event_id, alarm_level_priority, status, description,
                        port_addr, dt_addr, x_addr, net_address,
                        ak_operator, cl_operator, workstation_name,
                        uid1, uid1_name, uid2, uid2_name,
                        parm1, parm2, transaction_type, server_id, site_id
                    ) VALUES (
                        $alarm_id, $dt_date, $db_date, $ak_date, $cl_date,
                        $event_id, $alarm_level_priority, $status, $description,
                        $port_addr, $dt_addr, $x_addr, $net_address,
                        $ak_operator, $cl_operator, $workstation_name,
                        $uid1, $uid1_name, $uid2, $uid2_name,
                        $parm1, $parm2, $transaction_type, $server_id, $site_id
                    )
                    """;
                cmd.Parameters.Add(new DuckDBParameter("alarm_id", r.AlarmId));
                cmd.Parameters.Add(new DuckDBParameter("dt_date", (object?)r.DtDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("db_date", (object?)r.DbDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("ak_date", (object?)r.AkDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("cl_date", (object?)r.ClDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("event_id", r.EventId));
                cmd.Parameters.Add(new DuckDBParameter("alarm_level_priority", r.AlarmLevelPriority));
                cmd.Parameters.Add(new DuckDBParameter("status", (short)r.Status));
                cmd.Parameters.Add(new DuckDBParameter("description", (object?)r.Description ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("port_addr", r.PortAddr));
                cmd.Parameters.Add(new DuckDBParameter("dt_addr", r.DtAddr));
                cmd.Parameters.Add(new DuckDBParameter("x_addr", r.XAddr));
                cmd.Parameters.Add(new DuckDBParameter("net_address", (object?)r.NetAddress ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("ak_operator", (object?)r.AkOperator ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("cl_operator", (object?)r.ClOperator ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("workstation_name", (object?)r.WorkstationName ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid1", (object?)r.Uid1 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid1_name", (object?)r.Uid1Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid2", (object?)r.Uid2 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid2_name", (object?)r.Uid2Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("parm1", (object?)r.Parm1 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("parm2", (object?)r.Parm2 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("transaction_type", r.TransactionType));
                cmd.Parameters.Add(new DuckDBParameter("server_id", r.ServerID));
                cmd.Parameters.Add(new DuckDBParameter("site_id", r.SiteID));
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
            _logger.LogDebug("Ingested {Count} alarms", records.Count);
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    /// <summary>
    /// Append a batch of admin-action events to fact_software_events. Idempotent
    /// via INSERT OR IGNORE on log_id. operator_name is denormalized against
    /// dim_operators at insert time using a correlated subquery — this keeps
    /// the audit rows self-describing even if dim_operators changes later.
    /// </summary>
    public void IngestSoftwareEvents(IReadOnlyList<Models.SoftwareEventRecord> records)
    {
        if (records.Count == 0) return;

        using var tx = _conn.BeginTransaction();
        try
        {
            foreach (var r in records)
            {
                using var cmd = _conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    INSERT OR IGNORE INTO fact_software_events (
                        log_id, dt_date, pc_date_time, event_code, description,
                        operator_id, operator_name, net_address, security_domain_id
                    ) VALUES (
                        $log_id, $dt_date, $pc_date_time, $event_code, $description,
                        $operator_id,
                        (SELECT name FROM dim_operators WHERE operator_id = $operator_id),
                        $net_address, $security_domain_id
                    )
                    """;
                cmd.Parameters.Add(new DuckDBParameter("log_id", r.LogId));
                cmd.Parameters.Add(new DuckDBParameter("dt_date", r.DtDate));
                cmd.Parameters.Add(new DuckDBParameter("pc_date_time", (object?)r.PcDateTime ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("event_code", r.EventCode));
                cmd.Parameters.Add(new DuckDBParameter("description", r.Description));
                cmd.Parameters.Add(new DuckDBParameter("operator_id", (object?)r.OperatorId ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("net_address", (object?)r.NetAddress ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("security_domain_id", (object?)r.SecurityDomainId ?? DBNull.Value));
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
            _logger.LogDebug("Ingested {Count} software events", records.Count);
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    // ── Cursor management ───────────────────────────────────────────────

    public DateTime? GetCursor(string sourceTable)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT last_dt_date FROM meta_ingest_cursors WHERE source_table = $table";
        cmd.Parameters.Add(new DuckDBParameter("table", sourceTable));
        var result = cmd.ExecuteScalar();
        return result is DateTime dt ? dt : null;
    }

    public void UpdateCursor(string sourceTable, DateTime lastDtDate, long rowsIngested, string? error = null)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO meta_ingest_cursors (source_table, last_dt_date, last_run_at, rows_ingested, last_error)
            VALUES ($table, $last_dt_date, $now, $rows, $error)
            ON CONFLICT (source_table) DO UPDATE SET
                last_dt_date = $last_dt_date,
                last_run_at = $now,
                rows_ingested = meta_ingest_cursors.rows_ingested + $rows,
                last_error = $error
            """;
        cmd.Parameters.Add(new DuckDBParameter("table", sourceTable));
        cmd.Parameters.Add(new DuckDBParameter("last_dt_date", lastDtDate));
        cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
        cmd.Parameters.Add(new DuckDBParameter("rows", rowsIngested));
        cmd.Parameters.Add(new DuckDBParameter("error", (object?)error ?? DBNull.Value));
        cmd.ExecuteNonQuery();
    }

    // ── Query helpers for tools ─────────────────────────────────────────

    /// <summary>
    /// Resolves a door_id to its physical reader names and merges with any explicit
    /// reader filters. Tools take door_id directly so the LLM doesn't have to plumb
    /// reader arrays around. Returns:
    ///   - (false, null): no door_id provided; caller should pass through the original reader filters
    ///   - (true,  [..]): door_id resolved + merged with explicit reader filters
    ///   - (true,  []  ): door_id provided but the door has no readers in dim_readers
    ///                    → caller should short-circuit and return an empty result
    /// </summary>
    private (bool DoorIdProvided, List<string>? MergedReaders) ResolveDoorReaders(
        int? doorId, string? readerName, IReadOnlyList<string>? readerNames)
    {
        if (!doorId.HasValue) return (false, null);

        var resolved = new List<string>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM dim_readers WHERE door_id = $door_id";
            cmd.Parameters.Add(new DuckDBParameter("door_id", doorId.Value));
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) resolved.Add(reader.GetString(0));
        }

        if (readerName != null) resolved.Add(readerName);
        if (readerNames != null) resolved.AddRange(readerNames);

        return (true, resolved);
    }

    public long CountTransactions(DateTime since, DateTime until)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM fact_transactions
            WHERE dt_date >= $since AND dt_date < $until
            """;
        cmd.Parameters.Add(new DuckDBParameter("since", since));
        cmd.Parameters.Add(new DuckDBParameter("until", until));
        return (long)cmd.ExecuteScalar()!;
    }

    public DimensionCounts GetDimensionCounts()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT
                (SELECT COUNT(*) FROM dim_people),
                (SELECT COUNT(*) FROM dim_doors),
                (SELECT COUNT(*) FROM dim_readers),
                (SELECT COUNT(*) FROM dim_clearances),
                (SELECT COUNT(*) FROM dim_operators),
                (SELECT COUNT(*) FROM fact_transactions),
                (SELECT COUNT(*) FROM fact_alarms),
                (SELECT COUNT(*) FROM fact_software_events)
            """;
        using var rd = cmd.ExecuteReader();
        rd.Read();
        return new DimensionCounts(
            People:         rd.GetInt64(0),
            Doors:          rd.GetInt64(1),
            Readers:        rd.GetInt64(2),
            Clearances:     rd.GetInt64(3),
            Operators:      rd.GetInt64(4),
            Transactions:   rd.GetInt64(5),
            Alarms:         rd.GetInt64(6),
            SoftwareEvents: rd.GetInt64(7));
    }

    public long CountTransactions(DateTime since, DateTime until, int? eventCode = null,
        string? readerName = null, IReadOnlyList<string>? readerNames = null,
        long? personId = null, int? disposition = null, int? doorId = null)
    {
        var (doorProvided, mergedReaders) = ResolveDoorReaders(doorId, readerName, readerNames);
        if (doorProvided && mergedReaders!.Count == 0) return 0;

        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode,
            doorProvided ? null : readerName,
            doorProvided ? mergedReaders : readerNames,
            personId, disposition);

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
        foreach (var p in parameters) cmd.Parameters.Add(p);
        return (long)cmd.ExecuteScalar()!;
    }

    // ── Aggregation / samples ───────────────────────────────────────────

    /// <summary>
    /// Aggregate transactions grouped by a dimension. Returns top-N groups plus totals.
    /// Supported group_by values: "person", "door", "reader", "type", "hour", "day".
    /// "door" collapses multi-reader doors via dim_readers join (one row per logical door);
    /// "reader" groups by physical reader_name (one row per reader).
    /// </summary>
    public AggregationResult AggregateTransactions(
        string groupBy,
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        long? personId, int? disposition,
        int limit, int? doorId = null)
    {
        var (doorProvided, mergedReaders) = ResolveDoorReaders(doorId, readerName, readerNames);
        if (doorProvided && mergedReaders!.Count == 0)
            return new AggregationResult(groupBy, new List<AggregationBucket>(), 0, 0, false);

        var effectiveReaderName = doorProvided ? null : readerName;
        var effectiveReaderNames = doorProvided ? (IReadOnlyList<string>?)mergedReaders : readerNames;

        var groupByLower = groupBy.ToLowerInvariant();

        // group_by="door" needs a JOIN through dim_readers/dim_doors so multi-reader
        // doors collapse to one row. Routed to a dedicated helper because the SQL
        // shape (subquery + JOIN) is meaningfully different from the other groupings.
        if (groupByLower == "door")
        {
            return AggregateTransactionsByDoor(
                since, until, eventCode, effectiveReaderName, effectiveReaderNames,
                personId, disposition, limit);
        }

        // group_by="person" rolls credentials up to the real person via
        // dim_user_credentials → dim_people. key_id is a HostUserId, key is
        // the person's full_name. This is the ONLY group_by where the same
        // person's multiple badges get combined into one bucket.
        if (groupByLower == "person")
        {
            return AggregateTransactionsByPerson(
                since, until, eventCode, effectiveReaderName, effectiveReaderNames,
                personId, disposition, limit);
        }

        // group_by="credential" keeps the raw credential-level view: one bucket
        // per distinct uid1. key is the denormalized uid1_name, key_id is the
        // CredentialId. Useful for spotting a single rogue badge or for cases
        // where the owner changed between swipes.
        const string credentialKeyExpr =
            "COALESCE(uid1_name, 'Unknown credential ' || CAST(uid1 AS VARCHAR))";

        var (keyExpr, keyIdExpr, groupKey) = groupByLower switch
        {
            "credential" => (credentialKeyExpr, "uid1", "uid1_name, uid1"),
            "reader" => ("reader_name", "NULL", "reader_name"),
            "type" or "event" or "event_code" => ("description", "event_code", "event_code, description"),
            "hour" => ("CAST(DATE_TRUNC('hour', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('hour', dt_date)"),
            "day" => ("CAST(DATE_TRUNC('day', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('day', dt_date)"),
            _ => throw new ArgumentException($"Unsupported group_by: {groupBy}. Use person, credential, door, reader, type, hour, or day.")
        };

        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, effectiveReaderName, effectiveReaderNames, personId, disposition);

        // Total events matching the filter
        long totalEvents;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalEvents = (long)cmd.ExecuteScalar()!;
        }

        // Total distinct groups
        long totalGroups;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = groupByLower switch
            {
                "hour" => $"SELECT COUNT(DISTINCT DATE_TRUNC('hour', dt_date)) FROM fact_transactions {where}",
                "day" => $"SELECT COUNT(DISTINCT DATE_TRUNC('day', dt_date)) FROM fact_transactions {where}",
                "credential" => $"SELECT COUNT(DISTINCT uid1) FROM fact_transactions {where}",
                "reader" => $"SELECT COUNT(DISTINCT reader_name) FROM fact_transactions {where}",
                _ => $"SELECT COUNT(DISTINCT event_code) FROM fact_transactions {where}"
            };
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalGroups = (long)cmd.ExecuteScalar()!;
        }

        // Top-N groups (time buckets order chronologically; others by count desc)
        var orderBy = groupByLower is "hour" or "day" ? "key ASC" : "count DESC";
        var groups = new List<AggregationBucket>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT {keyExpr} AS key, {keyIdExpr} AS key_id, COUNT(*) AS count
                FROM fact_transactions {where}
                GROUP BY {groupKey}
                ORDER BY {orderBy}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                groups.Add(new AggregationBucket(
                    reader.IsDBNull(0) ? null : reader.GetValue(0).ToString(),
                    reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1)),
                    reader.GetInt64(2)));
            }
        }

        return new AggregationResult(
            groupBy,
            groups,
            totalEvents,
            totalGroups,
            Truncated: totalGroups > limit);
    }

    /// <summary>
    /// Aggregate transactions by real person. Resolves each uid1 through
    /// dim_user_credentials → dim_people so a person with multiple badges gets
    /// their events summed into one bucket. Transactions whose credential
    /// isn't in dim_user_credentials (orphans) roll up under a single
    /// "Unknown" bucket keyed on the denormalized uid1_name when available,
    /// so the model still sees them rather than silently dropping the rows.
    /// </summary>
    private AggregationResult AggregateTransactionsByPerson(
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        long? personId, int? disposition, int limit)
    {
        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, readerName, readerNames, personId, disposition);

        // Left-join through credentials → people so the bucket key is the real
        // HostUserId, not the credential id. Qualified column names throughout
        // because dim_user_credentials.person_id and dim_people.person_id both
        // exist and would otherwise be ambiguous.
        var fromJoin = """
            fact_transactions t
            LEFT JOIN dim_user_credentials c ON c.credential_id = t.uid1
            LEFT JOIN dim_people p ON p.person_id = c.person_id
            """;

        // Patch the WHERE clause: BuildTransactionFilter was written for an
        // un-aliased fact_transactions, so every column ref is bare. Re-prefix
        // them with t. so they resolve under the join.
        var whereQualified = where
            .Replace(" dt_date ", " t.dt_date ")
            .Replace(" event_code ", " t.event_code ")
            .Replace(" reader_name ", " t.reader_name ")
            .Replace(" disposition ", " t.disposition ")
            .Replace(" uid1 ", " t.uid1 ");

        long totalEvents;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM {fromJoin} {whereQualified}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalEvents = (long)cmd.ExecuteScalar()!;
        }

        // Composite group key: resolved rows collapse to "person:<id>",
        // orphans stay split by credential as "cred:<id>". So one bucket per
        // real person (regardless of how many badges they own) and one bucket
        // per orphan credential that couldn't be resolved.
        const string compositeGroupKey =
            "COALESCE('person:' || CAST(p.person_id AS VARCHAR), 'cred:' || CAST(t.uid1 AS VARCHAR))";

        long totalGroups;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT COUNT(DISTINCT {compositeGroupKey})
                FROM {fromJoin} {whereQualified}
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalGroups = (long)cmd.ExecuteScalar()!;
        }

        // key: person's full_name when matched, denormalized uid1_name for
        // orphans, synthesized "Unknown credential <id>" as last resort.
        // key_id: real HostUserId when matched, null for orphans.
        // MAX() is safe here because within a matched group all p.full_name
        // values are identical (same person), and within an orphan group all
        // t.uid1_name values are identical (same credential).
        var groups = new List<AggregationBucket>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT
                    COALESCE(
                        MAX(p.full_name),
                        MAX(t.uid1_name),
                        'Unknown credential ' || CAST(MAX(t.uid1) AS VARCHAR)
                    ) AS key,
                    MAX(p.person_id) AS key_id,
                    COUNT(*) AS count
                FROM {fromJoin} {whereQualified}
                GROUP BY {compositeGroupKey}
                ORDER BY count DESC
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                groups.Add(new AggregationBucket(
                    rd.IsDBNull(0) ? null : rd.GetValue(0).ToString(),
                    rd.IsDBNull(1) ? null : Convert.ToInt64(rd.GetValue(1)),
                    rd.GetInt64(2)));
            }
        }

        return new AggregationResult(
            "person",
            groups,
            totalEvents,
            totalGroups,
            Truncated: totalGroups > limit);
    }

    /// <summary>
    /// Aggregate transactions by logical door (one row per door, not per reader).
    /// Joins fact_transactions through dim_readers → dim_doors. Orphan readers
    /// (not in dim_readers, or with NULL door_id) are surfaced under their raw
    /// reader_name with a null key_id, so no data is silently dropped.
    /// </summary>
    private AggregationResult AggregateTransactionsByDoor(
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        long? personId, int? disposition,
        int limit)
    {
        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, readerName, readerNames, personId, disposition);

        long totalEvents;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalEvents = (long)cmd.ExecuteScalar()!;
        }

        // Distinct logical doors + orphan readers. COALESCE to reader_name for
        // orphans so they each count as their own group.
        long totalGroups;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT COUNT(DISTINCT COALESCE(CAST(d.door_id AS VARCHAR), ft.reader_name))
                FROM (SELECT * FROM fact_transactions {where}) ft
                LEFT JOIN dim_readers r ON r.name = ft.reader_name
                LEFT JOIN dim_doors d ON d.door_id = r.door_id
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalGroups = (long)cmd.ExecuteScalar()!;
        }

        var groups = new List<AggregationBucket>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT COALESCE(d.name, ft.reader_name) AS key,
                       d.door_id AS key_id,
                       COUNT(*) AS count
                FROM (SELECT * FROM fact_transactions {where}) ft
                LEFT JOIN dim_readers r ON r.name = ft.reader_name
                LEFT JOIN dim_doors d ON d.door_id = r.door_id
                GROUP BY COALESCE(d.name, ft.reader_name), d.door_id
                ORDER BY count DESC
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                groups.Add(new AggregationBucket(
                    reader.IsDBNull(0) ? null : reader.GetValue(0).ToString(),
                    reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1)),
                    reader.GetInt64(2)));
            }
        }

        return new AggregationResult("door", groups, totalEvents, totalGroups, Truncated: totalGroups > limit);
    }

    /// <summary>
    /// Return a bounded sample of raw events matching filters. Order is time_asc or time_desc.
    /// </summary>
    public SampleResult SampleTransactions(
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        long? personId, int? disposition,
        string order, int limit, int? doorId = null)
    {
        var (doorProvided, mergedReaders) = ResolveDoorReaders(doorId, readerName, readerNames);
        if (doorProvided && mergedReaders!.Count == 0)
            return new SampleResult(new List<SampleRow>(), 0, false);

        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode,
            doorProvided ? null : readerName,
            doorProvided ? mergedReaders : readerNames,
            personId, disposition);
        var orderSql = order.Equals("time_asc", StringComparison.OrdinalIgnoreCase) ? "dt_date ASC" : "dt_date DESC";

        long totalMatching;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalMatching = (long)cmd.ExecuteScalar()!;
        }

        var rows = new List<SampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT log_id, dt_date, event_code, description, disposition,
                       reader_name, uid1, uid1_name
                FROM fact_transactions {where}
                ORDER BY {orderSql}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new SampleRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        return new SampleResult(rows, totalMatching, Truncated: totalMatching > limit);
    }

    /// <summary>
    /// person_id filters on fact_transactions MUST expand through dim_user_credentials.
    /// fact_transactions.uid1 is a CredentialId, not a HostUserId — direct
    /// equality would only match by coincidence. Inlined as a SQL fragment so
    /// every filter-building site uses the same join path.
    /// </summary>
    internal const string PersonToUid1FilterFragment =
        "uid1 IN (SELECT credential_id FROM dim_user_credentials WHERE person_id = $pid)";

    internal static (string Where, List<DuckDBParameter> Parameters) BuildTransactionFilter(
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        long? personId, int? disposition)
    {
        var where = "WHERE dt_date >= $since AND dt_date < $until";
        var parameters = new List<DuckDBParameter>
        {
            new("since", since),
            new("until", until)
        };

        if (eventCode.HasValue)
        {
            where += " AND event_code = $event_code";
            parameters.Add(new DuckDBParameter("event_code", eventCode.Value));
        }

        // Combine single + list into one IN clause when either is present.
        var allReaders = new List<string>();
        if (readerName != null) allReaders.Add(readerName);
        if (readerNames != null) allReaders.AddRange(readerNames);
        if (allReaders.Count == 1)
        {
            where += " AND reader_name = $reader_name_0";
            parameters.Add(new DuckDBParameter("reader_name_0", allReaders[0]));
        }
        else if (allReaders.Count > 1)
        {
            var names = allReaders.Select((_, i) => $"$reader_name_{i}").ToArray();
            where += $" AND reader_name IN ({string.Join(", ", names)})";
            for (int i = 0; i < allReaders.Count; i++)
                parameters.Add(new DuckDBParameter($"reader_name_{i}", allReaders[i]));
        }

        if (personId.HasValue)
        {
            where += $" AND {PersonToUid1FilterFragment}";
            parameters.Add(new DuckDBParameter("pid", personId.Value));
        }
        if (disposition.HasValue)
        {
            where += " AND disposition = $disposition";
            parameters.Add(new DuckDBParameter("disposition", disposition.Value));
        }

        return (where, parameters);
    }

    // ── Dimension upserts ───────────────────────────────────────────────

    public void UpsertDoors(IReadOnlyList<Models.DoorRecord> doors)
    {
        if (doors.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var d in doors)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_doors (door_id, name, controller_addr, active, updated_at)
                VALUES ($id, $name, $addr, TRUE, $now)
                ON CONFLICT (door_id) DO UPDATE SET
                    name = $name,
                    controller_addr = $addr,
                    updated_at = $now
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", d.Id));
            cmd.Parameters.Add(new DuckDBParameter("name", d.Name));
            cmd.Parameters.Add(new DuckDBParameter("addr", (object?)d.Address ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} doors", doors.Count);
    }

    public void UpsertReaders(IReadOnlyList<Models.ReaderRecord> readers)
    {
        if (readers.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var r in readers)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_readers (reader_id, name, door_id, from_zone, to_zone, active, updated_at)
                VALUES ($id, $name, $door_id, $from, $to, TRUE, $now)
                ON CONFLICT (reader_id) DO UPDATE SET
                    name = $name,
                    door_id = $door_id,
                    from_zone = $from,
                    to_zone = $to,
                    updated_at = $now
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", r.Id));
            cmd.Parameters.Add(new DuckDBParameter("name", r.Name));
            cmd.Parameters.Add(new DuckDBParameter("door_id", (object?)r.DoorId ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("from", r.FromZone));
            cmd.Parameters.Add(new DuckDBParameter("to", r.ToZone));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} readers", readers.Count);
    }

    // ── Door catalog with derived status ────────────────────────────────

    /// <summary>
    /// Full door catalog with activity-derived status. Returns every door in
    /// <c>dim_doors</c> with its reader list, admin active flag, last_seen_at
    /// (most recent dt_date across all the door's readers), events_in_window
    /// (count in the last <paramref name="windowHours"/>), and open_alarms
    /// (unacked alarms with <c>net_address = controller_addr</c>, best-effort).
    /// Status is derived from activity: "active" if events_in_window > 0,
    /// "quiet" if no events in the window but last_seen_at within 7 days,
    /// "stale" if last_seen_at older than 7 days, "never_seen" if no events ever.
    /// NOTE: this is NOT real-time Velocity device state (online/offline,
    /// locked/unlocked, position sensor) — that requires a live SDK call.
    /// </summary>
    public List<DoorStatusRow> ListDoors(int windowHours, bool includeInactive, int limit)
    {
        var now = DateTime.UtcNow;
        var windowStart = now.AddHours(-windowHours);
        var stalenessCutoff = now.AddDays(-7);

        // One query: doors LEFT JOIN readers LEFT JOIN transactions, aggregated.
        var rows = new List<(int DoorId, string? Name, string? ControllerAddr, bool Active,
            string? ReaderNames, int ReaderCount,
            DateTime? LastSeen, long EventsInWindow)>();

        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT
                  d.door_id,
                  d.name,
                  d.controller_addr,
                  d.active,
                  STRING_AGG(DISTINCT r.name, ', ' ORDER BY r.name) AS reader_names,
                  COUNT(DISTINCT r.reader_id) AS reader_count,
                  MAX(t.dt_date) AS last_seen_at,
                  COUNT(*) FILTER (WHERE t.dt_date >= $window_start) AS events_in_window
                FROM dim_doors d
                LEFT JOIN dim_readers r ON r.door_id = d.door_id
                LEFT JOIN fact_transactions t ON t.reader_name = r.name
                GROUP BY d.door_id, d.name, d.controller_addr, d.active
                ORDER BY d.name
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("window_start", windowStart));
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add((
                    DoorId: reader.GetInt32(0),
                    Name: reader.IsDBNull(1) ? null : reader.GetString(1),
                    ControllerAddr: reader.IsDBNull(2) ? null : reader.GetString(2),
                    Active: reader.GetBoolean(3),
                    ReaderNames: reader.IsDBNull(4) ? null : reader.GetString(4),
                    ReaderCount: reader.IsDBNull(5) ? 0 : (int)reader.GetInt64(5),
                    LastSeen: reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                    EventsInWindow: reader.GetInt64(7)));
            }
        }

        // Second query: unacked alarms per controller_addr. Skipped entirely when
        // there are no rows (small systems with no alarms) to avoid an extra round trip.
        var openAlarmsByAddr = new Dictionary<string, long>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT net_address, COUNT(*) AS cnt
                FROM fact_alarms
                WHERE ak_date IS NULL AND net_address IS NOT NULL
                GROUP BY net_address
                """;
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                openAlarmsByAddr[reader.GetString(0)] = reader.GetInt64(1);
            }
        }

        var result = new List<DoorStatusRow>();
        foreach (var r in rows)
        {
            var openAlarms = (r.ControllerAddr != null && openAlarmsByAddr.TryGetValue(r.ControllerAddr, out var n))
                ? n : 0;

            string status;
            if (r.LastSeen == null) status = "never_seen";
            else if (r.EventsInWindow > 0) status = "active";
            else if (r.LastSeen >= stalenessCutoff) status = "quiet";
            else status = "stale";

            // Skip inactive doors if the caller didn't ask for them.
            if (!includeInactive && (status == "never_seen" || status == "stale")) continue;

            var readerList = string.IsNullOrEmpty(r.ReaderNames)
                ? new List<string>()
                : r.ReaderNames.Split(", ").ToList();

            result.Add(new DoorStatusRow(
                DoorId: r.DoorId,
                Name: r.Name,
                ControllerAddr: r.ControllerAddr,
                AdminActive: r.Active,
                ReaderCount: r.ReaderCount,
                ReaderNames: readerList,
                LastSeenAt: r.LastSeen,
                EventsInWindow: r.EventsInWindow,
                OpenAlarms: openAlarms,
                Status: status));
        }

        return result;
    }

    // ── Authorization dimension upserts ─────────────────────────────────

    public void UpsertClearances(IReadOnlyList<Models.ClearanceRecord> clearances)
    {
        if (clearances.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var c in clearances)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_clearances (clearance_id, name, schedule_name, active, updated_at)
                VALUES ($id, $name, $schedule, $active, $now)
                ON CONFLICT (clearance_id) DO UPDATE SET
                    name = $name,
                    schedule_name = $schedule,
                    active = $active,
                    updated_at = $now
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", c.Id));
            cmd.Parameters.Add(new DuckDBParameter("name", c.Name));
            cmd.Parameters.Add(new DuckDBParameter("schedule", (object?)c.ScheduleName ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("active", c.Active));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} clearances", clearances.Count);
    }

    public void UpsertReaderClearances(IReadOnlyList<Models.ReaderClearanceRecord> mappings)
    {
        if (mappings.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        // Authoritative replace: this is a full-snapshot refresh. Wipe and rewrite so
        // deletions in the source propagate. Bounded to a few hundred rows total.
        using (var clr = _conn.CreateCommand())
        {
            clr.Transaction = tx;
            clr.CommandText = "DELETE FROM dim_reader_clearances";
            clr.ExecuteNonQuery();
        }
        foreach (var m in mappings)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_reader_clearances (reader_id, clearance_id, updated_at)
                VALUES ($rid, $cid, $now)
                """;
            cmd.Parameters.Add(new DuckDBParameter("rid", m.ReaderId));
            cmd.Parameters.Add(new DuckDBParameter("cid", m.ClearanceId));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} reader-clearance mappings", mappings.Count);
    }

    public void UpsertPersonClearances(IReadOnlyList<Models.PersonClearanceRecord> assignments)
    {
        if (assignments.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        // Same authoritative-replace pattern.
        using (var clr = _conn.CreateCommand())
        {
            clr.Transaction = tx;
            clr.CommandText = "DELETE FROM dim_person_clearances";
            clr.ExecuteNonQuery();
        }
        foreach (var a in assignments)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_person_clearances (person_id, clearance_id, granted_at, expires_at, updated_at)
                VALUES ($pid, $cid, $granted, $expires, $now)
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", a.PersonId));
            cmd.Parameters.Add(new DuckDBParameter("cid", a.ClearanceId));
            cmd.Parameters.Add(new DuckDBParameter("granted", a.GrantedAt));
            cmd.Parameters.Add(new DuckDBParameter("expires", (object?)a.ExpiresAt ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} person-clearance assignments", assignments.Count);
    }

    // ── Authorization queries ───────────────────────────────────────────

    /// <summary>
    /// Is <paramref name="personId"/> currently authorized at any reader belonging
    /// to <paramref name="doorId"/>? Returns the list of granting clearances
    /// (by name) or an empty list if denied. "Current" means the assignment's
    /// <c>expires_at</c> is null or in the future.
    /// </summary>
    public AuthorizationResult CheckAuthorization(long personId, int doorId)
    {
        var now = DateTime.UtcNow;
        var granting = new List<GrantingClearance>();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT DISTINCT c.clearance_id, c.name, c.schedule_name,
                   pc.granted_at, pc.expires_at
            FROM dim_person_clearances pc
            JOIN dim_clearances c ON c.clearance_id = pc.clearance_id
            JOIN dim_reader_clearances rc ON rc.clearance_id = pc.clearance_id
            JOIN dim_readers r ON r.reader_id = rc.reader_id
            WHERE pc.person_id = $pid
              AND r.door_id = $did
              AND c.active = TRUE
              AND (pc.expires_at IS NULL OR pc.expires_at > $now)
            """;
        cmd.Parameters.Add(new DuckDBParameter("pid", (int)personId));
        cmd.Parameters.Add(new DuckDBParameter("did", doorId));
        cmd.Parameters.Add(new DuckDBParameter("now", now));

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            granting.Add(new GrantingClearance(
                ClearanceId: reader.GetInt32(0),
                Name: reader.GetString(1),
                ScheduleName: reader.IsDBNull(2) ? null : reader.GetString(2),
                GrantedAt: reader.GetDateTime(3),
                ExpiresAt: reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
        }

        return new AuthorizationResult(
            PersonId: personId,
            DoorId: doorId,
            Authorized: granting.Count > 0,
            GrantingClearances: granting);
    }

    /// <summary>Doors (collapsed via dim_readers) that <paramref name="personId"/> is currently authorized for.</summary>
    public List<AuthorizedDoor> GetAuthorizedDoors(long personId)
    {
        var now = DateTime.UtcNow;
        var rows = new List<AuthorizedDoor>();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT d.door_id, d.name, STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name)
            FROM dim_person_clearances pc
            JOIN dim_clearances c ON c.clearance_id = pc.clearance_id
            JOIN dim_reader_clearances rc ON rc.clearance_id = pc.clearance_id
            JOIN dim_readers r ON r.reader_id = rc.reader_id
            JOIN dim_doors d ON d.door_id = r.door_id
            WHERE pc.person_id = $pid
              AND c.active = TRUE
              AND (pc.expires_at IS NULL OR pc.expires_at > $now)
            GROUP BY d.door_id, d.name
            ORDER BY d.name
            """;
        cmd.Parameters.Add(new DuckDBParameter("pid", (int)personId));
        cmd.Parameters.Add(new DuckDBParameter("now", now));

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new AuthorizedDoor(
                DoorId: reader.GetInt32(0),
                DoorName: reader.IsDBNull(1) ? null : reader.GetString(1),
                ViaClearances: reader.IsDBNull(2) ? "" : reader.GetString(2)));
        }

        return rows;
    }

    /// <summary>People currently authorized for any reader belonging to <paramref name="doorId"/>.</summary>
    public List<AuthorizedPerson> GetAuthorizedPeopleForDoor(int doorId, int limit)
    {
        var now = DateTime.UtcNow;
        var rows = new List<AuthorizedPerson>();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT p.person_id, p.full_name,
                   STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name)
            FROM dim_person_clearances pc
            JOIN dim_clearances c ON c.clearance_id = pc.clearance_id
            JOIN dim_reader_clearances rc ON rc.clearance_id = pc.clearance_id
            JOIN dim_readers r ON r.reader_id = rc.reader_id
            JOIN dim_people p ON p.person_id = pc.person_id
            WHERE r.door_id = $did
              AND c.active = TRUE
              AND (pc.expires_at IS NULL OR pc.expires_at > $now)
            GROUP BY p.person_id, p.full_name
            ORDER BY p.full_name
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("did", doorId));
        cmd.Parameters.Add(new DuckDBParameter("now", now));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new AuthorizedPerson(
                PersonId: reader.GetInt32(0),
                PersonName: reader.IsDBNull(1) ? null : reader.GetString(1),
                ViaClearances: reader.IsDBNull(2) ? "" : reader.GetString(2)));
        }

        return rows;
    }

    public void UpsertPeople(IReadOnlyList<Models.PersonRecord> people)
    {
        if (people.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var p in people)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_people (person_id, first_name, last_name, full_name, active, updated_at)
                VALUES ($id, $first, $last, $full, TRUE, $now)
                ON CONFLICT (person_id) DO UPDATE SET
                    first_name = $first,
                    last_name = $last,
                    full_name = $full,
                    updated_at = $now
                """;
            var fullName = $"{p.FirstName} {p.LastName}".Trim();
            cmd.Parameters.Add(new DuckDBParameter("id", p.PersonId));
            cmd.Parameters.Add(new DuckDBParameter("first", (object?)p.FirstName ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("last", (object?)p.LastName ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("full", fullName));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} people", people.Count);
    }

    public void UpsertUserCredentials(IReadOnlyList<Models.UserCredentialRecord> credentials)
    {
        if (credentials.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        // Authoritative replace: delete and reinsert. Credentials get revoked
        // (rows removed upstream) so we can't just UPSERT — we'd leave stale
        // mappings pointing at people who no longer own the credential.
        using (var clr = _conn.CreateCommand())
        {
            clr.Transaction = tx;
            clr.CommandText = "DELETE FROM dim_user_credentials";
            clr.ExecuteNonQuery();
        }
        foreach (var c in credentials)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_user_credentials (
                    credential_id, person_id,
                    activation_date, expiration_date, is_activated, expiration_used,
                    updated_at
                )
                VALUES (
                    $cid, $pid,
                    $act, $exp, $is_act, $exp_used,
                    $now
                )
                """;
            cmd.Parameters.Add(new DuckDBParameter("cid", c.CredentialId));
            cmd.Parameters.Add(new DuckDBParameter("pid", c.PersonId));
            cmd.Parameters.Add(new DuckDBParameter("act", (object?)c.ActivationDate ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("exp", (object?)c.ExpirationDate ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("is_act", c.IsActivated));
            cmd.Parameters.Add(new DuckDBParameter("exp_used", c.ExpirationUsed));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} user credentials", credentials.Count);
    }

    public void UpsertOperators(IReadOnlyList<Models.OperatorRecord> operators)
    {
        if (operators.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        // Same authoritative-replace pattern as credentials: operators get
        // added/removed in the source system, and we don't want stale rows.
        using (var clr = _conn.CreateCommand())
        {
            clr.Transaction = tx;
            clr.CommandText = "DELETE FROM dim_operators";
            clr.ExecuteNonQuery();
        }
        foreach (var o in operators)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_operators (operator_id, name, full_name, description, enabled, updated_at)
                VALUES ($id, $name, $full, $desc, $enabled, $now)
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", o.OperatorId));
            cmd.Parameters.Add(new DuckDBParameter("name", o.Name));
            cmd.Parameters.Add(new DuckDBParameter("full", (object?)o.FullName ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("desc", (object?)o.Description ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("enabled", o.Enabled));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} operators", operators.Count);
    }

    // ── Dimension fuzzy search ──────────────────────────────────────────

    public List<DoorMatch> SearchDoors(string query, int limit)
    {
        // One row per (door, reader) via LEFT JOIN, then group in code so each door
        // carries its list of reader names. LEFT JOIN means doors with zero readers
        // still show up (shouldn't happen in practice, but be safe).
        var doorRows = new Dictionary<int, (string Name, string? ControllerAddr, List<string> Readers)>();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT d.door_id, d.name, d.controller_addr, r.name AS reader_name
            FROM dim_doors d
            LEFT JOIN dim_readers r ON r.door_id = d.door_id
            WHERE d.door_id IN (
                SELECT door_id FROM dim_doors
                WHERE name ILIKE $pattern
                ORDER BY LENGTH(name), name
                LIMIT $limit
            )
            ORDER BY d.name, r.name
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var doorId = reader.GetInt32(0);
            if (!doorRows.TryGetValue(doorId, out var entry))
            {
                entry = (
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    new List<string>());
                doorRows[doorId] = entry;
            }
            if (!reader.IsDBNull(3))
                entry.Readers.Add(reader.GetString(3));
        }

        return doorRows
            .Select(kv => new DoorMatch(kv.Key, kv.Value.Name, kv.Value.ControllerAddr, kv.Value.Readers))
            .OrderBy(m => m.Name.Length).ThenBy(m => m.Name)
            .ToList();
    }

    public List<ReaderMatch> SearchReaders(string query, int limit)
    {
        var results = new List<ReaderMatch>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT reader_id, name
            FROM dim_readers
            WHERE name ILIKE $pattern
            ORDER BY LENGTH(name), name
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new ReaderMatch(reader.GetInt32(0), reader.GetString(1)));
        }
        return results;
    }

    public List<PersonMatch> SearchPeople(string query, int limit)
    {
        var results = new List<PersonMatch>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT person_id, first_name, last_name, full_name
            FROM dim_people
            WHERE full_name ILIKE $pattern
               OR first_name ILIKE $pattern
               OR last_name ILIKE $pattern
            ORDER BY LENGTH(full_name), full_name
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new PersonMatch(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetString(3)));
        }
        return results;
    }

    public List<OperatorMatch> SearchOperators(string query, int limit)
    {
        var results = new List<OperatorMatch>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT operator_id, name, full_name, description, enabled
            FROM dim_operators
            WHERE name ILIKE $pattern
               OR full_name ILIKE $pattern
               OR description ILIKE $pattern
            ORDER BY LENGTH(name), name
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new OperatorMatch(
                OperatorId: reader.GetInt32(0),
                Name: reader.GetString(1),
                FullName: reader.IsDBNull(2) ? null : reader.GetString(2),
                Description: reader.IsDBNull(3) ? null : reader.GetString(3),
                Enabled: !reader.IsDBNull(4) && reader.GetBoolean(4)));
        }
        return results;
    }

    /// <summary>
    /// Backing query for search_admin_actions. Combines count + row sample
    /// in two statements so the tool can report `total_matching` alongside
    /// the bounded `events` list. All filters are optional; when none are
    /// set this returns the most recent N events in the window.
    /// descriptionQuery does a case-insensitive partial match — passing
    /// "Server Room" matches any row whose description contains that phrase,
    /// which is how the model will answer topical audit-trail questions
    /// without needing to know event codes.
    /// </summary>
    public (long TotalMatching, List<SoftwareEventRow> Events) SampleSoftwareEvents(
        DateTime since, DateTime until,
        int? operatorId, int? eventCode, string? descriptionQuery,
        string order, int limit)
    {
        var where = "WHERE dt_date >= $since AND dt_date < $until";
        var parameters = new List<DuckDBParameter>
        {
            new("since", since),
            new("until", until),
        };

        if (operatorId.HasValue)
        {
            where += " AND operator_id = $op_id";
            parameters.Add(new DuckDBParameter("op_id", operatorId.Value));
        }
        if (eventCode.HasValue)
        {
            where += " AND event_code = $event_code";
            parameters.Add(new DuckDBParameter("event_code", eventCode.Value));
        }
        if (!string.IsNullOrWhiteSpace(descriptionQuery))
        {
            where += " AND description ILIKE $desc_pattern";
            parameters.Add(new DuckDBParameter("desc_pattern", $"%{descriptionQuery}%"));
        }

        long totalMatching;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_software_events {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalMatching = (long)cmd.ExecuteScalar()!;
        }

        var orderSql = order.Equals("time_asc", StringComparison.OrdinalIgnoreCase) ? "dt_date ASC" : "dt_date DESC";
        var events = new List<SoftwareEventRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT log_id, dt_date, event_code, description,
                       operator_id, operator_name, net_address
                FROM fact_software_events {where}
                ORDER BY {orderSql}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                events.Add(new SoftwareEventRow(
                    LogId:        rd.GetInt32(0),
                    DtDate:       rd.GetDateTime(1),
                    EventCode:    rd.GetInt32(2),
                    Description:  rd.GetString(3),
                    OperatorId:   rd.IsDBNull(4) ? null : rd.GetInt32(4),
                    OperatorName: rd.IsDBNull(5) ? null : rd.GetString(5),
                    NetAddress:   rd.IsDBNull(6) ? null : rd.GetString(6)));
            }
        }

        return (totalMatching, events);
    }

    /// <summary>
    /// Backing query for list_expiring_credentials. Returns credentials whose
    /// expiration_date falls within the requested lookahead, optionally also
    /// including already-expired and/or perpetual (no-expiration) credentials.
    /// Rows are joined through dim_people so the model gets a readable name
    /// per credential. Perpetual rows are emitted with expiration_date=null
    /// and days_until_expiry=null — callers must handle the "no expiration"
    /// case explicitly in the response shape.
    /// </summary>
    public List<ExpiringCredentialRow> ListExpiringCredentials(
        int withinDays, bool includeExpired, bool includePerpetual,
        long? personId, int limit)
    {
        var now = DateTime.UtcNow;
        var lookahead = now.AddDays(withinDays);

        // Three candidate sets unioned based on caller flags. Always include
        // "expiring within window" (the primary use case). Optionally add
        // already-expired and perpetual. Single SQL so pagination/limit
        // applies to the combined set in one sort pass.
        var clauses = new List<string>
        {
            "(expiration_date IS NOT NULL AND expiration_date >= $now AND expiration_date <= $lookahead)"
        };
        if (includeExpired)
            clauses.Add("(expiration_date IS NOT NULL AND expiration_date < $now)");
        if (includePerpetual)
            clauses.Add("(expiration_date IS NULL)");

        var where = "(" + string.Join(" OR ", clauses) + ")";
        if (personId.HasValue)
            where += " AND c.person_id = $pid";

        var results = new List<ExpiringCredentialRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.credential_id, c.person_id, p.full_name,
                   c.activation_date, c.expiration_date,
                   c.is_activated, c.expiration_used
            FROM dim_user_credentials c
            LEFT JOIN dim_people p ON p.person_id = c.person_id
            WHERE {where}
            ORDER BY
                -- Perpetual rows (NULL expiration) sort last; within-window
                -- rows sort by expiration ASC (soonest first); already-expired
                -- rows naturally sort ahead of future expirations.
                c.expiration_date IS NULL,
                c.expiration_date ASC
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("now", now));
        cmd.Parameters.Add(new DuckDBParameter("lookahead", lookahead));
        if (personId.HasValue)
            cmd.Parameters.Add(new DuckDBParameter("pid", personId.Value));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));

        using var rd = cmd.ExecuteReader();
        while (rd.Read())
        {
            var expiration = rd.IsDBNull(4) ? (DateTime?)null : rd.GetDateTime(4);
            int? daysUntil = expiration.HasValue
                ? (int)Math.Round((expiration.Value - now).TotalDays)
                : null;
            results.Add(new ExpiringCredentialRow(
                CredentialId:     rd.GetInt32(0),
                PersonId:         rd.GetInt32(1),
                PersonName:       rd.IsDBNull(2) ? null : rd.GetString(2),
                ActivationDate:   rd.IsDBNull(3) ? null : rd.GetDateTime(3),
                ExpirationDate:   expiration,
                IsActivated:      !rd.IsDBNull(5) && rd.GetBoolean(5),
                ExpirationUsed:   !rd.IsDBNull(6) && rd.GetBoolean(6),
                DaysUntilExpiry:  daysUntil));
        }
        return results;
    }

    public List<EventTypeRow> ListEventTypes()
    {
        var results = new List<EventTypeRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT event_code, category, name, description FROM lookup_event_types ORDER BY event_code";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new EventTypeRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }
        return results;
    }

    public List<DispositionRow> ListDispositions()
    {
        var results = new List<DispositionRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT disposition, name FROM lookup_dispositions ORDER BY disposition";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new DispositionRow(reader.GetInt32(0), reader.GetString(1)));
        }
        return results;
    }

    // ── Inactive entities (set-difference query) ────────────────────────

    /// <summary>
    /// Find entities (people, doors, or readers) with zero activity in a time window.
    /// Returns each inactive entity with its all-time last_seen_at so callers can tell
    /// "never seen" (last_seen_at is null) from "active outside the window" (last_seen_at populated).
    ///
    /// Uses LEFT JOIN + HAVING COUNT(*) FILTER pattern so each entity is evaluated once.
    /// For entity="door", joins through dim_readers so multi-reader doors collapse into
    /// one logical door (consistent with aggregate_events(group_by="door")).
    /// </summary>
    public InactiveEntitiesResult GetInactiveEntities(
        string entity, DateTime since, DateTime until, int limit)
    {
        var entityLower = entity.ToLowerInvariant();

        // Total entity count for the chosen dimension
        var totalSql = entityLower switch
        {
            "person" => "SELECT COUNT(*) FROM dim_people",
            "door"   => "SELECT COUNT(*) FROM dim_doors",
            "reader" => "SELECT COUNT(*) FROM dim_readers",
            _ => throw new ArgumentException($"Unsupported entity: {entity}. Use person, door, or reader.")
        };

        long totalEntities;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = totalSql;
            totalEntities = (long)cmd.ExecuteScalar()!;
        }

        // Inactive-entity sample query — same shape three times. Parameterized by
        // table/columns. The HAVING clause is the truth filter; the LIMIT just bounds
        // the returned sample (not the true inactive total — see countSql below).
        var sampleSql = entityLower switch
        {
            "person" => """
                -- Expand person → their credentials → transactions. A person is
                -- inactive iff NONE of their credentials had a transaction in the
                -- window. LEFT JOIN through dim_user_credentials because
                -- fact_transactions.uid1 is a CredentialId, not a HostUserId.
                SELECT p.person_id, p.full_name, MAX(t.dt_date) AS last_seen_at
                FROM dim_people p
                LEFT JOIN dim_user_credentials c ON c.person_id = p.person_id
                LEFT JOIN fact_transactions t ON t.uid1 = c.credential_id
                GROUP BY p.person_id, p.full_name
                HAVING COUNT(t.log_id) FILTER (WHERE t.dt_date >= $since AND t.dt_date < $until) = 0
                ORDER BY last_seen_at NULLS LAST
                LIMIT $limit
                """,
            "door" => """
                SELECT d.door_id, d.name, MAX(t.dt_date) AS last_seen_at
                FROM dim_doors d
                LEFT JOIN dim_readers r ON r.door_id = d.door_id
                LEFT JOIN fact_transactions t ON t.reader_name = r.name
                GROUP BY d.door_id, d.name
                HAVING COUNT(t.log_id) FILTER (WHERE t.dt_date >= $since AND t.dt_date < $until) = 0
                ORDER BY last_seen_at NULLS LAST
                LIMIT $limit
                """,
            "reader" => """
                SELECT r.reader_id, r.name, MAX(t.dt_date) AS last_seen_at
                FROM dim_readers r
                LEFT JOIN fact_transactions t ON t.reader_name = r.name
                GROUP BY r.reader_id, r.name
                HAVING COUNT(t.log_id) FILTER (WHERE t.dt_date >= $since AND t.dt_date < $until) = 0
                ORDER BY last_seen_at NULLS LAST
                LIMIT $limit
                """,
            _ => throw new InvalidOperationException()
        };

        // Count of all inactive entities (without LIMIT) — needed because the LIMIT
        // bounds the returned sample, not the truth. Wraps the same HAVING query in
        // a subquery so callers can derive active_count = total - inactive_total
        // even when the inactive list is truncated.
        var countSql = entityLower switch
        {
            "person" => """
                SELECT COUNT(*) FROM (
                  SELECT p.person_id
                  FROM dim_people p
                  LEFT JOIN dim_user_credentials c ON c.person_id = p.person_id
                  LEFT JOIN fact_transactions t ON t.uid1 = c.credential_id
                  GROUP BY p.person_id
                  HAVING COUNT(t.log_id) FILTER (WHERE t.dt_date >= $since AND t.dt_date < $until) = 0
                )
                """,
            "door" => """
                SELECT COUNT(*) FROM (
                  SELECT d.door_id
                  FROM dim_doors d
                  LEFT JOIN dim_readers r ON r.door_id = d.door_id
                  LEFT JOIN fact_transactions t ON t.reader_name = r.name
                  GROUP BY d.door_id
                  HAVING COUNT(t.log_id) FILTER (WHERE t.dt_date >= $since AND t.dt_date < $until) = 0
                )
                """,
            "reader" => """
                SELECT COUNT(*) FROM (
                  SELECT r.reader_id
                  FROM dim_readers r
                  LEFT JOIN fact_transactions t ON t.reader_name = r.name
                  GROUP BY r.reader_id
                  HAVING COUNT(t.log_id) FILTER (WHERE t.dt_date >= $since AND t.dt_date < $until) = 0
                )
                """,
            _ => throw new InvalidOperationException()
        };

        long inactiveTotal;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = countSql;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            inactiveTotal = (long)cmd.ExecuteScalar()!;
        }

        var items = new List<InactiveEntity>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = sampleSql;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                items.Add(new InactiveEntity(
                    Id: reader.GetInt32(0),
                    Name: reader.IsDBNull(1) ? "(unknown)" : reader.GetString(1),
                    LastSeenAt: reader.IsDBNull(2) ? null : reader.GetDateTime(2)));
            }
        }

        return new InactiveEntitiesResult(entityLower, items, totalEntities, inactiveTotal);
    }

    public List<AlarmCategoryRow> ListAlarmCategories()
    {
        var results = new List<AlarmCategoryRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT category_id, name, description FROM lookup_alarm_categories ORDER BY category_id";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new AlarmCategoryRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2)));
        }
        return results;
    }

    // ── Time-series ─────────────────────────────────────────────────────

    /// <summary>
    /// Bucketed event counts over a time window. Buckets are returned chronologically
    /// and empty buckets are zero-filled so the caller gets a contiguous series.
    /// Supported buckets: "hour", "day", "week", "month".
    /// </summary>
    public TimeSeriesResult GetTransactionTimeSeries(
        string bucket,
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        long? personId, int? disposition, int? doorId = null)
    {
        var truncUnit = bucket.ToLowerInvariant() switch
        {
            "hour" => "hour",
            "day" => "day",
            "week" => "week",
            "month" => "month",
            _ => throw new ArgumentException($"Unsupported bucket: {bucket}. Use hour, day, week, or month.")
        };

        var (doorProvided, mergedReaders) = ResolveDoorReaders(doorId, readerName, readerNames);
        if (doorProvided && mergedReaders!.Count == 0)
            return new TimeSeriesResult(truncUnit, new List<TimeSeriesPoint>(), 0);

        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode,
            doorProvided ? null : readerName,
            doorProvided ? mergedReaders : readerNames,
            personId, disposition);

        // DuckDB's generate_series + date_trunc lets us zero-fill empty buckets
        // in a single query rather than post-processing in C#.
        var sql = $"""
            WITH buckets AS (
                SELECT DATE_TRUNC('{truncUnit}', ts) AS bucket
                FROM generate_series(
                    DATE_TRUNC('{truncUnit}', CAST($since AS TIMESTAMP)),
                    DATE_TRUNC('{truncUnit}', CAST($until AS TIMESTAMP)),
                    INTERVAL 1 {truncUnit}
                ) AS t(ts)
            ),
            counts AS (
                SELECT DATE_TRUNC('{truncUnit}', dt_date) AS bucket, COUNT(*) AS c
                FROM fact_transactions {where}
                GROUP BY 1
            )
            SELECT b.bucket, COALESCE(c.c, 0) AS c
            FROM buckets b
            LEFT JOIN counts c ON c.bucket = b.bucket
            WHERE b.bucket < CAST($until AS TIMESTAMP)
            ORDER BY b.bucket ASC
            """;

        var points = new List<TimeSeriesPoint>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            points.Add(new TimeSeriesPoint(reader.GetDateTime(0), reader.GetInt64(1)));
        }

        var total = points.Sum(p => p.Count);
        return new TimeSeriesResult(truncUnit, points, total);
    }

    // ── Single-row lookups ──────────────────────────────────────────────

    public TransactionDetail? GetTransaction(int logId)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT log_id, dt_date, pc_date_time, event_code, description,
                   disposition, transaction_type, report_as_alarm, alarm_level_priority,
                   port_addr, dt_addr, x_addr, net_address,
                   reader_name, from_zone, to_zone,
                   uid1, uid1_name, uid2, uid2_name
            FROM fact_transactions
            WHERE log_id = $log_id
            """;
        cmd.Parameters.Add(new DuckDBParameter("log_id", logId));

        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return null;

        return new TransactionDetail(
            LogId: reader.GetInt32(0),
            DtDate: reader.GetDateTime(1),
            PcDateTime: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            EventCode: reader.GetInt32(3),
            Description: reader.IsDBNull(4) ? null : reader.GetString(4),
            Disposition: reader.GetInt32(5),
            TransactionType: reader.IsDBNull(6) ? null : reader.GetInt32(6),
            ReportAsAlarm: reader.IsDBNull(7) ? null : reader.GetBoolean(7),
            AlarmLevelPriority: reader.IsDBNull(8) ? null : reader.GetInt32(8),
            PortAddr: reader.IsDBNull(9) ? null : reader.GetString(9),
            DtAddr: reader.IsDBNull(10) ? null : reader.GetString(10),
            XAddr: reader.IsDBNull(11) ? null : reader.GetString(11),
            NetAddress: reader.IsDBNull(12) ? null : reader.GetString(12),
            ReaderName: reader.IsDBNull(13) ? null : reader.GetString(13),
            FromZone: reader.IsDBNull(14) ? null : Convert.ToInt32(reader.GetValue(14)),
            ToZone: reader.IsDBNull(15) ? null : Convert.ToInt32(reader.GetValue(15)),
            PersonId: reader.IsDBNull(16) ? null : reader.GetInt32(16),
            PersonName: reader.IsDBNull(17) ? null : reader.GetString(17),
            Uid2: reader.IsDBNull(18) ? null : reader.GetInt32(18),
            Uid2Name: reader.IsDBNull(19) ? null : reader.GetString(19));
    }

    public AlarmDetail? GetAlarm(int alarmId)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT alarm_id, dt_date, db_date, ak_date, cl_date,
                   event_id, alarm_level_priority, status, description,
                   net_address, ak_operator, cl_operator, workstation_name,
                   uid1, uid1_name, parm1, parm2
            FROM fact_alarms
            WHERE alarm_id = $alarm_id
            """;
        cmd.Parameters.Add(new DuckDBParameter("alarm_id", alarmId));

        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return null;

        return new AlarmDetail(
            AlarmId: reader.GetInt32(0),
            DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
            DbDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            AkDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            ClDate: reader.IsDBNull(4) ? null : reader.GetDateTime(4),
            EventId: reader.GetInt32(5),
            AlarmLevelPriority: reader.IsDBNull(6) ? null : reader.GetInt32(6),
            Status: reader.IsDBNull(7) ? null : Convert.ToInt32(reader.GetValue(7)),
            Description: reader.IsDBNull(8) ? null : reader.GetString(8),
            NetAddress: reader.IsDBNull(9) ? null : reader.GetString(9),
            AkOperator: reader.IsDBNull(10) ? null : reader.GetString(10),
            ClOperator: reader.IsDBNull(11) ? null : reader.GetString(11),
            WorkstationName: reader.IsDBNull(12) ? null : reader.GetString(12),
            PersonId: reader.IsDBNull(13) ? null : Convert.ToInt64(reader.GetDouble(13)),
            PersonName: reader.IsDBNull(14) ? null : reader.GetString(14),
            Parm1: reader.IsDBNull(15) ? null : reader.GetString(15),
            Parm2: reader.IsDBNull(16) ? null : reader.GetString(16));
    }

    // ── Alarm queries ───────────────────────────────────────────────────

    public long CountAlarms(
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName)
    {
        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM fact_alarms {where}";
        foreach (var p in parameters) cmd.Parameters.Add(p);
        return (long)cmd.ExecuteScalar()!;
    }

    /// <summary>
    /// Aggregate alarms grouped by a dimension. Returns top-N groups plus totals.
    /// Supported group_by values: "event", "priority", "status", "person", "workstation", "hour", "day".
    /// </summary>
    public AggregationResult AggregateAlarms(
        string groupBy,
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName,
        int limit)
    {
        var (keyExpr, keyIdExpr, groupKey, distinctExpr) = groupBy.ToLowerInvariant() switch
        {
            "event" or "event_id" => ("CAST(event_id AS VARCHAR)", "event_id", "event_id", "event_id"),
            "priority" => ("CAST(alarm_level_priority AS VARCHAR)", "alarm_level_priority", "alarm_level_priority", "alarm_level_priority"),
            "status" => ("CAST(status AS VARCHAR)", "status", "status", "status"),
            "person" => ("uid1_name", "CAST(uid1 AS BIGINT)", "uid1_name, uid1", "uid1"),
            "workstation" => ("workstation_name", "NULL", "workstation_name", "workstation_name"),
            "hour" => ("CAST(DATE_TRUNC('hour', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('hour', dt_date)", "DATE_TRUNC('hour', dt_date)"),
            "day" => ("CAST(DATE_TRUNC('day', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('day', dt_date)", "DATE_TRUNC('day', dt_date)"),
            _ => throw new ArgumentException($"Unsupported group_by: {groupBy}. Use event, priority, status, person, workstation, hour, or day.")
        };

        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);

        long totalEvents;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_alarms {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalEvents = (long)cmd.ExecuteScalar()!;
        }

        long totalGroups;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(DISTINCT {distinctExpr}) FROM fact_alarms {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalGroups = (long)cmd.ExecuteScalar()!;
        }

        var orderBy = groupBy.ToLowerInvariant() is "hour" or "day" ? "key ASC" : "count DESC";
        var groups = new List<AggregationBucket>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT {keyExpr} AS key, {keyIdExpr} AS key_id, COUNT(*) AS count
                FROM fact_alarms {where}
                GROUP BY {groupKey}
                ORDER BY {orderBy}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                groups.Add(new AggregationBucket(
                    reader.IsDBNull(0) ? null : reader.GetValue(0).ToString(),
                    reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1)),
                    reader.GetInt64(2)));
            }
        }

        return new AggregationResult(
            groupBy,
            groups,
            totalEvents,
            totalGroups,
            Truncated: totalGroups > limit);
    }

    public AlarmSampleResult SampleAlarms(
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName,
        string order, int limit)
    {
        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);
        var orderSql = order.Equals("time_asc", StringComparison.OrdinalIgnoreCase) ? "dt_date ASC" : "dt_date DESC";

        long totalMatching;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_alarms {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalMatching = (long)cmd.ExecuteScalar()!;
        }

        var rows = new List<AlarmSampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT alarm_id, dt_date, ak_date, cl_date, event_id,
                       alarm_level_priority, status, description,
                       uid1, uid1_name, workstation_name
                FROM fact_alarms {where}
                ORDER BY {orderSql}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new AlarmSampleRow(
                    AlarmId: reader.GetInt32(0),
                    DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    AkDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    ClDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    EventId: reader.GetInt32(4),
                    AlarmLevelPriority: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    Status: reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6)),
                    Description: reader.IsDBNull(7) ? null : reader.GetString(7),
                    PersonId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetDouble(8)),
                    PersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    WorkstationName: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        return new AlarmSampleResult(rows, totalMatching, Truncated: totalMatching > limit);
    }

    // ── Daily security briefing ─────────────────────────────────────────

    /// <summary>
    /// One-call morning briefing for a single calendar day. Composes 6 queries:
    /// headline transaction totals, headline alarm totals, same two for the
    /// prior day (for deltas), notable deniers (people with ≥ threshold denied
    /// events), busiest doors (collapsed via dim_readers), and open alarms.
    /// Forced-open sample events come from fact_transactions event_code 4.
    /// The returned result includes the day window and prior-day window so
    /// tool layer can surface them without re-parsing.
    /// </summary>
    public DailySecurityBriefingResult GetDailySecurityBriefing(
        DateTime date,
        int topDoorsLimit,
        int notableDeniersThreshold,
        int forcedOpenSampleLimit,
        int openAlarmsLimit)
    {
        var dayStart = date.Date;
        var dayEnd = dayStart.AddDays(1);
        var priorStart = dayStart.AddDays(-1);
        var priorEnd = dayStart;

        // ── Headline (current day) ──────────────────────────────────────
        var headline = QueryBriefingMetrics(dayStart, dayEnd);
        var prior = QueryBriefingMetrics(priorStart, priorEnd);
        var deltas = new DailySecurityBriefingMetrics(
            headline.TotalAccess - prior.TotalAccess,
            headline.TotalDenied - prior.TotalDenied,
            headline.TotalAlarms - prior.TotalAlarms,
            headline.AlarmsUnacked - prior.AlarmsUnacked,
            headline.ForcedOpens - prior.ForcedOpens,
            headline.HeldOpens - prior.HeldOpens);

        // ── Forced-open sample events (transaction side: the actual door events) ──
        var forcedOpenEvents = new List<SampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT log_id, dt_date, event_code, description, disposition,
                       reader_name, uid1, uid1_name
                FROM fact_transactions
                WHERE dt_date >= $since AND dt_date < $until AND event_code = 4
                ORDER BY dt_date ASC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", dayStart));
            cmd.Parameters.Add(new DuckDBParameter("until", dayEnd));
            cmd.Parameters.Add(new DuckDBParameter("limit", forcedOpenSampleLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                forcedOpenEvents.Add(new SampleRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        // ── Notable deniers: people with ≥ threshold denied events ──────
        var notableDeniers = new List<NotableDenier>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT uid1, uid1_name, COUNT(*) AS denial_count
                FROM fact_transactions
                WHERE dt_date >= $since AND dt_date < $until
                  AND disposition > 1
                  AND uid1 > 0
                GROUP BY uid1, uid1_name
                HAVING COUNT(*) >= $threshold
                ORDER BY denial_count DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", dayStart));
            cmd.Parameters.Add(new DuckDBParameter("until", dayEnd));
            cmd.Parameters.Add(new DuckDBParameter("threshold", notableDeniersThreshold));
            cmd.Parameters.Add(new DuckDBParameter("limit", topDoorsLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                notableDeniers.Add(new NotableDenier(
                    PersonId: Convert.ToInt64(reader.GetValue(0)),
                    PersonName: reader.IsDBNull(1) ? null : reader.GetString(1),
                    DenialCount: reader.GetInt64(2)));
            }
        }

        // ── Busiest doors (collapsed through dim_readers → dim_doors) ──
        var busiestDoors = new List<DoorCountRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT d.door_id, d.name, COUNT(*) AS cnt
                FROM fact_transactions t
                JOIN dim_readers r ON r.name = t.reader_name
                JOIN dim_doors d ON d.door_id = r.door_id
                WHERE t.dt_date >= $since AND t.dt_date < $until
                GROUP BY d.door_id, d.name
                ORDER BY cnt DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", dayStart));
            cmd.Parameters.Add(new DuckDBParameter("until", dayEnd));
            cmd.Parameters.Add(new DuckDBParameter("limit", topDoorsLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                busiestDoors.Add(new DoorCountRow(
                    DoorId: reader.GetInt32(0),
                    DoorName: reader.IsDBNull(1) ? null : reader.GetString(1),
                    Count: reader.GetInt64(2)));
            }
        }

        // ── Open alarms (unacked, most recent first) ────────────────────
        var openAlarms = new List<AlarmSampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT alarm_id, dt_date, ak_date, cl_date, event_id,
                       alarm_level_priority, status, description,
                       uid1, uid1_name, workstation_name
                FROM fact_alarms
                WHERE dt_date >= $since AND dt_date < $until AND ak_date IS NULL
                ORDER BY dt_date DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", dayStart));
            cmd.Parameters.Add(new DuckDBParameter("until", dayEnd));
            cmd.Parameters.Add(new DuckDBParameter("limit", openAlarmsLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                openAlarms.Add(new AlarmSampleRow(
                    AlarmId: reader.GetInt32(0),
                    DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    AkDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    ClDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    EventId: reader.GetInt32(4),
                    AlarmLevelPriority: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    Status: reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6)),
                    Description: reader.IsDBNull(7) ? null : reader.GetString(7),
                    PersonId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetDouble(8)),
                    PersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    WorkstationName: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        return new DailySecurityBriefingResult(
            dayStart, dayEnd, priorStart, priorEnd,
            headline, prior, deltas,
            forcedOpenEvents, notableDeniers, busiestDoors, openAlarms);
    }

    private DailySecurityBriefingMetrics QueryBriefingMetrics(DateTime since, DateTime until)
    {
        // Transaction-side totals
        long totalAccess, totalDenied;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT
                  COUNT(*) FILTER (WHERE disposition = 1),
                  COUNT(*) FILTER (WHERE disposition > 1)
                FROM fact_transactions
                WHERE dt_date >= $since AND dt_date < $until
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            using var reader = cmd.ExecuteReader();
            reader.Read();
            totalAccess = reader.GetInt64(0);
            totalDenied = reader.GetInt64(1);
        }

        // Alarm-side totals (total, unacked, forced, held)
        long totalAlarms, alarmsUnacked, forcedOpens, heldOpens;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT
                  COUNT(*),
                  COUNT(*) FILTER (WHERE ak_date IS NULL),
                  COUNT(*) FILTER (WHERE event_id = 4),
                  COUNT(*) FILTER (WHERE event_id = 5)
                FROM fact_alarms
                WHERE dt_date >= $since AND dt_date < $until
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            using var reader = cmd.ExecuteReader();
            reader.Read();
            totalAlarms = reader.GetInt64(0);
            alarmsUnacked = reader.GetInt64(1);
            forcedOpens = reader.GetInt64(2);
            heldOpens = reader.GetInt64(3);
        }

        return new DailySecurityBriefingMetrics(
            totalAccess, totalDenied, totalAlarms, alarmsUnacked, forcedOpens, heldOpens);
    }

    // ── Forced-through attempts (credential sharing signal) ─────────────

    /// <summary>
    /// Find every "denied then granted within N seconds at the same reader" pair.
    /// Uses LEAD partitioned by reader_name ordered by dt_date, so each denied
    /// event's immediate successor at the same reader is the candidate granted
    /// event. Pairs where the successor is also denied, missing, or further than
    /// <paramref name="maxGapSeconds"/> away are discarded. This is the honest
    /// framing for the query gemini labeled "tailgating": true tailgating leaves
    /// no badge record, but this SQL does surface credential-share attempts
    /// where someone was denied and immediately someone else successfully
    /// badged through the same reader.
    /// </summary>
    public ForcedThroughResult GetForcedThroughAttempts(
        DateTime since, DateTime until, int? doorId, int maxGapSeconds, int limit)
    {
        // Door filter: resolve to reader names; unknown door = empty result.
        List<string>? readerNames = null;
        if (doorId.HasValue)
        {
            readerNames = new List<string>();
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "SELECT name FROM dim_readers WHERE door_id = $id";
            cmd.Parameters.Add(new DuckDBParameter("id", doorId.Value));
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) readerNames.Add(reader.GetString(0));
            if (readerNames.Count == 0)
                return new ForcedThroughResult(new List<ForcedThroughPair>(), 0);
        }

        var readerFilter = "";
        if (readerNames != null)
        {
            var placeholders = Enumerable.Range(0, readerNames.Count).Select(i => $"$rn_{i}");
            readerFilter = $" AND reader_name IN ({string.Join(", ", placeholders)})";
        }

        // Single CTE + window function. LEAD looks at the next row at the same reader.
        var sql = $"""
            WITH ordered AS (
                SELECT
                  log_id, dt_date, reader_name, disposition, description, uid1, uid1_name,
                  LEAD(log_id)       OVER (PARTITION BY reader_name ORDER BY dt_date) AS next_log_id,
                  LEAD(dt_date)      OVER (PARTITION BY reader_name ORDER BY dt_date) AS next_dt,
                  LEAD(disposition)  OVER (PARTITION BY reader_name ORDER BY dt_date) AS next_disposition,
                  LEAD(description)  OVER (PARTITION BY reader_name ORDER BY dt_date) AS next_description,
                  LEAD(uid1)         OVER (PARTITION BY reader_name ORDER BY dt_date) AS next_uid1,
                  LEAD(uid1_name)    OVER (PARTITION BY reader_name ORDER BY dt_date) AS next_uid1_name
                FROM fact_transactions
                WHERE dt_date >= $since AND dt_date < $until{readerFilter}
            )
            SELECT
              log_id, dt_date, disposition, description, uid1, uid1_name,
              next_log_id, next_dt, next_uid1, next_uid1_name, next_description,
              reader_name,
              EXTRACT(EPOCH FROM (next_dt - dt_date)) AS gap_seconds
            FROM ordered
            WHERE disposition > 1
              AND next_disposition = 1
              AND next_dt IS NOT NULL
              AND EXTRACT(EPOCH FROM (next_dt - dt_date)) <= $gap
            ORDER BY dt_date DESC
            LIMIT $limit
            """;

        var pairs = new List<ForcedThroughPair>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = sql;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            if (readerNames != null)
                for (int i = 0; i < readerNames.Count; i++)
                    cmd.Parameters.Add(new DuckDBParameter($"rn_{i}", readerNames[i]));
            cmd.Parameters.Add(new DuckDBParameter("gap", (double)maxGapSeconds));
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                pairs.Add(new ForcedThroughPair(
                    DeniedLogId: reader.GetInt32(0),
                    DeniedTime: reader.GetDateTime(1),
                    DeniedDisposition: reader.GetInt32(2),
                    DeniedDescription: reader.IsDBNull(3) ? null : reader.GetString(3),
                    DeniedPersonId: Convert.ToInt64(reader.GetValue(4)),
                    DeniedPersonName: reader.IsDBNull(5) ? null : reader.GetString(5),
                    GrantedLogId: reader.GetInt32(6),
                    GrantedTime: reader.GetDateTime(7),
                    GrantedPersonId: Convert.ToInt64(reader.GetValue(8)),
                    GrantedPersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    GrantedDescription: reader.IsDBNull(10) ? null : reader.GetString(10),
                    ReaderName: reader.IsDBNull(11) ? null : reader.GetString(11),
                    GapSeconds: Convert.ToDouble(reader.GetValue(12))));
            }
        }

        return new ForcedThroughResult(pairs, pairs.Count);
    }

    // ── Daily attendance ────────────────────────────────────────────────

    /// <summary>
    /// First/last granted badge per person per day over the time window.
    /// Granted-only (disposition = 1) and excludes system events (uid1 = 0).
    /// Optional <paramref name="personId"/> narrows to a single person across
    /// multiple days. Rows are ordered day DESC, then last_seen DESC, so the
    /// most recent departures float to the top.
    /// </summary>
    public DailyAttendanceResult GetDailyAttendance(
        DateTime since, DateTime until, long? personId, int limit)
    {
        // Roll up through credentials → people. A person with multiple
        // badges should appear as ONE row per day, not one row per badge.
        // Orphan credentials (uid1 not in dim_user_credentials) collapse
        // under a synthetic "credential <id>" bucket via COALESCE so
        // their activity is still surfaced rather than silently dropped.
        var fromJoin = """
            fact_transactions t
            LEFT JOIN dim_user_credentials c ON c.credential_id = t.uid1
            LEFT JOIN dim_people p ON p.person_id = c.person_id
            """;

        var where = "WHERE t.dt_date >= $since AND t.dt_date < $until AND t.disposition = 1 AND t.uid1 > 0";
        if (personId.HasValue) where += " AND c.person_id = $pid";

        // Composite group key: resolved persons collapse to "person:<id>",
        // orphans stay split per credential as "cred:<id>". Same shape we
        // use in AggregateTransactionsByPerson.
        const string compositeGroupKey =
            "COALESCE('person:' || CAST(c.person_id AS VARCHAR), 'cred:' || CAST(t.uid1 AS VARCHAR))";

        long totalRows;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT COUNT(*) FROM (
                    SELECT {compositeGroupKey} AS group_key,
                           DATE_TRUNC('day', t.dt_date) AS day
                    FROM {fromJoin} {where}
                    GROUP BY group_key, day
                ) AS s
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            if (personId.HasValue) cmd.Parameters.Add(new DuckDBParameter("pid", personId.Value));
            totalRows = (long)cmd.ExecuteScalar()!;
        }

        var rows = new List<DailyAttendanceRow>();
        using (var cmd = _conn.CreateCommand())
        {
            // PersonId in the result is the real HostUserId for matched rows.
            // For orphan credentials we report the credential id (negated to
            // distinguish from real person ids) — this is the only branch where
            // PersonId might NOT be a HostUserId, and only for orphans we have
            // no other id to report.
            cmd.CommandText = $"""
                SELECT
                    COALESCE(MAX(c.person_id), -MAX(t.uid1)) AS person_id,
                    COALESCE(MAX(p.full_name), MAX(t.uid1_name), 'Unknown credential ' || CAST(MAX(t.uid1) AS VARCHAR)) AS person_name,
                    DATE_TRUNC('day', t.dt_date) AS day,
                    MIN(t.dt_date) AS first_seen,
                    MAX(t.dt_date) AS last_seen,
                    COUNT(*) AS event_count
                FROM {fromJoin} {where}
                GROUP BY {compositeGroupKey}, DATE_TRUNC('day', t.dt_date)
                ORDER BY day DESC, last_seen DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            if (personId.HasValue) cmd.Parameters.Add(new DuckDBParameter("pid", personId.Value));
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new DailyAttendanceRow(
                    PersonId: Convert.ToInt64(reader.GetValue(0)),
                    PersonName: reader.IsDBNull(1) ? null : reader.GetString(1),
                    Day: reader.GetDateTime(2),
                    FirstSeen: reader.GetDateTime(3),
                    LastSeen: reader.GetDateTime(4),
                    EventCount: reader.GetInt64(5)));
            }
        }

        return new DailyAttendanceResult(since, until, rows, totalRows);
    }

    // ── Surrounding events (correlation window) ─────────────────────────

    /// <summary>
    /// Return every transaction and alarm whose dt_date falls within
    /// <paramref name="timestamp"/> ± <paramref name="windowMinutes"/>. If
    /// <paramref name="doorId"/> is supplied, transactions are restricted to
    /// that door's readers and alarms are restricted to rows whose
    /// <c>net_address</c> matches <c>dim_doors.controller_addr</c> (best-effort,
    /// may return zero if controller_addr is unset). Both lists are ordered
    /// ascending by time and capped at <paramref name="limit"/> rows per side.
    /// </summary>
    public SurroundingEventsResult GetSurroundingEvents(
        DateTime timestamp, int windowMinutes, int? doorId, int limit)
    {
        var since = timestamp.AddMinutes(-windowMinutes);
        var until = timestamp.AddMinutes(windowMinutes);

        // Resolve door_id → reader_names (+ controller_addr for alarms). A doorId
        // that has no readers in dim_readers collapses to an empty filter so
        // both queries return empty rather than matching everything.
        List<string>? readerNames = null;
        string? controllerAddr = null;
        if (doorId.HasValue)
        {
            readerNames = new List<string>();
            using (var cmd = _conn.CreateCommand())
            {
                cmd.CommandText = "SELECT name FROM dim_readers WHERE door_id = $id";
                cmd.Parameters.Add(new DuckDBParameter("id", doorId.Value));
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) readerNames.Add(reader.GetString(0));
            }

            using (var cmd = _conn.CreateCommand())
            {
                cmd.CommandText = "SELECT controller_addr FROM dim_doors WHERE door_id = $id";
                cmd.Parameters.Add(new DuckDBParameter("id", doorId.Value));
                var result = cmd.ExecuteScalar();
                controllerAddr = result == null || result is DBNull ? null : result.ToString();
            }
        }

        // ── Transactions ────────────────────────────────────────────────
        var events = new List<SampleRow>();
        var txWhere = "WHERE dt_date >= $since AND dt_date < $until";
        if (doorId.HasValue)
        {
            if (readerNames!.Count == 0)
            {
                txWhere += " AND FALSE";
            }
            else
            {
                var placeholders = Enumerable.Range(0, readerNames.Count).Select(i => $"$rn_{i}");
                txWhere += $" AND reader_name IN ({string.Join(", ", placeholders)})";
            }
        }

        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT log_id, dt_date, event_code, description, disposition,
                       reader_name, uid1, uid1_name
                FROM fact_transactions {txWhere}
                ORDER BY dt_date ASC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            if (doorId.HasValue)
                for (int i = 0; i < readerNames!.Count; i++)
                    cmd.Parameters.Add(new DuckDBParameter($"rn_{i}", readerNames[i]));
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                events.Add(new SampleRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        // ── Alarms ──────────────────────────────────────────────────────
        var alarms = new List<AlarmSampleRow>();
        // Door filter: unknown door (no readers) => empty; controller_addr unset => empty.
        var alarmBlocked = doorId.HasValue && (readerNames!.Count == 0 || controllerAddr == null);
        if (!alarmBlocked)
        {
            var alWhere = "WHERE dt_date >= $since AND dt_date < $until";
            if (doorId.HasValue) alWhere += " AND net_address = $addr";

            using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT alarm_id, dt_date, ak_date, cl_date, event_id,
                       alarm_level_priority, status, description,
                       uid1, uid1_name, workstation_name
                FROM fact_alarms {alWhere}
                ORDER BY dt_date ASC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            if (doorId.HasValue)
                cmd.Parameters.Add(new DuckDBParameter("addr", controllerAddr!));
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                alarms.Add(new AlarmSampleRow(
                    AlarmId: reader.GetInt32(0),
                    DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    AkDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    ClDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    EventId: reader.GetInt32(4),
                    AlarmLevelPriority: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    Status: reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6)),
                    Description: reader.IsDBNull(7) ? null : reader.GetString(7),
                    PersonId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetDouble(8)),
                    PersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    WorkstationName: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        return new SurroundingEventsResult(
            timestamp, windowMinutes, doorId, since, until, events, alarms);
    }

    // ── Door dossier ────────────────────────────────────────────────────

    /// <summary>
    /// Compose a single-door activity report. Transaction-side sections
    /// (summary, hourly_traffic, top_users, recent_denials) use the resolved
    /// reader_names for this door so multi-reader doors are handled correctly.
    /// Alarm-side sections are best-effort joined on
    /// <c>fact_alarms.net_address = dim_doors.controller_addr</c>; if the door
    /// has no controller_addr, or real data doesn't align, alarm counts come
    /// back as zero.
    /// </summary>
    public DoorDossierResult GetDoorDossier(
        int doorId, DateTime since, DateTime until,
        int topUsersLimit, int recentLimit)
    {
        // Door name + controller_addr
        string? doorName = null;
        string? controllerAddr = null;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name, controller_addr FROM dim_doors WHERE door_id = $id";
            cmd.Parameters.Add(new DuckDBParameter("id", doorId));
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                doorName = reader.IsDBNull(0) ? null : reader.GetString(0);
                controllerAddr = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }

        // Reader names for this door
        var readerNames = new List<string>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM dim_readers WHERE door_id = $id ORDER BY name";
            cmd.Parameters.Add(new DuckDBParameter("id", doorId));
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) readerNames.Add(reader.GetString(0));
        }

        // Build the shared "IN (…)" filter for transaction queries
        var emptyDoor = readerNames.Count == 0;
        string readerInClause;
        List<DuckDBParameter> MakeReaderParams(string prefix = "rn")
        {
            var list = new List<DuckDBParameter>();
            for (int i = 0; i < readerNames.Count; i++)
                list.Add(new DuckDBParameter($"{prefix}_{i}", readerNames[i]));
            return list;
        }
        if (emptyDoor)
        {
            // Impossible filter so every query returns empty without special-casing below.
            readerInClause = "FALSE";
        }
        else
        {
            var names = Enumerable.Range(0, readerNames.Count).Select(i => $"$rn_{i}");
            readerInClause = $"reader_name IN ({string.Join(", ", names)})";
        }

        // Summary: total_access, total_denied, distinct_people, first/last seen.
        // distinct_people MUST count real HostUserIds via the credentials join —
        // not raw uid1 values, which are CredentialIds. The composite group key
        // ('person:N' for matched, 'cred:N' for orphans) is the same shape we
        // use in AggregateTransactionsByPerson and GetDailyAttendance: a person
        // with two badges counts as one, an orphan credential counts as one.
        long totalAccess, totalDenied, distinctPeople;
        DateTime? firstSeen, lastSeen;
        // Reader-name filter has to be re-prefixed with t. when we add the join.
        var readerInClauseT = readerInClause.Replace("reader_name", "t.reader_name");
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT
                  COUNT(*) FILTER (WHERE t.disposition = 1),
                  COUNT(*) FILTER (WHERE t.disposition > 1),
                  COUNT(DISTINCT COALESCE('person:' || CAST(c.person_id AS VARCHAR), 'cred:' || CAST(t.uid1 AS VARCHAR))),
                  MIN(t.dt_date),
                  MAX(t.dt_date)
                FROM fact_transactions t
                LEFT JOIN dim_user_credentials c ON c.credential_id = t.uid1
                WHERE t.dt_date >= $since AND t.dt_date < $until AND {readerInClauseT}
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            foreach (var p in MakeReaderParams()) cmd.Parameters.Add(p);
            using var reader = cmd.ExecuteReader();
            reader.Read();
            totalAccess = reader.GetInt64(0);
            totalDenied = reader.GetInt64(1);
            distinctPeople = reader.GetInt64(2);
            firstSeen = reader.IsDBNull(3) ? null : reader.GetDateTime(3);
            lastSeen = reader.IsDBNull(4) ? null : reader.GetDateTime(4);
        }

        // Total alarms — best-effort join on controller_addr
        long totalAlarms = 0;
        if (controllerAddr != null)
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = """
                SELECT COUNT(*) FROM fact_alarms
                WHERE dt_date >= $since AND dt_date < $until AND net_address = $addr
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            cmd.Parameters.Add(new DuckDBParameter("addr", controllerAddr));
            totalAlarms = (long)cmd.ExecuteScalar()!;
        }

        // Hourly pattern — zero-filled 0..23
        var hourCounts = new long[24];
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT CAST(EXTRACT(HOUR FROM dt_date) AS INTEGER) AS h, COUNT(*)
                FROM fact_transactions
                WHERE dt_date >= $since AND dt_date < $until AND {readerInClause}
                GROUP BY h
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            foreach (var p in MakeReaderParams()) cmd.Parameters.Add(p);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var hour = reader.GetInt32(0);
                if (hour >= 0 && hour < 24) hourCounts[hour] = reader.GetInt64(1);
            }
        }
        var hourlyTraffic = Enumerable.Range(0, 24)
            .Select(h => new HourlyBucket(h, hourCounts[h])).ToList();

        // Busiest = argmax; quietest = argmin over hours that had any activity.
        int? busiestHour = null, quietestHour = null;
        if (hourCounts.Any(c => c > 0))
        {
            busiestHour = Enumerable.Range(0, 24).OrderByDescending(h => hourCounts[h]).First();
            quietestHour = Enumerable.Range(0, 24)
                .Where(h => hourCounts[h] > 0)
                .OrderBy(h => hourCounts[h])
                .First();
        }

        // Top users — rolls up through credentials → people. A person with
        // two badges who used both at this door counts as ONE row with the
        // combined event count, not two separate rows. Same composite group
        // key + MAX() aggregation pattern as AggregateTransactionsByPerson.
        // For matched rows PersonId is the real HostUserId; for orphans it's
        // -uid1 (negative as a marker that the row is unresolved).
        var topUsers = new List<PersonCountRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT
                    COALESCE(MAX(c.person_id), -MAX(t.uid1)) AS person_id,
                    COALESCE(
                        MAX(p.full_name),
                        MAX(t.uid1_name),
                        'Unknown credential ' || CAST(MAX(t.uid1) AS VARCHAR)
                    ) AS person_name,
                    COUNT(*) AS cnt
                FROM fact_transactions t
                LEFT JOIN dim_user_credentials c ON c.credential_id = t.uid1
                LEFT JOIN dim_people p ON p.person_id = c.person_id
                WHERE t.dt_date >= $since AND t.dt_date < $until AND {readerInClauseT}
                GROUP BY COALESCE('person:' || CAST(c.person_id AS VARCHAR), 'cred:' || CAST(t.uid1 AS VARCHAR))
                ORDER BY cnt DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            foreach (var p in MakeReaderParams()) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", topUsersLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                topUsers.Add(new PersonCountRow(
                    PersonId: Convert.ToInt64(reader.GetValue(0)),
                    PersonName: reader.IsDBNull(1) ? null : reader.GetString(1),
                    Count: reader.GetInt64(2)));
            }
        }

        // Recent denials
        var recentDenials = new List<SampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT log_id, dt_date, event_code, description, disposition,
                       reader_name, uid1, uid1_name
                FROM fact_transactions
                WHERE dt_date >= $since AND dt_date < $until AND {readerInClause} AND disposition > 1
                ORDER BY dt_date DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            foreach (var p in MakeReaderParams()) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", recentLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                recentDenials.Add(new SampleRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        // Recent alarms — best-effort join on controller_addr
        var recentAlarms = new List<AlarmSampleRow>();
        if (controllerAddr != null)
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = """
                SELECT alarm_id, dt_date, ak_date, cl_date, event_id,
                       alarm_level_priority, status, description,
                       uid1, uid1_name, workstation_name
                FROM fact_alarms
                WHERE dt_date >= $since AND dt_date < $until AND net_address = $addr
                ORDER BY dt_date DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            cmd.Parameters.Add(new DuckDBParameter("addr", controllerAddr));
            cmd.Parameters.Add(new DuckDBParameter("limit", recentLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                recentAlarms.Add(new AlarmSampleRow(
                    AlarmId: reader.GetInt32(0),
                    DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    AkDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    ClDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    EventId: reader.GetInt32(4),
                    AlarmLevelPriority: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    Status: reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6)),
                    Description: reader.IsDBNull(7) ? null : reader.GetString(7),
                    PersonId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetDouble(8)),
                    PersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    WorkstationName: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        var summary = new DoorDossierSummary(
            totalAccess, totalDenied, totalAlarms, distinctPeople,
            busiestHour, quietestHour, firstSeen, lastSeen);
        return new DoorDossierResult(
            doorId, doorName, readerNames,
            summary, hourlyTraffic, topUsers, recentDenials, recentAlarms);
    }

    // ── Person dossier ──────────────────────────────────────────────────

    /// <summary>
    /// Compose a single-person activity report from ~5 internal queries.
    /// Distinct door count, top doors, hourly pattern, and recent denials
    /// come from fact_transactions; recent alarms come from fact_alarms.
    /// Hourly pattern is zero-filled to 24 buckets.
    /// </summary>
    public PersonDossierResult GetPersonDossier(
        long personId, DateTime since, DateTime until,
        int topDoorsLimit, int recentLimit)
    {
        // Person name (may be null if never seen in dim_people)
        string? personName;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = "SELECT full_name FROM dim_people WHERE person_id = $pid";
            cmd.Parameters.Add(new DuckDBParameter("pid", (int)personId));
            var result = cmd.ExecuteScalar();
            personName = result == null || result is DBNull ? null : result.ToString();
        }

        // Summary: total events, denials, distinct doors (collapsed via dim_readers), first/last seen
        long totalEvents, totalDenials, distinctDoors;
        DateTime? firstSeen, lastSeen;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT
                  COUNT(*),
                  COUNT(*) FILTER (WHERE disposition > 1),
                  COUNT(DISTINCT r.door_id),
                  MIN(t.dt_date),
                  MAX(t.dt_date)
                FROM fact_transactions t
                LEFT JOIN dim_readers r ON r.name = t.reader_name
                WHERE t.{PersonToUid1FilterFragment} AND t.dt_date >= $since AND t.dt_date < $until
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", personId));
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            using var reader = cmd.ExecuteReader();
            reader.Read();
            totalEvents = reader.GetInt64(0);
            totalDenials = reader.GetInt64(1);
            distinctDoors = reader.GetInt64(2);
            firstSeen = reader.IsDBNull(3) ? null : reader.GetDateTime(3);
            lastSeen = reader.IsDBNull(4) ? null : reader.GetDateTime(4);
        }

        // Total alarms for this person
        long totalAlarms;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT COUNT(*) FROM fact_alarms
                WHERE uid1 = $pid AND dt_date >= $since AND dt_date < $until
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", (double)personId));
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            totalAlarms = (long)cmd.ExecuteScalar()!;
        }

        // Top doors by count (joined through dim_readers so multi-reader doors collapse)
        var topDoors = new List<DoorCountRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT d.door_id, d.name, COUNT(*) AS cnt
                FROM fact_transactions t
                JOIN dim_readers r ON r.name = t.reader_name
                JOIN dim_doors d ON d.door_id = r.door_id
                WHERE t.{PersonToUid1FilterFragment} AND t.dt_date >= $since AND t.dt_date < $until
                GROUP BY d.door_id, d.name
                ORDER BY cnt DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", personId));
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            cmd.Parameters.Add(new DuckDBParameter("limit", topDoorsLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                topDoors.Add(new DoorCountRow(
                    DoorId: reader.GetInt32(0),
                    DoorName: reader.IsDBNull(1) ? null : reader.GetString(1),
                    Count: reader.GetInt64(2)));
            }
        }

        // Hourly pattern — zero-filled 0..23
        var hourCounts = new long[24];
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT CAST(EXTRACT(HOUR FROM dt_date) AS INTEGER) AS h, COUNT(*)
                FROM fact_transactions
                WHERE {PersonToUid1FilterFragment} AND dt_date >= $since AND dt_date < $until
                GROUP BY h
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", personId));
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var hour = reader.GetInt32(0);
                if (hour >= 0 && hour < 24) hourCounts[hour] = reader.GetInt64(1);
            }
        }
        var hourlyPattern = Enumerable.Range(0, 24)
            .Select(h => new HourlyBucket(h, hourCounts[h])).ToList();

        // Recent denials
        var recentDenials = new List<SampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT log_id, dt_date, event_code, description, disposition,
                       reader_name, uid1, uid1_name
                FROM fact_transactions
                WHERE {PersonToUid1FilterFragment} AND dt_date >= $since AND dt_date < $until AND disposition > 1
                ORDER BY dt_date DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", personId));
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            cmd.Parameters.Add(new DuckDBParameter("limit", recentLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                recentDenials.Add(new SampleRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        // Recent alarms
        var recentAlarms = new List<AlarmSampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT alarm_id, dt_date, ak_date, cl_date, event_id,
                       alarm_level_priority, status, description,
                       uid1, uid1_name, workstation_name
                FROM fact_alarms
                WHERE uid1 = $pid AND dt_date >= $since AND dt_date < $until
                ORDER BY dt_date DESC
                LIMIT $limit
                """;
            cmd.Parameters.Add(new DuckDBParameter("pid", (double)personId));
            cmd.Parameters.Add(new DuckDBParameter("since", since));
            cmd.Parameters.Add(new DuckDBParameter("until", until));
            cmd.Parameters.Add(new DuckDBParameter("limit", recentLimit));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                recentAlarms.Add(new AlarmSampleRow(
                    AlarmId: reader.GetInt32(0),
                    DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    AkDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    ClDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    EventId: reader.GetInt32(4),
                    AlarmLevelPriority: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    Status: reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6)),
                    Description: reader.IsDBNull(7) ? null : reader.GetString(7),
                    PersonId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetDouble(8)),
                    PersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    WorkstationName: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        var summary = new DossierSummary(
            totalEvents, totalDenials, totalAlarms, distinctDoors, firstSeen, lastSeen);
        return new PersonDossierResult(
            personId, personName, summary, topDoors, hourlyPattern, recentDenials, recentAlarms);
    }

    /// <summary>
    /// Alarm counts bucketed by hour/day/week/month with zero-fill. Same shape as
    /// <see cref="GetTransactionTimeSeries"/> but against <c>fact_alarms</c> with
    /// alarm-side filters.
    /// </summary>
    public TimeSeriesResult GetAlarmTimeSeries(
        string bucket,
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName)
    {
        var truncUnit = bucket.ToLowerInvariant() switch
        {
            "hour" => "hour",
            "day" => "day",
            "week" => "week",
            "month" => "month",
            _ => throw new ArgumentException($"Unsupported bucket: {bucket}. Use hour, day, week, or month.")
        };

        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);

        var sql = $"""
            WITH buckets AS (
                SELECT DATE_TRUNC('{truncUnit}', ts) AS bucket
                FROM generate_series(
                    DATE_TRUNC('{truncUnit}', CAST($since AS TIMESTAMP)),
                    DATE_TRUNC('{truncUnit}', CAST($until AS TIMESTAMP)),
                    INTERVAL 1 {truncUnit}
                ) AS t(ts)
            ),
            counts AS (
                SELECT DATE_TRUNC('{truncUnit}', dt_date) AS bucket, COUNT(*) AS c
                FROM fact_alarms {where}
                GROUP BY 1
            )
            SELECT b.bucket, COALESCE(c.c, 0) AS c
            FROM buckets b
            LEFT JOIN counts c ON c.bucket = b.bucket
            WHERE b.bucket < CAST($until AS TIMESTAMP)
            ORDER BY b.bucket ASC
            """;

        var points = new List<TimeSeriesPoint>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            points.Add(new TimeSeriesPoint(reader.GetDateTime(0), reader.GetInt64(1)));
        }

        var total = points.Sum(p => p.Count);
        return new TimeSeriesResult(truncUnit, points, total);
    }

    /// <summary>
    /// Compute alarm acknowledge/clear lifecycle metrics grouped by a dimension.
    /// Supported group_by values: "operator", "priority", "event", "day", "hour".
    /// Returns per-group avg/p90 ack minutes, avg clear minutes, still_open count,
    /// plus grand totals (total_alarms, total_unacked) unaffected by the grouping filter.
    /// </summary>
    public AlarmResponseMetricsResult GetAlarmResponseMetrics(
        string groupBy,
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName,
        int limit)
    {
        var (keyExpr, keyIdExpr, groupKey, extraFilter) = groupBy.ToLowerInvariant() switch
        {
            "operator" => ("ak_operator", "NULL", "ak_operator", "AND ak_operator IS NOT NULL"),
            "priority" => ("CAST(alarm_level_priority AS VARCHAR)", "alarm_level_priority", "alarm_level_priority", ""),
            "event" or "event_id" => ("CAST(event_id AS VARCHAR)", "event_id", "event_id", ""),
            "day" => ("CAST(DATE_TRUNC('day', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('day', dt_date)", ""),
            "hour" => ("CAST(DATE_TRUNC('hour', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('hour', dt_date)", ""),
            _ => throw new ArgumentException($"Unsupported group_by: {groupBy}. Use operator, priority, event, day, or hour.")
        };

        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);

        long totalAlarms;
        long totalUnacked;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT COUNT(*), COUNT(*) FILTER (WHERE ak_date IS NULL)
                FROM fact_alarms {where}
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            using var reader = cmd.ExecuteReader();
            reader.Read();
            totalAlarms = reader.GetInt64(0);
            totalUnacked = reader.GetInt64(1);
        }

        var orderBy = groupBy.ToLowerInvariant() is "hour" or "day" ? "key ASC" : "total DESC";
        var groupWhere = string.IsNullOrEmpty(extraFilter) ? where : $"{where} {extraFilter}";

        var groups = new List<AlarmResponseMetricsGroup>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT
                  {keyExpr} AS key,
                  {keyIdExpr} AS key_id,
                  COUNT(*) AS total,
                  AVG(CASE WHEN ak_date IS NOT NULL
                           THEN EXTRACT(EPOCH FROM (ak_date - dt_date)) / 60.0 END) AS avg_ack_minutes,
                  AVG(CASE WHEN cl_date IS NOT NULL
                           THEN EXTRACT(EPOCH FROM (cl_date - dt_date)) / 60.0 END) AS avg_clear_minutes,
                  quantile_cont(
                    CASE WHEN ak_date IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (ak_date - dt_date)) / 60.0 END,
                    0.9) AS p90_ack_minutes,
                  COUNT(*) FILTER (WHERE ak_date IS NULL) AS still_open
                FROM fact_alarms {groupWhere}
                GROUP BY {groupKey}
                ORDER BY {orderBy}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                groups.Add(new AlarmResponseMetricsGroup(
                    Key: reader.IsDBNull(0) ? null : reader.GetValue(0).ToString(),
                    KeyId: reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1)),
                    Total: reader.GetInt64(2),
                    AvgAckMinutes: reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)),
                    AvgClearMinutes: reader.IsDBNull(4) ? null : Convert.ToDouble(reader.GetValue(4)),
                    P90AckMinutes: reader.IsDBNull(5) ? null : Convert.ToDouble(reader.GetValue(5)),
                    StillOpen: reader.GetInt64(6)));
            }
        }

        return new AlarmResponseMetricsResult(groupBy, groups, totalAlarms, totalUnacked);
    }

    internal static (string Where, List<DuckDBParameter> Parameters) BuildAlarmFilter(
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName)
    {
        var where = "WHERE dt_date >= $since AND dt_date < $until";
        var parameters = new List<DuckDBParameter>
        {
            new("since", since),
            new("until", until)
        };

        if (eventId.HasValue)
        {
            where += " AND event_id = $event_id";
            parameters.Add(new DuckDBParameter("event_id", eventId.Value));
        }
        if (alarmLevelPriority.HasValue)
        {
            where += " AND alarm_level_priority = $priority";
            parameters.Add(new DuckDBParameter("priority", alarmLevelPriority.Value));
        }
        if (status.HasValue)
        {
            where += " AND status = $status";
            parameters.Add(new DuckDBParameter("status", (short)status.Value));
        }
        if (uid1.HasValue)
        {
            // uid1 is DOUBLE in fact_alarms (decompiled.cs:9364) — cast to match.
            where += " AND uid1 = $uid1";
            parameters.Add(new DuckDBParameter("uid1", (double)uid1.Value));
        }
        if (!string.IsNullOrWhiteSpace(workstationName))
        {
            where += " AND workstation_name = $workstation";
            parameters.Add(new DuckDBParameter("workstation", workstationName));
        }

        return (where, parameters);
    }

    public void Dispose()
    {
        _conn.Dispose();
    }
}

public record EventTypeRow(int EventCode, string Category, string Name, string? Description);
public record DispositionRow(int Disposition, string Name);
public record DoorMatch(int DoorId, string Name, string? ControllerAddr, List<string> Readers);
public record ReaderMatch(int ReaderId, string Name);
public record PersonMatch(int PersonId, string? FirstName, string? LastName, string FullName);

public record OperatorMatch(
    int OperatorId,
    string Name,
    string? FullName,
    string? Description,
    bool Enabled);

public record SoftwareEventRow(
    int LogId,
    DateTime DtDate,
    int EventCode,
    string Description,
    int? OperatorId,
    string? OperatorName,
    string? NetAddress);

public record ExpiringCredentialRow(
    int CredentialId,
    int PersonId,
    string? PersonName,
    DateTime? ActivationDate,
    DateTime? ExpirationDate,
    bool IsActivated,
    bool ExpirationUsed,
    int? DaysUntilExpiry);

public record DimensionCounts(
    long People,
    long Doors,
    long Readers,
    long Clearances,
    long Operators,
    long Transactions,
    long Alarms,
    long SoftwareEvents);

public record AggregationBucket(string? Key, long? KeyId, long Count);
public record AggregationResult(
    string GroupBy,
    List<AggregationBucket> Groups,
    long TotalEvents,
    long TotalGroups,
    bool Truncated);

public record SampleRow(
    int LogId,
    DateTime DtDate,
    int EventCode,
    string? Description,
    int Disposition,
    string? ReaderName,
    int PersonId,
    string? PersonName);
public record SampleResult(
    List<SampleRow> Events,
    long TotalMatching,
    bool Truncated);

public record AlarmCategoryRow(int CategoryId, string Name, string? Description);

public record InactiveEntity(long Id, string Name, DateTime? LastSeenAt);
public record InactiveEntitiesResult(
    string Entity,
    List<InactiveEntity> Items,
    long TotalEntities,
    long InactiveTotal);

public record TimeSeriesPoint(DateTime BucketStart, long Count);
public record TimeSeriesResult(string Bucket, List<TimeSeriesPoint> Points, long TotalEvents);

public record TransactionDetail(
    int LogId,
    DateTime DtDate,
    DateTime? PcDateTime,
    int EventCode,
    string? Description,
    int Disposition,
    int? TransactionType,
    bool? ReportAsAlarm,
    int? AlarmLevelPriority,
    string? PortAddr,
    string? DtAddr,
    string? XAddr,
    string? NetAddress,
    string? ReaderName,
    int? FromZone,
    int? ToZone,
    int? PersonId,
    string? PersonName,
    int? Uid2,
    string? Uid2Name);

public record AlarmDetail(
    int AlarmId,
    DateTime? DtDate,
    DateTime? DbDate,
    DateTime? AkDate,
    DateTime? ClDate,
    int EventId,
    int? AlarmLevelPriority,
    int? Status,
    string? Description,
    string? NetAddress,
    string? AkOperator,
    string? ClOperator,
    string? WorkstationName,
    long? PersonId,
    string? PersonName,
    string? Parm1,
    string? Parm2);

public record AlarmSampleRow(
    int AlarmId,
    DateTime? DtDate,
    DateTime? AkDate,
    DateTime? ClDate,
    int EventId,
    int? AlarmLevelPriority,
    int? Status,
    string? Description,
    long? PersonId,
    string? PersonName,
    string? WorkstationName);
public record AlarmSampleResult(
    List<AlarmSampleRow> Alarms,
    long TotalMatching,
    bool Truncated);

public record AlarmResponseMetricsGroup(
    string? Key,
    long? KeyId,
    long Total,
    double? AvgAckMinutes,
    double? AvgClearMinutes,
    double? P90AckMinutes,
    long StillOpen);
public record AlarmResponseMetricsResult(
    string GroupBy,
    List<AlarmResponseMetricsGroup> Groups,
    long TotalAlarms,
    long TotalUnacked);

public record DossierSummary(
    long TotalEvents,
    long TotalDenials,
    long TotalAlarms,
    long DistinctDoors,
    DateTime? FirstSeen,
    DateTime? LastSeen);
public record DoorCountRow(long? DoorId, string? DoorName, long Count);
public record HourlyBucket(int Hour, long Count);
public record PersonDossierResult(
    long PersonId,
    string? PersonName,
    DossierSummary Summary,
    List<DoorCountRow> TopDoors,
    List<HourlyBucket> HourlyPattern,
    List<SampleRow> RecentDenials,
    List<AlarmSampleRow> RecentAlarms);

public record PersonCountRow(long PersonId, string? PersonName, long Count);
public record DoorDossierSummary(
    long TotalAccess,
    long TotalDenied,
    long TotalAlarms,
    long DistinctPeople,
    int? BusiestHour,
    int? QuietestHour,
    DateTime? FirstSeen,
    DateTime? LastSeen);
public record DoorDossierResult(
    int DoorId,
    string? DoorName,
    List<string> ReaderNames,
    DoorDossierSummary Summary,
    List<HourlyBucket> HourlyTraffic,
    List<PersonCountRow> TopUsers,
    List<SampleRow> RecentDenials,
    List<AlarmSampleRow> RecentAlarms);

public record SurroundingEventsResult(
    DateTime Timestamp,
    int WindowMinutes,
    int? DoorId,
    DateTime WindowSince,
    DateTime WindowUntil,
    List<SampleRow> Events,
    List<AlarmSampleRow> Alarms);

public record DailySecurityBriefingMetrics(
    long TotalAccess,
    long TotalDenied,
    long TotalAlarms,
    long AlarmsUnacked,
    long ForcedOpens,
    long HeldOpens);
public record NotableDenier(long PersonId, string? PersonName, long DenialCount);
public record DoorStatusRow(
    int DoorId,
    string? Name,
    string? ControllerAddr,
    bool AdminActive,
    int ReaderCount,
    List<string> ReaderNames,
    DateTime? LastSeenAt,
    long EventsInWindow,
    long OpenAlarms,
    string Status);

public record GrantingClearance(
    int ClearanceId,
    string Name,
    string? ScheduleName,
    DateTime GrantedAt,
    DateTime? ExpiresAt);
public record AuthorizationResult(
    long PersonId,
    int DoorId,
    bool Authorized,
    List<GrantingClearance> GrantingClearances);
public record AuthorizedDoor(int DoorId, string? DoorName, string ViaClearances);
public record AuthorizedPerson(long PersonId, string? PersonName, string ViaClearances);

public record ForcedThroughPair(
    int DeniedLogId,
    DateTime DeniedTime,
    int? DeniedDisposition,
    string? DeniedDescription,
    long DeniedPersonId,
    string? DeniedPersonName,
    int GrantedLogId,
    DateTime GrantedTime,
    long GrantedPersonId,
    string? GrantedPersonName,
    string? GrantedDescription,
    string? ReaderName,
    double GapSeconds);
public record ForcedThroughResult(List<ForcedThroughPair> Pairs, long TotalPairs);

public record DailyAttendanceRow(
    long PersonId,
    string? PersonName,
    DateTime Day,
    DateTime FirstSeen,
    DateTime LastSeen,
    long EventCount);
public record DailyAttendanceResult(
    DateTime Since,
    DateTime Until,
    List<DailyAttendanceRow> Rows,
    long TotalRows);

public record DailySecurityBriefingResult(
    DateTime DayStart,
    DateTime DayEnd,
    DateTime PriorStart,
    DateTime PriorEnd,
    DailySecurityBriefingMetrics Headline,
    DailySecurityBriefingMetrics Prior,
    DailySecurityBriefingMetrics VsPriorDay,
    List<SampleRow> ForcedOpenEvents,
    List<NotableDenier> NotableDeniers,
    List<DoorCountRow> BusiestDoors,
    List<AlarmSampleRow> OpenAlarms);
