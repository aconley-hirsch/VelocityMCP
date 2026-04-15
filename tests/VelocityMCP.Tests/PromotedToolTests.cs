using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Tool-level smoke tests for tools whose underlying mirror methods already
/// have deep coverage in EndToEndTests but whose JSON serialization is
/// uncovered. One test per tool, covering the LLM-facing field names and
/// at least one behavior beyond "doesn't throw". This locks in the contract
/// the LLM relies on for the high-traffic workflows.
///
/// The deeper SQL behavior of each tool stays in the existing mirror-level
/// tests in EndToEndTests — these are an additional layer, not a replacement.
/// </summary>
public class PromotedToolTests : IDisposable
{
    private const int AlicePersonId = 100;
    private const int BobPersonId = 200;
    private const int AliceCredA = 5001;
    private const int AliceCredB = 5002;
    private const int BobCred = 5003;

    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public PromotedToolTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        SeedFixture();
    }

    private void SeedFixture()
    {
        _mirror.UpsertPeople(new List<PersonRecord>
        {
            new() { PersonId = AlicePersonId, FirstName = "Alice", LastName = "Anderson" },
            new() { PersonId = BobPersonId,   FirstName = "Bob",   LastName = "Brown" },
        });
        _mirror.UpsertUserCredentials(new List<UserCredentialRecord>
        {
            new() { CredentialId = AliceCredA, PersonId = AlicePersonId },
            new() { CredentialId = AliceCredB, PersonId = AlicePersonId },
            new() { CredentialId = BobCred,    PersonId = BobPersonId },
        });
        _mirror.UpsertDoors(new List<DoorRecord>
        {
            new() { Id = 10, Name = "Front Lobby" },
            new() { Id = 20, Name = "Server Room" },
        });
        _mirror.UpsertReaders(new List<ReaderRecord>
        {
            new() { Id = 50, Name = "Front Reader",  DoorId = 10 },
            new() { Id = 60, Name = "Server Reader", DoorId = 20 },
        });

        var now = DateTime.UtcNow;
        var transactions = new List<TransactionRecord>();
        int logId = 1;

        // Alice: 4 events (2 from each credential), one denied
        foreach (var (uid1, count) in new[] { (AliceCredA, 2), (AliceCredB, 2) })
            for (int i = 0; i < count; i++)
                transactions.Add(new TransactionRecord
                {
                    LogId = logId++,
                    DtDate = now.AddMinutes(-10 - logId),
                    EventCode = i == 0 ? 1 : 2,
                    Description = i == 0 ? "Access Granted" : "Access Denied",
                    Disposition = i == 0 ? 1 : 2,
                    ReaderName = "Front Reader",
                    Uid1 = uid1,
                    Uid1Name = "Alice Anderson",
                });

        // Bob: 1 event
        transactions.Add(new TransactionRecord
        {
            LogId = logId++,
            DtDate = now.AddMinutes(-30),
            EventCode = 1,
            Description = "Access Granted",
            Disposition = 1,
            ReaderName = "Server Reader",
            Uid1 = BobCred,
            Uid1Name = "Bob Brown",
        });
        _mirror.IngestTransactions(transactions);
    }

    // ── count_events ────────────────────────────────────────────────────

    [Fact]
    public void CountEventsTool_TotalCountReflectsSeed()
    {
        var json = CountEventsTool.CountEvents(_mirror, relative_window: "last_24h");
        using var doc = JsonDocument.Parse(json);

        // 5 total transactions in the window (4 Alice + 1 Bob)
        Assert.Equal(5, doc.RootElement.GetProperty("count").GetInt64());
    }

    [Fact]
    public void CountEventsTool_PersonIdFilterUsesCredentialsJoin()
    {
        // person_id filter expands through dim_user_credentials. Filtering
        // by Alice's HostUserId should return ALL 4 of her events (across
        // her two credentials), not just events from one badge.
        var json = CountEventsTool.CountEvents(
            _mirror, relative_window: "last_24h", person_id: AlicePersonId);
        using var doc = JsonDocument.Parse(json);

        Assert.Equal(4, doc.RootElement.GetProperty("count").GetInt64());
    }

    [Fact]
    public void CountEventsTool_ResponseHasWindowUsedBlock()
    {
        var json = CountEventsTool.CountEvents(_mirror, relative_window: "last_7d");
        using var doc = JsonDocument.Parse(json);

        var window = doc.RootElement.GetProperty("window_used");
        Assert.Equal("last_7d", window.GetProperty("relative_window").GetString());
        Assert.False(window.GetProperty("defaulted_since").GetBoolean());
        Assert.False(window.GetProperty("defaulted_until").GetBoolean());
    }

    // ── person_dossier ──────────────────────────────────────────────────

    [Fact]
    public void PersonDossierTool_RollsUpAcrossAllCredentials()
    {
        var json = PersonDossierTool.PersonDossier(_mirror, person_id: AlicePersonId);
        using var doc = JsonDocument.Parse(json);

        Assert.Equal(AlicePersonId, doc.RootElement.GetProperty("person_id").GetInt64());
        Assert.Equal("Alice Anderson", doc.RootElement.GetProperty("person_name").GetString());

        var summary = doc.RootElement.GetProperty("summary");
        // Alice has 4 events spanning both badges. The dossier sums across.
        Assert.Equal(4, summary.GetProperty("total_events").GetInt64());
        // 2 of the 4 events are denials (disposition > 1)
        Assert.Equal(2, summary.GetProperty("total_denials").GetInt64());
    }

    [Fact]
    public void PersonDossierTool_HourlyPatternIs24ZeroFilledBuckets()
    {
        var json = PersonDossierTool.PersonDossier(_mirror, person_id: AlicePersonId);
        using var doc = JsonDocument.Parse(json);

        var hours = doc.RootElement.GetProperty("hourly_pattern").EnumerateArray().ToList();
        Assert.Equal(24, hours.Count);
        // Hours are 0..23 in order
        for (int i = 0; i < 24; i++)
            Assert.Equal(i, hours[i].GetProperty("hour").GetInt32());
    }

    // ── check_authorization ─────────────────────────────────────────────

    [Fact]
    public void CheckAuthorizationTool_PointQueryReportsExpectedShape()
    {
        // No clearances seeded — Alice should not be authorized to door 10,
        // but the response shape MUST match the documented contract.
        var json = CheckAuthorizationTool.CheckAuthorization(
            _mirror, person_id: AlicePersonId, door_id: 10);
        using var doc = JsonDocument.Parse(json);

        Assert.Equal("point", doc.RootElement.GetProperty("mode").GetString());
        Assert.Equal(AlicePersonId, doc.RootElement.GetProperty("person_id").GetInt64());
        Assert.Equal(10, doc.RootElement.GetProperty("door_id").GetInt32());
        Assert.True(doc.RootElement.TryGetProperty("authorized", out _));
        Assert.True(doc.RootElement.TryGetProperty("via_clearances", out _));
        Assert.True(doc.RootElement.TryGetProperty("reason", out _));
    }

    // ── inactive_entities ───────────────────────────────────────────────

    [Fact]
    public void InactiveEntitiesTool_PersonBranchUsesCredentialsJoin()
    {
        // Bob has 1 event (in window). Alice has 4. No one is inactive
        // in last_30d → returned list is empty, but inactive_total counts
        // any person with zero events. Both Alice and Bob have events,
        // so inactive_total should be 0 (not 2 — that would be the bug
        // where the credentials join broke).
        var json = InactiveEntitiesTool.InactiveEntities(_mirror, entity: "person");
        using var doc = JsonDocument.Parse(json);

        Assert.Equal("person", doc.RootElement.GetProperty("entity").GetString());
        Assert.Equal(0, doc.RootElement.GetProperty("inactive_total").GetInt64());
        Assert.Equal(2, doc.RootElement.GetProperty("total_entities").GetInt64());
    }

    [Fact]
    public void InactiveEntitiesTool_DoorBranchReportsExpectedShape()
    {
        // Server Room has 1 event (Bob), Front Lobby has 4 (Alice). Both active
        // in 30 day window → 0 inactive doors.
        var json = InactiveEntitiesTool.InactiveEntities(_mirror, entity: "door");
        using var doc = JsonDocument.Parse(json);

        Assert.Equal("door", doc.RootElement.GetProperty("entity").GetString());
        Assert.Equal(2, doc.RootElement.GetProperty("total_entities").GetInt64());
    }

    // ── get_daily_attendance ────────────────────────────────────────────

    [Fact]
    public void GetDailyAttendanceTool_RollsUpAlicesBadgesIntoOneRow()
    {
        var json = GetDailyAttendanceTool.GetDailyAttendance(_mirror);
        using var doc = JsonDocument.Parse(json);

        var rows = doc.RootElement.GetProperty("rows").EnumerateArray()
            .Where(r => r.GetProperty("person_id").GetInt64() > 0)  // skip orphans
            .ToList();

        // Two distinct people (Alice + Bob), each with one day's attendance.
        // Alice's two badges should NOT produce two rows — they collapse via
        // the credentials join we wrote to fix the daily-attendance rollup.
        var alice = rows.SingleOrDefault(r => r.GetProperty("person_id").GetInt64() == AlicePersonId);
        Assert.True(alice.ValueKind != JsonValueKind.Undefined,
            "Alice should appear exactly once in daily attendance — credentials must collapse");
        // 2 of Alice's 4 events are granted (disposition = 1) — daily
        // attendance only counts granted events.
        Assert.Equal(2, alice.GetProperty("event_count").GetInt64());
    }

    [Fact]
    public void GetDailyAttendanceTool_ResponseHasDocumentedFieldNames()
    {
        var json = GetDailyAttendanceTool.GetDailyAttendance(_mirror);
        using var doc = JsonDocument.Parse(json);
        var row = doc.RootElement.GetProperty("rows").EnumerateArray().First();

        foreach (var field in new[] { "person_id", "person_name", "day", "first_seen",
                                      "last_seen", "duration_minutes", "event_count" })
        {
            Assert.True(row.TryGetProperty(field, out _), $"missing field: {field}");
        }
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
