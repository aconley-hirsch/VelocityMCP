using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Locks in current behavior of aggregate_alarms across every group_by mode.
/// IMPORTANT — semantic question pinned by these tests:
///
/// fact_alarms.uid1 may be a CredentialId (like fact_transactions.uid1) or a
/// HostUserId. The current code treats it as a HostUserId for the person path
/// — group_by="person" buckets directly on uid1 with no credentials join.
/// We can't verify which is correct from the live HIRSCH site because every
/// alarm there has UID1=0 (Door Forced/Held Open events don't carry a person).
///
/// These tests therefore fix the *current* behavior so any future correction
/// (introducing a credentials join the way we did for transactions) breaks
/// them loudly and forces a deliberate decision.
/// </summary>
public class AggregateAlarmsToolTests : IDisposable
{
    private const int OperatorAlarmUid = 5001;       // suspected: credential id
    private const int SecurityAlarmUid = 5002;       // suspected: credential id
    private const int OrphanAlarmUid = 9999;         // never enrolled

    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public AggregateAlarmsToolTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        SeedFixture();
    }

    private void SeedFixture()
    {
        var now = DateTime.UtcNow;
        // 6 alarms, deliberately spanning every dimension we want to aggregate.
        // Two for the same uid1 to exercise person grouping, two priorities,
        // two event ids, two workstations, one with no person.
        _mirror.IngestAlarms(new List<AlarmRecord>
        {
            new()
            {
                AlarmId = 1, DtDate = now.AddMinutes(-10),
                EventId = 4, AlarmLevelPriority = 1, Status = 0,
                Description = "Door Forced Open",
                Uid1 = OperatorAlarmUid, Uid1Name = "Alice Anderson",
                WorkstationName = "VELOCITY-WS1",
            },
            new()
            {
                AlarmId = 2, DtDate = now.AddMinutes(-20),
                EventId = 5, AlarmLevelPriority = 1, Status = 1,
                Description = "Door Held Open",
                Uid1 = OperatorAlarmUid, Uid1Name = "Alice Anderson",
                WorkstationName = "VELOCITY-WS1",
            },
            new()
            {
                AlarmId = 3, DtDate = now.AddMinutes(-30),
                EventId = 4, AlarmLevelPriority = 2, Status = 2,
                Description = "Door Forced Open",
                Uid1 = SecurityAlarmUid, Uid1Name = "Bob Brown",
                WorkstationName = "VELOCITY-WS2",
            },
            new()
            {
                AlarmId = 4, DtDate = now.AddMinutes(-40),
                EventId = 4, AlarmLevelPriority = 1, Status = 0,
                Description = "Door Forced Open",
                Uid1 = OrphanAlarmUid, Uid1Name = "Ghost Walker",
                WorkstationName = "VELOCITY-WS2",
            },
            new()
            {
                AlarmId = 5, DtDate = now.AddMinutes(-50),
                EventId = 5, AlarmLevelPriority = 3, Status = 1,
                Description = "Door Held Open",
                Uid1 = null, Uid1Name = null,        // unattributed alarm
                WorkstationName = "VELOCITY-WS1",
            },
            // One outside the default 24-hour window — should not appear in default queries
            new()
            {
                AlarmId = 6, DtDate = now.AddDays(-30),
                EventId = 4, AlarmLevelPriority = 1, Status = 2,
                Description = "Door Forced Open (ancient)",
                Uid1 = OperatorAlarmUid, Uid1Name = "Alice Anderson",
                WorkstationName = "VELOCITY-WS1",
            },
        });
    }

    [Fact]
    public void GroupByPerson_BucketsAlarmsByRawUid1_CurrentBehavior()
    {
        // CURRENT BEHAVIOR: alarms are grouped by raw uid1 with no credentials
        // join. Alice (uid1=5001) gets one bucket of 2 alarms, Bob (5002) gets
        // one of 1, the orphan (9999) gets one of 1, the unattributed alarm
        // (uid1=null) is excluded from the COUNT(DISTINCT uid1) groups count
        // but still shows in total_events.
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "person");
        var groups = ParseGroups(json);

        Assert.Equal(2, groups.Single(g => g.KeyId == OperatorAlarmUid).Count);
        Assert.Equal(1, groups.Single(g => g.KeyId == SecurityAlarmUid).Count);
        Assert.Equal(1, groups.Single(g => g.KeyId == OrphanAlarmUid).Count);

        // Note for future readers: if someone later fixes alarms to roll up
        // through dim_user_credentials → dim_people the way we did for
        // transactions, this test will fail because Alice's two alarms might
        // be combined with other credentials owned by the same person, and
        // key_id would change from CredentialId to HostUserId. That's the
        // intended outcome — see the class comment.
    }

    [Fact]
    public void GroupByPerson_KeyIdIsRawUid1NotResolved()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "person");
        var groups = ParseGroups(json);

        // key_id values are exactly the uid1 doubles cast back to long.
        // No credentials join; no rollup.
        var ids = groups.Select(g => g.KeyId).Where(id => id.HasValue).Select(id => id!.Value).ToHashSet();
        Assert.Contains((long)OperatorAlarmUid, ids);
        Assert.Contains((long)SecurityAlarmUid, ids);
        Assert.Contains((long)OrphanAlarmUid, ids);
    }

    [Fact]
    public void GroupByPriority_CountsAlarmsPerPriorityLevel()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "priority");
        var groups = ParseGroups(json);

        // Priority 1: alarms 1, 2, 4 (3 in window)
        // Priority 2: alarm 3 (1)
        // Priority 3: alarm 5 (1)
        Assert.Equal(3, groups.Single(g => g.Key == "1").Count);
        Assert.Equal(1, groups.Single(g => g.Key == "2").Count);
        Assert.Equal(1, groups.Single(g => g.Key == "3").Count);
    }

    [Fact]
    public void GroupByEvent_CountsAlarmsPerEventId()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "event");
        var groups = ParseGroups(json);

        // event_id=4: alarms 1, 3, 4 (3 in window). event_id=5: alarms 2, 5 (2).
        Assert.Equal(3, groups.Single(g => g.Key == "4").Count);
        Assert.Equal(2, groups.Single(g => g.Key == "5").Count);
    }

    [Fact]
    public void GroupByStatus_CountsAlarmsPerStatusCode()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "status");
        var groups = ParseGroups(json);

        // status 0: alarms 1, 4 → 2.   status 1: 2, 5 → 2.   status 2: 3 → 1.
        Assert.Equal(2, groups.Single(g => g.Key == "0").Count);
        Assert.Equal(2, groups.Single(g => g.Key == "1").Count);
        Assert.Equal(1, groups.Single(g => g.Key == "2").Count);
    }

    [Fact]
    public void GroupByWorkstation_CountsAlarmsPerWorkstation()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "workstation");
        var groups = ParseGroups(json);

        // VELOCITY-WS1: alarms 1, 2, 5 → 3.   VELOCITY-WS2: 3, 4 → 2.
        Assert.Equal(3, groups.Single(g => g.Key == "VELOCITY-WS1").Count);
        Assert.Equal(2, groups.Single(g => g.Key == "VELOCITY-WS2").Count);
    }

    [Fact]
    public void Default_ExcludesAlarmsOlderThanTwentyFourHours()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "event");

        using var doc = JsonDocument.Parse(json);
        // 5 in-window alarms total (alarm 6 is 30 days old, excluded).
        Assert.Equal(5, doc.RootElement.GetProperty("total_events").GetInt64());
    }

    [Fact]
    public void PersonIdFilter_ScopesByRawUid1()
    {
        // person_id filter on alarms is a direct uid1 match — there's no
        // credentials subquery on the alarm side. This test pins that.
        var json = AggregateAlarmsTool.AggregateAlarms(
            _mirror, group_by: "event", person_id: OperatorAlarmUid);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("total_events").GetInt64());
    }

    [Fact]
    public void EventIdFilter_ScopesByEventId()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(
            _mirror, group_by: "priority", event_id: 4);

        using var doc = JsonDocument.Parse(json);
        // event_id=4 in window: alarms 1, 3, 4 → 3 events
        Assert.Equal(3, doc.RootElement.GetProperty("total_events").GetInt64());
    }

    [Fact]
    public void WorkstationFilter_ScopesByExactName()
    {
        var json = AggregateAlarmsTool.AggregateAlarms(
            _mirror, group_by: "event", workstation_name: "VELOCITY-WS2");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("total_events").GetInt64());
    }

    [Fact]
    public void UnsupportedGroupBy_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            AggregateAlarmsTool.AggregateAlarms(_mirror, group_by: "not_a_real_dimension"));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private record GroupRow(string? Key, long? KeyId, long Count);

    private static List<GroupRow> ParseGroups(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("groups")
            .EnumerateArray()
            .Select(g => new GroupRow(
                Key: g.GetProperty("key").ValueKind == JsonValueKind.Null ? null : g.GetProperty("key").GetString(),
                KeyId: g.GetProperty("key_id").ValueKind == JsonValueKind.Null ? null : g.GetProperty("key_id").GetInt64(),
                Count: g.GetProperty("count").GetInt64()))
            .ToList();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
