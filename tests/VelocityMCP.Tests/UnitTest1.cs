using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

public class EndToEndTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;

    public EndToEndTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
    }

    [Fact]
    public void Schema_creates_tables_and_passes_pii_assertion()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();
        schema.AssertNoPiiColumns(); // Should not throw
    }

    [Fact]
    public async Task FakeClient_generates_transactions()
    {
        using var client = new FakeVelocityClient();
        await client.ConnectAsync();

        var records = await client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1));
        Assert.NotEmpty(records);
        Assert.All(records, r =>
        {
            Assert.True(r.LogId > 0);
            Assert.NotNull(r.ReaderName);
        });
    }

    [Fact]
    public void Ingest_and_count_round_trips()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        var count = mirror.CountTransactions(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1));
        Assert.Equal(records.Count, count);
    }

    [Fact]
    public void Count_with_filters_works()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        // Count only "Access Granted" (event_code=1)
        var grantedCount = mirror.CountTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            eventCode: 1);
        var expectedGranted = records.Count(r => r.EventCode == 1);
        Assert.Equal(expectedGranted, grantedCount);
    }

    [Fact]
    public void Timeseries_returns_contiguous_zero_filled_buckets()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-6)).Result;
        mirror.IngestTransactions(records);

        var result = mirror.GetTransactionTimeSeries(
            bucket: "hour",
            since: DateTime.UtcNow.AddHours(-6),
            until: DateTime.UtcNow,
            eventCode: null, readerName: null, readerNames: null,
            uid1: null, disposition: null);

        // 6 hours → at least 6 contiguous buckets (may be 6 or 7 depending on clock boundary)
        Assert.InRange(result.Points.Count, 6, 7);
        Assert.Equal(records.Count, result.TotalEvents);
        // Buckets must be ordered ascending
        for (int i = 1; i < result.Points.Count; i++)
            Assert.True(result.Points[i].BucketStart > result.Points[i - 1].BucketStart);
    }

    [Fact]
    public void Get_transaction_by_log_id_returns_row()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        var target = records[0];
        var detail = mirror.GetTransaction(target.LogId);
        Assert.NotNull(detail);
        Assert.Equal(target.LogId, detail!.LogId);
        Assert.Equal(target.EventCode, detail.EventCode);
        Assert.Equal(target.ReaderName, detail.ReaderName);

        Assert.Null(mirror.GetTransaction(-99999));
    }

    [Fact]
    public void Alarm_ingest_and_query_round_trips()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var alarms = client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestAlarms(alarms);

        var count = mirror.CountAlarms(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            eventId: null, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null);
        Assert.Equal(alarms.Count, count);

        // Aggregate by priority
        var agg = mirror.AggregateAlarms(
            groupBy: "priority",
            since: DateTime.UtcNow.AddDays(-1), until: DateTime.UtcNow.AddDays(1),
            eventId: null, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null, limit: 10);
        Assert.Equal(alarms.Count, agg.TotalEvents);
        Assert.NotEmpty(agg.Groups);

        // Sample alarms
        var sample = mirror.SampleAlarms(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            eventId: null, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null,
            order: "time_desc", limit: 5);
        Assert.NotEmpty(sample.Alarms);
        Assert.Equal(alarms.Count, sample.TotalMatching);

        // Single-row lookup
        var first = sample.Alarms[0];
        var detail = mirror.GetAlarm(first.AlarmId);
        Assert.NotNull(detail);
        Assert.Equal(first.AlarmId, detail!.AlarmId);
    }

    [Fact]
    public void Door_id_filter_resolves_to_reader_names()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        // Seed dim_readers so door_id can be resolved
        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        // Front Door (door_id=1) has two readers: "Front Door Reader 1" + "Front Door Reader 2"
        var doorCount = mirror.CountTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            doorId: 1);

        var reader1Count = mirror.CountTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            readerName: "Front Door Reader 1");
        var reader2Count = mirror.CountTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            readerName: "Front Door Reader 2");

        // door_id=1 must equal the sum of its individual readers (no overlap, no leakage)
        Assert.Equal(reader1Count + reader2Count, doorCount);
    }

    [Fact]
    public void Door_id_with_no_readers_returns_empty()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result);

        // door_id=999 doesn't exist → must return 0, not match everything
        var count = mirror.CountTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            doorId: 999);
        Assert.Equal(0, count);

        var sample = mirror.SampleTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            null, null, null, null, null, "time_desc", 10, doorId: 999);
        Assert.Empty(sample.Events);

        var agg = mirror.AggregateTransactions(
            "person", DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            null, null, null, null, null, 10, doorId: 999);
        Assert.Empty(agg.Groups);
        Assert.Equal(0, agg.TotalEvents);
    }

    [Fact]
    public void Aggregate_by_door_collapses_multi_reader_doors()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result);

        // group_by="reader" — should produce a row per physical reader
        var byReader = mirror.AggregateTransactions(
            "reader", DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            null, null, null, null, null, 50);

        // group_by="door" — should collapse Front Door's two readers into one row
        var byDoor = mirror.AggregateTransactions(
            "door", DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            null, null, null, null, null, 50);

        // Same total events, fewer (or equal) groups
        Assert.Equal(byReader.TotalEvents, byDoor.TotalEvents);
        Assert.True(byDoor.TotalGroups <= byReader.TotalGroups);

        // Front Door + Lobby + Parking Garage each have 2 readers but should be 1 door bucket each.
        // Of the 7 fake doors, 4 are single-reader (1 reader = 1 bucket either way),
        // 3 are multi-reader. So byDoor.TotalGroups should be at most 7.
        Assert.True(byDoor.TotalGroups <= 7);

        // "Front Door" should appear as a single bucket if any traffic hit it
        var frontDoorBucket = byDoor.Groups.FirstOrDefault(g => g.Key == "Front Door");
        if (frontDoorBucket != null)
        {
            // Its count must equal the sum of Reader 1 + Reader 2 buckets in the reader grouping
            var r1 = byReader.Groups.FirstOrDefault(g => g.Key == "Front Door Reader 1")?.Count ?? 0;
            var r2 = byReader.Groups.FirstOrDefault(g => g.Key == "Front Door Reader 2")?.Count ?? 0;
            Assert.Equal(r1 + r2, frontDoorBucket.Count);
        }
    }

    [Fact]
    public void Person_id_round_trips_across_transaction_and_alarm_tools()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result);
        mirror.IngestAlarms(client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-1)).Result);

        // Pull a person_id from an alarm sample (where it originates as a double in fact_alarms)
        var alarmSample = mirror.SampleAlarms(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            null, null, null, null, null, "time_desc", 5);
        Assert.NotEmpty(alarmSample.Alarms);

        var personIdFromAlarm = alarmSample.Alarms.First(a => a.PersonId.HasValue).PersonId!.Value;

        // Feed that same long value into a transaction count — must work without casts/conversions
        var txCount = mirror.CountTransactions(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            uid1: personIdFromAlarm);

        // Sanity: same value works in alarm count too
        var alarmCount = mirror.CountAlarms(
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
            null, null, null, personIdFromAlarm, null);

        // Both calls accepted the long value without error; counts are non-negative
        Assert.True(txCount >= 0);
        Assert.True(alarmCount >= 0);
    }

    [Fact]
    public void Inactive_entities_math_checks_out()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        // Populate dimensions + ingest one hour of activity
        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result);

        // For each entity type: active + inactive must equal total
        foreach (var entity in new[] { "person", "door", "reader" })
        {
            var result = mirror.GetInactiveEntities(
                entity,
                since: DateTime.UtcNow.AddDays(-1),
                until: DateTime.UtcNow.AddDays(1),
                limit: 100);

            Assert.True(result.TotalEntities > 0, $"{entity}: expected non-zero total");
            Assert.True(result.InactiveTotal >= 0);
            Assert.True(result.InactiveTotal <= result.TotalEntities,
                $"{entity}: inactive ({result.InactiveTotal}) exceeded total ({result.TotalEntities})");

            // Every returned item must lack any in-window activity.
            // Verify by cross-checking against count_events for that person/reader.
            if (entity == "person")
            {
                foreach (var item in result.Items)
                {
                    var count = mirror.CountTransactions(
                        DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1),
                        uid1: item.Id);
                    Assert.Equal(0, count);
                }
            }
        }
    }

    [Fact]
    public void Inactive_entities_surfaces_last_seen_at_for_outside_window()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        // Ingest events from 2-3 hours ago — these are OUTSIDE a 1-hour-ago window
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-3)).Result);

        // Query with a 1-hour window: any person whose only events were 2-3 hours ago
        // should appear as inactive WITH last_seen_at populated (not null)
        var result = mirror.GetInactiveEntities(
            "person",
            since: DateTime.UtcNow.AddMinutes(-30),
            until: DateTime.UtcNow,
            limit: 100);

        // At least some items should have last_seen_at populated (proving the
        // ingested-but-outside-window case works) — exact count depends on random
        // distribution but should generally be > 0
        var withLastSeen = result.Items.Where(i => i.LastSeenAt.HasValue).ToList();
        Assert.NotEmpty(withLastSeen);

        // All items with last_seen_at must have it BEFORE the window started
        var windowStart = DateTime.UtcNow.AddMinutes(-30);
        Assert.All(withLastSeen, i =>
            Assert.True(i.LastSeenAt!.Value < windowStart,
                $"last_seen_at {i.LastSeenAt} should predate window start {windowStart}"));
    }

    [Fact]
    public void Inactive_entities_door_collapses_multi_reader_doors()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        // No transactions → every door + every reader should be inactive

        var byDoor = mirror.GetInactiveEntities("door",
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, limit: 100);
        var byReader = mirror.GetInactiveEntities("reader",
            DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, limit: 100);

        // Fake client has 7 doors and 10 readers; with no activity all are inactive
        Assert.Equal(7, byDoor.InactiveTotal);
        Assert.Equal(10, byReader.InactiveTotal);
        // Multi-reader doors collapse: door count < reader count
        Assert.True(byDoor.InactiveTotal < byReader.InactiveTotal);
    }

    [Fact]
    public void Alarm_response_metrics_computes_ack_and_clear_durations()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var alarms = client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestAlarms(alarms);

        var result = mirror.GetAlarmResponseMetrics(
            groupBy: "priority",
            since: DateTime.UtcNow.AddDays(-1), until: DateTime.UtcNow.AddDays(1),
            eventId: null, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null, limit: 10);

        Assert.Equal(alarms.Count, result.TotalAlarms);
        Assert.Equal(alarms.Count(a => a.AkDate == null), result.TotalUnacked);
        Assert.NotEmpty(result.Groups);

        // Per-group total sums back to the grand total (priority has no extra filter).
        Assert.Equal(result.TotalAlarms, result.Groups.Sum(g => g.Total));

        // At least one group should have a real avg_ack_minutes since the fake client
        // acks roughly half of alarms and avg is over 1-60 minutes.
        var acked = result.Groups.Where(g => g.AvgAckMinutes.HasValue).ToList();
        Assert.NotEmpty(acked);
        Assert.All(acked, g => Assert.InRange(g.AvgAckMinutes!.Value, 0, 120));

        // still_open per group must sum to total_unacked
        Assert.Equal(result.TotalUnacked, result.Groups.Sum(g => g.StillOpen));
    }

    [Fact]
    public void Alarm_response_metrics_operator_group_excludes_unacked_rows()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.IngestAlarms(client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-1)).Result);

        var result = mirror.GetAlarmResponseMetrics(
            groupBy: "operator",
            since: DateTime.UtcNow.AddDays(-1), until: DateTime.UtcNow.AddDays(1),
            eventId: null, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null, limit: 10);

        // Grand totals still include unacked rows
        Assert.True(result.TotalAlarms >= result.Groups.Sum(g => g.Total));
        // Every operator group must have a non-null key and still_open == 0
        // (unacked rows have no ak_operator and are filtered out of the group set)
        Assert.All(result.Groups, g =>
        {
            Assert.NotNull(g.Key);
            Assert.Equal(0, g.StillOpen);
            Assert.True(g.AvgAckMinutes.HasValue);
        });
    }

    [Fact]
    public void Person_dossier_composes_summary_top_doors_and_recent_rows()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);
        mirror.IngestAlarms(client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-1)).Result);

        // Pick a person who actually has some traffic
        var targetPersonId = records
            .GroupBy(r => r.Uid1)
            .OrderByDescending(g => g.Count())
            .First().Key;

        var dossier = mirror.GetPersonDossier(
            personId: targetPersonId,
            since: DateTime.UtcNow.AddDays(-1),
            until: DateTime.UtcNow.AddDays(1),
            topDoorsLimit: 5,
            recentLimit: 5);

        Assert.Equal(targetPersonId, dossier.PersonId);
        Assert.NotNull(dossier.PersonName);

        // Summary totals align with filtered counts
        var expectedEvents = records.Count(r => r.Uid1 == targetPersonId);
        Assert.Equal(expectedEvents, dossier.Summary.TotalEvents);

        var expectedDenials = records.Count(r => r.Uid1 == targetPersonId && r.Disposition > 1);
        Assert.Equal(expectedDenials, dossier.Summary.TotalDenials);

        Assert.NotNull(dossier.Summary.FirstSeen);
        Assert.NotNull(dossier.Summary.LastSeen);
        Assert.True(dossier.Summary.LastSeen >= dossier.Summary.FirstSeen);

        // Hourly pattern is always 24 zero-filled buckets
        Assert.Equal(24, dossier.HourlyPattern.Count);
        Assert.Equal(Enumerable.Range(0, 24), dossier.HourlyPattern.Select(h => h.Hour));
        Assert.Equal(expectedEvents, dossier.HourlyPattern.Sum(h => h.Count));

        // Top doors — each reported count > 0 and every recent denial row is actually denied
        Assert.All(dossier.TopDoors, d => Assert.True(d.Count > 0));
        Assert.All(dossier.RecentDenials, e => Assert.True(e.Disposition > 1));

        // Recent denials ordered newest first
        for (int i = 1; i < dossier.RecentDenials.Count; i++)
            Assert.True(dossier.RecentDenials[i - 1].DtDate >= dossier.RecentDenials[i].DtDate);
    }

    [Fact]
    public void Person_dossier_distinct_doors_collapses_multi_reader_doors()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        var targetPersonId = records.First().Uid1;

        var dossier = mirror.GetPersonDossier(
            personId: targetPersonId,
            since: DateTime.UtcNow.AddDays(-1),
            until: DateTime.UtcNow.AddDays(1),
            topDoorsLimit: 20,
            recentLimit: 0);

        // distinct_doors must not exceed 7 (total fake doors) and must not exceed
        // the number of distinct readers this person touched (since doors collapse readers).
        Assert.True(dossier.Summary.DistinctDoors <= 7);
        var distinctReaders = records.Where(r => r.Uid1 == targetPersonId)
            .Select(r => r.ReaderName).Distinct().Count();
        Assert.True(dossier.Summary.DistinctDoors <= distinctReaders);
    }

    [Fact]
    public void Door_dossier_resolves_readers_and_totals_align_with_raw_records()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        // door_id=1 is "Front Door" (two readers) — chosen because multi-reader doors
        // exercise the reader_name IN (…) path.
        var dossier = mirror.GetDoorDossier(
            doorId: 1,
            since: DateTime.UtcNow.AddDays(-1),
            until: DateTime.UtcNow.AddDays(1),
            topUsersLimit: 5,
            recentLimit: 5);

        Assert.Equal(1, dossier.DoorId);
        Assert.NotNull(dossier.DoorName);
        Assert.True(dossier.ReaderNames.Count >= 2,
            $"Front Door should have at least 2 readers, got {dossier.ReaderNames.Count}");

        // Totals align with hand-counted records filtered to this door's readers
        var doorRecs = records.Where(r => dossier.ReaderNames.Contains(r.ReaderName ?? "")).ToList();
        var expectedAccess = doorRecs.Count(r => r.Disposition == 1);
        var expectedDenied = doorRecs.Count(r => r.Disposition > 1);
        var expectedPeople = doorRecs.Select(r => r.Uid1).Distinct().Count();
        Assert.Equal(expectedAccess, dossier.Summary.TotalAccess);
        Assert.Equal(expectedDenied, dossier.Summary.TotalDenied);
        Assert.Equal(expectedPeople, dossier.Summary.DistinctPeople);

        // Hourly traffic: 24 zero-filled buckets, sum equals total rows at this door
        // (which may exceed access+denied because disposition=0 "None" rows count too).
        Assert.Equal(24, dossier.HourlyTraffic.Count);
        Assert.Equal(doorRecs.Count, dossier.HourlyTraffic.Sum(h => h.Count));
        Assert.True(dossier.HourlyTraffic.Sum(h => h.Count)
            >= dossier.Summary.TotalAccess + dossier.Summary.TotalDenied);

        // If any activity happened, busiest_hour is set and points at a non-zero bucket
        if (dossier.Summary.TotalAccess + dossier.Summary.TotalDenied > 0)
        {
            Assert.NotNull(dossier.Summary.BusiestHour);
            Assert.True(dossier.HourlyTraffic[dossier.Summary.BusiestHour!.Value].Count > 0);
            Assert.NotNull(dossier.Summary.QuietestHour);
            Assert.True(dossier.HourlyTraffic[dossier.Summary.QuietestHour!.Value].Count > 0,
                "quietest_hour should point at an active bucket, not a zero one");
        }

        // Top users: counts descending; every recent denial row belongs to one of this door's readers
        Assert.All(dossier.RecentDenials, e =>
            Assert.Contains(e.ReaderName ?? "", dossier.ReaderNames));
        Assert.All(dossier.RecentDenials, e => Assert.True(e.Disposition > 1));
        for (int i = 1; i < dossier.TopUsers.Count; i++)
            Assert.True(dossier.TopUsers[i - 1].Count >= dossier.TopUsers[i].Count);
    }

    [Fact]
    public void Door_dossier_unknown_door_returns_empty_summary()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result);

        var dossier = mirror.GetDoorDossier(
            doorId: 999,
            since: DateTime.UtcNow.AddDays(-1),
            until: DateTime.UtcNow.AddDays(1),
            topUsersLimit: 5,
            recentLimit: 5);

        Assert.Equal(999, dossier.DoorId);
        Assert.Null(dossier.DoorName);
        Assert.Empty(dossier.ReaderNames);
        Assert.Equal(0, dossier.Summary.TotalAccess);
        Assert.Equal(0, dossier.Summary.TotalDenied);
        Assert.Equal(0, dossier.Summary.DistinctPeople);
        Assert.Null(dossier.Summary.BusiestHour);
        Assert.Empty(dossier.TopUsers);
        Assert.Empty(dossier.RecentDenials);
    }

    [Fact]
    public void Timeseries_alarms_returns_contiguous_zero_filled_buckets()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        var alarms = client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-6)).Result;
        mirror.IngestAlarms(alarms);

        var result = mirror.GetAlarmTimeSeries(
            bucket: "hour",
            since: DateTime.UtcNow.AddHours(-6),
            until: DateTime.UtcNow,
            eventId: null, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null);

        // 6 hours → 6 or 7 contiguous buckets depending on clock boundary
        Assert.InRange(result.Points.Count, 6, 7);
        Assert.Equal(alarms.Count, result.TotalEvents);
        for (int i = 1; i < result.Points.Count; i++)
            Assert.True(result.Points[i].BucketStart > result.Points[i - 1].BucketStart);
    }

    [Fact]
    public void Timeseries_alarms_filter_by_event_id_matches_count_alarms()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.IngestAlarms(client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-6)).Result);

        var since = DateTime.UtcNow.AddDays(-1);
        var until = DateTime.UtcNow.AddDays(1);

        // Pick event_id 5 (Door Held Open) — forced (4) excludes person, so 5 always has some rows in the seed
        var heldCount = mirror.CountAlarms(
            since, until, eventId: 5, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null);

        var series = mirror.GetAlarmTimeSeries(
            bucket: "day",
            since: since, until: until,
            eventId: 5, alarmLevelPriority: null, status: null,
            uid1: null, workstationName: null);

        // Sum of zero-filled buckets must equal the scalar count under the same filter
        Assert.Equal(heldCount, series.TotalEvents);
    }

    [Fact]
    public void Surrounding_events_window_is_symmetric_and_ordered()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);
        mirror.IngestAlarms(client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-1)).Result);

        // Center on an actual event so the window definitely contains something.
        var center = records[records.Count / 2].DtDate;
        const int windowMinutes = 5;

        var result = mirror.GetSurroundingEvents(
            timestamp: center, windowMinutes: windowMinutes, doorId: null, limit: 50);

        Assert.Equal(center, result.Timestamp);
        Assert.Equal(windowMinutes, result.WindowMinutes);
        Assert.Equal(center.AddMinutes(-windowMinutes), result.WindowSince);
        Assert.Equal(center.AddMinutes(windowMinutes), result.WindowUntil);

        // Every event must fall inside the window and be ordered ascending
        Assert.All(result.Events, e =>
        {
            Assert.True(e.DtDate >= result.WindowSince);
            Assert.True(e.DtDate < result.WindowUntil);
        });
        for (int i = 1; i < result.Events.Count; i++)
            Assert.True(result.Events[i].DtDate >= result.Events[i - 1].DtDate);

        // The center event is itself part of the ingested dataset, so the events list can't be empty
        Assert.NotEmpty(result.Events);

        // Hand-count expectation: every record in the raw list that falls within the window
        var expected = records.Count(r =>
            r.DtDate >= result.WindowSince && r.DtDate < result.WindowUntil);
        Assert.True(result.Events.Count <= expected);  // capped by limit
        if (expected <= 50) Assert.Equal(expected, result.Events.Count);
    }

    [Fact]
    public void Surrounding_events_door_id_filters_to_that_doors_readers()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        // Front Door (door_id=1) has readers "Front Door Reader 1" and "…2"
        var center = DateTime.UtcNow.AddMinutes(-30);
        var result = mirror.GetSurroundingEvents(
            timestamp: center, windowMinutes: 60, doorId: 1, limit: 50);

        // Every returned event must be on one of Front Door's readers
        Assert.All(result.Events, e =>
        {
            Assert.NotNull(e.ReaderName);
            Assert.Contains("Front Door Reader", e.ReaderName!);
        });

        // Unknown door → both lists empty, no exception
        var unknown = mirror.GetSurroundingEvents(
            timestamp: center, windowMinutes: 60, doorId: 999, limit: 50);
        Assert.Empty(unknown.Events);
        Assert.Empty(unknown.Alarms);
    }

    [Fact]
    public void Daily_security_briefing_headline_matches_hand_counts()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);

        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-12)).Result;
        mirror.IngestTransactions(records);
        var alarms = client.GetAlarmLogAsync(DateTime.UtcNow.AddHours(-12)).Result;
        mirror.IngestAlarms(alarms);

        // Today covers the last 12 hours of activity
        var today = DateTime.UtcNow.Date;
        var briefing = mirror.GetDailySecurityBriefing(
            today,
            topDoorsLimit: 5,
            notableDeniersThreshold: 1,   // low threshold so any denier qualifies in a short test run
            forcedOpenSampleLimit: 5,
            openAlarmsLimit: 5);

        // Hand-counted transaction totals
        var dayRecs = records.Where(r => r.DtDate >= today && r.DtDate < today.AddDays(1)).ToList();
        Assert.Equal(dayRecs.Count(r => r.Disposition == 1), briefing.Headline.TotalAccess);
        Assert.Equal(dayRecs.Count(r => r.Disposition > 1), briefing.Headline.TotalDenied);

        // Hand-counted alarm totals
        var dayAlarms = alarms.Where(a => a.DtDate.HasValue && a.DtDate >= today && a.DtDate < today.AddDays(1)).ToList();
        Assert.Equal(dayAlarms.Count, briefing.Headline.TotalAlarms);
        Assert.Equal(dayAlarms.Count(a => a.AkDate == null), briefing.Headline.AlarmsUnacked);
        Assert.Equal(dayAlarms.Count(a => a.EventId == 4), briefing.Headline.ForcedOpens);
        Assert.Equal(dayAlarms.Count(a => a.EventId == 5), briefing.Headline.HeldOpens);

        // Delta sanity: headline - prior == deltas
        Assert.Equal(
            briefing.Headline.TotalAccess - briefing.Prior.TotalAccess,
            briefing.VsPriorDay.TotalAccess);
        Assert.Equal(
            briefing.Headline.TotalAlarms - briefing.Prior.TotalAlarms,
            briefing.VsPriorDay.TotalAlarms);

        // Open alarms: every row in the section must have no ak_date
        Assert.All(briefing.OpenAlarms, a => Assert.Null(a.AkDate));

        // Busiest doors: counts descending and every row resolves to a door name
        for (int i = 1; i < briefing.BusiestDoors.Count; i++)
            Assert.True(briefing.BusiestDoors[i - 1].Count >= briefing.BusiestDoors[i].Count);
        Assert.All(briefing.BusiestDoors, d => Assert.NotNull(d.DoorName));
    }

    [Fact]
    public void Daily_security_briefing_forced_opens_have_no_person()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-12)).Result);

        var briefing = mirror.GetDailySecurityBriefing(
            DateTime.UtcNow.Date,
            topDoorsLimit: 5,
            notableDeniersThreshold: 3,
            forcedOpenSampleLimit: 10,
            openAlarmsLimit: 5);

        // Fake client sets uid1=0 / uid1_name=null for event_code=4 per the forced-open fix.
        Assert.All(briefing.ForcedOpenEvents, e =>
        {
            Assert.Equal(4, e.EventCode);
            Assert.Equal(0, e.PersonId);
            Assert.Null(e.PersonName);
        });
    }

    [Fact]
    public void Daily_attendance_min_max_per_person_matches_hand_computation()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-12)).Result;
        mirror.IngestTransactions(records);

        var since = DateTime.UtcNow.AddDays(-1);
        var until = DateTime.UtcNow.AddDays(1);

        var result = mirror.GetDailyAttendance(
            since: since, until: until, personId: null, limit: 100);

        Assert.NotEmpty(result.Rows);

        // Hand-compute first/last per person over the day window for granted-only events
        foreach (var row in result.Rows)
        {
            var expected = records
                .Where(r => r.Uid1 == row.PersonId
                            && r.Disposition == 1
                            && r.DtDate >= row.Day
                            && r.DtDate < row.Day.AddDays(1))
                .ToList();

            Assert.NotEmpty(expected);
            Assert.Equal(expected.Count, row.EventCount);
            // DuckDB rounds TIMESTAMP to microseconds; tolerate sub-tick drift
            Assert.True(Math.Abs((expected.Min(r => r.DtDate) - row.FirstSeen).TotalMilliseconds) < 1);
            Assert.True(Math.Abs((expected.Max(r => r.DtDate) - row.LastSeen).TotalMilliseconds) < 1);
        }

        // Rows ordered by day DESC, then last_seen DESC within each day
        for (int i = 1; i < result.Rows.Count; i++)
        {
            var a = result.Rows[i - 1];
            var b = result.Rows[i];
            Assert.True(
                a.Day > b.Day || (a.Day == b.Day && a.LastSeen >= b.LastSeen),
                $"row {i - 1} ({a.Day:d} {a.LastSeen}) should come before row {i} ({b.Day:d} {b.LastSeen})");
        }
    }

    [Fact]
    public void Daily_attendance_person_id_filter_scopes_results()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        var records = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(records);

        var targetPersonId = (long)records
            .Where(r => r.Disposition == 1 && r.Uid1 > 0)
            .GroupBy(r => r.Uid1)
            .OrderByDescending(g => g.Count())
            .First().Key;

        var result = mirror.GetDailyAttendance(
            since: DateTime.UtcNow.AddDays(-1),
            until: DateTime.UtcNow.AddDays(1),
            personId: targetPersonId,
            limit: 10);

        Assert.NotEmpty(result.Rows);
        Assert.All(result.Rows, r => Assert.Equal(targetPersonId, r.PersonId));
    }

    [Fact]
    public void Forced_through_attempts_detects_denied_then_granted_pairs()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        // Craft deterministic transactions so we know exactly which pairs should surface.
        var anchor = new DateTime(2026, 4, 12, 10, 0, 0, DateTimeKind.Utc);
        VelocityMCP.Data.Models.TransactionRecord Mk(int logId, int secs, int disposition, string reader, int uid1) =>
            new VelocityMCP.Data.Models.TransactionRecord
            {
                LogId = logId,
                DtDate = anchor.AddSeconds(secs),
                EventCode = disposition == 1 ? 1 : 2,
                Description = disposition == 1 ? "Access Granted" : "Access Denied",
                Disposition = disposition,
                ReaderName = reader,
                Uid1 = uid1,
                Uid1Name = $"User {uid1}",
                PortAddr = "1", DtAddr = "1", XAddr = "0",
                NetAddress = "192.168.1.10",
                FromZone = 0, ToZone = 1,
                ServerID = 1, SecurityDomainID = 1,
            };

        mirror.IngestTransactions(new List<VelocityMCP.Data.Models.TransactionRecord>
        {
            // Pair 1: denied → granted at R1 after 3s (gap=3)
            Mk(1001, 0,  2, "Front Door Reader 1", 1001),
            Mk(1002, 3,  1, "Front Door Reader 1", 1002),

            // Pair 2: denied → granted at R1 after 5s (gap=5)
            Mk(1003, 60, 2, "Front Door Reader 1", 1003),
            Mk(1004, 65, 1, "Front Door Reader 1", 1004),

            // NOT a pair: denied followed by granted 20s later (> max_gap 10)
            Mk(1005, 120, 2, "Front Door Reader 1", 1005),
            Mk(1006, 140, 1, "Front Door Reader 1", 1006),

            // NOT a pair: denied at a DIFFERENT reader from the following granted
            Mk(1007, 200, 2, "Side Office Door",   1007),
            Mk(1008, 202, 1, "Loading Dock",       1008),

            // NOT a pair: granted without a preceding denied
            Mk(1009, 300, 1, "Server Room", 1009),
        });

        var result = mirror.GetForcedThroughAttempts(
            since: anchor.AddMinutes(-1),
            until: anchor.AddMinutes(10),
            doorId: null,
            maxGapSeconds: 10,
            limit: 50);

        // Two pairs expected; ordered by denied_time DESC, so Pair 2 (t+60) before Pair 1 (t+0)
        Assert.Equal(2, result.Pairs.Count);

        var p2 = result.Pairs[0];
        Assert.Equal(1003, p2.DeniedLogId);
        Assert.Equal(1004, p2.GrantedLogId);
        Assert.Equal("Front Door Reader 1", p2.ReaderName);
        Assert.Equal(5, p2.GapSeconds, precision: 0);

        var p1 = result.Pairs[1];
        Assert.Equal(1001, p1.DeniedLogId);
        Assert.Equal(1002, p1.GrantedLogId);
        Assert.Equal(3, p1.GapSeconds, precision: 0);

        // Tighten max_gap to 4s → only Pair 1 survives
        var tight = mirror.GetForcedThroughAttempts(
            since: anchor.AddMinutes(-1), until: anchor.AddMinutes(10),
            doorId: null, maxGapSeconds: 4, limit: 50);
        Assert.Single(tight.Pairs);
        Assert.Equal(1001, tight.Pairs[0].DeniedLogId);
    }

    [Fact]
    public void Check_authorization_point_query_returns_granting_clearances()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.UpsertClearances(client.GetClearancesAsync().Result);
        mirror.UpsertReaderClearances(client.GetReaderClearancesAsync().Result);
        mirror.UpsertPersonClearances(client.GetPersonClearancesAsync().Result);

        // Person 1000 (Jane Smith, index 0) is the super-user with "All Hours" → authorized for every door.
        // Server Room (door_id=4) is guarded by "Server Room" + "All Hours" + "Business Hours" clearances.
        var result = mirror.CheckAuthorization(personId: 1000, doorId: 4);
        Assert.True(result.Authorized);
        Assert.Contains(result.GrantingClearances, c => c.Name == "All Hours");
        Assert.Contains(result.GrantingClearances, c => c.Name == "Business Hours");
    }

    [Fact]
    public void Check_authorization_excludes_expired_assignments()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.UpsertClearances(client.GetClearancesAsync().Result);
        mirror.UpsertReaderClearances(client.GetReaderClearancesAsync().Result);

        // Person 1003 with a SINGLE clearance for Server Room that already expired.
        mirror.UpsertPersonClearances(new List<VelocityMCP.Data.Models.PersonClearanceRecord>
        {
            new()
            {
                PersonId = 1003,
                ClearanceId = 4,  // Server Room
                GrantedAt = DateTime.UtcNow.AddDays(-60),
                ExpiresAt = DateTime.UtcNow.AddDays(-1),
            },
        });

        var result = mirror.CheckAuthorization(personId: 1003, doorId: 4);
        Assert.False(result.Authorized);
        Assert.Empty(result.GrantingClearances);
    }

    [Fact]
    public void Check_authorization_person_to_doors_collapses_through_dim_readers()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.UpsertClearances(client.GetClearancesAsync().Result);
        mirror.UpsertReaderClearances(client.GetReaderClearancesAsync().Result);
        mirror.UpsertPersonClearances(client.GetPersonClearancesAsync().Result);

        // Person 1000 is the super-user → 7 doors (all of them, collapsed from 10 readers)
        var doors = mirror.GetAuthorizedDoors(1000);
        Assert.Equal(7, doors.Count);
        Assert.All(doors, d => Assert.NotNull(d.DoorName));

        // Every super-user door row should mention "All Hours" in via_clearances
        Assert.All(doors, d => Assert.Contains("All Hours", d.ViaClearances));
    }

    [Fact]
    public void Check_authorization_door_to_people_returns_super_user()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.UpsertClearances(client.GetClearancesAsync().Result);
        mirror.UpsertReaderClearances(client.GetReaderClearancesAsync().Result);
        mirror.UpsertPersonClearances(client.GetPersonClearancesAsync().Result);

        // Executive Suite (door_id=5): everyone with Business Hours can reach it (all 10 people)
        // plus the super-user has All Hours too. All 10 fake people should show up.
        var people = mirror.GetAuthorizedPeopleForDoor(doorId: 5, limit: 100);
        Assert.Equal(10, people.Count);

        // The super-user (person 1000) is the only one with "All Hours" listed
        var superUser = people.Single(p => p.PersonId == 1000);
        Assert.Contains("All Hours", superUser.ViaClearances);
    }

    [Fact]
    public void List_doors_returns_catalog_with_derived_status()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.IngestTransactions(client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result);

        var rows = mirror.ListDoors(windowHours: 24, includeInactive: true, limit: 100);

        // All 7 fake doors should be present (catalog listing)
        Assert.Equal(7, rows.Count);

        // Every door row has a name and a reader_count matching dim_readers
        Assert.All(rows, r =>
        {
            Assert.NotNull(r.Name);
            Assert.True(r.ReaderCount >= 1, $"{r.Name} expected at least 1 reader, got {r.ReaderCount}");
            Assert.Equal(r.ReaderCount, r.ReaderNames.Count);
        });

        // Multi-reader doors: Front Door (1), Parking Garage (6), Lobby (7) each have 2 readers
        var frontDoor = rows.Single(r => r.DoorId == 1);
        Assert.Equal(2, frontDoor.ReaderCount);
        Assert.Contains("Front Door Reader 1", frontDoor.ReaderNames);
        Assert.Contains("Front Door Reader 2", frontDoor.ReaderNames);

        // Status sanity: an hour of ingested traffic means at least one door should be 'active'
        Assert.Contains(rows, r => r.Status == "active");
        Assert.All(rows, r => Assert.Contains(r.Status, new[] { "active", "quiet", "stale", "never_seen" }));
    }

    [Fact]
    public void List_doors_includeinactive_false_hides_never_seen_and_stale()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        // No transactions ingested → every door is 'never_seen'

        var withInactive = mirror.ListDoors(windowHours: 24, includeInactive: true, limit: 100);
        Assert.Equal(7, withInactive.Count);
        Assert.All(withInactive, r => Assert.Equal("never_seen", r.Status));

        var withoutInactive = mirror.ListDoors(windowHours: 24, includeInactive: false, limit: 100);
        Assert.Empty(withoutInactive);
    }

    [Fact]
    public void Lookup_alarm_categories_returns_seed_data()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        var categories = mirror.ListAlarmCategories();
        Assert.NotEmpty(categories);
        Assert.Contains(categories, c => c.Name == "Duress");
        Assert.Contains(categories, c => c.Name == "Tamper");
    }

    [Fact]
    public void ResponseShaper_passes_small_payload_unchanged()
    {
        var payload = new
        {
            items = Enumerable.Range(0, 5).Select(i => new { id = i, name = $"item-{i}" }),
            total = 5
        };

        // Cap of 8KB; small payload should pass through with all items
        var json = ResponseShaper.SerializeWithCap(_ => payload, 5);

        var parsed = JsonDocument.Parse(json);
        Assert.Equal(5, parsed.RootElement.GetProperty("items").GetArrayLength());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= ResponseShaper.DefaultMaxBytes);
    }

    [Fact]
    public void ResponseShaper_truncates_oversize_payload_and_preserves_valid_json()
    {
        // Build a payload that is guaranteed to overflow a 1KB cap
        const int fullCount = 100;
        var bigItems = Enumerable.Range(0, fullCount)
            .Select(i => new
            {
                id = i,
                name = $"item-{i}",
                description = new string('x', 80),  // padded to ensure each row ~120 bytes
                tags = new[] { "alpha", "beta", "gamma" }
            })
            .ToList();

        var json = ResponseShaper.SerializeWithCap(n => new
        {
            items = bigItems.Take(n),
            returned = n,
            total = fullCount,
            truncated_due_to_size = n < fullCount
        }, fullCount, maxBytes: 1024);

        var byteLen = Encoding.UTF8.GetByteCount(json);
        Assert.True(byteLen <= 1024, $"shaped JSON was {byteLen} bytes, expected <= 1024");

        // Must still be valid JSON
        var parsed = JsonDocument.Parse(json);
        var returned = parsed.RootElement.GetProperty("returned").GetInt32();
        var total = parsed.RootElement.GetProperty("total").GetInt32();
        var flag = parsed.RootElement.GetProperty("truncated_due_to_size").GetBoolean();

        Assert.Equal(fullCount, total);
        Assert.True(returned < fullCount, "expected truncation");
        Assert.True(flag, "truncated_due_to_size flag should be set");
        Assert.Equal(returned, parsed.RootElement.GetProperty("items").GetArrayLength());
    }

    [Fact]
    public void ResponseShaper_serialize_uses_shared_options()
    {
        var json = ResponseShaper.Serialize(new { a = 1, b = "two" });
        // WriteIndented = true → output should contain newlines
        Assert.Contains("\n", json);
        var parsed = JsonDocument.Parse(json);
        Assert.Equal(1, parsed.RootElement.GetProperty("a").GetInt32());
        Assert.Equal("two", parsed.RootElement.GetProperty("b").GetString());
    }

    [Fact]
    public void Cursor_round_trips()
    {
        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        schema.EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        Assert.Null(mirror.GetCursor("Log_Transactions"));

        var now = DateTime.UtcNow;
        mirror.UpdateCursor("Log_Transactions", now, 100);

        var cursor = mirror.GetCursor("Log_Transactions");
        Assert.NotNull(cursor);
        // DuckDB timestamp precision may round slightly
        Assert.True(Math.Abs((cursor.Value - now).TotalSeconds) < 1);
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        // DuckDB may also create .wal file
        var wal = _dbPath + ".wal";
        if (File.Exists(wal)) File.Delete(wal);
    }
}
