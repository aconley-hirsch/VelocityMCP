using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;

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
