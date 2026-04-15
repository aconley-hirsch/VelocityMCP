using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// server_info now reports live counts from every dim and fact table — this
/// is what the model uses to ground "how many X are in the system" questions
/// without having to call any data tool.
/// </summary>
public class ServerInfoToolTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;

    public ServerInfoToolTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
    }

    [Fact]
    public void ServerInfo_IncludesAllExpectedCountFields()
    {
        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        var json = ServerInfoTool.ServerInfo(mirror);
        using var doc = JsonDocument.Parse(json);
        var counts = doc.RootElement.GetProperty("counts");

        // Every count field documented in the tool description must be present.
        // Tests fail loudly if a future refactor drops one.
        Assert.True(counts.TryGetProperty("people", out _));
        Assert.True(counts.TryGetProperty("doors", out _));
        Assert.True(counts.TryGetProperty("readers", out _));
        Assert.True(counts.TryGetProperty("clearances", out _));
        Assert.True(counts.TryGetProperty("operators", out _));
        Assert.True(counts.TryGetProperty("transactions", out _));
        Assert.True(counts.TryGetProperty("alarms", out _));
        Assert.True(counts.TryGetProperty("software_events", out _));
    }

    [Fact]
    public void ServerInfo_CountsReflectSeededData()
    {
        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        using var client = new FakeVelocityClient();

        // Seed every dim the fake client provides + one fact stream so we can
        // verify counts are wired correctly across all sources.
        mirror.UpsertPeople(client.GetPersonsAsync().Result);
        mirror.UpsertDoors(client.GetDoorsAsync().Result);
        mirror.UpsertReaders(client.GetReadersAsync().Result);
        mirror.UpsertClearances(client.GetClearancesAsync().Result);
        mirror.UpsertOperators(client.GetOperatorsAsync().Result);
        var people = client.GetPersonsAsync().Result;
        var operators = client.GetOperatorsAsync().Result;
        var transactions = client.GetLogTransactionsAsync(DateTime.UtcNow.AddHours(-1)).Result;
        mirror.IngestTransactions(transactions);

        var json = ServerInfoTool.ServerInfo(mirror);
        using var doc = JsonDocument.Parse(json);
        var counts = doc.RootElement.GetProperty("counts");

        Assert.Equal(people.Count, counts.GetProperty("people").GetInt64());
        Assert.Equal(operators.Count, counts.GetProperty("operators").GetInt64());
        Assert.Equal(transactions.Count, counts.GetProperty("transactions").GetInt64());
        // software_events not seeded → must be 0 (proves the count is real, not stub)
        Assert.Equal(0, counts.GetProperty("software_events").GetInt64());
    }

    [Fact]
    public void ServerInfo_AlwaysReturnsServerTimeAndTimezone()
    {
        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        var json = ServerInfoTool.ServerInfo(mirror);
        using var doc = JsonDocument.Parse(json);

        var serverTime = doc.RootElement.GetProperty("server_time").GetString();
        Assert.False(string.IsNullOrEmpty(serverTime));
        var parsed = DateTime.Parse(serverTime!).ToUniversalTime();
        // server_time must be within a few seconds of "now" — anchors the
        // model's interpretation of relative phrases like "yesterday".
        Assert.True(Math.Abs((parsed - DateTime.UtcNow).TotalSeconds) < 5);

        Assert.False(string.IsNullOrEmpty(doc.RootElement.GetProperty("timezone").GetString()));
        Assert.Equal("VelocityMCP", doc.RootElement.GetProperty("server_name").GetString());
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
