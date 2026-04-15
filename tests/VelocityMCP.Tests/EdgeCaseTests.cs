using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Edge case batch: empty result sets, parameter clamping, time-window
/// boundary inclusivity. None of these are likely to reveal a bug today,
/// but they pin behavior that's easy to break by accident in future
/// refactors. One small fixture, multiple targeted assertions.
/// </summary>
public class EdgeCaseTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public EdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        // Two transactions at known instants for boundary tests.
        // BoundaryEvent at exactly _boundary.
        // OutsideEvent at _boundary - 1ms (just before).
        var tx = new List<TransactionRecord>
        {
            new()
            {
                LogId = 1, DtDate = Boundary, EventCode = 1,
                Description = "At boundary", Disposition = 1,
                ReaderName = "Test Reader", Uid1 = 5001, Uid1Name = "Boundary Tester",
            },
            new()
            {
                LogId = 2, DtDate = Boundary.AddMilliseconds(-1), EventCode = 1,
                Description = "Before boundary", Disposition = 1,
                ReaderName = "Test Reader", Uid1 = 5001, Uid1Name = "Boundary Tester",
            },
        };
        _mirror.IngestTransactions(tx);
    }

    private static readonly DateTime Boundary = new(2026, 4, 14, 12, 0, 0, DateTimeKind.Utc);

    // ── Empty result sets ───────────────────────────────────────────────

    [Fact]
    public void AggregateEvents_EmptyWindow_ReturnsZeroGroupsAndZeroEvents()
    {
        // A future window with nothing in it.
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            since: "2099-01-01T00:00:00Z",
            until: "2099-01-02T00:00:00Z");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(0, doc.RootElement.GetProperty("total_events").GetInt64());
        Assert.Equal(0, doc.RootElement.GetProperty("total_groups").GetInt64());
        // groups must be an empty array, never null
        Assert.Equal(JsonValueKind.Array, doc.RootElement.GetProperty("groups").ValueKind);
        Assert.Equal(0, doc.RootElement.GetProperty("groups").GetArrayLength());
    }

    [Fact]
    public void CountEvents_FutureWindow_ReturnsZero()
    {
        var json = CountEventsTool.CountEvents(
            _mirror,
            since: "2099-01-01T00:00:00Z",
            until: "2099-01-02T00:00:00Z");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(0, doc.RootElement.GetProperty("count").GetInt64());
    }

    [Fact]
    public void SearchAdminActions_NoMatchingEvents_ReturnsEmptyArray()
    {
        // No software events seeded → the response must be empty array, not null.
        var json = SearchAdminActionsTool.SearchAdminActions(_mirror);
        using var doc = JsonDocument.Parse(json);

        Assert.Equal(0, doc.RootElement.GetProperty("total_matching").GetInt64());
        Assert.Equal(JsonValueKind.Array, doc.RootElement.GetProperty("events").ValueKind);
        Assert.Equal(0, doc.RootElement.GetProperty("events").GetArrayLength());
    }

    // ── Parameter clamping ──────────────────────────────────────────────

    [Fact]
    public void SearchAdminActions_OverMaxLimit_ClampsToMax()
    {
        // Tool description says max 100. Pass something larger → must clamp,
        // not throw, not return more than 100.
        var json = SearchAdminActionsTool.SearchAdminActions(
            _mirror, limit: 9999);
        using var doc = JsonDocument.Parse(json);

        var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();
        Assert.True(events.Count <= 100,
            $"limit clamping failed: got {events.Count} events, expected ≤100");
    }

    [Fact]
    public void AggregateEvents_OverMaxLimit_ClampsToFifty()
    {
        // Max documented at 50. Larger should clamp.
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            relative_window: "last_24h",
            limit: 9999);
        using var doc = JsonDocument.Parse(json);

        var groups = doc.RootElement.GetProperty("groups").EnumerateArray().ToList();
        Assert.True(groups.Count <= 50);
    }

    [Fact]
    public void ListExpiringCredentials_OverMaxLimit_ClampsTo500()
    {
        // Max documented at 500.
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_perpetual: true, limit: 999_999);
        using var doc = JsonDocument.Parse(json);

        var creds = doc.RootElement.GetProperty("credentials").EnumerateArray().ToList();
        Assert.True(creds.Count <= 500);
    }

    // ── Time-window boundary inclusivity ────────────────────────────────

    [Fact]
    public void TimeWindow_SinceIsInclusive_UntilIsExclusive()
    {
        // Standard SQL/DuckDB convention: WHERE dt_date >= $since AND dt_date < $until.
        // Event at exactly Boundary should be IN the window when since=Boundary,
        // and OUT of the window when until=Boundary.
        //
        // Event 1: at Boundary
        // Event 2: at Boundary - 1ms

        // Window [Boundary, Boundary+1ms) — should include only event 1.
        var json1 = CountEventsTool.CountEvents(
            _mirror,
            since: Boundary.ToString("o"),
            until: Boundary.AddMilliseconds(1).ToString("o"));
        using var doc1 = JsonDocument.Parse(json1);
        Assert.Equal(1, doc1.RootElement.GetProperty("count").GetInt64());

        // Window [Boundary-2ms, Boundary) — should include only event 2 (1 ms before).
        // Event 1 (at exactly Boundary) is excluded by the strict-less-than upper bound.
        var json2 = CountEventsTool.CountEvents(
            _mirror,
            since: Boundary.AddMilliseconds(-2).ToString("o"),
            until: Boundary.ToString("o"));
        using var doc2 = JsonDocument.Parse(json2);
        Assert.Equal(1, doc2.RootElement.GetProperty("count").GetInt64());

        // Window [Boundary-2ms, Boundary+1ms) — should include both events.
        var json3 = CountEventsTool.CountEvents(
            _mirror,
            since: Boundary.AddMilliseconds(-2).ToString("o"),
            until: Boundary.AddMilliseconds(1).ToString("o"));
        using var doc3 = JsonDocument.Parse(json3);
        Assert.Equal(2, doc3.RootElement.GetProperty("count").GetInt64());
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
