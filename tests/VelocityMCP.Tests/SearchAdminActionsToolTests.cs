using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// search_admin_actions combines count + sample with filters. The most
/// important behavior to lock in is the description_query substring match
/// (which is how the model answers topical audit-trail questions without
/// knowing event codes), and the operator_name resolution from dim_operators.
/// </summary>
public class SearchAdminActionsToolTests : IDisposable
{
    private const int AdminOperatorId = 1;
    private const int SecOpsOperatorId = 2;

    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public SearchAdminActionsToolTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        SeedFixture();
    }

    private void SeedFixture()
    {
        // Two operators — IngestSoftwareEvents resolves operator_name via a
        // correlated subquery against dim_operators, so this MUST be seeded
        // before the IngestSoftwareEvents call below.
        _mirror.UpsertOperators(new List<OperatorRecord>
        {
            new() { OperatorId = AdminOperatorId, Name = "Administrator", Enabled = true },
            new() { OperatorId = SecOpsOperatorId, Name = "SecurityOps",  Enabled = true },
        });

        var now = DateTime.UtcNow;
        _mirror.IngestSoftwareEvents(new List<SoftwareEventRecord>
        {
            // Recent events spanning two operators and three event codes.
            new()
            {
                LogId = 1, DtDate = now.AddMinutes(-10), EventCode = 1037,
                Description = "Credential 5001: HID-STANDARD was added by Administrator",
                OperatorId = AdminOperatorId,
            },
            new()
            {
                LogId = 2, DtDate = now.AddMinutes(-20), EventCode = 1039,
                Description = "Credential 5002 was changed by Administrator",
                OperatorId = AdminOperatorId,
            },
            new()
            {
                LogId = 3, DtDate = now.AddMinutes(-30), EventCode = 1040,
                Description = "Person Server Room User was added by SecurityOps",
                OperatorId = SecOpsOperatorId,
            },
            new()
            {
                LogId = 4, DtDate = now.AddMinutes(-40), EventCode = 1022,
                Description = "Operator SecurityOps logged on to workstation VELOCITY-WS1",
                OperatorId = SecOpsOperatorId,
            },
            new()
            {
                LogId = 5, DtDate = now.AddMinutes(-50), EventCode = 1037,
                Description = "Credential 5003: SAFR was added by Administrator",
                OperatorId = AdminOperatorId,
            },
            // One ancient event outside the default 7-day window — must NOT
            // be returned by default queries.
            new()
            {
                LogId = 6, DtDate = now.AddDays(-30), EventCode = 1037,
                Description = "Credential 4000: Ancient Badge was added by Administrator",
                OperatorId = AdminOperatorId,
            },
        });
    }

    [Fact]
    public void Default_ReturnsEventsInTimeDescOrder()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(_mirror);
        var events = ParseEvents(json);

        Assert.NotEmpty(events);
        // Default order is most-recent first.
        for (int i = 1; i < events.Count; i++)
            Assert.True(events[i - 1].DtDate >= events[i].DtDate,
                $"events should be time_desc but row {i - 1} ({events[i - 1].DtDate}) precedes row {i} ({events[i].DtDate})");
    }

    [Fact]
    public void Default_ExcludesEventsOutsideSevenDayWindow()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(_mirror);
        var events = ParseEvents(json);

        // The 30-day-old event (LogId=6) must not be returned by default.
        Assert.DoesNotContain(events, e => e.LogId == 6);
        // total_matching reflects the filter, not the table size.
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(5, doc.RootElement.GetProperty("total_matching").GetInt64());
    }

    [Fact]
    public void DescriptionQuery_FiltersBySubstringMatch()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(
            _mirror, description_query: "Credential");
        var events = ParseEvents(json);

        // 3 of the 5 in-window events mention "Credential".
        Assert.Equal(3, events.Count);
        Assert.All(events, e => Assert.Contains("Credential", e.Description));
    }

    [Fact]
    public void DescriptionQuery_IsCaseInsensitive()
    {
        var lowerJson = SearchAdminActionsTool.SearchAdminActions(
            _mirror, description_query: "credential");
        var upperJson = SearchAdminActionsTool.SearchAdminActions(
            _mirror, description_query: "CREDENTIAL");

        var lowerCount = ParseEvents(lowerJson).Count;
        var upperCount = ParseEvents(upperJson).Count;

        Assert.Equal(lowerCount, upperCount);
        Assert.Equal(3, lowerCount);
    }

    [Fact]
    public void OperatorIdFilter_ScopesToOneOperator()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(
            _mirror, operator_id: SecOpsOperatorId);
        var events = ParseEvents(json);

        // SecurityOps did 2 things in the 7-day window.
        Assert.Equal(2, events.Count);
        Assert.All(events, e =>
        {
            Assert.Equal(SecOpsOperatorId, e.OperatorId);
            Assert.Equal("SecurityOps", e.OperatorName);
        });
    }

    [Fact]
    public void EventCodeFilter_ScopesToOneCode()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(
            _mirror, event_code: 1037);
        var events = ParseEvents(json);

        // 1037 = Credential added. Two in-window matches.
        Assert.Equal(2, events.Count);
        Assert.All(events, e => Assert.Equal(1037, e.EventCode));
    }

    [Fact]
    public void OperatorName_ResolvedFromDimOperators()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(_mirror);
        var events = ParseEvents(json);

        // operator_name is denormalized at IngestSoftwareEvents time via a
        // correlated subquery. Both seeded operators must come back resolved.
        Assert.Contains(events, e => e.OperatorName == "Administrator");
        Assert.Contains(events, e => e.OperatorName == "SecurityOps");
    }

    [Fact]
    public void OrderTimeAsc_ReversesDefaultOrder()
    {
        var json = SearchAdminActionsTool.SearchAdminActions(_mirror, order: "time_asc");
        var events = ParseEvents(json);

        Assert.NotEmpty(events);
        for (int i = 1; i < events.Count; i++)
            Assert.True(events[i - 1].DtDate <= events[i].DtDate,
                $"events should be time_asc but row {i - 1} ({events[i - 1].DtDate}) follows row {i} ({events[i].DtDate})");
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private record EventRow(
        int LogId,
        DateTime DtDate,
        int EventCode,
        string Description,
        int? OperatorId,
        string? OperatorName);

    private static List<EventRow> ParseEvents(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("events")
            .EnumerateArray()
            .Select(e => new EventRow(
                LogId: e.GetProperty("log_id").GetInt32(),
                DtDate: DateTime.Parse(e.GetProperty("dt_date").GetString()!),
                EventCode: e.GetProperty("event_code").GetInt32(),
                Description: e.GetProperty("description").GetString()!,
                OperatorId: e.GetProperty("operator_id").ValueKind == JsonValueKind.Null
                    ? null
                    : e.GetProperty("operator_id").GetInt32(),
                OperatorName: e.GetProperty("operator_name").ValueKind == JsonValueKind.Null
                    ? null
                    : e.GetProperty("operator_name").GetString()))
            .ToList();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
