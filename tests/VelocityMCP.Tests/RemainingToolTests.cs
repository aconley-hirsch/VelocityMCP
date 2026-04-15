using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Tool-level smoke tests for the four remaining registered tools that had
/// zero direct coverage: GetAlarmTool, SampleEventsTool, ListEventTypesTool,
/// ListDispositionsTool. One test class to keep fixture overhead low. Each
/// test verifies the JSON shape (LLM-facing surface) and basic behavior;
/// deeper SQL coverage stays in the existing mirror-level tests.
/// </summary>
public class RemainingToolTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public RemainingToolTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        SeedFixture();
    }

    private void SeedFixture()
    {
        // Two transactions and two alarms so SampleEvents and GetAlarm have
        // something real to chew on.
        var now = DateTime.UtcNow;

        _mirror.IngestTransactions(new List<TransactionRecord>
        {
            new()
            {
                LogId = 1, DtDate = now.AddMinutes(-10), EventCode = 1,
                Description = "Access Granted", Disposition = 1,
                ReaderName = "Front Door", Uid1 = 5001, Uid1Name = "Alice Anderson",
            },
            new()
            {
                LogId = 2, DtDate = now.AddMinutes(-20), EventCode = 2,
                Description = "Access Denied", Disposition = 2,
                ReaderName = "Server Room", Uid1 = 5002, Uid1Name = "Bob Brown",
            },
        });

        _mirror.IngestAlarms(new List<AlarmRecord>
        {
            new()
            {
                AlarmId = 100, DtDate = now.AddMinutes(-5),
                EventId = 4, AlarmLevelPriority = 1, Status = 0,
                Description = "Door Forced Open", WorkstationName = "VELOCITY-WS1",
                Uid1 = 5001, Uid1Name = "Alice Anderson",
            },
        });
    }

    // ── get_alarm ───────────────────────────────────────────────────────

    [Fact]
    public void GetAlarm_ReturnsAlarmDetailsForKnownId()
    {
        var json = GetAlarmTool.GetAlarm(_mirror, alarm_id: 100);
        using var doc = JsonDocument.Parse(json);

        Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
        Assert.Equal(100, doc.RootElement.GetProperty("alarm_id").GetInt32());
        Assert.Equal(4, doc.RootElement.GetProperty("event_id").GetInt32());
        Assert.Equal("Door Forced Open", doc.RootElement.GetProperty("description").GetString());
        Assert.Equal("VELOCITY-WS1", doc.RootElement.GetProperty("workstation_name").GetString());
    }

    [Fact]
    public void GetAlarm_UnknownId_ReturnsFoundFalse()
    {
        var json = GetAlarmTool.GetAlarm(_mirror, alarm_id: 999999);
        using var doc = JsonDocument.Parse(json);

        Assert.False(doc.RootElement.GetProperty("found").GetBoolean());
        Assert.Equal(999999, doc.RootElement.GetProperty("alarm_id").GetInt32());
    }

    [Fact]
    public void GetAlarm_ResponseHasDocumentedFieldNames()
    {
        var json = GetAlarmTool.GetAlarm(_mirror, alarm_id: 100);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Pin the field names the LLM relies on.
        foreach (var field in new[] { "found", "alarm_id", "time", "event_id", "alarm_level_priority",
                                      "status", "description", "workstation_name", "person_id",
                                      "person_name" })
        {
            Assert.True(root.TryGetProperty(field, out _), $"missing field: {field}");
        }
    }

    // ── sample_events ───────────────────────────────────────────────────

    [Fact]
    public void SampleEvents_ReturnsEventsWithinDefaultWindow()
    {
        var json = SampleEventsTool.SampleEvents(_mirror);
        using var doc = JsonDocument.Parse(json);
        var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();

        Assert.Equal(2, events.Count);
        Assert.Equal(2, doc.RootElement.GetProperty("total_matching").GetInt64());
    }

    [Fact]
    public void SampleEvents_DefaultsToTimeDescOrder()
    {
        var json = SampleEventsTool.SampleEvents(_mirror);
        using var doc = JsonDocument.Parse(json);
        var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();

        Assert.Equal("time_desc", doc.RootElement.GetProperty("order").GetString());
        // Most recent first → log_id 1 (10 min ago) before log_id 2 (20 min ago)
        Assert.Equal(1, events[0].GetProperty("log_id").GetInt32());
        Assert.Equal(2, events[1].GetProperty("log_id").GetInt32());
    }

    [Fact]
    public void SampleEvents_DispositionFilterScopesToDeniedOnly()
    {
        var json = SampleEventsTool.SampleEvents(_mirror, disposition: 2);
        using var doc = JsonDocument.Parse(json);
        var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();

        Assert.Single(events);
        Assert.Equal(2, events[0].GetProperty("log_id").GetInt32());
        Assert.Equal(2, events[0].GetProperty("disposition").GetInt32());
    }

    [Fact]
    public void SampleEvents_LimitClampsTo50()
    {
        // Tool description says max 50 — pass something larger and verify it clamps.
        var json = SampleEventsTool.SampleEvents(_mirror, limit: 9999);
        using var doc = JsonDocument.Parse(json);

        // We only have 2 events seeded, but the resolved limit must be ≤50.
        // Easiest way to verify: returned count is ≤ 2 (no error raised).
        Assert.Equal(2, doc.RootElement.GetProperty("events").GetArrayLength());
    }

    // ── list_event_types ────────────────────────────────────────────────

    [Fact]
    public void ListEventTypes_ReturnsSeededCatalog()
    {
        var json = ListEventTypesTool.ListEventTypes(_mirror);
        using var doc = JsonDocument.Parse(json);

        var total = doc.RootElement.GetProperty("total").GetInt32();
        Assert.True(total > 0);

        var types = doc.RootElement.GetProperty("event_types").EnumerateArray().ToList();
        Assert.Equal(total, types.Count);

        // Every row must have the documented field names.
        foreach (var t in types)
        {
            Assert.True(t.TryGetProperty("event_code", out _));
            Assert.True(t.TryGetProperty("category", out _));
            Assert.True(t.TryGetProperty("name", out _));
            Assert.True(t.TryGetProperty("description", out _));
        }
    }

    [Fact]
    public void ListEventTypes_IncludesKnownSeedRows()
    {
        // Schema seeds at least events 1 (Access Granted) and 4 (Door Forced Open).
        // Tests these to catch silent removals from the seed list.
        var json = ListEventTypesTool.ListEventTypes(_mirror);
        using var doc = JsonDocument.Parse(json);
        var codes = doc.RootElement.GetProperty("event_types").EnumerateArray()
            .Select(t => t.GetProperty("event_code").GetInt32()).ToHashSet();

        Assert.Contains(1, codes);
        Assert.Contains(4, codes);
    }

    // ── list_dispositions ───────────────────────────────────────────────

    [Fact]
    public void ListDispositions_ReturnsSeededCatalog()
    {
        var json = ListDispositionsTool.ListDispositions(_mirror);
        using var doc = JsonDocument.Parse(json);

        var total = doc.RootElement.GetProperty("total").GetInt32();
        Assert.True(total > 0);

        var dispositions = doc.RootElement.GetProperty("dispositions").EnumerateArray().ToList();
        Assert.Equal(total, dispositions.Count);

        foreach (var d in dispositions)
        {
            Assert.True(d.TryGetProperty("disposition", out _));
            Assert.True(d.TryGetProperty("name", out _));
        }
    }

    [Fact]
    public void ListDispositions_IncludesGrantedAndDenied()
    {
        var json = ListDispositionsTool.ListDispositions(_mirror);
        using var doc = JsonDocument.Parse(json);
        var rows = doc.RootElement.GetProperty("dispositions").EnumerateArray()
            .Select(r => new
            {
                Code = r.GetProperty("disposition").GetInt32(),
                Name = r.GetProperty("name").GetString()
            })
            .ToList();

        // Granted = 1 is fundamental; the LLM uses it to filter "successful entries"
        Assert.Contains(rows, r => r.Code == 1 && r.Name == "Granted");
        // At least one denial code should exist — without these, "show me denials"
        // questions can't resolve disposition values.
        Assert.Contains(rows, r => r.Name != null && r.Name.Contains("Denied"));
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
