using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Smoke tests for the four fuzzy-search entry-point tools: find_doors,
/// find_readers, find_people, find_operators. These are the FIRST tool the
/// LLM calls in nearly every workflow, so a regression in their JSON shape
/// or filter behavior cascades to every downstream tool. One test class
/// covers all four to keep the fixture cost low.
/// </summary>
public class FindToolsTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public FindToolsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        SeedFixture();
    }

    private void SeedFixture()
    {
        // Two doors with multiple readers each — exercises the door→readers
        // grouping in find_doors's response.
        _mirror.UpsertDoors(new List<DoorRecord>
        {
            new() { Id = 10, Name = "Front Lobby" },
            new() { Id = 20, Name = "Server Room" },
        });

        _mirror.UpsertReaders(new List<ReaderRecord>
        {
            new() { Id = 50, Name = "Front Lobby Entry", DoorId = 10 },
            new() { Id = 51, Name = "Front Lobby Exit",  DoorId = 10 },
            new() { Id = 60, Name = "Server Room Entry", DoorId = 20 },
        });

        _mirror.UpsertPeople(new List<PersonRecord>
        {
            new() { PersonId = 100, FirstName = "Alice", LastName = "Anderson" },
            new() { PersonId = 200, FirstName = "Bob",   LastName = "Brown" },
            new() { PersonId = 300, FirstName = "Carol", LastName = "Anderson" }, // shares last name
        });

        _mirror.UpsertOperators(new List<OperatorRecord>
        {
            new() { OperatorId = 1, Name = "Administrator", FullName = "System Administrator", Enabled = true },
            new() { OperatorId = 2, Name = "AuditorReadOnly", FullName = "Read-Only Auditor", Enabled = true },
        });
    }

    // ── find_doors ──────────────────────────────────────────────────────

    [Fact]
    public void FindDoors_ReturnsMatchByPartialName()
    {
        var json = FindDoorsTool.FindDoors(_mirror, query: "Lobby");
        using var doc = JsonDocument.Parse(json);
        var matches = doc.RootElement.GetProperty("matches").EnumerateArray().ToList();

        Assert.Single(matches);
        var lobby = matches[0];
        Assert.Equal(10, lobby.GetProperty("door_id").GetInt32());
        Assert.Equal("Front Lobby", lobby.GetProperty("name").GetString());
    }

    [Fact]
    public void FindDoors_ResponseIncludesReadersListPerDoor()
    {
        var json = FindDoorsTool.FindDoors(_mirror, query: "Lobby");
        using var doc = JsonDocument.Parse(json);
        var lobby = doc.RootElement.GetProperty("matches").EnumerateArray().Single();
        var readers = lobby.GetProperty("readers").EnumerateArray()
            .Select(r => r.GetString()).ToList();

        // Front Lobby has Entry + Exit. The find_doors → reader_names chain
        // is what every door-scoped query uses, so this *must* be locked in.
        Assert.Equal(2, readers.Count);
        Assert.Contains("Front Lobby Entry", readers);
        Assert.Contains("Front Lobby Exit", readers);
    }

    [Fact]
    public void FindDoors_IsCaseInsensitive()
    {
        var lower = FindDoorsTool.FindDoors(_mirror, query: "lobby");
        var upper = FindDoorsTool.FindDoors(_mirror, query: "LOBBY");

        var lowerCount = JsonDocument.Parse(lower).RootElement.GetProperty("matches").GetArrayLength();
        var upperCount = JsonDocument.Parse(upper).RootElement.GetProperty("matches").GetArrayLength();
        Assert.Equal(1, lowerCount);
        Assert.Equal(lowerCount, upperCount);
    }

    [Fact]
    public void FindDoors_NoMatch_ReturnsEmptyArrayNotNull()
    {
        var json = FindDoorsTool.FindDoors(_mirror, query: "nonexistent");
        using var doc = JsonDocument.Parse(json);

        var matches = doc.RootElement.GetProperty("matches");
        Assert.Equal(JsonValueKind.Array, matches.ValueKind);
        Assert.Equal(0, matches.GetArrayLength());
        Assert.Equal(0, doc.RootElement.GetProperty("total").GetInt32());
    }

    // ── find_readers ────────────────────────────────────────────────────

    [Fact]
    public void FindReaders_MatchesAcrossMultipleDoors()
    {
        // "Entry" matches both Front Lobby Entry and Server Room Entry.
        var json = FindReadersTool.FindReaders(_mirror, query: "Entry");
        using var doc = JsonDocument.Parse(json);
        var matches = doc.RootElement.GetProperty("matches").EnumerateArray().ToList();

        Assert.Equal(2, matches.Count);
        var ids = matches.Select(m => m.GetProperty("reader_id").GetInt32()).ToHashSet();
        Assert.Contains(50, ids);
        Assert.Contains(60, ids);
    }

    [Fact]
    public void FindReaders_ResponseHasDocumentedFieldNames()
    {
        var json = FindReadersTool.FindReaders(_mirror, query: "Server");
        using var doc = JsonDocument.Parse(json);
        var match = doc.RootElement.GetProperty("matches").EnumerateArray().Single();

        Assert.True(match.TryGetProperty("reader_id", out _));
        Assert.True(match.TryGetProperty("name", out _));
    }

    // ── find_people ─────────────────────────────────────────────────────

    [Fact]
    public void FindPeople_MatchesByFirstNameOrLastName()
    {
        var json = FindPeopleTool.FindPeople(_mirror, query: "Anderson");
        using var doc = JsonDocument.Parse(json);
        var matches = doc.RootElement.GetProperty("matches").EnumerateArray().ToList();

        // Both Alice Anderson and Carol Anderson should come back.
        Assert.Equal(2, matches.Count);
        var ids = matches.Select(m => m.GetProperty("person_id").GetInt32()).ToHashSet();
        Assert.Contains(100, ids);
        Assert.Contains(300, ids);
    }

    [Fact]
    public void FindPeople_ResponseHasDocumentedFieldNames()
    {
        var json = FindPeopleTool.FindPeople(_mirror, query: "Alice");
        using var doc = JsonDocument.Parse(json);
        var match = doc.RootElement.GetProperty("matches").EnumerateArray().Single();

        // The LLM relies on these exact field names — every workflow that
        // does find_people → person_dossier reads `person_id` from this row.
        Assert.True(match.TryGetProperty("person_id", out _));
        Assert.True(match.TryGetProperty("first_name", out _));
        Assert.True(match.TryGetProperty("last_name", out _));
        Assert.True(match.TryGetProperty("full_name", out _));
        Assert.Equal("Alice Anderson", match.GetProperty("full_name").GetString());
    }

    [Fact]
    public void FindPeople_LimitClampsResults()
    {
        var json = FindPeopleTool.FindPeople(_mirror, query: "Anderson", limit: 1);
        using var doc = JsonDocument.Parse(json);
        var matches = doc.RootElement.GetProperty("matches").EnumerateArray().ToList();

        Assert.Single(matches);
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
    }

    // ── find_operators ──────────────────────────────────────────────────

    [Fact]
    public void FindOperators_MatchesByNameSubstring()
    {
        var json = FindOperatorsTool.FindOperators(_mirror, query: "admin");
        using var doc = JsonDocument.Parse(json);
        var matches = doc.RootElement.GetProperty("matches").EnumerateArray().ToList();

        Assert.Single(matches);
        Assert.Equal(1, matches[0].GetProperty("operator_id").GetInt32());
        Assert.Equal("Administrator", matches[0].GetProperty("name").GetString());
    }

    [Fact]
    public void FindOperators_AlsoMatchesFullNameField()
    {
        // Should match against full_name too — "Auditor" doesn't appear in name.
        var json = FindOperatorsTool.FindOperators(_mirror, query: "Auditor");
        using var doc = JsonDocument.Parse(json);
        var matches = doc.RootElement.GetProperty("matches").EnumerateArray().ToList();

        Assert.Single(matches);
        Assert.Equal(2, matches[0].GetProperty("operator_id").GetInt32());
    }

    [Fact]
    public void FindOperators_ResponseHasDocumentedFieldNames()
    {
        var json = FindOperatorsTool.FindOperators(_mirror, query: "admin");
        using var doc = JsonDocument.Parse(json);
        var match = doc.RootElement.GetProperty("matches").EnumerateArray().Single();

        Assert.True(match.TryGetProperty("operator_id", out _));
        Assert.True(match.TryGetProperty("name", out _));
        Assert.True(match.TryGetProperty("full_name", out _));
        Assert.True(match.TryGetProperty("description", out _));
        Assert.True(match.TryGetProperty("enabled", out _));
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
