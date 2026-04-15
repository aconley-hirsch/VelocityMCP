using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Locks in the credentials→people rollup for door dossier. The pre-existing
/// EndToEndTests.Door_dossier_resolves_readers_and_totals_align_with_raw_records
/// test passed vacuously: it compared `expectedPeople = records.Select(r => r.Uid1).Distinct()`
/// (a credential count) against `dossier.Summary.DistinctPeople` (also a
/// credential count), so both sides used the same wrong concept and the
/// assertion was always true. These tests catch the actual semantic — a
/// person with two badges must count as ONE distinct person on the door.
/// </summary>
public class DoorDossierToolTests : IDisposable
{
    private const int AlicePersonId = 100;
    private const int BobPersonId = 200;
    private const int AliceCredA = 5001;
    private const int AliceCredB = 5002;
    private const int BobCred = 5003;
    private const int OrphanCred = 9999;

    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public DoorDossierToolTests()
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
            // OrphanCred deliberately not enrolled — used to test the
            // unresolved-credential branch of the rollup.
        });
        _mirror.UpsertDoors(new List<DoorRecord>
        {
            new() { Id = 10, Name = "Front Lobby" },
        });
        _mirror.UpsertReaders(new List<ReaderRecord>
        {
            new() { Id = 50, Name = "Front Lobby Reader", DoorId = 10 },
        });

        var now = DateTime.UtcNow;
        _mirror.IngestTransactions(new List<TransactionRecord>
        {
            // Alice swipes badge A twice
            new()
            {
                LogId = 1, DtDate = now.AddMinutes(-10), EventCode = 1, Description = "Access Granted",
                Disposition = 1, ReaderName = "Front Lobby Reader",
                Uid1 = AliceCredA, Uid1Name = "Alice Anderson",
            },
            new()
            {
                LogId = 2, DtDate = now.AddMinutes(-15), EventCode = 1, Description = "Access Granted",
                Disposition = 1, ReaderName = "Front Lobby Reader",
                Uid1 = AliceCredA, Uid1Name = "Alice Anderson",
            },
            // Alice swipes badge B once
            new()
            {
                LogId = 3, DtDate = now.AddMinutes(-20), EventCode = 1, Description = "Access Granted",
                Disposition = 1, ReaderName = "Front Lobby Reader",
                Uid1 = AliceCredB, Uid1Name = "Alice Anderson",
            },
            // Bob swipes once
            new()
            {
                LogId = 4, DtDate = now.AddMinutes(-25), EventCode = 1, Description = "Access Granted",
                Disposition = 1, ReaderName = "Front Lobby Reader",
                Uid1 = BobCred, Uid1Name = "Bob Brown",
            },
            // Orphan credential — should add 1 to distinct_people (its own bucket)
            new()
            {
                LogId = 5, DtDate = now.AddMinutes(-30), EventCode = 1, Description = "Access Granted",
                Disposition = 1, ReaderName = "Front Lobby Reader",
                Uid1 = OrphanCred, Uid1Name = "Ghost Walker",
            },
        });
    }

    [Fact]
    public void DistinctPeople_RollsAlicesTwoBadgesIntoOnePerson()
    {
        var json = DoorDossierTool.DoorDossier(_mirror, door_id: 10);
        using var doc = JsonDocument.Parse(json);
        var summary = doc.RootElement.GetProperty("summary");

        // 5 events → 3 distinct PEOPLE: Alice (2 badges), Bob, Orphan credential.
        // Pre-fix this would have returned 4 (counting Alice's two credentials separately).
        Assert.Equal(3, summary.GetProperty("distinct_people").GetInt64());
        // Total access count is unchanged — this is per-row, not per-person.
        Assert.Equal(5, summary.GetProperty("total_access").GetInt64());
    }

    [Fact]
    public void TopUsers_RollsAlicesTwoBadgesIntoOneBucketWithCombinedCount()
    {
        var json = DoorDossierTool.DoorDossier(_mirror, door_id: 10);
        using var doc = JsonDocument.Parse(json);
        var topUsers = doc.RootElement.GetProperty("top_users").EnumerateArray().ToList();

        // Alice should be ONE row with count = 3 (2 from badge A + 1 from badge B).
        var alice = topUsers.SingleOrDefault(u =>
            u.GetProperty("person_id").GetInt64() == AlicePersonId);
        Assert.True(alice.ValueKind != JsonValueKind.Undefined,
            "Alice should appear once in top_users — credentials must collapse");
        Assert.Equal(3, alice.GetProperty("count").GetInt64());
        Assert.Equal("Alice Anderson", alice.GetProperty("person_name").GetString());

        // Bob should also appear, with his real HostUserId (200), not BobCred (5003).
        var bob = topUsers.SingleOrDefault(u =>
            u.GetProperty("person_id").GetInt64() == BobPersonId);
        Assert.True(bob.ValueKind != JsonValueKind.Undefined,
            "Bob should appear once in top_users with his HostUserId");
        Assert.Equal(1, bob.GetProperty("count").GetInt64());
    }

    [Fact]
    public void TopUsers_OrphanCredentialAppearsWithNegativePersonId()
    {
        var json = DoorDossierTool.DoorDossier(_mirror, door_id: 10);
        using var doc = JsonDocument.Parse(json);
        var topUsers = doc.RootElement.GetProperty("top_users").EnumerateArray().ToList();

        // Orphan credentials still appear — we don't drop activity. The marker
        // is a negative person_id (= -CredentialId) and a fallback name.
        var orphan = topUsers.SingleOrDefault(u =>
            u.GetProperty("person_id").GetInt64() == -OrphanCred);
        Assert.True(orphan.ValueKind != JsonValueKind.Undefined,
            $"Orphan credential {OrphanCred} should appear with person_id={-OrphanCred}");
        Assert.Equal(1, orphan.GetProperty("count").GetInt64());
    }

    [Fact]
    public void TopUsers_KeyIdNeverContainsCredentialIds()
    {
        var json = DoorDossierTool.DoorDossier(_mirror, door_id: 10);
        using var doc = JsonDocument.Parse(json);
        var topUsers = doc.RootElement.GetProperty("top_users").EnumerateArray().ToList();

        // Critical regression guard: no positive person_id in the result should
        // equal a known CredentialId. If we see 5001/5002/5003 we know the join
        // broke and we're back to credential-level reporting.
        var positiveIds = topUsers
            .Select(u => u.GetProperty("person_id").GetInt64())
            .Where(id => id > 0)
            .ToHashSet();
        Assert.DoesNotContain(AliceCredA, positiveIds);
        Assert.DoesNotContain(AliceCredB, positiveIds);
        Assert.DoesNotContain(BobCred,   positiveIds);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
