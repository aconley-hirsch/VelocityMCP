using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Locks in the semantics of group_by="person" (rolls credentials up to
/// real HostUserIds) vs group_by="credential" (raw badge view). Also
/// exercises the orphan credential fallback and the relative_window
/// time math. Hand-built fixture instead of the FakeVelocityClient so
/// the credential/person mapping is deterministic and we can construct
/// specific edge cases (multi-badge person, orphan credential).
/// </summary>
public class AggregateEventsToolTests : IDisposable
{
    private const int AlicePersonId = 100;
    private const int BobPersonId = 200;
    private const int AliceCredA = 5001;     // Alice's first badge
    private const int AliceCredB = 5002;     // Alice's second badge — same person
    private const int BobCred = 5003;        // Bob's only badge
    private const int OrphanCred = 9999;     // No matching dim_user_credentials row

    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public AggregateEventsToolTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();
        _mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);

        SeedFixture();
    }

    private void SeedFixture()
    {
        // Two real people, three credentials (Alice has two), one orphan
        // credential whose row has no entry in dim_user_credentials.
        _mirror.UpsertPeople(new List<PersonRecord>
        {
            new() { PersonId = AlicePersonId, FirstName = "Alice", LastName = "Anderson" },
            new() { PersonId = BobPersonId, FirstName = "Bob", LastName = "Brown" },
        });

        _mirror.UpsertUserCredentials(new List<UserCredentialRecord>
        {
            new() { CredentialId = AliceCredA, PersonId = AlicePersonId },
            new() { CredentialId = AliceCredB, PersonId = AlicePersonId },
            new() { CredentialId = BobCred,  PersonId = BobPersonId },
            // OrphanCred deliberately NOT in this list — the join must fall back.
        });

        _mirror.UpsertReaders(new List<ReaderRecord>
        {
            new() { Id = 1, Name = "Front Door Reader" },
        });

        // Six events for Alice (3 from each credential), 2 for Bob, 1 orphan.
        // Spread across the last 6 hours so any reasonable window catches them.
        var now = DateTime.UtcNow;
        var transactions = new List<TransactionRecord>();
        int logId = 1;
        foreach (var (uid1, uid1Name, count) in new[]
        {
            (AliceCredA, "Alice Anderson", 3),
            (AliceCredB, "Alice Anderson", 3),
            (BobCred,    "Bob Brown",      2),
            (OrphanCred, "Ghost Walker",   1),  // uid1_name still populated by SDK
        })
        {
            for (int i = 0; i < count; i++)
            {
                transactions.Add(new TransactionRecord
                {
                    LogId = logId++,
                    DtDate = now.AddMinutes(-30 - i),
                    EventCode = 1,
                    Description = "Access Granted",
                    Disposition = 1,
                    ReaderName = "Front Door Reader",
                    Uid1 = uid1,
                    Uid1Name = uid1Name,
                });
            }
        }
        _mirror.IngestTransactions(transactions);
    }

    [Fact]
    public void GroupByPerson_RollsAlicesTwoCredentialsIntoOneBucket()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            relative_window: "last_24h");

        var groups = ParseGroups(json);

        // Alice has 6 events from her two credentials. The rollup must
        // collapse them into a single bucket — the whole point of the join.
        var alice = groups.Single(g => g.KeyId == AlicePersonId);
        Assert.Equal(6, alice.Count);
        Assert.Equal("Alice Anderson", alice.Key);
    }

    [Fact]
    public void GroupByPerson_KeyIdIsHostUserIdNotCredentialId()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            relative_window: "last_24h");

        var groups = ParseGroups(json);
        // Alice's bucket key_id must be the HostUserId (100), not a credential
        // id (5001 or 5002). This is the exact bug we shipped a fix for.
        Assert.Contains(groups, g => g.KeyId == AlicePersonId);
        Assert.DoesNotContain(groups, g => g.KeyId == AliceCredA);
        Assert.DoesNotContain(groups, g => g.KeyId == AliceCredB);
    }

    [Fact]
    public void GroupByPerson_OrphanCredentialFallsBackToUid1Name()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            relative_window: "last_24h");

        var groups = ParseGroups(json);
        // Orphan credentials (uid1 not in dim_user_credentials) get their own
        // bucket keyed on the denormalized uid1_name. key_id is null because
        // we have no HostUserId to report.
        var orphan = groups.Single(g => g.Key == "Ghost Walker");
        Assert.Null(orphan.KeyId);
        Assert.Equal(1, orphan.Count);
    }

    [Fact]
    public void GroupByPerson_ReturnsExactlyThreeBuckets()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            relative_window: "last_24h");

        var doc = JsonDocument.Parse(json);
        // Alice (rolled up) + Bob (single credential) + Orphan = 3 buckets.
        // Total events = 6 + 2 + 1 = 9.
        Assert.Equal(3, doc.RootElement.GetProperty("total_groups").GetInt64());
        Assert.Equal(9, doc.RootElement.GetProperty("total_events").GetInt64());
    }

    [Fact]
    public void GroupByCredential_KeepsAlicesTwoBadgesSeparate()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "credential",
            relative_window: "last_24h");

        var groups = ParseGroups(json);
        // Now we expect 4 distinct buckets: Alice's two badges + Bob's badge + the orphan.
        // Each of Alice's badges has 3 events.
        Assert.Equal(4, groups.Count);
        Assert.Single(groups, g => g.KeyId == AliceCredA && g.Count == 3);
        Assert.Single(groups, g => g.KeyId == AliceCredB && g.Count == 3);
    }

    [Fact]
    public void GroupByCredential_KeyIdIsCredentialIdNotPersonId()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "credential",
            relative_window: "last_24h");

        var groups = ParseGroups(json);
        // The whole point of group_by=credential is that key_id reports the
        // raw credential — never the rolled-up HostUserId.
        Assert.DoesNotContain(groups, g => g.KeyId == AlicePersonId);
        Assert.DoesNotContain(groups, g => g.KeyId == BobPersonId);
        Assert.Contains(groups, g => g.KeyId == AliceCredA);
        Assert.Contains(groups, g => g.KeyId == BobCred);
    }

    [Fact]
    public void RelativeWindowLast7d_ResolvesToSevenDayWindow()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person",
            relative_window: "last_7d");

        using var doc = JsonDocument.Parse(json);
        var window = doc.RootElement.GetProperty("window_used");
        var since = DateTime.Parse(window.GetProperty("since").GetString()!);
        var until = DateTime.Parse(window.GetProperty("until").GetString()!);

        Assert.InRange((until - since).TotalDays, 6.99, 7.01);
        Assert.Equal("last_7d", window.GetProperty("relative_window").GetString());
    }

    [Fact]
    public void Default_NoTimeWindow_DefaultsTo24Hours()
    {
        var json = AggregateEventsTool.AggregateEvents(
            _mirror,
            group_by: "person");

        using var doc = JsonDocument.Parse(json);
        var window = doc.RootElement.GetProperty("window_used");
        var since = DateTime.Parse(window.GetProperty("since").GetString()!);
        var until = DateTime.Parse(window.GetProperty("until").GetString()!);

        Assert.InRange((until - since).TotalHours, 23.99, 24.01);
        Assert.True(window.GetProperty("defaulted_since").GetBoolean());
        Assert.True(window.GetProperty("defaulted_until").GetBoolean());
    }

    [Fact]
    public void UnsupportedGroupBy_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            AggregateEventsTool.AggregateEvents(
                _mirror,
                group_by: "not_a_real_dimension",
                relative_window: "last_24h"));
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
        // Mirror has a long-lived connection — no Dispose. File lock releases
        // on process exit; cleanup is best-effort.
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
