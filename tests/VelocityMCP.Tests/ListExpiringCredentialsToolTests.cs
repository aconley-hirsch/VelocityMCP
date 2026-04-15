using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;
using VelocityMCP.Data.Models;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Locks in the include_expired / include_perpetual / person_id branches.
/// Hand-built fixture so each row's expiration date is known relative to "now".
/// </summary>
public class ListExpiringCredentialsToolTests : IDisposable
{
    private const int AlicePersonId = 100;
    private const int BobPersonId = 200;
    private const int CarolPersonId = 300;
    private const int DavidPersonId = 400;

    private const int CredAliceExpired = 5001;       // expired 30 days ago
    private const int CredAliceSoon = 5002;          // expires in 5 days
    private const int CredBobLater = 5003;           // expires in 60 days (outside default window)
    private const int CredCarolPerpetual = 5004;     // no expiration date
    private const int CredDavidExpiringEdge = 5005;  // expires in 28 days (inside default 30d)

    private readonly string _dbPath;
    private readonly string _connString;
    private readonly DuckDbMirror _mirror;

    public ListExpiringCredentialsToolTests()
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
            new() { PersonId = CarolPersonId, FirstName = "Carol", LastName = "Chen" },
            new() { PersonId = DavidPersonId, FirstName = "David", LastName = "Davis" },
        });

        var now = DateTime.UtcNow;
        _mirror.UpsertUserCredentials(new List<UserCredentialRecord>
        {
            new()
            {
                CredentialId = CredAliceExpired,
                PersonId = AlicePersonId,
                ActivationDate = now.AddYears(-1),
                ExpirationDate = now.AddDays(-30),    // expired
                IsActivated = true,
                ExpirationUsed = true,
            },
            new()
            {
                CredentialId = CredAliceSoon,
                PersonId = AlicePersonId,
                ActivationDate = now.AddDays(-30),
                ExpirationDate = now.AddDays(5),      // expiring in 5 days
                IsActivated = true,
                ExpirationUsed = true,
            },
            new()
            {
                CredentialId = CredBobLater,
                PersonId = BobPersonId,
                ActivationDate = now.AddDays(-30),
                ExpirationDate = now.AddDays(60),     // outside default 30d window
                IsActivated = true,
                ExpirationUsed = true,
            },
            new()
            {
                CredentialId = CredCarolPerpetual,
                PersonId = CarolPersonId,
                ActivationDate = now.AddDays(-30),
                ExpirationDate = null,                // perpetual
                IsActivated = true,
                ExpirationUsed = false,
            },
            new()
            {
                CredentialId = CredDavidExpiringEdge,
                PersonId = DavidPersonId,
                ActivationDate = now.AddDays(-30),
                ExpirationDate = now.AddDays(28),     // inside default 30d window
                IsActivated = true,
                ExpirationUsed = true,
            },
        });
    }

    [Fact]
    public void Default_ReturnsOnlyExpiringWithinThirtyDayWindow()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(_mirror);
        var creds = ParseCredentials(json);

        // Alice (5 days) and David (28 days) — both inside default 30 days.
        // Bob (60 days), Carol (perpetual), Alice's expired (-30 days) all excluded.
        var ids = creds.Select(c => c.CredentialId).ToHashSet();
        Assert.Contains(CredAliceSoon, ids);
        Assert.Contains(CredDavidExpiringEdge, ids);
        Assert.DoesNotContain(CredBobLater, ids);
        Assert.DoesNotContain(CredCarolPerpetual, ids);
        Assert.DoesNotContain(CredAliceExpired, ids);
    }

    [Fact]
    public void IncludeExpired_AlsoReturnsExpiredCredentials()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_expired: true);
        var creds = ParseCredentials(json);

        var ids = creds.Select(c => c.CredentialId).ToHashSet();
        Assert.Contains(CredAliceExpired, ids);  // now included
        Assert.Contains(CredAliceSoon, ids);
        Assert.Contains(CredDavidExpiringEdge, ids);
        Assert.DoesNotContain(CredCarolPerpetual, ids);  // still not perpetual
    }

    [Fact]
    public void IncludePerpetual_AlsoReturnsCredentialsWithNullExpiration()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_perpetual: true);
        var creds = ParseCredentials(json);

        var ids = creds.Select(c => c.CredentialId).ToHashSet();
        Assert.Contains(CredCarolPerpetual, ids);
        // Perpetual credentials sort last and have null expiration_date / null days_until_expiry.
        var perpetual = creds.Single(c => c.CredentialId == CredCarolPerpetual);
        Assert.Null(perpetual.ExpirationDate);
        Assert.Null(perpetual.DaysUntilExpiry);
        Assert.Equal("perpetual", perpetual.Status);
    }

    [Fact]
    public void IncludeBoth_ReturnsExpiringExpiredAndPerpetual()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_expired: true, include_perpetual: true);
        var creds = ParseCredentials(json);

        // All 5 seeded credentials are reachable except Bob (60 days, outside window).
        var ids = creds.Select(c => c.CredentialId).ToHashSet();
        Assert.Equal(4, creds.Count);
        Assert.Contains(CredAliceExpired, ids);
        Assert.Contains(CredAliceSoon, ids);
        Assert.Contains(CredCarolPerpetual, ids);
        Assert.Contains(CredDavidExpiringEdge, ids);
        Assert.DoesNotContain(CredBobLater, ids);
    }

    [Fact]
    public void PersonIdFilter_ScopesToOnePerson()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_expired: true, person_id: AlicePersonId);
        var creds = ParseCredentials(json);

        // Alice has two credentials (expired + expiring soon) — both should appear,
        // and nothing else.
        Assert.Equal(2, creds.Count);
        Assert.All(creds, c => Assert.Equal(AlicePersonId, c.PersonId));
    }

    [Fact]
    public void StatusField_DerivesCorrectlyForEachBucket()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_expired: true, include_perpetual: true, within_days: 30);
        var creds = ParseCredentials(json);

        Assert.Equal("expired", creds.Single(c => c.CredentialId == CredAliceExpired).Status);
        Assert.Equal("expiring_soon", creds.Single(c => c.CredentialId == CredAliceSoon).Status);
        Assert.Equal("expiring_later", creds.Single(c => c.CredentialId == CredDavidExpiringEdge).Status);
        Assert.Equal("perpetual", creds.Single(c => c.CredentialId == CredCarolPerpetual).Status);
    }

    [Fact]
    public void DaysUntilExpiry_NegativeForExpiredCredentials()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_expired: true);
        var creds = ParseCredentials(json);

        var expired = creds.Single(c => c.CredentialId == CredAliceExpired);
        Assert.NotNull(expired.DaysUntilExpiry);
        Assert.True(expired.DaysUntilExpiry < 0,
            $"expected negative days for expired credential, got {expired.DaysUntilExpiry}");
    }

    [Fact]
    public void PersonNameResolved_FromDimPeople()
    {
        var json = ListExpiringCredentialsTool.ListExpiringCredentials(
            _mirror, include_expired: true, person_id: AlicePersonId);
        var creds = ParseCredentials(json);

        Assert.All(creds, c => Assert.Equal("Alice Anderson", c.PersonName));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private record CredentialRow(
        int CredentialId,
        long PersonId,
        string? PersonName,
        DateTime? ExpirationDate,
        int? DaysUntilExpiry,
        string Status);

    private static List<CredentialRow> ParseCredentials(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("credentials")
            .EnumerateArray()
            .Select(c => new CredentialRow(
                CredentialId: c.GetProperty("credential_id").GetInt32(),
                PersonId: c.GetProperty("person_id").GetInt64(),
                PersonName: c.GetProperty("person_name").GetString(),
                ExpirationDate: c.GetProperty("expiration_date").ValueKind == JsonValueKind.Null
                    ? null
                    : DateTime.Parse(c.GetProperty("expiration_date").GetString()!),
                DaysUntilExpiry: c.GetProperty("days_until_expiry").ValueKind == JsonValueKind.Null
                    ? null
                    : c.GetProperty("days_until_expiry").GetInt32(),
                Status: c.GetProperty("status").GetString()!))
            .ToList();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
