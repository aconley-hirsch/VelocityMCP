using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;

namespace VelocityMCP.Tests;

/// <summary>
/// Layer 4 PII guarantee tests. The happy-path test (`Schema_creates_tables_and_passes_pii_assertion`
/// in EndToEndTests) only proves AssertNoPiiColumns doesn't throw on a clean schema.
/// These tests prove it actually FIRES when the schema gets contaminated — without
/// that assurance, a refactor that silently turns the guard into a no-op would
/// disable our last line of PII defense without a single red test.
/// </summary>
public class PiiAssertionTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;

    public PiiAssertionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
    }

    [Fact]
    public void Throws_when_pin_column_added_to_fact_transactions()
    {
        // Bring up the normal schema first.
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        // Then contaminate it: ALTER fact_transactions to add a pin column.
        // This simulates a refactor accident where someone re-introduces PII.
        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "ALTER TABLE fact_transactions ADD COLUMN pin VARCHAR";
            cmd.ExecuteNonQuery();
        }

        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        var ex = Assert.Throws<InvalidOperationException>(() => schema.AssertNoPiiColumns());

        Assert.Contains("Prohibited PII column 'pin'", ex.Message);
        Assert.Contains("fact_transactions", ex.Message);
    }

    [Fact]
    public void Throws_when_code_column_added_to_fact_alarms()
    {
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "ALTER TABLE fact_alarms ADD COLUMN code VARCHAR";
            cmd.ExecuteNonQuery();
        }

        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        var ex = Assert.Throws<InvalidOperationException>(() => schema.AssertNoPiiColumns());

        Assert.Contains("Prohibited PII column 'code'", ex.Message);
        Assert.Contains("fact_alarms", ex.Message);
    }

    [Fact]
    public void Throws_when_pin_column_added_to_fact_software_events()
    {
        // fact_software_events is the newest fact table — confirms the guard
        // covers it (we explicitly added it to the assertion's table list).
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "ALTER TABLE fact_software_events ADD COLUMN pin VARCHAR";
            cmd.ExecuteNonQuery();
        }

        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        var ex = Assert.Throws<InvalidOperationException>(() => schema.AssertNoPiiColumns());

        Assert.Contains("fact_software_events", ex.Message);
    }

    [Fact]
    public void Case_insensitive_match_on_prohibited_column_names()
    {
        // The ProhibitedColumns set uses StringComparer.OrdinalIgnoreCase, so
        // 'PIN' and 'Pin' should be caught the same as 'pin'.
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            // DuckDB lowercases identifiers by default unless quoted, so we
            // quote to preserve the uppercase form we want to test.
            cmd.CommandText = "ALTER TABLE fact_transactions ADD COLUMN \"PIN\" VARCHAR";
            cmd.ExecuteNonQuery();
        }

        var schema = new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance);
        Assert.Throws<InvalidOperationException>(() => schema.AssertNoPiiColumns());
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
