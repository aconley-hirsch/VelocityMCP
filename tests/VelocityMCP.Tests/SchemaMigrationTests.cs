using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using VelocityMCP.Data;

namespace VelocityMCP.Tests;

/// <summary>
/// Pins the upgrade path for dim_user_credentials. The schema gained four
/// new columns (activation_date, expiration_date, is_activated, expiration_used)
/// after the initial release. MigrateDimensionTables uses ALTER TABLE ADD
/// COLUMN IF NOT EXISTS so customers with existing duckdb files don't have
/// to delete and re-ingest. These tests guarantee that:
///
///   1. Opening an old-shape file actually adds the new columns.
///   2. Existing rows survive the migration.
///   3. The migration is idempotent — running it twice is a no-op.
///
/// Without these, a refactor that silently breaks the migration would
/// destroy customer data on next start without raising any flag.
/// </summary>
public class SchemaMigrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connString;

    public SchemaMigrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        _connString = $"Data Source={_dbPath}";
    }

    [Fact]
    public void Migrate_adds_missing_columns_to_existing_dim_user_credentials()
    {
        // 1. Build an old-shape dim_user_credentials by hand — the same DDL
        //    we shipped in the initial schema, before activation/expiration
        //    columns were added.
        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE dim_user_credentials (
                    credential_id   INTEGER PRIMARY KEY,
                    person_id       INTEGER NOT NULL,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO dim_user_credentials (credential_id, person_id, updated_at)
                VALUES (5001, 100, CURRENT_TIMESTAMP), (5002, 200, CURRENT_TIMESTAMP);
                """;
            cmd.ExecuteNonQuery();
        }

        // 2. Run the schema bring-up — should ALTER the existing table to add
        //    the four new columns without dropping the existing rows.
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        // 3. Verify the new columns exist.
        var columns = GetColumnNames("dim_user_credentials");
        Assert.Contains("activation_date", columns);
        Assert.Contains("expiration_date", columns);
        Assert.Contains("is_activated", columns);
        Assert.Contains("expiration_used", columns);

        // 4. Verify the original rows are still present (migration didn't drop the table).
        Assert.Equal(2, RowCount("dim_user_credentials"));
        Assert.Equal(2, ScalarLong(
            "SELECT COUNT(*) FROM dim_user_credentials WHERE credential_id IN (5001, 5002)"));

        // 5. Verify the new columns are NULL on the migrated rows (no default backfill).
        Assert.Equal(2, ScalarLong(
            "SELECT COUNT(*) FROM dim_user_credentials WHERE expiration_date IS NULL"));
    }

    [Fact]
    public void Migrate_is_idempotent_when_columns_already_exist()
    {
        // 1. Run the schema bring-up once on an empty file — creates the table
        //    in its current shape with all four date columns.
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        // Insert a row using the new shape.
        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO dim_user_credentials
                    (credential_id, person_id, activation_date, expiration_date, is_activated, expiration_used, updated_at)
                VALUES
                    (5001, 100, '2025-01-01 00:00:00', '2026-01-01 00:00:00', TRUE, TRUE, CURRENT_TIMESTAMP);
                """;
            cmd.ExecuteNonQuery();
        }

        // 2. Run EnsureCreated AGAIN — must be a no-op, must not drop or
        //    recreate the table, must not lose the row.
        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        // 3. Verify the row is still there with all its data intact.
        Assert.Equal(1, RowCount("dim_user_credentials"));
        Assert.Equal(1, ScalarLong(
            "SELECT COUNT(*) FROM dim_user_credentials WHERE credential_id = 5001 AND expiration_date IS NOT NULL"));
    }

    [Fact]
    public void Existing_data_works_after_migration_with_new_writes()
    {
        // Build the old shape, add some data, run the migration, then write
        // new-shape data via UpsertUserCredentials — the new mirror code
        // must coexist with pre-migration rows.
        using (var conn = new DuckDBConnection(_connString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE dim_user_credentials (
                    credential_id   INTEGER PRIMARY KEY,
                    person_id       INTEGER NOT NULL,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO dim_user_credentials (credential_id, person_id, updated_at)
                VALUES (5001, 100, CURRENT_TIMESTAMP);
                """;
            cmd.ExecuteNonQuery();
        }

        new DuckDbSchema(_connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        using var mirror = new DuckDbMirror(_connString, NullLogger<DuckDbMirror>.Instance);
        // UpsertUserCredentials uses DELETE + INSERT (replace-all), so the
        // pre-migration row gets replaced. New row writes the date columns.
        mirror.UpsertUserCredentials(new List<Data.Models.UserCredentialRecord>
        {
            new()
            {
                CredentialId = 5002,
                PersonId = 200,
                ActivationDate = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                ExpirationDate = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                IsActivated = true,
                ExpirationUsed = true,
            }
        });

        Assert.Equal(1, RowCount("dim_user_credentials"));
        Assert.Equal(1, ScalarLong(
            "SELECT COUNT(*) FROM dim_user_credentials WHERE expiration_date IS NOT NULL"));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private HashSet<string> GetColumnNames(string table)
    {
        using var conn = new DuckDBConnection(_connString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'";
        using var reader = cmd.ExecuteReader();
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (reader.Read()) names.Add(reader.GetString(0));
        return names;
    }

    private long RowCount(string table) => ScalarLong($"SELECT COUNT(*) FROM {table}");

    private long ScalarLong(string sql)
    {
        using var conn = new DuckDBConnection(_connString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
