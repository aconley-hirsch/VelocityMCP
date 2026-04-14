using System.Data;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace VelocityMCP.Data;

/// <summary>
/// Creates and validates the DuckDB analytical mirror schema.
/// Enforces PII exclusion at the structural level (Layer 1 + Layer 4 defense).
/// </summary>
public sealed class DuckDbSchema
{
    private readonly string _connectionString;
    private readonly ILogger<DuckDbSchema> _logger;

    /// <summary>
    /// Column names that must never appear in any fact table.
    /// Layer 4 of PII defense: startup assertion fails the process if found.
    /// </summary>
    private static readonly HashSet<string> ProhibitedColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "pin", "code"
    };

    public DuckDbSchema(string connectionString, ILogger<DuckDbSchema> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public void EnsureCreated()
    {
        using var conn = new DuckDBConnection(_connectionString);
        conn.Open();

        CreateFactTransactions(conn);
        CreateFactAlarms(conn);
        CreateMetaTables(conn);
        CreateDimensionTables(conn);
        CreateLookupTables(conn);
        SeedLookupTables(conn);

        _logger.LogInformation("DuckDB schema initialized");
    }

    /// <summary>
    /// Layer 4 PII defense: inspects every fact table for prohibited column names.
    /// Fails fast with an exception if PIN or CODE columns are found.
    /// Must be called at startup before any ingest runs.
    /// </summary>
    public void AssertNoPiiColumns()
    {
        using var conn = new DuckDBConnection(_connectionString);
        conn.Open();

        string[] factTables = ["fact_transactions", "fact_alarms"];

        foreach (var table in factTables)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'";
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var colName = reader.GetString(0);
                if (ProhibitedColumns.Contains(colName))
                {
                    var msg = $"FATAL: Prohibited PII column '{colName}' found in table '{table}'. " +
                              "The DuckDB mirror must never contain PIN or CODE data. " +
                              "Delete the .duckdb file and fix the schema before restarting.";
                    _logger.LogCritical(msg);
                    throw new InvalidOperationException(msg);
                }
            }
        }

        _logger.LogInformation("PII column assertion passed — no prohibited columns found");
    }

    private static void CreateFactTransactions(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS fact_transactions (
                log_id              INTEGER PRIMARY KEY,
                dt_date             TIMESTAMP NOT NULL,
                pc_date_time        TIMESTAMP,
                event_code          INTEGER NOT NULL,
                description         VARCHAR,
                disposition         INTEGER NOT NULL,
                transaction_type    INTEGER,
                report_as_alarm     BOOLEAN,
                alarm_level_priority INTEGER,
                port_addr           VARCHAR,
                dt_addr             VARCHAR,
                x_addr              VARCHAR,
                net_address         VARCHAR,
                door_or_expansion   SMALLINT,
                reader              SMALLINT,
                reader_name         VARCHAR,
                from_zone           SMALLINT,
                to_zone             SMALLINT,
                uid1                INTEGER,
                uid1_name           VARCHAR,
                uid2                INTEGER,
                uid2_name           VARCHAR,
                server_id           INTEGER,
                security_domain_id  INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_tx_dt_date  ON fact_transactions(dt_date);
            CREATE INDEX IF NOT EXISTS idx_tx_reader   ON fact_transactions(reader_name);
            CREATE INDEX IF NOT EXISTS idx_tx_uid1     ON fact_transactions(uid1);
            CREATE INDEX IF NOT EXISTS idx_tx_event    ON fact_transactions(event_code);
            """;
        cmd.ExecuteNonQuery();
    }

    private static void CreateFactAlarms(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS fact_alarms (
                alarm_id            INTEGER PRIMARY KEY,
                dt_date             TIMESTAMP,
                db_date             TIMESTAMP,
                ak_date             TIMESTAMP,
                cl_date             TIMESTAMP,
                event_id            INTEGER NOT NULL,
                alarm_level_priority INTEGER,
                status              SMALLINT,
                description         VARCHAR,
                port_addr           INTEGER,
                dt_addr             INTEGER,
                x_addr              INTEGER,
                net_address         VARCHAR,
                ak_operator         VARCHAR,
                cl_operator         VARCHAR,
                workstation_name    VARCHAR,
                uid1                DOUBLE,
                uid1_name           VARCHAR,
                uid2                DOUBLE,
                uid2_name           VARCHAR,
                parm1               VARCHAR,
                parm2               VARCHAR,
                transaction_type    INTEGER,
                server_id           INTEGER,
                site_id             INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_al_dt_date   ON fact_alarms(dt_date);
            CREATE INDEX IF NOT EXISTS idx_al_event_id  ON fact_alarms(event_id);
            CREATE INDEX IF NOT EXISTS idx_al_uid1      ON fact_alarms(uid1);
            """;
        cmd.ExecuteNonQuery();
    }

    private static void CreateMetaTables(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS meta_ingest_cursors (
                source_table    VARCHAR PRIMARY KEY,
                last_dt_date    TIMESTAMP NOT NULL,
                last_run_at     TIMESTAMP NOT NULL,
                rows_ingested   BIGINT NOT NULL DEFAULT 0,
                last_error      VARCHAR
            );

            CREATE TABLE IF NOT EXISTS meta_schema_version (
                version         INTEGER PRIMARY KEY,
                applied_at      TIMESTAMP NOT NULL,
                description     VARCHAR
            );

            INSERT OR IGNORE INTO meta_schema_version VALUES (1, CURRENT_TIMESTAMP, 'Initial schema — fact_transactions, fact_alarms, meta tables');
            """;
        cmd.ExecuteNonQuery();
    }

    private static void CreateLookupTables(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS lookup_event_types (
                event_code      INTEGER PRIMARY KEY,
                category        VARCHAR NOT NULL,
                name            VARCHAR NOT NULL,
                description     VARCHAR
            );

            CREATE TABLE IF NOT EXISTS lookup_dispositions (
                disposition     INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL
            );

            -- Alarm categories are a discovery catalog for the LLM so it can map
            -- user words like "duress" or "tamper" to a category name. Future phases
            -- will link fact_alarms rows to a category via policy file or SDK load.
            CREATE TABLE IF NOT EXISTS lookup_alarm_categories (
                category_id     INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL,
                description     VARCHAR
            );
            """;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Seeds lookup tables with initial values. Safe to call repeatedly (INSERT OR IGNORE).
    /// For the MVP these mirror the fake data event types; later phases will load from
    /// a YAML policy file and/or reconcile against live Velocity data.
    /// </summary>
    private static void SeedLookupTables(DuckDBConnection conn)
    {
        var eventTypes = new (int Code, string Category, string Name, string Description)[]
        {
            (1, "transaction", "Access Granted", "Credential accepted at a reader"),
            (2, "transaction", "Access Denied - Invalid Credential", "Credential not recognized by the reader"),
            (3, "transaction", "Access Denied - Invalid PIN", "Wrong PIN entered at a PIN reader"),
            (4, "alarm",       "Door Forced Open", "Door opened without a valid credential"),
            (5, "alarm",       "Door Held Open", "Door held open past its configured timer"),
            (6, "external",    "Request to Exit", "Egress motion sensor or button triggered"),
            (7, "external",    "Door Locked", "Door lock engaged"),
            (8, "external",    "Door Unlocked", "Door lock released"),
        };

        using var tx = conn.BeginTransaction();
        foreach (var et in eventTypes)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT OR IGNORE INTO lookup_event_types (event_code, category, name, description)
                VALUES ($code, $category, $name, $description)
                """;
            cmd.Parameters.Add(new DuckDBParameter("code", et.Code));
            cmd.Parameters.Add(new DuckDBParameter("category", et.Category));
            cmd.Parameters.Add(new DuckDBParameter("name", et.Name));
            cmd.Parameters.Add(new DuckDBParameter("description", et.Description));
            cmd.ExecuteNonQuery();
        }

        var dispositions = new (int Code, string Name)[]
        {
            (0, "None"),
            (1, "Granted"),
            (2, "Denied - Invalid Credential"),
            (3, "Denied - Invalid PIN"),
        };

        foreach (var d in dispositions)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT OR IGNORE INTO lookup_dispositions (disposition, name)
                VALUES ($code, $name)
                """;
            cmd.Parameters.Add(new DuckDBParameter("code", d.Code));
            cmd.Parameters.Add(new DuckDBParameter("name", d.Name));
            cmd.ExecuteNonQuery();
        }

        var alarmCategories = new (int Id, string Name, string Description)[]
        {
            (1, "Access",   "Access control alarms — forced open, held open, invalid credential at a reader"),
            (2, "Security", "Intrusion and perimeter alarms — zone faults, motion, glass-break"),
            (3, "Tamper",   "Enclosure, reader, or device tamper alarms"),
            (4, "Duress",   "Duress code entered at a reader or panic button"),
            (5, "System",   "Controller/panel/workstation health, communication loss, power"),
            (6, "Fire",     "Fire panel integration alarms"),
        };

        foreach (var c in alarmCategories)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT OR IGNORE INTO lookup_alarm_categories (category_id, name, description)
                VALUES ($id, $name, $description)
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", c.Id));
            cmd.Parameters.Add(new DuckDBParameter("name", c.Name));
            cmd.Parameters.Add(new DuckDBParameter("description", c.Description));
            cmd.ExecuteNonQuery();
        }

        tx.Commit();
    }

    private static void CreateDimensionTables(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS dim_doors (
                door_id         INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL,
                controller_addr VARCHAR,
                active          BOOLEAN DEFAULT TRUE,
                last_seen_at    TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- dim_readers.door_id is the link from reader → door (many-to-one).
            -- Matches real Velocity where a door has 1..N readers (entry/exit).
            CREATE TABLE IF NOT EXISTS dim_readers (
                reader_id       INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL,
                door_id         INTEGER,
                from_zone       INTEGER,
                to_zone         INTEGER,
                active          BOOLEAN DEFAULT TRUE,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_readers_door ON dim_readers(door_id);

            CREATE TABLE IF NOT EXISTS dim_people (
                person_id       INTEGER PRIMARY KEY,
                first_name      VARCHAR,
                last_name       VARCHAR,
                full_name       VARCHAR,
                active          BOOLEAN DEFAULT TRUE,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Credential → person mapping. Velocity's fact_transactions.uid1 is
            -- a CredentialId, not a HostUserId. Tools that want "events for
            -- person X" have to expand person → list of credentials first.
            CREATE TABLE IF NOT EXISTS dim_user_credentials (
                credential_id   INTEGER PRIMARY KEY,
                person_id       INTEGER NOT NULL,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_creds_person ON dim_user_credentials(person_id);

            -- Authorization / policy dimension.
            -- dim_clearances: named groupings (Velocity calls them "access levels")
            --   schedule_name is informational in v1 (e.g. "24x7", "Business Hours 8-18 M-F");
            --   proper time-schedule enforcement is deferred to when real Velocity is wired up.
            CREATE TABLE IF NOT EXISTS dim_clearances (
                clearance_id    INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL,
                schedule_name   VARCHAR,
                active          BOOLEAN DEFAULT TRUE,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Which readers a clearance grants access to.
            CREATE TABLE IF NOT EXISTS dim_reader_clearances (
                reader_id       INTEGER NOT NULL,
                clearance_id    INTEGER NOT NULL,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (reader_id, clearance_id)
            );
            CREATE INDEX IF NOT EXISTS idx_rc_clearance ON dim_reader_clearances(clearance_id);

            -- Which clearances a person currently holds.
            -- expires_at NULL means indefinite. Filter on (expires_at IS NULL OR expires_at > now())
            -- to get "currently active" assignments.
            CREATE TABLE IF NOT EXISTS dim_person_clearances (
                person_id       INTEGER NOT NULL,
                clearance_id    INTEGER NOT NULL,
                granted_at      TIMESTAMP NOT NULL,
                expires_at      TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (person_id, clearance_id)
            );
            CREATE INDEX IF NOT EXISTS idx_pc_clearance ON dim_person_clearances(clearance_id);
            """;
        cmd.ExecuteNonQuery();
    }
}
