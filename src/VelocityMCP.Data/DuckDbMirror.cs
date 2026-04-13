using System.Data;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using VelocityMCP.Data.Models;

namespace VelocityMCP.Data;

/// <summary>
/// Read/write access to the DuckDB analytical mirror.
/// Ingest methods use parameterized INSERT with ON CONFLICT for de-duplication.
/// Query methods are used by MCP tool handlers.
/// </summary>
public sealed class DuckDbMirror : IDisposable
{
    private readonly DuckDBConnection _conn;
    private readonly ILogger<DuckDbMirror> _logger;

    public DuckDbMirror(string connectionString, ILogger<DuckDbMirror> logger)
    {
        _conn = new DuckDBConnection(connectionString);
        _conn.Open();
        _logger = logger;
    }

    // ── Ingest ──────────────────────────────────────────────────────────

    public void IngestTransactions(IReadOnlyList<TransactionRecord> records)
    {
        if (records.Count == 0) return;

        using var tx = _conn.BeginTransaction();
        try
        {
            foreach (var r in records)
            {
                using var cmd = _conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    INSERT OR IGNORE INTO fact_transactions (
                        log_id, dt_date, pc_date_time, event_code, description,
                        disposition, transaction_type, report_as_alarm, alarm_level_priority,
                        port_addr, dt_addr, x_addr, net_address,
                        door_or_expansion, reader, reader_name,
                        from_zone, to_zone,
                        uid1, uid1_name, uid2, uid2_name,
                        server_id, security_domain_id
                    ) VALUES (
                        $log_id, $dt_date, $pc_date_time, $event_code, $description,
                        $disposition, $transaction_type, $report_as_alarm, $alarm_level_priority,
                        $port_addr, $dt_addr, $x_addr, $net_address,
                        $door_or_expansion, $reader, $reader_name,
                        $from_zone, $to_zone,
                        $uid1, $uid1_name, $uid2, $uid2_name,
                        $server_id, $security_domain_id
                    )
                    """;
                cmd.Parameters.Add(new DuckDBParameter("log_id", r.LogId));
                cmd.Parameters.Add(new DuckDBParameter("dt_date", r.DtDate));
                cmd.Parameters.Add(new DuckDBParameter("pc_date_time", (object?)r.PcDateTime ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("event_code", r.EventCode));
                cmd.Parameters.Add(new DuckDBParameter("description", (object?)r.Description ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("disposition", r.Disposition));
                cmd.Parameters.Add(new DuckDBParameter("transaction_type", r.TransactionType));
                cmd.Parameters.Add(new DuckDBParameter("report_as_alarm", r.ReportAsAlarm));
                cmd.Parameters.Add(new DuckDBParameter("alarm_level_priority", r.AlarmLevelPriority));
                cmd.Parameters.Add(new DuckDBParameter("port_addr", (object?)r.PortAddr ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("dt_addr", (object?)r.DtAddr ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("x_addr", (object?)r.XAddr ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("net_address", (object?)r.NetAddress ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("door_or_expansion", (short)r.DoorOrExpansion));
                cmd.Parameters.Add(new DuckDBParameter("reader", (short)r.Reader));
                cmd.Parameters.Add(new DuckDBParameter("reader_name", (object?)r.ReaderName ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("from_zone", (object?)r.FromZone ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("to_zone", (object?)r.ToZone ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid1", r.Uid1));
                cmd.Parameters.Add(new DuckDBParameter("uid1_name", (object?)r.Uid1Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid2", r.Uid2));
                cmd.Parameters.Add(new DuckDBParameter("uid2_name", (object?)r.Uid2Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("server_id", r.ServerID));
                cmd.Parameters.Add(new DuckDBParameter("security_domain_id", r.SecurityDomainID));
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
            _logger.LogDebug("Ingested {Count} transactions", records.Count);
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    public void IngestAlarms(IReadOnlyList<AlarmRecord> records)
    {
        if (records.Count == 0) return;

        using var tx = _conn.BeginTransaction();
        try
        {
            foreach (var r in records)
            {
                using var cmd = _conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    INSERT OR IGNORE INTO fact_alarms (
                        alarm_id, dt_date, db_date, ak_date, cl_date,
                        event_id, alarm_level_priority, status, description,
                        port_addr, dt_addr, x_addr, net_address,
                        ak_operator, cl_operator, workstation_name,
                        uid1, uid1_name, uid2, uid2_name,
                        parm1, parm2, transaction_type, server_id, site_id
                    ) VALUES (
                        $alarm_id, $dt_date, $db_date, $ak_date, $cl_date,
                        $event_id, $alarm_level_priority, $status, $description,
                        $port_addr, $dt_addr, $x_addr, $net_address,
                        $ak_operator, $cl_operator, $workstation_name,
                        $uid1, $uid1_name, $uid2, $uid2_name,
                        $parm1, $parm2, $transaction_type, $server_id, $site_id
                    )
                    """;
                cmd.Parameters.Add(new DuckDBParameter("alarm_id", r.AlarmId));
                cmd.Parameters.Add(new DuckDBParameter("dt_date", (object?)r.DtDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("db_date", (object?)r.DbDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("ak_date", (object?)r.AkDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("cl_date", (object?)r.ClDate ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("event_id", r.EventId));
                cmd.Parameters.Add(new DuckDBParameter("alarm_level_priority", r.AlarmLevelPriority));
                cmd.Parameters.Add(new DuckDBParameter("status", (short)r.Status));
                cmd.Parameters.Add(new DuckDBParameter("description", (object?)r.Description ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("port_addr", r.PortAddr));
                cmd.Parameters.Add(new DuckDBParameter("dt_addr", r.DtAddr));
                cmd.Parameters.Add(new DuckDBParameter("x_addr", r.XAddr));
                cmd.Parameters.Add(new DuckDBParameter("net_address", (object?)r.NetAddress ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("ak_operator", (object?)r.AkOperator ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("cl_operator", (object?)r.ClOperator ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("workstation_name", (object?)r.WorkstationName ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid1", (object?)r.Uid1 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid1_name", (object?)r.Uid1Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid2", (object?)r.Uid2 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("uid2_name", (object?)r.Uid2Name ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("parm1", (object?)r.Parm1 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("parm2", (object?)r.Parm2 ?? DBNull.Value));
                cmd.Parameters.Add(new DuckDBParameter("transaction_type", r.TransactionType));
                cmd.Parameters.Add(new DuckDBParameter("server_id", r.ServerID));
                cmd.Parameters.Add(new DuckDBParameter("site_id", r.SiteID));
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
            _logger.LogDebug("Ingested {Count} alarms", records.Count);
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    // ── Cursor management ───────────────────────────────────────────────

    public DateTime? GetCursor(string sourceTable)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT last_dt_date FROM meta_ingest_cursors WHERE source_table = $table";
        cmd.Parameters.Add(new DuckDBParameter("table", sourceTable));
        var result = cmd.ExecuteScalar();
        return result is DateTime dt ? dt : null;
    }

    public void UpdateCursor(string sourceTable, DateTime lastDtDate, long rowsIngested, string? error = null)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO meta_ingest_cursors (source_table, last_dt_date, last_run_at, rows_ingested, last_error)
            VALUES ($table, $last_dt_date, $now, $rows, $error)
            ON CONFLICT (source_table) DO UPDATE SET
                last_dt_date = $last_dt_date,
                last_run_at = $now,
                rows_ingested = meta_ingest_cursors.rows_ingested + $rows,
                last_error = $error
            """;
        cmd.Parameters.Add(new DuckDBParameter("table", sourceTable));
        cmd.Parameters.Add(new DuckDBParameter("last_dt_date", lastDtDate));
        cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
        cmd.Parameters.Add(new DuckDBParameter("rows", rowsIngested));
        cmd.Parameters.Add(new DuckDBParameter("error", (object?)error ?? DBNull.Value));
        cmd.ExecuteNonQuery();
    }

    // ── Query helpers for tools ─────────────────────────────────────────

    public long CountTransactions(DateTime since, DateTime until)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM fact_transactions
            WHERE dt_date >= $since AND dt_date < $until
            """;
        cmd.Parameters.Add(new DuckDBParameter("since", since));
        cmd.Parameters.Add(new DuckDBParameter("until", until));
        return (long)cmd.ExecuteScalar()!;
    }

    public long CountTransactions(DateTime since, DateTime until, int? eventCode = null,
        string? readerName = null, IReadOnlyList<string>? readerNames = null,
        int? uid1 = null, int? disposition = null)
    {
        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, readerName, readerNames, uid1, disposition);

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
        foreach (var p in parameters) cmd.Parameters.Add(p);
        return (long)cmd.ExecuteScalar()!;
    }

    // ── Aggregation / samples ───────────────────────────────────────────

    /// <summary>
    /// Aggregate transactions grouped by a dimension. Returns top-N groups plus totals.
    /// Supported group_by values: "person", "door", "type", "hour", "day".
    /// </summary>
    public AggregationResult AggregateTransactions(
        string groupBy,
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        int? uid1, int? disposition,
        int limit)
    {
        var (keyExpr, keyIdExpr, groupKey) = groupBy.ToLowerInvariant() switch
        {
            "person" => ("uid1_name", "uid1", "uid1_name, uid1"),
            "door" or "reader" => ("reader_name", "NULL", "reader_name"),
            "type" or "event" or "event_code" => ("description", "event_code", "event_code, description"),
            "hour" => ("CAST(DATE_TRUNC('hour', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('hour', dt_date)"),
            "day" => ("CAST(DATE_TRUNC('day', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('day', dt_date)"),
            _ => throw new ArgumentException($"Unsupported group_by: {groupBy}. Use person, door, type, hour, or day.")
        };

        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, readerName, readerNames, uid1, disposition);

        // Total events matching the filter
        long totalEvents;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalEvents = (long)cmd.ExecuteScalar()!;
        }

        // Total distinct groups
        long totalGroups;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = groupBy.ToLowerInvariant() switch
            {
                "hour" => $"SELECT COUNT(DISTINCT DATE_TRUNC('hour', dt_date)) FROM fact_transactions {where}",
                "day" => $"SELECT COUNT(DISTINCT DATE_TRUNC('day', dt_date)) FROM fact_transactions {where}",
                "person" => $"SELECT COUNT(DISTINCT uid1) FROM fact_transactions {where}",
                "door" or "reader" => $"SELECT COUNT(DISTINCT reader_name) FROM fact_transactions {where}",
                _ => $"SELECT COUNT(DISTINCT event_code) FROM fact_transactions {where}"
            };
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalGroups = (long)cmd.ExecuteScalar()!;
        }

        // Top-N groups (time buckets order chronologically; others by count desc)
        var orderBy = groupBy.ToLowerInvariant() is "hour" or "day" ? "key ASC" : "count DESC";
        var groups = new List<AggregationBucket>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT {keyExpr} AS key, {keyIdExpr} AS key_id, COUNT(*) AS count
                FROM fact_transactions {where}
                GROUP BY {groupKey}
                ORDER BY {orderBy}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                groups.Add(new AggregationBucket(
                    reader.IsDBNull(0) ? null : reader.GetValue(0).ToString(),
                    reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1)),
                    reader.GetInt64(2)));
            }
        }

        return new AggregationResult(
            groupBy,
            groups,
            totalEvents,
            totalGroups,
            Truncated: totalGroups > limit);
    }

    /// <summary>
    /// Return a bounded sample of raw events matching filters. Order is time_asc or time_desc.
    /// </summary>
    public SampleResult SampleTransactions(
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        int? uid1, int? disposition,
        string order, int limit)
    {
        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, readerName, readerNames, uid1, disposition);
        var orderSql = order.Equals("time_asc", StringComparison.OrdinalIgnoreCase) ? "dt_date ASC" : "dt_date DESC";

        long totalMatching;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_transactions {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalMatching = (long)cmd.ExecuteScalar()!;
        }

        var rows = new List<SampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT log_id, dt_date, event_code, description, disposition,
                       reader_name, uid1, uid1_name
                FROM fact_transactions {where}
                ORDER BY {orderSql}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new SampleRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        return new SampleResult(rows, totalMatching, Truncated: totalMatching > limit);
    }

    internal static (string Where, List<DuckDBParameter> Parameters) BuildTransactionFilter(
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        int? uid1, int? disposition)
    {
        var where = "WHERE dt_date >= $since AND dt_date < $until";
        var parameters = new List<DuckDBParameter>
        {
            new("since", since),
            new("until", until)
        };

        if (eventCode.HasValue)
        {
            where += " AND event_code = $event_code";
            parameters.Add(new DuckDBParameter("event_code", eventCode.Value));
        }

        // Combine single + list into one IN clause when either is present.
        var allReaders = new List<string>();
        if (readerName != null) allReaders.Add(readerName);
        if (readerNames != null) allReaders.AddRange(readerNames);
        if (allReaders.Count == 1)
        {
            where += " AND reader_name = $reader_name_0";
            parameters.Add(new DuckDBParameter("reader_name_0", allReaders[0]));
        }
        else if (allReaders.Count > 1)
        {
            var names = allReaders.Select((_, i) => $"$reader_name_{i}").ToArray();
            where += $" AND reader_name IN ({string.Join(", ", names)})";
            for (int i = 0; i < allReaders.Count; i++)
                parameters.Add(new DuckDBParameter($"reader_name_{i}", allReaders[i]));
        }

        if (uid1.HasValue)
        {
            where += " AND uid1 = $uid1";
            parameters.Add(new DuckDBParameter("uid1", uid1.Value));
        }
        if (disposition.HasValue)
        {
            where += " AND disposition = $disposition";
            parameters.Add(new DuckDBParameter("disposition", disposition.Value));
        }

        return (where, parameters);
    }

    // ── Dimension upserts ───────────────────────────────────────────────

    public void UpsertDoors(IReadOnlyList<Models.DoorRecord> doors)
    {
        if (doors.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var d in doors)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_doors (door_id, name, controller_addr, active, updated_at)
                VALUES ($id, $name, $addr, TRUE, $now)
                ON CONFLICT (door_id) DO UPDATE SET
                    name = $name,
                    controller_addr = $addr,
                    updated_at = $now
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", d.Id));
            cmd.Parameters.Add(new DuckDBParameter("name", d.Name));
            cmd.Parameters.Add(new DuckDBParameter("addr", (object?)d.Address ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} doors", doors.Count);
    }

    public void UpsertReaders(IReadOnlyList<Models.ReaderRecord> readers)
    {
        if (readers.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var r in readers)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_readers (reader_id, name, door_id, from_zone, to_zone, active, updated_at)
                VALUES ($id, $name, $door_id, $from, $to, TRUE, $now)
                ON CONFLICT (reader_id) DO UPDATE SET
                    name = $name,
                    door_id = $door_id,
                    from_zone = $from,
                    to_zone = $to,
                    updated_at = $now
                """;
            cmd.Parameters.Add(new DuckDBParameter("id", r.Id));
            cmd.Parameters.Add(new DuckDBParameter("name", r.Name));
            cmd.Parameters.Add(new DuckDBParameter("door_id", (object?)r.DoorId ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("from", r.FromZone));
            cmd.Parameters.Add(new DuckDBParameter("to", r.ToZone));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} readers", readers.Count);
    }

    public void UpsertPeople(IReadOnlyList<Models.PersonRecord> people)
    {
        if (people.Count == 0) return;
        using var tx = _conn.BeginTransaction();
        foreach (var p in people)
        {
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dim_people (person_id, first_name, last_name, full_name, active, updated_at)
                VALUES ($id, $first, $last, $full, TRUE, $now)
                ON CONFLICT (person_id) DO UPDATE SET
                    first_name = $first,
                    last_name = $last,
                    full_name = $full,
                    updated_at = $now
                """;
            var fullName = $"{p.FirstName} {p.LastName}".Trim();
            cmd.Parameters.Add(new DuckDBParameter("id", p.PersonId));
            cmd.Parameters.Add(new DuckDBParameter("first", (object?)p.FirstName ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("last", (object?)p.LastName ?? DBNull.Value));
            cmd.Parameters.Add(new DuckDBParameter("full", fullName));
            cmd.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        _logger.LogInformation("Upserted {Count} people", people.Count);
    }

    // ── Dimension fuzzy search ──────────────────────────────────────────

    public List<DoorMatch> SearchDoors(string query, int limit)
    {
        // One row per (door, reader) via LEFT JOIN, then group in code so each door
        // carries its list of reader names. LEFT JOIN means doors with zero readers
        // still show up (shouldn't happen in practice, but be safe).
        var doorRows = new Dictionary<int, (string Name, string? ControllerAddr, List<string> Readers)>();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT d.door_id, d.name, d.controller_addr, r.name AS reader_name
            FROM dim_doors d
            LEFT JOIN dim_readers r ON r.door_id = d.door_id
            WHERE d.door_id IN (
                SELECT door_id FROM dim_doors
                WHERE name ILIKE $pattern
                ORDER BY LENGTH(name), name
                LIMIT $limit
            )
            ORDER BY d.name, r.name
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var doorId = reader.GetInt32(0);
            if (!doorRows.TryGetValue(doorId, out var entry))
            {
                entry = (
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    new List<string>());
                doorRows[doorId] = entry;
            }
            if (!reader.IsDBNull(3))
                entry.Readers.Add(reader.GetString(3));
        }

        return doorRows
            .Select(kv => new DoorMatch(kv.Key, kv.Value.Name, kv.Value.ControllerAddr, kv.Value.Readers))
            .OrderBy(m => m.Name.Length).ThenBy(m => m.Name)
            .ToList();
    }

    public List<ReaderMatch> SearchReaders(string query, int limit)
    {
        var results = new List<ReaderMatch>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT reader_id, name
            FROM dim_readers
            WHERE name ILIKE $pattern
            ORDER BY LENGTH(name), name
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new ReaderMatch(reader.GetInt32(0), reader.GetString(1)));
        }
        return results;
    }

    public List<PersonMatch> SearchPeople(string query, int limit)
    {
        var results = new List<PersonMatch>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT person_id, first_name, last_name, full_name
            FROM dim_people
            WHERE full_name ILIKE $pattern
               OR first_name ILIKE $pattern
               OR last_name ILIKE $pattern
            ORDER BY LENGTH(full_name), full_name
            LIMIT $limit
            """;
        cmd.Parameters.Add(new DuckDBParameter("pattern", $"%{query}%"));
        cmd.Parameters.Add(new DuckDBParameter("limit", limit));
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new PersonMatch(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetString(3)));
        }
        return results;
    }

    public List<EventTypeRow> ListEventTypes()
    {
        var results = new List<EventTypeRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT event_code, category, name, description FROM lookup_event_types ORDER BY event_code";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new EventTypeRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }
        return results;
    }

    public List<DispositionRow> ListDispositions()
    {
        var results = new List<DispositionRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT disposition, name FROM lookup_dispositions ORDER BY disposition";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new DispositionRow(reader.GetInt32(0), reader.GetString(1)));
        }
        return results;
    }

    public List<AlarmCategoryRow> ListAlarmCategories()
    {
        var results = new List<AlarmCategoryRow>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT category_id, name, description FROM lookup_alarm_categories ORDER BY category_id";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new AlarmCategoryRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2)));
        }
        return results;
    }

    // ── Time-series ─────────────────────────────────────────────────────

    /// <summary>
    /// Bucketed event counts over a time window. Buckets are returned chronologically
    /// and empty buckets are zero-filled so the caller gets a contiguous series.
    /// Supported buckets: "hour", "day", "week", "month".
    /// </summary>
    public TimeSeriesResult GetTransactionTimeSeries(
        string bucket,
        DateTime since, DateTime until,
        int? eventCode, string? readerName, IReadOnlyList<string>? readerNames,
        int? uid1, int? disposition)
    {
        var truncUnit = bucket.ToLowerInvariant() switch
        {
            "hour" => "hour",
            "day" => "day",
            "week" => "week",
            "month" => "month",
            _ => throw new ArgumentException($"Unsupported bucket: {bucket}. Use hour, day, week, or month.")
        };

        var (where, parameters) = BuildTransactionFilter(
            since, until, eventCode, readerName, readerNames, uid1, disposition);

        // DuckDB's generate_series + date_trunc lets us zero-fill empty buckets
        // in a single query rather than post-processing in C#.
        var sql = $"""
            WITH buckets AS (
                SELECT DATE_TRUNC('{truncUnit}', ts) AS bucket
                FROM generate_series(
                    DATE_TRUNC('{truncUnit}', CAST($since AS TIMESTAMP)),
                    DATE_TRUNC('{truncUnit}', CAST($until AS TIMESTAMP)),
                    INTERVAL 1 {truncUnit}
                ) AS t(ts)
            ),
            counts AS (
                SELECT DATE_TRUNC('{truncUnit}', dt_date) AS bucket, COUNT(*) AS c
                FROM fact_transactions {where}
                GROUP BY 1
            )
            SELECT b.bucket, COALESCE(c.c, 0) AS c
            FROM buckets b
            LEFT JOIN counts c ON c.bucket = b.bucket
            WHERE b.bucket < CAST($until AS TIMESTAMP)
            ORDER BY b.bucket ASC
            """;

        var points = new List<TimeSeriesPoint>();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            points.Add(new TimeSeriesPoint(reader.GetDateTime(0), reader.GetInt64(1)));
        }

        var total = points.Sum(p => p.Count);
        return new TimeSeriesResult(truncUnit, points, total);
    }

    // ── Single-row lookups ──────────────────────────────────────────────

    public TransactionDetail? GetTransaction(int logId)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT log_id, dt_date, pc_date_time, event_code, description,
                   disposition, transaction_type, report_as_alarm, alarm_level_priority,
                   port_addr, dt_addr, x_addr, net_address,
                   reader_name, from_zone, to_zone,
                   uid1, uid1_name, uid2, uid2_name
            FROM fact_transactions
            WHERE log_id = $log_id
            """;
        cmd.Parameters.Add(new DuckDBParameter("log_id", logId));

        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return null;

        return new TransactionDetail(
            LogId: reader.GetInt32(0),
            DtDate: reader.GetDateTime(1),
            PcDateTime: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            EventCode: reader.GetInt32(3),
            Description: reader.IsDBNull(4) ? null : reader.GetString(4),
            Disposition: reader.GetInt32(5),
            TransactionType: reader.IsDBNull(6) ? null : reader.GetInt32(6),
            ReportAsAlarm: reader.IsDBNull(7) ? null : reader.GetBoolean(7),
            AlarmLevelPriority: reader.IsDBNull(8) ? null : reader.GetInt32(8),
            PortAddr: reader.IsDBNull(9) ? null : reader.GetString(9),
            DtAddr: reader.IsDBNull(10) ? null : reader.GetString(10),
            XAddr: reader.IsDBNull(11) ? null : reader.GetString(11),
            NetAddress: reader.IsDBNull(12) ? null : reader.GetString(12),
            ReaderName: reader.IsDBNull(13) ? null : reader.GetString(13),
            FromZone: reader.IsDBNull(14) ? null : Convert.ToInt32(reader.GetValue(14)),
            ToZone: reader.IsDBNull(15) ? null : Convert.ToInt32(reader.GetValue(15)),
            PersonId: reader.IsDBNull(16) ? null : reader.GetInt32(16),
            PersonName: reader.IsDBNull(17) ? null : reader.GetString(17),
            Uid2: reader.IsDBNull(18) ? null : reader.GetInt32(18),
            Uid2Name: reader.IsDBNull(19) ? null : reader.GetString(19));
    }

    public AlarmDetail? GetAlarm(int alarmId)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            SELECT alarm_id, dt_date, db_date, ak_date, cl_date,
                   event_id, alarm_level_priority, status, description,
                   net_address, ak_operator, cl_operator, workstation_name,
                   uid1, uid1_name, parm1, parm2
            FROM fact_alarms
            WHERE alarm_id = $alarm_id
            """;
        cmd.Parameters.Add(new DuckDBParameter("alarm_id", alarmId));

        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return null;

        return new AlarmDetail(
            AlarmId: reader.GetInt32(0),
            DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
            DbDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            AkDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            ClDate: reader.IsDBNull(4) ? null : reader.GetDateTime(4),
            EventId: reader.GetInt32(5),
            AlarmLevelPriority: reader.IsDBNull(6) ? null : reader.GetInt32(6),
            Status: reader.IsDBNull(7) ? null : Convert.ToInt32(reader.GetValue(7)),
            Description: reader.IsDBNull(8) ? null : reader.GetString(8),
            NetAddress: reader.IsDBNull(9) ? null : reader.GetString(9),
            AkOperator: reader.IsDBNull(10) ? null : reader.GetString(10),
            ClOperator: reader.IsDBNull(11) ? null : reader.GetString(11),
            WorkstationName: reader.IsDBNull(12) ? null : reader.GetString(12),
            PersonId: reader.IsDBNull(13) ? null : Convert.ToInt64(reader.GetDouble(13)),
            PersonName: reader.IsDBNull(14) ? null : reader.GetString(14),
            Parm1: reader.IsDBNull(15) ? null : reader.GetString(15),
            Parm2: reader.IsDBNull(16) ? null : reader.GetString(16));
    }

    // ── Alarm queries ───────────────────────────────────────────────────

    public long CountAlarms(
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName)
    {
        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM fact_alarms {where}";
        foreach (var p in parameters) cmd.Parameters.Add(p);
        return (long)cmd.ExecuteScalar()!;
    }

    /// <summary>
    /// Aggregate alarms grouped by a dimension. Returns top-N groups plus totals.
    /// Supported group_by values: "event", "priority", "status", "person", "workstation", "hour", "day".
    /// </summary>
    public AggregationResult AggregateAlarms(
        string groupBy,
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName,
        int limit)
    {
        var (keyExpr, keyIdExpr, groupKey, distinctExpr) = groupBy.ToLowerInvariant() switch
        {
            "event" or "event_id" => ("CAST(event_id AS VARCHAR)", "event_id", "event_id", "event_id"),
            "priority" => ("CAST(alarm_level_priority AS VARCHAR)", "alarm_level_priority", "alarm_level_priority", "alarm_level_priority"),
            "status" => ("CAST(status AS VARCHAR)", "status", "status", "status"),
            "person" => ("uid1_name", "CAST(uid1 AS BIGINT)", "uid1_name, uid1", "uid1"),
            "workstation" => ("workstation_name", "NULL", "workstation_name", "workstation_name"),
            "hour" => ("CAST(DATE_TRUNC('hour', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('hour', dt_date)", "DATE_TRUNC('hour', dt_date)"),
            "day" => ("CAST(DATE_TRUNC('day', dt_date) AS VARCHAR)", "NULL", "DATE_TRUNC('day', dt_date)", "DATE_TRUNC('day', dt_date)"),
            _ => throw new ArgumentException($"Unsupported group_by: {groupBy}. Use event, priority, status, person, workstation, hour, or day.")
        };

        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);

        long totalEvents;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_alarms {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalEvents = (long)cmd.ExecuteScalar()!;
        }

        long totalGroups;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(DISTINCT {distinctExpr}) FROM fact_alarms {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalGroups = (long)cmd.ExecuteScalar()!;
        }

        var orderBy = groupBy.ToLowerInvariant() is "hour" or "day" ? "key ASC" : "count DESC";
        var groups = new List<AggregationBucket>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT {keyExpr} AS key, {keyIdExpr} AS key_id, COUNT(*) AS count
                FROM fact_alarms {where}
                GROUP BY {groupKey}
                ORDER BY {orderBy}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                groups.Add(new AggregationBucket(
                    reader.IsDBNull(0) ? null : reader.GetValue(0).ToString(),
                    reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1)),
                    reader.GetInt64(2)));
            }
        }

        return new AggregationResult(
            groupBy,
            groups,
            totalEvents,
            totalGroups,
            Truncated: totalGroups > limit);
    }

    public AlarmSampleResult SampleAlarms(
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName,
        string order, int limit)
    {
        var (where, parameters) = BuildAlarmFilter(
            since, until, eventId, alarmLevelPriority, status, uid1, workstationName);
        var orderSql = order.Equals("time_asc", StringComparison.OrdinalIgnoreCase) ? "dt_date ASC" : "dt_date DESC";

        long totalMatching;
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM fact_alarms {where}";
            foreach (var p in parameters) cmd.Parameters.Add(p);
            totalMatching = (long)cmd.ExecuteScalar()!;
        }

        var rows = new List<AlarmSampleRow>();
        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT alarm_id, dt_date, ak_date, cl_date, event_id,
                       alarm_level_priority, status, description,
                       uid1, uid1_name, workstation_name
                FROM fact_alarms {where}
                ORDER BY {orderSql}
                LIMIT $limit
                """;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.Parameters.Add(new DuckDBParameter("limit", limit));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new AlarmSampleRow(
                    AlarmId: reader.GetInt32(0),
                    DtDate: reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    AkDate: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    ClDate: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    EventId: reader.GetInt32(4),
                    AlarmLevelPriority: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    Status: reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6)),
                    Description: reader.IsDBNull(7) ? null : reader.GetString(7),
                    PersonId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetDouble(8)),
                    PersonName: reader.IsDBNull(9) ? null : reader.GetString(9),
                    WorkstationName: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        return new AlarmSampleResult(rows, totalMatching, Truncated: totalMatching > limit);
    }

    internal static (string Where, List<DuckDBParameter> Parameters) BuildAlarmFilter(
        DateTime since, DateTime until,
        int? eventId, int? alarmLevelPriority, int? status,
        long? uid1, string? workstationName)
    {
        var where = "WHERE dt_date >= $since AND dt_date < $until";
        var parameters = new List<DuckDBParameter>
        {
            new("since", since),
            new("until", until)
        };

        if (eventId.HasValue)
        {
            where += " AND event_id = $event_id";
            parameters.Add(new DuckDBParameter("event_id", eventId.Value));
        }
        if (alarmLevelPriority.HasValue)
        {
            where += " AND alarm_level_priority = $priority";
            parameters.Add(new DuckDBParameter("priority", alarmLevelPriority.Value));
        }
        if (status.HasValue)
        {
            where += " AND status = $status";
            parameters.Add(new DuckDBParameter("status", (short)status.Value));
        }
        if (uid1.HasValue)
        {
            // uid1 is DOUBLE in fact_alarms (decompiled.cs:9364) — cast to match.
            where += " AND uid1 = $uid1";
            parameters.Add(new DuckDBParameter("uid1", (double)uid1.Value));
        }
        if (!string.IsNullOrWhiteSpace(workstationName))
        {
            where += " AND workstation_name = $workstation";
            parameters.Add(new DuckDBParameter("workstation", workstationName));
        }

        return (where, parameters);
    }

    public void Dispose()
    {
        _conn.Dispose();
    }
}

public record EventTypeRow(int EventCode, string Category, string Name, string? Description);
public record DispositionRow(int Disposition, string Name);
public record DoorMatch(int DoorId, string Name, string? ControllerAddr, List<string> Readers);
public record ReaderMatch(int ReaderId, string Name);
public record PersonMatch(int PersonId, string? FirstName, string? LastName, string FullName);

public record AggregationBucket(string? Key, long? KeyId, long Count);
public record AggregationResult(
    string GroupBy,
    List<AggregationBucket> Groups,
    long TotalEvents,
    long TotalGroups,
    bool Truncated);

public record SampleRow(
    int LogId,
    DateTime DtDate,
    int EventCode,
    string? Description,
    int Disposition,
    string? ReaderName,
    int PersonId,
    string? PersonName);
public record SampleResult(
    List<SampleRow> Events,
    long TotalMatching,
    bool Truncated);

public record AlarmCategoryRow(int CategoryId, string Name, string? Description);

public record TimeSeriesPoint(DateTime BucketStart, long Count);
public record TimeSeriesResult(string Bucket, List<TimeSeriesPoint> Points, long TotalEvents);

public record TransactionDetail(
    int LogId,
    DateTime DtDate,
    DateTime? PcDateTime,
    int EventCode,
    string? Description,
    int Disposition,
    int? TransactionType,
    bool? ReportAsAlarm,
    int? AlarmLevelPriority,
    string? PortAddr,
    string? DtAddr,
    string? XAddr,
    string? NetAddress,
    string? ReaderName,
    int? FromZone,
    int? ToZone,
    int? PersonId,
    string? PersonName,
    int? Uid2,
    string? Uid2Name);

public record AlarmDetail(
    int AlarmId,
    DateTime? DtDate,
    DateTime? DbDate,
    DateTime? AkDate,
    DateTime? ClDate,
    int EventId,
    int? AlarmLevelPriority,
    int? Status,
    string? Description,
    string? NetAddress,
    string? AkOperator,
    string? ClOperator,
    string? WorkstationName,
    long? PersonId,
    string? PersonName,
    string? Parm1,
    string? Parm2);

public record AlarmSampleRow(
    int AlarmId,
    DateTime? DtDate,
    DateTime? AkDate,
    DateTime? ClDate,
    int EventId,
    int? AlarmLevelPriority,
    int? Status,
    string? Description,
    long? PersonId,
    string? PersonName,
    string? WorkstationName);
public record AlarmSampleResult(
    List<AlarmSampleRow> Alarms,
    long TotalMatching,
    bool Truncated);
