using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace VelocityMCP.Data;

/// <summary>
/// Background service that polls the Velocity SDK for new log data and ingests it
/// into the DuckDB mirror. Runs on a configurable interval (default 30s).
/// On first run against an empty mirror, performs a bounded backfill (default 7 days).
/// </summary>
public sealed class IngestWorker : BackgroundService
{
    private readonly IVelocityClient _sdk;
    private readonly DuckDbMirror _mirror;
    private readonly ILogger<IngestWorker> _logger;
    private readonly TimeSpan _interval;
    private readonly TimeSpan _policyInterval;
    private readonly TimeSpan _backfillHorizon;
    private readonly int _bulkBackfillCalls;

    public IngestWorker(
        IVelocityClient sdk,
        DuckDbMirror mirror,
        ILogger<IngestWorker> logger,
        TimeSpan? interval = null,
        TimeSpan? backfillHorizon = null,
        int bulkBackfillCalls = 1,
        TimeSpan? policyInterval = null)
    {
        _sdk = sdk;
        _mirror = mirror;
        _logger = logger;
        _interval = interval ?? TimeSpan.FromSeconds(30);
        _policyInterval = policyInterval ?? TimeSpan.FromMinutes(5);
        _backfillHorizon = backfillHorizon ?? TimeSpan.FromDays(7);
        _bulkBackfillCalls = Math.Max(1, bulkBackfillCalls);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Ingest worker starting. Fact interval: {Interval}s, Policy interval: {PolicyInterval}s, Backfill horizon: {Horizon} days",
            _interval.TotalSeconds, _policyInterval.TotalSeconds, _backfillHorizon.TotalDays);

        if (!_sdk.IsConnected)
        {
            await _sdk.ConnectAsync(stoppingToken);
            _logger.LogInformation("SDK connected");
        }

        // Refresh dimension + policy tables at startup
        await RefreshDimensions(stoppingToken);

        // Backfill if no cursor exists
        await BackfillIfNeeded("Log_Transactions", stoppingToken);
        await BackfillIfNeeded("AlarmLog", stoppingToken);

        // Two parallel loops: facts (fast) + policy/dimensions (slow). Policy data
        // changes per-day, not per-second, so a 5-minute beat avoids hammering the
        // SQL Server with the bulk clearance queries on every fact tick.
        var factLoop = RunFactLoop(stoppingToken);
        var policyLoop = RunPolicyLoop(stoppingToken);
        await Task.WhenAll(factLoop, policyLoop);
    }

    private async Task RunFactLoop(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await IngestCycle(stoppingToken);
        }
    }

    private async Task RunPolicyLoop(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_policyInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await RefreshDimensions(stoppingToken);
        }
    }

    private async Task RefreshDimensions(CancellationToken ct)
    {
        try
        {
            var doors = await _sdk.GetDoorsAsync(ct);
            _mirror.UpsertDoors(doors);

            var readers = await _sdk.GetReadersAsync(ct);
            _mirror.UpsertReaders(readers);

            var people = await _sdk.GetPersonsAsync(ct);
            _mirror.UpsertPeople(people);

            // Credential→person mapping — required so transaction tools can
            // resolve fact_transactions.uid1 (a CredentialId) back to a real
            // HostUserId. Refreshed on the policy cadence like the dim tables.
            var credentials = await _sdk.GetUserCredentialsAsync(ct);
            _mirror.UpsertUserCredentials(credentials);

            // Authorization / policy dimensions. These are small (hundreds of rows
            // at most) and change slowly — same refresh cadence as the rest.
            var clearances = await _sdk.GetClearancesAsync(ct);
            _mirror.UpsertClearances(clearances);

            var readerClearances = await _sdk.GetReaderClearancesAsync(ct);
            _mirror.UpsertReaderClearances(readerClearances);

            var personClearances = await _sdk.GetPersonClearancesAsync(ct);
            _mirror.UpsertPersonClearances(personClearances);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Dimension refresh failed");
        }
    }

    private async Task BackfillIfNeeded(string sourceTable, CancellationToken ct)
    {
        var existingCursor = _mirror.GetCursor(sourceTable);
        if (existingCursor.HasValue)
        {
            _logger.LogInformation("{Table}: existing cursor at {Cursor}, skipping backfill",
                sourceTable, existingCursor.Value);
            return;
        }

        if (_bulkBackfillCalls > 1)
        {
            await BulkBackfill(sourceTable, ct);
            return;
        }

        var horizon = DateTime.UtcNow - _backfillHorizon;
        _logger.LogInformation("{Table}: no cursor found, backfilling from {Horizon}",
            sourceTable, horizon);

        // Walk backwards in daily chunks from today to horizon
        var chunkEnd = DateTime.UtcNow;
        var totalRows = 0L;

        while (chunkEnd > horizon && !ct.IsCancellationRequested)
        {
            var chunkStart = chunkEnd.AddDays(-1);
            if (chunkStart < horizon) chunkStart = horizon;

            try
            {
                // SDK uses strict > so subtract 1ms to include boundary
                var sinceDate = chunkStart.AddMilliseconds(-1);
                int count = 0;

                if (sourceTable == "Log_Transactions")
                {
                    var records = await _sdk.GetLogTransactionsAsync(sinceDate, ct);
                    // Filter to only the chunk window to avoid overlap
                    var inWindow = records.Where(r => r.DtDate >= chunkStart && r.DtDate < chunkEnd).ToList();
                    _mirror.IngestTransactions(inWindow);
                    count = inWindow.Count;
                }
                else if (sourceTable == "AlarmLog")
                {
                    var records = await _sdk.GetAlarmLogAsync(sinceDate, ct);
                    var inWindow = records.Where(r => r.DtDate.HasValue && r.DtDate.Value >= chunkStart && r.DtDate.Value < chunkEnd).ToList();
                    _mirror.IngestAlarms(inWindow);
                    count = inWindow.Count;
                }

                totalRows += count;
                _logger.LogDebug("{Table}: backfill chunk {Start:d} → {End:d}: {Count} rows",
                    sourceTable, chunkStart, chunkEnd, count);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "{Table}: backfill chunk {Start:d} → {End:d} failed, continuing",
                    sourceTable, chunkStart, chunkEnd);
            }

            chunkEnd = chunkEnd.AddDays(-1);
        }

        // Set initial cursor to now
        _mirror.UpdateCursor(sourceTable, DateTime.UtcNow, totalRows);
        _logger.LogInformation("{Table}: backfill complete. {TotalRows} rows ingested", sourceTable, totalRows);
    }

    // One-shot cold-start seed path for the fake SDK. Calls the client N times with
    // the full horizon window; because the fake client mints unique keys on every
    // call, each batch adds fresh rows instead of colliding. For the real SDK this
    // path is bypassed (_bulkBackfillCalls defaults to 1), so no duplicate fetches.
    private async Task BulkBackfill(string sourceTable, CancellationToken ct)
    {
        var horizon = DateTime.UtcNow - _backfillHorizon;
        _logger.LogInformation("{Table}: no cursor, bulk seeding {Calls} batches from {Horizon}",
            sourceTable, _bulkBackfillCalls, horizon);

        var sinceDate = horizon.AddMilliseconds(-1);
        long totalRows = 0;
        DateTime maxDt = horizon;

        for (int batch = 0; batch < _bulkBackfillCalls && !ct.IsCancellationRequested; batch++)
        {
            try
            {
                int count = 0;

                if (sourceTable == "Log_Transactions")
                {
                    var records = await _sdk.GetLogTransactionsAsync(sinceDate, ct);
                    if (records.Count > 0)
                    {
                        _mirror.IngestTransactions(records);
                        count = records.Count;
                        var batchMax = records.Max(r => r.DtDate);
                        if (batchMax > maxDt) maxDt = batchMax;
                    }
                }
                else if (sourceTable == "AlarmLog")
                {
                    var records = await _sdk.GetAlarmLogAsync(sinceDate, ct);
                    if (records.Count > 0)
                    {
                        _mirror.IngestAlarms(records);
                        count = records.Count;
                        var dated = records.Where(r => r.DtDate.HasValue).ToList();
                        if (dated.Count > 0)
                        {
                            var batchMax = dated.Max(r => r.DtDate!.Value);
                            if (batchMax > maxDt) maxDt = batchMax;
                        }
                    }
                }

                totalRows += count;
                _logger.LogDebug("{Table}: bulk batch {Batch}/{Total}: {Count} rows",
                    sourceTable, batch + 1, _bulkBackfillCalls, count);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "{Table}: bulk batch {Batch} failed, continuing",
                    sourceTable, batch + 1);
            }
        }

        _mirror.UpdateCursor(sourceTable, DateTime.UtcNow, totalRows);
        _logger.LogInformation("{Table}: bulk backfill complete. {TotalRows} rows ingested",
            sourceTable, totalRows);
    }

    private async Task IngestCycle(CancellationToken ct)
    {
        await IngestTable("Log_Transactions", ct);
        await IngestTable("AlarmLog", ct);
    }

    private async Task IngestTable(string sourceTable, CancellationToken ct)
    {
        try
        {
            var cursor = _mirror.GetCursor(sourceTable);
            if (!cursor.HasValue)
            {
                _logger.LogWarning("{Table}: no cursor, skipping incremental ingest", sourceTable);
                return;
            }

            // SDK strict > off-by-one: subtract 1ms to include boundary events
            var sinceDate = cursor.Value.AddMilliseconds(-1);

            int count = 0;
            DateTime maxDt = cursor.Value;

            if (sourceTable == "Log_Transactions")
            {
                var records = await _sdk.GetLogTransactionsAsync(sinceDate, ct);
                if (records.Count > 0)
                {
                    _mirror.IngestTransactions(records);
                    count = records.Count;
                    maxDt = records.Max(r => r.DtDate);
                }
            }
            else if (sourceTable == "AlarmLog")
            {
                var records = await _sdk.GetAlarmLogAsync(sinceDate, ct);
                if (records.Count > 0)
                {
                    _mirror.IngestAlarms(records);
                    count = records.Count;
                    maxDt = records.Where(r => r.DtDate.HasValue).Max(r => r.DtDate!.Value);
                }
            }

            if (count > 0)
            {
                _mirror.UpdateCursor(sourceTable, maxDt, count);
                _logger.LogInformation("{Table}: ingested {Count} rows, cursor → {Cursor}",
                    sourceTable, count, maxDt);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{Table}: ingest cycle failed", sourceTable);
            _mirror.UpdateCursor(sourceTable,
                _mirror.GetCursor(sourceTable) ?? DateTime.UtcNow,
                0, ex.Message);
        }
    }
}
