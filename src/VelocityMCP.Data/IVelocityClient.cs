using VelocityMCP.Data.Models;

namespace VelocityMCP.Data;

/// <summary>
/// Abstraction over the Velocity SDK. The real implementation wraps VelocityAdapter.dll
/// (Windows only); the fake generates synthetic data for Mac dev and testing.
/// All methods return our allow-listed DTOs — PII fields are never exposed.
/// </summary>
public interface IVelocityClient : IDisposable
{
    Task ConnectAsync(CancellationToken ct = default);
    bool IsConnected { get; }

    Task<List<TransactionRecord>> GetLogTransactionsAsync(DateTime sinceDate, CancellationToken ct = default);
    Task<List<AlarmRecord>> GetAlarmLogAsync(DateTime sinceDate, CancellationToken ct = default);

    Task<List<DoorRecord>> GetDoorsAsync(CancellationToken ct = default);
    Task<List<ReaderRecord>> GetReadersAsync(CancellationToken ct = default);
    Task<List<PersonRecord>> GetPersonsAsync(CancellationToken ct = default);
    Task<List<UserCredentialRecord>> GetUserCredentialsAsync(CancellationToken ct = default);

    // ── Authorization / policy dimensions ──────────────────────────────
    // These feed dim_clearances / dim_reader_clearances / dim_person_clearances.
    // Refreshed on the same cadence as GetDoorsAsync etc. — policy data changes
    // slowly (admin edits per day, not per-second events), so a mirror is
    // cheap. A live-lookup hook can be added here later for freshness-sensitive
    // "right now, is X allowed?" queries.

    Task<List<ClearanceRecord>> GetClearancesAsync(CancellationToken ct = default);
    Task<List<ReaderClearanceRecord>> GetReaderClearancesAsync(CancellationToken ct = default);
    Task<List<PersonClearanceRecord>> GetPersonClearancesAsync(CancellationToken ct = default);
}
