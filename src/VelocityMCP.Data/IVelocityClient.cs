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
}
