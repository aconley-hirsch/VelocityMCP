namespace VelocityMCP.Data.Models;

/// <summary>
/// Mirror of SDK's LogTransaction with PII fields (PIN, CODE, ROMID) excluded.
/// This is the allow-listed projection — fields not listed here must never be persisted.
/// </summary>
public sealed class TransactionRecord
{
    public int LogId { get; init; }
    public DateTime DtDate { get; init; }
    public DateTime? PcDateTime { get; init; }
    public int EventCode { get; init; }
    public string? Description { get; init; }
    public int Disposition { get; init; }
    public int TransactionType { get; init; }
    public bool ReportAsAlarm { get; init; }
    public int AlarmLevelPriority { get; init; }
    public string? PortAddr { get; init; }
    public string? DtAddr { get; init; }
    public string? XAddr { get; init; }
    public string? NetAddress { get; init; }
    public byte DoorOrExpansion { get; init; }
    public byte Reader { get; init; }
    public string? ReaderName { get; init; }
    public int? FromZone { get; init; }
    public int? ToZone { get; init; }
    public int Uid1 { get; init; }
    public string? Uid1Name { get; init; }
    public int Uid2 { get; init; }
    public string? Uid2Name { get; init; }
    public int ServerID { get; init; }
    public int SecurityDomainID { get; init; }
}
