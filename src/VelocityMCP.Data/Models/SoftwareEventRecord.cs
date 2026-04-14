namespace VelocityMCP.Data.Models;

/// <summary>
/// Mirror of SDK's LogSoftware — Velocity's audit trail of administrative
/// actions (credential added, clearance modified, operator login, etc.).
/// Source: Log_Software table. The Description column is already human-
/// readable ("Credential 1061: SAFR was added by Administrator") so there's
/// no need for an event-code lookup table — event_code is kept only for
/// scoped filtering.
/// </summary>
public sealed class SoftwareEventRecord
{
    public int LogId { get; init; }
    public DateTime DtDate { get; init; }             // from EventTime
    public DateTime? PcDateTime { get; init; }
    public int EventCode { get; init; }               // from Event
    public string Description { get; init; } = "";
    public int? OperatorId { get; init; }
    public string? NetAddress { get; init; }
    public int? SecurityDomainId { get; init; }
}
