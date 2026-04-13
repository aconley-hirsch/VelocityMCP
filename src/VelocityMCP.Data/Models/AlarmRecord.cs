namespace VelocityMCP.Data.Models;

/// <summary>
/// Mirror of SDK's AlarmLog. Note: UID1/UID2 are double in the source schema
/// (decompiled.cs:9364), unlike LogTransaction which uses int.
/// </summary>
public sealed class AlarmRecord
{
    public int AlarmId { get; init; }
    public DateTime? DtDate { get; init; }
    public DateTime? DbDate { get; init; }
    public DateTime? AkDate { get; init; }
    public DateTime? ClDate { get; init; }
    public int EventId { get; init; }
    public int AlarmLevelPriority { get; init; }
    public byte Status { get; init; }
    public string? Description { get; init; }
    public int PortAddr { get; init; }
    public int DtAddr { get; init; }
    public int XAddr { get; init; }
    public string? NetAddress { get; init; }
    public string? AkOperator { get; init; }
    public string? ClOperator { get; init; }
    public string? WorkstationName { get; init; }
    public double? Uid1 { get; init; }
    public string? Uid1Name { get; init; }
    public double? Uid2 { get; init; }
    public string? Uid2Name { get; init; }
    public string? Parm1 { get; init; }
    public string? Parm2 { get; init; }
    public int TransactionType { get; init; }
    public int ServerID { get; init; }
    public int SiteID { get; init; }
}
