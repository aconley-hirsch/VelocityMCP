namespace VelocityMCP.Data.Models;

/// <summary>
/// An access level / clearance — a named grouping of readers that, when assigned
/// to a person, grants them access to those readers. <see cref="ScheduleName"/>
/// is informational in v1 (e.g. "24x7", "Business Hours"); proper schedule
/// enforcement is deferred until the real Velocity SDK is wired up.
/// </summary>
public sealed class ClearanceRecord
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public string? ScheduleName { get; init; }
    public bool Active { get; init; } = true;
}

/// <summary>Mapping row: reader <c>reader_id</c> is part of clearance <c>clearance_id</c>.</summary>
public sealed class ReaderClearanceRecord
{
    public int ReaderId { get; init; }
    public int ClearanceId { get; init; }
}

/// <summary>
/// Mapping row: person <c>person_id</c> holds clearance <c>clearance_id</c>.
/// <c>ExpiresAt</c> null means indefinite.
/// </summary>
public sealed class PersonClearanceRecord
{
    public int PersonId { get; init; }
    public int ClearanceId { get; init; }
    public DateTime GrantedAt { get; init; }
    public DateTime? ExpiresAt { get; init; }
}
