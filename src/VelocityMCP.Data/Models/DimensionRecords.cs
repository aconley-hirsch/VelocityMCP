namespace VelocityMCP.Data.Models;

public sealed class DoorRecord
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public byte Index { get; init; }
    public int ControllerId { get; init; }
    public string? Address { get; init; }
}

public sealed class ReaderRecord
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public int FromZone { get; init; }
    public int ToZone { get; init; }
    /// <summary>Door this reader belongs to, if known. Null for unassociated readers.</summary>
    public int? DoorId { get; init; }
}

public sealed class PersonRecord
{
    public int PersonId { get; init; }
    public string? FirstName { get; init; }
    public string? LastName { get; init; }
}

/// <summary>
/// Credential-to-person mapping. Velocity stores one row per badge/card/PIN
/// in UserCredentials; fact_transactions.uid1 is the CredentialId, so resolving
/// "events for person X" requires a join through this table.
/// </summary>
public sealed class UserCredentialRecord
{
    public int CredentialId { get; init; }
    public int PersonId { get; init; }
}
