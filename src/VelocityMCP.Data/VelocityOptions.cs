namespace VelocityMCP.Data;

/// <summary>
/// Configuration for the real Velocity SDK connector. Bound from the
/// "Velocity" config section. All values are also overridable via the
/// VELOCITY_SQL_SERVER / VELOCITY_DATABASE / VELOCITY_APP_ROLE_PW env
/// vars to match the SDK's StarterTemplate sample.
/// </summary>
public sealed class VelocityOptions
{
    /// <summary>SQL Server instance hosting the Velocity database (e.g. <c>VELOCITY-SQL\SQLEXPRESS</c>).</summary>
    public string SqlServer { get; set; } = @".\SQLEXPRESS";

    /// <summary>Velocity database name (almost always "Velocity").</summary>
    public string Database { get; set; } = "Velocity";

    /// <summary>
    /// Optional SQL application role password. When null/empty the connector uses
    /// Windows authentication; when set it uses the SDK's app-role overload.
    /// </summary>
    public string? AppRolePassword { get; set; }

    /// <summary>If true (default), use the synthetic FakeVelocityClient instead of the real SDK.</summary>
    public bool UseFake { get; set; } = true;
}
