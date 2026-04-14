using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class ServerInfoTool
{
    [McpServerTool(Name = "server_info", Destructive = false, ReadOnly = true),
     Description("Returns current server time, timezone, schema version, AND the total row " +
                 "counts for every dimension and fact table in the mirror (people, doors, " +
                 "readers, clearances, operators, transactions, alarms, software_events). " +
                 "Call this first to ground yourself in 'now' before interpreting natural-" +
                 "language time references AND to answer 'how many X are in the system' " +
                 "questions directly from the `counts` field — no further tool calls needed.")]
    public static string ServerInfo(DuckDbMirror mirror)
    {
        var counts = mirror.GetDimensionCounts();
        var result = new
        {
            server_time = DateTime.UtcNow.ToString("o"),
            timezone = TimeZoneInfo.Local.Id,
            utc_offset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).ToString(),
            schema_version = 1,
            server_name = "VelocityMCP",
            server_version = "0.1.0-dev",
            counts = new
            {
                people = counts.People,
                doors = counts.Doors,
                readers = counts.Readers,
                clearances = counts.Clearances,
                operators = counts.Operators,
                transactions = counts.Transactions,
                alarms = counts.Alarms,
                software_events = counts.SoftwareEvents
            }
        };

        return ResponseShaper.Serialize(result);
    }
}
