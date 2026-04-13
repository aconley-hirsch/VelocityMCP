using System.ComponentModel;
using ModelContextProtocol.Server;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class ServerInfoTool
{
    [McpServerTool(Name = "server_info", Destructive = false, ReadOnly = true),
     Description("Returns current server time, timezone, and schema version. " +
                 "Call this first to ground yourself in 'now' before interpreting " +
                 "natural-language time references like 'yesterday' or 'last week'.")]
    public static string ServerInfo()
    {
        var result = new
        {
            server_time = DateTime.UtcNow.ToString("o"),
            timezone = TimeZoneInfo.Local.Id,
            utc_offset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).ToString(),
            schema_version = 1,
            server_name = "VelocityMCP",
            server_version = "0.1.0-dev"
        };

        return ResponseShaper.Serialize(result);
    }
}
