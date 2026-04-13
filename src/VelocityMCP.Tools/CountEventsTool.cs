using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class CountEventsTool
{
    [McpServerTool(Name = "count_events", Destructive = false, ReadOnly = true),
     Description("Count access control events in a time window. Returns a single count with the resolved time window. " +
                 "Use this for questions like 'how many transactions in the last hour?' or " +
                 "'how many times was the front door forced open this week?'. " +
                 "For breakdowns by dimension (person, door, type), use aggregate_events instead. " +
                 "When filtering by door, pass all its reader names via `reader_names` (not the door name) — " +
                 "find_doors returns the reader list you need.")]
    public static string CountEvents(
        DuckDbMirror mirror,
        [Description("Start of time window (ISO 8601). Defaults to 24 hours ago if omitted.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now if omitted.")]
        string? until = null,
        [Description("Filter by event code (e.g. 4 = Door Forced Open). Use list_event_types to discover codes.")]
        int? event_code = null,
        [Description("Filter by a single exact reader name. Prefer `reader_names` if you have the list from find_doors.")]
        string? reader_name = null,
        [Description("Filter by a list of exact reader names — matches any of them. Use this when find_doors returns multiple readers for a single logical door (e.g. entry + exit).")]
        string[]? reader_names = null,
        [Description("Filter by person ID (UID1). Use find_people to look up IDs.")]
        int? person_id = null,
        [Description("Filter by disposition (1 = Granted, 2 = Denied, etc.). Use list_dispositions to see values.")]
        int? disposition = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;

        var count = mirror.CountTransactions(
            sinceDate, untilDate, event_code, reader_name, reader_names,
            person_id, disposition);

        var result = new
        {
            count,
            window_used = new
            {
                since = sinceDate.ToString("o"),
                until = untilDate.ToString("o"),
                defaulted_since = since == null,
                defaulted_until = until == null
            }
        };

        return JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
    }
}
