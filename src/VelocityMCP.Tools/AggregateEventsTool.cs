using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class AggregateEventsTool
{
    [McpServerTool(Name = "aggregate_events", Destructive = false, ReadOnly = true),
     Description("Group access events by a dimension and return top-N buckets with counts. " +
                 "This is the workhorse tool for 'top X' and 'breakdown by Y' questions like " +
                 "'top 5 busiest doors this week', 'events per person today', 'forced opens by day', " +
                 "or 'how many distinct people used the side office' (use total_groups for that). " +
                 "Response includes total_events (grand total), total_groups (distinct values), " +
                 "truncated flag, and the resolved time window. " +
                 "Supported group_by values: person, door, type, hour, day. " +
                 "Filters work the same as count_events. " +
                 "For a single count with no breakdown, use count_events instead.")]
    public static string AggregateEvents(
        DuckDbMirror mirror,
        [Description("Dimension to group by: 'person', 'door', 'type', 'hour', or 'day'.")]
        string group_by,
        [Description("Start of time window (ISO 8601). Defaults to 24 hours ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Filter by event code. Use list_event_types to discover codes.")]
        int? event_code = null,
        [Description("Filter by a single exact reader name. Prefer `reader_names` for multi-reader doors.")]
        string? reader_name = null,
        [Description("Filter by a list of exact reader names. Use this with find_doors output to filter by a logical door that has multiple readers.")]
        string[]? reader_names = null,
        [Description("Filter by person ID. Use find_people to discover.")]
        int? person_id = null,
        [Description("Filter by disposition code. Use list_dispositions to discover.")]
        int? disposition = null,
        [Description("Maximum number of groups to return. Defaults to 10, max 50.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 10, 50);

        var result = mirror.AggregateTransactions(
            group_by, sinceDate, untilDate,
            event_code, reader_name, reader_names, person_id, disposition,
            effectiveLimit);

        var payload = new
        {
            group_by = result.GroupBy,
            groups = result.Groups.Select(g => new
            {
                key = g.Key,
                key_id = g.KeyId,
                count = g.Count
            }),
            total_events = result.TotalEvents,
            total_groups = result.TotalGroups,
            truncated = result.Truncated,
            window_used = new
            {
                since = sinceDate.ToString("o"),
                until = untilDate.ToString("o"),
                defaulted_since = since == null,
                defaulted_until = until == null
            }
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
    }
}
