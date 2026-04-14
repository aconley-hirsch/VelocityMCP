using System.ComponentModel;
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
                 "Supported group_by values: person, credential, door, reader, type, hour, day. " +
                 "IMPORTANT: 'person' rolls up all badges a person owns into ONE bucket — key_id is a real person_id (usable with person_dossier, find_people follow-ups). " +
                 "'credential' groups by individual badge/card (key_id is a credential_id) — use only when credential-level granularity matters (e.g. 'is a specific badge being over-used'). " +
                 "'door' collapses multi-reader doors into one row per logical door (preferred for 'busiest doors' questions). " +
                 "'reader' groups by individual physical reader (use only when reader-level granularity matters). " +
                 "Filters work the same as count_events — prefer `door_id` over `reader_names` for door-scoped filtering. " +
                 "TIME WINDOWS: for phrases like 'last week', 'yesterday', 'this month' pass `relative_window` " +
                 "('last_7d', 'yesterday', 'this_month' — see that parameter for the full list). " +
                 "Do NOT ask the user for ISO dates when a named window fits. " +
                 "For a single count with no breakdown, use count_events instead.")]
    public static string AggregateEvents(
        DuckDbMirror mirror,
        [Description("Dimension to group by: 'person' (real person, rolled up across their badges), 'credential' (individual badge/card), 'door' (logical, collapses multi-reader doors), 'reader' (physical), 'type', 'hour', or 'day'.")]
        string group_by,
        [Description(TimeWindow.ParameterDescription)]
        string? relative_window = null,
        [Description("Start of time window (ISO 8601). Prefer `relative_window` for phrases like 'last week'. Defaults to 24 hours ago when neither this nor relative_window is set.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Prefer `relative_window` for phrases like 'last week'. Defaults to now.")]
        string? until = null,
        [Description("Filter by event code. Use list_event_types to discover codes.")]
        int? event_code = null,
        [Description("Filter by logical door ID. Use find_doors to discover. The tool resolves to all of the door's readers automatically — preferred over reader_name/reader_names.")]
        int? door_id = null,
        [Description("Filter by a single exact reader name. Prefer `door_id` for door-scoped questions.")]
        string? reader_name = null,
        [Description("Filter by a list of exact reader names. Prefer `door_id` for door-scoped questions.")]
        string[]? reader_names = null,
        [Description("Filter by person ID. Use find_people to discover. Same person_id type works across both transaction and alarm tools.")]
        long? person_id = null,
        [Description("Filter by disposition code. Use list_dispositions to discover.")]
        int? disposition = null,
        [Description("Maximum number of groups to return. Defaults to 10, max 50.")]
        int? limit = null)
    {
        var (sinceDate, untilDate) = TimeWindow.Resolve(
            relative_window, since, until, defaultWindow: TimeSpan.FromHours(24));
        var effectiveLimit = Math.Min(limit ?? 10, 50);

        var result = mirror.AggregateTransactions(
            group_by, sinceDate, untilDate,
            event_code, reader_name, reader_names, person_id, disposition,
            effectiveLimit, door_id);

        var fullCount = result.Groups.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            group_by = result.GroupBy,
            groups = result.Groups.Take(n).Select(g => new
            {
                key = g.Key,
                key_id = g.KeyId,
                count = g.Count
            }),
            total_events = result.TotalEvents,
            total_groups = result.TotalGroups,
            truncated = result.Truncated || n < fullCount,
            truncated_due_to_size = n < fullCount,
            window_used = new
            {
                since = sinceDate.ToString("o"),
                until = untilDate.ToString("o"),
                relative_window,
                defaulted_since = since == null && relative_window == null,
                defaulted_until = until == null && relative_window == null
            }
        }, fullCount);
    }
}
