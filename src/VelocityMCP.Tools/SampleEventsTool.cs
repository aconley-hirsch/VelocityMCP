using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class SampleEventsTool
{
    [McpServerTool(Name = "sample_events", Destructive = false, ReadOnly = true),
     Description("Return a bounded sample of individual access events matching filters. " +
                 "Use this for questions about SPECIFIC events rather than counts or breakdowns — " +
                 "e.g. 'who was the last person through the front door?' (limit=1, order=time_desc), " +
                 "'show me the most recent forced-open events', or " +
                 "'what happened at the server room this morning?'. " +
                 "For counts and aggregations use count_events or aggregate_events instead. " +
                 "Response includes total_matching (how many matched before the limit), " +
                 "truncated flag, and the resolved time window. " +
                 "Default limit is 10, max is 50 — narrow filters before asking for more.")]
    public static string SampleEvents(
        DuckDbMirror mirror,
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
        [Description("Sort order: 'time_desc' (most recent first, default) or 'time_asc' (oldest first).")]
        string? order = null,
        [Description("Maximum number of events to return. Defaults to 10, max 50.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 10, 50);
        var effectiveOrder = order ?? "time_desc";

        var result = mirror.SampleTransactions(
            sinceDate, untilDate,
            event_code, reader_name, reader_names, person_id, disposition,
            effectiveOrder, effectiveLimit);

        var payload = new
        {
            events = result.Events.Select(e => new
            {
                log_id = e.LogId,
                time = e.DtDate.ToString("o"),
                event_code = e.EventCode,
                description = e.Description,
                disposition = e.Disposition,
                reader_name = e.ReaderName,
                person_id = e.PersonId,
                person_name = e.PersonName
            }),
            returned = result.Events.Count,
            total_matching = result.TotalMatching,
            truncated = result.Truncated,
            order = effectiveOrder,
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
