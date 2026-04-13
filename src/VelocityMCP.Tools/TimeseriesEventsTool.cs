using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class TimeseriesEventsTool
{
    [McpServerTool(Name = "timeseries_events", Destructive = false, ReadOnly = true),
     Description("Return a zero-filled time series of event counts bucketed by hour, day, week, or month. " +
                 "Use this for trend questions like 'show me access traffic per hour for the last 24 hours', " +
                 "'how did forced-open events trend across the week?', or " +
                 "'daily badge-ins for the server room this month'. " +
                 "Empty buckets are returned as zero so the caller sees a contiguous series. " +
                 "Filters work the same as count_events. " +
                 "For a single grand total, use count_events; for top-N breakdowns by dimension, use aggregate_events. " +
                 "Response includes bucket unit, points array, total_events, and the resolved time window.")]
    public static string TimeseriesEvents(
        DuckDbMirror mirror,
        [Description("Bucket unit: 'hour', 'day', 'week', or 'month'.")]
        string bucket,
        [Description("Start of time window (ISO 8601). Defaults depend on bucket: 24h for hour, 14d for day, 12w for week, 12mo for month.")]
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
        int? disposition = null)
    {
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var defaultSince = bucket.ToLowerInvariant() switch
        {
            "hour" => untilDate.AddHours(-24),
            "day" => untilDate.AddDays(-14),
            "week" => untilDate.AddDays(-7 * 12),
            "month" => untilDate.AddMonths(-12),
            _ => untilDate.AddHours(-24)
        };
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : defaultSince;

        var result = mirror.GetTransactionTimeSeries(
            bucket, sinceDate, untilDate,
            event_code, reader_name, reader_names, person_id, disposition);

        var payload = new
        {
            bucket = result.Bucket,
            points = result.Points.Select(p => new
            {
                bucket_start = p.BucketStart.ToString("o"),
                count = p.Count
            }),
            total_events = result.TotalEvents,
            returned = result.Points.Count,
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
