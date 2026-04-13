using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class TimeseriesAlarmsTool
{
    [McpServerTool(Name = "timeseries_alarms", Destructive = false, ReadOnly = true),
     Description("Return a zero-filled time series of alarm counts bucketed by hour, day, week, or month. " +
                 "The alarm parity of timeseries_events. Use this for trend questions like " +
                 "'alarms per hour for the last 24 hours', 'daily priority-1 alarm volume this month', " +
                 "or 'how did unacked alarms trend across the week?'. " +
                 "Empty buckets are zero-filled. Filters match count_alarms / aggregate_alarms: " +
                 "event_id, alarm_level_priority, status, person_id, workstation_name. " +
                 "For a single grand total use count_alarms; for top-N by dimension use aggregate_alarms; " +
                 "for ack/clear lifecycle metrics use alarm_response_metrics.")]
    public static string TimeseriesAlarms(
        DuckDbMirror mirror,
        [Description("Bucket unit: 'hour', 'day', 'week', or 'month'.")]
        string bucket,
        [Description("Start of time window (ISO 8601). Defaults depend on bucket: 24h for hour, 14d for day, 12w for week, 12mo for month.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Filter by the Velocity event_id that triggered the alarm.")]
        int? event_id = null,
        [Description("Filter by alarm priority level (lower = more severe in Velocity convention).")]
        int? alarm_level_priority = null,
        [Description("Filter by alarm status code (e.g. 0=new, 1=acknowledged, 2=cleared).")]
        int? status = null,
        [Description("Filter by person ID. Use find_people to discover. Same person_id type works across both transaction and alarm tools.")]
        long? person_id = null,
        [Description("Filter by exact workstation_name (operator console that received/handled the alarm).")]
        string? workstation_name = null)
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

        var result = mirror.GetAlarmTimeSeries(
            bucket, sinceDate, untilDate,
            event_id, alarm_level_priority, status, person_id, workstation_name);

        var fullCount = result.Points.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            bucket = result.Bucket,
            points = result.Points.Take(n).Select(p => new
            {
                bucket_start = p.BucketStart.ToString("o"),
                count = p.Count
            }),
            total_events = result.TotalEvents,
            returned = n,
            truncated_due_to_size = n < fullCount,
            window_used = new
            {
                since = sinceDate.ToString("o"),
                until = untilDate.ToString("o"),
                defaulted_since = since == null,
                defaulted_until = until == null
            }
        }, fullCount);
    }
}
