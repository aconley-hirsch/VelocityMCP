using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class AlarmResponseMetricsTool
{
    [McpServerTool(Name = "alarm_response_metrics", Destructive = false, ReadOnly = true),
     Description("Compute alarm acknowledge and clear lifecycle metrics from fact_alarms. " +
                 "Unlocks compliance/SLA questions like 'average time to acknowledge alarms this week', " +
                 "'which operator is fastest at clearing alarms', or " +
                 "'how many alarms took more than 30 minutes to acknowledge'. " +
                 "For each group returns total, avg_ack_minutes, avg_clear_minutes, p90_ack_minutes, " +
                 "and still_open (count with no ak_date). " +
                 "Top-level totals (total_alarms, total_unacked) are over the full filter, " +
                 "unaffected by group-specific filters. " +
                 "Supported group_by values: 'operator' (ak_operator; unacked rows excluded from groups), " +
                 "'priority' (alarm_level_priority), 'event' (event_id), 'day', 'hour'. " +
                 "Standard alarm filters apply (event_id, alarm_level_priority, status, person_id, workstation_name). " +
                 "Default limit is 10, max 50.")]
    public static string AlarmResponseMetrics(
        DuckDbMirror mirror,
        [Description("Dimension to group by: 'operator', 'priority', 'event', 'day', or 'hour'.")]
        string group_by,
        [Description("Start of time window (ISO 8601). Defaults to 24 hours ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Filter by the Velocity event_id that triggered the alarm.")]
        int? event_id = null,
        [Description("Filter by alarm priority level.")]
        int? alarm_level_priority = null,
        [Description("Filter by alarm status code.")]
        int? status = null,
        [Description("Filter by person ID. Use find_people to discover. Same person_id type works across both transaction and alarm tools.")]
        long? person_id = null,
        [Description("Filter by exact workstation_name.")]
        string? workstation_name = null,
        [Description("Maximum number of groups to return. Defaults to 10, max 50.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 10, 50);

        var result = mirror.GetAlarmResponseMetrics(
            group_by, sinceDate, untilDate,
            event_id, alarm_level_priority, status, person_id, workstation_name,
            effectiveLimit);

        var fullCount = result.Groups.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            group_by = result.GroupBy,
            groups = result.Groups.Take(n).Select(g => new
            {
                key = g.Key,
                key_id = g.KeyId,
                total = g.Total,
                avg_ack_minutes = g.AvgAckMinutes,
                avg_clear_minutes = g.AvgClearMinutes,
                p90_ack_minutes = g.P90AckMinutes,
                still_open = g.StillOpen
            }),
            total_alarms = result.TotalAlarms,
            total_unacked = result.TotalUnacked,
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
