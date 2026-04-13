using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

// ── Alarm query tools ───────────────────────────────────────────────
// These parallel count_events / aggregate_events / sample_events but operate on
// fact_alarms, which has a different schema: no reader_name, uid1 is DOUBLE,
// and the primary discriminators are event_id + alarm_level_priority + status.

[McpServerToolType]
public sealed class CountAlarmsTool
{
    [McpServerTool(Name = "count_alarms", Destructive = false, ReadOnly = true),
     Description("Count alarms in a time window. Returns a single count with the resolved time window. " +
                 "Use this for questions like 'how many alarms fired today?', " +
                 "'how many priority 1 alarms this week?', " +
                 "or 'how many unacknowledged alarms from workstation OPS-01?'. " +
                 "For breakdowns by dimension (event, priority, status, person, workstation) use aggregate_alarms. " +
                 "For individual alarm rows use sample_alarms. Note: alarm filtering is schema-distinct from transactions — " +
                 "use event_id (not event_code) and there is no reader_name filter.")]
    public static string CountAlarms(
        DuckDbMirror mirror,
        [Description("Start of time window (ISO 8601). Defaults to 24 hours ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Filter by the Velocity event_id that triggered the alarm. Use list_event_types to discover codes.")]
        int? event_id = null,
        [Description("Filter by alarm priority level (lower = more severe in Velocity convention).")]
        int? alarm_level_priority = null,
        [Description("Filter by alarm status code (e.g. 0=new, 1=acknowledged, 2=cleared — verify against your Velocity policy).")]
        int? status = null,
        [Description("Filter by person ID (uid1). Use find_people to discover. Note: uid1 is a double in fact_alarms.")]
        long? person_id = null,
        [Description("Filter by exact workstation_name (operator console that received/handled the alarm).")]
        string? workstation_name = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;

        var count = mirror.CountAlarms(
            sinceDate, untilDate,
            event_id, alarm_level_priority, status, person_id, workstation_name);

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

[McpServerToolType]
public sealed class AggregateAlarmsTool
{
    [McpServerTool(Name = "aggregate_alarms", Destructive = false, ReadOnly = true),
     Description("Group alarms by a dimension and return top-N buckets with counts. " +
                 "Workhorse for 'top X' and 'breakdown by Y' questions on alarms like " +
                 "'top 5 alarm priorities this week', 'alarms per workstation today', " +
                 "'how many distinct event_ids fired alarms' (use total_groups for that), or " +
                 "'alarm volume per hour across the last 24h'. " +
                 "Response includes total_events (grand total), total_groups (distinct values), " +
                 "truncated flag, and the resolved time window. " +
                 "Supported group_by values: event, priority, status, person, workstation, hour, day. " +
                 "Filters work the same as count_alarms.")]
    public static string AggregateAlarms(
        DuckDbMirror mirror,
        [Description("Dimension to group by: 'event', 'priority', 'status', 'person', 'workstation', 'hour', or 'day'.")]
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
        [Description("Filter by person ID (uid1).")]
        long? person_id = null,
        [Description("Filter by exact workstation_name.")]
        string? workstation_name = null,
        [Description("Maximum number of groups to return. Defaults to 10, max 50.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 10, 50);

        var result = mirror.AggregateAlarms(
            group_by, sinceDate, untilDate,
            event_id, alarm_level_priority, status, person_id, workstation_name,
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

[McpServerToolType]
public sealed class SampleAlarmsTool
{
    [McpServerTool(Name = "sample_alarms", Destructive = false, ReadOnly = true),
     Description("Return a bounded sample of individual alarms matching filters. " +
                 "Use this for questions about SPECIFIC alarms rather than counts or breakdowns — " +
                 "e.g. 'show me the most recent priority 1 alarms', " +
                 "'what was the last alarm from workstation OPS-01?' (limit=1, order=time_desc), " +
                 "or 'list today's unacknowledged alarms'. " +
                 "For counts and aggregations use count_alarms or aggregate_alarms instead. " +
                 "Response includes total_matching (how many matched before the limit), " +
                 "truncated flag, and the resolved time window. " +
                 "Default limit is 10, max is 50. To see the full row (ak_operator, parm1, parm2, etc.) " +
                 "call get_alarm on the alarm_id.")]
    public static string SampleAlarms(
        DuckDbMirror mirror,
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
        [Description("Filter by person ID (uid1).")]
        long? person_id = null,
        [Description("Filter by exact workstation_name.")]
        string? workstation_name = null,
        [Description("Sort order: 'time_desc' (most recent first, default) or 'time_asc' (oldest first).")]
        string? order = null,
        [Description("Maximum number of alarms to return. Defaults to 10, max 50.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 10, 50);
        var effectiveOrder = order ?? "time_desc";

        var result = mirror.SampleAlarms(
            sinceDate, untilDate,
            event_id, alarm_level_priority, status, person_id, workstation_name,
            effectiveOrder, effectiveLimit);

        var payload = new
        {
            alarms = result.Alarms.Select(a => new
            {
                alarm_id = a.AlarmId,
                time = a.DtDate?.ToString("o"),
                ak_time = a.AkDate?.ToString("o"),
                cl_time = a.ClDate?.ToString("o"),
                event_id = a.EventId,
                alarm_level_priority = a.AlarmLevelPriority,
                status = a.Status,
                description = a.Description,
                person_id = a.PersonId,
                person_name = a.PersonName,
                workstation_name = a.WorkstationName
            }),
            returned = result.Alarms.Count,
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
