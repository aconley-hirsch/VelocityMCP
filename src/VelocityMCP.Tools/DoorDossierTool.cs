using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class DoorDossierTool
{
    [McpServerTool(Name = "door_dossier", Destructive = false, ReadOnly = true),
     Description("One-call activity report for a single door (the location-dimension counterpart to person_dossier). " +
                 "Replaces a 5-call workflow with one envelope containing: summary " +
                 "(total_access, total_denied, total_alarms, distinct_people, busiest_hour, quietest_hour, first_seen, last_seen), " +
                 "reader_names (physical readers resolved from dim_readers for this logical door), " +
                 "hourly_traffic (24 zero-filled buckets 0..23), top_users, recent_denials, recent_alarms. " +
                 "Multi-reader doors are handled correctly: the transaction-side sections query every reader " +
                 "belonging to the door. Alarm-side sections are best-effort joined on " +
                 "fact_alarms.net_address = dim_doors.controller_addr — a door with no controller_addr returns zero alarms. " +
                 "Default window is 7 days; default limits are 5 (max 20). " +
                 "'Access' = disposition 1; 'denied' = disposition > 1.")]
    public static string DoorDossier(
        DuckDbMirror mirror,
        [Description("Required. Velocity door_id. Use find_doors to resolve from a name.")]
        int door_id,
        [Description("Start of time window (ISO 8601). Defaults to 7 days ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Number of top users (people) to return. Defaults to 5, max 20.")]
        int? top_users_limit = null,
        [Description("Number of recent denials and recent alarms to return (each). Defaults to 5, max 20.")]
        int? recent_limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddDays(-7);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveTop = Math.Min(top_users_limit ?? 5, 20);
        var effectiveRecent = Math.Min(recent_limit ?? 5, 20);

        var result = mirror.GetDoorDossier(
            door_id, sinceDate, untilDate, effectiveTop, effectiveRecent);

        var fullRecent = Math.Max(result.RecentDenials.Count, result.RecentAlarms.Count);
        return ResponseShaper.SerializeWithCap(n => new
        {
            door_id = result.DoorId,
            door_name = result.DoorName,
            reader_names = result.ReaderNames,
            summary = new
            {
                total_access = result.Summary.TotalAccess,
                total_denied = result.Summary.TotalDenied,
                total_alarms = result.Summary.TotalAlarms,
                distinct_people = result.Summary.DistinctPeople,
                busiest_hour = result.Summary.BusiestHour,
                quietest_hour = result.Summary.QuietestHour,
                first_seen = result.Summary.FirstSeen?.ToString("o"),
                last_seen = result.Summary.LastSeen?.ToString("o")
            },
            hourly_traffic = result.HourlyTraffic.Select(h => new
            {
                hour = h.Hour,
                count = h.Count
            }),
            top_users = result.TopUsers.Select(u => new
            {
                person_id = u.PersonId,
                person_name = u.PersonName,
                count = u.Count
            }),
            recent_denials = result.RecentDenials.Take(n).Select(e => new
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
            recent_alarms = result.RecentAlarms.Take(n).Select(a => new
            {
                alarm_id = a.AlarmId,
                time = a.DtDate?.ToString("o"),
                ak_time = a.AkDate?.ToString("o"),
                cl_time = a.ClDate?.ToString("o"),
                event_id = a.EventId,
                alarm_level_priority = a.AlarmLevelPriority,
                status = a.Status,
                description = a.Description,
                workstation_name = a.WorkstationName
            }),
            truncated_due_to_size = n < fullRecent,
            window_used = new
            {
                since = sinceDate.ToString("o"),
                until = untilDate.ToString("o"),
                defaulted_since = since == null,
                defaulted_until = until == null
            }
        }, fullRecent);
    }
}
