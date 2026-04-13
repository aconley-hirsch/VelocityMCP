using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class PersonDossierTool
{
    [McpServerTool(Name = "person_dossier", Destructive = false, ReadOnly = true),
     Description("One-call investigation report for a single person. Replaces a 5-call workflow " +
                 "(count, aggregate by door, aggregate by hour, sample denials, sample alarms) with a single " +
                 "envelope containing: summary (total_events, total_denials, total_alarms, distinct_doors, " +
                 "first_seen, last_seen), top_doors (collapsed through dim_readers so multi-reader doors " +
                 "count once), hourly_pattern (24 zero-filled buckets 0..23), recent_denials, recent_alarms. " +
                 "Use for compliance audits, offboarding checks, and incident reviews. " +
                 "Call find_people first to resolve a name to person_id. " +
                 "Default window is 30 days; default recent_limit is 5 (max 20). " +
                 "'Denied' means disposition > 1 (any non-Granted credentialed event).")]
    public static string PersonDossier(
        DuckDbMirror mirror,
        [Description("Required. Velocity person_id. Use find_people to resolve from a name.")]
        long person_id,
        [Description("Start of time window (ISO 8601). Defaults to 30 days ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Number of top doors to return. Defaults to 5, max 20.")]
        int? top_doors_limit = null,
        [Description("Number of recent denials and recent alarms to return (each). Defaults to 5, max 20.")]
        int? recent_limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddDays(-30);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveTop = Math.Min(top_doors_limit ?? 5, 20);
        var effectiveRecent = Math.Min(recent_limit ?? 5, 20);

        var result = mirror.GetPersonDossier(
            person_id, sinceDate, untilDate, effectiveTop, effectiveRecent);

        // Binary-search cap: shrink recent_denials + recent_alarms in lockstep if the
        // full envelope exceeds 8KB. Top-level summary, top_doors, and hourly_pattern
        // are always included (they are small and fixed-size).
        var fullRecent = Math.Max(result.RecentDenials.Count, result.RecentAlarms.Count);
        return ResponseShaper.SerializeWithCap(n => new
        {
            person_id = result.PersonId,
            person_name = result.PersonName,
            summary = new
            {
                total_events = result.Summary.TotalEvents,
                total_denials = result.Summary.TotalDenials,
                total_alarms = result.Summary.TotalAlarms,
                distinct_doors = result.Summary.DistinctDoors,
                first_seen = result.Summary.FirstSeen?.ToString("o"),
                last_seen = result.Summary.LastSeen?.ToString("o")
            },
            top_doors = result.TopDoors.Select(d => new
            {
                door_id = d.DoorId,
                name = d.DoorName,
                count = d.Count
            }),
            hourly_pattern = result.HourlyPattern.Select(h => new
            {
                hour = h.Hour,
                count = h.Count
            }),
            recent_denials = result.RecentDenials.Take(n).Select(e => new
            {
                log_id = e.LogId,
                time = e.DtDate.ToString("o"),
                event_code = e.EventCode,
                description = e.Description,
                disposition = e.Disposition,
                reader_name = e.ReaderName
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
