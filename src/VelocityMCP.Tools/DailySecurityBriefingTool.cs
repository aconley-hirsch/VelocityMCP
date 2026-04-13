using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class DailySecurityBriefingTool
{
    [McpServerTool(Name = "daily_security_briefing", Destructive = false, ReadOnly = true),
     Description("One-call morning briefing that composes 6 internal queries into a single situational-awareness " +
                 "envelope for a given calendar day. The synthesis capstone — replaces a manual workflow that " +
                 "would otherwise require count_events + count_alarms + aggregate_events(door) + sample_events " +
                 "(forced) + aggregate_events(person, disposition>1) + sample_alarms(unacked). " +
                 "Returns: headline (total_access, total_denied, total_alarms, alarms_unacked, forced_opens, held_opens), " +
                 "prior day's same metrics, vs_prior_day (headline minus prior), " +
                 "forced_open_events (samples from fact_transactions event_code=4 — no person attached), " +
                 "notable_deniers (people with ≥ threshold denied events), " +
                 "busiest_doors (collapsed through dim_readers so multi-reader doors count once), " +
                 "open_alarms (ak_date IS NULL, most recent first). " +
                 "Use for 'give me the morning briefing', 'what happened yesterday', 'compliance status for a given day'. " +
                 "Default date is yesterday; default notable_deniers_threshold is 3. " +
                 "All section limits default to 5 (max 20).")]
    public static string DailySecurityBriefing(
        DuckDbMirror mirror,
        [Description("Calendar date (ISO 8601, e.g. '2026-04-12'). Defaults to yesterday (UTC).")]
        string? date = null,
        [Description("Person qualifies as a notable denier when their denied-event count within the day meets or exceeds this threshold. Default 3.")]
        int? notable_deniers_threshold = null,
        [Description("Max rows for busiest_doors and notable_deniers. Default 5, max 20.")]
        int? top_limit = null,
        [Description("Max rows for forced_open_events samples. Default 5, max 20.")]
        int? forced_open_sample_limit = null,
        [Description("Max rows for open_alarms. Default 5, max 20.")]
        int? open_alarms_limit = null)
    {
        // "yesterday" anchored to UTC midnight so the window is a full calendar day
        var parsedDate = date != null
            ? DateTime.Parse(date).ToUniversalTime().Date
            : DateTime.UtcNow.Date.AddDays(-1);

        var effectiveThreshold = Math.Max(notable_deniers_threshold ?? 3, 1);
        var effectiveTop = Math.Min(top_limit ?? 5, 20);
        var effectiveForced = Math.Min(forced_open_sample_limit ?? 5, 20);
        var effectiveOpen = Math.Min(open_alarms_limit ?? 5, 20);

        var result = mirror.GetDailySecurityBriefing(
            parsedDate, effectiveTop, effectiveThreshold, effectiveForced, effectiveOpen);

        static object Metrics(DailySecurityBriefingMetrics m) => new
        {
            total_access = m.TotalAccess,
            total_denied = m.TotalDenied,
            total_alarms = m.TotalAlarms,
            alarms_unacked = m.AlarmsUnacked,
            forced_opens = m.ForcedOpens,
            held_opens = m.HeldOpens
        };

        // Shrink all four variable collections in lockstep if the envelope
        // overflows 8 KB — there are five sections competing for the budget
        // (headline/prior/deltas are fixed-size).
        var fullMax = new[]
        {
            result.ForcedOpenEvents.Count,
            result.NotableDeniers.Count,
            result.BusiestDoors.Count,
            result.OpenAlarms.Count
        }.Max();

        return ResponseShaper.SerializeWithCap(n => new
        {
            date = result.DayStart.ToString("yyyy-MM-dd"),
            window = new
            {
                since = result.DayStart.ToString("o"),
                until = result.DayEnd.ToString("o")
            },
            prior_window = new
            {
                since = result.PriorStart.ToString("o"),
                until = result.PriorEnd.ToString("o")
            },
            headline = Metrics(result.Headline),
            prior = Metrics(result.Prior),
            vs_prior_day = Metrics(result.VsPriorDay),
            forced_open_events = result.ForcedOpenEvents.Take(n).Select(e => new
            {
                log_id = e.LogId,
                time = e.DtDate.ToString("o"),
                reader_name = e.ReaderName,
                description = e.Description
            }),
            notable_deniers = result.NotableDeniers.Take(n).Select(d => new
            {
                person_id = d.PersonId,
                person_name = d.PersonName,
                denial_count = d.DenialCount
            }),
            busiest_doors = result.BusiestDoors.Take(n).Select(d => new
            {
                door_id = d.DoorId,
                name = d.DoorName,
                count = d.Count
            }),
            open_alarms = result.OpenAlarms.Take(n).Select(a => new
            {
                alarm_id = a.AlarmId,
                time = a.DtDate?.ToString("o"),
                event_id = a.EventId,
                alarm_level_priority = a.AlarmLevelPriority,
                description = a.Description,
                workstation_name = a.WorkstationName
            }),
            truncated_due_to_size = n < fullMax
        }, fullMax);
    }
}
