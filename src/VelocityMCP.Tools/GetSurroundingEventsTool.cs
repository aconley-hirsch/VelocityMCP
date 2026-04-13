using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class GetSurroundingEventsTool
{
    [McpServerTool(Name = "get_surrounding_events", Destructive = false, ReadOnly = true),
     Description("Return every transaction and alarm that happened within ±window_minutes of a given timestamp. " +
                 "Answers 'what else was happening when X fired?' without exposing a temporal-join builder the LLM " +
                 "could misuse — the caller reasons about correlation from the raw timeline. " +
                 "Use this for investigation questions like 'what else happened when the loading dock alarm went off at 2am?', " +
                 "'show me everything around the forced-open event at reader 3', or " +
                 "'pull the 10 minutes around this denied badge read'. " +
                 "Parameters: timestamp (required, ISO 8601), window_minutes (default 5, max 60), door_id (optional). " +
                 "When door_id is set, transactions are restricted to that door's readers and alarms are restricted to " +
                 "net_address = dim_doors.controller_addr (best-effort — an unknown door or missing controller_addr " +
                 "returns an empty alarms array). Both arrays are ordered ASC by time, so older rows come first. " +
                 "Default per-side limit is 25, max 50.")]
    public static string GetSurroundingEvents(
        DuckDbMirror mirror,
        [Description("Required. Center of the correlation window (ISO 8601). Everything ± window_minutes is returned.")]
        string timestamp,
        [Description("Half-width of the correlation window in minutes. Defaults to 5, max 60.")]
        int? window_minutes = null,
        [Description("Optional. Scope to a specific logical door. Use find_doors to resolve. If set, the tool uses the door's reader_names for transactions and controller_addr for alarms.")]
        int? door_id = null,
        [Description("Maximum rows per side (transactions and alarms each). Defaults to 25, max 50.")]
        int? limit = null)
    {
        var center = DateTime.Parse(timestamp).ToUniversalTime();
        var effectiveWindow = Math.Min(Math.Max(window_minutes ?? 5, 1), 60);
        var effectiveLimit = Math.Min(limit ?? 25, 50);

        var result = mirror.GetSurroundingEvents(
            center, effectiveWindow, door_id, effectiveLimit);

        // Cap shrinks events + alarms in lockstep if the full envelope overflows 8 KB.
        var fullMax = Math.Max(result.Events.Count, result.Alarms.Count);
        return ResponseShaper.SerializeWithCap(n => new
        {
            timestamp = result.Timestamp.ToString("o"),
            window_minutes = result.WindowMinutes,
            door_id = result.DoorId,
            window = new
            {
                since = result.WindowSince.ToString("o"),
                until = result.WindowUntil.ToString("o")
            },
            events = result.Events.Take(n).Select(e => new
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
            alarms = result.Alarms.Take(n).Select(a => new
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
            events_total = result.Events.Count,
            alarms_total = result.Alarms.Count,
            truncated_due_to_size = n < fullMax
        }, fullMax);
    }
}
