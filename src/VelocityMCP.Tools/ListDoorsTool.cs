using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class ListDoorsTool
{
    [McpServerTool(Name = "list_doors", Destructive = false, ReadOnly = true),
     Description("Return the full door catalog with activity-derived status. " +
                 "Answers 'show me all doors in the system', 'which doors have had activity today?', " +
                 "'which doors might be offline?', 'give me a door health roll-up'. " +
                 "For each door returns: door_id, name, controller_addr, reader_count, reader_names, " +
                 "admin_active (dim_doors.active flag), last_seen_at (most recent event across all the door's readers), " +
                 "events_in_window (count in the last active_window_hours), open_alarms (unacked alarms " +
                 "best-effort joined via net_address = controller_addr), and a derived status label. " +
                 "Status values: " +
                 "'active' (events_in_window > 0), " +
                 "'quiet' (no events in window but last_seen_at within 7 days — usually means a low-traffic door, not broken), " +
                 "'stale' (last_seen_at older than 7 days — possibly misconfigured or offline), " +
                 "'never_seen' (configured but no events ever — possibly a new door or a misconfiguration). " +
                 "IMPORTANT: this status is DERIVED from historical events, NOT live Velocity device state " +
                 "(online/offline, locked/unlocked, position sensor). Real-time device status would require " +
                 "a live SDK call, which is not currently available. For door-scoped investigation details " +
                 "call door_dossier on the door_id. Default window is 24 hours; default limit is 50 (max 200).")]
    public static string ListDoors(
        DuckDbMirror mirror,
        [Description("Hours to count toward the 'active' status label and events_in_window field. Defaults to 24.")]
        int? active_window_hours = null,
        [Description("Include doors with status='never_seen' or 'stale'. Defaults to true — set false to see only doors with recent activity.")]
        bool? include_inactive = null,
        [Description("Maximum number of doors to return. Defaults to 50, max 200.")]
        int? limit = null)
    {
        var effectiveWindow = Math.Max(active_window_hours ?? 24, 1);
        var effectiveInclude = include_inactive ?? true;
        var effectiveLimit = Math.Min(limit ?? 50, 200);

        var rows = mirror.ListDoors(effectiveWindow, effectiveInclude, effectiveLimit);
        var fullCount = rows.Count;

        // Roll-up counts for quick LLM consumption without re-iterating the list.
        var summary = new
        {
            total = fullCount,
            active = rows.Count(r => r.Status == "active"),
            quiet = rows.Count(r => r.Status == "quiet"),
            stale = rows.Count(r => r.Status == "stale"),
            never_seen = rows.Count(r => r.Status == "never_seen"),
            with_open_alarms = rows.Count(r => r.OpenAlarms > 0),
        };

        return ResponseShaper.SerializeWithCap(n => new
        {
            summary,
            doors = rows.Take(n).Select(d => new
            {
                door_id = d.DoorId,
                name = d.Name,
                status = d.Status,
                controller_addr = d.ControllerAddr,
                admin_active = d.AdminActive,
                reader_count = d.ReaderCount,
                reader_names = d.ReaderNames,
                last_seen_at = d.LastSeenAt?.ToString("o"),
                events_in_window = d.EventsInWindow,
                open_alarms = d.OpenAlarms
            }),
            returned = n,
            active_window_hours = effectiveWindow,
            truncated_due_to_size = n < fullCount,
            notes = "status is derived from historical event activity, not real-time device state."
        }, fullCount);
    }
}
