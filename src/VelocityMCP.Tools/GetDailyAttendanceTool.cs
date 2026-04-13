using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class GetDailyAttendanceTool
{
    [McpServerTool(Name = "get_daily_attendance", Destructive = false, ReadOnly = true),
     Description("First and last granted badge per person per day. Answers operational questions that are " +
                 "structurally hard otherwise: 'what time did the cleaning crew arrive and leave last night?', " +
                 "'who was still in the building after 8pm?', 'how long was Alice onsite yesterday?', " +
                 "'show me everyone who badged in before 6am this week'. " +
                 "Only counts granted events (disposition = 1) and excludes system events (uid1 = 0). " +
                 "Returns one row per (person, day) with first_seen, last_seen, and event_count. " +
                 "Rows are ordered by day DESC, then last_seen DESC — newest departures at the top. " +
                 "Parameters: since/until window, optional person_id to scope to a single person, limit. " +
                 "Default window is the last 24 hours; default limit is 20 (max 100).")]
    public static string GetDailyAttendance(
        DuckDbMirror mirror,
        [Description("Start of time window (ISO 8601). Defaults to 24 hours ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Optional filter: scope to a single person_id (from find_people). Without this, all people in the window are returned.")]
        long? person_id = null,
        [Description("Maximum number of (person, day) rows to return. Defaults to 20, max 100.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 20, 100);

        var result = mirror.GetDailyAttendance(
            sinceDate, untilDate, person_id, effectiveLimit);

        var fullCount = result.Rows.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            rows = result.Rows.Take(n).Select(r => new
            {
                person_id = r.PersonId,
                person_name = r.PersonName,
                day = r.Day.ToString("yyyy-MM-dd"),
                first_seen = r.FirstSeen.ToString("o"),
                last_seen = r.LastSeen.ToString("o"),
                duration_minutes = Math.Round((r.LastSeen - r.FirstSeen).TotalMinutes, 1),
                event_count = r.EventCount
            }),
            returned = n,
            total_rows = result.TotalRows,
            truncated = result.TotalRows > fullCount,
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
