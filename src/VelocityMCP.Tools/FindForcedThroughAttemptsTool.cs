using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class FindForcedThroughAttemptsTool
{
    [McpServerTool(Name = "find_forced_through_attempts", Destructive = false, ReadOnly = true),
     Description("Find 'denied then granted within N seconds at the same reader' pairs — a credential-share / " +
                 "force-through attempt signal. Surfaces cases where someone was denied at a reader and then " +
                 "another badge successfully passed the same reader seconds later. NOTE: this is NOT true " +
                 "tailgating detection — tailgating leaves no badge record for the second person, so it's " +
                 "undetectable from log data alone. This tool catches the adjacent pattern (someone tried to " +
                 "badge through with a rejected credential, then used a valid one, or someone else let them in). " +
                 "Use for questions like 'any denied-then-granted sequences at the server room yesterday?', " +
                 "'show me credential-share attempts in the last week', or " +
                 "'who was denied at door X right before someone else got in?'. " +
                 "Uses DuckDB window functions (LEAD partitioned by reader) so the pairing is strictly " +
                 "'denied_i followed by the next event at the same reader being granted within max_gap_seconds'. " +
                 "Default window is 24 hours, default max_gap_seconds is 10 (max 300), default limit is 20 (max 100).")]
    public static string FindForcedThroughAttempts(
        DuckDbMirror mirror,
        [Description("Start of time window (ISO 8601). Defaults to 24 hours ago.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Optional. Scope to a specific logical door. Use find_doors to resolve. Resolves to the door's reader names automatically.")]
        int? door_id = null,
        [Description("Maximum gap in seconds between the denied event and the following granted event to count as a pair. Defaults to 10, max 300.")]
        int? max_gap_seconds = null,
        [Description("Maximum number of pairs to return. Defaults to 20, max 100.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddHours(-24);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveGap = Math.Min(Math.Max(max_gap_seconds ?? 10, 1), 300);
        var effectiveLimit = Math.Min(limit ?? 20, 100);

        var result = mirror.GetForcedThroughAttempts(
            sinceDate, untilDate, door_id, effectiveGap, effectiveLimit);

        var fullCount = result.Pairs.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            pairs = result.Pairs.Take(n).Select(p => new
            {
                reader_name = p.ReaderName,
                gap_seconds = p.GapSeconds,
                denied = new
                {
                    log_id = p.DeniedLogId,
                    time = p.DeniedTime.ToString("o"),
                    disposition = p.DeniedDisposition,
                    description = p.DeniedDescription,
                    person_id = p.DeniedPersonId,
                    person_name = p.DeniedPersonName
                },
                granted = new
                {
                    log_id = p.GrantedLogId,
                    time = p.GrantedTime.ToString("o"),
                    description = p.GrantedDescription,
                    person_id = p.GrantedPersonId,
                    person_name = p.GrantedPersonName
                }
            }),
            returned = n,
            total_pairs = result.TotalPairs,
            truncated_due_to_size = n < fullCount,
            max_gap_seconds = effectiveGap,
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
