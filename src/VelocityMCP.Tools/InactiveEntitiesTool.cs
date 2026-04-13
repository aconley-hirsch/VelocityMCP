using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class InactiveEntitiesTool
{
    [McpServerTool(Name = "inactive_entities", Destructive = false, ReadOnly = true),
     Description("Find people, doors, or readers with ZERO activity in a time window. " +
                 "Set-difference query that's structurally hard for an LLM to compose from " +
                 "count_events alone (would need N+1 calls). Use this for compliance, offboarding, " +
                 "and physical-health questions like 'which employees haven't badged in for 30 days?', " +
                 "'are there any doors that haven't been used this month?', " +
                 "or 'show me readers with no activity this week — they might be offline'. " +
                 "Each result includes last_seen_at (the all-time most recent event) so you can " +
                 "distinguish 'never seen' (null) from 'inactive in the window but seen earlier'. " +
                 "For entity='door', multi-reader doors are collapsed via dim_readers — one row per logical door. " +
                 "Default window is 30 days, default limit is 20 (max 50).")]
    public static string InactiveEntities(
        DuckDbMirror mirror,
        [Description("Which dimension to check: 'person', 'door' (logical, collapses multi-reader doors), or 'reader' (physical).")]
        string entity,
        [Description("Start of time window (ISO 8601). Defaults to 30 days ago. An entity is 'inactive' if it has zero events between since and until.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Defaults to now.")]
        string? until = null,
        [Description("Maximum number of inactive entities to return. Defaults to 20, max 50.")]
        int? limit = null)
    {
        var sinceDate = since != null ? DateTime.Parse(since).ToUniversalTime() : DateTime.UtcNow.AddDays(-30);
        var untilDate = until != null ? DateTime.Parse(until).ToUniversalTime() : DateTime.UtcNow;
        var effectiveLimit = Math.Min(limit ?? 20, 50);

        var result = mirror.GetInactiveEntities(entity, sinceDate, untilDate, effectiveLimit);

        var fullCount = result.Items.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            entity = result.Entity,
            inactive = result.Items.Take(n).Select(i => new
            {
                id = i.Id,
                name = i.Name,
                last_seen_at = i.LastSeenAt?.ToString("o"),
                ever_seen = i.LastSeenAt != null
            }),
            returned = n,
            inactive_total = result.InactiveTotal,
            total_entities = result.TotalEntities,
            active_count = result.TotalEntities - result.InactiveTotal,
            sample_truncated = result.InactiveTotal > fullCount,
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
