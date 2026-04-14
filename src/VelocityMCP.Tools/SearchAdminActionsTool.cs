using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class SearchAdminActionsTool
{
    [McpServerTool(Name = "search_admin_actions", Destructive = false, ReadOnly = true),
     Description("Search the operator audit trail — Velocity's Log_Software table, " +
                 "which records every administrative action (credential added, clearance " +
                 "modified, operator logged in, person added, etc.). " +
                 "Use this for compliance/audit questions: 'who made changes last week', " +
                 "'what did operator Administrator do yesterday', 'show me every credential " +
                 "change this month', 'was anything modified on the Server Room clearance'. " +
                 "Returns total_matching (grand total for the filters) and a bounded list of " +
                 "events — each event has a fully human-readable `description` (e.g. " +
                 "'Credential 1061: SAFR was added by Administrator'), an operator_name " +
                 "resolved via dim_operators, and a raw event_code for downstream filtering. " +
                 "TIME WINDOWS: for phrases like 'last week', 'yesterday', 'this month' pass " +
                 "`relative_window` — do NOT ask the user for ISO dates when a named window fits. " +
                 "FUZZY TOPIC SEARCH: the `description_query` parameter matches any substring " +
                 "of the event description — pass 'Server Room' to find every action that " +
                 "mentions Server Room without needing to know event codes.")]
    public static string SearchAdminActions(
        DuckDbMirror mirror,
        [Description(TimeWindow.ParameterDescription)]
        string? relative_window = null,
        [Description("Start of time window (ISO 8601). Prefer `relative_window` for relative phrases. Defaults to 7 days ago when neither this nor relative_window is set.")]
        string? since = null,
        [Description("End of time window (ISO 8601). Prefer `relative_window`. Defaults to now.")]
        string? until = null,
        [Description("Filter by operator ID. Use find_operators to look up IDs by name. Same id as OperatorId in Velocity.")]
        int? operator_id = null,
        [Description("Filter by raw event code. 1022=operator logon, 1031=operator logoff, 1037=credential added, 1039=credential changed, 1040=person added, 1041=person changed. Usually you don't need this — the description is already human-readable and description_query is a better filter.")]
        int? event_code = null,
        [Description("Case-insensitive substring match against the event description. THE PRIMARY TOPIC FILTER — use this for questions like 'changes to the server room' (pass 'Server Room') or 'credential changes' (pass 'Credential'). Skip event_code when using this.")]
        string? description_query = null,
        [Description("Sort order: 'time_desc' (default, most recent first) or 'time_asc' (chronological).")]
        string? order = null,
        [Description("Maximum number of events to return. Defaults to 20, max 100.")]
        int? limit = null)
    {
        var (sinceDate, untilDate) = TimeWindow.Resolve(
            relative_window, since, until, defaultWindow: TimeSpan.FromDays(7));
        var effectiveOrder = order ?? "time_desc";
        var effectiveLimit = Math.Min(limit ?? 20, 100);

        var (totalMatching, events) = mirror.SampleSoftwareEvents(
            sinceDate, untilDate,
            operator_id, event_code, description_query,
            effectiveOrder, effectiveLimit);

        var fullCount = events.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            total_matching = totalMatching,
            returned = Math.Min(n, fullCount),
            truncated_due_to_size = n < fullCount,
            events = events.Take(n).Select(e => new
            {
                log_id = e.LogId,
                dt_date = e.DtDate.ToString("o"),
                event_code = e.EventCode,
                description = e.Description,
                operator_id = e.OperatorId,
                operator_name = e.OperatorName,
                net_address = e.NetAddress
            }),
            window_used = new
            {
                since = sinceDate.ToString("o"),
                until = untilDate.ToString("o"),
                relative_window,
                defaulted_since = since == null && relative_window == null,
                defaulted_until = until == null && relative_window == null
            }
        }, fullCount);
    }
}
