using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class FindOperatorsTool
{
    [McpServerTool(Name = "find_operators", Destructive = false, ReadOnly = true),
     Description("Fuzzy search for Velocity operators (admins / workstation users). " +
                 "Returns up to `limit` matches with their operator_id, login name, full " +
                 "name, description, and enabled flag. " +
                 "Call this BEFORE search_admin_actions when the user mentions an operator " +
                 "by name (e.g. 'what did Administrator do last week', 'show me every change " +
                 "made by the security team') — the result gives you the operator_id to pass " +
                 "as the filter. " +
                 "This is the operator-side companion to find_people. Operators are a separate " +
                 "entity from people: operators are the admins who log into Velocity to manage " +
                 "it, while people are the cardholders.")]
    public static string FindOperators(
        DuckDbMirror mirror,
        [Description("Search term (partial name, full name, or description). Case-insensitive. Examples: 'admin', 'security', 'auditor'.")]
        string query,
        [Description("Maximum number of matches to return. Defaults to 5.")]
        int? limit = null)
    {
        var effectiveLimit = limit ?? 5;
        var matches = mirror.SearchOperators(query, effectiveLimit);

        var result = new
        {
            query,
            matches = matches.Select(m => new
            {
                operator_id = m.OperatorId,
                name = m.Name,
                full_name = m.FullName,
                description = m.Description,
                enabled = m.Enabled
            }),
            total = matches.Count,
            truncated = matches.Count == effectiveLimit
        };
        return ResponseShaper.Serialize(result);
    }
}
