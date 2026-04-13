using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class ListEventTypesTool
{
    [McpServerTool(Name = "list_event_types", Destructive = false, ReadOnly = true),
     Description("Returns the catalog of all event codes with their category, name, and description. " +
                 "Call this FIRST when the user mentions a specific kind of event " +
                 "(e.g. 'invalid PIN', 'forced open', 'denied access', 'held open') " +
                 "so you can find the matching event_code to pass to count_events or other filter tools. " +
                 "The catalog is small (~a dozen entries) and cheap to fetch.")]
    public static string ListEventTypes(DuckDbMirror mirror)
    {
        var rows = mirror.ListEventTypes();
        var result = new
        {
            event_types = rows.Select(r => new
            {
                event_code = r.EventCode,
                category = r.Category,
                name = r.Name,
                description = r.Description
            }),
            total = rows.Count
        };
        return ResponseShaper.Serialize(result);
    }
}

[McpServerToolType]
public sealed class ListDispositionsTool
{
    [McpServerTool(Name = "list_dispositions", Destructive = false, ReadOnly = true),
     Description("Returns the catalog of disposition codes (Granted, Denied - Invalid Credential, etc.). " +
                 "Call this when the user asks about granted vs denied access and you need to " +
                 "map those words to disposition codes for count_events filtering.")]
    public static string ListDispositions(DuckDbMirror mirror)
    {
        var rows = mirror.ListDispositions();
        var result = new
        {
            dispositions = rows.Select(r => new
            {
                disposition = r.Disposition,
                name = r.Name
            }),
            total = rows.Count
        };
        return ResponseShaper.Serialize(result);
    }
}

[McpServerToolType]
public sealed class LookupAlarmCategoriesTool
{
    [McpServerTool(Name = "lookup_alarm_categories", Destructive = false, ReadOnly = true),
     Description("Returns the catalog of alarm categories (Access, Security, Tamper, Duress, System, Fire). " +
                 "Call this FIRST when the user asks about a class of alarm — 'tamper alarms', 'duress events', " +
                 "'system faults' — so you know which categories exist and their descriptions. " +
                 "Note: this is a discovery catalog; alarm filtering in count_alarms / sample_alarms / aggregate_alarms " +
                 "currently uses event_id and priority, not category_id. Category-level filtering lands when policy " +
                 "mapping is wired up against real Velocity data.")]
    public static string LookupAlarmCategories(DuckDbMirror mirror)
    {
        var rows = mirror.ListAlarmCategories();
        var result = new
        {
            alarm_categories = rows.Select(r => new
            {
                category_id = r.CategoryId,
                name = r.Name,
                description = r.Description
            }),
            total = rows.Count
        };
        return ResponseShaper.Serialize(result);
    }
}
