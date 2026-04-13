using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class FindDoorsTool
{
    [McpServerTool(Name = "find_doors", Destructive = false, ReadOnly = true),
     Description("Fuzzy search for doors by name. Returns up to `limit` matches. " +
                 "Each match includes the door's logical name AND its list of physical reader names " +
                 "(a door often has multiple readers — entry + exit, or multiple turnstiles). " +
                 "Call this BEFORE count_events/sample_events/aggregate_events when the user mentions " +
                 "a door by partial name (e.g. 'front door', 'server room'). " +
                 "IMPORTANT: the `readers` array on each match is what you pass as the `reader_names` " +
                 "filter — NOT the door name itself. The fact tables store reader names, not door names. " +
                 "If only one door matches, pass all its readers in one call. " +
                 "If multiple doors match, pick the best one or ask the user to disambiguate.")]
    public static string FindDoors(
        DuckDbMirror mirror,
        [Description("Search term (partial name). Case-insensitive. Examples: 'front', 'server', 'parking'.")]
        string query,
        [Description("Maximum number of matches to return. Defaults to 5.")]
        int? limit = null)
    {
        var effectiveLimit = limit ?? 5;
        var matches = mirror.SearchDoors(query, effectiveLimit);

        var result = new
        {
            query,
            matches = matches.Select(m => new
            {
                door_id = m.DoorId,
                name = m.Name,
                controller_addr = m.ControllerAddr,
                readers = m.Readers
            }),
            total = matches.Count,
            truncated = matches.Count == effectiveLimit
        };
        return ResponseShaper.Serialize(result);
    }
}

[McpServerToolType]
public sealed class FindReadersTool
{
    [McpServerTool(Name = "find_readers", Destructive = false, ReadOnly = true),
     Description("Fuzzy search for readers by name. Reader names are what count_events uses " +
                 "for the reader_name filter. Use this when the user names a reader or when " +
                 "find_doors didn't return the specific reader you need. " +
                 "Returns up to `limit` matches with their reader IDs and exact names.")]
    public static string FindReaders(
        DuckDbMirror mirror,
        [Description("Search term (partial name). Case-insensitive.")]
        string query,
        [Description("Maximum number of matches to return. Defaults to 5.")]
        int? limit = null)
    {
        var effectiveLimit = limit ?? 5;
        var matches = mirror.SearchReaders(query, effectiveLimit);

        var result = new
        {
            query,
            matches = matches.Select(m => new
            {
                reader_id = m.ReaderId,
                name = m.Name
            }),
            total = matches.Count,
            truncated = matches.Count == effectiveLimit
        };
        return ResponseShaper.Serialize(result);
    }
}

[McpServerToolType]
public sealed class FindPeopleTool
{
    [McpServerTool(Name = "find_people", Destructive = false, ReadOnly = true),
     Description("Fuzzy search for people by name. Returns up to `limit` matches with their person IDs (UID1). " +
                 "Call this BEFORE count_events when the user mentions someone by name " +
                 "(e.g. 'Jane Smith', 'Johnson', 'Alice'). " +
                 "The result gives you the person_id to pass as a filter. " +
                 "If multiple candidates are returned, present them to the user or pick the best match.")]
    public static string FindPeople(
        DuckDbMirror mirror,
        [Description("Search term (full name, first name, or last name). Case-insensitive. Examples: 'jane', 'smith', 'Jane Smith'.")]
        string query,
        [Description("Maximum number of matches to return. Defaults to 5.")]
        int? limit = null)
    {
        var effectiveLimit = limit ?? 5;
        var matches = mirror.SearchPeople(query, effectiveLimit);

        var result = new
        {
            query,
            matches = matches.Select(m => new
            {
                person_id = m.PersonId,
                first_name = m.FirstName,
                last_name = m.LastName,
                full_name = m.FullName
            }),
            total = matches.Count,
            truncated = matches.Count == effectiveLimit
        };
        return ResponseShaper.Serialize(result);
    }
}
