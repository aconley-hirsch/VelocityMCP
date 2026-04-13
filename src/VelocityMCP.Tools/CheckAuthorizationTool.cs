using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class CheckAuthorizationTool
{
    [McpServerTool(Name = "check_authorization", Destructive = false, ReadOnly = true),
     Description("Answer policy questions — 'who is ALLOWED to enter X?' — not historical questions. " +
                 "Every other tool reports what DID happen; this tool reports what SHOULD happen according to " +
                 "Velocity's clearance/access-level policy. " +
                 "Three modes, auto-detected from which parameters are set: " +
                 "(1) person_id + door_id → point query: 'is this person currently authorized at this door?' " +
                 "Returns authorized yes/no plus the list of clearances that grant it. " +
                 "(2) person_id alone → 'which doors can this person access?' Returns a collapsed door list " +
                 "(multi-reader doors count once) with the clearances granting each. " +
                 "(3) door_id alone → 'who can access this door?' Returns a people list with the clearances granting each. " +
                 "Authorization is evaluated at the CURRENT moment: only assignments with no expires_at or expires_at > now count. " +
                 "NOTE: time-of-day schedules are not enforced in v1 — the tool reports whether the person HOLDS a " +
                 "clearance that maps to the reader, and surfaces the clearance's schedule_name (e.g. '24x7', 'Business Hours 8-18 M-F') " +
                 "so the caller can reason about whether the current time is within schedule.")]
    public static string CheckAuthorization(
        DuckDbMirror mirror,
        [Description("Person to check. Use find_people to resolve a name to person_id.")]
        long? person_id = null,
        [Description("Door to check. Use find_doors to resolve a name to door_id.")]
        int? door_id = null,
        [Description("Max rows for the people list when only door_id is supplied. Defaults to 25, max 100.")]
        int? limit = null)
    {
        var effectiveLimit = Math.Min(limit ?? 25, 100);

        // Mode 1: point query
        if (person_id.HasValue && door_id.HasValue)
        {
            var result = mirror.CheckAuthorization(person_id.Value, door_id.Value);
            return ResponseShaper.Serialize(new
            {
                mode = "point",
                person_id = result.PersonId,
                door_id = result.DoorId,
                authorized = result.Authorized,
                via_clearances = result.GrantingClearances.Select(c => new
                {
                    clearance_id = c.ClearanceId,
                    name = c.Name,
                    schedule_name = c.ScheduleName,
                    granted_at = c.GrantedAt.ToString("o"),
                    expires_at = c.ExpiresAt?.ToString("o")
                }),
                reason = result.Authorized
                    ? "Person holds one or more currently-active clearances that map to this door."
                    : "Person holds no currently-active clearance mapping to any reader on this door.",
                notes = "Schedule enforcement is deferred. Check via_clearances[].schedule_name if the caller cares about time-of-day policy."
            });
        }

        // Mode 2: all doors for a person
        if (person_id.HasValue)
        {
            var doors = mirror.GetAuthorizedDoors(person_id.Value);
            var fullCount = doors.Count;
            return ResponseShaper.SerializeWithCap(n => new
            {
                mode = "person_to_doors",
                person_id = person_id.Value,
                authorized_doors = doors.Take(n).Select(d => new
                {
                    door_id = d.DoorId,
                    name = d.DoorName,
                    via_clearances = d.ViaClearances
                }),
                returned = n,
                total = fullCount,
                truncated_due_to_size = n < fullCount
            }, fullCount);
        }

        // Mode 3: all people for a door
        if (door_id.HasValue)
        {
            var people = mirror.GetAuthorizedPeopleForDoor(door_id.Value, effectiveLimit);
            var fullCount = people.Count;
            return ResponseShaper.SerializeWithCap(n => new
            {
                mode = "door_to_people",
                door_id = door_id.Value,
                authorized_people = people.Take(n).Select(p => new
                {
                    person_id = p.PersonId,
                    person_name = p.PersonName,
                    via_clearances = p.ViaClearances
                }),
                returned = n,
                total = fullCount,
                truncated = fullCount == effectiveLimit,
                truncated_due_to_size = n < fullCount
            }, fullCount);
        }

        // Neither parameter supplied
        return ResponseShaper.Serialize(new
        {
            error = "check_authorization requires at least person_id or door_id (or both).",
            usage = "Set person_id + door_id for a point query, person_id alone for all-doors-for-a-person, or door_id alone for all-people-for-a-door."
        });
    }
}
