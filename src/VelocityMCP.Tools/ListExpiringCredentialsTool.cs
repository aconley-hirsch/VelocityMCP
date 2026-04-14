using System.ComponentModel;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class ListExpiringCredentialsTool
{
    [McpServerTool(Name = "list_expiring_credentials", Destructive = false, ReadOnly = true),
     Description("List credentials (badges/cards) by their expiration date. " +
                 "Use for: 'whose badge is expiring soon', 'show me all credentials expiring " +
                 "this month', 'who has an expired badge', 'does anyone have a perpetual " +
                 "credential' (include_perpetual=true), or 'why was Alice denied yesterday' " +
                 "(call this with person_id and include_expired=true to surface a lapsed badge). " +
                 "Returns one row per matching credential with the owner's name from dim_people, " +
                 "the credential_id, the expiration/activation dates, and days_until_expiry " +
                 "(negative for already-expired, null for perpetual). " +
                 "DENIAL TRIAGE: if check_authorization returns allowed=false for a person who " +
                 "DOES hold a matching clearance, the most common cause is an expired credential — " +
                 "call this next with that person_id and include_expired=true.")]
    public static string ListExpiringCredentials(
        DuckDbMirror mirror,
        [Description("Lookahead window in days — credentials expiring on or before (now + this many days) are returned. Defaults to 30 days.")]
        int? within_days = null,
        [Description("Also return credentials that have already expired (expiration_date in the past). Default false. Set true for denial triage and 'who has an expired badge' questions.")]
        bool? include_expired = null,
        [Description("Also return credentials with no expiration date (perpetual). Default false. Set true for 'who has an indefinite badge' or 'find credentials that never expire' questions.")]
        bool? include_perpetual = null,
        [Description("Filter to a single person_id (a real HostUserId from find_people or aggregate_events group_by='person'). Useful for denial triage on a specific user.")]
        long? person_id = null,
        [Description("Maximum number of credentials to return. Defaults to 50, max 500.")]
        int? limit = null)
    {
        var effectiveWithinDays = within_days ?? 30;
        var effectiveIncludeExpired = include_expired ?? false;
        var effectiveIncludePerpetual = include_perpetual ?? false;
        var effectiveLimit = Math.Min(limit ?? 50, 500);

        var rows = mirror.ListExpiringCredentials(
            effectiveWithinDays, effectiveIncludeExpired, effectiveIncludePerpetual,
            person_id, effectiveLimit);

        var fullCount = rows.Count;
        return ResponseShaper.SerializeWithCap(n => new
        {
            returned = Math.Min(n, fullCount),
            truncated_due_to_size = n < fullCount,
            filter = new
            {
                within_days = effectiveWithinDays,
                include_expired = effectiveIncludeExpired,
                include_perpetual = effectiveIncludePerpetual,
                person_id
            },
            credentials = rows.Take(n).Select(r => new
            {
                credential_id = r.CredentialId,
                person_id = r.PersonId,
                person_name = r.PersonName ?? $"(unknown person {r.PersonId})",
                activation_date = r.ActivationDate?.ToString("o"),
                expiration_date = r.ExpirationDate?.ToString("o"),
                is_activated = r.IsActivated,
                expiration_used = r.ExpirationUsed,
                days_until_expiry = r.DaysUntilExpiry,
                status = DeriveStatus(r)
            })
        }, fullCount);
    }

    private static string DeriveStatus(ExpiringCredentialRow r)
    {
        if (r.ExpirationDate is null) return "perpetual";
        if (r.DaysUntilExpiry is < 0) return "expired";
        if (r.DaysUntilExpiry is <= 7) return "expiring_soon";
        return "expiring_later";
    }
}
