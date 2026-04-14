namespace VelocityMCP.Tools;

/// <summary>
/// Resolves a time window from either a named relative shortcut OR explicit
/// since/until ISO 8601 strings. Lets tools accept "last_7d" instead of
/// forcing the caller to compute timestamps — small local models ask the
/// user for dates rather than do date math themselves.
/// </summary>
internal static class TimeWindow
{
    public const string ParameterDescription =
        "Named relative time window. Use this for phrases like 'today', 'yesterday', " +
        "'last week', 'this month' — NEVER ask the user for dates when a named window fits. " +
        "Accepted values: 'today', 'yesterday', 'last_24h', 'last_7d' (alias 'last_week'), " +
        "'last_30d' (alias 'last_month'), 'this_week' (Mon 00:00 local → now), " +
        "'this_month' (1st 00:00 local → now). Ignored when both `since` and `until` are supplied.";

    public static (DateTime since, DateTime until) Resolve(
        string? relativeWindow,
        string? since,
        string? until,
        TimeSpan defaultWindow)
    {
        // Explicit since/until win if provided.
        if (since != null && until != null)
        {
            return (DateTime.Parse(since).ToUniversalTime(),
                    DateTime.Parse(until).ToUniversalTime());
        }

        if (!string.IsNullOrWhiteSpace(relativeWindow))
        {
            return ResolveRelative(relativeWindow.Trim().ToLowerInvariant());
        }

        // Partial explicit + default for the missing end.
        var nowUtc = DateTime.UtcNow;
        var resolvedSince = since != null
            ? DateTime.Parse(since).ToUniversalTime()
            : nowUtc - defaultWindow;
        var resolvedUntil = until != null
            ? DateTime.Parse(until).ToUniversalTime()
            : nowUtc;
        return (resolvedSince, resolvedUntil);
    }

    private static (DateTime since, DateTime until) ResolveRelative(string key)
    {
        var nowUtc = DateTime.UtcNow;
        var localNow = DateTime.Now;
        var localToday = localNow.Date;

        return key switch
        {
            "today"      => (localToday.ToUniversalTime(), nowUtc),
            "yesterday"  => (localToday.AddDays(-1).ToUniversalTime(),
                             localToday.ToUniversalTime()),
            "last_24h" or "last_1d" or "last_day" =>
                            (nowUtc.AddDays(-1), nowUtc),
            "last_7d" or "last_week" or "past_week" =>
                            (nowUtc.AddDays(-7), nowUtc),
            "last_30d" or "last_month" or "past_month" =>
                            (nowUtc.AddDays(-30), nowUtc),
            "this_week"  => (StartOfWeek(localToday).ToUniversalTime(), nowUtc),
            "this_month" => (new DateTime(localToday.Year, localToday.Month, 1,
                                          0, 0, 0, DateTimeKind.Local).ToUniversalTime(),
                             nowUtc),
            _ => throw new ArgumentException(
                    $"Unknown relative_window '{key}'. " +
                    "Valid values: today, yesterday, last_24h, last_7d (last_week), " +
                    "last_30d (last_month), this_week, this_month.")
        };
    }

    private static DateTime StartOfWeek(DateTime date)
    {
        // ISO week: Monday is day 0. .NET DayOfWeek has Sunday=0, Monday=1, ...
        int daysFromMonday = ((int)date.DayOfWeek + 6) % 7;
        return date.AddDays(-daysFromMonday);
    }
}
