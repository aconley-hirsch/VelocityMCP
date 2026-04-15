using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Pure-logic tests for TimeWindow.Resolve. No fixture, no DuckDB —
/// these exercise the date math directly. Each test pins behavior the
/// model relies on when it sees relative phrases like "last week" or
/// "this month" in user questions.
/// </summary>
public class TimeWindowTests
{
    private static readonly TimeSpan DefaultWindow = TimeSpan.FromHours(24);

    [Fact]
    public void NoArgs_FallsBackToDefaultWindow()
    {
        var (since, until) = TimeWindow.Resolve(null, null, null, DefaultWindow);
        var span = until - since;
        Assert.InRange(span.TotalHours, 23.99, 24.01);
        Assert.True(until <= DateTime.UtcNow.AddSeconds(1));
    }

    [Fact]
    public void ExplicitSinceUntil_OverridesRelativeWindow()
    {
        // When both explicit + relative are passed, explicit wins.
        var explicitSince = "2026-04-01T00:00:00Z";
        var explicitUntil = "2026-04-08T00:00:00Z";
        var (since, until) = TimeWindow.Resolve("last_30d", explicitSince, explicitUntil, DefaultWindow);

        Assert.Equal(new DateTime(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc), since);
        Assert.Equal(new DateTime(2026, 4, 8, 0, 0, 0, DateTimeKind.Utc), until);
    }

    [Fact]
    public void Today_StartsAtLocalMidnight()
    {
        var (since, until) = TimeWindow.Resolve("today", null, null, DefaultWindow);

        // since must equal today 00:00 local, expressed as UTC
        var expectedSince = DateTime.Today.ToUniversalTime();
        Assert.Equal(expectedSince, since);
        Assert.True(until <= DateTime.UtcNow.AddSeconds(1));
        Assert.True(until - since <= TimeSpan.FromHours(25)); // some TZ headroom
    }

    [Fact]
    public void Yesterday_SpansFullPriorDay()
    {
        var (since, until) = TimeWindow.Resolve("yesterday", null, null, DefaultWindow);

        // yesterday is exactly [prevDay 00:00 local, today 00:00 local) → 24 hours
        Assert.Equal(24.0, (until - since).TotalHours, precision: 1);
        Assert.Equal(DateTime.Today.ToUniversalTime(), until);
    }

    [Fact]
    public void Last24h_ReturnsRolling24HourWindow()
    {
        var (since, until) = TimeWindow.Resolve("last_24h", null, null, DefaultWindow);
        var span = until - since;
        Assert.InRange(span.TotalHours, 23.99, 24.01);
    }

    [Theory]
    [InlineData("last_24h")]
    [InlineData("last_1d")]
    [InlineData("last_day")]
    public void Last24h_HasMultipleAliases(string alias)
    {
        var (since, until) = TimeWindow.Resolve(alias, null, null, DefaultWindow);
        Assert.InRange((until - since).TotalHours, 23.99, 24.01);
    }

    [Theory]
    [InlineData("last_7d")]
    [InlineData("last_week")]
    [InlineData("past_week")]
    public void Last7d_AndAliases_ReturnSevenDayWindow(string alias)
    {
        var (since, until) = TimeWindow.Resolve(alias, null, null, DefaultWindow);
        var span = until - since;
        Assert.InRange(span.TotalDays, 6.99, 7.01);
    }

    [Theory]
    [InlineData("last_30d")]
    [InlineData("last_month")]
    [InlineData("past_month")]
    public void Last30d_AndAliases_ReturnThirtyDayWindow(string alias)
    {
        var (since, until) = TimeWindow.Resolve(alias, null, null, DefaultWindow);
        var span = until - since;
        Assert.InRange(span.TotalDays, 29.99, 30.01);
    }

    [Fact]
    public void ThisWeek_StartsOnMonday()
    {
        var (since, _) = TimeWindow.Resolve("this_week", null, null, DefaultWindow);

        // "since" should be midnight on Monday of the current local week.
        // Convert back to local to inspect the day of week — UTC may have
        // already crossed midnight.
        var localSince = since.ToLocalTime();
        Assert.Equal(DayOfWeek.Monday, localSince.DayOfWeek);
        Assert.Equal(0, localSince.Hour);
        Assert.Equal(0, localSince.Minute);
    }

    [Fact]
    public void ThisMonth_StartsOnFirstDayOfMonth()
    {
        var (since, _) = TimeWindow.Resolve("this_month", null, null, DefaultWindow);
        var localSince = since.ToLocalTime();
        Assert.Equal(1, localSince.Day);
        Assert.Equal(0, localSince.Hour);
        Assert.Equal(0, localSince.Minute);
    }

    [Fact]
    public void UnknownRelativeWindow_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            TimeWindow.Resolve("not_a_real_window", null, null, DefaultWindow));
        Assert.Contains("Unknown relative_window", ex.Message);
    }

    [Fact]
    public void RelativeWindow_IsCaseInsensitive()
    {
        var (since1, until1) = TimeWindow.Resolve("LAST_7D", null, null, DefaultWindow);
        var (since2, until2) = TimeWindow.Resolve("last_7d", null, null, DefaultWindow);

        // Both should resolve to ~same window (within the few microseconds
        // between the two calls).
        Assert.True(Math.Abs((since1 - since2).TotalSeconds) < 1);
        Assert.True(Math.Abs((until1 - until2).TotalSeconds) < 1);
    }

    [Fact]
    public void SinceOnly_FillsUntilWithNow()
    {
        var explicitSince = DateTime.UtcNow.AddDays(-3).ToString("o");
        var (since, until) = TimeWindow.Resolve(null, explicitSince, null, DefaultWindow);

        Assert.True(until <= DateTime.UtcNow.AddSeconds(1));
        Assert.True(until > DateTime.UtcNow.AddSeconds(-5));
    }

    [Fact]
    public void UntilOnly_FillsSinceFromDefaultWindow()
    {
        var explicitUntil = DateTime.UtcNow.ToString("o");
        var (since, until) = TimeWindow.Resolve(null, null, explicitUntil, TimeSpan.FromDays(2));

        Assert.InRange((until - since).TotalDays, 1.99, 2.01);
    }
}
