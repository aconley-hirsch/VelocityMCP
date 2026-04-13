using System.Text;
using System.Text.Json;

namespace VelocityMCP.Tools;

/// <summary>
/// Shared JSON serialization + soft byte-cap enforcement for tool responses.
///
/// Two design goals:
///   1. One <see cref="JsonSerializerOptions"/> instance for the whole process —
///      avoids per-call allocations that show up in every tool today.
///   2. Tools returning variable-size collections (events, alarms, groups, points)
///      can call <see cref="SerializeWithCap"/> to guarantee the response stays
///      under <see cref="DefaultMaxBytes"/>. If the full payload exceeds the cap,
///      the helper binary-searches for the largest item count whose serialized
///      output fits, and returns that.
///
/// Tools are responsible for setting <c>truncated_due_to_size</c> flags inside
/// their payload by checking <c>n &lt; fullItemCount</c> in the closure.
/// </summary>
public static class ResponseShaper
{
    /// <summary>8 KB cap matches the planned MCP token budget per response.</summary>
    public const int DefaultMaxBytes = 8 * 1024;

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    /// <summary>
    /// Serialize a fixed-size payload using the shared options. No size check —
    /// use this for tools whose output is bounded by construction (counts,
    /// catalogs, single-row lookups).
    /// </summary>
    public static string Serialize(object payload) =>
        JsonSerializer.Serialize(payload, JsonOptions);

    /// <summary>
    /// Serialize a payload with soft byte-cap enforcement. <paramref name="buildPayload"/>
    /// is a closure that takes a max item count and returns the payload object;
    /// the helper first tries with <paramref name="fullItemCount"/>, and if the
    /// serialized result exceeds <paramref name="maxBytes"/>, binary-searches for
    /// the largest item count that fits.
    ///
    /// The closure should set any <c>truncated_due_to_size</c> flag itself by
    /// comparing the parameter <c>n</c> against the original <paramref name="fullItemCount"/>.
    /// </summary>
    public static string SerializeWithCap<T>(
        Func<int, T> buildPayload,
        int fullItemCount,
        int maxBytes = DefaultMaxBytes)
    {
        // Fast path: try the full payload first
        var fullJson = JsonSerializer.Serialize(buildPayload(fullItemCount), JsonOptions);
        if (Encoding.UTF8.GetByteCount(fullJson) <= maxBytes)
            return fullJson;

        // Slow path: binary search [0, fullItemCount-1] for the largest fitting count
        int lo = 0;
        int hi = fullItemCount - 1;
        string bestJson = JsonSerializer.Serialize(buildPayload(0), JsonOptions);

        while (lo <= hi)
        {
            int mid = (lo + hi) / 2;
            var trial = JsonSerializer.Serialize(buildPayload(mid), JsonOptions);
            if (Encoding.UTF8.GetByteCount(trial) <= maxBytes)
            {
                bestJson = trial;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return bestJson;
    }
}
