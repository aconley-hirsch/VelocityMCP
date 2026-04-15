using System.Reflection;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// Reflection-based guard against the "I added a tool class but forgot to
/// register it in Program.cs" bug. We hit this exact regression earlier in
/// the project and only caught it because a manual smoke test happened to
/// fail. This test catches it permanently — a new [McpServerToolType] that
/// isn't wired into AddMcpTools fails the build.
/// </summary>
public class ToolRegistrationTests
{
    [Fact]
    public void Every_tool_class_in_assembly_is_registered_in_Program_cs()
    {
        // 1. Find every [McpServerToolType] class shipped in the Tools assembly.
        var toolAssembly = typeof(ServerInfoTool).Assembly;
        var declaredToolTypes = toolAssembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() != null)
            .Select(t => t.Name)
            .ToHashSet();

        Assert.NotEmpty(declaredToolTypes);

        // 2. Read Program.cs from the repo source tree and pull out every
        //    .WithTools<T>() generic argument. Walks up from the test binary
        //    location until it finds the src/ folder — works regardless of
        //    where the build output lives.
        var programCsPath = LocateProgramCs();
        var programSource = File.ReadAllText(programCsPath);

        var registered = Regex.Matches(programSource, @"\.WithTools<([A-Za-z0-9_]+)>\(\)")
            .Select(m => m.Groups[1].Value)
            .ToHashSet();

        Assert.NotEmpty(registered);

        // 3. The set of declared tool classes must be a subset of registered tools.
        // Anything declared but not registered is the bug we're guarding against.
        var unregistered = declaredToolTypes.Except(registered).ToList();
        Assert.True(unregistered.Count == 0,
            $"Tool class(es) declared with [McpServerToolType] but not registered " +
            $"in Program.cs:AddMcpTools — add `.WithTools<{string.Join(">().WithTools<", unregistered)}>()`. " +
            $"Missing: {string.Join(", ", unregistered)}");

        // 4. Reverse direction: anything registered but not declared is dead code
        // (or a typo that compiles green because the type still exists).
        var orphaned = registered.Except(declaredToolTypes).ToList();
        Assert.True(orphaned.Count == 0,
            $"Type(s) registered in Program.cs but not marked [McpServerToolType] — " +
            $"likely a stale entry: {string.Join(", ", orphaned)}");
    }

    /// <summary>
    /// Walk up from the test binary location until we find a directory
    /// that contains both src/ and tests/, then return src/VelocityMCP/Program.cs.
    /// Avoids hardcoding any absolute path so the test works on CI, dev
    /// boxes, and worktrees identically.
    /// </summary>
    private static string LocateProgramCs()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            var src = Path.Combine(dir.FullName, "src");
            var tests = Path.Combine(dir.FullName, "tests");
            if (Directory.Exists(src) && Directory.Exists(tests))
            {
                var path = Path.Combine(src, "VelocityMCP", "Program.cs");
                if (File.Exists(path)) return path;
                throw new FileNotFoundException(
                    $"Found repo root at '{dir.FullName}' but Program.cs is missing at '{path}'");
            }
            dir = dir.Parent;
        }
        throw new DirectoryNotFoundException(
            $"Walked up from '{AppContext.BaseDirectory}' to filesystem root without finding " +
            "a directory containing both src/ and tests/");
    }
}
