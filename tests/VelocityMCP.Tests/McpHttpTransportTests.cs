using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using VelocityMCP.Data;
using VelocityMCP.Tools;

namespace VelocityMCP.Tests;

/// <summary>
/// End-to-end smoke test of the MCP HTTP transport. Stands up a minimal
/// in-process WebApplication mirroring Program.cs's MCP setup, binds to
/// a random localhost port, and exercises the wire path:
///
///   1. POST / with JSON-RPC `initialize` — proves the streamable-HTTP
///      transport is reachable, accepts the handshake, and returns a valid
///      response with our server name.
///   2. POST / with `tools/list` — proves every registered tool is exposed
///      via the protocol, not just held in memory by the DI container.
///
/// This is the only place in the suite that exercises the actual HTTP wire
/// path — the rest of the tests call tool methods directly. If the MCP
/// package, its routing, or the JSON-RPC framing breaks, this catches it.
/// Two tests is enough; deeper protocol behavior is the MCP package's job.
/// </summary>
public class McpHttpTransportTests : IAsyncLifetime
{
    private WebApplication? _app;
    private HttpClient? _client;
    private string _dbPath = "";

    public async Task InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"velocity_test_{Guid.NewGuid():N}.duckdb");
        var connString = $"Data Source={_dbPath}";
        new DuckDbSchema(connString, NullLogger<DuckDbSchema>.Instance).EnsureCreated();

        // Mirror Program.cs's MCP host setup but with a minimal service surface:
        // - DuckDbMirror as a singleton (tools that take it as a parameter resolve via DI)
        // - No IngestWorker, no real Velocity SDK, no policy refresh — we're testing
        //   the wire transport, not the data layer.
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();  // keep test output quiet
        builder.WebHost.UseUrls("http://127.0.0.1:0");  // OS-assigned port

        builder.Services.AddSingleton(_ =>
            new DuckDbMirror(connString, NullLogger<DuckDbMirror>.Instance));

        // Register the same tools Program.cs registers. We use a small subset
        // here just to verify the transport works — a full registration list
        // is what ToolRegistrationTests already locks down.
        builder.Services.AddMcpServer(opts =>
        {
            opts.ServerInfo = new() { Name = "VelocityMCP-Test", Version = "0.0.0-test" };
        })
        .WithHttpTransport(opts => opts.Stateless = true)
        .WithTools<ServerInfoTool>()
        .WithTools<ListEventTypesTool>()
        .WithTools<ListDispositionsTool>();

        _app = builder.Build();
        _app.MapMcp();

        await _app.StartAsync();

        var address = _app.Urls.First();
        _client = new HttpClient { BaseAddress = new Uri(address) };
        _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    }

    [Fact]
    public async Task Initialize_handshake_returns_server_info()
    {
        var request = """
            {
              "jsonrpc": "2.0",
              "id": 1,
              "method": "initialize",
              "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "transport-test", "version": "0.1" }
              }
            }
            """;

        var response = await _client!.PostAsync("/", new StringContent(request, Encoding.UTF8, "application/json"));
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync();
        // Response is SSE-framed: `event: message\ndata: {json}\n\n`. Extract the data line.
        var json = ExtractJsonFromSse(body);
        using var doc = JsonDocument.Parse(json);

        var result = doc.RootElement.GetProperty("result");
        Assert.Equal("VelocityMCP-Test", result.GetProperty("serverInfo").GetProperty("name").GetString());
        Assert.True(result.TryGetProperty("capabilities", out var caps));
        Assert.True(caps.TryGetProperty("tools", out _),
            "MCP server must advertise tools capability after initialize");
    }

    [Fact]
    public async Task Tools_list_returns_every_registered_tool()
    {
        var request = """
            {
              "jsonrpc": "2.0",
              "id": 2,
              "method": "tools/list",
              "params": {}
            }
            """;

        var response = await _client!.PostAsync("/", new StringContent(request, Encoding.UTF8, "application/json"));
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync();
        var json = ExtractJsonFromSse(body);
        using var doc = JsonDocument.Parse(json);

        var tools = doc.RootElement.GetProperty("result").GetProperty("tools").EnumerateArray().ToList();

        // We registered 3 tools above (ServerInfo, ListEventTypes, ListDispositions).
        // The exact count must match — proves the MCP framework is exposing every
        // registered tool, not silently swallowing any.
        Assert.Equal(3, tools.Count);

        var names = tools.Select(t => t.GetProperty("name").GetString()).ToHashSet();
        Assert.Contains("server_info", names);
        Assert.Contains("list_event_types", names);
        Assert.Contains("list_dispositions", names);

        // Each tool exposed must have a non-empty description and an input
        // schema — these are the strings the LLM uses to pick tools.
        foreach (var tool in tools)
        {
            var name = tool.GetProperty("name").GetString();
            Assert.False(string.IsNullOrWhiteSpace(tool.GetProperty("description").GetString()),
                $"tool '{name}' has no description");
            Assert.True(tool.TryGetProperty("inputSchema", out _),
                $"tool '{name}' has no input schema");
        }
    }

    /// <summary>
    /// MCP responses come back as SSE frames: `event: message\ndata: {json}\n\n`.
    /// Strip the framing and return the JSON payload.
    /// </summary>
    private static string ExtractJsonFromSse(string sse)
    {
        var dataLine = sse.Split('\n').FirstOrDefault(l => l.StartsWith("data: "));
        if (dataLine is null)
            throw new InvalidOperationException($"No data line in SSE response. Body: {sse}");
        return dataLine.Substring("data: ".Length).TrimEnd('\r');
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_app != null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_dbPath + ".wal"); } catch { /* ignore */ }
    }
}
