using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using VelocityMCP.Data;
using VelocityMCP.Tools;

const string DefaultDbPath = "velocity.duckdb";

// ── Determine transport mode ────────────────────────────────────────
// --transport=http or --transport=stdio (default: http)
var transport = args.FirstOrDefault(a => a.StartsWith("--transport="))
    ?.Split('=', 2)[1] ?? "http";

if (transport == "stdio")
    await RunStdio(args);
else
    await RunHttp(args);

// ── Shared service registration ─────────────────────────────────────

void RegisterServices(IServiceCollection services, IConfiguration config)
{
    var dbPath = config["DuckDb:Path"] ?? DefaultDbPath;
    var connectionString = $"Data Source={dbPath}";
    var useFakeClient = config["Velocity:UseFake"] != "false";

    services.AddSingleton(sp =>
    {
        var schema = new DuckDbSchema(connectionString, sp.GetRequiredService<ILogger<DuckDbSchema>>());
        schema.EnsureCreated();
        schema.AssertNoPiiColumns();
        return schema;
    });

    services.AddSingleton(sp =>
    {
        _ = sp.GetRequiredService<DuckDbSchema>();
        return new DuckDbMirror(connectionString, sp.GetRequiredService<ILogger<DuckDbMirror>>());
    });

    if (useFakeClient)
    {
        services.AddSingleton<IVelocityClient, FakeVelocityClient>();
    }
    else
    {
        throw new PlatformNotSupportedException(
            "Real VelocityAdapter SDK requires Windows x64. Set Velocity:UseFake=true for development.");
    }

    // Fake SDK mints unique LogIds per call, so we can amass ~3k transactions
    // in one shot on cold start by calling it 30 times. Real SDK would just
    // refetch the same rows, so it stays at 1.
    var bulkBackfillCalls = useFakeClient
        ? (int.TryParse(config["Ingest:BulkBackfillCalls"], out var b) ? b : 30)
        : 1;

    services.AddHostedService(sp => new IngestWorker(
        sp.GetRequiredService<IVelocityClient>(),
        sp.GetRequiredService<DuckDbMirror>(),
        sp.GetRequiredService<ILogger<IngestWorker>>(),
        interval: TimeSpan.FromSeconds(int.TryParse(config["Ingest:IntervalSeconds"], out var i) ? i : 30),
        backfillHorizon: TimeSpan.FromDays(int.TryParse(config["Ingest:BackfillDays"], out var d) ? d : 7),
        bulkBackfillCalls: bulkBackfillCalls
    ));
}

IMcpServerBuilder AddMcpTools(IMcpServerBuilder mcpBuilder)
{
    return mcpBuilder
        .WithTools<CountEventsTool>()
        .WithTools<ServerInfoTool>()
        .WithTools<ListEventTypesTool>()
        .WithTools<ListDispositionsTool>()
        .WithTools<LookupAlarmCategoriesTool>()
        .WithTools<FindDoorsTool>()
        .WithTools<ListDoorsTool>()
        .WithTools<FindReadersTool>()
        .WithTools<FindPeopleTool>()
        .WithTools<AggregateEventsTool>()
        .WithTools<SampleEventsTool>()
        .WithTools<TimeseriesEventsTool>()
        .WithTools<GetEventTool>()
        .WithTools<GetAlarmTool>()
        .WithTools<CountAlarmsTool>()
        .WithTools<AggregateAlarmsTool>()
        .WithTools<SampleAlarmsTool>()
        .WithTools<AlarmResponseMetricsTool>()
        .WithTools<TimeseriesAlarmsTool>()
        .WithTools<PersonDossierTool>()
        .WithTools<DoorDossierTool>()
        .WithTools<GetSurroundingEventsTool>()
        .WithTools<DailySecurityBriefingTool>()
        .WithTools<GetDailyAttendanceTool>()
        .WithTools<FindForcedThroughAttemptsTool>()
        .WithTools<CheckAuthorizationTool>()
        .WithTools<InactiveEntitiesTool>();
}

// ── HTTP/SSE mode ───────────────────────────────────────────────────

async Task RunHttp(string[] args)
{
    var builder = WebApplication.CreateBuilder(args);

    var port = builder.Configuration["Mcp:Port"] ?? "3001";

    RegisterServices(builder.Services, builder.Configuration);

    AddMcpTools(
        builder.Services.AddMcpServer(options =>
        {
            options.ServerInfo = new() { Name = "VelocityMCP", Version = "0.1.0-dev" };
        })
        .WithHttpTransport(options => options.Stateless = true)
    );

    var app = builder.Build();

    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    logger.LogInformation("VelocityMCP starting (HTTP/SSE on port {Port}) — transport: http", port);

    app.MapMcp();

    await app.RunAsync($"http://0.0.0.0:{port}");
}

// ── Stdio mode ──────────────────────────────────────────────────────

async Task RunStdio(string[] args)
{
    var builder = Host.CreateApplicationBuilder(args);

    // Stdio transport uses stdout for JSON-RPC — logs must go to stderr
    builder.Logging.AddConsole(options => options.LogToStandardErrorThreshold = LogLevel.Trace);

    RegisterServices(builder.Services, builder.Configuration);

    AddMcpTools(
        builder.Services.AddMcpServer(options =>
        {
            options.ServerInfo = new() { Name = "VelocityMCP", Version = "0.1.0-dev" };
        })
        .WithStdioServerTransport()
    );

    var host = builder.Build();

    var logger = host.Services.GetRequiredService<ILogger<Program>>();
    logger.LogInformation("VelocityMCP starting — transport: stdio");

    await host.RunAsync();
}
