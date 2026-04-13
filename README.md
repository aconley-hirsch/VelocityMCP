# VelocityMCP

A Model Context Protocol (MCP) server that turns a [Hirsch Velocity](https://www.hirschsecure.com/us/en-us/products/access-control-systems/security-management-system) physical-access-control system into a natural-language query surface. It reads transactions, alarms, doors, people, and clearances from the Velocity SDK, mirrors them into a local DuckDB analytical database, and exposes 27 purpose-built tools over HTTP so any MCP-capable client — Claude, LM Studio, Cursor, or a custom agent — can answer questions like *"how many denied reads at the server room this week?"* or *"who is currently authorized to enter the executive suite?"* in one round trip.

---

## What you can ask it

VelocityMCP is designed so a language model can answer real security operations questions without chaining five raw SQL-ish tool calls. A few examples the current tool surface handles directly:

**Operations / daily briefings**
- *"Give me yesterday's morning briefing — totals, deltas vs the day before, and any open alarms."*
- *"Show me every door in the system and flag any that haven't been used in the last 24 hours."*
- *"Which doors had the most traffic today?"*

**Compliance / audit**
- *"Is Jane Smith authorized to enter the server room?"*
- *"Which doors can Bob Johnson access right now?"*
- *"Who is allowed into the executive suite?"*
- *"Which employees haven't badged in for 30 days?"*

**Investigation**
- *"What else was happening in the building when the loading dock alarm went off at 2am?"*
- *"Were there any denied-then-granted sequences at the same reader in the last week?"*
- *"What time did Alice Williams arrive and leave yesterday?"*
- *"Pull a 30-day activity report for person 1003 — doors, hourly pattern, denials, alarms."*

**Ad-hoc analytics**
- *"Alarms per hour across the last 24 hours — just the priority-1 ones."*
- *"Average time to acknowledge alarms this week, broken down by operator."*
- *"How many distinct people badged through the parking garage today?"*

The server is built so you can issue these in plain English and the model picks the right tool (or tools) without the user ever seeing the schema.

---

## Why it exists

Velocity has a rich SDK but no natural-language interface. Operators answer ad-hoc questions either by hand-writing SQL against the Velocity SQL Server, clicking through the Velocity UI one screen at a time, or exporting to Excel and pivoting. A compliance question like *"which employees badged in after 10pm last week?"* can take 20 minutes even for someone who knows the system.

VelocityMCP closes that gap. The LLM sees a curated, read-only set of high-level tools (not raw SQL) and produces answers in seconds. Security auditors, compliance officers, facility managers, and incident investigators all get the same conversational interface on top of the same data.

---

## Architecture

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   MCP client     │      │   VelocityMCP    │      │  Velocity SDK    │
│  (LM Studio,     │◄────►│    (this repo)   │◄────►│  (VelocityAdapter│
│   Claude, etc.)  │ HTTP │                  │      │      .dll)       │
└──────────────────┘      └──────────────────┘      └──────────────────┘
                                   │
                                   ▼
                          ┌──────────────────┐
                          │   DuckDB mirror  │
                          │ (local .duckdb)  │
                          └──────────────────┘
```

**Four layers:**

1. **`IVelocityClient`** — abstraction over the Velocity SDK. Two implementations:
   - **`FakeVelocityClient`** — deterministic synthetic data generator for Mac/Linux development, testing, and demos without a Windows host.
   - **`RealVelocityClient`** — wraps `VelocityAdapter.dll` (Windows x64 only, see [Connecting to real Velocity](#connecting-to-real-velocity)).
2. **`IngestWorker`** — background hosted service that polls the client on a configurable interval (default 30s), performs a bounded backfill on cold start (default 7 days), and refreshes dimensions (doors, readers, people, clearances) on the same cadence.
3. **`DuckDbMirror`** — the analytical layer. All MCP tool queries hit DuckDB directly, never the Velocity SDK, so response latency is sub-millisecond and the real Velocity database is never touched at query time.
4. **MCP tools** — 27 typed tools grouped into catalog / query / investigation / authorization concerns. Each tool has a carefully written description the LLM reads to decide when to call it, plus typed parameters that constrain inputs.

The DuckDB file (`velocity.duckdb` by default) holds `fact_transactions`, `fact_alarms`, several `dim_*` tables for doors/readers/people/clearances, and a `meta_ingest_cursors` table that tracks how far forward the ingest has walked.

---

## Privacy and PII defense

VelocityMCP takes a four-layer approach to keeping PINs, card codes, and credential secrets out of the mirror:

1. **Allow-listed projections.** Every DTO returned from `IVelocityClient` (`TransactionRecord`, `AlarmRecord`, `PersonRecord`, etc.) is a hand-curated subset of the SDK's fields. `PIN`, `CODE`, `CardData`, `MATCH`, and `StampNo` are never read into these records.
2. **Schema never stores them.** `fact_transactions` and `fact_alarms` have no columns named `pin`, `code`, or any other prohibited field — the DuckDB schema literally cannot hold them.
3. **Ingest SQL omits them.** The `INSERT INTO fact_*` statements in `DuckDbMirror.IngestTransactions` / `IngestAlarms` don't reference any PII-bearing columns.
4. **Startup assertion.** On every server start, `DuckDbSchema.AssertNoPiiColumns()` scans `information_schema.columns` for the fact tables and throws a fatal `InvalidOperationException` if any prohibited column name is found. The server refuses to start against a corrupted schema.

The layers are independent — any single layer failing still keeps PII out of the mirror and the query surface. Schedule-enforcement data (time zones, function types) is also not mirrored in v1, so the tool surface reports who *holds* a clearance but defers time-of-day enforcement to the caller's reasoning.

---

## Tool catalog

Twenty-seven tools grouped by concern. Each tool description in the code itself is written to help the LLM pick the right one for a given question — the summaries below are short.

### Catalog and lookup

| Tool | Purpose |
|---|---|
| `server_info` | Returns build version, transport, database path, and ingest cadence. |
| `list_event_types` | Full catalog of Velocity event codes with category and description. |
| `list_dispositions` | Maps disposition codes (1 = Granted, 2 = Denied – Invalid Credential, etc.) to names. |
| `lookup_alarm_categories` | Returns the alarm category catalog (Duress, Tamper, Access, Security, etc.). |
| `find_doors` | Fuzzy search doors by partial name. Returns door_id + reader list. |
| `list_doors` | Full door catalog with activity-derived status (active / quiet / stale / never_seen), last_seen_at, events_in_window, open_alarms. |
| `find_readers` | Fuzzy search readers by partial name. |
| `find_people` | Fuzzy search people by first/last/full name. Returns person_id. |

### Event queries (fact_transactions)

| Tool | Purpose |
|---|---|
| `count_events` | Single scalar count with standard filters (event_code, disposition, person_id, door_id, time window). |
| `aggregate_events` | Top-N breakdown by dimension (person, door, reader, hour, day, event type, disposition). |
| `sample_events` | Return a bounded, ordered slice of matching transaction rows. |
| `timeseries_events` | Zero-filled count buckets by hour / day / week / month. |
| `get_event` | Full row detail for a single `log_id`. |

### Alarm queries (fact_alarms)

| Tool | Purpose |
|---|---|
| `count_alarms` | Scalar count with alarm-side filters (event_id, priority, status, person_id, workstation). |
| `aggregate_alarms` | Top-N breakdown by alarm dimension. |
| `sample_alarms` | Bounded, ordered slice of alarm rows. |
| `timeseries_alarms` | Zero-filled alarm counts bucketed by hour / day / week / month. |
| `get_alarm` | Full row detail for a single `alarm_id`. |
| `alarm_response_metrics` | Acknowledge/clear lifecycle metrics (avg_ack_minutes, p90_ack_minutes, avg_clear_minutes, still_open) grouped by operator, priority, event, day, or hour. |

### Reports and dossiers

| Tool | Purpose |
|---|---|
| `person_dossier` | One-call report for a single person: summary, top doors, 24-hour pattern, recent denials, recent alarms. Replaces a 5-call investigation workflow. |
| `door_dossier` | Location-dimension counterpart to person_dossier: per-door summary, hourly traffic, top users, recent denials, recent alarms. |
| `daily_security_briefing` | Morning briefing for a given calendar day — headline metrics, day-over-day deltas, forced-open samples, notable deniers, busiest doors, open alarms. |

### Investigation and specialty

| Tool | Purpose |
|---|---|
| `get_surrounding_events` | Every transaction and alarm within ±window_minutes of a given timestamp — the honest alternative to a temporal-join builder the LLM could misuse. |
| `get_daily_attendance` | First/last granted badge per person per day, with duration. |
| `find_forced_through_attempts` | Denied-then-granted sequences at the same reader within N seconds (credential-share / force-through signal). NOTE: this is not true tailgating detection — tailgating leaves no badge record for the second person. |
| `inactive_entities` | Set-difference query: which doors, readers, or people have zero activity in a window? Useful for compliance (*"who hasn't badged in 30 days?"*) and device health (*"which doors might be offline?"*). |

### Authorization (policy)

| Tool | Purpose |
|---|---|
| `check_authorization` | Policy-layer query, not historical. Auto-detects three modes from which parameters are set: `person_id + door_id` → point query (*"is this person currently authorized here?"*), `person_id` alone → all doors a person can access, `door_id` alone → all people authorized for a door. Filters to currently-active assignments (`expires_at IS NULL OR expires_at > now()`). |

---

## Requirements

### Development (Mac, Linux, or Windows)

- **.NET 8 SDK** (`dotnet --version` ≥ 8.0.100)
- Any MCP client to exercise the tools — LM Studio, Claude Desktop, Cursor, or the MCP reference client

No Velocity SDK required for development. The default config uses `FakeVelocityClient`, which produces deterministic synthetic data so the full tool surface is exercisable offline.

### Production (Windows)

- **Windows x64** — VelocityAdapter.dll is managed .NET Framework but x64-only
- **.NET 8 runtime** (bundled into the self-contained single-file publish)
- **Velocity SQL Server** reachable from the host
- **VelocityAdapter.dll** and its ~76 supporting DLLs deployable next to the exe (the supporting DLLs include Microsoft.Data.SqlClient, Azure.Identity, Newtonsoft.Json, NLog, and the Hirsch license manager)

---

## Quick start

### Mac / Linux development

```bash
git clone git@github.com:aconley-hirsch/VelocityMCP.git
cd VelocityMCP

# Restore and build
dotnet restore
dotnet build

# Run the full test suite (40 tests, all against the fake client)
dotnet test

# Start the server on http://0.0.0.0:3001
dotnet run --project src/VelocityMCP/VelocityMCP.csproj
```

On first run against an empty DuckDB file, the ingest worker performs a bulk cold-start backfill (~2,800 transactions + ~300 alarms spread over 7 days from the fake client) before the periodic ingest loop takes over. The MCP endpoint is `http://localhost:3001/mcp`.

### Windows production (requires the real Velocity SDK)

> `RealVelocityClient` is planned but not yet in `main`. See [Connecting to real Velocity](#connecting-to-real-velocity) for the full plan and current status.

Once the real client lands:

```powershell
# Clone
git clone git@github.com:aconley-hirsch/VelocityMCP.git
cd VelocityMCP

# Publish a self-contained single-file exe targeting Windows x64
dotnet publish src/VelocityMCP/VelocityMCP.csproj `
  -c Release `
  -f net8.0-windows `
  -r win-x64 `
  --self-contained true `
  -p:PublishSingleFile=true `
  -p:IncludeNativeLibrariesForSelfExtract=true `
  -o .\publish

# Copy the Velocity SDK alongside the exe (single-file publish cannot embed
# unmanaged assemblies loaded via reflection)
xcopy /Y /E C:\path\to\SDK\VelocityAdapter\*.dll .\publish\

# Provide config and run
cd .\publish
.\VelocityMCP.exe
```

`VelocityMCP.exe` is ~70 MB before the SDK copy, ~150 MB after.

---

## Configuration

Configuration is read from `appsettings.json`, `appsettings.{Environment}.json`, and environment variables (standard ASP.NET Core configuration). The most relevant keys:

| Key | Default | Description |
|---|---|---|
| `Mcp:Port` | `3001` | HTTP port the MCP endpoint binds to. |
| `DuckDb:Path` | `velocity.duckdb` | Path to the DuckDB file. Relative paths are resolved against the exe's working directory. |
| `Velocity:UseFake` | `true` | Set to `false` to switch from `FakeVelocityClient` to `RealVelocityClient`. Currently throws `PlatformNotSupportedException` until the real client lands. |
| `Velocity:SqlServer` | *(unset)* | SQL Server hostname for the Velocity database. When unset, the real client falls back to Velocity's Windows registry entries. |
| `Velocity:Database` | *(unset)* | Database name. Same registry-fallback behavior. |
| `Ingest:IntervalSeconds` | `30` | Periodic ingest interval. |
| `Ingest:BackfillDays` | `7` | Cold-start backfill horizon. |
| `Ingest:BulkBackfillCalls` | `30` (fake) / `1` (real) | Number of SDK calls on cold start. The fake client mints unique IDs per call, so 30 calls amass ~3k transactions. Real SDK only needs 1 call. |

Example `appsettings.Production.json` for a Windows deploy:

```json
{
  "Mcp": {
    "Port": "3001"
  },
  "DuckDb": {
    "Path": "C:\\ProgramData\\VelocityMCP\\velocity.duckdb"
  },
  "Velocity": {
    "UseFake": "false",
    "SqlServer": "VELOCITYHOST\\SQLEXPRESS",
    "Database": "Velocity"
  },
  "Ingest": {
    "IntervalSeconds": "30",
    "BackfillDays": "7"
  }
}
```

### Transport modes

By default the server runs HTTP/SSE (`http://0.0.0.0:3001/mcp`). For MCP clients that need stdio transport (Claude Desktop, some local agents), run with `--transport=stdio`:

```bash
dotnet run --project src/VelocityMCP/VelocityMCP.csproj -- --transport=stdio
```

In stdio mode, logs are redirected to stderr so stdout stays clean for JSON-RPC.

---

## Connecting to real Velocity

`FakeVelocityClient` ships in the repo and powers all development and testing. `RealVelocityClient`, which wraps Hirsch's `VelocityAdapter.dll`, is planned but not yet committed to `main`. The full implementation plan — including the field-by-field SDK DTO mapping, the Person → Credential → IAccessFunction → DoorGroup projection strategy for clearances, and the `parm1`/`parm2` PII hole mitigation — is tracked separately and executed on a Windows build host where the SDK is available.

Key points of the plan:

- **Dual targeting.** `VelocityMCP.Data` and `VelocityMCP` both get `<TargetFrameworks>net8.0;net8.0-windows</TargetFrameworks>`. Mac builds see only `net8.0` (the fake client path). Windows builds add `net8.0-windows`, which references `VelocityAdapter.dll` via a conditional `<Reference>` with `Private=false`.
- **`RealVelocityClient.cs` is wrapped in `#if WINDOWS`.** Mac development stays unaffected; `dotnet test` continues to pass against the fake client.
- **Clearance projection.** The Velocity SDK has no direct "clearance" concept — the chain is `Person → Credential → IAccessFunction → DoorGroup`. `RealVelocityClient` walks that chain, strips the PII fields on each credential (`PIN`, `MATCH`, `CardData`, `StampNo`), and projects into the `ClearanceRecord` / `ReaderClearanceRecord` / `PersonClearanceRecord` shapes the mirror already expects.
- **Single-file Windows publish.** `dotnet publish -c Release -f net8.0-windows -r win-x64 --self-contained -p:PublishSingleFile=true` produces a deployable `VelocityMCP.exe`. `VelocityAdapter.dll` + its supporting DLLs are deployed alongside the exe.

Until the real client lands, setting `Velocity:UseFake=false` fails fast at startup with a clear `PlatformNotSupportedException`.

---

## Tests

```bash
dotnet test
```

Forty integration tests covering the full tool surface plus supporting infrastructure (schema assertions, cursor round-trips, response shaper cap enforcement, PII defense). All tests use `FakeVelocityClient` with deterministic RNG seeds, so they run identically on Mac, Linux, and Windows.

If the local .NET 8 runtime is missing but a newer runtime is available:

```bash
dotnet test /p:RollForward=LatestMajor
```

---

## Repository layout

```
src/
├── VelocityMCP/                       # ASP.NET Core host + MCP transport wiring
│   └── Program.cs                     # Service registration, tool registration, HTTP/stdio mode
├── VelocityMCP.Data/                  # Velocity client abstraction + DuckDB mirror
│   ├── IVelocityClient.cs             # Interface implemented by fake and real clients
│   ├── FakeVelocityClient.cs          # Deterministic synthetic data for dev/test
│   ├── DuckDbSchema.cs                # CREATE TABLE + PII assertion
│   ├── DuckDbMirror.cs                # Ingest + all query methods
│   ├── IngestWorker.cs                # Background polling + cold-start backfill
│   └── Models/                        # Allow-listed DTOs (no PII fields)
└── VelocityMCP.Tools/                 # Twenty-seven MCP tool classes
    ├── ResponseShaper.cs              # 8 KB soft cap with binary-search trimming
    ├── CountEventsTool.cs             # ... one file per tool (mostly)
    ├── AlarmQueryTools.cs             # (legacy: 3 tools in one file, to be split)
    └── ...

tests/
└── VelocityMCP.Tests/                 # xUnit integration tests against FakeVelocityClient
```

---

## Design notes

### The 8 KB response cap

Every tool that returns a variable-size collection uses `ResponseShaper.SerializeWithCap`, which serializes the full payload first and — if it exceeds 8 KB — binary-searches for the largest item count whose serialized output fits. The `truncated_due_to_size` flag is set in the response so the caller knows the list was trimmed. The cap prevents the MCP client from ever receiving a response that would blow past a typical context window's budget for a single tool call.

### The `door_id` parameter pattern

Early tool drafts forced the LLM to call `find_doors`, extract a reader array, and plumb it as `reader_names` on every subsequent call. This produced an anti-pattern where the LLM had to understand the door→reader relationship. All transaction query tools now accept `door_id` directly; `DuckDbMirror` resolves it to reader names internally via `dim_readers` and, for grouped outputs, collapses multi-reader doors to a single logical door row. The LLM never touches the reader plumbing.

### Report-shaped tools (dossiers)

`person_dossier`, `door_dossier`, and `daily_security_briefing` each compose 5–8 internal queries into one response envelope. Without them, a compliance investigation is a 5-call chain the LLM has to sequence and synthesize. With them, it's a single call the model can reason about directly. These are the highest-leverage tools in the catalog.

### Derived vs. live device state

Tools like `list_doors` and `inactive_entities` report *historical activity* — "when was this door last seen in the event log." They do **not** report real-time Velocity device state (online/offline, locked/unlocked, door position sensor). Those live fields require a live SDK call, which is planned as a future extension but not in the current surface. Tool descriptions are explicit about this distinction so the LLM doesn't accidentally claim live state when it only has historical data.

---

## Status

All 27 tools from the Phase A–D build queue are shipped and tested. Remaining work:

- **`RealVelocityClient`** — Windows-side SDK wrapper (plan complete, implementation pending on a Windows build host)
- **File split refactor** — `DuckDbMirror.cs` has grown past 1,500 lines and is a candidate for partial-class splitting
- **Live SDK hooks** — optional real-time device state queries for `list_doors` and `check_authorization`, behind an opt-in flag

---

## Security

This repository contains no secrets, no PII, and no live Velocity data. `FakeVelocityClient` generates synthetic records with clearly fake names (Jane Smith, Bob Johnson, etc.). Production deployments must store their `appsettings.Production.json` SQL credentials out-of-band — the committed example files contain placeholder hostnames only.
