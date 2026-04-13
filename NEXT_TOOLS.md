# VelocityMCP — Next Tools Plan

Phased build plan derived from the multi-agent counselors review (claude-opus + codex-5.3-xhigh + gemini-3-pro). Each task is one cohesive unit of work: DuckDbMirror method(s) + tool file + registration + tests where applicable.

**Legend:** ✅ done · ⏳ pending · 🔄 in progress · 🤔 needs decision

---

## Phase A — Foundation

Anti-pattern fixes the counselors all flagged. Must land before new tools so they inherit the right shape instead of perpetuating the bad ones.

### ✅ A1 — Add `door_id` filter to all transaction query tools
Every transaction query tool (`count_events`, `aggregate_events`, `sample_events`, `timeseries_events`) now accepts a `door_id` parameter that resolves to reader names internally via `dim_readers`. Eliminates the #1 anti-pattern (3/3 counselor consensus) where the LLM had to call `find_doors`, extract a reader array, and plumb it as `reader_names`. Also fixes NEXTSTEPS limitation #1 — `aggregate_events(group_by="door")` now joins through `dim_readers`/`dim_doors` so multi-reader doors collapse to one row instead of producing separate "Front Door Reader 1" / "Front Door Reader 2" buckets.

### ✅ A2 — Normalize `person_id` type across transaction and alarm tools
Unified all 7 query tools on `long? person_id`. Previously transactions used `int?` and alarms used `long?` because `uid1` is INTEGER in `fact_transactions` but DOUBLE in `fact_alarms`. The schema mismatch was leaking to LLM-facing parameters and forcing operators to remember which type went where. DuckDB widens `INTEGER = BIGINT` comparisons transparently, so one type works for both fact tables.

### ✅ A3 — Build shared 8KB response shaper helper
New `ResponseShaper` class with `Serialize(payload)` (fixed-size, uses static shared `JsonSerializerOptions`) and `SerializeWithCap<T>(buildPayload, fullItemCount, maxBytes)` (binary-searches for the largest item count whose serialized output fits the 8KB cap). Applied to all 5 variable-size tools (`sample_events`, `sample_alarms`, `aggregate_events`, `aggregate_alarms`, `timeseries_events`); each tool's response now includes a `truncated_due_to_size` flag when the shaper trimmed items. The remaining 11 fixed-size tools were also migrated to `ResponseShaper.Serialize` to eliminate the per-call `JsonSerializerOptions` allocation that was duplicated everywhere.

---

## Phase B — Highest-confidence new tools

The three tools with the strongest counselor consensus. All foundation prerequisites are now satisfied; each task below can be built independently in any order.

### ✅ B1 — `alarm_response_metrics`
Compute alarm acknowledge and clear lifecycle metrics from data already sitting in `fact_alarms` (`ak_date`, `cl_date`, `ak_operator`, `cl_operator`). Pure SQL using `DATEDIFF` + `PERCENTILE_CONT`. Parameters: time window, group_by (operator | priority | event | day | hour), standard alarm filters. Returns groups with `avg_ack_minutes`, `avg_clear_minutes`, `p90_ack_minutes`, `still_open` count, plus `total_alarms` and `total_unacked`. Unlocks compliance/SLA questions like "average time to acknowledge alarms this week", "which operator is fastest at clearing alarms", "how many alarms took more than 30 minutes to acknowledge". Both claude-opus and codex-5.3-xhigh ranked this their independent #1 — gemini missed it. No new schema, no new ingest. **Blocked by:** A3 (done).

### ✅ B2 — `person_dossier`
First true report-shaped tool — establishes the synthesis pattern that Phase C report tools will inherit. Replaces a 5-call investigation workflow with one call. Parameters: `person_id` (required, from `find_people`), time window (default 30 days). Returns a single envelope with `summary` (total_events, total_denials, total_alarms, distinct_doors, first_seen, last_seen), `top_doors` (reader breakdown), `hourly_pattern`, `recent_denials`, `recent_alarms`. All sections respect the 8KB cap from A3. Internally composes ~5 DuckDB queries. Highest leverage tool for compliance auditors and physical-security investigators. 3/3 counselor consensus. **Blocked by:** A1, A2, A3 (all done).

### ⏳ B3 — `inactive_entities`
Set-difference query — which doors, readers, or people have zero activity in a given window? Structurally impossible today without N+1 tool calls (LLM has to call `find_doors`, then `count_events` per door, then reason about which returned zero). Parameters: `entity` (door | reader | person), time window, limit. SQL: `LEFT JOIN dim_* to fact_* WHERE fact IS NULL` plus `MAX(dt_date) AS last_seen_at`. For `entity=door`, joins through `dim_readers.door_id` to collapse to logical doors. Compliance + offboarding + IT health monitoring use cases ("which employees haven't badged in 30 days?", "which doors might be offline?"). Easy build. 3/3 counselor consensus. **Blocked by:** A1 (done).

---

## Phase C — Round out the surface

Composition tools and parity fills. C4 is the synthesis capstone that depends on B1/B2/B3/C1/C2.

### ✅ C1 — `door_dossier`
Same report pattern as `person_dossier` but for the location dimension. Parameters: `door_id` (required, from `find_doors`), time window (default 7 days). Returns `summary` (total_access, total_denied, total_alarms, distinct_people, busiest_hour, quietest_hour), `hourly_traffic`, `top_users`, `recent_denials`, `recent_alarms`. Building managers think in doors not people — primary user persona for this tool. Reuses query patterns from B2. 3/3 counselor consensus. **Blocked by:** A1 (done), A3 (done), B2.

### ✅ C2 — `timeseries_alarms`
Trivial parity fill — transactions have count/aggregate/sample/timeseries but alarms only have count/aggregate/sample. Copy `TimeseriesEventsTool.cs`, adapt to `fact_alarms` schema and alarm filters (event_id, alarm_level_priority, status, person_id, workstation_name). Add corresponding `GetAlarmTimeSeries` method to `DuckDbMirror`. Easy win, eliminates an obvious asymmetry the LLM would notice. 2/3 counselor consensus (claude + codex). **Blocked by:** A2 (done), A3 (done).

### ✅ C3 — `get_surrounding_events`
Gemini's framing of correlation, chosen over claude-opus's generic `correlate_events` engine and codex's `alarm_access_correlation`. Argument: exposing a temporal join builder to an LLM is a hallucination risk; just give the LLM "what else happened around timestamp T" and let its own reasoning correlate. Parameters: `timestamp` (required, ISO 8601), `window_minutes` (default 5), `door_id` (optional). Returns events array + alarms array within ±window of timestamp, ordered by time. Easy build (two range queries). Investigation use case: "what else was happening in the building when the loading dock alarm went off at 2am?". **Blocked by:** A1 (done), A3 (done).

### ✅ C4 — `daily_security_briefing`
3/3 counselor consensus on highest demo impact. Build LAST in Phase C — composes infrastructure from B1, B2, B3, C1, C2. Parameters: `date` (default yesterday). Returns `headline` (total_access, total_denied, total_alarms, alarms_unacked, forced_opens, held_opens), `vs_prior_day` deltas, `notable_events` (forced opens, multiple denials from same person), `busiest_doors`, `open_alarms`. Hardest tool to build because of 6-8 internal queries plus strict 8KB budget across many sections. The tool that justifies the MCP architecture in a sales demo: "morning briefing" → complete situational awareness in one call. **Blocked by:** B1, B2, B3, C1, C2.

---

## Phase D — Investigate / decide

Investigative items requiring scope conversation before becoming build tasks, plus two scoped tools that gemini surfaced uniquely.

### ✅ D1 — `check_authorization`
The policy-gap fix. Hybrid mirror-first approach: policy dimensions live in DuckDB, refreshed on the same cadence as `dim_people`/`dim_doors` (slow-moving, small volume). Live-SDK fallback for freshness-sensitive "right now" queries can be added later when the real SDK arrives. Three new tables: `dim_clearances` (id, name, schedule_name informational), `dim_reader_clearances` (reader → clearance mapping, authoritative-replace on refresh), `dim_person_clearances` (person → clearance with granted_at + nullable expires_at, authoritative-replace on refresh). `IVelocityClient` extended with `GetClearancesAsync` / `GetReaderClearancesAsync` / `GetPersonClearancesAsync`. `FakeVelocityClient` seeds 5 clearances ("All Hours", "Business Hours", "Executive Suite", "Server Room", "Parking Only"), each person gets "Business Hours" + one specialty + optional "All Hours" super-user. `IngestWorker.RefreshDimensions` pulls + upserts the 3 new tables. Tool auto-detects 3 modes from which params are set: `person_id + door_id` → point query (authorized yes/no + via_clearances[]); `person_id` alone → all doors the person can access (collapsed through `dim_readers`); `door_id` alone → all people authorized for that door. All modes filter to currently-active assignments (`expires_at IS NULL OR expires_at > now()`). Schedule enforcement deferred — tool surfaces `schedule_name` so caller can reason about time-of-day if needed.

### ✅ D2 — `get_daily_attendance`
Gemini-only suggestion. First/last badge per person per day for "what time did the cleaning crew arrive and leave?" or "who was in the building after 8pm?". Currently impossible: `aggregate_events` only counts, `sample_events` would require paging. SQL: `SELECT uid1, uid1_name, DATE_TRUNC('day', dt_date), MIN(dt_date), MAX(dt_date) FROM fact_transactions WHERE disposition=1 GROUP BY 1,2,3`. Easy build, common operational question. **Open question:** build standalone or fold into `person_dossier` as another section.

### ✅ D3 — `find_forced_through_attempts`
Reframed from gemini's original `find_tailgating_events` after user pushback: true tailgating leaves no badge record for the second person, so it's undetectable from log data alone. Same SQL (DuckDB `LEAD` over `fact_transactions` partitioned by `reader_name` ordered by `dt_date`) but honest framing — surfaces "denied followed by granted within N seconds at the same reader" as a credential-share / force-through signal. Parameters: time window, `door_id`, `max_gap_seconds` (default 10, max 300), limit. Returns pairs of `[denied_event, granted_event]` with gap_seconds. Door filter resolves to reader names. Unknown door returns empty.

### 🤔 D4 — Decide: minimal `compare_periods` tool
Sharp counselor disagreement: claude-opus and codex say build it (small LLMs botch arithmetic), gemini says drop it (LLMs are great at comparing JSON). Verdict depends on the demo target. For LM Studio + Qwen on the RTX 4070 (the actual demo stack), claude+codex's argument wins. If built, keep it MINIMAL: compare two windows server-side, return delta + delta_pct. No fancy grouping. Parameters: metric (events | alarms), `period_a_since`, `period_a_until`, `period_b_since`, `period_b_until`, plus standard filters. **Not yet built** — decide first whether to commit.

---

## Critical path

```
A1 ─┐
A2 ─┼─→ B2 ──→ C1 ──→ C4
A3 ─┘    │      │      ↑
         │      └──────┤
         └─→ B3 ───────┤
                       │
A3 ──→ B1 ─────────────┤
                       │
A3 + A2 → C2 ──────────┘
```

Phase A is complete. After A lands, B1, B2, B3 can run in parallel. C4 is the synthesis capstone — blocked by 5 upstream tools because it composes their outputs.

## Suggested cadence

1. **B1** — `alarm_response_metrics` (highest-leverage easy win, isolated scope)
2. **B3** — `inactive_entities` (easy, immediate user value)
3. **B2** — `person_dossier` (establishes the report-shaped tool pattern)
4. **C1** — `door_dossier` (applies the pattern to the location dimension)
5. **C2** — `timeseries_alarms` (trivial parity fill)
6. **C3** — `get_surrounding_events` (simple correlation tool)
7. **C4** — `daily_security_briefing` (synthesis capstone)
8. **D2** — `get_daily_attendance` (or fold into B2)
9. **D3** — `find_tailgating_events` (demo-worthy)
10. **D4 / D1** — decisions, then potentially build

## Open decisions parked in Phase D

- **D1 `check_authorization`** — needs scope conversation; touches the SDK boundary
- **D4 `compare_periods`** — counselors disagreed; pragmatic call for the demo stack
