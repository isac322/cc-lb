# RFC-0003: Principal-scoped prompt-cache keep-alive

- Feature Name: `prompt-cache-keepalive`
- Start Date: 2026-07-06
- Status: Draft — implementation in progress on branch `opencode/rfc-0003-cache-keepalive`.
- Related PRs: (to be filled)
- Related ADRs: [0004 — Keep-alive scheduler and dispatch strategy](../adr/0004-cache-keepalive-scheduler.md)
- Source: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>.
- Retrieved: 2026-07-27.

## Summary

Add an opt-in, per-principal feature that keeps Anthropic prompt-cache entries
warm across long-running agent turns. When enabled, cc-lb tracks each cache-
using session by `thread_id`, classifies every completed response as "the
agent is still executing tools" vs "waiting for the human user", and — for
the former — schedules a synthetic `max_tokens: 0` request against the same
upstream shortly before the cache TTL expires. A cache hit refreshes the full
TTL, and the refresh itself has no extra charge, so a single successful
keep-alive extends the window; misses tear the session down. Nothing runs
for principals without the config, and nothing runs for requests without a
`cache_control` breakpoint.

## Motivation

Anthropic prompt caching has two TTL modes: `"5m"` (default since 2026-03-06)
and `"1h"`. Cache hits refresh the TTL back to its full duration, and that
refresh adds no charge beyond the cache read itself. Cache misses force
cc-lb operators to pay the entire prefix cost again. In real agent workflows
we routinely observe:

- Bash / test / build tool calls that block for 3–20 minutes,
- Human review pauses that exceed the 5 m default but stay well under 1 h,
- Interactive planning sessions where the agent is doing multi-turn
  reasoning and the human is not responding for ~ tens of minutes.

Every such gap that crosses the TTL boundary evicts the shared system prompt
+ tool-catalogue prefix (typically 8k–40k tokens) and forces a full rewrite
on the next real user message. On production `isac-opencode` traffic the
observed rewrite cost dominates the per-request latency and quota
consumption for long sessions.

An automated keep-alive that fires only while the agent is provably still
in-turn eliminates most of these rewrites without generating a cache-miss
storm during idle nights.

## Design goals

1. **Off by default.** Zero behavior change for principals that do not opt
   in. Zero behavior change for requests that do not carry `cache_control`.
2. **No cross-principal contention.** Per-principal config, per-principal
   metrics labels, per-principal budget caps.
3. **Cheap when idle.** Session state is in-memory, per-replica, sized by
   active sessions only. No DB persistence, no cross-replica coordination.
4. **Bounded cost.** Every session has a `max_refreshes` cap and a
   `max_total_duration` wall-clock cap. New real requests always cancel
   pending keep-alives.
5. **Never break the request path.** Snapshot capture and scheduling MUST
   be side-effect-free with respect to the response the client sees.
   Errors log and drop the session, never propagate.
6. **Fail closed on ambiguity.** When the classifier cannot decide, the
   current release does *not* fire a keep-alive. The LLM judge config is
   reserved for a future implementation and is rejected while unimplemented.

## Non-goals

- Sub-second scheduling precision. Slop of ~ 30 s (before the 5 m boundary)
  is fine because Anthropic warms the cache on any read that lands inside
  the window.
- Multi-provider keep-alive. Bedrock, Vertex, and OpenRouter are out of
  scope for v1 — see the "Provider gating" section below.
- Coordination across replicas. If a session's requests are load-balanced
  across two cc-lb replicas we accept that both replicas may independently
  schedule a keep-alive — the second one just observes a cache hit and is
  cheap.

## Architecture

### Component overview

```
                              ┌──────────────────────────┐
   real /v1/messages ─────►   │       Lifecycle          │  ─────► client
                              │  ┌────────────────────┐  │
                              │  │ shape → sign →     │  │
                              │  │ dispatch → decode  │  │
                              │  └─────────┬──────────┘  │
                              └────────────┼─────────────┘
                              hook: after shape / before sign
                                           │
                          (capture RequestSnapshot: url, method,
                           headers, shaped body Bytes, upstream_id, ttl)
                                           │
                              ┌────────────▼─────────────┐
                              │      Cache-keepalive     │
                              │      module              │
                              │                          │
                              │ ┌────────┐  ┌─────────┐  │
                              │ │Session │  │Classi-  │  │
                              │ │Key     │  │fier     │  │
                              │ └────┬───┘  └────┬────┘  │
                              │      │           │       │
                              │      ▼           ▼       │
                              │ ┌─────────────────────┐  │
                              │ │  KeepaliveScheduler │  │
                              │ │  DashMap<Session,   │  │
                              │ │     SessionEntry>   │  │
                              │ └──────────┬──────────┘  │
                              │            ▼             │
                              │ ┌─────────────────────┐  │
                              │ │  KeepaliveDispatcher│  │
                              │ │  (Anthropic HTTP    │  │
                              │ │   POST max_tokens:0)│  │
                              │ └─────────────────────┘  │
                              └──────────────────────────┘
```

### Session identity — `SessionKey`

Sessions are keyed by `(principal_id, session_id)`:

- Preferred: `session_id = thread_id` from one of these client headers, in
  order — `x-claude-code-session-id`, `x-claude-session-id`,
  `x-session-affinity`, `x-session-id`. (These are the same headers WRH
  already uses for replica affinity, so the keep-alive session naturally
  hashes to the same replica as the real traffic.)
- Fallback: `session_id` is the first cache breakpoint's v3 prefix key from
  request cache metadata.

Principals without one of these headers and without a cache breakpoint have
no session identity and are silently skipped.

### Request snapshot — `RequestSnapshot`

Captured *after* `shape_request` (so plugin-injected system/messages are
included) and *before* `sign_request` (so we do not persist stale
credentials). Content:

- `url: Url` — final upstream URL,
- `method: Method` — always POST for `/v1/messages`,
- `headers: HeaderMap` — client headers minus hop-by-hop,
- `body: Bytes` — shaped body (validated as JSON object; rejected if
  larger than `snapshot_max_bytes`, default 512 KiB),
- `upstream_id: Uuid` — so the scheduler can re-fetch a fresh signer at
  fire time,
- `ttl: CacheTtl` — `Ttl5m` or `Ttl1h`, extracted from the request's
  `cache_control` breakpoint.

`build_keepalive_body()` produces the fire-time body with `max_tokens = 0`.
Anthropic rejects that pre-warm form when `stream: true`, extended thinking
(`thinking.type = "enabled"`), structured outputs (`output_config.format`),
or `tool_choice` of type `tool` or `any` is present, and it rejects the form
inside Message Batches. The builder removes `stream`, removes
`output_config.format`, coerces the incompatible tool choices to `auto`, and
coerces any `thinking` object to `type: "disabled"` while removing
`budget_tokens`.

The messages-scoped thinking salt keeps this thinking-disabled pre-warm
prefix distinct from real thinking-enabled traffic. The probe can therefore
refresh genuine tools- and system-tier entries without creating a false
message-tier warm entry. `output_config.effort` remains on the keep-alive
body because it participates in the message-tier prefix.

### Classifier — `HeuristicClassifier`

Applied on the *response* body of every real completed request. Returns
`UserTurn`, `AgentInTurn`, or `Ambiguous`. See ADR 0004 for the full
mapping table. Key rules:

- `stop_reason == "pause_turn"` or `"compaction"` → **AgentInTurn** (both
  are server-side "keep going" signals).
- `stop_reason == "refusal"` or `"model_context_window_exceeded"` →
  **UserTurn**.
- `stop_reason == "tool_use"` with at least one *client* `tool_use` block
  whose name is not on the wait-for-user list → **AgentInTurn**.
- `stop_reason == "tool_use"` with only wait-for-user tools
  (`attempt_completion`, `ExitPlanMode`, `ask_followup_question`, …) or
  only `server_tool_use` → **UserTurn**.
- `stop_reason == "end_turn"` with a `compaction` content block or with a
  completion-tool defined-but-not-invoked → **AgentInTurn**.
- `stop_reason == "end_turn"` (plain) → **UserTurn** (unless
  `treat_end_turn_as_ambiguous` opts in).
- `stop_reason == "max_tokens" | "stop_sequence"` → **Ambiguous**.

`Ambiguous` responses behave like `UserTurn` in this release (fail-safe:
prefer eviction over paying for a wasted refresh). The `llm_judge` field is
reserved for a future implementation; the admin API rejects non-null values.

### Scheduler — `KeepaliveScheduler`

Single per-replica `Arc<KeepaliveScheduler>`. State:

```rust
DashMap<SessionKey, SessionEntry {
    generation: u64,
    refresh_count: u32,
    first_scheduled_at: Instant,
    snapshot: Arc<RequestSnapshot>,
    params: ScheduleParams,
    cancel_tx: Option<oneshot::Sender<()>>,
    principal_name: Arc<str>,
}>
```

`schedule_or_replace` atomically bumps `generation`, cancels the previous
`cancel_tx`, resets counters, records `active_sessions` metric, and spawns
a `tokio::select! { sleep(delay), cancel_rx }` task. On fire the task
re-locks the entry, checks the generation matches (race guard: the sleep
completed while a cancel was already queued but not yet delivered), and
dispatches. On `CacheHit` it re-installs the same entry with counters
preserved and a fresh cancel channel; on `CacheMiss` or dispatch error it
cancels the session (`UpstreamGone`).

Existing timing behavior anchors each refresh window to response start.
`cache_anchor_at.elapsed()` is carried as `cache_anchor_age` into
`ScheduleParams::from_cache_anchor_age`, which subtracts that age from the
configured refresh delay rather than starting a full new window after body
collection.

`cancel(key, reason)` removes the entry, closes the channel, records the
cancellation reason, and updates the `active_sessions` gauge.

`shutdown()` cancels every entry with `reason=Shutdown` and is wired via
`SignalHandle::add_shutdown_hook`.

### Dispatcher — `KeepaliveDispatcher`

An `#[async_trait]` object that owns the machinery needed to (1) look up
the upstream by `snapshot.upstream_id`, (2) get a fresh signer via the
existing signer factory, (3) rebuild + sign the request with the
`build_keepalive_body()` payload, (4) POST it to Anthropic, (5) parse the
JSON response to classify hit/miss/error. The interface is minimal:

```rust
#[async_trait]
pub trait KeepaliveDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, snapshot: &RequestSnapshot) -> DispatchOutcome;
}

pub enum DispatchOutcome { CacheHit, CacheMiss, Error(String) }
```

A cache hit is recognised via `usage.cache_read_input_tokens > 0` in the
response body (documented Anthropic behavior). Empty `content: []`,
`stop_reason: "max_tokens"`, and `usage.cache_read_input_tokens > 0` is
the expected happy path.

### Storage schema

`PrincipalRecord.cache_keepalive: Option<CacheKeepaliveConfig>`, with
migrations `0038_principals_cache_keepalive.sql` (sqlite, TEXT column) and
`0069_principals_cache_keepalive.sql` (postgres, JSONB column). `Option`
means `None` for legacy rows and for new principals that omit the field.

### Provider gating

v1 fires keep-alives only against direct Anthropic OAuth upstreams.
`AnthropicApiKey` is disabled until storage-backed keep-alive signing lands;
downstream proxy keys are never replayed to Anthropic. Other providers are
skipped at fire time:

- **Bedrock, Vertex, OpenRouter:** `max_tokens: 0` schema behavior is
  untested / disallowed. OpenRouter's schema requires `max_tokens >= 1`.
  We do not attempt the request.
- **Message Batches:** `max_tokens: 0` is rejected inside a batch. The
  keep-alive dispatcher sends only standalone Messages API requests and
  never attempts batch pre-warming.
- **Detection point:** the upstream record's `kind` is inspected in the
  dispatcher; unsupported upstreams return a dispatch error and the session
  is cancelled with `reason=UpstreamGone`.

## Configuration

`CacheKeepaliveConfig` on the principal (all fields have documented defaults;
config surface is JSON persisted through the admin API):

- `enabled: bool` (default `false`; when `false` the whole feature short-
  circuits — no snapshot, no schedule, no classify).
- `refresh_lead_time_5m_secs: u32` (default `30`) — fire at TTL − lead.
- `refresh_lead_time_1h_secs: u32` (default `300`).
- `max_refreshes_per_session: u32` (default `12`) — hard cap; then the
  session self-cancels with `reason=MaxRefreshes`.
- `max_total_duration_secs: u64` (default `14400` = 4 h) — wall-clock cap.
- `snapshot_max_bytes: u32` (default `524288`) — snapshot rejected if
  shaped body exceeds this.
- `classifier: ClassifierConfig`
  - `extra_wait_for_user_tools: Vec<String>` (default `[]`).
  - `treat_end_turn_as_ambiguous: bool` (default `false`).
  - `llm_judge: Option<LlmJudgeConfig>` (default `None`, reserved for a future
    implementation; non-null values are rejected by the admin API).

Admin API additions: `POST /admin/v1/principals` and
`PATCH /admin/v1/principals/{id}` accept `cache_keepalive`;
`GET` returns it in `PrincipalResponse`; audit payload records
`cache_keepalive` in the changed-fields list.

## Observability

Prometheus metrics (all labelled by `principal_id` at minimum):

- `cc_lb_cache_keepalive_scheduled_total{principal_id, ttl}`
- `cc_lb_cache_keepalive_fired_total{principal_id, ttl, result=hit|miss|error}`
- `cc_lb_cache_keepalive_cancelled_total{principal_id, reason=new_request|no_cache_control|max_refreshes|max_duration|user_turn_detected|shutdown|upstream_gone|snapshot_too_large}`
- `cc_lb_cache_keepalive_classifier_decisions_total{decision, source=heuristic}` (`source=llm` reserved)
- `cc_lb_cache_keepalive_llm_latency_seconds` (reserved histogram; zero samples in this release)
- `cc_lb_cache_keepalive_active_sessions{principal_id}` (gauge)

Existing per-request `cache_read_input_tokens` telemetry naturally
reflects success without new event schema.

## Risks & mitigations

1. **Body size blowup.** Snapshots hold `Bytes` for every active session.
   Mitigation: `snapshot_max_bytes` cap (default 512 KiB) rejects
   pathologically large requests; DashMap size tracked by the
   `active_sessions` gauge with an operator alert.
2. **Concurrent-write cache misses.** Two real requests with the same
   prefix but fresh cache writes can race; a keep-alive fired during the
   race may see a miss and drop the session. Mitigation: session re-
   establishes automatically on the next real successful request.
3. **Classifier drift.** New Anthropic clients may adopt tool names we do
   not know. Mitigation: `extra_wait_for_user_tools` is per-principal,
   plus `treat_end_turn_as_ambiguous` for future judge-backed principals.
4. **Unimplemented LLM judge.** Mitigation: the admin API rejects non-null
   `llm_judge`, and any legacy stored value fails closed as `UserTurn`.
5. **Race between cancel and fire.** Guarded by the generation counter:
   the scheduled task re-locks the entry and returns without dispatching
   if generations no longer match.
6. **Signer drift.** Snapshot captures body only; the dispatcher fetches
   the current signer from the factory at fire time. OAuth refresh
   continues to work as normal.

## Alternatives considered

1. **apalis-managed jobs.** Rejected — the required cancel-by-key API is
   backend-specific, and cc-lb already carries a custom `idempotency_key`
   migration to work around apalis's upsert gap. See ADR 0004.
2. **Server-side cache extension without a fake request.** Not offered
   by Anthropic today; only cache *reads* refresh the TTL.
3. **Client-driven keep-alive.** Would require every agent framework to
   ship extension code and cannot use OAuth-scoped subscriptions.
4. **Dummy 1-token generation.** Wastes at least 1 output token per
   refresh, generates measurable inference cost, and cannot be marked
   safely as a probe. `max_tokens: 0` is documented and free.

## Rollout

- **Phase 0 (default off):** ship code with feature disabled at the
  principal level. Bootstrap principals unchanged.
- **Phase 1 (internal opt-in):** enable on a single test principal in
  staging with `max_refreshes = 2, max_total_duration = 600s`. Verify
  metrics and log output.
- **Phase 2 (production canary):** enable on one production principal
  with defaults. Watch `fired_total{result=miss}` and
  `cancelled_total{reason}` distributions for a week.
- **Phase 3 (documentation):** publish `docs/cache-keepalive-operator.md`
  and add a dashboard row alongside the existing warm-up dashboards.

## Unresolved questions

- Do we want a shorter default `max_total_duration` for principals whose
  workload is bursty (single-shot completions dominated)? Deferred until
  after production canary data lands.
- Should the future LLM judge default to Anthropic Haiku for the
  `same-vendor-cheap-model` argument, or default off? Current release
  reserves `llm_judge` and rejects non-null values.
