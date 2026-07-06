# ADR 0004 — Cache-keepalive scheduler, keep-alive request shape, and turn classifier

- Status: Accepted
- Date: 2026-07-06
- Ships with: (to be filled)
- Related RFC: [RFC-0003 — Principal-scoped prompt-cache keep-alive](../rfc/0003-prompt-cache-keepalive.md)

## Context

RFC-0003 defines a principal-scoped feature that, when enabled, fires
synthetic Anthropic requests to keep prompt-cache prefixes warm across
long-running agent turns. Three sub-decisions had multiple credible
options and non-obvious tradeoffs:

1. **Scheduler backend** — how to hold "one delayed cancellable task per
   session" state that respects both a hard `max_refreshes` cap and
   preemptive cancellation on new real requests.
2. **Keep-alive request shape** — what to send upstream to reset the TTL
   without spending output tokens or breaking cache invariants.
3. **Turn classifier** — how to decide, given a completed response, whether
   the agent is still executing (fire) or waiting for the user (stand down).

## Decision

### 1. Scheduler: homegrown `DashMap<SessionKey, SessionEntry>` + `tokio::spawn` + `oneshot::Sender<()>`

**Chosen** over the apalis job framework the codebase already uses for
warmup scheduling. Key rejections of the apalis path:

- **`MemoryStorage` does not honor delays.** For the RAM-only session
  state we want, the only working delay-honoring backends are SQL/Redis —
  both of which force persistence and cross-replica coordination we
  explicitly do not want.
- **No storage-agnostic cancel-by-key API.** apalis exposes
  `Worker::kill(worker_id, task_id)` for *running* jobs only; cancelling
  a queued delayed job requires raw SQL keyed by `idempotency_key`. Every
  backend needs its own DDL. cc-lb already ships a custom
  `0001_apalis_partial_unique.sql` and `sqlite_enqueue.rs::135-160`
  workaround for apalis's missing upsert-by-key, so the "add another
  bespoke SQL escape hatch" cost is a real, measured team burden.
- **Redis backend promotes scheduled jobs every 30 s by default.** That
  is inside our ~30 s slop budget but is a sharp bound where our
  homegrown scheduler has millisecond precision on `tokio::time::sleep`.
- **Restart resurrection.** apalis persistent backends re-enqueue jobs
  from disk on restart. For a stateless session cache this is a bug:
  the request that authorised the keep-alive is gone, so the resurrected
  fire is guaranteed to be a cache miss.

The homegrown pattern already exists in cc-lb — see
`KeyConcurrencyManager` (`DashMap` + Tokio semaphores), `BreakerRegistry`,
`BulkheadRegistry`, and the OAuth `RefreshLocks` single-flight — so this
adds no new architectural surface. The critical invariant, guarding the
race where a `tokio::time::sleep` wins against a pending cancel, is
enforced by a monotonically bumped `generation: u64` per session entry:
the spawned fire task re-locks the entry and returns early if
`entry.generation != expected_generation`.

### 2. Keep-alive request shape: `max_tokens: 0` on the direct Anthropic Messages API

**Chosen** based on the officially documented behavior in the Messages API
reference: "Set to `0` to populate the prompt cache without generating a
response." Cache reads reset the TTL to the full 5 m / 1 h duration
(Anthropic prompt-caching docs). The concrete rules baked into
`RequestSnapshot::build_keepalive_body`:

- **Set `max_tokens = 0`.** Response is `content: []`,
  `stop_reason: "max_tokens"`, and (on hit) `usage.cache_read_input_tokens > 0`.
- **Strip `stream`.** `stream: true` + `max_tokens: 0` is rejected as
  `invalid_request_error` (400).
- **Disable `thinking`.** Set `thinking.type = "disabled"` and remove
  `budget_tokens`. Extended thinking with `max_tokens: 0` is a documented
  400.
- **Remove `output_config.format`.** Structured output is a documented
  400 with `max_tokens: 0`.
- **Downgrade `tool_choice: {tool|any}` → `{auto}`.** Forced tool
  invocation is a documented 400 with `max_tokens: 0`. `auto` is legal.
- **Reject non-Anthropic upstreams.** OpenRouter's schema requires
  `max_tokens >= 1`; Bedrock and Vertex have not been verified. v1 hard-
  gates on the upstream `kind` in the dispatcher.

Alternatives rejected:

- **1-token dummy generation.** Costs at least one output token per fire
  (currently ~$0.0000005 for Haiku but not $0). Also has a nonzero risk
  of "generating a real answer" that pollutes any downstream logging.
- **Server-side TTL extension API.** Does not exist today; requesting one
  would take months and does not solve the immediate problem.
- **Piggyback on the next real user message.** By definition the user has
  not sent one yet.

### 3. Turn classifier: heuristic stop-reason + content-block table, with fail-closed ambiguous responses

**Chosen** because empirical review of Anthropic client behavior across
Claude Code, Cline, Roo, Aider, Continue, OpenCode, OpenHands, and Cursor
showed that `stop_reason` alone is not deterministic but `stop_reason` +
inspection of `content` block types + a client-tool-name allow-list is
correct in every non-adversarial case we observed. The full table is in
RFC-0003; the salient rules are:

- **`tool_use` + any regular client `tool_use` block → AgentInTurn.** Even
  for `Bash` / `Browser` / `Computer` tools, whose semantics are opaque
  to us. The Anthropic protocol requires the client to send back a
  `tool_result` message before the model can continue; if the user
  cancels, the next request will simply not include our keep-alive
  window because the session key is stale. Accepting the ~cents cost of
  a false-positive fire is preferable to missing every genuine long
  tool call.
- **`tool_use` with only `server_tool_use` blocks → UserTurn.** The server
  already handled it.
- **`tool_use` where every client `tool_use` block name is on the
  wait-for-user list → UserTurn.** Built-in list: `AskUserQuestion`,
  `ExitPlanMode`, `ask_followup_question`, `ask_question`,
  `cursor_ask_question`, `question`, `attempt_completion`,
  `submit_and_exit`, `finish`, `plan_mode_respond`, `act_mode_respond`,
  `report_bug`. Cross-verified across the client set above. Extendable
  per-principal via `extra_wait_for_user_tools`.
- **`end_turn` with a completion tool defined-but-not-invoked →
  AgentInTurn.** e.g. Cline's `attempt_completion`, OpenHands' `finish`,
  OpenCode's `ExitPlanMode`. Convention: if the framework declared the
  tool and the model did not call it, the model is not done.
- **`end_turn` with a `compaction` content block → AgentInTurn.** The
  documented `pause_after_compaction=true` context-management beta.
- **`pause_turn` / `compaction` stop_reason → AgentInTurn.**
- **`refusal` / `model_context_window_exceeded` → UserTurn.**
- **`max_tokens` / `stop_sequence` → Ambiguous.**
- **`null` / unknown → UserTurn** (fail-safe).

`Ambiguous` responses default to `UserTurn` in this release (fail-safe:
better to lose a warm-cache and re-write than to spend money on a wasted
fire). The `llm_judge` config field is reserved for a future provider-agnostic
judge and is rejected by the admin API while it is unimplemented.

## Consequences

- **Positive:** No new persistent state, no new coordinator, no new
  storage backend to operate. The scheduler follows the same pattern the
  engine already uses in three other places (breakers, bulkheads, OAuth
  locks). The classifier is deterministic on 90%+ of production
  responses and its failure mode is silent stand-down, not silent fire.
- **Neutral:** Keep-alive is direct-Anthropic-only in v1. Bedrock / Vertex
  / OpenRouter operators cannot opt in until we run the schema
  compatibility research (tracked in RFC-0003 unresolved questions).
- **Negative:** Session state is per-replica RAM. If a session's requests
  cross replicas (WRH gives affinity but not exclusivity) both replicas
  may schedule independently. The second replica's fire will observe a
  cache hit and cost is bounded by `max_refreshes` on each side. Cost
  ceiling per session per replica is `max_refreshes * max_tokens_0_cost`,
  which is single-digit cents even in worst case.
- **Reversibility:** All feature-gated by `enabled: bool` on the principal
  config and by the presence of an `Arc<KeepaliveScheduler>` on
  `Lifecycle`. Removing the feature is deleting the module and dropping
  the DB column; no ambient dependencies leak out.
