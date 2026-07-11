# ADR 0004 — Cache-keepalive scheduler, keep-alive request shape, and turn classifier

- Status: Accepted; Section 1 amended on 2026-07-08
- Date: 2026-07-06
- Ships with: (to be filled)
- Related RFC: [RFC-0003 — Principal-scoped prompt-cache keep-alive](../rfc/0003-prompt-cache-keepalive.md)
- Amendment: Section 1 supersedes the v1 in-memory scheduler for GA/Kubernetes deployments. Sections 2 and 3 remain unchanged.

## Context

RFC-0003 defines a principal-scoped feature that, when enabled, fires synthetic Anthropic requests to keep prompt-cache prefixes warm across long-running agent turns. Four sub-decisions had multiple credible options and non-obvious tradeoffs:

1. **Scheduler backend** — how to hold "one delayed cancellable task per session" state that respects both a hard `max_refreshes` cap and preemptive cancellation on new real requests.
2. **Keep-alive request shape** — what to send upstream to reset the TTL without spending output tokens or breaking cache invariants.
3. **Turn classifier** — how to decide, given a completed response, whether the agent is still executing (fire) or waiting for the user (stand down).
4. **Refresh timing anchor** — whether the first keep-alive is scheduled from response completion or from prompt-cache creation / response begin.

The original v1 scheduler decision optimized for a single-replica canary and explicitly avoided durable state. The 2026-07-08 amendment revisits that decision for GA/Kubernetes, where process-local state loss, cross-replica scheduling, and restart behavior are part of the product requirement instead of non-goals.

## Decision

### 1. Scheduler: homegrown `DashMap<SessionKey, SessionEntry>` + `tokio::spawn` + `oneshot::Sender<()>` (historical v1 decision)

> Superseded by Section 1'. Retained as the historical record for the v1 single-replica/canary implementation. One supporting detail below is stale: the Redis "30 s scheduled promotion" claim should not be used as current rationale. The durable GA decision is Section 1'.

**Chosen** over the apalis job framework the codebase already uses for warmup scheduling. Key rejections of the apalis path:

- **`MemoryStorage` does not honor delays.** For the RAM-only session state we want, the only working delay-honoring backends are SQL/Redis — both of which force persistence and cross-replica coordination we explicitly do not want.
- **No storage-agnostic cancel-by-key API.** apalis exposes `Worker::kill(worker_id, task_id)` for *running* jobs only; cancelling a queued delayed job requires raw SQL keyed by `idempotency_key`. Every backend needs its own DDL. cc-lb already ships a custom `0001_apalis_partial_unique.sql` and `sqlite_enqueue.rs::135-160` workaround for apalis's missing upsert-by-key, so the "add another bespoke SQL escape hatch" cost is a real, measured team burden.
- **Redis backend promotes scheduled jobs every 30 s by default.** That is inside our ~30 s slop budget but is a sharp bound where our homegrown scheduler has millisecond precision on `tokio::time::sleep`.
- **Restart resurrection.** apalis persistent backends re-enqueue jobs from disk on restart. For a stateless session cache this is a bug: the request that authorised the keep-alive is gone, so the resurrected fire is guaranteed to be a cache miss.

The homegrown pattern already exists in cc-lb — see `KeyConcurrencyManager` (`DashMap` + Tokio semaphores), `BreakerRegistry`, `BulkheadRegistry`, and the OAuth `RefreshLocks` single-flight — so this adds no new architectural surface. The critical invariant, guarding the race where a `tokio::time::sleep` wins against a pending cancel, is enforced by a monotonically bumped `generation: u64` per session entry: the spawned fire task re-locks the entry and returns early if `entry.generation != expected_generation`.

### 1'. Scheduler (amended): Apalis scheduled-job transport + durable `cache_keepalive_sessions` fence

**Chosen for GA/Kubernetes.** Prompt-cache keepalive must use the existing Apalis-backed distributed scheduler as the only scheduled-job backend: Apalis owns delayed enqueue, durable queueing, worker claiming, `(job_type, idempotency_key)` uniqueness, and execution transport. The keepalive feature must not keep a second `tokio::spawn` timer system for production scheduling.

Apalis job rows are not the session state store. They model a single execution attempt with a queue lifecycle (`Pending`, `Queued`, `Running`, terminal). Keepalive needs a domain lifecycle: current generation, refresh count, first scheduled time, cache anchor, expiry, status, upstream, principal label/id, schedule params/caps (`max_refreshes`, `max_total_duration`), and encrypted snapshot payload. That state lives in a durable `cache_keepalive_sessions` fence table (or an equivalent store API backed by SQLite/Postgres). This table is not a second scheduler; it is the authoritative current-generation fence/source-of-truth, analogous to existing scheduler effect/claim tables such as metadata refresh claims.

The amended scheduling invariants are:

- **Generation-keyed jobs.** Every scheduled keepalive job uses an idempotency key shaped like `cache_keepalive:<session_key_hash>:<generation>`. Session identity lives in the fence row and non-secret metadata; the idempotency key is per scheduled generation, not per session forever.
- **Replace by generation bump + enqueue.** A new real request resets `refresh_count` and `first_scheduled_at`, bumps the durable generation, stores the new encrypted payload/state, and enqueues a new delayed Apalis job with `run_at_timestamp(cache_anchor_at + ttl - lead)`. A successful keepalive hit preserves the session duration anchor, increments `refresh_count`, records a new hit/cache anchor from the synthetic dispatch response begin, bumps generation, and schedules the next job from that new anchor.
- **Cancel/cleanup is narrow.** The enqueue path may delete only the exact old `Pending` job as cleanup. It must never delete `Queued` or `Running` rows, because a worker may already own them and deletion cannot abort an in-flight handler safely.
- **Stale jobs no-op.** Every worker performs a pre-dispatch generation check and an atomic post-dispatch conditional transition against `cache_keepalive_sessions`. Terminal mark, purge, and self-reschedule operations must be compare-and-set style updates such as `WHERE session_key_hash = ? AND generation = ? AND status IN (...)`; `0 rows affected` means the job is stale and must no-op. The worker must not do a separate check-then-act that can overwrite a newer real request.
- **No Apalis retry loop.** Keepalive jobs use `max_attempts = 1` (the existing maintenance/fail-fast class). Cache miss, dispatch error, decrypt failure, stale generation, unsupported provider, and expired session are terminal/noop business outcomes recorded through keepalive metrics, not scheduler retries or exponential backoff.
- **Storage boundary.** SQLite remains supported for local development, CI, and bounded single-node deployments. Postgres is the production multi-replica/HA target. Redis is out of scope and no-go until separately proven for these generation/fence semantics.
- **Request path isolation.** Keepalive snapshot persistence and job enqueue are best-effort side effects. Fence write, payload encryption/storage, or Apalis enqueue failures must log/metric and stand down without changing the client-visible proxy response.
- **Partial failure discipline.** The implementation must either mutate the fence and enqueue the Apalis job in one storage transaction where feasible, or use explicit saga/outbox/reconcile state for `enqueue_pending` / orphan cases. A committed active fence row with no job, or an enqueued job with no committed fence row, must have an intentional recover/no-op path and tests.

The durable implementation keeps the engine/scheduler module boundary intact. `cc-lb-engine` defines a scheduling port and owns classification, snapshot construction, and cache-anchor extraction; it must not depend on `cc-lb-scheduler`, Apalis, SQLx pools, or storage adapters. Storage record/operation traits live in `cc-lb-storage-api`; SQLite/Postgres adapters implement them; `cc-lb-scheduler` owns `CacheKeepaliveJob` and workers; `cc-lb-server` wires the dependencies.

The amended decision preserves the feature/configuration surface: per-principal opt-in, cache-control-only tracking, 5 m / 1 h TTLs, `refresh_lead_time_5m_secs = 30`, `refresh_lead_time_1h_secs = 300`, `max_refreshes_per_session = 12`, `max_total_duration_secs = 14400`, `snapshot_max_bytes = 524288`, session identity from `x-claude-code-session-id`, `x-claude-session-id`, `x-session-affinity`, `x-session-id`, then cache-prefix-hash fallback, unchanged v1 classifier behavior, and reserved/rejected `llm_judge`.

Observability must remain secret-safe. Keepalive metrics should report scheduled/fired/cancelled/stale/noop/hit/miss/error/decrypt_failed outcomes. Scheduler/admin failure views may show job type, status, sanitized reason, hashed session/idempotency summaries, and bounded non-secret metadata only; they must not expose plaintext prompts, downstream auth, or raw encrypted payload blobs.

The minimum acceptance suite for this amended scheduler is: replace pending, queued stale no-op, running race post-check, hit reschedules until max refresh and max-duration caps, miss/error terminal no-retry, concurrent schedules/idempotency collapse, SQLite/Postgres parity, reboot durability, payload secrecy/AAD tamper, request-path DB failure isolation, TTL-anchor long streaming response, decrypt failure terminal no-op, and unsupported provider terminal no-op.

The original Apalis objections change under the amended requirements:

- `MemoryStorage` remains unsuitable, but GA/Kubernetes wants durable scheduling rather than RAM-only timers.
- The missing storage-agnostic cancel-by-session-key API is no longer the core primitive. The core primitive is a durable generation fence plus per-generation jobs; safe cleanup can be SQL-specific and limited to old `Pending` rows.
- Restart resurrection is no longer inherently a bug. A resurrected job is valid only when the session row is still active, unexpired, and at the same generation; otherwise it no-ops. This makes restart behavior deliberate rather than accidental.
- The stale Redis scheduled-promotion rationale is not needed for the amended decision. Redis is simply not part of the supported keepalive scheduler backend until a separate design proves it.

Effective provider support remains direct Anthropic OAuth. `AnthropicApiKey` remains blocked until storage-backed signing lands; durable keepalive must not replay downstream proxy auth headers.

### 2. Keep-alive request shape: `max_tokens: 0` on the direct Anthropic Messages API

Unchanged by the Section 1' amendment.

**Chosen** based on the officially documented behavior in the Messages API reference: "Set to `0` to populate the prompt cache without generating a response." Cache reads reset the TTL to the full 5 m / 1 h duration (Anthropic prompt-caching docs). The concrete rules baked into `RequestSnapshot::build_keepalive_body`:

- **Set `max_tokens = 0`.** Response is `content: []`, `stop_reason: "max_tokens"`, and (on hit) `usage.cache_read_input_tokens > 0`.
- **Strip `stream`.** `stream: true` + `max_tokens: 0` is rejected as `invalid_request_error` (400).
- **Disable `thinking`.** Set `thinking.type = "disabled"` and remove `budget_tokens`. Extended thinking with `max_tokens: 0` is a documented 400.
- **Remove `output_config.format`.** Structured output is a documented 400 with `max_tokens: 0`.
- **Downgrade `tool_choice: {tool|any}` -> `{auto}`.** Forced tool invocation is a documented 400 with `max_tokens: 0`. `auto` is legal.
- **Reject non-Anthropic upstreams.** OpenRouter's schema requires `max_tokens >= 1`; Bedrock and Vertex have not been verified. v1 hard-gates on the upstream `kind` in the dispatcher.

Alternatives rejected:

- **1-token dummy generation.** Costs at least one output token per fire (currently ~$0.0000005 for Haiku but not $0). Also has a nonzero risk of "generating a real answer" that pollutes any downstream logging.
- **Server-side TTL extension API.** Does not exist today; requesting one would take months and does not solve the immediate problem.
- **Piggyback on the next real user message.** By definition the user has not sent one yet.

### 3. Turn classifier: heuristic stop-reason + content-block table, with fail-closed ambiguous responses

Unchanged by the Section 1' amendment.

**Chosen** because empirical review of Anthropic client behavior across Claude Code, Cline, Roo, Aider, Continue, OpenCode, OpenHands, and Cursor showed that `stop_reason` alone is not deterministic but `stop_reason` + inspection of `content` block types + a client-tool-name allow-list is correct in every non-adversarial case we observed. The full table is in RFC-0003; the salient rules are:

- **`tool_use` + any regular client `tool_use` block -> AgentInTurn.** Even for `Bash` / `Browser` / `Computer` tools, whose semantics are opaque to us. The Anthropic protocol requires the client to send back a `tool_result` message before the model can continue; if the user cancels, the next request will simply not include our keep-alive window because the session key is stale. Accepting the ~cents cost of a false-positive fire is preferable to missing every genuine long tool call.
- **`tool_use` with only `server_tool_use` blocks -> UserTurn.** The server already handled it.
- **`tool_use` where every client `tool_use` block name is on the wait-for-user list -> UserTurn.** Built-in list: `AskUserQuestion`, `ExitPlanMode`, `ask_followup_question`, `ask_question`, `cursor_ask_question`, `question`, `attempt_completion`, `submit_and_exit`, `finish`, `plan_mode_respond`, `act_mode_respond`, `report_bug`. Cross-verified across the client set above. Extendable per-principal via `extra_wait_for_user_tools`.
- **`end_turn` with a completion tool defined-but-not-invoked -> AgentInTurn.** e.g. Cline's `attempt_completion`, OpenHands' `finish`, OpenCode's `ExitPlanMode`. Convention: if the framework declared the tool and the model did not call it, the model is not done.
- **`end_turn` with a `compaction` content block -> AgentInTurn.** The documented `pause_after_compaction=true` context-management beta.
- **`pause_turn` / `compaction` stop_reason -> AgentInTurn.**
- **`refusal` / `model_context_window_exceeded` -> UserTurn.**
- **`max_tokens` / `stop_sequence` -> Ambiguous.**
- **`null` / unknown -> UserTurn** (fail-safe).

`Ambiguous` responses default to `UserTurn` in this release (fail-safe: better to lose a warm-cache and re-write than to spend money on a wasted fire). The `llm_judge` config field is reserved for a future provider-agnostic judge and is rejected by the admin API while it is unimplemented.

### 4. Refresh timing anchor: cache creation / response begin, not response completion

Prompt-cache TTL starts when Anthropic creates the cache prefix, which official wording describes as once the response begins. In streaming responses, the observable anchor is the `message_start` event and its `usage` cache fields. It is not officially specified as the first thinking delta; thinking/content deltas happen after `message_start` and should not be treated as the authoritative anchor.

The first keepalive must schedule by absolute cache anchor:

```text
run_at = cache_anchor_at + ttl - lead
```

It must not schedule by response completion:

```text
wrong_run_at = response_completed_at + ttl - lead
```

Worked example: for a 5 minute cache, 30 second lead, and a 2 minute model response, the cache should refresh at request/cache-anchor + 270 seconds. That is final response completion + 150 seconds. Scheduling final response completion + 270 seconds fires at anchor + 390 seconds, after the 5 minute cache has expired.

The implementation should capture `cache_anchor_at` from streaming `message_start`. For non-streaming requests, use the earliest reliable upstream response-begin timestamp, falling back to request dispatch time when response-begin is unavailable. `StreamingKeepaliveResponse::observe` already parses `message_start` and cache usage fields; the missing work is to timestamp that event and propagate it into scheduling.

Every successful synthetic keepalive read creates a new sliding-window anchor. Therefore the next self-reschedule must use the keepalive dispatch's own response-begin / cache-hit anchor, not the original real-response anchor.

### 5. Keep-alive payload at rest: AEAD-encrypted, AAD-bound, and short-lived

Durable keepalive requires storing enough of the shaped request to rebuild a cache-hit probe. That payload includes sensitive prompt/tool material and must never be stored as plaintext.

Persisted keepalive payloads must use the existing `cc-lb-aead` service, preferably through an `AeadEncryptedField<T>`-style wrapper. AAD must bind at least:

- principal id,
- session key hash,
- generation,
- upstream id,
- job type / payload purpose.

Downstream `x-api-key` and `Authorization` headers must not be persisted or replayed. Keepalive dispatch re-fetches the upstream and signs at fire time with storage-backed upstream credentials or fresh OAuth, using the same signer factory path as real requests. Protocol headers required by Anthropic, such as `anthropic-version` and required betas, may be retained as non-secret metadata when needed.

Apalis job rows should stay lightweight: job type, session key hash, generation, upstream/principal identifiers, and any non-secret routing/fence metadata. The encrypted snapshot should live in `cache_keepalive_sessions` or a dedicated `cache_keepalive_payloads` side table. Payload rows must be purged aggressively on success, cancel, miss/error, expiry, and orphan cleanup; ordinary multi-day Apalis retention is not acceptable for prompt payload ciphertext.

## Consequences

- **Positive:** Durable keepalive becomes restart- and replica-safe for GA/Kubernetes. The feature reuses the existing distributed scheduler infrastructure, pool isolation, job metrics, idempotency key discipline, and worker claim semantics instead of maintaining a parallel production timer subsystem. Stale `Queued`/`Running` work becomes harmless through durable generation fences.
- **Positive:** The TTL anchor fix makes keepalive useful for long streaming responses: the first refresh is scheduled from `message_start` / cache creation, not from final response completion.
- **Neutral:** The request body shape and turn classifier decisions remain unchanged. Keepalive remains direct-Anthropic-only in v1, effectively `AnthropicOauth` until storage-backed signing for `AnthropicApiKey` is implemented.
- **Negative:** The feature now owns durable state: `cache_keepalive_sessions`, optional encrypted payload rows, migrations for SQLite/Postgres, purge/retention logic, AEAD key availability, and stricter no-plaintext observability rules. SQLite remains a local/CI/single-node backend; production multi-replica keepalive should use Postgres.
- **Negative:** The scheduler worker must treat cache miss, stale generation, decrypt failure, unsupported provider, and dispatch error as terminal/noop business outcomes with metrics, not as retryable Apalis failures. This differs from retryable entity jobs and must be tested explicitly.
- **Reversibility:** The feature remains principal-gated by `cache_keepalive.enabled` and runtime wiring. Reverting the durable scheduler means disabling the job registration, removing the keepalive enqueue port implementation, and dropping `cache_keepalive_sessions` / payload tables in a follow-up migration. Request shape and classifier code are independent and can remain even if durable scheduling is disabled.
