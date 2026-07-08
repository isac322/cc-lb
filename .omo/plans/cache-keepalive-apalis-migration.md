# cache-keepalive-apalis-migration - Work Plan

## TL;DR (For humans)

**What you'll get:** Prompt-cache keepalive moves from per-process timers to the existing distributed Apalis scheduler, while keeping a durable per-session fence so old queued/running work cannot refresh the wrong cache. The first refresh also moves to the correct Anthropic cache anchor (`message_start` / response begin), not final response completion.

**Why this approach:** Apalis becomes the only scheduled-job backend, but not the session-state store. The separate `cache_keepalive_sessions` row is the small source of truth that makes generation races, restarts, and cross-replica execution safe.

**What it will NOT do:** It will not delete `Queued`/`Running` jobs, rely on Apalis retries, persist plaintext prompts or downstream auth, or make the request path fail because keepalive scheduling failed.

**Effort:** Large
**Risk:** High - touches request-path lifecycle timing, scheduler execution, persistent encrypted payload storage, and SQLite/Postgres parity.
**Decisions to sanity-check:** Payload side table vs inline payload, first-upstream-event fallback for non-stream anchor, and using the existing `AeadService` with keepalive-specific AAD.

Your next move: run the implementation plan only after the ADR/plan critic wave has no gaps.

---

> TL;DR (machine): Large/high-risk Rust migration: Apalis transport + durable generation fence + TTL-anchor fix + AEAD payload storage, with tests-first phases and no proxy-path behavior regressions.

Current audit closeout status:

- Audit blockers addressed: durable Apalis enqueue/worker path now has focused server tests without the old in-memory scheduler; initial and post-hit enqueue failure paths terminalize the committed generation; Postgres conformance covers concurrent real-request generation bumps and stale-CAS hit reschedule behavior.
- Still not a full final acceptance pass: spawned/process-level enabled OAuth, streaming `message_start` E2E, reboot/process worker stress, shutdown leak checks, and the full DB-failure isolation matrix remain open under Tasks 11-13.
- Evidence updated in `.omo/evidence/cache-keepalive-apalis-migration/verification-summary.md` and task files 3, 6, 11, 12, and 13.

## Scope

### Must have

- Supersede the v1 in-memory `DashMap<SessionKey, SessionEntry>` scheduler for GA/Kubernetes with Apalis as the only scheduled-job/timer/worker transport.
- Add durable `cache_keepalive_sessions` state as the authoritative current-generation fence. This is session state, not a second scheduler.
- Preserve the existing keepalive config field names and defaults:
  - `enabled`.
  - `refresh_lead_time_5m_secs` default `30`.
  - `refresh_lead_time_1h_secs` default `300`.
  - `max_refreshes_per_session` default `12`.
  - `max_total_duration_secs` default `14400`.
  - `snapshot_max_bytes` default `524288`.
  - `classifier.extra_wait_for_user_tools`, `classifier.treat_end_turn_as_ambiguous`, reserved/rejected `classifier.llm_judge`.
- Preserve current request shape semantics: Anthropic synthetic `max_tokens=0`, remove `stream`, disable thinking, remove `output_config.format`, relax forced `tool_choice` to `auto`.
- Preserve the v1 classifier semantics: `AgentInTurn` schedules, `UserTurn` cancels/stands down, `Ambiguous` fails closed while `llm_judge` remains unimplemented.
- Preserve per-principal opt-in and cache-control-only behavior. Principals without `cache_keepalive.enabled=true` and requests without `cache_control` must not allocate snapshots or schedule work.
- Preserve session identity order: `x-claude-code-session-id`, `x-claude-session-id`, `x-session-affinity`, `x-session-id`, then cache-prefix-hash fallback.
- Fix refresh timing: `run_at = cache_anchor_at + ttl - lead`. For streaming, collect `cache_anchor_at` from `message_start`; for non-stream, use first upstream response event, falling back to request dispatch time if unavailable.
- Treat the official anchor as response begin / `message_start`, not first thinking delta.
- Use generation-keyed Apalis idempotency: `cache_keepalive:<session_key_hash>:<generation>`.
- Bump generation on new real request and on successful self-reschedule. Worker checks generation before dispatch and after dispatch.
- Make post-dispatch state changes atomic conditional transitions. Any terminal mark, purge, or self-reschedule must update with a generation/status predicate; `0 rows affected` is a stale no-op.
- Preserve current cap semantics: a new real request resets `refresh_count` and `first_scheduled_at`; a successful keepalive hit increments `refresh_count`, preserves the session duration anchor, records the hit's response-begin/cache anchor, and schedules the next job from that new anchor.
- Handle partial state/enqueue failures intentionally: either fence mutation and Apalis enqueue share a transaction where feasible, or the store records explicit `enqueue_pending`/orphan state with reconciliation. No active committed fence row may be left forever without a job.
- Allow cleanup to delete only the exact old `Pending` job. Never delete `Queued` or `Running` jobs.
- Set keepalive jobs to `max_attempts=1`; misses/errors/stale/decrypt-failures/unsupported providers are terminal/noop business outcomes with metrics, not Apalis retries.
- Persist prompt payloads only encrypted at rest with AEAD and AAD bound to principal/session/generation/upstream/job-type.
- Never persist downstream `x-api-key` or `Authorization`; re-sign at fire time with storage-backed upstream credentials or fresh OAuth.
- Keep Apalis job rows lightweight; put encrypted snapshot payload in `cache_keepalive_sessions` or a dedicated `cache_keepalive_payloads` side table.
- Support SQLite for local/CI/single-node/bounded low-volume and Postgres for production multi-replica/HA. Redis remains out-of-scope/no-go until separately proven.
- Purge expired sessions, orphan payloads, terminal keepalive Apalis rows, and stale pending jobs. Do not rely on ordinary multi-day Apalis retention for prompt payload ciphertext.
- Extend observability without plaintext: scheduled/fired/cancelled/stale/noop/hit/miss/error/decrypt_failed metrics and hashed session/idempotency summaries only.

### Must NOT have (guardrails, anti-slop, scope boundaries)

- Do not make `cc-lb-engine` depend on `cc-lb-scheduler`, Apalis, SQLx pools, or storage adapters.
- Do not let keepalive scheduling or persistence errors fail the client-visible proxy response.
- Do not implement post-dispatch logic as check-then-act if a newer real request can win between the check and the update.
- Do not delete `Queued` or `Running` Apalis jobs as a cancellation mechanism.
- Do not rely on Apalis retry/backoff for keepalive business outcomes.
- Do not persist plaintext prompt/tool payloads.
- Do not persist or replay downstream proxy auth headers.
- Do not expose plaintext prompt payloads through logs, admin scheduler failures, metrics, traces, debug output, or test artifacts.
- Do not make `apalis_max_attempts` user-tunable; keepalive attempts are fixed at one.
- Do not expose a `store_downstream_auth_headers` setting; downstream auth persistence is forbidden.
- Do not broaden provider support beyond direct Anthropic OAuth until storage-backed signing is implemented for `AnthropicApiKey` and other providers are separately proven.
- Do not weaken, ignore, or delete failing tests to pass CI.

## Verification strategy

> Zero human intervention - all verification is agent-executed.

- Test decision: TDD. Each implementation phase starts with a failing unit/integration/conformance test or migration test before code changes.
- Default verification:
  - `cargo test -p cc-lb-storage-api cache_keepalive`
  - `cargo test -p cc-lb-storage-sqlite cache_keepalive`
  - `cargo test -p cc-lb-storage-postgres cache_keepalive` when `DATABASE_URL` points at a safe local test database.
  - `cargo test -p cc-lb-engine cache_keepalive`
  - `cargo test -p cc-lb-scheduler cache_keepalive`
  - `cargo build --workspace`
  - `cargo build --workspace --features postgres`
  - `lsp_diagnostics` on every changed Rust file after each edit batch.
- Evidence path: `.omo/evidence/cache-keepalive-apalis-migration/` with one evidence file per todo. Evidence must include command, exit code, and the key assertion or failure mode proven.
- Proxy-path QA: run `proxy-e2e-qa` after implementation because lifecycle scheduling and dispatch affect client-visible proxy semantics even when response bytes must remain unchanged.
- QA database rule: never run verification against the current/live cc-lb database in place. Before any SQLite or Postgres QA/e2e/stress step that needs existing cc-lb data, create a separate snapshot/clone database in `.omo/evidence/cache-keepalive-apalis-migration/` or `/tmp`, point the process under test at that snapshot, and record evidence that the source database was not mutated. For SQLite this means copying the database file before use; for Postgres this means a separate test database/schema restored/cloned from a dump or fixture. If a snapshot cannot be created safely, mark that QA row BLOCKED rather than using the current DB directly.

## Execution strategy

### Parallel execution waves

Wave 1 is storage foundation and can split SQLite/Postgres/store-trait work after the shared API shape lands. Wave 2 wires engine scheduling ports and TTL anchors. Wave 3 adds scheduler job/worker and server wiring. Wave 4 adds cleanup/observability/docs. Wave 5 is cross-backend/e2e/stress verification. Do not merge waves if a later wave needs an API from an earlier wave.

### Dependency matrix

| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |
| 1. Storage API model | None | 2, 3, 4, 5, 6 | None |
| 2. SQLite store+migration | 1 | 5, 8, 13 | 3 |
| 3. Postgres store+migration | 1 | 5, 8, 13 | 2 |
| 4. Engine enqueue port + anchor model | 1 | 6, 7, 9 | None |
| 5. Payload encryption/AAD round-trip | 1, 2, 3 | 6, 8 | 4 |
| 6. Apalis `CacheKeepaliveJob` + handler | 4, 5 | 7, 8, 10 | None |
| 7. Lifecycle anchor capture and enqueue | 4, 6 | 11, 13 | None |
| 8. Cleanup/purge job | 2, 3, 5, 6 | 13 | 9, 10 |
| 9. Metrics/admin redaction | 4, 6 | 13 | 8, 10 |
| 10. Docs/runbook updates | ADR + 6 | 13 | 8, 9 |
| 11. Server wiring | 6, 7 | 12, 13 | None |
| 12. End-to-end proxy QA | 11 | 13 | None |
| 13. Parity/stress/reboot suite | 2, 3, 8, 9, 11, 12 | Final | None |

## Todos

> Implementation + Test = ONE todo. Never separate.

- [x] 1. Define durable keepalive session records and store API
  What to do / Must NOT do: Add storage-api types for `CacheKeepaliveSession`, optional `CacheKeepalivePayload`, status enum, generation fence operations, and store trait methods. Include fields: `session_key_hash`, `principal_id`, `upstream_id`, `generation`, `refresh_count`, `first_scheduled_at`, `cache_anchor_at`, `ttl`, `status`, `expires_at`, `created_at`, `updated_at`, current job key or generation id, enqueue state, and encrypted payload reference/blob. Define reset/preserve semantics: new real request resets `refresh_count` and `first_scheduled_at`; self-refresh increments `refresh_count` and preserves the duration anchor while updating the cache anchor. Do not put SQLx, Apalis, or engine-specific dispatch types in the API.
  Parallelization: Wave 1 | Blocked by: none | Blocks: 2, 3, 4, 5, 6
  References: `crates/cc-lb-storage-api/src/cache_keepalive.rs`; `docs/adr/0004-cache-keepalive-scheduler.md` Sections 1', 4, 5; `.omo/ulw-research/20260708-apalis-keepalive-migration-consensus/SYNTHESIS.md`
  Acceptance criteria (agent-executable): storage-api unit tests cover serialization, status transitions, generation bump semantics, reset-vs-preserve counter semantics, enqueue-pending/orphan state if needed, atomic conditional transition API shape, and JSON/text mapping for SQLite-compatible complex fields.
  QA scenarios: `cargo test -p cc-lb-storage-api cache_keepalive`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-1-storage-api.md`
  Commit: Y | `feat(storage-api): define durable cache keepalive session state`

- [x] 2. Add SQLite migrations and store implementation
  What to do / Must NOT do: Add SQLite migration `0042_cache_keepalive_sessions.sql` for `cache_keepalive_sessions` and, if using a side table, `cache_keepalive_payloads`. Implement the store operations using SQLite transactions and WAL-friendly short writes. Do not use network-filesystem assumptions; follow SQLite locking rules.
  Parallelization: Wave 1 | Blocked by: 1 | Blocks: 5, 8, 13 | Can parallelize with: 3
  References: `crates/cc-lb-storage-sqlite/migrations/0041_principals_cache_keepalive.sql`; `AGENTS.md` SQLite rules; `docs/adr/0004-cache-keepalive-scheduler.md` Section 1'
  Acceptance criteria (agent-executable): SQLite conformance tests prove insert/bump generation, replace payload, mark terminal, purge expired, and stale generation no-op lookup.
  QA scenarios: `cargo test -p cc-lb-storage-sqlite cache_keepalive`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-2-sqlite.md`
  Commit: Y | `feat(sqlite): persist cache keepalive session fences`

- [x] 3. Add Postgres migrations and store implementation
  What to do / Must NOT do: Add Postgres migration `0072_cache_keepalive_sessions.sql` with indexes for due/expiry/purge queries and status/generation lookup. Implement transactionally equivalent store operations. Do not depend on SQLite-only behavior.
  Parallelization: Wave 1 | Blocked by: 1 | Blocks: 5, 8, 13 | Can parallelize with: 2
  References: `crates/cc-lb-storage-postgres/migrations/0071_principals_cache_keepalive.sql`; `docs/scheduler.md` pool isolation; `docs/adr/0004-cache-keepalive-scheduler.md` Section 1'
  Acceptance criteria (agent-executable): Postgres conformance tests prove the same semantics as SQLite, including concurrent generation bumps and transaction isolation around stale rows.
  QA scenarios: `cargo test -p cc-lb-storage-postgres cache_keepalive` with a safe local `DATABASE_URL`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-3-postgres.md`
  Closeout note: `cargo test -p cc-lb-storage-postgres cache_keepalive` passed against isolated `CI_POSTGRES_URL` schema and now includes concurrent real-request generation bumps plus concurrent stale-CAS hit-reschedule coverage.
  Commit: Y | `feat(postgres): persist cache keepalive session fences`

- [ ] 4. Replace in-engine timer dependency with an enqueue port and cache anchor model
  What to do / Must NOT do: Add an engine-side trait such as `CacheKeepaliveEnqueuer` that accepts session key, encrypted/snapshot payload input, `cache_anchor_at`, TTL, caps, and non-secret routing metadata. Keep `cc-lb-engine` free of Apalis/DB dependencies. Update scheduling calculations to use `run_at = cache_anchor_at + ttl - lead` and define non-stream fallback policy. Do not fail the proxy response if the enqueuer errors.
  Parallelization: Wave 2 | Blocked by: 1 | Blocks: 6, 7, 9
  References: `crates/cc-lb-engine/src/cache_keepalive/scheduler.rs`; `crates/cc-lb-engine/src/cache_keepalive/lifecycle_glue.rs`; `crates/cc-lb-storage-api/src/cache_keepalive.rs::refresh_delay_secs`; `docs/adr/0004-cache-keepalive-scheduler.md` Section 4
  Acceptance criteria (agent-executable): tests prove 5m/30s with 2m response schedules completion+150s, not completion+270s; repeated hit scheduling uses the synthetic hit's response-begin/cache anchor, not the original real-response anchor; disabled config/no-cache-control does not call the enqueuer.
  QA scenarios: `cargo test -p cc-lb-engine cache_keepalive`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-4-engine-port-anchor.md`
  Commit: Y | `feat(engine): enqueue keepalive from cache anchor`

- [ ] 5. Add AEAD-encrypted payload representation and secrecy tests
  What to do / Must NOT do: Define `KeepaliveSnapshotPayload` and encrypted wrapper using `AeadService`/`AeadEncryptedField` patterns. AAD must bind principal, session hash, generation, upstream id, and job type. Strip downstream `x-api-key`/`Authorization`; preserve only required non-secret Anthropic protocol headers. Do not log plaintext in `Debug`, errors, metrics, admin output, or test fixtures.
  Parallelization: Wave 2 | Blocked by: 1, 2, 3 | Blocks: 6, 8
  References: `crates/cc-lb-aead`; `crates/cc-lb-engine/src/cache_keepalive/request_snapshot.rs`; `docs/adr/0004-cache-keepalive-scheduler.md` Section 5
  Acceptance criteria (agent-executable): payload round-trip succeeds with correct AAD; tampered AAD fails; serialized DB rows do not contain plaintext prompt markers or downstream auth header values.
  QA scenarios: `cargo test -p cc-lb-storage-api cache_keepalive_payload`; `cargo test -p cc-lb-engine cache_keepalive_snapshot`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-5-payload-aead.md`
  Commit: Y | `feat(keepalive): encrypt persisted snapshot payloads`

- [ ] 6. Implement Apalis `CacheKeepaliveJob` and worker handler
  What to do / Must NOT do: Add `CacheKeepaliveJob` in `cc-lb-scheduler` with lightweight fields only: session hash, generation, principal/upstream ids, TTL/anchor metadata or references. Enqueue with `TaskBuilder::run_at_timestamp(run_at).with_idempotency_key(cache_keepalive:<session_key_hash>:<generation>)`. Handler performs pre-dispatch fence check, decrypts payload, dispatches via the existing keepalive dispatcher logic, then performs atomic post-dispatch conditional transition: reschedule on hit only if `session_key_hash` + `generation` + status still match, or terminally mark/purge on miss/error/limits only if the same predicate matches. `0 rows affected` is stale no-op. Use `RetryClass::Maintenance`/`max_attempts=1`. Do not delete `Queued`/`Running` and do not return retry outcomes for business misses.
  Parallelization: Wave 3 | Blocked by: 4, 5 | Blocks: 7, 8, 10, 11
  References: `crates/cc-lb-scheduler/src/jobs/metadata_refresh.rs`; `crates/cc-lb-scheduler/src/retry.rs`; `docs/scheduler.md`; `docs/adr/0004-cache-keepalive-scheduler.md` Section 1'
  Acceptance criteria (agent-executable): unit tests cover fresh hit reschedule from the hit's new anchor, miss terminal no retry, stale pre-check no-op, stale post-check no reschedule, stale post-dispatch terminal update no-op, real-request race between post-check and update, decrypt failure terminal no-op, unsupported provider terminal no-op.
  QA scenarios: `cargo test -p cc-lb-scheduler cache_keepalive`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-6-apalis-job.md`
  Closeout note: focused server tests now prove the durable Apalis happy path without the old in-memory scheduler: persisted session/payload plus Apalis job, worker decrypt/re-sign/dispatch, and hit reschedule through the durable pusher. This remains unchecked because the full terminal/race matrix in the acceptance criteria is not fully covered here.
  Commit: Y | `feat(scheduler): run cache keepalive jobs through Apalis`

- [ ] 7. Wire streaming `message_start` anchor capture into lifecycle enqueue
  What to do / Must NOT do: Extend the existing streaming observer to timestamp `message_start` and cache usage fields, then pass `cache_anchor_at` to the enqueue port. For non-streaming, capture first upstream response event when available, falling back to request dispatch time. Keep official wording: response begin/message_start, not first thinking delta. Do not wait until response completion to start the TTL clock.
  Parallelization: Wave 3 | Blocked by: 4, 6 | Blocks: 11, 12, 13
  References: `crates/cc-lb-engine/src/cache_keepalive/lifecycle_glue.rs::StreamingKeepaliveResponse::observe`; `.omo/ulw-research/20260708-anthropic-cache-ttl/verify-cache-ttl.md`; `docs/adr/0004-cache-keepalive-scheduler.md` Section 4
  Acceptance criteria (agent-executable): streaming fixture with `message_start` before a long body schedules from anchor; no-stream fixture schedules from fallback; classifier decisions still happen exactly once per completed request.
  QA scenarios: `cargo test -p cc-lb-engine lifecycle_keepalive_anchor`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-7-message-start-anchor.md`
  Commit: Y | `fix(keepalive): anchor refresh timing to message_start`

- [x] 8. Add keepalive cleanup and retention path
  What to do / Must NOT do: Add a dedicated cleanup job or extend a prompt-cache purge-style singleton to remove expired sessions, orphan payloads, terminal keepalive Apalis rows, and stale old `Pending` jobs. Do not rely on ordinary multi-day Apalis housekeeping for prompt payload ciphertext. Never delete `Queued`/`Running` rows.
  Parallelization: Wave 4 | Blocked by: 2, 3, 5, 6 | Blocks: 13 | Can parallelize with: 9, 10
  References: `crates/cc-lb-scheduler/src/jobs/apalis_housekeeping.rs`; `docs/adr/0004-cache-keepalive-scheduler.md` Sections 1', 5
  Acceptance criteria (agent-executable): cleanup tests prove expired payload/session purge, orphan payload purge, terminal keepalive row pruning, stale Pending cleanup, and no deletion of Queued/Running.
  QA scenarios: `cargo test -p cc-lb-scheduler cache_keepalive_cleanup`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-8-cleanup.md`
  Commit: Y | `feat(scheduler): purge expired cache keepalive state`

- [x] 9. Add metrics/admin redaction for durable keepalive
  What to do / Must NOT do: Preserve existing `cc_lb_cache_keepalive_*` metrics and add/extend outcomes for `stale`, `noop`, `decrypt_failed`, `hit`, `miss`, `error`, and cancellation reasons. Ensure `cclb_scheduler_*` job metrics identify `CacheKeepaliveJob` without plaintext. Admin scheduler failures must show job type, hashed session/idempotency, status, and sanitized reason only. Do not expose encrypted payload blobs unless explicitly redacted and bounded.
  Parallelization: Wave 4 | Blocked by: 4, 6 | Blocks: 13 | Can parallelize with: 8, 10
  References: `crates/cc-lb-engine/src/cache_keepalive/metrics.rs`; `crates/cc-lb-scheduler/src/scheduler_metrics.rs`; `crates/cc-lb-scheduler/src/admin.rs`; `docs/adr/0004-cache-keepalive-scheduler.md` Consequences
  Acceptance criteria (agent-executable): tests/snapshots prove metrics labels are bounded and admin/failure serialization contains no plaintext prompt/auth material.
  QA scenarios: `cargo test -p cc-lb-scheduler scheduler_admin_cache_keepalive`; `cargo test -p cc-lb-engine cache_keepalive_metrics`; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-9-observability.md`
  Commit: Y | `feat(observability): report durable cache keepalive outcomes safely`

- [x] 10. Update operator and scheduler docs
  What to do / Must NOT do: Update `docs/scheduler.md` job table with `CacheKeepaliveJob`; update `docs/cache-keepalive-operator.md` to correct the old completion+4m30s wording and document message-start anchoring, Apalis transport, durable session fence, storage boundary, payload encryption, purge behavior, and metrics. Update RFC-0003 only if the implementation PR includes a doc consistency pass; otherwise link to amended ADR as the superseding decision. Do not leave stale "No DB persistence" or "Coordination across replicas is a non-goal" claims unqualified for GA.
  Parallelization: Wave 4 | Blocked by: ADR, 6 | Blocks: 13 | Can parallelize with: 8, 9
  References: `docs/scheduler.md`; `docs/cache-keepalive-operator.md`; `docs/rfc/0003-prompt-cache-keepalive.md`; `docs/adr/0004-cache-keepalive-scheduler.md`
  Acceptance criteria (agent-executable): docs mention `message_start`, `cache_keepalive_sessions`, generation idempotency, no Queued/Running delete, no retries, AEAD/AAD, SQLite/Postgres boundary, Redis no-go.
  QA scenarios: markdown link check or `rg` assertions for required terms; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-10-docs.md`
  Commit: Y | `docs(keepalive): document durable Apalis scheduling semantics`

- [ ] 11. Wire server construction and shutdown
  What to do / Must NOT do: Construct the Apalis-backed keepalive enqueuer and worker dependencies in `cc-lb-server`, wiring storage pools, `AeadService`, upstream lookup/signing, HTTP client, and scheduler registration. Keep request-path scheduling best-effort: enqueue/store failures log and metric, but the original client response is unaffected. If fence mutation and Apalis enqueue cannot share a transaction, implement explicit saga/outbox/reconcile semantics so partial failures are recoverable or no-op safely. Remove or feature-gate the old in-memory scheduler wiring only after parity tests pass.
  Parallelization: Wave 4 | Blocked by: 6, 7 | Blocks: 12, 13
  References: `crates/cc-lb-server/src/app.rs`; `crates/cc-lb-engine/src/lifecycle.rs`; `docs/adr/0004-cache-keepalive-scheduler.md` Section 1'
  Acceptance criteria (agent-executable): server starts with feature disabled; enabled principal creates no scheduler dependency cycle; shutdown does not leak workers; fence write failure, payload encrypt/store failure, and enqueue failure return original response unchanged and leave no permanently active jobless session.
  QA scenarios: targeted server integration tests; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-11-server-wiring.md`
  Closeout note: focused tests now cover durable enqueuer wiring and both initial/post-hit enqueue failure terminalization. This remains unchecked because shutdown leak checks, fence-write failure isolation, and payload encrypt/store failure isolation are still open.
  Commit: Y | `feat(server): wire durable cache keepalive scheduling`

- [ ] 12. Run proxy-path end-to-end QA
  What to do / Must NOT do: Verify client-visible proxy semantics do not regress. Test disabled principal, no-cache-control request, enabled Anthropic OAuth request, streaming long response with `message_start`, and enqueue failure. Do not use real production services; use existing local test harness/mocks unless explicitly approved. Do not point the proxy QA run at the current/live cc-lb DB directly; create and use a separate DB snapshot/clone and prove the source DB was not mutated.
  Parallelization: Wave 5 | Blocked by: 11 | Blocks: 13
  References: `docs/adr/0004-cache-keepalive-scheduler.md`; `proxy-e2e-qa` skill acceptance criteria
  Acceptance criteria (agent-executable): response bytes/status/headers are unchanged for disabled/no-op paths; enabled path enqueues asynchronously; failure to enqueue does not fail proxy response; QA evidence names the snapshot DB path or cloned Postgres database and records that the original cc-lb DB was not used for writes.
  QA scenarios: run proxy e2e harness with SQLite; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-12-proxy-e2e.md`
  Commit: Y | `test(proxy): cover durable cache keepalive request path`

- [ ] 13. Run parity, race, reboot, and secrecy stress suite
  What to do / Must NOT do: Run the required acceptance suite: replace-pending, queued-stale-no-op, running-race post-check, hit-reschedules-until-max-caps, hit reschedules from the latest synthetic hit anchor, miss/error terminal no-retry, concurrent schedules/idempotency collapse, SQLite/Postgres parity, reboot durability, payload secrecy/AAD tamper, DB-failure isolation, TTL-anchor long streaming response, decrypt failure and unsupported provider terminal no-op. DB-failure isolation must enumerate fence write failure, payload encrypt/store failure, Apalis enqueue conflict, enqueue-after-fence-success failure, fence-commit-after-enqueue-success failure, worker pre-check DB failure, worker post-dispatch CAS DB failure, and cleanup DB failure. Do not rerun CI to force green; investigate any flake root cause. Do not run stress/parity/reboot QA against the current/live cc-lb DB in place; snapshot/clone to a separate QA DB first and capture the snapshot receipt.
  Parallelization: Final implementation wave | Blocked by: 2, 3, 8, 9, 11, 12 | Blocks: final release
  References: `AGENTS.md` CI flake handling; `.omo/ulw-research/20260708-apalis-keepalive-migration-consensus/SYNTHESIS.md`; `.omo/ulw-research/20260708-anthropic-cache-ttl/verify-cache-ttl.md`
  Acceptance criteria (agent-executable): all required tests pass on SQLite; Postgres suite passes against safe local test DB; stress evidence records race budgets and no plaintext leakage; every DB-backed QA artifact identifies the isolated snapshot/clone DB and states how live/current DB mutation was prevented.
  QA scenarios: targeted cargo test invocations plus stress loops; Evidence `.omo/evidence/cache-keepalive-apalis-migration/task-13-final-stress.md`
  Closeout note: focused verification, builds, durable-worker tests, partial enqueue failure tests, and Postgres concurrency/CAS checks passed. This remains unchecked because the full process-level stress/reboot/E2E/DB-failure matrix was not run.
  Commit: Y | `test(keepalive): prove durable scheduler race invariants`

## Final verification wave

> Runs in parallel after ALL todos. ALL must APPROVE. Surface results and wait for the user's explicit okay before declaring complete.

- [ ] F1. Plan compliance audit: compare implementation against this plan and ADR 0004 amended Sections 1', 4, 5. Must prove every Must Have and Must NOT item.
- [ ] F2. Code quality review: run an Oracle/reviewer pass on module boundaries, retry semantics, generation fencing, AEAD usage, and SQLite/Postgres SQL.
- [ ] F3. Real manual QA: execute proxy e2e paths and scheduler worker paths through actual local services/mocks, not just unit tests.
- [ ] F4. Scope fidelity: verify no provider expansion, no public test-only production API, no plaintext/admin leak, no unrelated refactors, and no stale docs.

## Commit strategy

- Commit future implementation by layer, not as one giant change:
  1. storage API records + migrations + conformance tests,
  2. engine enqueue port + TTL anchor capture tests,
  3. payload encryption/AAD tests,
  4. scheduler job/handler tests,
  5. server wiring + proxy-path tests,
  6. cleanup/observability/docs,
  7. final race/parity/e2e suite.
- Each commit must build/test green for the files it touches.
- Commit messages must not include `Co-authored-by`, Sisyphus attribution, or other automated trailers unless explicitly requested.
- This planning/ADR update itself should be a single docs commit only if the user later asks for a commit.

## Success criteria

- ADR 0004 records the amended decision: Apalis is the only scheduled-job backend for GA/Kubernetes; `cache_keepalive_sessions` is the durable session fence, not a second scheduler.
- The implementation keeps request shape, classifier, config names/defaults, and admin behavior stable while re-anchoring lead times to cache creation / `message_start`.
- Every scheduled job has per-generation idempotency and durable pre/post generation checks.
- No `Queued` or `Running` job deletion is used for cancellation.
- Keepalive jobs have one attempt; business outcomes do not retry.
- Payloads are AEAD-encrypted with AAD bound to principal/session/generation/upstream/job-type; downstream auth headers are neither persisted nor replayed.
- SQLite and Postgres both pass conformance tests; production multi-replica/HA documentation says Postgres.
- Cleanup removes expired sessions, orphan payloads, terminal keepalive rows, and stale pending rows without touching active claimed work.
- Observability and admin output contain no plaintext prompt/auth material.
- Required acceptance tests pass: replace pending, queued stale no-op, running race post-check, atomic post-dispatch transition race, hit reschedules until max caps from the latest hit anchor, reset-vs-preserve counter semantics, miss/error terminal no-retry, concurrent schedules, partial fence/enqueue failure recovery, SQLite/Postgres parity, reboot durability, payload secrecy/AAD tamper, DB failure isolation, TTL anchor long streaming response, decrypt failure and unsupported provider terminal no-op.

## Decision coverage map

| Session decision | Covered in |
| --- | --- |
| Existing ADR DashMap scheduler superseded but retained as history | ADR Section 1, Scope, Todos 4/6/11 |
| Current scheduler fields map to durable fence | Scope, Todo 1, Success criteria |
| TTL anchor from cache creation/`message_start`, not completion | Scope, Todo 4, Todo 7, Todo 13 |
| Streaming anchor is `message_start`, not first thinking delta; `observe` gap | Scope, Todo 7 |
| Per-principal opt-in, cache_control-only, TTLs, session headers, classifier, request shape, provider gating | Scope, Must have, Todo 10 |
| Apalis only scheduled-job backend; session table is state not scheduler | TL;DR, Scope, Todo 6, Success criteria |
| Generation idempotency `cache_keepalive:<session_key_hash>:<generation>` | Scope, Todo 6, Success criteria |
| Pending-only cleanup; never Queued/Running; pre/post fences | Must NOT, Todo 6, Todo 8, Success criteria |
| `max_attempts=1`; no retry for business outcomes | Must have, Todo 6, Success criteria |
| AEAD/AAD, no downstream auth persistence, re-sign at fire, lightweight Apalis row | Scope, Todo 5, Success criteria |
| SQLite local/CI/single-node; Postgres prod HA; Redis no-go | Scope, Todos 2/3/10, Success criteria |
| Cleanup/retention | Todo 8, Success criteria |
| Observability/admin redaction | Todo 9, Success criteria |
| Config fields/defaults preserved; lead-time semantics re-anchored; no public retry/auth knobs | Scope, Must NOT, Success criteria |
| Required acceptance tests | Todo 13, Success criteria |
| Module boundaries and request-path failure isolation | Must NOT, Todo 4, Todo 11, Final verification |
| Atomic post-dispatch transitions and partial enqueue/fence failure handling | Scope, Todo 1, Todo 6, Todo 11, Todo 13 |
| Successful hit updates next cache anchor; counter reset/preserve semantics | Scope, Todo 1, Todo 4, Todo 6, Todo 13 |
