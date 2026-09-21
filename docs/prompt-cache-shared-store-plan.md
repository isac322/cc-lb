# Prompt-Cache Shared Observation Store — Implementation Plan

**Status at design sign-off: review complete — final design confirmed (recorded before implementation and QA began). For current QA execution results, see the [QA document](prompt-cache-shared-store-qa.md).**

Preceding document: [Decision Ledger](prompt-cache-shared-store-decisions.md) · Following document: [QA Plan](prompt-cache-shared-store-qa.md)

This plan translates the decision ledger's final design into implementation units. **User priority: response delivery & availability > cache hits > latency.** Cache-hit gains justify normal bounded DB lookup latency, and cache subsystem errors must not reject or fail a request.

---

## 1. Goals and non-goals

**Goals**
- Retire pod-local observation-cache authority; have routing batch-query the shared store (PostgreSQL primary) directly.
- Start async storage at the first valid provider observation, with no DB wait before the first stream frame.
- Remove the 60-second debounce and the observation-only hydration/NOTIFY paths.
- Ensure the same key's valid expiry never regresses by arrival order in the DB (atomic winner update). Cross-request in-memory merging is removed.

**Non-goals**
- Redis implementation (only a future optimization candidate), durable outbox, retry loops, new external services.
- Changes to other hydration/NOTIFY channels such as quota, rate-limit, or subscriptions.
- New admin-screen consumption of observation data — no new UI beyond dead config-metadata cleanup.
- PR merge — merge approval is not part of this work.

---

## 2. Integration interfaces

### 2.1 Read — routing path

- Add a **candidate batch lookup** to the existing `PromptCacheObservationStore` trait in `crates/cc-lb-storage-api/src/prompt_cache_observation.rs`. Conceptual signature (final naming fixed at implementation):
  - `list_active_for_candidates(upstream_ids, canonical_model_id, prefix_keys, not_expired_at_unix_secs) -> Vec<PromptCacheObservationRecord>`
  - Replaces the per-upstream repeated calls of the existing `list_active_for_upstream*`. No default empty implementation or silent fallback — each backend implements it explicitly.
- `crates/cc-lb-engine/src/lifecycle.rs`: perform one async lookup **before** entering the synchronous routing region (`route_span.enter()`, ~2584). **A normal bounded DB await is allowed** — cache-hit gains justify this cost.
  - The lookup result is an ordinary transient result used only within that request. No separate snapshot framework is built.
  - The lookup only captures the view at query time and does not reflect future commits in flight. Rows may expire between lookup and decision, so re-check `expires_at > now` at use time.
  - **Grace is not re-applied on reads.** Expiry is already computed and stored as `now + ttl − grace` at observation creation (`lifecycle.rs:1622–1629`). Reads only compare `expires_at > now` — adding `now + grace` again is a forbidden double application.
  - The scan is lock-free in memory and creates no unnecessary string allocations per lookup (e.g. `to_owned` for key comparison).
  - Skip the query when `prefix_keys` or the candidate upstreams are empty.
  - **On lookup failure**: report via OTel and continue neutral routing without cache affinity (P2). Do not conflate failure with cold.
- `crates/cc-lb-control/src/dynamic_view.rs`·`traits.rs`: expose the store access path for routing on `DynamicView`, and remove `PromptCacheObservationCacheLike` usage as routing authority. Keep the existing routing health/quota/filter priorities unchanged.

### 2.2 Write — observation publish path

- `crates/cc-lb-engine/src/sse_relay.rs` (fast SSE path): change the current structure that decodes at `message_start` and buffers until `message_stop` (387–410) to **publish immediately when the first valid observation is obtained at `message_start`**. Guarantee idempotent once-per-request publish.
- `crates/cc-lb-engine/src/lifecycle.rs` (compat SSE path ~4022–4470, buffered non-stream ~3522–3532): same rule — publish asynchronously as soon as the first valid provider usage is obtained. For non-stream, response-receive completion is the first acquisition point.
- Publish port: consolidate to a **direct port** that injects `PromptCacheObservationSinkLike` (`cc-lb-control/src/traits.rs`) into `LifecycleContext`/`SseRelay`, removing the `InMemoryBus` hop and the double queue in `lifecycle_prompt_cache_observation_subscriber.rs`.
- `crates/cc-lb-server/src/prompt_cache_observation_sink.rs`: async writer on the existing single bounded queue. Non-blocking `try_send` attempt — no pre-checks, reservation, or waiting. Enqueue failure (queue full/closed) and subsequent DB write failure are reported via OTel and the request continues (P3). No retries, no outbox.
- `crates/cc-lb-server/src/app.rs`: on graceful shutdown, attempt a queue drain within the existing `SHUTDOWN_TASK_TIMEOUT` (500ms, `app.rs:91`). No promise of full preservation; the shutdown framework is not extended.
- **Abort invariant**: valid usage evidence already enqueued is not cancelled by client disconnect alone, and no write is created before evidence is obtained.

### 2.3 Store — atomic winner update

- `crates/cc-lb-storage-postgres/src/adapter/prompt_cache_observation.rs`: change `ON CONFLICT ... DO UPDATE` to a **winner-consistent update**. The same key's valid expiry never regresses by arrival order, and the winner record's metadata is preserved with it — raising only `expires_at` to the max while overwriting the remaining columns with loser values is forbidden. Ties are resolved by expiry, then `last_observed_at`; repeated writes of the same observation are idempotent. Remove the observation-specific `pg_notify` publish.
- `crates/cc-lb-storage-sqlite/src/adapter/prompt_cache_observation.rs`: implement the same contract with a SQLite `UPSERT` to preserve local/CI parity.
- Migrations: start with the existing `0072_v3_prompt_cache_observations.sql` (PG) / `0042_v3_prompt_cache_observations.sql` (SQLite) tables and indexes. **New schema or indexes are added only after query-plan measurement (`EXPLAIN`) confirms the need** — no measured index proof is claimed.

### 2.4 Failure telemetry

- Cache subsystem errors (lookup failure, enqueue failure, write failure) are recorded with a child operation/span and existing counters, plus a new narrow read-failure counter.
- The root span of a successful response is not marked as an error. Provider usage metrics are unaffected.

---

## 3. Confirmed completion/failure contract (user decision)

| Policy | Confirmed behavior |
|---|---|
| **P1 completion boundary** | Independent completion. After publish, no commit-wait or join point on the request-end path. Commit failures are recorded via OTel without breaking the in-flight response. |
| **P2 read failure** | Report lookup failure via OTel and continue routing without cache affinity. Record the failure as a distinct signal, not as cold. Do not reject the request because a cache observation failed. |
| **P3 write saturation** | On obtaining an observation, immediately attempt a non-blocking `try_send` to the existing single bounded queue. Enqueue failure and DB write failure are reported via OTel and the request continues. No pre-reservation, acceptance check, request rejection, retry, or new durable queue. |

---

## 4. Per-file/symbol-group change plan

### 4.1 Storage API / PostgreSQL / SQLite

| File | Change |
|---|---|
| `crates/cc-lb-storage-api/src/prompt_cache_observation.rs` | Add candidate batch lookup to `PromptCacheObservationStore`; codify the `upsert_observation` contract as atomic winner update |
| `crates/cc-lb-storage-postgres/src/adapter/prompt_cache_observation.rs` | `ANY()`-based batch lookup, winner-consistent upsert, remove observation `pg_notify` |
| `crates/cc-lb-storage-sqlite/src/adapter/prompt_cache_observation.rs` | `IN`-binding batch lookup, same winner-consistent upsert |
| `crates/cc-lb-storage-conformance` | Add parity verification of batch lookup and winner update across both backends |

### 4.2 Engine — routing + SSE fast/compat/buffered

| File | Change |
|---|---|
| `crates/cc-lb-engine/src/lifecycle.rs` | Batch lookup + transient result passing before `route_span` entry; replace `build_candidates_with_matches` warmth lookup with lookup-result-based logic; remove local-cache dependency from `prompt_cache_observation_context` (~1483); move publish points earlier for compat SSE (~4022–4470) and buffered (~3522) |
| `crates/cc-lb-engine/src/sse_relay.rs` | Immediate publish at `message_start`, remove the `message_stop` buffer path, idempotent once-per-request publish |
| `crates/cc-lb-engine/src/lifecycle_prompt_cache_observation_subscriber.rs` | Remove or shrink with the direct-port switch — decide after caller survey |

### 4.3 Control — dynamic view / traits / event bus

| File | Change |
|---|---|
| `crates/cc-lb-control/src/traits.rs` | Clean up `PromptCacheObservationCacheLike` routing-authority methods (`snapshot_for_upstream` etc.); `PromptCacheObservationSinkLike` keeps the non-blocking `try_send` contract |
| `crates/cc-lb-control/src/dynamic_view.rs` | Remove the `prompt_cache_observation_cache` field and `prompt_cache_observation_cache_opt()`, or replace with a store handle; tidy the `prompt_cache_observation_sink` path |
| Event bus | Remove the observation-only `InMemoryBus` channel and subscriber (replaced by the direct port). Other lifecycle event buses are kept |

### 4.4 Server — wiring / sink / cache / notification

| File | Change |
|---|---|
| `crates/cc-lb-server/src/prompt_cache_observation_cache.rs` | Remove the routing-local observation map, `hydrate_from_store`, and debounce state. **The `thread_usage` family is separate-lineage diagnostic telemetry — keep it separate and preserved**; do not treat it as warmth authority |
| `crates/cc-lb-server/src/prompt_cache_observation_sink.rs` | Existing single bounded-queue writer, non-blocking `try_send` + OTel failure reporting, `CancellationToken` drain |
| `crates/cc-lb-server/src/notify_listener.rs` | Remove only the `ChangeChannel::PromptCacheObservation` hydrate branch. Keep the rate-limit/quota channels and the rebind path |
| `crates/cc-lb-server/src/dynamic_view_builder.rs` | Remove observation-cache creation and hydrate calls (~210–234, ~317–324); wire the store handle |
| `crates/cc-lb-server/src/app.rs` | Sink lifecycle; drain wiring within the existing 500ms shutdown timeout |

### 4.5 Config / schema / admin config metadata

| File | Change |
|---|---|
| `crates/cc-lb-config/src/types.rs` | Delete `PromptCacheShadowConfig.refresh_debounce_secs`. Local-cache-only fields such as `max_live_entries_per_partition` are cleaned up together after caller survey. **The `grace_margin_secs` policy is unchanged** — kept because expiry computation still uses it |
| `crates/cc-lb-admin/web/src/lib/configEditorModel.ts` | Clean up UI metadata and descriptions for the deleted config fields |
| Config docs/examples | Remove dead keys (clean cutover, no shim) |

### 4.6 Tests / docs

- Update existing tests in `prompt_cache_observation_cache.rs`, `sse_relay.rs`, and `lifecycle.rs` to the new contract, or delete tests that pin implementation details.
- Keep only tests with regression value: DB winner-consistent update, batch-lookup filtering, immediate `message_start` publish, immediate reflection after debounce removal.
- QA scenarios follow the [QA Plan](prompt-cache-shared-store-qa.md).

---

## 5. Pre-implementation investigation (required)

1. **Full caller survey**: confirm every reference to `PromptCacheObservationCacheLike`, `prompt_cache_observation_cache_opt`, `hydrate_from_store`, `PromptCacheObservationSinkLike`, `refresh_debounce_secs`, and the `PromptCacheObservation` channel. **LSP reference lookup is required; source grep and codegraph callers/impact are auxiliary** — codegraph alone does not substitute for LSP. Confirm all source callers before changing an exported API.
2. **Query-plan measurement**: check `EXPLAIN` of the batch-lookup SQL on the real schema and decide whether an index is needed.
3. **Apply confirmed contracts**: reflect the P1–P3 user decisions from §3 in the interface contracts.

---

## 6. Ownership boundaries and order of work

- **Main = integration owner.** Responsible for final integration and conflict resolution of shared files (`lifecycle.rs`, `traits.rs`, `dynamic_view.rs`).
- Parallel work boundaries: storage adapters (4.1) / engine publish+lookup (4.2) / server wiring (4.4) / config+UI (4.5) touch disjoint files and can proceed in parallel. Fix the `traits.rs`·`dynamic_view.rs` contracts first, then start.
- Order: pre-investigation (§5) → fix interface contracts → per-group implementation → local smoke/QA/regression + internal review → commit and PR creation → GitHub CI and review until green. **No merge without separate approval.**

---

## 7. Design principles for a future Redis backend

- Keep the `PromptCacheObservationStore` read (candidate-key batch lookup) and write (atomic winner update) contracts backend-neutral — a shape implementable with Redis `MGET`/Lua atomic update.
- However, **no "drop-in replacement" is promised.** A future backend must satisfy the same behavioral contract and needs its own wiring, configuration, and validation.
- Do not etch Redis-specific concepts (key schema, TTL commands) into the interface in this change.

## 8. Explicit non-guarantees

- Starting an async write does not mean committed before the next turn. The guarantee begins at successful commit.
- A DB read failure is not cold; report via OTel and continue neutral routing.
- The provider's internal cache state is out of scope.
- Shutdown drain is best-effort within the existing 500ms. Loss on failure or kill is not guaranteed against.
- No distributed exactly-once is claimed.
- No pre-fix runtime reproduction on PostgreSQL yet — covered in QA.
