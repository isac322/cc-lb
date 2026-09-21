# Prompt-Cache Shared Observation Store — Decision Ledger

**Status at design sign-off: review complete — final design confirmed (recorded before implementation and QA began). For current QA execution results, see the [QA document](prompt-cache-shared-store-qa.md).**

Related documents: [Implementation Plan](prompt-cache-shared-store-plan.md) · [QA Plan](prompt-cache-shared-store-qa.md)
Baseline commit: `917a1003893d87a307f297863a662b6f273e2904` (v0.5.0), issue #825
Source record: `/tmp/cc-lb-825-planning/session-dialogue.txt` (RECORD 1–117)

This document is the final decision ledger, confirmed after the full session dialogue, the scout-panel review, the user's priority re-review directive, and Main's integration audit (including the DesignPriority·FailurePriority consensus). **User priority: response delivery & availability > cache hits > latency.** Cache-hit gains justify normal bounded DB lookup latency, but a cache subsystem error must not reject or fail a request. Main filtered out exaggerated claims from the panel reports; they are not recorded in this ledger.

---

## 1. Confirmed facts

### 1.1 Defect #825 is real and its root cause is confirmed

Reproduced against the real SQLite store and cache code at HEAD `917a1003`.

| Time | Action | Reloaded cache | Control |
|---|---|---|---|
| T+0 | First observation recorded | expires T+270 | same |
| T+40 | Lifetime refreshed by a hit | expires T+310 | same |
| T+60 | Stale DB row reloaded (hydration) | **regresses to T+270** | stays T+310 |
| T+280 | Cache lookup | **judged absent (cold)** | present as expected (warm) |

Additionally, **DB writes in new-record → old-record order** also regressed expiry from 310 to 270. The defect exists in two places.

- The in-memory merge replaces the whole entry with no freshness comparison: `crates/cc-lb-server/src/prompt_cache_observation_cache.rs`
- The DB UPSERT likewise overwrites unconditionally: `crates/cc-lb-storage-{sqlite,postgres}/src/adapter/prompt_cache_observation.rs`
- The 60-second store skip (debounce) leaves the DB stale: the `refresh_debounce_secs` path in `prompt_cache_observation_cache.rs`; the setting lives in `crates/cc-lb-config/src/types.rs` `PromptCacheShadowConfig`

Reproduction logs: `/tmp/cc-lb-825-repro-native.log`, `/tmp/cc-lb-825-controls.log`.
**A pre-fix runtime reproduction on PostgreSQL has not been performed yet** — it is judged to be the same defect because it shares the same UPSERT code path, but the measured evidence is SQLite-only.

### 1.2 Admin screens do not consume the observation table directly

The dashboard's cache tokens and cost are read from usage aggregation (`cc-lb-admin/src/dashboard.rs`); per-request hit/miss is read from the request log (`cc-lb-admin/src/events.rs`). Changing the observation-storage structure does not change the data sources of these screens. Only the admin UI metadata for the removed config field (`cc-lb-admin/web/src/lib/configEditorModel.ts`) is cleaned up — any further new UI is out of scope.

### 1.3 grace is already deducted at observation-creation time (audit correction)

`prompt_cache_observation_expires_at` at `lifecycle.rs:1622–1629` computes `now + ttl − grace` and stores it on the record. The lookup side (`prompt_cache_observation_cache.rs:305`) only compares `expires_at > now`. Therefore **the read path must not add `now + grace` again** — grace is applied exactly once. A delayed commit does not restart the TTL either — expiry is an absolute timestamp computed at observation time.

---

## 2. Consolidated final decisions (KEEP / CHANGE / REMOVE)

### KEEP — retained

| Item | Detail |
|---|---|
| PostgreSQL-first + SQLite preserved | Use the existing PostgreSQL primary as the shared store. SQLite keeps the same contract for local development and CI |
| Narrow backend-neutral trait | Keep and extend the existing `PromptCacheObservationStore` (candidate-key batch read + atomic monotonic write). A future backend such as Redis must satisfy the same behavioral contract and needs its own wiring and validation — no "drop-in replacement" is promised |
| Start with the existing schema and indexes | The `prompt_cache_observations` table and existing indexes are judged sufficient initially. New schema or indexes only after query-plan measurement proves the need — no measured index proof is claimed |
| Routing priorities: health, quota, filters | Keep the existing routing pipeline's health/quota/filter priorities unchanged. The cache score is only one factor within it |
| thread_usage diagnostic telemetry | `thread_usage_score`, `record_thread_usage`, and `lineage_counterfactual_from_thread_usage` are counterfactual diagnostic telemetry of a separate lineage. They are not warmth authority and are kept separate and preserved |
| quota/rate-limit hydration/NOTIFY | Only the observation-specific path is removed; the other channels are kept |
| Telemetry and request log | Existing observability is preserved. Provider usage metrics are unaffected |

### CHANGE — modified

| Item | Detail |
|---|---|
| Routing lookup | Retire pod-local observation-cache authority; batch-query the request's candidate upstream × prefix keys from the shared store. **A normal bounded DB await is allowed** — cache-hit gains justify this cost. The result is an ordinary transient lookup result used only within that request; no separate snapshot framework is built |
| Observation publish | As soon as the first valid provider usage is obtained (SSE `message_start`, the same point on compat SSE, response-receive completion for buffered non-stream), enqueue non-blocking via `try_send` to a direct sink. No pre-checks, no waiting |
| Write path | Remove the `InMemoryBus` hop plus the subscriber's double queue; simplify to a direct `PromptCacheObservationSinkLike` port plus the existing single bounded queue |
| DB update | Atomic winner update: the same key's valid expiry never regresses by arrival order, and the winner record's metadata is preserved with it (no mixing stale metadata). Ties are resolved by expiry, then `last_observed_at`. Repeated writes of the same observation are idempotent |
| Read failure | On lookup failure, report via OTel and continue routing neutrally without cache affinity. The failure is recorded as a distinct signal, not conflated with cold. The request is never failed |
| Write failure | Enqueue failure (queue full/closed) and DB write failure are reported via OTel and the request continues |
| Failure telemetry | Cache errors are recorded with a child operation/span and existing counters, plus a new narrow read-failure counter. The root of a successful response is not marked as an error |
| Shutdown | On graceful shutdown, attempt a drain within the existing `SHUTDOWN_TASK_TIMEOUT` (500ms) in `app.rs`. The shutdown framework is not extended |

### REMOVE — removed/rejected

| Item | Reason |
|---|---|
| 60-second debounce (`refresh_debounce_secs`) | Direct cause of #825. Fully deleted from config, schema, UI metadata, and docs — no shim |
| Observation-only hydration/NOTIFY | Unnecessary once routing reads the shared store directly |
| Pod-local warmth cache authority/fallback | A replica-local warmth fallback breaks the shared contract |
| Pre-dispatch capacity reservation/acceptance check | Explicitly rejected by the user — "you'd reject a request in the name of optimization?" |
| Request rejection (503) on cache failure | Violates the availability priority |
| Commit-wait barrier at response end | The user chose independent completion |
| Arbitrary short timeout values | Ungrounded numbers are not baked into the design |
| Redis implementation now | Only a future optimization candidate. The user directed PostgreSQL-first because of the all-miss risk at cold start |
| Retry loops, durable outbox, new external services | No benefit for the complexity; must not be introduced without separate approval |
| Distributed exactly-once claims | This design does not provide that guarantee and does not claim it |

---

## 3. Confirmed completion/failure contract (user decision — FINAL)

### P1. Completion boundary — independent completion

Commits proceed in the background independent of response completion. No join point waits for pending stores to commit at response end. Commit failures are recorded via OTel without breaking the in-flight response. Client abort is not a separate product question — valid usage evidence already enqueued is not cancelled by disconnect alone, and no write is created before evidence is obtained.

### P2. Read failure — continue without cache affinity

On lookup failure, report via OTel and continue with normal routing rules without cache affinity. The failure is recorded as a state distinct from "no observation (cold)". A request is not rejected with 5xx because a cache observation failed.

### P3. Write overload/failure — attempt immediately; on failure report via OTel and continue

User's words: "just try it, and if it errors, mark it properly in otel and move on … getting the response out matters more than a cache hit … you'd reject a request in the name of optimization?" — On obtaining an observation, immediately attempt a non-blocking `try_send` to the existing single bounded queue; failures are reported via OTel and the request continues.

---

## 4. Chronology of discarded proposals (SUPERSEDED)

| Discarded proposal | History | Replacement |
|---|---|---|
| Keep pod-local cache + in-memory monotonic merge (initial analysis, RECORD 50) | At RECORD 54 the user demanded "unconditional real-time sharing between pods" and ordered a structural re-review | Direct shared-store lookup |
| Keep the 60-second debounce | User rebuttal at RECORD 51; an optimization with no performance basis | Full removal |
| Wait for DB commit before the first response event (RECORD 71–73) | User rebutted at RECORD 74 on session-sequentiality grounds | Immediate async publish |
| Immediate full switch to Redis | At RECORD 90 the user directed PostgreSQL-first, judging the cold-start all-miss risk and accepting 100ms | PostgreSQL-first + narrow trait |
| Consensus layer such as etcd/Raft | Overengineering | Reuse existing PostgreSQL |
| Hypothesis that admin screens consume observations in real time | Investigation found no direct consumer of the observation table | Only dead config-metadata cleanup |
| Freshness judgment by comparing `last_observed_at` | It is the subscriber's processing time, not the observation time, so it can misjudge | Atomic winner update in the DB |
| Pre-dispatch capacity reservation (Main's proposal) | Explicitly rejected by the user during the priority re-review | Immediate attempt + OTel reporting |
| Re-applying `now + grace` on the read path | Audit correction — grace is already deducted at creation (§1.3) | `expires_at > now` comparison only |

---

## 5. Panel contributions and Main's corrections

| Panel | Accepted contribution | Main's corrections/footnotes |
|---|---|---|
| DecisionAudit | Chronology ledger; separation of confirmed/rejected/undecided | Elevated from panel consensus to mandatory user confirmation → closed as user decisions |
| ReadArchitecture | Candidate batch-lookup interface; async fetch outside the synchronous routing region | Banned per-lookup `to_owned` allocation; corrected the `GREATEST(expires_at)` + `EXCLUDED.*` metadata-overwrite sketch for violating winner-metadata consistency; index sufficiency is a measurement task; simplified the snapshot framework to an ordinary transient lookup result |
| WriteArchitecture | Direct sink port; double-queue removal; `CancellationToken` drain | Pre-reservation was rejected by the user; validity is source-inspection-based, not runtime proof; thread_usage is a diagnostic lineage |
| QADesign | Multi-replica scenarios; `fake-anthropic` timing control | Excluded external real APIs and non-deterministic sleeps; details in the QA document |
| DesignPriority·FailurePriority (audit) | Consensus to re-review all decisions against the priority criteria; failure-telemetry layering | Main rejected the double-grace, zero-latency, and at-least-once exaggerations |

**Excluded exaggerations**: 0ms latency, 100% guarantees, an arbitrary 50ms timeout, the 11-minute premise, "perfect Redis replacement", index-scan guarantees, treating DB errors as cold, unconditional full preservation on shutdown, distributed exactly-once.

---

## 6. Explicit non-guarantees

- **Starting an async write ≠ committed before the next turn.** The guarantee begins at successful commit. A next request arriving immediately after a response completes may not see a still-uncommitted observation — a gap the user accepted.
- **The provider's internal cache state is not guaranteed.** What is shared is evidence observed from provider responses, not the provider cache's current state.
- **Losing an observation ≠ deleting the provider cache.** Losing an observation only loses the basis for a warm judgment, raising the chance of a miss.
- **Shutdown drain is best-effort within the existing 500ms.** Loss on failure or kill is not guaranteed against, and no extended shutdown framework is added.
- **No pre-fix runtime reproduction on PostgreSQL yet** — covered in QA.

---

## 7. Evidence paths

- Session dialogue: `/tmp/cc-lb-825-planning/session-dialogue.txt`
- Reproduction logs: `/tmp/cc-lb-825-repro-native.log`, `/tmp/cc-lb-825-controls.log`
- Panel reports: `agent://DecisionAudit`, `agent://ReadArchitecture`, `agent://WriteArchitecture`, `agent://QADesign`
- Key sources: `crates/cc-lb-storage-api/src/prompt_cache_observation.rs`, `crates/cc-lb-server/src/prompt_cache_observation_cache.rs`, `crates/cc-lb-server/src/prompt_cache_observation_sink.rs`, `crates/cc-lb-server/src/notify_listener.rs`, `crates/cc-lb-engine/src/sse_relay.rs`, `crates/cc-lb-engine/src/lifecycle.rs`, `crates/cc-lb-config/src/types.rs`
