# Prompt-Cache Shared Store QA Matrix and Execution Results

Related documents: [prompt-cache-shared-store-decisions.md](prompt-cache-shared-store-decisions.md), [prompt-cache-shared-store-plan.md](prompt-cache-shared-store-plan.md)

This is the QA list and execution results for the PostgreSQL-first shared observation store, finalized before implementation. Items designated to be measured without pass criteria are recorded as `RECORDED`; behavior-verification items are recorded as `PASS`. Execution logs and local-environment limitations are noted in each row's evidence.

## 1. Contract under verification

The observable contract the implementation must satisfy is as follows. The priority is **response delivery/availability > cache hits > latency**.

- Async storage starts the moment an observation is acquired. Response streaming and completion proceed independently of the DB commit — no wait before the first frame and no completion-boundary barrier (P1-A confirmed).
- On every request, routing batch-queries only the candidate keys from the shared store. Per-pod long-lived caches, observation NOTIFY, and hydration paths are removed from routing.
- The guarantee's starting point is **successful store commit**. Starting an async write does not guarantee visibility on the next turn.
- Writes arriving out of order for the same key must not regress the valid expiry. When the expiry-extending side wins, that observation's metadata stays consistent.
- A DB read failure differs from cold cache (confirmed no observation). It is treated as an unknown cache state, and routing continues with a neutral score without cache affinity (P2-A confirmed). The error is reported via OTel.
- Actual hits in the provider's internal cache are not guaranteed. What is verified is the observation state cc-lb records and reads.
- Write publish is attempted via `try_send` on a single bounded non-blocking sink. On queue saturation or store failure, report via OTel metrics/spans and continue the request. No capacity reservation, pre-check, request rejection, retry, outbox, or new infrastructure (P3 confirmed). Response delivery takes precedence over cache hits.

- No admin screen reads the observation table directly. However, the admin routing preview **indirectly consumes** observations through the engine's routing scorer, so it is verified by QA-H7.

## 2. Status rules

| Status | Meaning |
|---|---|
| `PENDING` | Not run. Run after implementation and record the result. |
| `PASS` | Execution complete; all Then assertions satisfied. Record the run artifact path in the evidence column. |
| `FAIL` | Execution complete; one or more Then assertions unmet. Record the failure output in the evidence column. |
| `BLOCKED` | Cannot run. Record the reason linked to an unverified precondition in §9. Do not pre-declare blockers that were not observed. |
| `RECORDED` | Executed but not a PASS/FAIL judgment row (baseline reproduction, measurement/observation record). Cite the artifact without re-running. |

Execution evidence is stored under `target/test-evidence/issue-825/` as relative paths.

## 3. Confirmed policies

The user finalized the following policies on 2026-09-20, replacing the earlier OPEN alternatives. Main and the peer reviewers confirmed the design against those priorities. Execution results are recorded in §7: behavioral scenarios are `PASS`, while baseline reproduction and measurement-only scenarios are `RECORDED`. These results establish the documented contracts within the tested environments, not an unrestricted claim of no regressions. See §4, §5, and QA-H6 for provider, reproducibility, and macOS limitations.

| ID | Issue | Confirmed behavior |
|---|---|---|
| **P1** | Completion boundary | **A. Fully async**: response completion is independent of the DB commit. Both streaming and non-streaming complete normally even while a write is blocked/failing. Next-turn visibility depends on whether the commit completed. |
| **P2** | Shared-DB read failure at routing time | **A. fail-open**: a read failure is treated as Unknown, not cold, and routing continues with a neutral score without cache-affinity weight. The error is reported via OTel. |
| **P3** | Write publish saturation/failure | **Direct attempt + failure reporting**: `try_send` to a single bounded non-blocking sink with no capacity reservation, pre-check, or request rejection. On failure, report via OTel metrics/spans and continue the request. No retry, outbox, or new infrastructure. |

## 4. Common environment

- **Isolation**: every run uses only local isolated resources. SQLite uses a per-test temporary directory; PostgreSQL uses a test-only database/schema; ports are randomly assigned or the multi-replica script's fixed localhost ports (8888, 8889, 8001, 8002, 8003, 8004, 18888). Never write to production data, credentials, or endpoints.
- **Credentials**: only dummy test keys are used (e.g. the fixed dummy tokens in the multi-replica script). No real provider keys, OAuth credentials, or production DB URLs are recorded in any artifact. Evidence logs are stored with credentials masked.
- **Fake-provider boundary**: `tests/fixtures/fake-anthropic` is a contract-faithful mock. A PASS on this harness proves cc-lb's observe/store/query behavior, but does not prove internal cache hits of a real paid provider. Live-provider verification is out-of-scope work requiring separate approval.
- **Determinism**: no `sleep` assertions that hide races. Synchronization uses test-scoped barriers (channels, `tokio::sync::Barrier`, gated mock stores, the fake provider's frame-control headers). No test-only flush is added to the production public API.
- **Time control**: component tests advance time with `cc_lb_clock::TestClock` (`advance_secs`/`advance`). `TestClock` is valid only in the in-process harness and does not apply to the separate-process servers spawned by `common::spawn_test_server_*`. Process-level tests compare absolute timestamps on DB rows directly.
- **Measurement assertions**: latency figures such as p50/p95/p99 are measured and recorded. No arbitrary preset threshold (e.g. 50ms) is a PASS condition. Assertions like 0ms latency or 100% guarantees are forbidden.

## 5. Execution commands

Test target names follow the `[[test]]` registration in `Cargo.toml`. For `cc-lb-server`, `cc-lb-admin`, `cc-lb-storage-postgres`, and `tests-multi-replica`, the integration test target name is `integration`, registered as modules in `tests/all.rs`.

```bash
# cc-lb-server integration tests (SQLite default). Example filtering only the prompt_cache_live_qa module.
CC_LB_ADMIN_SKIP_SPA=1 CC_LB_SKIP_WASM_FIXTURE_BUILD=1 \
  cargo test -p cc-lb-server --test integration prompt_cache_live_qa -- --nocapture

# cc-lb-admin integration tests (admin API contracts such as RequestEvent/rollup)
CC_LB_ADMIN_SKIP_SPA=1 CC_LB_SKIP_WASM_FIXTURE_BUILD=1 \
  cargo test -p cc-lb-admin --test integration

# PostgreSQL storage adapter live tests (CI_POSTGRES_URL required)
CI_POSTGRES_URL="postgres://cc_lb:cc_lb@127.0.0.1:5432/cc_lb" \
  cargo test -p cc-lb-storage-postgres --test integration -- --test-threads=1

# 2-replica + shared PostgreSQL 18 E2E (Docker required, DOCKER_HOST=tcp://localhost:2375)
CC_LB_MULTI_REPLICA_E2E=1 \
CC_LB_MULTI_REPLICA_POSTGRES_URL="postgres://cc_lb:cc_lb@127.0.0.1:5432/cc_lb" \
  bash tests/multi-replica/multi-replica-postgres.sh
# or the cargo wrapper:
CC_LB_MULTI_REPLICA_E2E=1 CC_LB_MULTI_REPLICA_POSTGRES_URL="postgres://..." \
  cargo test -p tests-multi-replica --test integration

# Admin Web
cd crates/cc-lb-admin/web
bun install --frozen-lockfile
bun run --shell=bun build
bun run --shell=bun lint
bun run --shell=bun typecheck
bunx --bun vitest run --passWithNoTests --maxWorkers=1 --no-file-parallelism
```

CI requirements (per `.github/workflows/ci.yml`, `web.yml`):

```bash
cargo fmt --check
bash scripts/lint-audit-redaction.sh
cargo clippy --workspace --all-targets --no-default-features --features sqlite -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --features postgres -- -D warnings
cargo deny check
cargo llvm-cov nextest --workspace \
  --exclude cc-lb-loadgen --exclude cc-lb-stress-suite \
  --all-features --test-threads=4 --no-report
cargo llvm-cov report --lcov --output-path target/coverage.lcov \
  --ignore-filename-regex '^(tests/|fuzz/|examples/|third_party/|tests/fixtures/)'
scripts/coverage-gate.sh target/coverage.lcov
```

**Reproducibility note.** The committed `tests/multi-replica/multi-replica-postgres.sh` / `tests-multi-replica` suite exercises generic OAuth, dynamic-view synchronization, and failover — it does **not** exercise prompt-cache breakpoints or peer-cache routing. The issue-825-specific runs (two-process shared PostgreSQL, live DB-fault injection, OTLP span capture, shutdown-drain gates) were driven by disposable scripts under `/tmp/cc-lb-825-qa`, and the evidence under `target/test-evidence/issue-825/` is local and git-ignored — it is not available from a fresh clone. The committed Rust regression tests are reproducible; do not claim the whole live QA reruns with one command from a fresh clone.

**PostgreSQL verification caveat.** The conformance Postgres runner (`tests/storage_roundtrips_postgres.rs:630-659`) early-returns and Rust reports `ok` when neither `CI_POSTGRES_URL` nor `PG_URL` is set — do not count that as PostgreSQL verification. `CI_POSTGRES_URL` must point at a dedicated test database. To run the both-backend regression explicitly:

```bash
: "${CI_POSTGRES_URL:?Set a dedicated PostgreSQL test URL}"
cargo test -p cc-lb-storage-conformance --no-default-features --features sqlite,postgres --test integration prompt_cache_observation_ -- --nocapture
```

The `cc-lb-storage-postgres` `prompt_cache_observation` adapter tests are marked `#[ignore]`; if running those explicitly, pass `-- --ignored` — do not blanket-run all tests with `--ignored`.

## 6. Assertion rules

- Do not assert behavior solely from the **absence** of a metric or log. An absence assertion is always paired with an observable behavior assertion (routing result, DB row, response body).
- Do not assert on SQL source text. Query behavior is verified via results and `EXPLAIN` output.
- Do not assume a fixed upper bound on the candidate-key count. Batch-lookup verification is based on the actual candidate set derived from the request.
- Do not hide races with `sleep`. When waiting is needed, use condition polling with a bounded deadline (the existing `wait_for_*` pattern in `support.rs`) or an explicit barrier.
- The fake provider's `x-fake-ttft-ms`, `x-fake-inter-token-ms`, `x-fake-delta-count`, and `x-fake-mode` headers are means of creating latency, **not deterministic barriers**. A scenario that "asserts the DB while holding provider completion" synchronizes via a test-scoped explicit gate (the fixture's stream-hold hook + a store-commit confirmation signal). The x-fake latency headers are only for auxiliary smoke. No production public API is added for this.

## 7. QA matrix

### A. Baseline reproduction

#### QA-A1. #825 baseline reproduction (hydration regresses a locally refreshed expiry)

- **Status**: RECORDED
- **Given**: the real SQLite store and cache at HEAD `917a1003893d87a307f297863a662b6f273e2904`. Of two caches that received the same observation, only one gets a DB reload applied.
- **When**: T+0 first record (expiry T+270) → T+40 lifetime refresh by a hit (expiry T+310) → T+60 stale DB record reload → T+280 lookup.
- **Then**: the reloaded cache's expiry regresses to T+270 and the T+280 lookup judges it absent. The control stays at T+310. A DB write in new-record → old-record order also regresses expiry 310→270.
- **Environment**: isolated local SQLite, native execution.
- **Evidence**: `/tmp/cc-lb-825-repro-native.log`, `/tmp/cc-lb-825-controls.log`.
- **Note**: the same defect on PostgreSQL has not yet been proven by execution. Since the new structure has no hydration path, this scenario's post-change coverage moves to the §8 mapping and QA-B7/B8, QA-C1.

### B. Write path

#### QA-B1. Fast-path SSE `message_start` early publish

- **Status**: PASS
- **Given**: a streaming request. The fake provider carries cache usage on `message_start`. Stream hold is fixed by a test-scoped explicit gate (a fixture hook that holds the final frame after sending `message_start` until a test signal) — the `x-fake-*` latency headers are not deterministic barriers and are used only for auxiliary smoke.
- **When**: after confirming via a test barrier that the server received the `message_start` usage (regardless of client frame receipt), query the store and confirm the commit while the final frame is still held. Then release the gate.
- **Then**: the `(upstream_id, canonical_model_id, v3_prefix_key, ttl_class)` observation row is committed before the stream completes. The old behavior of deferring publish until `message_stop` would fail this assertion.
- **Environment**: live QA harness in the `cargo test -p cc-lb-server --test integration` scope, isolated SQLite.
- **Evidence**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` (23 pass — production Lifecycle fast-path early publish, commit confirmed while the gated store holds the final frame), `target/test-evidence/issue-825/observation_failure_isolation.log` (9 pass — SQLite commit via the real server sink confirmed before the final gate frame).

#### QA-B2. Compat SSE path `message_start` early publish

- **Status**: PASS
- **Given**: a streaming response passing through a dialect with an SSE event transform hook (compat path). The `sse_event_transform_hook` branch of `relay_response`.
- **When**: same as QA-B1 — after the usage-receive barrier, query the store and confirm the commit while the final frame is held.
- **Then**: the transform path also publishes early, identically. The publish point is the same regardless of whether a transform is applied.
- **Environment**: requires a test fixture expressing a compat dialect. If no such fixture exists, record it in §9 and run after adding the fixture — not BLOCKED.
- **Evidence**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` (23 pass including compat-path early publish), `target/test-evidence/issue-825/observation_failure_isolation.log` (real server sink compat commit confirmed).

#### QA-B3. First frame delivered while the DB write is blocked

- **Status**: PASS
- **Given**: a test-only store that holds writes on a gate (extending the existing `BlockingStore` pattern with a controllable gate instead of `future::pending`). A streaming request.
- **When**: the observation write stays blocked at the gate.
- **Then**: the first SSE frame (`message_start` or the first delta) is delivered to the client without a DB commit. "DB commit complete before the first delta" is not required. The write completes after the gate is released.
- **Environment**: in-process harness (needs a point where the gated store can be injected).
- **Evidence**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` — first frame delivered while the gated store is blocked. Ordering of frame receipt vs commit recorded.

#### QA-B4. Publish around abort and absence of duplicate publish

- **Status**: PASS
- **Given**: two streaming requests. (a) After confirming via a test barrier that the server received the `message_start` usage (regardless of whether the client received the frame), forcibly close the client connection. (b) Close the client connection before `message_start` arrives. Publish count is measured with a test-only observing store wrapper.
- **When**: after each case, query the store and check the publish counter.
- **Then**: (a) the row based on the already-observed `message_start` is committed and preserved. (b) an abort before observation produces no publish. Since the DB row count (1 row per upsert key) alone cannot prove the absence of duplicate publishes, assert that the observing wrapper recorded exactly 1 accepted publish batch — there must be no second publish at `message_stop` or at abort. This is a **local single-publish** contract. No distributed exactly-once/at-least-once guarantee is required.
- **Environment**: live QA harness, forced client socket close.
- **Evidence**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` — 23 pass including before/after abort cases. Per-case DB row counts and publish counters.

#### QA-B5. Non-streaming (buffered) publish

- **Status**: PASS
- **Given**: a request with `stream` unset/`false`. A buffered response including usage via `MessageScript`'s `ScriptedMessageResponse`.
- **When**: after receiving the response, query the store, then send a second request with the same prefix.
- **Then**: the observation row exists after the response completes, and the second request's routing reflects that observation. Per P1-A, response completion does not imply a commit, so commit completion is confirmed via a test barrier before the second request.
- **Evidence**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — buffered publish confirmed in a real 2-process PG environment.

#### QA-B6. New-key write

- **Status**: PASS
- **Given**: an observation for a new `(upstream, model, prefix, ttl)` key absent from the store.
- **When**: run the upsert.
- **Then**: the row is inserted and every field (`expires_at`, `last_observed_at`, `hash_schema_version`, `prefix_content_block_index`, `estimated_prefix_tokens`, `token_estimate_source`) matches the recorded values.
- **Environment**: store-level contract test. Both SQLite and Postgres.
- **Evidence**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — SQLite+PG 22 store tests × 20 runs = 440 pass.

#### QA-B7. Out-of-order write — a stale observation arrives late

- **Status**: PASS
- **Given**: the same key. First commit a write with expiry T+310.
- **When**: then commit a stale observation write with expiry T+270. Also run the opposite direction (old value committed, then new value committed as a normal refresh).
- **Then**: out-of-order — `expires_at` stays at T+310 and the winner's (T+310 observation's) metadata remains consistent. "Keep only expiry at max and overwrite metadata with the stale values" is a fail. In-order — the new write refreshes the old row normally. A lookup at T+280 finds the key valid.
- **Environment**: store-level contract test. Both SQLite and Postgres. In baseline QA-A1, SQLite regressed under this ordering, so this is a regression-prevention row.
- **Evidence**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 440 pass for out-of-order/in-order monotonic merge.

#### QA-B8. Concurrent writes — overlapping updates from two pods/two clients

- **Status**: PASS
- **Given**: two writes with different expiries for the same key, issued concurrently from separate connections. Additionally prepare a same-expiry, different-metadata (tie) case.
- **When**: query after both commits complete. The tie case is also run in both arrival orders.
- **Then**: the final `expires_at` is the larger of the two, and the metadata is consistent with that winner observation. The result is the same regardless of commit order. For a tie (same `expires_at`), the larger `last_observed_at` wins and its metadata remains consistent — overwriting with arbitrary stale metadata is a fail. Re-publishing a fully identical observation (identity replay) is idempotent.
- **Environment**: Postgres runs with 2 separate connections. SQLite runs both sequential and concurrent writes on the same file.
- **Evidence**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 440 pass including concurrent writes, ties (coherent tie), and idempotent replay.

#### QA-B9. Observation loss reporting and request continuation on publish saturation (P3 confirmed)

- **Status**: PASS
- **Given**: the non-blocking sink's bounded queue deliberately filled.
- **When**: dispatch a request carrying a new observation.
- **Then**: the request is not rejected and completes normally. The `try_send` failure is observably recorded via OTel metrics/spans. Assert there is no capacity reservation, pre-check, or request rejection. The dropped observation does not appear in the DB — this loss is allowed behavior and only reporting is required.
- **Environment**: requires a test hook that fills the queue (§9).
- **Evidence**: `target/test-evidence/issue-825/observation_failure_isolation.log` — queue-full/closed × stream/buffered all HTTP 200 + real counter/WARN confirmed. However, the queue cases are not OTLP-exported, so the in-process counter/WARN is the evidence (see QA-F4).

### C. Read path

#### QA-C1. Two-process read-after-commit (no NOTIFY/rebind)

- **Status**: PASS
- **Given**: cc-lb processes A and B attached to a shared PostgreSQL. Send a Turn 1 request to A and confirm the observation commit.
- **When**: send a Turn 2 request with the same prefix to B. B's local state is empty.
- **Then**: B routes to the upstream A recorded, using only the shared-DB lookup — however, this expectation holds only under **controlled equal conditions** (candidates equally eligible under other constraints such as health/quota/rate-limit). A warm observation overriding health/quota/rate-limit constraints is a fail. It must hold without observation NOTIFY receipt or view rebind — the key point is that this scenario passes even after the observation channel (`cclb_prompt_cache_observation_changed`) is removed.
- **Environment**: `tests/multi-replica/multi-replica-postgres.sh` extended, or an equivalent 2-process harness. Docker Postgres 18.
- **Evidence**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — C1 4/4 pass on real 2-process PG. B's `request_events_v1` `upstream_id` and the fake provider's receipt record.

#### QA-C2. Immediate next turn (P1-A confirmed)

- **Status**: RECORDED — `target/test-evidence/issue-825/runtime/RESULTS.tsv` QA-C2: turn2_upstream recorded, commit_seen_at=1789919627 turn2_done=1789919627.
- **Given**: immediately after the Turn 1 response completes, send Turn 2 with no polling or artificial wait.
- **When**: the next request at the same moment the response completes.
- **Then**: per P1-A, Turn 2 reflecting the Turn 1 observation is not a guarantee. This row records behavior: reflected if the commit completed, not reflected if not — both are allowed. Record the Turn 1 commit time and the Turn 2 routing result together to observe the size of the uncommitted window.
- **Evidence**: Turn 2 routing result and Turn 1 commit time.

#### QA-C3. Request snapshot consistency

- **Status**: PASS
- **Given**: a request mid-routing computation. Between lookup time and selection time, inject (a) a new observation commit and (b) expiry elapsing on an existing row, respectively.
- **When**: apply the lookup result to that request's selection.
- **Then**: a request's routing uses only that request's lookup result — one-shot data, not a separate snapshot framework but a request-scoped lookup result. (a) A row committed after the lookup is not reflected in this request (it is reflected in the next request). (b) **Expiry is re-evaluated at selection time** — a row whose expiry elapsed between lookup and selection is excluded from this decision. Confirm there is no warmth cache reusing observations across requests.
- **Environment**: in-process harness. `TestClock` makes expiry elapse deterministically.
- **Evidence**: `target/test-evidence/issue-825/lifecycle-regressions.log` (59 pass, 1 existing ignored — includes the snapshot-consistency case).

#### QA-C4. Empty candidate key set — lookup skipped

- **Status**: PASS
- **Given**: a request with no cache breakpoint (empty candidate key set).
- **When**: run routing.
- **Then**: no read query is issued to the observation store. Routing completes normally. The assertion uses a query-counting wrapper store (no log-absence assertions).
- **Environment**: in-process harness, counting store wrapper.
- **Evidence**: `target/test-evidence/issue-825/lifecycle-regressions.log` — 59 pass including the empty-key lookup-skip case.

#### QA-C5. Lookup bounded to exact candidate keys

- **Status**: PASS
- **Given**: many rows for other upstreams/models unrelated to the request's candidate `(upstream × model × prefix × ttl)` key set exist in the store.
- **When**: run the routing lookup.
- **Then**: the lookup is bounded to the request-derived candidates' upstream·model·prefix and the TTL classes the request allows. An unbounded lookup with no bindings (returning the whole table) is a fail. Returned rows are a subset of the candidate set. The planner choosing a seq scan on a small table is not itself a failure — the forbidden target is a query with no candidate bounding.
- **Environment**: store level + routing level. On Postgres, confirm index usage with `EXPLAIN`.
- **Evidence**: `target/test-evidence/issue-825/lifecycle-regressions.log` — 59 pass including the candidate-bounded lookup case. For `EXPLAIN` output see pg-explain/sqlite-plan under `target/test-evidence/issue-825/runtime/measure-lookup/`.

#### QA-C6. Query plan and latency measurement

- **Status**: RECORDED — measurement complete (a record row, not a threshold evaluation).
- **Given**: an observation table of representative cardinality (candidate-key count and table row count grounded in the real workload — no fixed upper-bound assumption).
- **When**: run the routing lookup repeatedly to measure p50/p95/p99 and capture `EXPLAIN (ANALYZE, BUFFERS)`.
- **Then**: record the measurements and the plan. No PASS/FAIL threshold is set in advance — no arbitrary cache-specific latency ceiling; record lookup latency and cache-hit gains together for evaluation. The measurement result is the sole basis for needing a new index/schema — do not presuppose a schema change before measuring.
- **Environment**: Postgres live. The same measurement is run in parallel on SQLite for comparison.
- **Evidence**: `target/test-evidence/issue-825/runtime/measure-lookup/summary.txt` + pg-explain/sqlite-plan — 200 iterations, live + synthetic 50k rows. pg-live p50=0.040ms p95=0.047ms p99=0.069ms, pg-synth p50=0.167ms p95=0.178ms p99=0.185ms, sqlite-live p50=0.008ms p99=0.012ms, sqlite-synth p50=0.073ms p99=0.090ms. Limitation: this is not an end-to-end/network/production-cardinality measurement, so no conclusion is drawn for that scope.

#### QA-D1. Expiry boundary correctness

- **Status**: PASS
- **Given**: a row with `expires_at = T`.
- **When**: query at `now = T-1`, `now = T`, `now = T+1` respectively.
- **Then**: per the `expires_at > now` contract, returned at `T-1`, not returned at `T` and `T+1`. This boundary is a fixed contract and is not adjusted to match implementation results. Grace is deducted only once at write time (`prompt_cache_observation_expires_at` in `lifecycle.rs`) — deducting grace again on the lookup/re-store path is a fail. Even if the commit is delayed, the stored absolute `expires_at` stays relative to observation time; the TTL is not counted again from store time.
- **Environment**: store level, `TestClock` or fixed-time binding.
- **Evidence**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 440 pass including expiry boundary, no double grace, and no TTL reset on delayed commit.

#### QA-D2. Key-component isolation

- **Status**: PASS
- **Given**: seed rows each differing in exactly one of upstream, canonical model, prefix, TTL class, or hash schema version.
- **When**: query and route with a specific `(upstream, model, prefix, ttl)` combination.
- **Then**: each dimension is isolated independently. Warm rows of other upstreams/models/prefixes/TTLs do not leak into this request's score. Rows whose `hash_schema_version` differs from `HASH_SCHEMA_VERSION` are excluded from warmth.
- **Environment**: store level + routing level.
- **Evidence**: `target/test-evidence/issue-825/lifecycle-regressions.log` — 59 pass including the schema+TTL isolation case. `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 440 pass for store-level isolation.

#### QA-D3. Pod/DB restart and TTL non-extension

- **Status**: PASS
- **Given**: after committing an observation row, (a) restart the cc-lb pod, (b) stop/start the Postgres container.
- **When**: after the restart completes, query and route at a point before expiry.
- **Then**: the committed observation remains valid after restart. Expiry is based on an absolute timestamp and is not extended by a restart — it is read with its remaining lifetime reduced by the restart duration. Rows past expiry do not resurrect.
- **Environment**: multi-replica harness or single process + Docker Postgres restart.
- **Evidence**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — TTL non-extension confirmed after pod restart + Postgres container restart.

### E. Regression — removal targets

#### QA-E1. 60-second debounce removal — immediate re-store

- **Status**: PASS
- **Given**: a key with a committed observation. At component level, advance `TestClock` by `advance_secs(40)` (below the 60-second debounce window).
- **When**: a second observation (hit refresh) occurs for the same key.
- **Then**: a new write is published and committed without waiting 60 seconds. Assert the write occurs while guaranteed to be inside the debounce window via TestClock advancement — not a wall-clock trick like a `>` comparison within the same second.
- **Environment**: in-process component test (a point where `TestClock` can be injected). At process level, substitute a comparison of the DB row's `last_observed_at`.
- **Evidence**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — immediate re-store (rewrite) after debounce removal confirmed.

#### QA-E2. Observation-only hydration/NOTIFY removal — quota/rate-limit unaffected

- **Status**: PASS
- **Given**: a process running the new structure.
- **When**: (a) after an observation write, check a peer pod's behavior. (b) raise a quota/rate-limit change event.
- **Then**: (a) routing works via direct lookup as in QA-C1, and the observation hydration path (the `PromptCacheObservation` branch of `hydrate_from_store`, the `cclb_prompt_cache_observation_changed` channel) does not exist. (b) hydration/rebind of other channels such as `UpstreamRateLimit` and `SubscriptionQuota` works as before — the removal scope is only the observation-specific path.
- **Environment**: multi-replica or notify_listener-level test.
- **Evidence**: (a) `target/test-evidence/issue-825/runtime/RESULTS.tsv` C1 — direct-lookup routing without the observation channel. (b) `target/test-evidence/issue-825/notify_listener.log` — 5/5 pass, including quota/rate-limit hydration working without a view rebuild.

#### QA-E3. Dead config removal (clean cutover)

- **Status**: PASS
- **Given**: an old-format config file containing removal-target settings such as `refresh_debounce_secs`.
- **When**: run config load/validation; render the admin settings screen.
- **Then**: the removed fields are absent from the schema, parser, and admin UI (the `prompt_cache_shadow.refresh_debounce_secs` entry in `configEditorModel.ts`). No no-op compatibility shim or warn-and-ignore option. An old config file containing removed keys is rejected on load per the `deny_unknown_fields` contract.
- **Environment**: `cargo test -p cc-lb-server --test integration` (validate family) + `bunx --bun vitest run` + browser QA (QA-H4).
- **Evidence**: `/tmp/cc-lb-825-removed_nested_fields_and_aliases_are_rejected.log` — old config with removed nested fields/aliases rejected by `deny_unknown_fields`. `target/test-evidence/issue-825/browser/desktop_prompt_cache_shadow_*.png` — removed fields absent from the UI.

### F. Failure/saturation — prove cache subsystem errors do not fail requests

#### QA-F1. DB read failure at routing time (P2-A confirmed)

- **Status**: PASS
- **Given**: a store or network block that can inject a DB connection failure/timeout at routing-lookup time.
- **When**: send a request while reads are failing.
- **Then**: the request does not fail. Routing continues with a neutral score without cache-affinity weight, and the read failure is not recorded as cold cache (confirmed no observation). The error is reported via OTel. "Interpreting failure as cold" or rejecting the request is a fail.
- **Environment**: a failure-injectable store wrapper or the container-control pattern of `db_unreachable_503.rs`.
- **Evidence**: `target/test-evidence/issue-825/lifecycle-regressions.log` — 59 pass including the read-failure Unknown↔cold distinction case. At real HTTP level: `target/test-evidence/issue-825/runtime/fault-response-{false,true}.txt` + `fault-exported-spans.log` + `fault-span-summary.json` — after injecting a read failure via table rename, HTTP 200, OTLP lookup Error child span + successful 200 root confirmed.

#### QA-F2. Write failure handling (P1-A confirmed)

- **Status**: PASS
- **Given**: a store whose `upsert` always fails (existing `FailingStore` pattern).
- **When**: proceed through response completion after an observation occurs.
- **Then**: the write failure is observably recorded via OTel metrics/spans, and both streaming and non-streaming responses complete normally (existing `observation_failure_isolation` contract preserved). The failed observation is not committed and does not appear in subsequent lookups — the guarantee's starting point is commit success. Confirm there is no retry or outbox.
- **Environment**: in-process harness.
- **Evidence**: `target/test-evidence/issue-825/observation_failure_isolation.log` — write-fail × stream/buffered HTTP 200 + real counter/WARN. `target/test-evidence/issue-825/runtime/fault-response-{false,true}.txt` — HTTP 200 + OTLP error child span under injected write failure.

#### QA-F3. Behavior under saturation (P3 confirmed)

- **Status**: PASS
- **Given**: a burst of observations while the non-blocking sink's bounded queue is exhausted.
- **When**: additional observations under saturation.
- **Then**: same criteria as QA-B9. Requests are not rejected and complete normally, and `try_send` failures are reported via OTel. A silent drop (unreported loss) is a fail.
- **Environment**: same as QA-B9.
- **Evidence**: `target/test-evidence/issue-825/observation_failure_isolation.log` — queue-full/closed × stream/buffered HTTP 200 + counter/WARN. The queue cases are not OTLP-exported; the in-process counter/WARN is the evidence.

#### QA-F4. Cache failure matrix — stream/non-stream response success and OTel reporting

- **Status**: PASS
- **Given**: inject each cache-subsystem failure individually: (a) routing read failure, (b) publish queue saturation, (c) queue closed, (d) store write failure. Apply each failure to both streaming and non-streaming requests.
- **When**: run requests to completion under each failure.
- **Then**: responses complete normally in every combination — no request fails or is rejected because of a cache error. Each failure is reported as an OTel child operation error/counter, and the successful HTTP root span is not wrongly marked as an error. Provider hit/miss counters stay based on actual provider responses, independent of cache-observation failures. On read failure, routing applies the normal constraints (health/quota/rate-limit) unchanged with a neutral score.
- **Environment**: failure-injection store wrapper + queue-control hook + OTel/metrics capture.
- **Evidence**: `target/test-evidence/issue-825/runtime/fault-response-{false,true}.txt` + `fault-metrics-before/after` + `fault-exported-spans.log` + `fault-span-summary.json` — real HTTP 200 under table-rename read/write failure, matching OTLP lookup Error child span, successful 200 root. Queue cases are not OTLP-exported — the in-process counter/WARN in `target/test-evidence/issue-825/observation_failure_isolation.log` is the evidence.

### G. Lifecycle

#### QA-G1. Shutdown drain vs force-kill

- **Status**: PASS
- **Given**: a state with observations published but not yet committed. The DB responds normally.
- **When**: (a) SIGTERM (graceful drain), (b) SIGKILL, (c) DB failure or shutdown-deadline exceeded during drain.
- **Then**: (a) with a healthy DB, queued writes finish committing within the existing shutdown bound (500ms) before the process exits — "tried to complete" alone is a fail; the commit is confirmed by a DB query after exit. Synchronization uses a controllable test gate; drain is not assumed via wall-clock sleep. (b) SIGKILL may lose uncommitted observations. (c) on DB failure/deadline exceeded, loss is allowed and that limit is documented. No assertion like "all data preserved on shutdown".
- **Environment**: process-level test. See the existing `drain_*` test pattern.
- **Evidence**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` + `runtime/g1/` — after confirming an INSERT block via a PG SHARE lock with `pg_stat_activity`: SIGTERM + lock release → queued writes committed (7→9); SIGKILL + lock held → loss (1→1); SIGTERM + released after deadline expiry → loss (1→1).

### H. No-regression

#### QA-H1. Thread lineage diagnostics unchanged

- **Status**: PASS
- **Given**: a request scenario that produces thread usage/lineage diagnostics (the existing `record_thread_usage_from_response` path).
- **When**: run the same scenario before and after the change.
- **Then**: thread-lineage diagnostic output and the related `request_events_v1` fields keep the same meaning as before the change.
- **Environment**: live QA harness.
- **Evidence**: `target/test-evidence/issue-825/observation_failure_isolation.log` — 10/10 pass. Real Lifecycle buffered+streamed → real Tracker diagnostic path (observer bus fixture) verified: creation count 1600→400 with existing equivalence preserved, no other thread leaks, no track loss on DB failure.

#### QA-H2. RequestEvent/rollup no-regression across the transition

- **Status**: PASS
- **Given**: the same request sequence before and after the observation-structure change.
- **When**: query the `request_events_v1` payload cache-analysis fields (`matched_v3_cache_key`, `lookback_distance`, `predicted_*`, `token_estimate_source`, etc.) and the usage rollup.
- **Then**: the request log and rollup keep their schema and meaning across the transition. The observation-storage change does not change this consumer's data source.
- **Environment**: `cargo test -p cc-lb-admin --test integration` + the live QA harness's `wait_for_lookback_event` family assertions.
- **Evidence**: existing contract parity as the baseline (not a byte-identical raw DB dump claim). `target/test-evidence/issue-825/qa825-baseline-rollup.json` — baseline conformance 3/3 pass, the same 3 tests run on both SQLite+PG as current. `target/test-evidence/issue-825/usage-rollup.log` — current head 3/3 pass. The request-event payload cache-field assertions (`matched_v3_cache_key`, `lookback_distance`, `predicted_*`, `token_estimate_source`) of baseline+current live QA hold identically on both. See `runtime/events-{before,after}.json`, `usage-{before,after}.json`.

#### QA-H3. Existing prompt_cache_live_qa scenarios pass

- **Status**: PASS
- **Given**: the entire existing `prompt_cache_live_qa` module (lookback hit, breakpoint isolation, TTL branching, etc.).
- **When**: run the whole module.
- **Then**: the existing scenarios pass under the new structure. If `refresh_debounce_secs = 0` becomes meaningless after debounce removal, the config is cleaned up and the tests must still pass with the same intent.
- **Environment**: `cargo test -p cc-lb-server --test integration prompt_cache_live_qa`.
- **Evidence**: `target/test-evidence/issue-825/qa825-live-stress.json` — live QA 8 cases × 20 runs = 160 pass on latest source (4 test threads, 77.1s). `target/test-evidence/issue-825/qa825-baseline-live.json` — baseline 917a1003 isolated-scratch live QA 8/8 pass. Note: two test preconditions were fixed during the run — (1) a race where the old wait function did not wait for the independent observation writer (fixed to wait for exactly 1 committed row before turn 2), (2) an issue where the 3 negative-invalidation cases could pass vacuously from a cold state (added an exact message-prefix commit barrier before the second request; existing assertions kept). Final fmt + clippy sqlite/postgres also passed (`qa825-static-gates.json`).

#### QA-H4. Admin settings screen browser QA

- **Status**: PASS
- **Given**: the admin settings screen including the removed config fields.
- **When**: open the settings screen in a browser, check the `prompt_cache_shadow` section, and round-trip a config save/load.
- **Then**: from the consumer's view, the removed fields are not visible in the UI, and save/load round-trip of valid config works without error. The browser QA scope is only the actually removed settings UI change and does not include new dashboard verification. Assertions are on consumer-visible UI behavior and the config round-trip, not on implementation details such as the section list in `configEditorModel.test.ts`.
- **Environment**: `bun run --shell=bun build` output + browser (frontend QA procedure), vitest.
- **Evidence**: `target/test-evidence/issue-825/browser/desktop_prompt_cache_shadow_*.png` — removed-field absence and config round-trip confirmed in the browser.

#### QA-H5. Telemetry and request-log retention

- **Status**: PASS
- **Given**: a request that produces an observation.
- **When**: query the metrics endpoint and the request log.
- **Then**: valid observation metrics and request-log fields — write failure, publish, etc. — are preserved or migrated to fit the new structure. Metrics that lost meaning as local-cache-only may be removed, but the removed items and reasons are recorded in docs. Only the debounce, hydration, and observation-NOTIFY paths are removed.
- **Environment**: live QA harness + `/metrics` scrape.
- **Evidence**: metric inventory comparison (diff vs HEAD + runtime scrape). Kept: `cc_lb_cache_observation_dropped_total`, `cc_lb_cache_observation_write_failed_total`, `cc_lb_cache_hit_total`, `cc_lb_cache_miss_total`, `cc_lb_lifecycle_cache_hit_miss_events_total` — `target/test-evidence/issue-825/runtime/observation-metrics.txt`, `metrics-{A,B}-{before,after}.txt`, `fault-metrics-{before,after}.txt` (write_failed{store="postgres"}=2, miss counter measured). Added: `cc_lb_cache_observation_read_failed_total` (new shared-lookup failure path, P2-A). Changed: `dropped`'s reason label — `abort` removed (the publish point moved to `message_start`, eliminating the pre-observation abort drop path; removed together with the subscriber deletion), `channel_closed` added (distinguishes queue-closed drops from queue_full). Removal reasons are recorded in the `crates/cc-lb-observability/RUNBOOK.md` diff. No other observation metric names were deleted — the deleted `prompt_cache_observation_cache.rs` emitted no metrics.

#### QA-H6. Full regression suite

- **Status**: PASS — on the implementation commit `fc7130984aafe89a1f8569b3d6b99b807ef06dec`, rebased preserving `master`'s request-log classification change (#826), Linux `ci`, `web`, and `publish-check` all passed. The local integration regression right after the rebase also passed 77/77. The three workflows also passed on the original implementation commit `8ca710e7`. The release-only artifact verification job is SKIPPED per PR conditions and is not counted as executed verification. The remaining 4 PDK SIGSEGVs and the missing cargo-deny limitation of the local macOS full run are kept separately. A Linux CI pass is not interpreted as a full macOS pass.
- **Given**: the current PR head commit (not a merged HEAD, since there is no merge approval).
- **When**: run all §5 CI commands (fmt, audit-redaction lint, clippy sqlite/postgres, deny, llvm-cov nextest, coverage gate, web build/lint/typecheck/vitest). Additionally, run the deterministic cases (QA-B6/B7/B8, QA-D1/D2 and other store/routing-level cases) 20+ times each on SQLite and Postgres to check for instability. A failed CI job is not passed by re-running without fixing the cause.
- **Then**: all pass. Postgres-dependent tests run under `CI_POSTGRES_URL`. An intermittent failure in repeated runs is recorded as FAIL, not dismissed as a flake.
- **Environment**: same conditions as CI.
- **Evidence**: [CI: fmt·deny·both clippy·nextest/coverage·E2E](https://github.com/isac322/cc-lb/actions/runs/35529947107), [web](https://github.com/isac322/cc-lb/actions/runs/35529947161), [publish-check](https://github.com/isac322/cc-lb/actions/runs/35529947146). Local evidence under `target/test-evidence/issue-825/`: SQLite/PostgreSQL store regression 22 tests × 20 runs, live proxy 8 × 20 runs, isolated-PG re-verification 4/4, trybuild pass logs. The PDK macOS IPS `EXC_BAD_ACCESS` evidence is `pdk-crash.log`; the timed-out lldb run is not success evidence.

#### QA-H7. Admin routing preview no-regression (async transition)

- **Status**: PASS
- **Given**: `Lifecycle::preview_route` transitioned to async for the shared-store lookup, with the admin `RoutePreviewPort`/handler/server-adapter callers migrated together. Prepare four store states: committed warm observation, cold (no observation), expired observation, and read failure.
- **When**: call the admin routing preview API in each state.
- **Then**: the preview reflects the same scoring semantics as before and the current routing constraints (health/quota/rate-limit) — a warm observation is reflected in the preview score, cold/expired are not reflected, and a read failure is fail-open neutral score + OTel error report with the preview not failing with 503. The preview call does not actually call upstreams. If the async transition silently removes the preview's cache-score feature, that is a fail.
- **Environment**: `cargo test -p cc-lb-admin --test integration` + failure-injection store.
- **Evidence**: `target/test-evidence/issue-825/runtime/preview-h7-{warm,cold,expired,unknown}.json` + `preview-{warm,known-cold,recovered,read-failure,inspect}.json` — warm reflects the warm upstream winner and `matched_v3_cache_key`, cold/expired are not reflected, read failure is fail-open neutral + OTel error. The preview does not call upstreams.

## 8. Mapping the #825 invariant to the new structure

The original #825 observation invariant was "a locally refreshed expiry must not regress due to a stale DB reload". Since the new structure has no hydration and no per-pod long-lived cache, the invariant maps as follows.

| Old invariant (hydration structure) | New-structure counterpart invariant | Verification rows |
|---|---|---|
| A stale DB reload does not regress a locally refreshed expiry | An out-of-order write for the same key does not regress the committed expiry | QA-B7, QA-B8 |
| Judged warm at the T+280 lookup | A committed observation is returned and reflected in routing at a pre-expiry lookup | QA-D1, QA-C1 |
| New remote records reflected normally | Observations committed by another process are reflected via lookup without NOTIFY | QA-C1 |
| Real expiry works normally | Absolute expiry timestamp basis; not extended by restart | QA-D1, QA-D3 |
| upstream/model/prefix/TTL isolation | Same | QA-D2 |

## 9. Unverified preconditions

Items that need confirmation before execution. Do not declare unobserved blockers; adjust rows based on confirmation results.

| # | Precondition | How to confirm | Affected rows |
|---|---|---|---|
| U1 | Whether a test fixture expressing the compat SSE path (dialect with `sse_event_transform_hook`) exists | Inspect fixture/test code | QA-B2 |
| U2 | An injection point for a gated store that can deterministically hold/release writes | Confirm the sink/store boundary during implementation | QA-B3, QA-B9, QA-F3 |
| U3 | The shape of a hook that exhausts the non-blocking sink queue in tests | Confirm the sink boundary during implementation | QA-B9, QA-F3 |
| U4 | `TestClock` cannot be injected into a process-level server — decide the component-test location | Confirm during implementation | QA-E1, QA-C3 |
| U5 | Measured basis for representative cardinality (candidate-key count, table row count) | Investigate production metrics/logs or agree on a reasonable estimate | QA-C6 |
| U6 | Whether the #825-shaped defect on Postgres (out-of-order upsert regression) is reproduced beforehand | Run QA-B7 against the pre-change Postgres adapter first | QA-B7 baseline |

## 10. Evidence record format

After running each row, record the following in the evidence column.

```text
Run at (UTC):
Runner/environment: (local|CI, OS, Postgres version)
Command: (the exact command from §5)
Result: PASS|FAIL|BLOCKED
Artifacts: (log/dump/screenshot paths)
Notes: (observed exceptions, contract-interpretation memos)
```

Evidence is stored under `target/test-evidence/` in per-test directories with credentials masked.
