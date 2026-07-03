# RFC-0002 Fix — Test Case Catalog

Consensus of 3 independent test-design agents (behavior verification, regression, live QA). Every H/M-checklist item has ≥ 1 test. Live QA covers the operator-visible path.

## Static / compile-time gates

- **TC-STATIC-1** · No compile references to `LifecycleContext::update_usage`, `set_prebuilt_event`, `attach_cache_metadata`. `rg` returns 0 code hits under `crates/`.
- **TC-STATIC-2** · No `RequestEvent {` literals in handler success paths (`crates/cc-lb-core/src/lifecycle.rs`).
- **TC-STATIC-3** · `TerminalState` struct field list excludes `usage`, `prebuilt_event`, `cache_state`, `cache_control_block_count`, `cache_breakpoints`, `cache_prefix_hash`.
- **TC-STATIC-4** · Default config values: `lifecycle_limit_reconcile_subscriber.enabled = true`; TTL sweeper spawns regardless of config gate.
- **TC-STATIC-5** · `terminal_observer` module renamed to `lifecycle_context`; grep returns 0 production hits (docs/tests may keep the term).

## Unit tests

- **TC-U-M2-1** · Assembler receives only `RequestTerminated` (no partial) → writes minimal assembler row with `error_code=terminal_without_partial`, `request_id=req_unknown_shadow`, all usage/cache fields NULL.
- **TC-U-M2-2** · Partial with no usage + `RequestTerminated(Success)` → assembler row with status=200, no usage.
- **TC-U-M1-1** · `LifecycleEvent::UpstreamResponseStarted` carries `HeaderSnapshot` with sanitized subset (content-type, x-request-id, rate-limit headers). No `authorization` header included.
- **TC-U-H5-1** · Fresh config parse: `lifecycle_limit_reconcile_subscriber.{enabled}` = `(true)`.
- **TC-U-H5-2** · `LimitReconcileSubscriber` receives `LimitDecision::Reserved { reservation_id: "" }` + terminate → no panic; increments `empty_reservation_id` outcome counter.
- **TC-U-M4-1** · `RequestEventWriterSource` semantics: given all 9 permutations of `(source ∈ {Legacy, Both, Shadow}) × (old_flag ∈ {true, false, unset})`, only `source` decides.

## Integration tests

- **TC-INT-H1-1** · Boot server with SQLite + writer enabled; hit `/api/oauth/usage`, `/admin/health`, unknown fallback, method-not-allowed. Assert `SELECT COUNT(*) FROM request_events_v1` unchanged.
- **TC-INT-H1-2** · Proxy timeout still writes row with `error_code=tower_timeout`; middleware still active on lifecycle routes.
- **TC-INT-H2-1** · Removed — legacy row filtering became obsolete when PR #276 deleted the inline handler writer; all returned rows are assembler rows.
- **TC-INT-H2-2** · Removed — legacy row filtering became obsolete when PR #276 deleted the inline handler writer; all returned rows are assembler rows.
- **TC-INT-H3-1** · Removed — legacy-vs-assembler parity is no longer meaningful after PR #276 deleted the legacy writer, so no correlation join is needed.
- **TC-INT-H3-2** · Two sequential requests: cache-bearing then cache-free. Assert second assembler row has NULL cache fields (no cross-request leakage via event_id keying).
- **TC-INT-H4-1** · Fake LimitEngine counts `reconcile` vs `reconcile_by_id` calls. One `/v1/messages` success → legacy `reconcile` count = 0, `reconcile_by_id` count = 1.
- **TC-INT-H4-2** · Streaming request → same assertion after full stream drain.
- **TC-INT-H4-3** · Race check: reservation state transitions to reconciled exactly once. No double-refund.
- **TC-INT-H6-1** · Server boots without `limit_reservation_ttl` config. TTL sweeper still runs; metric `cc_lb_limit_reservation_ttl_evicted_total` is registered.
- **TC-INT-H6-2** · Config `limit_reservation_ttl.ttl_secs=1, tick_secs=1`. Reserve without terminate; wait 2s. Assert eviction metric increments, next reservation succeeds.
- **TC-INT-H9-1** · Two sequential requests on same process. First succeeds with rich usage/cache. Second fails (parse error). Assert second row's usage/cache/cost fields are NULL — no leak from prebuilt state.
- **TC-INT-M3-1** · Dynamic hook reload. First request fires hook A, reload replaces with hook B, second request fires hook B (not A). Adapter must not snapshot hooks at startup.

## Live-QA (real server + real DB + real /metrics)

Each LIVE-QA uses distinct ports and separate `/tmp/cc-lb-liveqa-N/` dir; harness reused across scenarios.

- **LIVE-1** · Default boot: SQLite, no `request_event_writer_source` override.
  - Trigger: single `POST /v1/messages` happy path.
  - Verify: `SELECT COUNT(*) FROM request_events_v1` = 1; status=200; `error_code IS NULL`.
  - Metrics: `cc_lb_limit_reservation_ttl_evicted_total` and `cc_lb_limit_reconcile_subscriber_rows_total{outcome="reconciled"}` both present.
  - Covers: QA1, QA2, H5, H6.

- **LIVE-2** · Same boot; hit `GET /api/oauth/usage`.
  - Verify: total row count unchanged; `error_code='terminal_dropped'` count = 0.
  - Covers: QA5, H1.

- **LIVE-3** · Same boot; hit `GET /nope` (404) and `PUT /v1/messages` (405).
  - Verify: row count unchanged; no lifecycle metrics fire for these paths.
  - Covers: H1.

- **LIVE-4** · Same boot; upstream forced 500.
  - Verify: 1 row with status=500, `error_code='upstream_5xx'`.
  - Covers: QA3.

- **LIVE-5** · Same boot; streaming request.
  - Verify: row has `sse_event_count > 0`, stream timing fields non-null, cache fields populated when applicable.
  - Covers: QA4.

- **LIVE-6** · Cache-bearing request.
  - Verify: 1 assembler row; cache fields are populated.
  - Covers: QA6, H3.

- **LIVE-7** · Admin `/admin/v1/events/recent?limit=10`.
  - Verify: raw SQL count matches API `count`.
  - Covers: QA7.

- **LIVE-9** · Default subscriber auth; principal with tight limit.
  - Verify: response row has `limit_reconcile_ms=0`; `cc_lb_limit_reconcile_subscriber_rows_total{outcome="reconciled"}` increments.
  - Covers: QA9, H4, H5.

- **LIVE-10** · `limit_reservation_ttl.{ttl_secs=1, tick_secs=1}`; trigger reservation but timeout the request; wait 3s; retry.
  - Verify: `cc_lb_limit_reservation_ttl_evicted_total >= 1`; retry returns 200 (capacity refunded).
  - Covers: QA8, H6.

- **LIVE-11** · Complete N requests; SIGTERM.
  - Verify: no assembler row missing; no null `event_id`; log has no panic.
  - Covers: QA10.

- **LIVE-12** · Happy path.
  - Verify: 1 row, status=200.
  - Also runs `rg` pre-check for deleted symbols (STATIC-1 + STATIC-2 + STATIC-3 + STATIC-5).
  - Covers: H7, H8, H9.

## Priority breakdown

- **P0 blocker** (must pass before PR merge): all STATIC-*, all TC-INT-H*, all LIVE-1..LIVE-12.
- **P1 must-have**: TC-U-M1-1, TC-U-M2-*, TC-INT-M3-1, LIVE-8 detail.
- **P2 nice**: L1 (config namespace), L2 (module rename).
