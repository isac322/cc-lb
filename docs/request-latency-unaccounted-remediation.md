# Request latency unaccounted — root-cause remediation design

- Status: implementation complete; isolated proxy/storage/API/metrics/browser QA complete (external OTLP collector export excluded)
- Written: 2026-09-10
- Scope: proxy request lifecycle, request event/storage/admin API, Logs latency UI, Prometheus/OTLP
- Related docs: `docs/request-setup-timing-breakdown.md`
- Execution QA: Cases F–K in `.agents/skills/user-flow-qa/references/scenarios/request-log-observability.md`
- Completion verified: 2026-09-10

## 1. Purpose and non-goals

This document explains the problem where `Unaccounted` in the production request timeline hides the real cause of latency, and defines the instrumentation, storage, API, UI, and QA contract that an implementer applies as-is.

The goals of this change are:

1. Record client request body receive time as an independent stage.
2. Reduce the residual between `duration_ms` and the stage sum to **10ms or less** on normal proxy requests.
3. Preserve partial stream time up to the cancellation point even on 499 requests where the client aborts the stream.
4. Separate `source_kind="renewal"` from the proxy timeline so the entire renewal duration does not appear as `Unaccounted`.
5. Preserve compatibility for existing rows, mixed-version deployments, SQLite/PostgreSQL, and the recent/delta/detail APIs.

This document does not propose performance optimization itself or a re-decomposition of the already-implemented 8 setup stages. The ~0.24–1.23s latency observed in production is mostly client body upload/read time; the current problem is the instrumentation gap that fails to record that time. Request/response bytes, routing, signing, plugin execution, retry, timeout, and status/body/header behavior are unchanged.

## 2. Conclusion

The overall `duration_ms` clock of `LifecycleContext` starts in `lifecycle_middleware` in `crates/cc-lb-server/src/app.rs`. Then `lifecycle_handler` awaits `read_request_body` and calls `Lifecycle::handle` only after the body is fully collected.

By contrast, the `proxy_setup_ms` clock starts after `Lifecycle::handle` is entered. Therefore the following interval is included in `duration_ms` but in no request event stage:

```text
LifecycleContext::new
  → middleware/handler entry
  → read_request_body await
  → Lifecycle::handle entry
```

Cross-referencing the post-header, pre-upstream-dispatch time of large normal requests by request ID shows that this gap containing `read_request_body` explains **99.75~99.98%** of the recorded `Unaccounted`. The normal stream itself is recorded as `stream_total_ms == upstream_body_ms`, so a completed stream relay is not missing.

Two separate problems were also confirmed:

- 499 client cancellation records only status/error in `DownstreamStreamDropGuard::drop` and does not store the elapsed time since relay start as terminal timing.
- The renewal publisher records only `duration_ms` and publishes all proxy/setup/body timings as `None`. Applying the proxy formula makes the entire renewal `Unaccounted`.

## 3. Production investigation method

### 3.1 Data cross-referencing procedure

The investigation proceeded in this order:

1. Queried `source_kind`, status, `duration_ms`, `proxy_setup_ms`, `shape_ms`, `sign_ms`, `upstream_ttfb_ms`, `upstream_body_ms`, `stream_total_ms`, `limit_reconcile_ms` per request ID from production request events.
2. Separated normal proxy, 499, and renewal. Different source/termination models were not mixed into one distribution.
3. Computed the stage sum and `Unaccounted` the same way the current admin-web does.
4. Cross-referenced the lifecycle start timestamp, `proxy.read_request_body`, `Lifecycle::handle`, and signed-request dispatch timestamps for the same request ID in traces/logs.
5. Derived the instrumentation gap by excluding already-recorded overlapping stages such as `proxy_setup_ms` and `shape_ms` from the post-header, pre-dispatch wall time.
6. Compared original request body size with the direction of the gap. For representative large requests, ingress request and outbound request bytes were also cross-checked to confirm the difference was not caused by body transformation.
7. Checked Thanos pod CPU/memory/throttling, connection reuse, DNS/connect, affinity resolve, and request/response plugin timing within the same event window.

The current UI's proxy stage sum effectively adds each of the following parent durations once, even though the detail display subdivides them:

```text
current_accounted =
    proxy_setup_ms
  + shape_ms
  + sign_ms
  + upstream_ttfb_ms
  + upstream_body_ms
  + limit_reconcile_ms

current_unaccounted = max(0, duration_ms - current_accounted)
```

`bulkhead_wait_ms`, `dns_ms`, `connect_ms`, and the auth/route/limit/setup detail stages are child breakdowns of `upstream_ttfb_ms` or `proxy_setup_ms` respectively. Parent and child are never summed together.

### 3.2 Normal proxy distribution

The `Unaccounted` distribution of production normal requests fell in the following ranges depending on aggregation window and event/trace computation boundary differences:

| Percentile | `Unaccounted` |
| --- | ---: |
| p50 | ~243~257ms |
| p90 | ~624~630ms |
| p99 | ~1.03~1.08s |
| max | 1.225s |

This is not a rounding-level residual. It is hundreds of ms from the median, and the tail exceeds 1 second.

### 3.3 Representative request ID cross-reference

| Request ID | Lifecycle start | Dispatch | Header→dispatch | Gap after excluding recorded prior stages | Recorded `Unaccounted` | Explained ratio | Original body |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `req_server_711` | 07:29:56.849Z | 07:29:57.999188Z | 1,150.188ms | 1,003.188ms (excluding `proxy_setup_ms=138`, `shape_ms=9`) | 1,005ms | 99.820% | 854,336B |
| `req_server_1112` | 07:54:24.595Z | 07:54:25.720519Z | 1,125.519ms | 993.519ms (excluding `proxy_setup_ms=124`, `shape_ms=8`) | 996ms | 99.751% | 917,567B |
| `req_server_860` | cross-referenced the same way | cross-referenced the same way | - | 1,156.763ms | 1,157ms | 99.980% | large request |

For `req_server_711` and `req_server_1112`, ingress request body and outbound body matched at 854,336B and 917,567B respectively. The larger the body, the longer the client→server upload/read interval, and that interval was missing from existing stages. After implementation, `request_body_bytes` must be stored so this correlation can be verified directly across the whole distribution.

A counterexample for completed normal streams was also confirmed:

- `req_server_960`: `stream_total_ms=22,873`, `upstream_body_ms=22,873`

In other words, normal stream relay does reach the terminal event. The UI must count the two fields as one stage only once, and the cause of the current large `Unaccounted` must not be attributed to missing stream relay.

## 4. Ruled-out causes

The following items do not deny the possibility of separate performance problems; they mean these are not the main cause of this several-hundred-ms~1.2s `Unaccounted`.

### 4.1 CPU and memory pressure

- In Thanos, cc-lb pod CPU peaked at about 0.081 core against a 2-core limit.
- CPU throttling was 0%.
- Memory peaked at about 163MB against a 1GiB limit.
- Therefore CPU saturation, throttling, and memory-limit approach do not explain the ~1s gap in representative requests.
- More directly, the gap exists before `Lifecycle::handle` entry and grew with body bytes.

### 4.2 DNS and connect

- Representative requests took the connection-reuse path.
- DNS/connect is a child stage of `upstream_ttfb_ms`, after the signed request is dispatched upstream.
- The investigated gap is after header receipt and **before dispatch**, so DNS/connect cannot be the cause in execution order.

### 4.3 Routing/affinity

- Cumulative affinity resolve was 0.102s over 44 calls, about 2.3ms per request.
- Routing and affinity are included inside `proxy_setup_ms`.
- `proxy_setup_ms` itself was 124~138ms for representative requests, while the separate gap was ~994~1,157ms. Routing cost already included in the parent cannot explain this gap again.

### 4.4 Plugin and prompt-cache analysis

- Response plugin `transform_response` was about 0.35ms.
- Request shape/sign are recorded in `shape_ms` and `sign_ms` respectively.
- Prompt-cache parse/queue/structure/serialize/token-key/count/tokenize and signer preparation are existing detail stages inside `proxy_setup_ms`.
- The body-read gap in representative requests occurred before those stages started.

Therefore adding plugin or tokenizer time again is double counting, not a root fix.

## 5. Current code instrumentation boundaries

### 5.1 Overall duration

`crates/cc-lb-server/src/app.rs::lifecycle_middleware` calls `LifecycleContext::new`. `crates/cc-lb-engine/src/terminal_observer.rs` stores `Instant::now()` at that point and records `started.elapsed()` as `duration_ms` when `RequestTerminated` is published.

### 5.2 Missing request body read

`crates/cc-lb-server/src/app.rs::lifecycle_handler` executes in this order:

1. Split request parts/body
2. `read_request_body` await
3. Rebuild `Request<Bytes>`
4. Call `Lifecycle::handle`

The `proxy.read_request_body` tracing span already exists but is not propagated to a request event field. Body size also exists in `ParseInfo.body_bytes` but is not preserved as a request-body-specific field on the final `RequestEvent`. The existing `RequestEvent.body_bytes` is response/stream body bytes and must not be reused.

### 5.3 `proxy_setup_ms`

The local `started` in `crates/cc-lb-engine/src/lifecycle.rs::Lifecycle::handle` starts at handle entry and records up to `dispatch_started`, just before the first upstream attempt, as `proxy_setup_ms`. Therefore server body read is structurally excluded.

### 5.4 Existing setup detail stages

The contract of the existing implementation and `docs/request-setup-timing-breakdown.md` is preserved.

| Field | Current boundary |
| --- | --- |
| `json_parse_ms` | `sonic_rs::from_slice` inside `RequestBodyView::new` |
| `cache_tokenizer_queue_ms` | prompt-cache executor semaphore wait |
| `cache_structure_ms` | block flatten, breakpoint, structural prefix hash/lookback |
| `cache_serialize_ms` | exact token-prefix bytes and breakpoint offset materialization |
| `cache_token_key_ms` | credential-scoped exact token-prefix BLAKE3 key |
| `cache_count_lookup_ms` | distributed count-cache claim/hit/miss/coalesced wait/retrieval |
| `cache_tokenize_ms` | leader/fallback exact BPE tokenization |
| `prepare_signer_ms` | signer factory build, lookup/decrypt/lazy OAuth refresh |

These eight fields are children of `proxy_setup_ms`. The new `request_body_read_ms` is a preceding sibling outside `proxy_setup_ms` and does not overlap `json_parse_ms`.

### 5.5 Normal response body

- The non-stream path stores post-response-headers body collection as `upstream_body_ms`.
- The stream path computes `stream_total_ms` from `relay_start` to completion and also puts the same value into terminal timing's `upstream_body_ms`.
- Therefore the two values can be equal on normal streams. Include only once in the timeline sum.

### 5.6 499 Drop path

`DownstreamStreamDropGuard::drop` sets status 499 and `client_closed_request` and records the termination outcome metric when an incomplete stream is dropped. However, it does not own `relay_start` and does not call `set_termination_timings`, so stream time already elapsed before cancellation is not preserved. The subsequent terminal `duration_ms` includes that time, leaving the entire partial relay as `Unaccounted`.

### 5.7 Renewal path

`crates/cc-lb-server/src/scheduler_dispatch/cache_keepalive/lifecycle.rs::publish_renewal_lifecycle` publishes `source_kind="renewal"` and `duration_ms` but leaves all of the following fields `None`:

- `bulkhead_wait_ms`, `dns_ms`, `connect_ms`, `shape_ms`, `sign_ms`, `upstream_ttfb_ms`
- `stream_total_ms`, `upstream_body_ms`, `first_body_chunk_ms`
- `proxy_setup_ms`, setup detail stages

Renewal is a synthetic source that does not go through client ingress/body read or the normal proxy pipeline. Do not force-fill the proxy stage formula; separate it into a per-source-kind model.

## 6. Target timeline and invariants

### 6.1 Normal proxy target timeline

```text
Lifecycle duration
├─ Request body read       request_body_read_ms
├─ Proxy setup             proxy_setup_ms
│  ├─ JSON parse           json_parse_ms
│  ├─ Cache queue/analysis cache_*_ms
│  ├─ Auth                 auth_ms
│  ├─ Route                route_ms
│  ├─ Limit reserve        limit_reserve_ms
│  ├─ Prepare signer       prepare_signer_ms
│  └─ Other setup          derived residual inside proxy_setup_ms
├─ Shape                   shape_ms
├─ Sign                    sign_ms
├─ Upstream to headers     upstream_ttfb_ms
│  ├─ Bulkhead wait        bulkhead_wait_ms
│  ├─ DNS                  dns_ms
│  ├─ Connect              connect_ms
│  └─ Provider wait        derived residual inside upstream_ttfb_ms
├─ Response body           one of stream_total_ms / upstream_body_ms
└─ Finalize                finalize_ms
```

Parent and child may appear together in the detail display but are included only once in the sum.

The new formula for normal proxy is:

```text
body_phase_ms =
  if stream_total_ms is present: stream_total_ms
  else: upstream_body_ms ?? 0

accounted_ms =
    request_body_read_ms
  + proxy_setup_ms
  + shape_ms
  + sign_ms
  + upstream_ttfb_ms
  + body_phase_ms
  + finalize_ms

unaccounted_ms = max(0, duration_ms - accounted_ms)
```

The acceptance budget for a normal proxy final row with the new instrumentation present is:

```text
unaccounted_ms <= 10ms
```

Rounding of each `u64` millisecond field can produce floor/round error per stage. The 10ms budget exists to absorb that error and middleware scheduling gaps; it is not a blanket allowance for new tens-to-hundreds-of-ms residuals.

### 6.2 499 target timeline

```text
Lifecycle duration
├─ Request body read       request_body_read_ms
├─ Proxy setup/dispatch    existing proxy stages
├─ Upstream to headers     upstream_ttfb_ms
├─ Partial response body   upstream_body_ms
└─ Finalize/drop           finalize_ms
```

For 499, preserve in the existing `upstream_body_ms` the partial elapsed from `relay_start` to the monotonic instant the guard observed downstream cancellation. Do not falsely record `stream_total_ms` for an incomplete stream. The UI uses `stream_total_ms` as the body stage for a complete stream and `upstream_body_ms` otherwise — once only.

### 6.3 Renewal target timeline

Do not apply the proxy formula to `source_kind="renewal"`. In this change, display the already-existing `duration_ms` as a single `Renewal cycle` stage.

```text
Renewal cycle = duration_ms
Renewal unaccounted = 0
```

Do not guess new renewal dispatch/connect/body detail stages or synthesize them into proxy fields. Separate source-specific stages can be added later when the renewal dispatcher provides real monotonic boundaries, but that is out of scope for this remediation.

## 7. New field contract

The following is the **fixed contract** that every implementation segment must uphold together:

| Field | Rust/JSON type | Source | Exact meaning | Summation rule |
| --- | --- | --- | --- | --- |
| `request_body_read_ms` | `Option<u64>` / nullable optional number | server `lifecycle_handler` | Monotonic elapsed from just before the `read_request_body` call until success or a read/limit error return | Independent parent stage before `proxy_setup_ms` |
| `request_body_bytes` | `Option<u64>` / nullable optional number | server body collector | Number of original ingress body bytes successfully collected and passed to `Lifecycle::handle` | Not used in the time sum |
| `finalize_ms` | `Option<u64>` / nullable optional number | engine terminal path | Mandatory finalization elapsed from after response body completion or cancellation observation until just before `RequestTerminated` is published | Parent stage that includes limit/accounting reconcile |

Additional rules:

- All timings are measured on a monotonic clock and never produce negative/NaN/Infinity.
- `0` is a measured value. Distinguish it from missing/null and preserve it through transport/storage/UI.
- `request_body_bytes` is not copied by trusting the `Content-Length` header. Success rows record the actually collected `Bytes::len()`.
- `request_body_bytes` is separate from the existing response `body_bytes`.
- When body-too-large is rejected immediately on `Content-Length` alone, the actual received body size is unknown, so `request_body_bytes=None` is allowed. When the cap is exceeded mid-stream-read, record the value only if the implementation can safely return the actually observed bytes.
- Normal completed streams preserve `stream_total_ms` as before. For 499 client cancellation, the rule is `stream_total_ms=None`, `upstream_body_ms=Some(partial_elapsed)`.

## 8. Lifecycle/event change contract

### 8.1 Server ingress

Implement the following in `crates/cc-lb-server/src/app.rs`:

1. Take a monotonic start just before `read_request_body`.
2. On success, store elapsed and the actual `body.len()` on `LifecycleContext` immediately.
3. On too-large/read error, store elapsed first as well.
4. Error paths that have a context use explicit terminal status/error and `finish()` rather than relying on the Drop fallback.
5. Record `request_body_read_ms` and, on success, `http.request.body.size` on the existing `proxy.read_request_body` span.
6. Do not clone body bytes or serialize/parse them an extra time.

### 8.2 Terminal state and lifecycle event

Implement the following in `crates/cc-lb-engine/src/terminal_observer.rs` and `crates/cc-lb-lifecycle/src/event.rs`:

- Add three optional fields to `TerminalState` and `LifecycleEvent::RequestTerminated`.
- Request body timing must be storable via a setter before `Lifecycle::handle` is called.
- Take a monotonic finalize start right after the upstream body frame loop ends for non-stream, and right after the last downstream body frame is confirmed for stream.
- Store `finalize_ms` just before calling `finish()`, after limit/accounting reconcile and terminal status/error finalization are done. Therefore limit/accounting reconcile time is included inside `finalize_ms`.
- Drop, timeout, early rejection, and normal stream/non-stream must not overwrite already-completed stages. Paths that need to re-invoke a setter must not turn `Some` back into `None`.

### 8.3 499 Drop

`DownstreamStreamDropGuard` owns `relay_start` or an equivalent start instant.

1. Normal `finish()` keeps the existing complete timing.
2. On incomplete Drop, take the cancellation instant once.
3. Store the partial elapsed relative to `relay_start` into the existing `upstream_body_ms` missing slot first. Do not overwrite an existing value.
4. Then set 499 and `client_closed_request`.
5. Record the existing stream termination span/metric.
6. Store the elapsed time after cancellation observation as `finalize_ms` and explicitly close the terminal event with `observer.finish()`.
7. Do not set `stream_total_ms` for incomplete streams.
8. Preserve the existing meaning of `cc_lb_stream_terminations_total{outcome="client_cancelled"}`.
9. Do not drain upstream/body further after cancellation to inflate elapsed.

### 8.4 Assembler

In `crates/cc-lb-engine/src/lifecycle_event_assembler.rs`, store the three new fields on `Partial` and copy them identically to the following outputs. Preserve 499 partial timing through the existing `upstream_body_ms` path:

- terminal live partial
- final `RequestEvent`
- timings already received in the orphan/finalization fallback

Values must not be lost under event orderings where `RequestTerminated` arrives before/after `StreamCompleted`. Do not overwrite a 499's partial `upstream_body_ms` with a later missing value.

### 8.5 Renewal publisher

Do not fake-fill proxy timings into the renewal lifecycle payload. `source_kind="renewal"` and `duration_ms` are the basis for source-specific UI selection. The three new fields are `None` by default on renewal rows.

## 9. RequestEvent, storage, API change contract

### 9.1 Rust DTO

Add the same optional fields to the following structures:

- `cc_lb_request_log::RequestEvent`
- assembler live `RequestEventPartial` family
- `cc_lb_storage_api::RequestEventListItem`
- admin DTOs used by recent/delta/final/detail responses

The serde contract is `#[serde(default, skip_serializing_if = "Option::is_none")]`. Detail payload, recent list, final update, and terminal live partial must return the same values.

### 9.2 SQLite

The SQLite list hot path reads materialized `list_*` columns without raw JSON parse. Add the following nullable columns in `0080_request_event_lifecycle_timing_list_fields.sql`:

```text
list_request_body_read_ms INTEGER NULL
list_request_body_bytes   INTEGER NULL
list_finalize_ms           INTEGER NULL
```

Update the append INSERT/bind, `request_event_list_sql`, `ListRow`, `list_row_to_item`, and conformance fixtures together. Do not scan the whole table to backfill existing rows.

### 9.3 PostgreSQL

PostgreSQL decodes a bounded page's payload once as `ListPayload`. Add the three new fields to the `ListPayload` and `RequestEventListItem` mapping. Preserve the existing JSONB payload storage and detail byte contract; do not add separate materialized columns/indexes just for this remediation.

### 9.4 Admin API and web schema

Update all of the following in `crates/cc-lb-admin/web/src/lib/api.ts`:

- `RequestEventPartialSchema`
- final event loose schema/transform
- `RequestEvent` TypeScript interface
- fixtures and schema tests

Validate timing/bytes fields as finite, non-negative, nullable, optional numbers. Distinguish missing, null, and 0. API field names must exactly match Rust/JSON; do not create aliases.

## 10. UI change contract

### 10.1 Source-kind branching

Check `source_kind` first:

- `proxy` or missing legacy proxy: use the proxy timeline
- `renewal`: use the single source-specific timeline `Renewal cycle = duration_ms`

Do not show `Unaccounted 100%` on renewal rows. Also do not show proxy-only `Internal pre`, DNS/connect, or stream relay explanations on renewal.

### 10.2 Proxy stage computation

Apply the following rules in `computeStageGroups.ts` and `LatencyTimeline.tsx`:

1. Add `Request body read` as the first stage of `internal_pre`.
2. Display `request_body_bytes` alongside the stage detail as human-readable bytes.
3. Keep the existing `proxy_setup_ms` child stage and parent summation rules.
4. For a complete stream, use `stream_total_ms` as the body parent and do not add the equal `upstream_body_ms` again.
5. For a 499 incomplete stream, display partial `upstream_body_ms` as the `Partial stream (client cancelled)` stage.
6. `Finalize` uses the `finalize_ms` parent in the sum.
7. On normal proxy with all new instrumentation present, display the residual as `Unaccounted` and make it visually identifiable when it exceeds the 10ms budget.
8. Clamp negative residuals with `max(0, ...)`, but do not hide over-accounting from tests. Development computation helpers must also expose the raw signed residual or `accounted_ms` for verification.

### 10.3 Legacy fallback

Existing proxy rows without the three new fields keep the current timeline computation. Mixed rows with one or more new timings sum only the present fields and treat missing as 0.

Existing renewal rows can use the new renewal view with `source_kind="renewal"` alone, so no backfill is needed. Very old rows without even `source_kind` keep the proxy legacy fallback.

### 10.4 Descriptions

UI descriptions must state the boundary precisely:

- Request body read: time for the server to fully receive the client request body; excludes JSON parse
- Partial stream: partial response body time delivered from after response headers until client cancellation is observed
- Finalize: mandatory accounting/finalization after body completion/cancellation and before the terminal event is published
- Renewal cycle: total time of the cache-keepalive renewal performed by the scheduler; not a proxy request breakdown

## 11. Per-termination-path invariants

| Path | Required invariant |
| --- | --- |
| Normal non-stream 2xx | `request_body_read_ms`, `request_body_bytes`, `proxy_setup_ms`, `upstream_ttfb_ms`, `upstream_body_ms`, `finalize_ms` preserved; residual ≤10ms |
| Normal stream 2xx | `stream_total_ms` preserved; body summed once even if `upstream_body_ms` is equal; residual ≤10ms |
| Upstream 4xx/5xx complete body | Actually completed body stage and finalize preserved; status/error semantics unchanged |
| Client cancellation 499 | `upstream_body_ms` preserves relay start→cancel partial elapsed; `stream_total_ms` is not made to look complete; status 499 and `client_closed_request` preserved |
| Body too large | Measured `request_body_read_ms` preserved; `request_body_bytes=None` if actual bytes unknown; 413 semantics unchanged |
| Body read error | Read elapsed preserved; terminates with explicit terminal error; does not become unrelated `terminal_dropped` due to the Drop fallback |
| Auth/route/limit/signer early error | Body read and completed setup stages preserved; upstream/body stages that never started stay missing |
| Tower timeout | Fields completed up to the timeout preserved; nonexistent stages are not synthesized |
| Renewal success/error | `source_kind="renewal"`; proxy formula not used; a single renewal duration explains the whole timeline |
| Legacy row | Missing new fields allowed; existing computation/rendering unbroken |

All paths uphold the following global conditions:

```text
0 <= each timing
accounted parent stages do not overlap
UI never sums a parent and its child twice
request_body_bytes != response body_bytes
```

## 12. Migration and backward compatibility

1. All three new fields are optional and default to `None` for historical payloads.
2. The SQLite migration adds only nullable columns and does not run a historical backfill scan.
3. PostgreSQL historical JSONB is not modified.
4. The new server must be able to read old rows.
5. The new admin-web must render old/mixed/new payloads.
6. Distinguish missing from measured zero.
7. During rollout, display without panic/schema rejection even if some new rows partially lack fields.
8. Do not change the meaning of existing `body_bytes`, `stream_total_ms`, `upstream_body_ms`, or setup detail fields.
9. Do not add aliases, deprecated duplicate fields, or historical value synthesis.
10. Schema/DTO changes cut over at once in the order lifecycle → assembler → request log → storage API/backend → admin API/web.

## 13. Prometheus and OTLP contract

### 13.1 Prometheus

Do not use request ID, principal, model, or raw body-size values as labels. Use only the fixed vocabulary.

Proposed metrics:

```text
cc_lb_request_stage_duration_seconds{source_kind,stage,outcome}
cc_lb_request_body_size_bytes{source_kind,outcome}
cc_lb_request_unaccounted_duration_seconds{source_kind,outcome}
cc_lb_request_unaccounted_over_budget_total{source_kind,outcome}
```

Fixed label values:

- `source_kind`: `proxy|renewal|unknown`
- `stage`: `request_body_read|proxy_setup|shape|sign|upstream_ttfb|response_body|finalize|renewal_cycle`
- `outcome`: `success|client_cancelled|error|timeout`

Rules:

- `request_stage_duration_seconds` observes only parent stages actually present on the event.
- `response_body` observes only the single value selected among complete stream/non-stream or 499 partial, distinguished by `outcome`.
- Renewal observes only `renewal_cycle` and does not emit proxy stages as 0.
- `unaccounted_over_budget_total` increments when residual >10ms on normal proxy with complete new instrumentation. Legacy/mixed rows are excluded from the budget alert denominator.
- Body-size histogram buckets must distinguish the production 854KB/918KB cases from the body cap. Do not add unbounded labels.

### 13.2 OTLP/tracing

Actual span names and boundaries:

- `proxy.read_request_body`: wraps request body collect and records `http.request.body.size`, `cc_lb.request_body_read_ms`, `outcome`.
- `proxy.handle`: starts at `Lifecycle::handle` entry. Because `LifecycleContext` keeps a clone of this span in terminal state, the span survives until `RequestTerminated` is published even when a streaming response is returned first. Just before terminal, records `cc_lb.request.finalize_ms` and `cc_lb.request.unaccounted_ms` with the same parent timing values as the event.
- `proxy.finish_response`: wraps buffered response body collect and mandatory finalization; records `cc_lb.response_body_ms` and `cc_lb.request.finalize_ms`.
- `proxy.response_stream`: wraps the downstream stream relay. Records the existing `stream_total_ms` on complete, or partial `cc_lb.response_body_ms` and `stream.outcome="client_cancelled"` on client cancellation.
- `cache_keepalive.renewal_lifecycle`: wraps only the existing renewal lifecycle event publication from `RequestStarted` to `RequestTerminated`. Records `cc_lb.source_kind="renewal"` and `cc_lb.renewal_cycle_ms=duration_ms`; does not synthesize proxy stages.

The `proxy.handle` unaccounted computation subtracts each present `request_body_read_ms`, `proxy_setup_ms`, `shape_ms`, `sign_ms`, `upstream_ttfb_ms`, the selected response-body parent, and `finalize_ms` exactly once from `duration_ms`. Sums and differences use saturating arithmetic. Do not add new high-cardinality duplicate attributes for request body content, credentials, or request ID. Existing request ID correlation attributes may be used as-is.

## 14. Implementation results

The five stages below are implemented through lifecycle, storage/API, UI, metrics, and regression verification. Each stage's items preserve the contract and acceptance boundary used at implementation time.

### Stage 1: ingress and lifecycle contract

- Measure `request_body_read_ms` and `request_body_bytes` in the server.
- Add the three new fields to `LifecycleContext` terminal state and `RequestTerminated`.
- Explicitly close the body error/413 terminal path.

Acceptance:

- Slow chunked upload wall time lands in `request_body_read_ms`.
- Does not overlap JSON parse time.
- `request_body_bytes` is exact for the 854,336B/917,567B fixtures.

### Stage 2: normal/Drop final timing

- Record `finalize_ms` for normal stream/non-stream.
- Preserve partial elapsed at cancellation in the existing `upstream_body_ms` from the Drop guard.
- Define the parent/child clock boundary in one place.

Acceptance:

- Normal proxy residual ≤10ms.
- Mid-stream cancellation partial elapsed preserved within tolerance.
- Complete stream and 499 partial stream are not summed together.

### Stage 3: event/storage/API propagation

- Update assembler, request log, storage API, SQLite/PostgreSQL, admin recent/delta/detail.
- Add the SQLite nullable materialized list column migration.

Acceptance:

- The three new values match across lifecycle input, final payload, DB row, recent, delta/final, and detail.
- Old rows and mixed rows deserialize/list/render.

### Stage 4: source-kind UI

- Change proxy computation to the new parent stage model.
- Display 499 `Partial stream (client cancelled)` and renewal `Renewal cycle` separately.
- Keep the legacy fallback.

Acceptance:

- No proxy `Unaccounted 100%` appears on renewal.
- 499 shows partial body time before cancellation.
- Normal stream body is not summed twice.

### Stage 5: metrics/OTLP and regression QA

- Add low-cardinality metrics and span attributes.
- Run the proxy/UI matrix below.

Acceptance:

- Metrics scrape and in-process span recording match the source/stage semantics of request events. External OTLP collector export was not exercised because local config has no endpoint.
- The new instrumentation does not change proxy response semantics or request bytes.

## 15. Proxy-path and data QA — PASS

On 2026-09-10, Cases F–K were verified in an isolated run using a fake upstream, dynamic ports, and a temporary SQLite DB. The production DB was not modified. Temporary directories and screenshot paths are not promised as permanent evidence; only reproducible command scopes and observed values are recorded below. Past Cases A–E results are kept separately in the scenario document's Historical Verdict.

### 15.1 Execution results

| Scope | Verdict | Observed values |
| --- | --- | --- |
| Slow chunked ingress | PASS | On the 180,000B final request, `request_body_read_ms=2,030`, `duration_ms=2,048`, raw residual `3ms`. Upload delay was included in the body-read stage. |
| Exact request bodies | PASS | For 854,336B and 917,567B respectively, event `request_body_bytes`, upstream capture length, and SHA-256 of input fixture vs capture matched. Raw residuals were `3ms` and `2ms`. |
| Normal non-stream | PASS | Residual summing the selected response-body parent and `finalize_ms` once each was `2ms`, inside the 10ms budget. |
| Normal complete stream | PASS | `stream_total_ms=9,961` and `upstream_body_ms=9,961` were equal and the body parent was summed once. Raw residual was `1ms`. |
| Client-cancelled 499 | PASS | `upstream_body_ms=1,273`, `stream_total_ms` missing, raw residual `2ms`. Status/error stayed `499`/`client_closed_request`. |
| Renewal | PASS | `source_kind="renewal"`, `duration_ms=3,210` used as a single `Renewal cycle`; no proxy timing synthesized. |
| Storage/API parity | PASS | SQLite payload matched materialized `list_request_body_read_ms`, `list_request_body_bytes`, `list_finalize_ms`. Recent, delta final, detail, and SQLite returned the same values including optional/missing semantics. |
| Metrics | PASS | Scrape confirmed only the fixed vocabulary of `source_kind`/`stage`/`outcome` was used; stage and unaccounted residual histograms confirmed. Request ID, principal, model, raw body size were not used as new labels. |
| OTLP | LIMITED PASS | Span recording tests passed; collector export not exercised because local config has no endpoint. |

### 15.2 Cases F–K verdict

| Case | Required surfaces | Verdict | Evidence |
| --- | --- | --- | --- |
| F — slow chunked ingress and 854,336B parity | Proxy, SQLite, recent/delta/detail, upstream capture | PASS | 180,000B slow chunk: body read `2,030ms`, duration `2,048ms`, residual `3ms`. Separate 854,336B exact-body run: event bytes, capture length, and SHA-256 matched; residual `3ms`. |
| G/H — 917,567B and non-stream accounting | Proxy, SQLite, recent/delta/detail | PASS | Exact 917,567B propagated across payload/materialized/API fields and matched upstream capture length/SHA-256; residual `2ms`. |
| I — complete stream accounting | Proxy, SQLite, recent/delta/detail | PASS | `stream_total_ms=upstream_body_ms=9,961`; body counted once; residual `1ms`. |
| J — 499 partial stream | Proxy, SQLite, recent/delta/detail | PASS | `upstream_body_ms=1,273`, complete `stream_total_ms` absent, residual `2ms`. |
| K — renewal timeline | SQLite, recent/delta/detail | PASS | `duration_ms=3,210` rendered as one source-specific cycle with proxy timing absent. |

### 15.3 Automated validation

| Command scope | Result |
| --- | --- |
| Rust workspace check and clippy, SQLite and PostgreSQL feature configurations | PASS |
| Engine tests | PASS — 508 |
| Server tests | PASS — 176 |
| Lifecycle tests | PASS — 6 |
| Pricing tests | PASS — 35 + 9 |
| Storage SQLite tests | PASS — 12 + 37 |
| Storage PostgreSQL tests | PASS — 11 + 27 |
| Admin event tests | PASS — 24 |
| Admin-web tests | PASS — 71 files, 684 tests |

### 15.4 Proxy path performance regression verification

Current head and `origin/master` were compared three times each, alternating, under the same release build, loopback fake upstream, and raw TCP load generator. Each run used 1,000 non-streaming requests (concurrency 4) and 300 streaming requests (concurrency 8).

| Metric | Current head 3-run median | `origin/master` 3-run median | Difference |
| --- | ---: | ---: | ---: |
| Non-streaming p50 proxy overhead | 0.343ms | 0.347ms | -0.004ms |
| Non-streaming p99 proxy overhead | 0.434ms | 0.627ms | -0.193ms |
| Streaming p50 proxy overhead | 0.867ms | 0.702ms | +0.165ms |
| Streaming p50 per-event overhead | 0.016ms | 0.013ms | +0.003ms |

All runs passed the repository budgets of non-streaming p50 under 5ms, non-streaming p99 under 20ms, and streaming p50 event overhead under 5ms. Streaming p99 had large run-to-run variance on both revisions due to loopback host scheduling outliers and is not a budget verdict metric.

Static hot-path review shows the work added to the request executor is only 3~4 monotonic clock reads, 3~4 uncontended lifecycle-state locks, and span field records per request. No work was added to the SSE chunk loop and the happy-path lifecycle event count did not grow. Timing aggregation's String allocations, map lookups, and histogram recording run on the dedicated logger task, and event fanout uses the existing `try_send` drop-and-count approach so it does not block the request executor. Added in-flight state is about 152B per request, about 1.5MB for 10,000 long-lived streams. The logger map is capped at 16,384 entries, deterministically evicts the oldest active entry, and records an eviction counter.

Therefore no measurable non-streaming regression existed, and the streaming median difference of 0.165ms per request and 0.003ms per event was small relative to budget. No meaningful latency, throughput, memory, or backpressure regression was observed on the proxy path.

## 16. UI QA — PASS

The actual isolated Logs route was checked at 1280×800 desktop and 375×812 mobile viewports.

| Surface | Verdict | Observed values |
| --- | --- | --- |
| Desktop proxy timeline | PASS | `Request body read` and `Finalize` displayed; residual inside the 10ms budget. |
| Mobile 499 timeline | PASS | `Partial stream (client cancelled)` displayed; no complete stream stage synthesized. |
| Renewal drawer | PASS | `Renewal cycle` displayed as `3,210ms`; no `Unaccounted` or `Internal pre`. |
| Layout/runtime | PASS | No horizontal overflow at either inspected viewport; no browser console errors. |

UI results are limited to the two viewports above and the Cases F–K drawer paths. 768×1024, external OTLP collector, and separate deployment environments are not claimed as evidence from this isolated run.

## 17. Final completion criteria

The implementation and the 2026-09-10 isolated QA satisfied the following conditions:

1. `request_body_read_ms` and actual `request_body_bytes` are recorded on normal proxy final rows.
2. The correlation between body bytes and body-read duration reproduces on the 854KB/918KB and slow-upload fixtures.
3. `Unaccounted` on normal stream/non-stream proxy with complete new instrumentation is 10ms or less each.
4. 499 preserves partial elapsed from relay start to cancellation in the existing `upstream_body_ms`.
5. Renewal is separated from the proxy timeline and the entire duration is explained as `Renewal cycle`.
6. Lifecycle, assembler, `RequestEvent`, SQLite/PostgreSQL, recent/delta/detail API, and TypeScript schema/UI follow the same three-new-field contract.
7. Parent/child stages do not overlap and normal stream body is not double-summed.
8. Backward compatibility is preserved for historical/mixed rows without backfill.
9. Prometheus upheld the low-cardinality contract and residual histogram contract, and in-process span recording tests passed. External OTLP collector export was not exercised because local config has no endpoint.
10. Cases F–K proxy/storage/API/metrics/UI paths passed and request body length/SHA-256 plus existing status/error semantics were preserved.
