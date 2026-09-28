# Request timing breakdown

Request logs expose eight optional setup timings and nine optional request/response I/O observations. They originate in the proxy lifecycle and remain attached to the same request through terminal lifecycle events, request-event assembly, storage payloads, admin APIs, and the Logs latency UI.

## Setup fields and boundaries

The existing setup fields keep their original boundaries:

| Field | Measurement boundary |
| --- | --- |
| `json_parse_ms` | Only `sonic_rs::from_slice` inside `RequestBodyView::new`. Header handling and metadata extraction are outside the interval. |
| `cache_tokenizer_queue_ms` | Waiting for the `PromptCacheAnalysisExecutor` semaphore permit. Blocking analysis work starts after this interval. |
| `cache_structure_ms` | Block flattening, breakpoint resolution, structural prefix-chain hashing, and structural lookback construction. Exact token-prefix byte materialization and token-key hashing are outside the interval. |
| `cache_serialize_ms` | Materializing exact serialized prefix bytes and breakpoint offsets for token counting. Structural analysis and token-key hashing are outside the interval. |
| `cache_token_key_ms` | Computing credential-scoped exact token-prefix BLAKE3 keys from the materialized bytes. Serialization and cache access are outside the interval. |
| `cache_count_lookup_ms` | The sum of distributed exact token-count cache intervals: claim, hit/miss bookkeeping, leader completion, coalesced follower wait, and count retrieval. Actual leader or fallback tokenization intervals are excluded. Paths that bypass the count cache record measured zero. |
| `cache_tokenize_ms` | Exact BPE tokenization used only when serialized prefix bytes fall between the hybrid threshold's fast-reject and fast-accept bounds. Byte fast paths and reference fallbacks record `0`; a warm analysis can still record a positive value when its prefix remains in the ambiguous byte band. |
| `prepare_signer_ms` | The full awaited `signer_factory.build(&route.upstream)` call, including lookup, decrypt, and lazy OAuth refresh when present. Request signing remains in `sign_ms`. |

Values use fractional milliseconds (`f64` in Rust and `number` in TypeScript). Lifecycle-produced values come from monotonic `Duration` measurements and are finite and non-negative. Every field is optional so rows written by earlier versions still deserialize and render.

## Request and response I/O fields

These fields are observations at the proxy's visible boundaries. They do not measure the complete physical path between the client, cc-lb, and the provider.

| Field | Measurement boundary and interpretation |
| --- | --- |
| `request_body_first_chunk_ms` | From request-body collection start to the first non-empty DATA frame. It is `None` if no non-empty DATA frame arrives. This marker overlaps the request-body parent and the wait/process split, so it is diagnostic only and must not be added to totals. |
| `request_body_receive_ms` | From the first non-empty DATA frame to collection end, including success, read failure, cancellation, or a size-cap stop. It is `None` if no non-empty DATA frame arrives. This marker overlaps the wait/process split and is diagnostic only. |
| `request_body_wait_ms` | Sum of time awaiting request-body frames. It combines client pacing, downstream transit, and runtime scheduling visible after handler entry. It is not local processing and is not an RTT measurement. |
| `request_body_process_ms` | Elapsed synchronous work between frame awaits, including frame handling, copying, and validation. It is disjoint from `request_body_wait_ms`, but it is elapsed local work rather than CPU-cycle time. |
| `request_body_chunk_count` | Count of non-empty request DATA frames collected. A measured `0` is distinct from a missing observation. |
| `response_body_wait_ms` | Sum of time awaiting upstream response-body frames. It combines provider generation, upstream transit, and runtime scheduling; it cannot be assigned wholly to provider processing or network transit. |
| `response_body_process_ms` | Elapsed local relay or buffered-body work inside the existing response-body parent. Work that begins after that parent, including later finalization or decode work, is not added to this field. |
| `response_body_downstream_poll_gap_ms` | For streamed responses, the observed gaps from yielding an item until the downstream consumer polls for the next item. It combines consumer pacing, backpressure, and runtime scheduling. It is not a TCP acknowledgement, wire-delivery time, or RTT. Buffered non-stream responses leave it missing because delivery after finalization is not observed. |
| `retry_overhead_ms` | Recorded only when a retry actually starts. It covers the earlier attempt path from its dispatch start through the retry boundary and is disjoint from the final attempt's reset stage timings. It is a mixed cc-lb/upstream interval, not a pure local or provider value. |

The existing `request_body_read_ms` remains the parent from handler-side body-read start to collection completion. `request_body_first_chunk_ms` and `request_body_receive_ms` are overlapping markers within that parent. `request_body_wait_ms` and `request_body_process_ms` form the non-overlapping diagnostic split used for attribution. A size cap rejected from `Content-Length` can therefore have a measured zero-frame/zero-wait result without a first-chunk or receive marker.

The selected response-body parent remains `stream_total_ms` for a completed stream, `upstream_body_ms` for a buffered response, or the partial `upstream_body_ms` for a client-cancelled `499`. Response wait, local process, and downstream poll-gap values are children of that parent. Any positive parent remainder stays unattributed rather than being assigned to provider or network time.

## Setup execution order

The request path records setup stages in this order:

1. Parse the request JSON and store `json_parse_ms` in the terminal lifecycle context.
2. Wait for the prompt-cache executor permit and record `cache_tokenizer_queue_ms`.
3. Run structural analysis and record `cache_structure_ms`.
4. Materialize exact token-prefix bytes and record `cache_serialize_ms`.
5. Compute credential-scoped token keys and record `cache_token_key_ms`.
6. Claim or retrieve cached prefix sizes and record `cache_count_lookup_ms`. Hybrid threshold evaluation runs afterward; only ambiguous prefixes are tokenized and timed as `cache_tokenize_ms`.
7. Authenticate, route, and reserve limits using their existing timing fields.
8. Build the selected upstream signer and record `prepare_signer_ms`.
9. Shape and sign the outbound request using the existing `shape_ms` and `sign_ms` stages.

The terminal lifecycle context merges completed I/O snapshots field by field: a missing value in a later ingress, response, cancellation, or retry snapshot does not erase an earlier observation. Normal completion and terminal error/cancellation paths publish the values available at termination.

## Setup residual and parent chronology

For a final row with at least one setup timing field, setup details replace the legacy `Setup overhead` row and retain this residual:

```text
Other setup = max(
  0,
  proxy_setup_ms
    - auth_ms
    - route_ms
    - limit_reserve_ms
    - json_parse_ms
    - cache_structure_ms
    - cache_token_key_ms
    - cache_count_lookup_ms
    - cache_tokenizer_queue_ms
    - cache_serialize_ms
    - cache_tokenize_ms
    - prepare_signer_ms
)
```

Missing optional values contribute zero only to this residual calculation. A present `0` remains a measured value and renders as `0 ms`; it is not treated as missing.

When all eight setup fields are absent, the UI keeps the legacy fallback:

```text
Setup overhead = max(0, proxy_setup_ms - auth_ms - route_ms - limit_reserve_ms)
```

The chronological timeline continues to use parent intervals. Its pre-upstream parent grouping is `request_body_read_ms + proxy_setup_ms + shape_ms + sign_ms`; the setup children explain `proxy_setup_ms` but do not enlarge it. The upstream chronology derives its header-wait residual from the `upstream_ttfb_ms` parent and retains the selected response-body parent. The SSE marker origin therefore remains based on the parent chronology, not on a sum of child diagnostics.

Parent accounting subtracts each of these at most once from total request duration:

```text
request_body_read
+ proxy_setup
+ shape
+ sign
+ upstream_ttfb
+ selected response_body parent
+ finalize
+ retry_overhead, when a retry started
```

The final attempt's shape, sign, and upstream timings are reset before retry dispatch, while `retry_overhead_ms` accounts for the earlier attempt path once. Request/response child observations and overlapping markers never join this parent sum. Responsibility totals use those child observations only to divide a parent across boundaries; arithmetic remainders and retry aggregates stay unattributed rather than being assigned to a guessed owner.

## Responsibility-based UI attribution

The Logs latency cell and Request Detail Sheet use one shared calculation and present five responsibility groups:

| Group | Included measured values | Important limitation |
| --- | --- | --- |
| **Downstream** | `request_body_wait_ms` plus streamed `response_body_downstream_poll_gap_ms` when present. | These observations combine client/consumer pacing, downstream transit, backpressure, and runtime scheduling. They are not network RTT measurements. Downstream TLS and request headers before handler entry, and buffered response delivery after finalization, are not observed. |
| **cc-lb** | The setup parent (or measured setup children if the parent is absent), `shape_ms`, `sign_ms`, `bulkhead_wait_ms`, request/response local process observations, and the finalize parent. | This is elapsed local work and local waiting, not CPU time. Parent values replace their children when both exist. |
| **Upstream net** | Recorded `dns_ms` plus `connect_ms`. | `connect_ms` combines TCP and TLS connection work. A reused connection can legitimately show zero with a `Warm pool` marker. |
| **Upstream wait** | The non-negative header residual `upstream_ttfb_ms - bulkhead_wait_ms - dns_ms - connect_ms`, plus measured `response_body_wait_ms`. | These waits combine provider generation, upstream transit, and runtime scheduling. The available timestamps cannot separate those components. |
| **Unattributed** | `retry_overhead_ms`, request/response parent remainders, and positive time outside the additive parent accounting. | Retry overhead is one aggregate for completed prior attempts, so its cc-lb, network, and upstream portions cannot be reconstructed. Other residuals have no finer timing witness. |

The compact Logs popover keeps the original section layout. The Request Detail Sheet shows the same responsibility totals and distribution above the existing chronological request timeline. The chronology, SSE markers, and detailed stage rows remain separate because responsibility attribution is not a wall-clock sequence.

Measurement limitations are attached to the relevant group or item as contextual help rather than shown as a permanent warning block. Renewal lifecycle rows remain one `Renewal cycle`; live partial rows show only observations available so far, without labeling elapsed in-flight time as residual latency.

## Propagation and compatibility

`RequestIoTimings` carries the nine optional fields nested under `io_timings` on `RequestTerminated`. The lifecycle assembler merges terminal partials and flattens them into `RequestEvent`, `RequestEventPartial`, and list DTO fields. Recent, delta/final, and detail APIs expose the same optional values. The TypeScript/Zod contract accepts only finite, non-negative durations and non-negative integer chunk counts while preserving missing, `null`, and measured zero.

SQLite migration `0082_request_event_io_timing_list_fields.sql` adds eight nullable REAL `list_*` columns and one nullable INTEGER chunk-count column. It performs no historical backfill, so older rows keep these values missing. New rows bind the materialized list columns directly. PostgreSQL keeps the fields in its bounded lightweight JSON payload projection and needs no corresponding schema migration.

SQLite migration `0077_request_event_setup_timing_list_fields.sql` continues to provide the eight nullable setup columns described above. Neither migration changes historical payloads or synthesizes measurements for old rows.

## Prometheus stage metrics

The lifecycle event logger emits present timing values to:

```text
cc_lb_request_stage_duration_seconds{source_kind,stage,outcome}
```

Its labels use fixed vocabularies:

- `source_kind`: `proxy`, `renewal`, or `unknown`
- `outcome`: `success`, `client_cancelled`, `error`, or `timeout`
- parent `stage`: `request_body_read`, `proxy_setup`, `shape`, `sign`, `upstream_ttfb`, `response_body`, `finalize`, or `renewal_cycle`
- diagnostic I/O `stage`: `request_body_first_chunk_marker`, `request_body_receive_marker`, `request_body_wait_mixed`, `request_body_process`, `response_body_wait_mixed`, `response_body_process`, `downstream_poll_gap_mixed`, or `retry_overhead_mixed`

The logger records fractional I/O values without converting a measured zero to missing. It does not expose request IDs, principals, models, raw body sizes, or other unbounded values as labels.

Do not sum every `stage` series. The first-chunk and receive markers overlap their parent and split children; response children overlap the response parent. Use parent stages plus `retry_overhead_mixed` for parent accounting, or query one diagnostic stage at a time. Prometheus histograms aggregate different requests and cannot reconstruct a per-request critical path by adding stage quantiles.

## Historical verification record

The following results are historical evidence for the original eight setup fields, recorded on September 8, 2026. They do not verify the newer nine I/O fields, four-category UI, or a current browser build:

- Rust setup-field tests, formatter, and targeted suites passed at that historical revision.
- Web setup-field tests, formatter, typecheck, and production build passed at that historical revision.
- The isolated SQLite cold-to-warm setup-field proxy scenario passed and preserved byte-identical request bodies.
- Chromium setup-field QA covered the then-current desktop/mobile, light/dark, legacy, fractional, zero, keyboard, overflow, and SSE-origin cases.

Current I/O and four-category verification must be reported from its own evidence. This document does not claim deployment, production latency improvement, or completed browser QA for that newer work.
