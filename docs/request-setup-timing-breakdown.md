# Request setup timing breakdown

Request logs expose eight optional, request-scoped setup timings. They originate in the proxy lifecycle and remain attached to the same request through terminal lifecycle events, request-event assembly, storage payloads, admin APIs, and the Logs latency UI.

## Fields and boundaries

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

## Execution order

The request path records stages in this order:

1. Parse the request JSON and store `json_parse_ms` in the terminal lifecycle context.
2. Wait for the prompt-cache executor permit and record `cache_tokenizer_queue_ms`.
3. Run structural analysis and record `cache_structure_ms`.
4. Materialize exact token-prefix bytes and record `cache_serialize_ms`.
5. Compute credential-scoped token keys and record `cache_token_key_ms`.
6. Claim or retrieve cached prefix sizes and record `cache_count_lookup_ms`. Hybrid threshold evaluation runs afterward; only ambiguous prefixes are tokenized and timed as `cache_tokenize_ms`.
7. Authenticate, route, and reserve limits using their existing timing fields.
8. Build the selected upstream signer and record `prepare_signer_ms`.
9. Shape and sign the outbound request using the existing `shape_ms` and `sign_ms` stages.

The terminal lifecycle context owns the latest completed setup timings. Normal completion, provider failure, signer failure, limit rejection, and drop-based termination all serialize the same timing value into `RequestTerminated`. The assembler copies it into live terminal partials and the final `RequestEvent`.

Live in-flight partials emitted before termination do not yet receive these setup timings. `RequestTerminated` carries the completed values, so the terminal partial and final row contain them. The final-row UI contract does not require a new lifecycle event.

## Sum invariants

For a final row with at least one new timing field, the UI replaces the legacy `Setup overhead` row with the measured stages and:

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

Missing optional values contribute zero to this residual calculation. A present `0` remains a measured value and renders as `0 ms`; it is not treated as missing.

`shape_ms` and `sign_ms` remain outside `proxy_setup_ms`. The `Internal pre` aggregate and SSE marker origin retain the existing `proxy_setup_ms + shape_ms + sign_ms` meaning. They do not sum the new child stages independently, so inconsistent or rounded child values cannot shift SSE markers. Prompt-cache parent totals are not added beside their children.

Timeline stage details retain measured zero and fractional setup values. The absolute bar omits zero and sub-pixel setup segments so their synthetic hit targets cannot overlap at the same position. The `Internal pre` group header uses the proxy-setup-based aggregate rather than the raw child sum.

When all eight fields are absent, the UI keeps the legacy fallback:

```text
Setup overhead = max(0, proxy_setup_ms - auth_ms - route_ms - limit_reserve_ms)
```

## Storage and compatibility

SQLite and PostgreSQL continue to persist the complete `RequestEvent` JSON payload. SQLite migration `0077_request_event_setup_timing_list_fields.sql` only adds eight nullable REAL `list_*` columns, and the append path binds them directly for new rows. It performs no historical backfill scan: rows written before this version keep the fields missing and use the legacy `Setup overhead` fallback. The bounded list query reads only materialized columns; it does not fetch or parse raw payloads and adds no repeated `json_extract` expressions to the hot list query. PostgreSQL keeps its bounded small `ListPayload` decode. Recent-list, delta/final, and event-detail admin paths therefore expose the same values for newly instrumented rows.

No routing, cache-control breakpoint, TTL, lookback, prefix-hash, selected-upstream, request-body, Prometheus metric, or proxy response semantics change.

## Verification checklist

Implementation:

- [x] All eight fields are measured with fractional-millisecond resolution.
- [x] Prompt-cache queue, structure, serialization, token-key, count-cache, and tokenization boundaries are separate.
- [x] Terminal lifecycle state preserves completed timings on success and early termination.
- [x] Final and live terminal partial request events expose the fields.
- [x] SQLite and PostgreSQL list projections carry the fields; SQLite uses migration-backed materialized REAL columns and PostgreSQL keeps its bounded payload decoder.
- [x] Recent-list, delta/final, and event-detail admin API contracts carry the same timing fields.
- [x] Zod and TypeScript contracts distinguish missing, null, zero, and fractional values.
- [x] Timeline and LatencyCell cover new, legacy, zero, partial, residual-clamp, and SSE-origin behavior.

Execution (verified September 8, 2026):

- [x] Rust formatter and targeted suites passed.
- [x] Web formatter, typecheck, tests, and production build passed.
- [x] Isolated SQLite proxy cold-to-warm E2E passed.
- [x] Chromium QA passed at desktop/mobile and light/dark viewports.

## Verification results

- **Rust:** `cc-lb-engine` library tests passed with 458 passed and 1 ignored. The `lifecycle_terminal` integration test passed all 4 tests. Lifecycle, request-log, storage API, SQLite storage, and admin recent/delta suites passed; the final SQLite groups included 12 and 34 passing tests. The exact recent and delta admin tests each passed. `cargo fmt` passed. The PostgreSQL exact test compiled but performed its documented no-op because no local PostgreSQL URL was available; CI PostgreSQL remains the live round-trip gate.
- **Web:** The current-head focused run passed 5 files and 98 Vitest tests. Biome, typecheck, and the production build passed. Biome reported only the repository's existing schema-version information.
- **Proxy E2E:** A 2,048,385-byte request with four cache breakpoints returned `200` and `pong` both times. Cold wall time was 1,403.831 ms, with `proxy_setup_ms=1393` and `cache_tokenize_ms=1219.967083`. Warm wall time was 175.369 ms, with `proxy_setup_ms=167` and `cache_tokenize_ms=0.0`. Payload, SQLite materialized list columns, recent, delta, and detail returned identical eight-field values. The fake upstream received byte-identical bodies with SHA-256 `df15ad60...63d4`.
- **Chromium QA:** Cold and warm rows passed at 1280×800 light; warm passed at 1280×800 dark and 375×812 light/dark; legacy fallback passed at 1280 light and 375 dark. The eight-stage order, `Other setup`, fractional display, warm `0 ms`, keyboard focus, internal scroll reachability, horizontal overflow, and console-error checks passed. The only SSE `ERR_ABORTED` occurred during the expected context-close cleanup.
- **Independent review:** Backend and frontend spec audits passed every assigned S item. Final code review passed, and the security review reported zero findings.
