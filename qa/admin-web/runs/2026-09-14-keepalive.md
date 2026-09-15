# Keepalive Admin Web performance run — 2026-09-14

## Verdict

**Historical performance finding: FAIL — severe Principal-cardinality-dependent backend latency. Coverage verdict: NOT VERIFIED.**

This historical run reported observations across 22 active Principals and the Keepalive sessions filters. Its exhaustive-completion claim is withdrawn: pagination included a 20-page cap without sufficient raw terminal-cursor proof, and three detail UI/direct measurements lack independent request timestamps. The earlier representative-page result and the first `limit=30` rerun are also invalid as exhaustive QA. The JSON is preserved unchanged as historical evidence, not a completed execution ledger.

Machine-readable evidence: `qa/admin-web/runs/2026-09-14-keepalive.json`.

Correction recorded on 2026-09-15: `qa/admin-web/runs/2026-09-14-keepalive-review.json` identifies the evidence limitations and the source JSON hash. A replacement production run must capture independent requests and actual terminal cursors. Later isolated Keepalive regression tests do not replace that production run.

The new run is tracked in `qa/admin-web/runs/2026-09-15/manifest.json`. It is explicitly `IN_PROGRESS` and does not yet supersede the incomplete historical Keepalive measurements. Follow its per-artifact evidence and outstanding matrix rather than interpreting this cross-link as a replacement PASS.

## Historically reported counts — not verified completion

- Active Principals: **22 / 22**
- Horizons: `24h`, `7d`, `all`
- Filters: `all`, `renewed`, `scheduled`, `capped`, `expired`, `not_tracked`, `error`
- UI-triggered combinations: **462 / 462** (`22 × 3 × 7`)
- Equivalent direct-fetch combinations: **462 / 462**
- UI requests containing an invalid `limit` parameter: **0**
- Summary records with a first request and two subsequent requests: **22 / 22**. These labels do not prove a cold OS/DB cache.
- Principal detail measurements: **3 / 3 Principals with returned session rows**
- Concurrent summary/list/detail polling records: **3 / 3**
- Pagination runs reported as terminal: **10 / 10 — completion claim withdrawn; actual terminal traversal remains unverified.**
- Reported unique execution keys: **962 / 962**. Matching record counts do not establish valid coverage or independent measurements.
- Authorization tokens or secret values stored in the ledger: **0**

## Sessions list latency

All figures below use the UI's actual request format. The frontend omits `limit`; the backend applies its default page size of 50.

| Measurement | UI-triggered total | Equivalent direct fetch |
|---|---:|---:|
| Minimum | 211 ms | 211 ms |
| p50 | 250 ms | 237 ms |
| p95 | 384 ms | 377 ms |
| Maximum | **28,336 ms** | **28,017 ms** |

The global p95 hides the outlier because 21 of 22 Principals have little or no costly Keepalive history. Entity-level measurement is therefore mandatory.

## Principal-level result

| Principal group | UI p50 | UI maximum | Result |
|---|---:|---:|---|
| `isac-pi` | **24,614 ms** | **28,336 ms** | FAIL |
| `shuwn-claude` | 281 ms | 343 ms | PASS |
| `isac-claude` | 259 ms | 461 ms | PASS |
| Remaining 19 Principals | 227–260 ms | 256–416 ms | PASS |

## Detail request latency

| Principal | UI-triggered detail | Direct fetch | Payload |
|---|---:|---:|---:|
| `isac-claude` | 296 ms | 296 ms | 769 B UI resource |
| `shuwn-claude` | 287 ms | 287 ms | 752 B UI resource |
| `isac-pi` | **21,130 ms** | **21,130 ms** | 763 B UI resource |

The response payload is less than 1 KiB, so the 21-second delay is not browser parsing or transfer time.

## Concurrent polling

When a session detail is open, the page keeps three independent five-second poll streams active.

| Principal | Observation window | Summary polls | List polls | Detail polls | Observed totals |
|---|---:|---:|---:|---:|---|
| `isac-claude` | 15 s | 3 | 2 | 2 | 250–376 ms |
| `shuwn-claude` | 15 s | 3 | 2 | 2 | 279–388 ms |
| `isac-pi` | 45 s | 2 | 2 | 2 | **16,720–22,505 ms** |

For `isac-pi`, each five-second poll takes substantially longer than its configured interval. Summary, list, and detail work overlap and continuously place expensive full-history reads in flight.

## Root cause

The delay is in the API/database aggregation path, not the browser or payload transfer:

1. `CacheKeepaliveCard` calls `GET /admin/v1/principals/{id}/cache-keepalive?limit=0` every five seconds.
2. `CacheKeepaliveSessionsDrawer` calls the same list endpoint for each horizon/filter and polls every five seconds while open.
3. `SessionDetailPane` calls the detail endpoint every five seconds while open.
4. Both list and detail handlers call `view::list_all` before producing their requested projection.
5. `view::list_all` repeatedly pages through the Principal's entire sessions/decisions corpus in batches of 1,000.
6. `load_activity_batch` then loads turns for all collected session hashes with `WHERE principal_id = $1 AND session_key_hash = ANY($2)` and recalculates pricing/P&L in Rust.
7. Filtered list requests still perform the unfiltered full-history pass for summary calculation before fetching the filtered page.
8. Detail lookup performs the same full-history scan before finding one requested item.
9. List rendering can additionally issue sequential upstream-name lookups for distinct upstreams in the page.

Relevant source paths:

- `crates/cc-lb-admin/src/v1/principal_cache_keepalive.rs`
- `crates/cc-lb-admin/src/v1/principal_cache_keepalive/view.rs`
- `crates/cc-lb-admin/src/cache_keepalive_view.rs`
- `crates/cc-lb-storage-postgres/src/adapter/cache_keepalive_session_reads.rs`
- `crates/cc-lb-admin/web/src/lib/queries.ts`

## Historical remediation proposals — not an approved implementation checklist

No application code was changed by this historical QA run. The proposals below preceded the equivalence review; not all were adopted. PR #791 implemented scoped reads, bounded pagination and indexes, and an explicitly approved cursor-window fix. Polling semantics and pricing were preserved. The original proposals are retained here for context, not as instructions to change current behavior:

1. Compute the four card metrics using bounded aggregate queries instead of loading all session and turn records.
2. Apply horizon/status/error filters in SQL before loading activity and pricing data.
3. Fetch only the requested list page and its turns; do not calculate summary through `list_all` on every filtered request.
4. Resolve a detail item directly by Principal and item ID before loading only that item's turns.
5. Batch upstream-name resolution or join names into the page query.
6. Prevent three overlapping five-second poll streams from repeatedly launching full-history work when the previous request remains in flight.

## Scope caveat

Neither the historical Keepalive ledger nor the broader Admin Web inventory establishes runtime-complete coverage. Both remain subject to full reconciliation. The broader inventory's 226 rows are generated catalog entries, not a verified source-wide denominator. See `docs/admin-web-qa-completion-plan-2026-09-15.md` for the replacement execution and evidence requirements. A route visit, representative entity, record count, or later isolated test PASS is not full production QA.
