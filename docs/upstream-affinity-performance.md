# Upstream affinity performance improvement

## Scope and preservation contract

Resolve the problems found in the Web Search affinity performance audit. The performance refactoring preserves functionality. The only separately user-approved change is the user-configurable TTL with a 90-day default. No routing functionality or operational data is changed other than expiry/cleanup under this retention policy. The contract of storing no ciphertext and finding the upstream by principal/provider/kind/SHA-256 is preserved.

- Keep 503 when there is no known key, and 400 when keys map to different known upstreams.
- When known and unregistered keys are mixed, route to the single existing upstream and learn the unregistered keys after success.
- Storage changes are atomic. Do not overwrite the same key of an active mapping to a different upstream. Only expired mappings may be replaced with a newly verified binding.
- Active same-target mappings keep the max `observed_at` and the existing NULL/max expiry merge. The configured TTL is a separate retention limit based on `observed_at`.
- Complete mapping storage before delivering the opaque response to the client. Do not change it to async fire-and-forget.
- Preserve SSE JSON type-first determination, multiline/CR framing, compression, transformation, error propagation, and the learn-only-on-success condition.
- Keep the general SSE raw-chunk path. Do not add unnecessary new DB lookups.
- The response traversal optimization must not miss responses the existing code learned, even when the request has no tools.

## Confirmed problems and root causes

| Problem | Cause | Improvement direction |
| --- | --- | --- |
| N-key writes are N sequential awaits | The storage API is batched, but the adapter decomposes it into per-row SQL | Multi-row UPSERT within the parameter limit, single transaction, preserve duplicate-key normalization and full rollback |
| Duplicate SSE JSON parse/allocation | usage and affinity each own raw-event ownership and parsing responsibility | Share event framing/JSON decoding so observers use the same parsed value |
| resolve O(N×M) comparisons | Request/response key membership verified by linear Vec scan | Linear processing with borrowed hash membership |
| General buffered JSON AST traversal | Insufficient separation between observation necessity and traversal entry points | Reuse functionality-preserving response-structure discrimination/single traversal; no semantic reduction via a simple tools gate |
| Unbounded row growth | NULL-expiry mappings persisted forever with no expiry lookup or physical cleanup policy | User-approved configurable TTL (default 90 days), same policy applied to existing NULL rows, bounded indexed purge outside requests |

## Prior audit baselines

The following are synthetic/local measurements from the prior audit, not production latency. Update them with same-condition comparisons after the change.

- SQLite bind: 19 keys ~0.4–2.8ms, 1,024 keys ~9–50ms+ (varies by environment).
- PG per-row writes: for a batch of 19, 21 sequential statements including BEGIN/COMMIT. When split across SSE events, each bind call is its own transaction, so the whole response is not computed as one transaction.
- Extra SSE parse: ~1.8µs/event for deltas, ~1.8ms CPU for 1,000 events plus temporary string allocations.
- resolve: 19-key ~2µs, 1,024 all-hit ~1–6.7ms; hash membership ~0.1ms range.
- SQLite 100k rows: composite PK MULTI-INDEX OR, p50 for 1/19/200 keys = 5.4/50.4/553.9µs. File ~29.4MB.

## Verification criteria

Use a local synthetic upstream and the real proxy lifecycle; no paid provider/production writes.

- SQLite and PostgreSQL: empty, same-target duplicates, cross-target duplicates, chunk-boundary conflict, rollback, expiry/timestamp merge.
- Proxy POST /v1/messages: plain JSON/SSE, Web Search JSON/SSE, known/unregistered/mixed keys, storage failure, malformed SSE, compression/transform, client-visible bytes, and selected upstream verification.
- Run the relevant existing tests and keep regression tests only for uncertain boundaries.
- Compare 0/19/1024 keys, SSE delta-heavy streams, and DB statement/transaction cost before and after.
- If an independent review finds a functional difference, contract preservation takes priority over performance gains.

## Retention cost and operational observation

The initial performance improvement deferred deletion because an arbitrary TTL would turn old replays into 503s. The user later approved the TTL configuration feature with a 90-day default. Accordingly, expiry and physical cleanup of old mappings are added as a retention policy. This is not a capacity limit guaranteeing a bound on inbound volume itself; mappings within the current policy scope continue to be retained.

### Approved TTL implementation contract

```toml
[upstream_affinity]
ttl_days = 90
```

- The default is 90 days. Specify a positive number of days; 0 is not allowed.
- The environment variable is `CC_LB_UPSTREAM_AFFINITY__TTL_DAYS`.
- Changes apply after a server restart. The engine's lookup policy and the background scheduler's cleanup policy use the same startup configuration.
- The TTL basis is the last successful binding's `observed_at_unix_secs`. It is not a sliding TTL that adds a write on every lookup.
- Expired when `now >= observed_at + TTL`. If a separate `expires_at_unix_secs` exists, that expiry condition also applies.
- Existing `expires_at = NULL` rows are looked up and cleaned up under the same TTL. Do not block the hot path with a bulk backfill UPDATE.
- Raising or lowering the setting applies the new basis to all still-stored rows after restart. Rows already physically deleted are not restored.
- If the issuing upstream of expired history is unknown, return the existing unknown-affinity 503. Do not guess and route to another account.
- Even if purge has not run yet, expired rows are not used for routing. When relearning from a new successful response, an expired conflicting row may be atomically replaced.
- Physical cleanup runs in an out-of-request singleton cron at a default 600-second interval with up to 30 seconds of jitter. One batch is at most 1,000 rows; one job is at most 1,024 batches. Policy expiry applies immediately and does not wait for the physical purge to run.
- Row deletion in SQLite enables page reuse but does not immediately shrink the DB file. Do not run VACUUM or capacity aggregation on the request path.

- `cc_lb_storage_operation_duration_seconds`: resolve/bind latency by store/operation/status. Total call time including pool wait and transaction wait.
- `cc_lb_storage_operation_errors_total`: error count for the same operation. Do not distinguish busy from conflict using this value alone.
- `cc_lb_upstream_affinity_batch_keys`: input key count of non-empty resolve/bind calls. Do not create per-principal/hash/upstream labels.
- Do not add duplicate pool-wait instrumentation. Check existing SQLx pool usage together with total call latency.

Row counts and table/index sizes are not queried on requests or metrics scrapes. Run the read-only SQL below separately during operational checks. Exact COUNTs on large tables are expensive, so match the cadence to traffic and retention volume.

Low-cost capacity/row-count estimate for PostgreSQL:

```sql
SELECT reltuples::bigint AS estimated_rows,
       pg_relation_size(oid) AS table_bytes,
       pg_indexes_size(oid) AS index_bytes,
       pg_total_relation_size(oid) AS total_bytes
FROM pg_class
WHERE oid = 'upstream_affinity_v1'::regclass;
```

`estimated_rows` depends on when statistics were last updated. Use `SELECT COUNT(*) FROM upstream_affinity_v1` only for checks that need an exact row count.

On SQLite, sum the table and its index pages with a read-only connection that supports `dbstat`:

```sql
SELECT name, SUM(pgsize) AS bytes
FROM dbstat
WHERE name = 'upstream_affinity_v1'
   OR name IN (
       SELECT name FROM sqlite_schema
       WHERE type = 'index' AND tbl_name = 'upstream_affinity_v1'
   )
GROUP BY name;
```

On SQLite builds without `dbstat`, do not mistake the whole DB's `page_count × page_size` for the affinity-only size. Inspect an offline copy with a separate supporting tool. Introducing a retention reclamation policy first requires the provider's expiry contract or a user-approved conversation retention limit.

## Implementation results

### Storage write boundary

- Duplicate inputs of the same key are normalized with a borrowed-key map. Different-target conflicts are rejected, and timestamp max/NULL-wins expiry is preserved.
- SQLite executes a multi-row UPSERT of at most 128 rows/898 binds (7 per row plus 2 TTL policy arguments). Variable-tail SQL is not kept in the statement cache; only the 1-row/128-row forms are reused so other storage queries' cache entries are not evicted.
- PostgreSQL passes 7 typed arrays plus 2 policy arguments to a fixed-SQL `UNNEST`. This removes the per-tail prepare/cache churn caused by variable `VALUES` SQL. String/hash data is borrowed, and the bounded array buffer is reused across chunks.
- All chunks are wrapped in a single transaction, and if the affected-row count differs from expectation, the whole transaction rolls back. The TTL feature passes the current time and policy to the lookup/write/purge APIs, and the SQLite 0079/PostgreSQL 0113 migrations add only the observed_at index. The SQLite affinity table is 0078. The unmerged affinity migration numbers were adjusted to avoid colliding with the 0077 request-timing migration on the latest master. No expiry backfill UPDATE is performed on existing rows.
- The 19-key bind SQL execution count computed from source dropped from 19+BEGIN/COMMIT to 1+BEGIN/COMMIT. For 1,024 keys it is 1,024+2 → 8+2. These are not RTTs measured by a DB wire trace, and transactions from different SSE events are not merged.

### Shared SSE parsing and preserved input semantics

- Parse each well-formed SSE event once into `ParsedSseEvent`; usage, provider-error, affinity, and keepalive reference the same value.
- A single `data:` line parses the original slice directly. Only well-formed multiline produces a joined string.
- Preserved the existing observers' tolerance for abnormal input. On the exception path where joined JSON fails, legacy line values are collected once: usage merges them in order, error uses the last value, and keepalive uses the first. Affinity does not use this fallback and fails as before.
- Unicode leading whitespace and a trailing `event: error` also preserve the existing error/accounting semantics.
- Fixed the existing inter-parser inconsistency where usage/keepalive missed values on well-formed multiline/CR-only SSE. Affinity already supported multiline joining before the change.

### Request/response traversal

- Request key membership now uses a borrowed HashSet, changing the O(N×M) comparison to expected O(N+M). 0-key returns immediately with no allocation, DB access, or instrumentation.
- The JSON extractor visits each composite node once, removing scalar recursion and double-visiting of already-identified Web Search content.
- Buffered-response inspection was not gated off by the request `tools` flag. That approach breaks unsolicited-response learning and is incompatible with the functionality-preservation requirement. The necessary inspection itself is kept; traversal cost was reduced.

## Before/after measurements

Used a local Apple Silicon machine and loopback Docker PostgreSQL 18. In the same release binary, the original adapter source was included verbatim in a wrapper, and the current adapter was called via a path dependency. Below are the p50 medians of two passes with the before/current execution order reversed. These numbers do not guarantee production latency or speedups across all workloads.

| Measurement | Before p50 | After p50 | Ratio |
| --- | ---: | ---: | ---: |
| SQLite 19-key new write | 0.428ms | 0.206ms | 2.08× |
| SQLite 1,024-key new write | 11.243ms | 2.447ms | 4.60× |
| SQLite 1,024-key update | 8.159ms | 1.997ms | 4.09× |
| SQLite 1,024-key concurrent write | 12.043ms | 3.802ms | 3.17× |
| PostgreSQL 19-key new write | 11.167ms | 7.171ms | 1.56× |
| PostgreSQL 1,024-key new write | 112.929ms | 16.377ms | 6.90× |
| PostgreSQL 1,024-key update | 108.241ms | 16.379ms | 6.61× |
| PostgreSQL 1,024-key concurrent write | 195.516ms | 38.277ms | 5.11× |

Concurrent writes are a 4-writer workload with distinct keys. Transaction/commit cost remains for small batches. PostgreSQL 19-key concurrent write p50 was 10.861→11.200ms — not improved. Read SQL was not changed, and in the same measurement 19-key resolve was SQLite 74.416→91.459µs, PostgreSQL 574.041→700.583µs. Therefore we do not claim reads or all small batches got faster.

In an additional 5-round paired check interleaving before/current calls without rebuilds, the median current/before p50 ratio was PostgreSQL 1-key new write 1.004, SQLite 19-key resolve 1.006, PostgreSQL 19-key resolve 1.006. The earlier large differences did not reproduce in this measurement. This is neither a claim that instrumentation cost is exactly 0 nor a guarantee of no regression in every environment.

CPU measurements using the final real `usage_parser.rs` and `upstream_affinity.rs` (259 SSE events, excluding keepalives): 19-key stream p50 370.916→152.875µs (2.43×), 1,024-key stream 1,196.583→653.042µs (1.83×). Output checksums were identical across all workloads. This is separate from network/DB/end-to-end proxy latency measurements.

Also compared the real extractor on pre-parsed buffered responses without Web Search. 25KB was 7.458→7.375µs and 251KB was 74.292→75.042µs — small differences — while a 2.52MB input with many composite nodes was 927.250→853.709µs (1.09×). Since the inspection was not removed, we do not claim a large improvement.

An algorithm microbenchmark replicating only the private Lifecycle matching was 1,024 keys p50 6,631.375→133.500µs. This is not a measurement of the real private function or a full HTTP call. At 1 key it was 0.083→0.167µs — the hash structure costs more — a tradeoff to remove the quadratic-time problem on large inputs.

## Performance refactoring stage verification (before TTL addition)

- That stage's PostgreSQL affinity integration: 1 passed. Verified array typing, duplicate merging, the 129-row chunk boundary, follow-up chunk conflict rollback, and the expiry and overflow error contracts on real PostgreSQL 18. The three test runs from the later TTL stage are recorded separately below.
- Full engine: 464 unit + 248 integration passed, each existing ignored test kept at 1. The strict multiline fallback regression added afterward also passed in a separate run.
- Full SQLite: 12 unit + 34 integration passed.
- Full observability: 12 unit + 18 integration passed.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` passed for the 4 relevant crates. The default Nix cargo has no Clippy, so the already-installed identical Rust 1.97.1 toolchain was used. No new packages were installed.
- Built a real `cc-lb` server binary from the current source and connected a loopback proxy `POST /v1/messages` to synthetic A/B upstreams. Verified plain JSON, JSON/SSE response learning and replay, unknown 503, mixed 400, and 0 upstream dispatches for unregistered requests.
- An ordinary non-Web-Search SSE response matched the mock's original bytes exactly.
- With an SSE stream open, read an opaque event, confirmed the DB mapping, then immediately replayed with another candidate present and confirmed it went to the original issuing upstream.
- On the real `/metrics`, verified nonzero duration histograms for 4 binds/5 resolves, the batch histogram, and the fixed store/operation/status labels. Also verified that after revoking the temporary managed key, a request with the same key returns 401.
- Reflected findings from the independent storage/engine review. Resolved the PostgreSQL variable-statement cache problem with fixed UNNEST, and preserved the existing abnormal-SSE error/usage/keepalive interpretation. No final blocking findings.

Errors fixed during verification are also recorded. The first compile failed because the metric registry's array length was not updated; fixed. The PostgreSQL overflow regression failed because the existing helper returns `Fatal` while the test expected `InvalidInput`, so only the test expectation was aligned to the existing contract. The HTTP harness initially expected a synthetic key different from the current signer, and the next control wrongly included a Web Search tool in a plain SSE case and failed. After correcting the harness's auth/control inputs without changing the real implementation's conditions, the final matrix passed.

No paid providers, production DB, or deployment were used. PostgreSQL used an isolated local test schema; SQLite used temporary files. The initial performance refactoring deferred arbitrary deletion; the later explicitly approved TTL feature added the retention period and background cleanup. It does not guarantee recovery of deleted old mappings or an absolute disk-capacity bound independent of input volume.

## Final verification after TTL addition

Re-ran after applying the user-approved 90-day default policy. The results below are kept separate from the pre-TTL performance/verification results above.

### Tests and independent review

| Scope | Passed |
| --- | ---: |
| Config unit / integration | 10 / 35 |
| Engine unit / integration | 465 / 252 |
| Scheduler unit / integration | 39 / 40 |
| Server unit / integration | 174 / 120 |
| Admin unit / integration | 53 / 257 |
| SQLite unit / integration | 12 / 36 |
| PostgreSQL affinity integration (`--ignored`, real local PostgreSQL 18) | 3 |

Existing ignored items were kept and no new bypasses were added. The three PostgreSQL tests cover batch conflict/rollback, the retention boundary and expired rebind, and bounded purge with concurrent-update protection. In particular, they verified that while a separate transaction holds a lock on an updated row, purge returns 0 rows without waiting, and that after commit the new binding is preserved.

Regenerated the config schema and passed the freshness check. Clippy `--all-targets -- -D warnings` passed for the 8 relevant crates. During integration, a missing `serde_json::Value` import in an existing server test surfaced as a compile error and was restored.

The independent storage/runtime review had no blocking correctness findings. Measurement additionally found that PostgreSQL's `DELETE ... USING` after the bounded-candidate CTE could sequentially scan the target table. It was changed to a DELETE over a `ctid` array locked within the same statement. Physical TIDs are not persistently stored, and `FOR UPDATE SKIP LOCKED` plus the external expiry-condition recheck are preserved. The final execution plans for both expiry paths were candidate `Limit → LockRows → Index Scan`, delete target `Tid Scan`.

Additionally checked both purge statements across 12 combinations of full 1,000 / near-empty / empty candidates and custom / generic prepared plans. In every combination, candidates used the index and `Limit/LockRows`, and there was no Seq Scan or Sort on candidates or delete targets. Full and generic plans used the target Tid Scan; near-empty/empty custom plans selectively used the relevant expiry index. The GUC for generic verification was set only inside the isolated test transaction and rolled back.

### Real HTTP and state-transition QA

Ran 18 HTTP scenarios on a real cc-lb using local SQLite with synthetic A/B upstreams. The normal control selected A, and history learned on B stayed on B within its validity period.

| Given / When | Then |
| --- | --- |
| Default omitted, legacy NULL-expiry row observed 89 days ago | HTTP 200, original issuer B selected |
| Past the 90-day boundary or explicit expiry elapsed | Local 503 `api_error`, no A/B dispatch increase |
| 2-day-old row, current 90 days; reload file to 1 day | current-config shows 1 day and restart-required; running policy is 90 days so 200 |
| Restart with 1-day setting on the same DB | Same row returns 503; lookup does not update the timestamp or delete the row |
| Restart after reloading to 90 days, same row not yet purged | 503 before restart, recovered to 200 after restart |
| File says 90 days, environment variable says 1 day at start | Effective TTL 1 day |
| With existing recurring config, set local QA purge interval to 1s and jitter to 0 | Real cron producer and Apalis worker record `Done` and delete 7 expired rows; fresh rows kept |
| Replay old history after purge | 503, no upstream dispatch |

The exact same-second boundary was verified with TestClock-based tests. The real HTTP QA verifies the `>= TTL` state after the seed time. We do not claim that rows already deleted are restored by a config change.

No new admin fire-now or test-only production API was added. Test keys were revoked, synthetic upstreams/principals were cleaned up, and credentials were confirmed absent from logs. No production DB or paid provider was used.

### Performance and execution plans including TTL

Compared the original adapter and the current TTL adapter in the same release binary on the same latest schema/indexes and pool. The baseline SQL/loop was used unchanged; only the changed trait's connection was handled via a scratch-local interface. Therefore this comparison isolates the adapter difference on the latest schema and is not a reproduction of the original PR schema's own performance.

Mean-latency current/baseline ratios for the 19/1,024-key active workload over two passes (3,184 samples) with before/current order reversed:

| backend | new bind 19 / 1,024 | upsert 19 / 1,024 | resolve 19 / 1,024 |
| --- | --- | --- | --- |
| SQLite | 0.761 / 0.219 | 0.820 / 0.386 | 1.001 / 1.090 |
| PostgreSQL | 0.836 / 0.156 | 0.921 / 0.174 | 1.294 / 1.052 |

We do not claim all lookups got faster. In an additional 5-round interleaved measurement of PostgreSQL 19-key lookup without rebuilds, the mean was 372.19→390.93µs and the median 365.21→374.13µs. Per-round ratio variance was large, so the estimated +5.03% mean is not statistically confirmed by that measurement alone.

Lookup plans kept SQLite's composite PK and PostgreSQL's pkey index/bitmap access. Purge bounds candidates with the explicit-expiry/observed-at indexes. After removing the PostgreSQL target sequential scan, the 1,000-row purge mean was explicit-expiry 9.356ms, retention 7.717ms (3 runs each; not production guarantees). The same batch measurement on SQLite was about 4.7–4.9ms.

## Final PR base integration

Integrated the base branch's request setup timing change and the v0.4.6 release. To avoid duplicating SQLite 0077 used by the base branch, the unmerged affinity table/index migrations were moved to 0078/0079, and fresh SQLite initialization plus config/engine/storage/scheduler/server/admin affinity verification passed.

The final engine review found that the base guard preventing empty prompt-cache observations on aborted SSE had disappeared, and restored it. The real Lifecycle SSE regression test failed without the guard due to `observations=0, dropped_below_threshold=2, dropped_aborted=0` events, and passed after restoration. This fix prevents an abort-metric behavior change unrelated to affinity.
