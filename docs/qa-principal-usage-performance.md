# Principal usage totals · polling QA

Written: before implementation

Consensus by: 6 independent investigation agents, 2 critical reviewers, 1 final judgment agent

Technical-review correction during implementation: the live-tail component guardrail
verifies `event total <= rollup virtual cost` per step bucket. If event cost were merged
into a single bucket over the whole range, one un-rolled-up tail would remove valid
components from the past, breaking value equality. Therefore, to preserve the consensus's
top-level condition of no behavior change, event-cost bucket granularity is kept and only
zero-filled dense series materialization is removed.

## Invariant contract

- Per-principal total cost and component breakdown values are identical to the existing full-series collapse.
- Preserve UUID and legacy/non-UUID principal normalization and merge results.
- The presence or absence of an upstream filter does not change the result row set or cost totals.
- Preserve negative/NULL cost handling and the `component_costs_recorded` determination.
- SQLite and PostgreSQL return identical results.
- Reflect the live request-event tail immediately; ETag stays disabled.
- Merging identical requests does not change response values, errors, or cancellation semantics.

## SQL shape · plan QA

- [x] The PostgreSQL no-upstream query has no nullable upstream predicate.
- [x] The PostgreSQL filtered query uses a separate static SQL shape.
- [x] SQLite also selects a static SQL shape based on the presence of an upstream filter.
- [x] When the UUID selection set is empty, the UUID branch is not executed.
- [x] The normalization branch runs exactly once per request over the entire selection key set, handling padded UUID, legacy, and unknown together.
- [x] Mixed UUID/non-UUID input merges the two results under the existing rules.
- [x] The PostgreSQL prepared generic no-upstream plan is a covering index-only shape and can achieve heap fetch 0.
- [x] The PostgreSQL filtered plan uses index-level cost columns without reading payload/TOAST.
- [x] Totals for NULL, negative, and partially recorded component values are identical to the existing SQL.

## Totals projection QA

- [x] `projection=totals` does not generate a dense principal series.
- [x] The totals path creates only sparse buckets for the per-step live-tail guardrail and does not generate a zero-filled dense series.
- [x] On the same fixture, the totals projection is field-by-field identical to collapsing the full projection.
- [x] Preserve the rollup + live event overlap/exclusion rules.
- [x] Empty principal selection and ranges with no data keep the existing empty results.
- [x] Upstream-filtered totals are also identical to the full projection collapse.

## Coalescing · freshness QA

- [x] Identical queries started concurrently perform only one backend computation.
- [x] If any query parameter differs, the computation is not shared.
- [x] On leader success, all waiters receive the identical result.
- [x] On leader error, waiters do not hang and receive the same error semantics.
- [x] Waiter cancellation does not cancel the leader computation.
- [x] Identical requests within the maximum 1–3 second microcache are reused.
- [x] After the TTL, a new computation is always performed.
- [x] New live request events are reflected in responses after the allowed microcache bound.
- [x] ETag continues to not be issued for principal grouping.

## API · browser QA

- [x] `/admin/usage?...group_by=principal&projection=totals` JSON schema and values are preserved.
- [x] In the mock-API browser, verified the Overview Top principals total and Input/Output/Cache 5m/Cache 1h/Cache read values.
- [x] Values increase after the allowed TTL on both the Rust cache/API transition and the browser 5-second poll consumer transition.
- [x] Changing the principal or upstream filter does not bleed into other query caches.
- [x] Browser loading, empty, initial error, and next-poll recovery states are correct.

## Performance QA

- [x] On PostgreSQL scratch, measure the optional-predicate before and the static no-upstream shape after on the same data.
- [x] After repeated runs with `plan_cache_mode=auto`, record the generic plan's Index Only Scan, Heap Fetches, and buffers.
- [x] Record the filtered path's plan and buffers separately.
- [x] On SQLite and PostgreSQL, measure sparse direct totals p50/p99 versus the full collapse.
- [x] With 50 concurrent identical requests, measure the reduction in backend computation count and wall time. The 50-way result covers the 10-way requirement.
- [x] Do not pin absolute-time thresholds as permanent CI tests; record them in the measurement report.

## Execution results

- PostgreSQL 150,000-row scratch, no-upstream generic shape:
  `74.330ms / Bitmap Heap Scan / 7,893 heap blocks` →
  `18.112ms / Index Only Scan / heap fetch 0 under isolated VACUUM`.
- PostgreSQL filtered UUID:
  `25.846ms / heap access` → `0.330ms / Index Only Scan / heap fetch 0`.
- Fixed normalized no-upstream/filtered: `0.868ms / 0.329ms`, both Index Only Scan.
- SQLite API, 8,000 events: cold p50 `25.468ms` → `17.634ms`,
  hot p50 `73.872ms` → `0.019ms`,
  50-way wall `2,536.515ms` → `21.091ms`.
- PostgreSQL API, 8,000 events: first cold `17.091ms` → `15.731ms`;
  cold p50 `17.233ms` → `22.009ms` — not improved on this fixture.
  Hot p50 `15.234ms` → `0.019ms`, 50-way wall `112.036ms` → `25.851ms`.
- All before/after API responses were the same `12,490` bytes, and the 50-way responses were also byte-equal.
- Cache tests: 50 waiters built once, TTL expiry, error fan-out/retry, waiter cancellation,
  leader panic recovery, storage/query-key isolation — all passed.
- Admin: unit `49/49`, integration `253/253`.
- Storage conformance: SQLite `78/78`, PostgreSQL `63/63`.
- The shared principal-cost conformance verifies, on both SQLite and PostgreSQL:
  initial append→query, follow-up append→requery delta on the same upstream, and
  no leakage to a different upstream.
- Web: typecheck, Biome lint, Vitest `60 files / 555 tests` passed.
- Playwright: `overview-principal-totals-refresh.spec.ts` 3 browser tests passed.
  Verified loading→initial values→5-second auto refresh, empty totals, recovery on the
  next poll after two 500s, and the total plus 5 component DOMs and totals request params.
  The browser uses a mock API; server cache/storage transitions are verified separately
  by Rust tests.
- Replaced the inherited prompt-cache heartbeat test's 50ms wall-clock race with a
  test-only rendezvous; the exact test passed a final `50/50` stress run.
- Independent SQL and cache/totals reviews: 0 findings.
- The PostgreSQL permanent plan test does not pin visibility-map numbers that vary with
  parallel snapshots; it verifies Index Only Scan for the four production SQL shapes.
  Heap fetch 0 was confirmed via isolated scratch VACUUM measurement.
- GitHub Actions: full CI passed on feature head `35d050373a23`. The inherited heartbeat
  test race exposed on the later doc-only head was fixed with deterministic test
  synchronization.

## Exit gate

- [x] Related Rust unit/integration/conformance tests pass
- [x] SQLite QA pass
- [x] PostgreSQL QA pass
- [x] Admin API point-in-time and state-transition QA pass
- [x] Mock-API real Overview browser transition and both backends' storage/API transitions pass per layer
- [x] Independent code review: 0 findings
- [x] Full GitHub CI pass
