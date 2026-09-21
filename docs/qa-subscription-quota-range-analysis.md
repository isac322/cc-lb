# Subscription quota range · analysis QA

Written: before implementation

Consensus by: 6 independent investigation agents, 2 critical reviewers, 1 final judgment agent

## Invariant contract

- Anchors older than the requested range are used only as the current-state seed.
- Bucket generation runs from `floor(since / bucket_secs)` to `until`.
- The 1-hour/60-second inclusive contract is 61 dense carry-forward points per window.
- Anchor-only points also keep `sample_count=0`, `observed=true`.
- Preserve the left anchor's exact timestamp, source, and value.
- Reset, gap, source merge, tie-breaking, and downsampling results preserve the existing requested-range semantics.
- PostgreSQL and SQLite return identical results.
- `/analysis` reads only the requested upstreams from the storage query.
- Keep the frontend series 30-second freshness and the analysis 120-second poll; both query keys use a stable range identity instead of the absolute clock.
- `/analysis` rejects request ranges exceeding 20,000 fixed 60-second buckets with `bucket_range_too_large` before the storage query.

## Storage · domain QA

- [x] SQLite public series: with anchors 1 day/30 days/180 days old, the output point count depends only on the requested range.
- [x] PostgreSQL public series: the same input returns the same points/timestamps/sources/values as SQLite.
- [x] Admin slim series: even when the anchor precedes `since`, returns all 61 dense points of the requested range.
- [x] Provider-lots aggregate: regardless of anchor age, loop work is bounded by the requested range.
- [x] When there is no anchor, the existing empty/unknown result is preserved.
- [x] When the anchor exactly equals `since`, no duplicate point is produced.
- [x] When the anchor is just before `since`, the first bucket has the correct carried state.
- [x] After a reset inside the range, previous values are not carried forward.
- [x] Same-timestamp ties with changing sources keep the existing precedence.
- [x] Long-gap and missing-observation semantics are preserved.
- [x] Downsampling targets only requested-range points and preserves the first point, last point, and existing uniform index selection.

## Analysis QA

- [x] The new storage API uses a non-empty upstream ID set as a mandatory SQL predicate.
- [x] The PostgreSQL plan has a shape that can use the `(upstream_id, resolution, bucket_start)` index.
- [x] The SQLite query also applies the upstream filter and time range in SQL.
- [x] Rollups for multiple upstreams are partitioned once per upstream.
- [x] Per-window computation does not deep-clone the original rows.
- [x] Utilization, deficit, observation counts, and sources in the analysis response keep their existing meaning.
- [x] After changing backend state, values refresh on the next analysis refresh.
- [x] `/analysis` allows exactly a 20,000-bucket range and returns HTTP 400 from 20,001.
- [x] The UI-supported ranges 1h/6h/24h/7d do not hit the guardrail.

## API · browser QA

- [x] `/series`, `/analysis`, `/aggregate` status and JSON schema are preserved.
- [x] Upstream Detail's Quota History line, latest, deficit, and loading/error/empty states are correct.
- [x] In the mock-API browser, verified the exact request range and correct chart render for each of the 1h/6h/24h/7d buttons.
- [x] SQLite/PostgreSQL storage·API transitions and the browser-layer series 30-second/analysis 120-second cadence each pass.
- [x] Under a virtual clock, there is no analysis request at +60 seconds, and at +120 seconds it refetches with the latest exact bounds.
- [x] Existing screen during loading, empty transition, previous data during analysis error, and successful recovery all display correctly.

## Performance QA

- [x] Prove with a work-count assertion that the walked bucket count is bounded by the requested range, not the anchor age.
- [x] On SQLite scratch, measure p50/p99 and RSS before/after for anchors 1 day/30 days/180 days old.
- [x] On PostgreSQL scratch, measure p50/p99 and buffers on the same data.
- [x] The `/analysis` filtered query's rows and buffers are proportional to the selected upstreams, not the total rollup count.
- [x] Do not pin absolute-time thresholds as permanent CI tests; record them in the measurement report.

## Execution results

- Series return work: 180-day anchor, 6 windows: `1,555,566` buckets → `366`.
- SQLite series p50/p99: `121.437/129.915ms` → `0.163/0.193ms`.
- PostgreSQL series p50/p99: `133.119/139.993ms` → `0.474/0.692ms`.
- SQLite provider-lots p50/p99: `3.647/3.856ms` → `0.091/0.109ms`.
- PostgreSQL provider-lots p50/p99: `3.114/3.395ms` → `0.218/0.289ms`.
- SQLite peak RSS: `560,758,784` bytes → `13,778,944` bytes.
- PostgreSQL analysis scratch: all upstreams `100,000 rows / 29.103ms / 101,087 shared hits` →
  selected upstream `1,000 rows / 0.937ms / 2,006 shared hits`.
- Storage conformance: SQLite `79/79`, PostgreSQL `64/64`.
- Admin: unit `19/19`, focused integration `15/15`, all `52 + 246` tests passed.
- Web: typecheck, Biome lint, Vitest `63 files / 573 tests` passed.
- Playwright: `upstream-quota-analysis-cadence.spec.ts` 6 browser tests passed.
  Verified 1h/6h/24h/7d exact ranges, delayed loading, empty transition, previous data
  during analysis error, successful recovery, +60-second series-only refresh, +120-second
  latest exact-bounds analysis refresh, and deficit/caveat DOM changes. The browser uses a
  mock API; real SQLite/PostgreSQL storage·API state transitions are verified by separate
  Rust conformance/integration tests.
- Independent reviews: both the range and analysis reviews had 0 findings.
- GitHub Actions: CI, Web, and Publish-check all passed on rebased feature head
  `66be8150fc6c`. Verified with the remote sccache backend 500 handled by uncached
  fallback and the inherited wall-clock heartbeat test race fixed by a deterministic
  rendezvous.

## Exit gate

- [x] Related Rust unit/integration/conformance tests pass
- [x] SQLite QA pass
- [x] PostgreSQL QA pass
- [x] Web typecheck/test pass
- [x] Mock-API real browser point/state transitions and both backends' storage/API transitions pass per layer
- [x] Independent code review: 0 findings
- [x] Full GitHub CI pass
