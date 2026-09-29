# Subscription quota range QA

The subscription quota `/analysis` endpoint was removed on 2026-09-28. This document keeps only the range-series contract and its results.

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
- Keep the frontend series 30-second freshness; the query key uses a stable range identity instead of the absolute clock.

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

## API · browser QA

- [x] `/series` and `/aggregate` status and JSON schema are preserved.
- [x] Upstream Detail's Quota History line, latest, and loading/error/empty states are correct.
- [x] In the mock-API browser, verified the exact request range and correct chart render for each of the 1h/6h/24h/7d buttons.
- [x] SQLite/PostgreSQL storage·API transitions and the browser-layer series 30-second cadence each pass.
- [x] Existing screen during loading, empty transition, and successful recovery all display correctly.

## Performance QA

- [x] Prove with a work-count assertion that the walked bucket count is bounded by the requested range, not the anchor age.
- [x] On SQLite scratch, measure p50/p99 and RSS before/after for anchors 1 day/30 days/180 days old.
- [x] On PostgreSQL scratch, measure p50/p99 and buffers on the same data.
- [x] Do not pin absolute-time thresholds as permanent CI tests; record them in the measurement report.

## Execution results

- Series return work: 180-day anchor, 6 windows: `1,555,566` buckets → `366`.
- SQLite series p50/p99: `121.437/129.915ms` → `0.163/0.193ms`.
- PostgreSQL series p50/p99: `133.119/139.993ms` → `0.474/0.692ms`.
- SQLite provider-lots p50/p99: `3.647/3.856ms` → `0.091/0.109ms`.
- PostgreSQL provider-lots p50/p99: `3.114/3.395ms` → `0.218/0.289ms`.
- SQLite peak RSS: `560,758,784` bytes → `13,778,944` bytes.
- Storage conformance: SQLite `79/79`, PostgreSQL `64/64`.
- Admin: unit `19/19`, focused integration `15/15`, all `52 + 246` tests passed.
- Web: typecheck, Biome lint, Vitest `63 files / 573 tests` passed.
- Playwright: the mock-API browser spec (now `upstream-quota-series-cadence.spec.ts`)
  verified 1h/6h/24h/7d exact ranges, delayed loading, empty transition, successful
  recovery, and +60-second series refresh. The browser uses a mock API; real
  SQLite/PostgreSQL storage·API state transitions are verified by separate Rust
  conformance/integration tests.
- Independent review of the range change had 0 findings.
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
