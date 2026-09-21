# Quota · Principal usage read-path scalability investigation

Investigation date: 2026-09-07

## Scope

Investigated the following two read paths of the Admin UI.

1. Subscription quota series, analysis, provider-lots aggregate
2. Overview principal usage totals

Prompt-cache tokenization and its related observability are handled in separate work and are out of scope for this document.

## Investigation method

6 independent investigation agents split the analysis across server, frontend, PostgreSQL, SQLite, performance measurement, and counterexamples. 2 critical reviewers rebutted and cross-reviewed each conclusion, and a final judgment agent fixed the implementation scope and QA contract. Reproduced the key complexity and plan shapes with scratch SQLite/PostgreSQL benchmarks.

## Quota conclusions

Rejected the diagnosis that the production `/series` 442–509ms is a simple DB or JSON bottleneck. The network/API baseline is 210–230ms, and the DB plan was 12.253ms, 7,786 rows, temp I/O 0.

The actual defect is that the series builder generates buckets from the DB left anchor's `changed_at`, not the request `since`. Checkpoints remain for a long time, but the guardrail only checks the requested range, so work grows as `O((until-anchor)/bucket_secs)`. Carry-forward generates every intermediate bucket, and downsampling also targets data before the requested range, losing visible-range resolution. The same problem exists in the admin/public PostgreSQL·SQLite series and the provider-lots aggregate.

On the scratch public SQLite path, a recent 1-hour/60-second/6-window request should normally process 366 buckets, but with a 1-day-old anchor it processed 8,646 buckets/4.112ms, with a 30-day-old anchor 60,000/133.829ms, and with a 180-day-old anchor 60,000/749.784ms. Provider-lots 5 windows under the same conditions were 0.928/14.318/80.362ms.

`/analysis` does not put the selected-upstream condition in SQL; it reads every upstream/principal/model minute rollup and filters in Rust. The 6 windows repeatedly scan the whole vector and clone matching rows, then build dense buckets and discard the majority with `sample_count=0`. The frontend puts a timestamp that changes every 60 seconds into the query key, so the nominally 120-second-poll analysis is actually re-requested every 60 seconds.

### Decisions

- Preserve each source stream's left anchor as the current-state seed.
- The actual bucket loop starts at `floor(since / bucket_secs)`.
- Preserve the dense carry-forward over the whole requested range, reset/gap/source merge, and the exact anchor timestamp/source/value contract.
- Add an upstream-filtered rollup storage API dedicated to `/analysis`, and partition once per upstream and borrow.
- Series and analysis query identity use a stable `range_secs`, and each refetch computes the latest exact `since/until` when it starts.
- Do not use a new series index, a plain ETag, an endpoint mega-merge, or response field reduction as the fundamental fix.

## Principal usage conclusions

PR #698/#700 removed the payload/TOAST JSON decode, but the principal-cost SQL's `($4::uuid IS NULL OR upstream_id = $4)` optional predicate references `upstream_id`, which is not in the covering index. A custom plan can constant-fold `None`, but a prepared generic plan cannot avoid heap access. The `Some` path cannot currently be index-only under any plan.

On PostgreSQL 18 scratch with 100,000 rows, the auto generic optional predicate produced a Bitmap Heap Scan, 1,640 heap blocks, 37.45ms, while the static shape without the predicate produced an Index Only Scan, heap fetch 0, 11.93ms. Absolute times are not production numbers, but the index-only collapse from the generic-plan switch was reproduced.

`projection=totals` only shrinks the response. The server still computes the full rollups, dense principal buckets, and bucketed live-event cost, then collapses at the end. The Overview 5-second poll creates 720 identical-range aggregations per browser per hour with no cache/coalescing. The rollup checkpoint ETag does not reflect the live tail, so it is not used.

### Decisions

- Both PostgreSQL and SQLite select a static SQL shape based on the presence of an upstream filter.
- The canonical UUID exact branch runs only when there is a UUID selection set.
- The normalization branch runs exactly once over the entire selection key set, handling padded UUID, legacy, and unknown.
- Restore the PostgreSQL no-upstream generic plan to a covering index-only shape and keep it there.
- Handle the filtered path with an upstream-leading covering index without payload/TOAST.
- `projection=totals` creates only sparse buckets for the per-step live-tail guardrail and does not create a zero-filled dense series.
- Merge identical principal totals requests with in-flight single-flight and a short freshness-safe microcache. ETag stays disabled.

## Completion conditions

Each problem is submitted as an independent PR. Each PR must satisfy the PostgreSQL and SQLite point-in-time and state-transition contracts, API results, the related browser surfaces, before/after measurements, independent review, and a full GitHub CI pass.
