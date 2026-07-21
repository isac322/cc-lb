# Overview Pooled Quota Fable Design

Date: 2026-07-21
Status: Approved

## Goal

Show the Fable pooled subscription quota on the Overview page when the selected dashboard time range contains persisted Fable pool-history data. Extend forward collection to persist Fable pooled snapshots, then backfill the retained historical Fable checkpoints into the pooled-history table.

## User-visible behavior

The Overview Pool quota card continues to show 5h and 7d unconditionally.

Fable appears as a third snapshot, legend item, and trend series only when the currently selected 1h, 6h, 24h, or 7d range contains at least one `7d_fable` pool-history point whose `utilization_percent` is not null. Changing the dashboard range recalculates visibility. Fable uses the existing pink quota color `#ec4899` and the existing `7d (Fable)` label vocabulary.

If the selected range has no persisted Fable utilization, the card does not reserve an empty Fable slot.

## Storage schema

Keep `pool_subscription_quota_history_v1` as the single pooled-history table.

Its current CHECK constraint excludes `7d_fable`, so both storage backends receive a migration that expands the allowed `quota_window` vocabulary.

SQLite follows the established table-rebuild pattern: create a replacement table with the expanded CHECK, copy every existing row, drop the old table, rename the replacement to `pool_subscription_quota_history_v1`, and recreate the index. The migration runs during service startup before normal writes resume.

PostgreSQL replaces the CHECK constraint in place. The migration sets `lock_timeout = '1s'`, adds the expanded constraint as `NOT VALID`, validates it while the old constraint still protects writes, then drops the old constraint in a metadata-only operation. Failure to acquire the final lock within one second aborts the migration instead of blocking traffic. The table is lean after the contributor JSON column removal, so no row rewrite is required.

Storage adapters and the `PoolQuotaHistoryStore` contract continue using the same table and require no routing split or v2 cutover.

## Forward collection

The pool-quota snapshot cron builds aggregate windows for 5h, 7d, and 7d Fable.

`pool_quota_snapshots_from_aggregate` accepts `SevenDayFable` in addition to the two existing pooled windows. The pool-history API parser accepts an explicit `7d_fable` request while retaining the existing 5h/7d defaults for callers that omit `windows`.

The Overview aggregate and pool-history requests explicitly include `7d_fable`.

## Overview data flow

The current aggregate response supplies the snapshot details shown by `PoolQuotaStackedBar`.

The selected-range pool-history response supplies the trend data and determines Fable visibility. A Fable history series is visible when it contains at least one point in the requested range with non-null utilization. When visible, the card renders:

- a Fable `PoolQuotaStackedBar` using the current aggregate window;
- a pink `7d_fable` area in `PoolQuotaThemedChart`;
- a Fable entry in `PoolQuotaLegend`;
- Fable values in chart maximum and latest-value calculations.

When the range contains no non-null Fable point, those elements are omitted while 5h and 7d remain unchanged.

## Historical backfill

The live SQLite database at `~/.local/share/cc-lb/storage.sqlite` was inspected read-only on 2026-07-21. It is healthy and in WAL mode. It contains 9,508 retained `7d_fable` checkpoints across seven upstreams from 2026-07-11 12:08:51.832 UTC through 2026-07-21 16:26:50.473 UTC, and no Fable rows in `pool_subscription_quota_history_v1`.

Backfill uses the existing 5h pooled snapshot timestamps within the retained Fable checkpoint range as the target time grid. This aligns historical Fable points with the cadence already used by the pooled chart instead of inventing a new timestamp sequence.

For each target timestamp, the backfill:

1. selects the latest Fable checkpoint at or before that timestamp for each upstream using the canonical `(changed_at_unix_millis, source, sample_id)` ordering;
2. resolves each upstream's effective plan tier and the tier's effective ratio at that timestamp from `upstream_plan_tier_history_v1` and `plan_tier_ratio_history_v1`;
3. computes `weighted_utilization_sum = Σ(utilization × ratio)`, `capacity_ratio_sum = Σ(ratio)`, and `utilization = weighted_utilization_sum / capacity_ratio_sum` for contributing upstreams;
4. records contributor, source, observation, and missing counts from the same selected rows;
5. upserts one `7d_fable` row at the target timestamp.

The operation is idempotent through the existing `(snapshot_at_unix_secs, quota_window)` primary key and upsert behavior. It backfills only timestamps for which at least one Fable utilization is available. It does not fabricate data before the first retained Fable checkpoint.

Historical replay preserves the plan-weighted utilization formula and effective-dated ratios. Bit-exact reconstruction of transient live-cache staleness is not required; the existing recompute ADR explicitly excludes that state. Backfilled rows use the historical storage path's fresh provider-lot semantics.

## Backfill safety and deployment sequence

1. Build and verify the implementation locally.
2. Create an online SQLite `.backup` of the live WAL database; never raw-copy it.
3. Apply the migration and backfill to the isolated copy.
4. Verify `PRAGMA quick_check`, preserved 5h/7d row counts, Fable row count and timestamp range, no duplicate primary keys, and independently recomputed sample timestamps.
5. Run the branch against the isolated database with recurring jobs and outbound traffic disabled; verify the Overview behavior in a real browser for ranges with and without Fable data.
6. Cut over the new binary so forward Fable snapshots begin writing.
7. Apply the same idempotent backfill transaction to the live database immediately after cutover.
8. Verify live database health, new Fable rows, ongoing cron writes, and range-dependent Overview visibility.

The live scheduler database is never copied into the QA clone, and the existing live service is not changed during clone verification.

## Error handling

An unknown pool-history window remains a 400 response. Migration lock acquisition fails quickly instead of blocking indefinitely. After clone validation, the backfill runs in one transaction; any error rolls back every inserted Fable row. Re-running after an unambiguous failure is safe because writes are idempotent.

A failed or ambiguous live write is inspected before retrying. No blind retry is allowed.

## Tests and verification

Implementation follows test-first development.

Backend coverage:

- the schema accepts `7d_fable` and preserves existing pooled rows;
- aggregate-to-snapshot conversion includes Fable;
- pool-history parsing accepts Fable and still rejects unsupported windows;
- the cron requests Fable;
- backfill computes weighted values with effective-dated tier ratios, handles source tie-breaking, omits pre-observation timestamps, and is idempotent;
- SQLite and PostgreSQL storage behavior remains equivalent.

Frontend coverage:

- Fable is hidden when the selected range has no non-null Fable history point;
- Fable appears when the selected range contains a non-null point;
- switching ranges updates visibility;
- chart data, latest values, maximum values, legend labels, colors, and snapshot layout include Fable only when visible;
- 5h and 7d never disappear.

Browser QA exercises the real Overview page at desktop and narrow viewports, changes all four time ranges, verifies visible and hidden Fable states, checks tooltip and legend text, and confirms no overflow or layout regression.
