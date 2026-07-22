# Fable pooled-quota SQLite backfill

One-time production procedure for populating historical `7d_fable` rows after the
release containing SQLite migration `0068_pool_quota_history_fable.sql` is
deployed. Run it from the same merged checkout as the deployed release.

The operator is SQLite-only. Without `--apply` it opens the database read-only.
With `--apply` it derives every row before writing, then inserts the historical
prefix in one `BEGIN IMMEDIATE` transaction. It never selects timestamps at or
after the earliest persisted `7d_fable` row, so post-deploy cron telemetry is
not overwritten. A failed apply rolls back the entire batch; a successful
rerun is a no-op.

## Preflight and backup

Use a local filesystem path; SQLite databases on NFS are unsupported.

```bash
cd /path/to/merged/cc-lb
export DB=/home/bhyoo/.local/share/cc-lb/storage.sqlite
export BACKUP="${DB}.before-fable-backfill-$(date -u +%Y%m%dT%H%M%SZ)"
export FABLE_BEFORE=$(sqlite3 -readonly "$DB" \
  "SELECT COUNT(*) FROM pool_subscription_quota_history_v1 WHERE quota_window='7d_fable'")
export LIVE_FABLE_START=$(sqlite3 -readonly "$DB" \
  "SELECT MIN(snapshot_at_unix_secs) FROM pool_subscription_quota_history_v1 WHERE quota_window='7d_fable'")
printf 'fable_before=%s live_fable_start=%s\n' "$FABLE_BEFORE" "${LIVE_FABLE_START:-none}"


sqlite3 -readonly "$DB" "
  PRAGMA journal_mode;
  PRAGMA quick_check;
  SELECT version, description, success
  FROM _sqlx_migrations
  WHERE version = 68;
  SELECT quota_window, COUNT(*), MIN(snapshot_at_unix_secs), MAX(snapshot_at_unix_secs)
  FROM pool_subscription_quota_history_v1
  GROUP BY quota_window
  ORDER BY quota_window;
"

df -h "$(dirname "$DB")"
sqlite3 "$DB" ".backup '$BACKUP'"
sqlite3 -readonly "$BACKUP" "PRAGMA quick_check;"
```

Stop if `journal_mode` is not `wal`, either `quick_check` is not `ok`, migration
68 is missing/unsuccessful, or disk space is insufficient for the backup.
Existing `7d_fable` rows are expected after deployment; record their count and
earliest timestamp. The operator treats that earliest timestamp as an immutable
live-data boundary. Also record the existing `5h` and `7d` counts and ranges.

## Dry run

```bash
python scripts/backfill-pool-quota-fable.py --database "$DB"
```

Expected output is exactly three summary lines:

```text
checkpoints=<N> upstreams=<N> target_timestamps=<N>
backfill_rows=<N> range_utc=<first>..<last>
mode=dry-run
```

For a first backfill, `checkpoints`, `target_timestamps`, and `backfill_rows`
should be non-zero. Zero rows means the historical prefix is already filled or
no eligible history exists; investigate before applying. The operator aligns
Fable history to existing `5h` snapshot timestamps inside the retained
checkpoint range, skips timestamps at/after the first live Fable row, uses the
latest non-null canonical checkpoint per upstream, and applies the effective
plan-tier ratio at each timestamp.

Confirm the dry run did not write:

```bash
test "$(sqlite3 -readonly "$DB" \
  "SELECT COUNT(*) FROM pool_subscription_quota_history_v1 WHERE quota_window='7d_fable'")" \
  = "$FABLE_BEFORE"
```

## Apply once

```bash
python scripts/backfill-pool-quota-fable.py --database "$DB" --apply
```

Do not interrupt the process. A busy database waits up to five seconds for the
single write transaction and then fails without partial rows.

## Verify

```bash
sqlite3 -readonly "$DB" "
  PRAGMA quick_check;

  SELECT quota_window, COUNT(*), MIN(snapshot_at_unix_secs), MAX(snapshot_at_unix_secs)
  FROM pool_subscription_quota_history_v1
  GROUP BY quota_window
  ORDER BY quota_window;

  SELECT 'duplicate_fable_keys', COUNT(*)
  FROM (
    SELECT snapshot_at_unix_secs
    FROM pool_subscription_quota_history_v1
    WHERE quota_window = '7d_fable'
    GROUP BY snapshot_at_unix_secs
    HAVING COUNT(*) > 1
  );

  SELECT 'rows_before_live_boundary', COUNT(*)
  FROM pool_subscription_quota_history_v1
  WHERE quota_window = '7d_fable'
    AND '$LIVE_FABLE_START' <> ''
    AND snapshot_at_unix_secs < CAST('$LIVE_FABLE_START' AS INTEGER);

  SELECT 'invalid_rows_before_live_boundary', COUNT(*)
  FROM pool_subscription_quota_history_v1
  WHERE quota_window = '7d_fable'
    AND '$LIVE_FABLE_START' <> ''
    AND snapshot_at_unix_secs < CAST('$LIVE_FABLE_START' AS INTEGER)
    AND (
      utilization IS NULL
      OR policy_version <> 1
      OR computed_at_unix_millis <> snapshot_at_unix_secs * 1000
    );
"
```

Pass criteria:

- `quick_check` is `ok`;
- the final `7d_fable` count is at least `FABLE_BEFORE + backfill_rows`
  (the live cron may append newer rows concurrently);
- `duplicate_fable_keys` is zero;
- when `LIVE_FABLE_START` was non-empty, `rows_before_live_boundary` equals
  `backfill_rows` and `invalid_rows_before_live_boundary` is zero;
- the recorded `5h` and `7d` counts and ranges are unchanged;
- the row at `LIVE_FABLE_START` still has its recorded live values;
- Overview shows Fable only in ranges containing persisted Fable rows.

## Failure and rollback

If apply exits non-zero, inspect the error and rerun the dry run first. The
transaction is rolled back, so no partial batch should exist. A later
`--apply` retry is idempotent.

If `LIVE_FABLE_START` was non-empty and the preflight confirmed that no
historical Fable rows existed before it, a data-only rollback can delete the
backfilled prefix without touching live cron rows:

```sql
BEGIN IMMEDIATE;
DELETE FROM pool_subscription_quota_history_v1
WHERE quota_window = '7d_fable'
  AND snapshot_at_unix_secs < <LIVE_FABLE_START>;
COMMIT;
```

Otherwise restore the SQLite backup during a maintenance window: stop the
cc-lb process, preserve the failed database for investigation, restore with
SQLite's `.restore` command rather than copying an active WAL file, verify
`PRAGMA quick_check`, then start cc-lb again. Do not restore while the service
has the database open.
