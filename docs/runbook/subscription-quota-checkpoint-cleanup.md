# Subscription Quota Checkpoint Cleanup Runbook

Use this runbook only after the checkpoint-history build is installed and the service can be taken fully offline. The cleanup is intentionally destructive: it drops `upstream_subscription_quota_observations_v1` and its raw-history indexes only after the offline command validates raw/checkpoint parity, then runs SQLite `wal_checkpoint(TRUNCATE)` and `VACUUM` for file-size recovery.

This runbook does not add retention, TTL, source collapse, monotonic filtering, or a minute-primary history table. ADR 0007 remains the product decision source.

## Preconditions

- The target database is a local SQLite file, not an NFS or network filesystem path.
- The installed `cc-lb` binary includes `compact-subscription-quota-history --drop-raw-observations`.
- The cleanup command must be run with an explicit `--storage-path`; the destructive flag is rejected when the path is omitted, even if `CC_LB_STORAGE_PATH` or the default local path would resolve.
- A maintenance window is required where `cc-lb.service` is stopped for the whole backfill/drop/checkpoint/VACUUM sequence.
- Sufficient free disk space must be available for one full database backup plus temporary SQLite VACUUM work.

## Variables

```bash
DB="$HOME/.local/share/cc-lb/storage.sqlite"
BACKUP_DIR="$HOME/.local/share/cc-lb/backups"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP="$BACKUP_DIR/storage.sqlite.$STAMP.before-subscription-quota-checkpoint-cleanup"
REPORT="$BACKUP_DIR/subscription-quota-checkpoint-cleanup.$STAMP.json"
```

## Stop Service

```bash
systemctl --user stop cc-lb.service
systemctl --user is-active cc-lb.service
```

Expected: `inactive` or `failed` after the stop command. Do not continue while the service is still running.

## Backup Database

```bash
mkdir -p "$BACKUP_DIR"
sqlite3 "$DB" ".backup '$BACKUP'"
sqlite3 "$BACKUP" "PRAGMA integrity_check;"
```

Expected: backup integrity returns `ok`. Stop and rollback if the backup cannot be created or fails integrity check.

## Compact, Drop Raw History, Checkpoint, And VACUUM

```bash
cc-lb compact-subscription-quota-history \
  --storage-path "$DB" \
  --drop-raw-observations \
  > "$REPORT"
```

The command fails closed before dropping raw history if raw rows are malformed, checkpoint validation mismatches, the checkpoint table is unavailable, the backfill completion marker is malformed, or raw/checkpoint parity fails. A successful report records the backfill report, raw rows dropped, checkpoint/latest rows preserved, page and freelist counts, database size before/after, and `integrity_check: "ok"`.

## Verify Offline Cleanup

```bash
sqlite3 "$DB" "
SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='upstream_subscription_quota_observations_v1';
SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1;
SELECT COUNT(*) FROM upstream_subscription_quota_latest_v1;
SELECT value FROM meta_v1 WHERE key='subscription_quota_checkpoint_backfill_v1_complete';
SELECT value FROM meta_v1 WHERE key='subscription_quota_checkpoint_cleanup_v1_complete';
PRAGMA integrity_check;
"
```

Expected: raw table count is `0`, checkpoint count is greater than zero for databases that had raw history, latest rows remain present, both marker rows exist, and integrity check returns `ok`.

## Start Service

```bash
systemctl --user start cc-lb.service
systemctl --user is-active cc-lb.service
```

Expected: `active`.

## Health Check

Run the configured health endpoints for the deployment ports:

```bash
curl -fsS http://127.0.0.1:<proxy-port>/healthz
curl -fsS http://127.0.0.1:<proxy-port>/readyz
curl -fsS http://127.0.0.1:<admin-port>/admin/health
```

Then send one normal authenticated proxy request through the configured client path and confirm it succeeds. If health or traffic fails, collect `journalctl --user -u cc-lb.service --since "$STAMP" --no-pager` before rollback.

## Rollback

Rollback restores the pre-cleanup backup and therefore restores `upstream_subscription_quota_observations_v1`.

```bash
systemctl --user stop cc-lb.service
cp "$DB" "$DB.$STAMP.failed-cleanup"
cp "$BACKUP" "$DB"
sqlite3 "$DB" "PRAGMA integrity_check;"
systemctl --user start cc-lb.service
systemctl --user is-active cc-lb.service
```

Expected: integrity returns `ok` before restart and service state returns `active` after restart. Re-run the same health checks after rollback.
