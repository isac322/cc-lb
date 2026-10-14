# Request diagnostics log and payload strip

This runbook moves the five request-event diagnostic fields out of the database and into the stdout diagnostics log. The fields are `cache_breakpoints`, `cache_control_message_indices`, `routing_trace`, `iterations`, and `internal_errors`. Each step that changes a production database, deployment, or configuration needs approval under your change process before you run it.

The procedure has three phases:

1. Turn on `observability.request_diagnostics_log` and copy existing rows into the log sink with `cc-lb request-events export-diagnostics`.
2. After reconciliation proves the sink holds every row, turn on `request_event_strip_diagnostics` so new rows are stored without the fields.
3. Remove the fields from older rows with `cc-lb request-events strip-diagnostics`, then reclaim space.

The strip deletes the only database copy of the fields. Do not start it until the final reconciliation in step 5 passes.

Line format, delivery semantics, and metrics are described in the [observability guide](https://cc-lb.bhyoo.com/docs/operations/observability/#request-diagnostics-log). The configuration keys are in the [configuration reference](https://cc-lb.bhyoo.com/docs/reference/configuration/#request-diagnostics).

## Before you start

- The log agent must route stdout lines that start with `{"schema":"cc_lb.request_diagnostics.v1",` to the diagnostics sink, and must reassemble container-runtime partial lines (Fluent Bit: `multiline.parser cri`). Lines are often larger than 16 KiB, and a runtime splits those into partial records.
- Diagnostics lines share the stdout pipe and its in-process lock with tracing logs. A stalled log collector or full stdout pipe therefore also delays request-path tracing writes, which can slow requests. Keep the collector healthy before turning the log on, and watch `cc_lb_dropped_events_total{reason=~"request_diagnostics_.*"}`: a rising count is the early sign of a stalled stdout.
- The sink must answer "distinct `event_id` per UTC day of `ts_ms`, keeping the first ingested line per `event_id` and excluding `ts_ms = 0`". Reconciliation depends on this query.
- Rows older than `request_event_retention_days` (default 90) are pruned. Any day the backfill has not exported before its prune is lost to the sink, so plan the backfill to finish ahead of the oldest days' prune.
- On Postgres, check free WAL space. Step 6 rewrites every pre-flip row, and step 8 rebuilds indexes. Both produce a lot of WAL.

### The release migrations

The release that adds these settings also applies these migrations on startup:

| Backend | Migration | Effect |
| --- | --- | --- |
| Postgres | `0134_drop_request_events_thread_ts_index` | Drops `request_events_v1_thread_ts`; `request_events_v1_thread_list_order_idx` serves the thread filter and histogram. |
| Postgres | `0135_drop_quota_checkpoint_duplicate_indexes` | Drops `upstream_subscription_quota_checkpoints_range_idx` and `upstream_subscription_quota_checkpoints_anchor_idx`; the primary key serves both orders. |
| Postgres | `0136_request_events_payload_storage_main` | `ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE MAIN`. Catalog-only, no table rewrite. `0138` reverts it. |
| Postgres | `0137_drop_request_events_lower_model_index` | Drops `request_events_v1_lower_model_list_order_idx`; the planner serves the model filter from the list order index. |
| Postgres | `0138_request_events_payload_storage_extended` | `ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE EXTENDED`. Catalog-only, no table rewrite. |
| SQLite | `0097_drop_request_events_thread_ts_index` | Drops `request_events_v1_thread_ts`. |
| SQLite | `0098_drop_quota_checkpoint_duplicate_indexes` | Drops the two duplicate checkpoint indexes. |

Each Postgres migration sets `SET LOCAL lock_timeout = '1s'` and runs inside sqlx's migration transaction under its advisory lock. If a lock is not granted within one second, the migration fails, the transaction rolls back without recording the version, and the new pod fails to start. Old pods keep serving when the rollout keeps them until a new pod is ready (for example `maxUnavailable: 0`). Fix the lock holder and restart the pod.

After the migrations, `request_events_v1.payload` is back on the default `STORAGE EXTENDED`. A deployment that ran `0136` without `0138` kept new payloads inline in the meantime; those values stay inline until they are rewritten. Setting the column to `STORAGE MAIN` is an operator step in step 4, after the strip is on in every pod.

The migrations are one-way. Once they are applied, an older binary fails at startup with sqlx `VersionMissing`. Rollback is a forward fix only.

## 0. Pre-deploy (Postgres)

1. Optionally drop the indexes ahead of time without blocking writes. Run each statement on its own, outside a transaction, through your approved online path:

   ```sql
   DROP INDEX CONCURRENTLY IF EXISTS request_events_v1_thread_ts;
   DROP INDEX CONCURRENTLY IF EXISTS upstream_subscription_quota_checkpoints_range_idx;
   DROP INDEX CONCURRENTLY IF EXISTS upstream_subscription_quota_checkpoints_anchor_idx;
   DROP INDEX CONCURRENTLY IF EXISTS request_events_v1_lower_model_list_order_idx;
   ```

   The migrations then find nothing to drop and only record their versions.

2. Confirm the applied migration set:

   ```sql
   SELECT version, success FROM _sqlx_migrations ORDER BY version DESC LIMIT 5;
   ```

3. Confirm no vacuum is running on the affected tables:

   ```sql
   SELECT p.pid, c.relname, p.phase
   FROM pg_stat_progress_vacuum p
   JOIN pg_class c ON c.oid = p.relid
   WHERE c.relname IN ('request_events_v1', 'upstream_subscription_quota_checkpoints_v1');
   ```

   Do not run a manual `VACUUM` during the rollout. A manual or anti-wraparound vacuum does not yield to the 1 s `lock_timeout`, so new pods keep failing to start until it ends.

## 1. Deploy with the log on

Deploy the new release with `request_diagnostics_log = true` under `[observability]` and the strip setting off. With Helm, set the observability key in `config.extraToml` or through `env`:

```yaml
config:
  requestEventStripDiagnostics: false
  extraToml: |
    [observability]
    request_diagnostics_log = true
```

```yaml
env:
  - name: CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG
    value: "true"
```

If `extraToml` already has an `[observability]` table, add the key to that table; TOML rejects a second one. Wait until every pod runs the new release, then check that `cc_lb_request_diagnostics_lines_total{outcome="written"}` increases and that lines reach the sink.

## 2. Backfill existing rows

Record the cursor after the rollout completes. Every row above it was written by a pod with the log on.

```sql
-- Postgres
SELECT COALESCE(MAX(seq), 0) AS c_log FROM request_events_v1;
-- SQLite
SELECT COALESCE(MAX(id), 0) AS c_log FROM request_events_v1;
```

Run the export with the server image, the same configuration file, and the same storage credentials, for example as a Kubernetes Job whose stdout goes through the same log pipeline:

```bash
cc-lb request-events export-diagnostics --config /etc/cc-lb/cc-lb.toml --until-seq "$C_LOG"
```

The command reads the configuration with the same loader and validation as `cc-lb config validate`, including environment overrides. It never runs migrations, refuses a database whose applied migrations do not match the binary, and on SQLite refuses to create a missing database file. It does not start the proxy or admin listeners.

- stdout carries only `cc_lb.request_diagnostics.v1` lines with `"source":"backfill"`.
- stderr carries JSON progress: `{"kind":"export_progress","last_seq":N,"rows":R,"seq_ceiling":C}` after each page, then `{"kind":"export_summary","rows":R,"last_seq":N,"seq_ceiling":C,"resume_safe":true,"days":{"YYYY-MM-DD":count},"days_excluded_ts0":Z}`.
- Errors go to stderr as `{"error":"..."}`. Exit code 2 means a configuration, argument, or schema problem, or a missing SQLite file; exit code 1 means any other failure.

Options:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--from-ts RFC3339` | none | Inclusive lower bound on the event timestamp. |
| `--to-ts RFC3339` | none | Exclusive upper bound on the event timestamp. |
| `--after-seq N` | `0` | Export rows with a cursor above `N`. Use the last `last_seq` to resume. |
| `--until-seq N` | stable ceiling | Export rows with a cursor up to and including `N`, clamped to the stable cursor ceiling. |
| `--ceiling-wait-secs N` | `60` | How long to wait for in-flight inserts to settle while computing the stable ceiling, 1 to 3600. A timeout exits 1. |
| `--page-size N` | `500` | Rows per storage page, 1 to 500. |
| `--max-rows-per-sec N` | `500` | Average export rate, at least 1. |
| `--count-only` | off | Print per-UTC-day stored row counts instead of exporting. Counts follow only `--from-ts`/`--to-ts`; combining it with `--after-seq` or `--until-seq` is rejected with a usage error (exit 2). |

Export the days closest to their prune first. Run one Job per time range, oldest first:

```bash
cc-lb request-events export-diagnostics --config /etc/cc-lb/cc-lb.toml \
  --until-seq "$C_LOG" --from-ts 2026-07-23T00:00:00Z --to-ts 2026-07-30T00:00:00Z
```

If a Job stops, rerun it with `--after-seq` set to the last `last_seq` it reported. A line exported twice is harmless, because the sink keeps the first line per `event_id`.

On Postgres a row's cursor is assigned before its transaction commits, so a lower cursor can become visible after a higher one. At start the export computes `seq_ceiling`, the highest cursor below which no row can still commit, and never reads past it, even when `--until-seq` or a `--from-ts`/`--to-ts` window reaches higher. The reported `last_seq` is therefore at or below `seq_ceiling`, and resuming from it cannot skip a row that committed late; `export_summary` reports this as `"resume_safe":true`. It is `false` only when you passed an `--after-seq` above the ceiling, in which case `last_seq` repeats that value unchanged. Rows above the ceiling are picked up by the next run. If `seq_ceiling` is below `C_LOG`, the run stopped short of `C_LOG`; rerun with `--after-seq` set to `last_seq` until `last_seq` reaches `C_LOG`.

## 3. Reconcile

Compare, for each UTC day, the sink's distinct `event_id` count with the database count:

```bash
cc-lb request-events export-diagnostics --config /etc/cc-lb/cc-lb.toml --count-only \
  --from-ts 2026-07-23T00:00:00Z --to-ts 2026-10-13T00:00:00Z
```

`--count-only` prints one line to stdout, `{"kind":"day_counts","days":{"YYYY-MM-DD":count}}`. It counts rows with a timestamp of at least 1 ms, so the sink side must also exclude `ts_ms = 0`. Compare only complete days, and skip the oldest day if the prune is already removing its rows.

For each day that does not match, export it again by time range (step 2). Some rows legitimately have no live line, such as keepalive renewal rows finalized after the assembler shut down; their diagnostic fields are always empty and a re-export fills the count. Keep reconciling live days for the period your team agreed on, and require zero missing rows before you continue.

## 4. Turn on the strip

Set `request_event_strip_diagnostics = true` (Helm: `config.requestEventStripDiagnostics: true`) and roll out. Validation rejects this setting unless `observability.request_diagnostics_log = true`. Admin API reads of new rows then omit the five fields; the diagnostics log is the only copy.

After every pod runs with the strip on, record the flip cursor with the step 2 query. This is `C_FLIP`. Rows above it are already stored without the fields.

With the strip on, the assembler waits up to 500 ms for queue space before it drops a line. A line dropped while the strip is on is lost for good. Alert on any increase of `cc_lb_dropped_events_total{reason=~"request_diagnostics_strip_.*"}`.

### Set payload storage to MAIN (Postgres)

After `C_FLIP` is recorded and before step 6, switch `request_events_v1.payload` to `STORAGE MAIN`. New and rewritten payloads are then compressed and kept inline in the heap, and move to TOAST only when a value still exceeds about 8 KB after compression. Stripped payloads written from then on create no TOAST data, and the step 6 rewrite moves the payloads it strips out of TOAST. Stripped rows written before this change, while the column was still `EXTENDED`, may already sit in TOAST; step 6 skips them because they carry no diagnostic fields, so step 7 moves them separately.

Do not apply it while any pod still has the strip off. Under `STORAGE MAIN`, full payloads that compress to 2 to 8 KB also stay inline. In a benchmark this grew the heap about ninefold and made histogram queries about five times slower, because every scan of the heap reads those payloads.

The change is catalog-only: it rewrites no rows, and existing values keep their current storage until they are updated. It takes a brief `ACCESS EXCLUSIVE` lock on `request_events_v1`, so run it with a short lock timeout and retry until it succeeds:

```sql
SET lock_timeout = '1s';
ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE MAIN;
```

Confirm it with `SELECT attstorage FROM pg_attribute WHERE attrelid = 'request_events_v1'::regclass AND attname = 'payload';`, which returns `m`. SQLite has no equivalent setting; skip this step there.

## 5. Final reconcile (hard gate)

Reconcile every day that contains rows with a cursor at or below `C_FLIP`, as in step 3. Do not continue until every day matches. Step 6 destroys the database copy.

## 6. Strip existing rows

Start with a dry run. It reads the same rows and reports the same counters without updating anything:

```bash
cc-lb request-events strip-diagnostics --config /etc/cc-lb/cc-lb.toml --until-seq "$C_FLIP" --dry-run
```

Then run it for real with throttling:

```bash
cc-lb request-events strip-diagnostics --config /etc/cc-lb/cc-lb.toml --until-seq "$C_FLIP" \
  --batch-size 500 --sleep-ms 200 --max-rows-per-sec 1000 --max-wal-bytes 2147483648
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--until-seq N` | required | Last cursor to strip, inclusive. Pass `C_FLIP`. |
| `--after-seq N` | `0` | Strip rows with a cursor above `N`. Use the last `last_seq` to resume. |
| `--batch-size N` | `500` | Rows per batch, 1 to 1000. |
| `--sleep-ms N` | `200` | Pause between batches, in milliseconds. |
| `--max-rows-per-sec N` | `1000` | Average row rate, at least 1. |
| `--max-wal-bytes N` | none | Postgres only; rejected on SQLite. Before each batch, pause in 5 s steps while `SUM(size) FROM pg_ls_waldir()` exceeds `N`. Needs `pg_monitor` or superuser; the command checks access before its first write and fails if it is denied. |
| `--dry-run` | off | Report the counters without updating any row. |

After each batch, stderr gets `{"kind":"strip_progress","last_seq":N,"scanned":S,"rewritten":W,"bytes_before":B0,"bytes_after":B1}` with running totals. `bytes_before` and `bytes_after` are raw payload byte lengths. While the WAL limit holds the run, stderr gets `{"kind":"wal_pause","wal_bytes":N,"max_wal_bytes":N}`.

Expect the following on Postgres. The figures come from a 50,000-row synthetic test; measure your own deployment before relying on them.

- Each rewritten row adds about 2 KB of heap, because the stripped payload is stored inline. For 1.5 million rows that is about 3 GB.
- Most updates are not HOT, because densely packed old pages have no free space. Every index on `request_events_v1` gains an entry per row.
- WAL is roughly the new heap bytes plus index entries plus full-page images, about 3 to 4 KB per row. Set `--max-wal-bytes` well below your free WAL space, or lower `--max-rows-per-sec`.
- Old TOAST values of the rows it rewrites are deleted, but their space is not returned until step 7. Rows it skips because they carry no diagnostic fields keep their TOAST values; step 7 covers them.

## 7. VACUUM (Postgres)

Record the TOAST relation size, then vacuum outside your backup window:

```sql
SELECT pg_size_pretty(pg_relation_size(reltoastrelid)) FROM pg_class WHERE relname = 'request_events_v1';
VACUUM (VERBOSE) request_events_v1;
```

VACUUM truncates the TOAST relation only down to its highest page that still holds a live chunk. Disk comes back in full only when no live TOAST chunk remains. In testing, stripping every row truncated the TOAST relation to zero, while leaving the newest 20% unstripped truncated nothing. Rows written already stripped before the `STORAGE MAIN` change in step 4 can still hold TOAST chunks, so expect them here.

If the size did not drop, find the rows that still hold chunks (Postgres 16 or later; this is a full scan):

```sql
SELECT seq FROM request_events_v1 WHERE pg_column_toast_chunk_id(payload) IS NOT NULL LIMIT 100;
```

These are payloads that exceed about 8 KB even after compression, rows written already stripped while the column was still `EXTENDED` (before the step 4 `STORAGE MAIN` change), or rows written above `C_FLIP` by a pod that still had the strip off, which step 4 prevents. Strip any that still carry diagnostics with `--after-seq` and `--until-seq`; the strip skips rows without diagnostic fields, so it does not move the already-stripped ones. Accept the rest, or move them inline with an approved update such as `UPDATE request_events_v1 SET payload = payload || ''::bytea WHERE seq = ANY(...)`, in small batches. Then vacuum again.

Truncation takes a brief `ACCESS EXCLUSIVE` lock on the TOAST relation, which standbys replay. The heap does not shrink; it grew in step 6.

On SQLite, freed pages go to the free list and are reused. `VACUUM` rebuilds the file to return them to the filesystem, but it needs free disk about the size of the database and locks the database while it runs, so plan a maintenance window.

## 8. REINDEX (Postgres)

Compare index sizes before and after step 6:

```sql
SELECT indexrelid::regclass AS index, pg_size_pretty(pg_relation_size(indexrelid))
FROM pg_index WHERE indrelid = 'request_events_v1'::regclass
ORDER BY pg_relation_size(indexrelid) DESC;
```

Rebuild bloated indexes one at a time with `REINDEX INDEX CONCURRENTLY <name>;` through your operator path, never through a migration. Let WAL drain between rebuilds.

## 9. Rollback

- Strip: set `request_event_strip_diagnostics = false` and roll out. New rows store the fields again. Rows already stripped stay stripped; their fields exist only in the sink. On Postgres, first revert the payload storage with the same lock timeout and retry, `ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE EXTENDED;`, so full payloads go to TOAST again. Values already stored inline stay inline until they are rewritten.
- Log: set `request_diagnostics_log = false` only after the strip is off, or validation rejects the configuration.
- Binary: once the migrations are applied, an older binary fails at startup with `VersionMissing`. Fix forward.
