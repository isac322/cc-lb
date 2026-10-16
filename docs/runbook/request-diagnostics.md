# Request diagnostics and relational event storage

This runbook describes the current request-event storage and diagnostics log contract. Request events are stored in `request_events_v1` as typed relational columns: all 99 operational, latency-breakdown, and abnormal-stream-error scalar fields are columns, the `payload` column is dropped, and request-event reads incur zero TOAST overhead.

The five deep diagnostic fields (`cache_breakpoints`, `cache_control_message_indices`, `routing_trace`, `iterations`, `internal_errors`) are not stored in `request_events_v1`. The optional `observability.request_diagnostics_log` setting writes one `cc_lb.request_diagnostics.v1` JSON line to stdout for each request event that is stored. The line is emitted after the row insert commits and before the final event update is published. Cache-keepalive renewal rows whose lifecycle events were dropped or finalized after the assembler stopped may have a stored row without a line.

## Diagnostics line contents

The line carries these five deep diagnostic fields:

- `cache_breakpoints` (with `lookback_prefixes` omitted)
- `cache_control_message_indices`
- `routing_trace`
- `iterations`
- `internal_errors`

It also carries these self-contained outcome and session-correlation fields:

- `thread_id`
- `observed_session_id`
- `input_tokens`
- `output_tokens`
- `cache_read_input_tokens`
- `cache_creation_input_tokens`
- `duration_ms`
- `upstream_ttfb_ms`

When routing produced a prompt-cache match prediction, the line also includes `matched_v3_cache_key`, `breakpoint_content_block_index`, `matched_content_block_index`, `lookback_distance`, `predicted_cache_read_tokens`, `predicted_cache_creation_tokens_5m`, and `predicted_cache_creation_tokens_1h`. The line includes the event identifiers and routing context needed to correlate it with the relational row. Live lines use the `cc_lb.request_diagnostics.v1` schema and are compact JSON followed by a newline.

## Before enabling the log

- Route stdout records beginning with `{"schema":"cc_lb.request_diagnostics.v1",` to the diagnostics sink.
- Reassemble container-runtime partial records. Lines can exceed 16 KiB; Fluent Bit users should use `multiline.parser cri`.
- Keep the stdout collector healthy. Diagnostics share the stdout pipe and its in-process lock with tracing logs.
- Alert on increases in `cc_lb_dropped_events_total` for diagnostics queue, worker, or shutdown reasons.
- Keep the event retention window aligned with the sink's retention window. Rows older than `request_event_retention_days` are pruned.

## Delivery and loss boundaries

The diagnostics queue has a fixed capacity of 1,024 lines. A full queue, an encoding or write error, a worker failure, or a shutdown deadline can prevent a line from reaching stdout. These failures do not remove the committed relational row. A process killed with `SIGKILL` or by the OOM killer can lose lines still in the queue.

The lifecycle assembler and diagnostics writer drain during graceful shutdown. The server waits for shutdown hooks up to the configured cleanup bound; anything still unwritten after that bound is lost from stdout but remains governed by the request-event row's storage outcome.

## Reconciliation

Reconcile the diagnostics sink against committed storage by querying `SELECT (ts AT TIME ZONE 'UTC')::date AS day, COUNT(DISTINCT event_id) FROM request_events_v1 WHERE ts > '1970-01-01T00:00:00Z' GROUP BY 1` and comparing against distinct `event_id` counts per UTC day in the log sink. Keep the first ingested line for each `event_id`; a late terminal lifecycle event can produce a second line for a row that the database already accepted.

Admin event detail, delta, and stream reads use the typed relational columns directly and do not deserialize a JSON payload; they return the 99 relational fields and omit the five deep diagnostic fields (which live only in the diagnostics log).

## Schema and rollout checks

PostgreSQL migration `0139_request_events_relational_columns.sql` adds the relational columns, backfills any existing rows where `payload IS NOT NULL` in bounded 2,000-row batches that commit independently and mark progress by setting `payload = NULL`, and then drops `payload`. SQLite migration `0099_request_events_relational_columns.sql` performs the equivalent backfill and `DROP COLUMN payload`. On a large existing PostgreSQL deployment, operators may pre-populate the columns and pre-create `idx_cache_keepalive_decisions_ts` (`ON cache_keepalive_decisions (ts)`) and `idx_cache_keepalive_turns_ts` (`ON cache_keepalive_turns (ts)`) with `CREATE INDEX CONCURRENTLY IF NOT EXISTS` before rolling out `0139`–`0142` so startup migration work is a fast no-op. Migrations are forward-only, and an older binary that does not know the applied versions will fail startup with `VersionMissing`.

After rollout, verify that:

1. `request_events_v1` has the typed operational, latency, and abnormal-stream-error columns.
2. `request_events_v1.payload` is absent.
3. Any deployment config or Helm values file has removed the retired `request_event_strip_diagnostics` key.
4. The diagnostics log setting is enabled where `cc_lb.request_diagnostics.v1` lines are routed and monitored.
5. The sink's distinct `event_id` count matches committed rows by UTC day, with first-write semantics.
