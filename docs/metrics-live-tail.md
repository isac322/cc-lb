# Dashboard Live-Tail Metrics

This is the canonical metric inventory for the dashboard live-tail redesign. Label values are backed by `cc_lb_engine::metrics_labels` where the label vocabulary is finite.

| Metric | Type | Labels | Description | Expected steady state |
| --- | --- | --- | --- | --- |
| `sse_partials_published_total` | Counter | `trigger=request_started|parse_completed|auth_completed|route_completed|upstream_response_started|usage_observed|stream_completed|request_terminated` | Memory-only partial request snapshots published by the lifecycle assembler; only final request events are persisted. | Increases with live request volume; `usage_observed` is throttled to about one update per event every 250 ms. |
| `sse_partials_throttled_total` | Counter | none | Usage partial emissions skipped by the 250 ms per-`event_id` throttle. | Non-zero during active streaming; growth should correlate with streaming token cadence. |
| `cc_lb_lifecycle_assembler_rows_total` | Counter | `outcome=written|written_orphan|written_after_grace|terminated_without_partial|orphan_ttl_evicted|cap_evicted` | Lifecycle assembler row outcomes, including orphan classification and map pressure evictions. | `written` dominates; orphan, TTL, and cap outcomes should be near zero. |
| `sse_backfill_pages_total` | Counter | none | SSE backfill pages queried during initial connect or reconnect. | Increases with dashboard reconnects. |
| `sse_backfill_rows_total` | Counter | none | Historical rows emitted through SSE backfill. | Usually low; spikes mean clients are reconnecting after gaps. |
| `sse_reset_events_sent_total` | Counter | `reason=backfill_cap|bus_lagged|storage_error` | Server reset frames sent to force client resynchronization. | Zero or near zero; any sustained increase needs investigation. |
| `sse_lagged_resync_total` | Counter | none | Legacy companion counter for broadcast lag resets. | Zero in normal operation. |
| `sse_reconnects_total` | Counter | none | Admin SSE stream connection attempts. | Tracks active dashboard reconnect cadence. |
| `sse_malformed_frames_total` | Counter | none | Server-side SSE message frames that could not be serialized. | Always zero. |
| `sse_storage_tail_polls_total` | Counter | none | Storage tail poll attempts by multi-instance pollers. | Increases every poll interval while PG notify transport is enabled. |
| `sse_storage_tail_lag_ms` | Histogram | none | Time between request event timestamp and storage-tail broadcast. | p95 should normally stay below 1000 ms. |
| `sse_storage_tail_backlog_rows` | Gauge | none | Rows found in the latest storage-tail poll. | Usually 0 between bursts; sustained high values mean poll interval or storage is lagging. |
| `sse_partial_notify_sent_total` | Counter | `outcome=sent|truncated_sent|failed` | PG NOTIFY partial publish outcomes. | `sent` dominates; `truncated_sent` only for large partial payloads; `failed` should be zero. |
| `sse_partial_notify_dropped_total` | Counter | `reason=queue_full|queue_closed|serialize_error|pg_error` | Partial updates dropped before or during PG NOTIFY publication. | Always zero. |
| `sse_pg_listener_reconnects_total` | Counter | `reason=connect_failed|subscribe_failed|recv_failed` | PG listener reconnect attempts after initial connection, LISTEN subscribe, or receive failures. | Always zero in steady state; sustained growth means the listener is in a reconnect storm or Postgres is unstable. |
| `sse_notify_http_fetches_total` | Counter | `outcome=success|not_found|unauthorized|timeout|network_error` | HTTP fallback fetch outcomes for truncated NOTIFY partial payloads. | Usually zero; `success` may rise with large partials; all failure outcomes should be zero. |
| `sse_notify_queue_usage_ratio` | Gauge | none | Value returned by `pg_notification_queue_usage()`. | Below 0.30 warning threshold; below 0.50 critical threshold. |
| `cc_lb_dropped_events_total` | Counter | `reason=lifecycle_assembler_full|sse_lagged|...` | Shared bounded-queue drop metric used by live-tail overflow alerts. | `reason="lifecycle_assembler_full"` must stay zero. |

## Label Taxonomy

- Reset reasons are `ResetReason::{BackfillCap, BusLagged, StorageError}`.
- Partial triggers are `PartialTrigger::{RequestStarted, ParseCompleted, AuthCompleted, RouteCompleted, UpstreamResponseStarted, UsageObserved, StreamCompleted, RequestTerminated}`. All eight partial snapshots are memory-only; only the final request event is durable.
- Notify drop reasons are `NotifyDropReason::{QueueFull, QueueClosed, SerializeError, PgError}`.
- PG listener reconnect reasons are `PgListenerReconnectReason::{ConnectFailed, SubscribeFailed, RecvFailed}`.
- Notify sent outcomes are `NotifySentOutcome::{Sent, TruncatedSent, Failed}`.
- Notify HTTP outcomes are `NotifyHttpOutcome::{Success, NotFound, Unauthorized, Timeout, NetworkError}`.
