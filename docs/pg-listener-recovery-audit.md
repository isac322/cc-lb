# PgListener Recovery Audit

This audit covers `PgListener::run` in `crates/cc-lb-core/src/pg_notify_fanout/listener.rs` after adding reconnect metrics for Task 2.4.

## Observable Failure Modes

### Initial connect failure

- Site: `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:65` calls `sqlx::postgres::PgListener::connect_with(&self.pg_pool)` at the top of the outer loop.
- Current behavior: on error, the listener records `sse_pg_listener_reconnects_total{reason="connect_failed"}` via `record_pg_listener_reconnect` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:68-70`, logs `warn` with `reason="connect_failed"` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:70`, then waits for a fixed 1 second before retrying at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:71-77`.
- Shutdown behavior: while sleeping in the retry branch, `shutdown_rx.changed()` races the sleep; a closed or true shutdown signal returns immediately at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:71-75`.
- Payload loss risk: any `pg_notify` payload emitted while no listener connection exists is lost by PostgreSQL NOTIFY semantics. The listener has no durable replay source for PG NOTIFY-only payloads.
- Missing metrics: resolved by `sse_pg_listener_reconnects_total{reason="connect_failed"}`.

### Subscribe failure after connect

- Site: after connect succeeds, `listener.listen(&self.channel).await` subscribes at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:80`.
- Current behavior: on error, the listener records `sse_pg_listener_reconnects_total{reason="subscribe_failed"}` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:81-82`, logs `warn` with `reason="subscribe_failed"` and `channel` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:83`, and immediately continues the outer loop at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:84`.
- Retry cadence: this branch has no explicit sleep. If connect succeeds and LISTEN fails repeatedly, it can reconnect in a tight loop bounded only by connect/listen latency.
- Shutdown behavior: there is no shutdown select around this immediate retry branch, so shutdown is observed at the next outer-loop connect attempt or at the following inner-loop select if subscribe later succeeds.
- Payload loss risk: because the listener is not subscribed, any payload emitted during this failure window is silently lost by PostgreSQL NOTIFY semantics.
- Missing metrics: resolved by `sse_pg_listener_reconnects_total{reason="subscribe_failed"}`. Log-rate limiting is still missing.

### Receive failure or transparent sqlx reconnect mid-stream

- Site: the inner loop awaits `listener.try_recv()` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:95` so sqlx's transparent reconnect signal is observable.
- Current behavior: successful notifications are parsed and published via `self.handle_payload(notification.payload()).await` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:97`. When sqlx detects a lost connection and successfully reconnects/resubscribes, `try_recv()` returns `Ok(None)`; the wrapper records `sse_pg_listener_reconnects_total{reason="recv_failed"}` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:98-101`, logs `warn` with `reason="recv_failed"` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:101`, and stays in the inner loop using the reconnected sqlx listener.
- Current error behavior: if `try_recv()` returns an actual error, the listener records the same `recv_failed` metric at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:103-105`, logs `warn` with `reason="recv_failed"` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:106`, breaks the inner loop at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:107`, and reconnects through the outer loop.
- Retry cadence: sqlx reconnects immediately before `Ok(None)` is returned. Actual receive errors break to the outer loop immediately; only subsequent connect failures take the fixed 1-second sleep.
- Shutdown behavior: normal shutdown is checked in the same inner `tokio::select!` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:88-93`. If a payload branch has already won the select, `handle_payload` runs to completion before shutdown is observed on the next iteration.
- Payload loss risk: payloads already returned by `try_recv()` are handled before the loop can observe shutdown. Payloads emitted while the underlying connection is down remain silently lost by PostgreSQL NOTIFY semantics, including the auto-reconnect window documented by sqlx.
- Missing metrics: resolved by `sse_pg_listener_reconnects_total{reason="recv_failed"}`.

### Shutdown signal during connect, subscribe, receive, or retry sleep

- Connect attempt: shutdown does not cancel an in-flight `connect_with` call at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:65`; it is observed only if connect fails and the retry-sleep select runs at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:71-75`, or after connect succeeds and the inner loop starts.
- Retry sleep: shutdown during the fixed 1-second retry sleep returns immediately through `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:71-75`.
- Subscribe attempt: shutdown does not cancel an in-flight `listen` call at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:80`.
- Receive loop: shutdown is handled directly in the inner select at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:88-93`.
- Payload loss risk: shutdown is intentionally terminal; already selected payloads are handled, but the listener does not drain additional queued PostgreSQL notifications after the shutdown branch wins.
- Missing metrics: no shutdown metric exists; that is acceptable because shutdown is expected operator lifecycle, not a reconnect failure.

## Silent Drops and Missing Metrics

- Silent drops remain possible for payloads emitted while the listener is disconnected or not yet subscribed. PostgreSQL NOTIFY is ephemeral, and `PgListener` has no storage replay path for plain NOTIFY payloads.
- The previous missing reconnect metric is fixed by `sse_pg_listener_reconnects_total{reason="connect_failed|subscribe_failed|recv_failed"}` in `crates/cc-lb-core/src/pg_notify_fanout/metrics.rs:27-28`.
- Subscribe-failure retry storms can still produce unbounded warning logs because that branch has no sleep and no log-rate limiter.

## Verified Invariants

- Payloads already received by the inner `try_recv()` branch are drained cleanly: `Ok(Some(notification))` is awaited through `handle_payload` at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:95-97`, and `handle_payload` publishes recognized updates at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:116-131` before the next shutdown check.
- Reconnect happens without process abort after repeated failures: the outer loop continues after connect failures at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:64-78`, after subscribe failures at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:80-85`, stays alive after sqlx-transparent recv reconnects at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:98-102`, and breaks/reconnects after actual recv errors at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:103-107`. `crates/cc-lb-core/tests/pg_listener_recovery.rs` exercises a backend termination recovery and repeated connect failures.
- Shutdown during connect-retry sleep returns immediately: the retry sleep and shutdown receiver are in the same select at `crates/cc-lb-core/src/pg_notify_fanout/listener.rs:71-75`, covered by `pg_listener_shuts_down_during_reconnect_sleep`.

## Recommendations

1. Keep the new `sse_pg_listener_reconnects_total{reason}` counter as the primary reconnect SLI and alert on reconnect storms. This task adds `LiveTailPgListenerReconnectStorm` at `rate(sse_pg_listener_reconnects_total[5m]) > 0.1` for 10 minutes.
2. Add exponential backoff with jitter for connect and subscribe failures in a follow-up. Do not change the current fixed 1-second connect retry inside this task.
3. Add log-rate limiting or periodic summarized warnings for reconnect storms, especially the subscribe-failure branch that currently retries without sleeping.
4. If zero payload loss across listener outages becomes a hard requirement, pair NOTIFY with durable storage replay or a monotonically ordered catch-up cursor. The current listener cannot recover NOTIFY payloads emitted while disconnected.
5. Consider wrapping in-flight `connect_with` and `listen` attempts in a shutdown-aware select with timeout if shutdown latency during network stalls becomes operationally visible.
