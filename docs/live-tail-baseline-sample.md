# Live-tail baseline summary

> **Baseline source**: single-operator personal-prod. Data reflects one
> cc-lb instance running on one operator's machine. **Do not**
> extrapolate p95/p99 values to fleet or aggregate production behavior.

- Window: `2026-07-05T07:06:44Z` — `2026-07-05T07:06:46Z`
- Sample count: 3
- Host: `macmini`
- cc-lb cmdline: `/home/bhyoo/.local/bin/cc-lb serve --config /home/bhyoo/.config/cc-lb/config.toml`

## Per-series values

| Series | Type | Samples | min | p50 | p95 | p99 | max | delta (last − first) |
|---|---|---|---|---|---|---|---|---|
| `cc_lb_dropped_events_total{reason="none"}` | counter | 3 | 0 | 0 | 0 | 0 | 0 | +0 |
| `cc_lb_dropped_events_total{reason="unknown"}` | counter | 3 | 0 | 0 | 0 | 0 | 0 | +0 |
| `cc_lb_lifecycle_assembler_rows_total{outcome="orphan_ttl_evicted"}` | counter | 3 | 17 | 17 | 17 | 17 | 17 | +0 |
| `cc_lb_lifecycle_assembler_rows_total{outcome="written"}` | counter | 3 | 50 | 50 | 50 | 50 | 50 | +0 |
| `sse_backfill_pages_total` | counter | 3 | 197 | 197 | 197 | 197 | 197 | +0 |
| `sse_backfill_rows_total` | counter | 3 | 14 | 14 | 14 | 14 | 14 | +0 |
| `sse_partials_published_total{trigger="request_started"}` | counter | 3 | 50 | 50 | 50 | 50 | 50 | +0 |
| `sse_partials_published_total{trigger="request_terminated"}` | counter | 3 | 50 | 50 | 50 | 50 | 50 | +0 |
| `sse_partials_published_total{trigger="route_completed"}` | counter | 3 | 50 | 50 | 50 | 50 | 50 | +0 |
| `sse_partials_published_total{trigger="stream_completed"}` | counter | 3 | 50 | 50 | 50 | 50 | 50 | +0 |
| `sse_partials_published_total{trigger="upstream_response_started"}` | counter | 3 | 50 | 50 | 50 | 50 | 50 | +0 |
| `sse_partials_published_total{trigger="usage_observed"}` | counter | 3 | 748 | 748 | 748 | 748 | 748 | +0 |
| `sse_partials_throttled_total` | counter | 3 | 64 | 64 | 64 | 64 | 64 | +0 |
| `sse_reconnects_total` | counter | 3 | 197 | 197 | 197 | 197 | 197 | +0 |

## Never-observed series

(Populated only after cross-referencing docs/metrics-live-tail.md — reserved for future extension.)

