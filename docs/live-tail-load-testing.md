# Live-Tail Load Testing

The live-tail load harness exercises cc-lb against `fake-anthropic` only. Do not point it at the real Anthropic API. The script starts a local SQLite-backed cc-lb, seeds a mock upstream, runs proxy traffic, keeps admin SSE subscribers connected, scrapes `/metrics`, and writes evidence JSON.

## Profiles

- `smoke`: 20 rps for 90 seconds, 20% streaming requests, 1 SSE subscriber. CI-safe, about 3 minutes including builds.
- `soak`: 100 rps for 15 minutes, 20% streaming requests, 3 SSE subscribers, 30 second reconnect churn. Primary manual pre-merge gate.
- `burst`: 500 rps for 3 minutes, 10% streaming requests, 5 SSE subscribers, 15 second reconnect churn. Stresses the 500-row storage-tail page boundary.
- `leak`: 50 rps for 60 minutes, 20% streaming requests, 2 SSE subscribers, 60 second reconnect churn. Used for RSS drift checks.

Run the smoke profile:

```bash
bash tests/load/live-tail-soak.sh smoke
```

Run the manual primary gate:

```bash
bash tests/load/live-tail-soak.sh --profile soak
```

Useful overrides are `CC_LB_LOAD_RPS`, `CC_LB_LOAD_DURATION_SECS`, `CC_LB_LOAD_STREAM_RATIO`, `CC_LB_LOAD_MAX_IN_FLIGHT`, `CC_LB_LOAD_SSE_SUBSCRIBERS`, `CC_LB_LOAD_RECONNECT_CHURN_SECS`, and `CC_LB_LOAD_METRICS_SCRAPE_SECS`.

## Pass Criteria

- Proxy success rate must be at least 99.9%.
- Actual RPS must stay within 5% of the configured target.
- `sse_storage_tail_lag_ms` p95 must be at most 1000 ms and max at most 5000 ms when the storage-tail histogram is present.
- `cc_lb_dropped_events_total{reason=~"lifecycle_.*_full"}` must remain 0.
- `sse_reset_events_sent_total{reason=~"bus_lagged|storage_error"}` must remain 0. Reset frames tagged `backfill_cap` are recorded but do NOT fail the run — they are the healthy system response when a reconnecting SSE client is more than 500 events behind (aggressive reconnect churn or long disconnect). The client picks up from head plus a REST delta backfill per the live-tail redesign plan §3.9.
- RSS slope thresholds are profile-scaled to account for backend-driven RSS growth:
  - `smoke`: not enforced (90 second run — allocator warmup dominates).
  - `soak` (100 rps × 15 min): at most 3 MiB/min. Accounts for SQLite WAL/page cache growth at sustained write load.
  - `burst` (500 rps × 3 min): at most 5 MiB/min. Same rationale scaled to 5× throughput.
  - `leak` (50 rps × 60 min): at most 1 MiB/min. 60-minute run amortizes warmup and steady-state cache fill, so any residual slope is a real leak.
- Assembler in-flight partials must stay below 5000 at 100 rps or lower, and below 10000 for the 500 rps burst profile when the gauge is present.

## Evidence JSON

Evidence is written to `tests/load/evidence/live-tail-soak.<profile>.json`. Metric scrapes are written to `tests/load/evidence/live-tail-soak.<profile>.metrics.jsonl`. The schema is documented by `tests/load/evidence/live-tail-soak.example.json`.

Key fields:

- `proxy_success_count` and `proxy_failure_count`: proxy request outcome counts.
- `actual_rps`: measured request completion rate.
- `sse_events_received_per_subscriber`, `sse_messages_received_per_subscriber`, `sse_resets_received_per_subscriber`: per-dashboard-subscriber SSE observations.
- `metric_series`: min/p50/p95/max summaries by metric name.
- `metric_last_values`: final labeled Prometheus samples, including reset and drop counters.
- `rss_growth_mib` and `rss_slope_mib_per_min`: process RSS drift from `/proc/<pid>/status`.

## Failure Diagnosis

- High storage-tail lag: inspect storage read pressure, SQLite lock contention, and storage-tail poll cadence. Check `sse_storage_tail_backlog_rows` and `sse_storage_tail_polls_total` in the metrics JSONL.
- Reset events: inspect `sse_reset_events_sent_total{reason=...}`. `bus_lagged` points at broadcast receiver pressure, `storage_error` points at storage query/tail failures, and `backfill_cap` means reconnect churn generated more than the backfill cap.
- Dropped lifecycle events: inspect `cc_lb_dropped_events_total{reason=...}`. Any `lifecycle_*_full` reason indicates bounded queue pressure in the lifecycle pipeline.
- RSS drift: run the `leak` profile and compare `rss_slope_mib_per_min` after the first few minutes. If the slope stays positive, inspect SSE subscriber state and assembler partial retention.

## Prerequisites

- SQLite on a local filesystem.
- `jq` for ad-hoc evidence inspection.
- `promtool` for optional post-run Prometheus rule or exposition analysis.
- Rust release builds for `fake-anthropic`, `cc-lb-server`, and `cc-lb-loadgen`; the script builds them automatically.
