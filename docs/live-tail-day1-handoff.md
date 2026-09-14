# cc-lb live-tail Day-1 handoff

Operator checklist for the first 72 hours after deploying the dashboard live-tail redesign. Complete every item before handing observability off to the on-call rotation.

## 1. Verify metrics ingestion

- Prometheus target list includes every cc-lb instance (`up{job="cc-lb"} == 1` for each).
- Every live-tail metric documented in [docs/metrics-live-tail.md](metrics-live-tail.md) has at least one successful scrape (or a documented reason for staying at zero — e.g. `sse_partial_notify_sent_total` will be absent on sqlite deployments, where the in-memory event bus replaces pg_notify fanout).

Quick check:

```bash
# From an environment that can reach Prometheus:
curl -s "$PROM_URL/api/v1/query" --data-urlencode 'query=up{job="cc-lb"}' | jq '.data.result'

# List every live-tail counter/gauge on a specific instance:
curl -s http://<cc-lb-metrics-host>:52253/metrics | grep -E '^(sse_|cc_lb_lifecycle_assembler_rows_total|cc_lb_dropped_events_total)'
```

## 2. Validate alert rules

The shipped rules live at [deploy/alerts/live-tail.yml](../deploy/alerts/live-tail.yml). CI already runs `promtool check rules` on this file as part of [`static-checks`](../.github/workflows/ci.yml), so syntax is guaranteed at merge time. Before rollout to production Prometheus:

1. Load the file into your Prometheus rule_files list (see [deploy/prometheus/cc-lb-scrape.yml](../deploy/prometheus/cc-lb-scrape.yml) for a reference scrape config).
2. Reload Prometheus (`SIGHUP` or `POST /-/reload`).
3. Confirm every rule appears under `/rules` in the Prometheus UI with `state: ok`.
4. In Alertmanager, verify routing pipelines exist for `severity=warning` and `severity=critical`.

Currently shipped rules (post-fix):

| Alert | Severity | Trigger | Fixed in |
| --- | --- | --- | --- |
| LiveTailNotifyQueueUsageHigh | warning | `sse_notify_queue_usage_ratio > 0.30 for 5m` | initial ship |
| LiveTailNotifyQueueUsageCritical | critical | `sse_notify_queue_usage_ratio > 0.50 for 2m` | Task 0.0 (was duplicate name) |
| LiveTailBackfillSpike | warning | `rate(sse_backfill_rows_total[1m]) > 1000 for 5m` | initial ship |
| LiveTailResetRateHigh | warning | `rate(sse_reset_events_sent_total[5m]) > 0.5 for 10m` | initial ship |
| LiveTailAssemblerMpscOverflow | critical | `rate(cc_lb_dropped_events_total{reason="lifecycle_assembler_full"}[5m]) > 0 for 1m` | initial ship |
| LiveTailStorageTailLag | warning | `histogram_quantile(0.95, sum by (le) (rate(sse_storage_tail_lag_ms_bucket[5m]))) > 1000 for 5m` | Task 0.0 (was malformed) |

Threshold formalization requires 7 days of production baseline data. Until Task 1.3 lands, treat every alert as an actionable investigation trigger, not a runbook item.

## 3. Capture the Day-1 snapshot

Once metrics are ingesting cleanly, take a snapshot of steady-state values so future regressions are measurable. Copy the output into your on-call runbook:

```
window: <UTC timestamp> to <UTC timestamp>, load: <req/s baseline>
sse_reset_events_sent_total rate5m: <value>
sse_storage_tail_lag_ms p95: <value>
sse_notify_queue_usage_ratio: <value>
cc_lb_dropped_events_total{reason="lifecycle_assembler_full"} rate5m: <value>
cc_lb_dropped_events_total{reason="sse_lagged"} rate5m: <value>
sse_partial_notify_dropped_total{reason=~".+"} rate5m: <values by reason>
sse_notify_http_fetches_total{outcome="success|not_found|unauthorized|timeout|network_error"} rate5m: <values>
```

Store the snapshot at `docs/live-tail-day1-snapshot-<date>.md` (or your team's operational archive).

## 4. Assign remediation owners

For each alert, assign a named on-call role. Suggested defaults:

| Alert | Remediation owner |
| --- | --- |
| LiveTailNotifyQueueUsage* | Platform on-call (PG NOTIFY capacity) |
| LiveTailBackfillSpike | Platform on-call (DB read load) |
| LiveTailResetRateHigh | Application on-call (SSE handler / bus configuration) |
| LiveTailAssemblerMpscOverflow | Application on-call (assembler queue capacity) |
| LiveTailStorageTailLag | Platform on-call (DB write-to-read latency) |

## 5. What to watch during the first week

- `sse_reset_events_sent_total{reason}` should stay flat at zero. Every `reason` value observed in the first week is a bug candidate — investigate before dismissing.
- `sse_notify_queue_usage_ratio` should stay well under 30%. If baseline is already above 15%, adjust the notify queue capacity or move to Redis Streams sooner rather than later.
- `cc_lb_dropped_events_total{reason="lifecycle_assembler_full"}` must remain zero. Any non-zero rate blocks the P0 acceptance gate for this deployment.
- Storage tail lag p95 should stay under 250 ms for a healthy multi-instance deployment (poll cadence is 250 ms). Sustained values above 500 ms indicate a slow DB or over-committed instance.

Follow-ups 1.1 (cross-instance integration test) and 1.2 (500 req/s load test) will provide reproducible baselines for these thresholds; until then, deviate cautiously from these guidelines.
