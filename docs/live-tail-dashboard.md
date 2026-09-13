# Live-Tail Grafana Dashboard

A compact "Live-tail Health" dashboard ships as [`deploy/grafana/live-tail.json`](../deploy/grafana/live-tail.json). It mirrors every rule in [`deploy/alerts/live-tail.yml`](../deploy/alerts/live-tail.yml) and adds a few traffic-KPI signals so an alert page has a first-click landing view. It is deliberately **one primary row + one collapsed tripwire row**, sized for a single operator rather than a fleet console.

## Import

Two supported paths — UI import is the recommended default.

### UI import (recommended for single-operator)

1. Open Grafana → Dashboards → New → Import.
2. Upload `deploy/grafana/live-tail.json` (or paste its contents).
3. Choose the Prometheus datasource that scrapes cc-lb. The dashboard exposes `$datasource` as a top-level variable so the same JSON works against dev/staging/prod.
4. Save the dashboard. The `uid` is `cc-lb-live-tail`; keep it stable so external links (alert annotations, bookmarks) do not break.

### File-based provisioning (optional, for operators who already provision)

The repo also ships a sample provider config at [`deploy/grafana/dashboards.yaml`](../deploy/grafana/dashboards.yaml).

1. Copy `deploy/grafana/live-tail.json` into the provisioning directory on the Grafana host, e.g. `/var/lib/grafana/dashboards/cc-lb/live-tail.json`.
2. Drop `deploy/grafana/dashboards.yaml` into `/etc/grafana/provisioning/dashboards/cc-lb.yaml`.
3. Restart Grafana or wait `updateIntervalSeconds` (30 s in the sample).

Do not treat provisioning as required. UI import is a valid steady state for a single-operator personal-prod deployment.

## Panel-to-alert mapping

Every alert in `deploy/alerts/live-tail.yml` has a matching dashboard panel. The idea is that when a page fires, the first click lands in an already-relevant chart.

| Panel | Alert (see `deploy/alerts/live-tail.yml`) | Threshold |
| --- | --- | --- |
| PG NOTIFY queue usage (bar gauge) | `LiveTailNotifyQueueUsageHigh` (warn) | `sse_notify_queue_usage_ratio > 0.30` for 5 m |
| PG NOTIFY queue usage (bar gauge) | `LiveTailNotifyQueueUsageCritical` (crit) | `sse_notify_queue_usage_ratio > 0.50` for 2 m |
| Backfill rows/s | `LiveTailBackfillSpike` | `rate(sse_backfill_rows_total[1m]) > 1000` for 5 m |
| Reset rate by reason | `LiveTailResetRateHigh` | `rate(sse_reset_events_sent_total[5m]) > 0.5` for 10 m |
| Assembler overflow (stat) | `LiveTailAssemblerMpscOverflow` | `rate(cc_lb_dropped_events_total{reason="lifecycle_assembler_full"}[5m]) > 0` for 1 m |
| Storage tail lag p95/p99 | `LiveTailStorageTailLag` | `histogram_quantile(0.95, sum by (le) (rate(sse_storage_tail_lag_ms_bucket[5m]))) > 1000` for 5 m |

## Reading empty panels

Live-tail is a "silence is healthy" system. Several panels are expected to hover at zero and only jump during real incidents:

- **Assembler overflow (stat)** — expected 0. Any nonzero value paints the panel red and matches `LiveTailAssemblerMpscOverflow`.
- **Reset rate by reason (timeseries)** — sustained nonzero means clients keep being asked to resync. Look at the `reason` label: `backfill_cap` (client too far behind), `bus_lagged` (broadcast slower than emit), `storage_error` (SSE storage tail query failing).
- **Tripwire row → malformed frames / lagged clients** — expected 0 in normal operation.
- **Tripwire row → HTTP fallback outcomes** — nonzero means partials exceeded the NOTIFY payload cap and the receiver had to fetch over HTTP; nonzero is not itself pathological but it does mean either a very large partial or a config drift on `NOTIFY_PAYLOAD_LIMIT_BYTES`.

If a whole panel is empty and the operator suspects a scrape drop, cross-check `up{job="cc-lb"}` in Prometheus first before assuming the metric itself is broken.

## Variables

- `$datasource` — Prometheus datasource. Pick this once per import.
- `$rate_window` — one of `1m` / `5m` / `15m`. Default `5m`. Applied to every `rate()` and histogram query so switching zoom levels is one dropdown.

No `$instance` variable ships by default — the single-operator deployment is one cc-lb per Prometheus job. If you scale to a fleet, add `$instance = label_values(up{job="cc-lb"}, instance)` and thread `{instance=~"$instance"}` through the queries.

## Local validation

The repo ships an offline validator at [`scripts/validate-grafana-dashboard.sh`](../scripts/validate-grafana-dashboard.sh). It runs in CI as part of [`static-checks`](../.github/workflows/ci.yml), but you can run it locally too:

```sh
scripts/validate-grafana-dashboard.sh
```

The script:

1. Parses the JSON with `jq`.
2. Asserts required top-level fields (`uid`, `title`, `schemaVersion`, `panels`).
3. Rejects duplicate panel IDs (flattens row children into the pool).
4. Rejects Prometheus targets whose `expr` is empty.
5. Extracts every PromQL `expr`, substitutes Grafana template variables with a fixed `5m` stand-in, wraps them as synthetic recording rules, and hands them to `promtool check rules`.
6. Cross-checks any `LiveTail…` name mentioned in a panel description or link against the alert names actually declared in `deploy/alerts/*.yml` — a typo in the mapping table above would fail here.

This does NOT prove the panels render correctly in Grafana. It only proves the JSON is well-formed, the PromQL is parseable, and the alert-panel mapping is real. Screenshots are the operator's responsibility once the dashboard is loaded against real traffic.

## Related

- Alert rules: [`deploy/alerts/live-tail.yml`](../deploy/alerts/live-tail.yml)
- Scrape config sample: [`deploy/prometheus/cc-lb-scrape.yml`](../deploy/prometheus/cc-lb-scrape.yml)
- Metric label taxonomy: [`docs/metrics-live-tail.md`](metrics-live-tail.md)
- Runbook: [`docs/runbook-live-tail.md`](runbook-live-tail.md)
- Baseline collection: [`docs/live-tail-baseline-collection.md`](live-tail-baseline-collection.md)
