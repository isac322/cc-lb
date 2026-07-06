# Live-tail baseline collection

Companion to [live-tail-day1-handoff.md](live-tail-day1-handoff.md). A lightweight scraper and summarizer produces a single-instance baseline over an arbitrary window (typically 7 days) so alert thresholds can be tuned against local data rather than intuition. Keep source metadata and measurements outside the public tree.

## What the baseline is (and isn't)

- **Is**: a periodic snapshot of the live-tail metric family from one local cc-lb instance. Sufficient for detecting obvious threshold misconfiguration and for spotting regressions the same operator introduces later.
- **Is NOT**: a fleet-wide or multi-instance signal. Percentiles derived here **must** be treated as a lower bound on deployment variability, not as the canonical distribution. Multi-tenant deployments will produce different values.

If the deployment ever grows beyond a single operator, revisit the collection strategy — the shipped Prometheus scrape config (`deploy/prometheus/cc-lb-scrape.yml`) is the migration path.

## Prerequisites

- cc-lb is running with its metrics endpoint reachable (default `127.0.0.1:52253/metrics`).
- `bash`, `curl`, `awk`, `grep`, `sed` — installed on any modern Linux/macOS.
- `python3` (>=3.9) for the summarizer.

## Collection

Run the scraper manually to verify it works before wiring a timer:

```bash
./scripts/live-tail-baseline-collect.sh \
    http://127.0.0.1:52253/metrics \
    "$XDG_DATA_HOME/cc-lb/live-tail-baseline.jsonl"
```

Each invocation appends one JSON record with the shape:

```json
{
  "ts": "<timestamp>",
  "host": "<local-host>",
  "cc_lb_cmd": "cc-lb serve --config ./cc-lb.toml",
  "samples": [
    {"metric": "sse_reconnects_total", "value": "0"},
    {"metric": "sse_partials_published_total{trigger=\"request_started\"}", "value": "0"},
    ...
  ]
}
```

### Systemd timer (recommended)

Create `~/.config/systemd/user/cc-lb-baseline-collect.service`:

```ini
[Unit]
Description=cc-lb live-tail baseline scrape

[Service]
Type=oneshot
ExecStart=%h/bin/live-tail-baseline-collect.sh http://127.0.0.1:52253/metrics %h/.local/share/cc-lb/live-tail-baseline.jsonl
```

And `~/.config/systemd/user/cc-lb-baseline-collect.timer`:

```ini
[Unit]
Description=cc-lb live-tail baseline scrape (every 5 minutes)

[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:

```bash
systemctl --user daemon-reload
systemctl --user enable --now cc-lb-baseline-collect.timer
```

At a regular interval over a collection window, the output remains a bounded local
artifact suitable for threshold review; keep it outside the public tree.

## Summary

Once you have a meaningful window (>=24h for smoke, 7d for threshold formalization), summarize:

```bash
./scripts/live-tail-baseline-summarize.py \
    "$XDG_DATA_HOME/cc-lb/live-tail-baseline.jsonl" \
    "<local-output>/live-tail-baseline-<date>.md"
```

The summary computes min / p50 / p95 / p99 / max / delta per series and includes a header block with source metadata. Treat the resulting report as an operator-local artifact rather than a committed snapshot.

## Alert threshold formalization

Collect at least 7 days of representative data before adjusting thresholds:

1. Compare each shipped alert threshold in [deploy/alerts/live-tail.yml](../deploy/alerts/live-tail.yml) against the summarized p95/p99.
2. If p99 already exceeds the alert threshold under nominal traffic, the threshold is misconfigured and must be raised. If p99 is orders of magnitude below the threshold, the threshold may be too lenient.
3. Document each adjustment in the alert rule's `annotations.summary` (e.g. `annotations.baseline_p99: "<baseline value>"`) so future maintainers understand the derivation.
