# Live-tail baseline collection

Companion to [live-tail-day1-handoff.md](live-tail-day1-handoff.md) and prerequisite for follow-up task 1.3 (alert threshold formalization). Ships a lightweight scraper + summarizer that produces a **single-operator personal-prod** baseline over an arbitrary window (typically 7 days) so alert thresholds can be tuned against real data rather than intuition.

## What the baseline is (and isn't)

- **Is**: a periodic snapshot of the live-tail metric family from ONE cc-lb instance running on ONE operator's machine. Sufficient for detecting obvious threshold misconfiguration and for spotting regressions the same operator introduces later.
- **Is NOT**: a fleet-wide or multi-instance signal. Percentiles derived here **must** be treated as a lower bound on production variability, not as the canonical distribution. Multi-tenant SaaS deployments will produce different values.

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
  "ts": "2026-07-05T06:20:00Z",
  "host": "operator-laptop",
  "cc_lb_cmd": "/home/bhyoo/.local/bin/cc-lb serve --config /home/bhyoo/.config/cc-lb/config.toml",
  "samples": [
    {"metric": "sse_reconnects_total", "value": "3"},
    {"metric": "sse_partials_published_total{trigger=\"request_started\"}", "value": "17"},
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
ExecStart=%h/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/swift-otter/scripts/live-tail-baseline-collect.sh http://127.0.0.1:52253/metrics %h/.local/share/cc-lb/live-tail-baseline.jsonl
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

At `every 5 minutes` for 7 days you get 2016 records. Filtered live-tail-only, the JSONL grows ~1-2 MB. Well under any disk budget.

## Summary

Once you have a meaningful window (>=24h for smoke, 7d for threshold formalization), summarize:

```bash
./scripts/live-tail-baseline-summarize.py \
    "$XDG_DATA_HOME/cc-lb/live-tail-baseline.jsonl" \
    docs/live-tail-baseline-2026-07-XX.md
```

The summary computes min / p50 / p95 / p99 / max / delta per series and includes a header block with source metadata. Commit the resulting `docs/live-tail-baseline-<date>.md` if you want to preserve the snapshot; otherwise treat it as an operator-local artifact.

## Feeding Task 1.3 (alert threshold formalization)

Task 1.3 is **blocked** until at least 7 days of collection exist. When you have that data:

1. Compare each shipped alert threshold in [deploy/alerts/live-tail.yml](../deploy/alerts/live-tail.yml) against the summarized p95/p99.
2. If p99 already exceeds the alert threshold under nominal traffic, the threshold is misconfigured and must be raised. If p99 is orders of magnitude below the threshold, the threshold may be too lenient.
3. Document each adjustment in the alert rule's `annotations.summary` (e.g. `annotations.baseline_p99: "0.03 over 7-day 2026-07-05 to 2026-07-12 window"`) so future maintainers understand the derivation.

## Sample snapshot

A committed sample snapshot lives at [live-tail-baseline-sample.md](live-tail-baseline-sample.md), proving the collect + summarize pipeline works end-to-end. It is **not** a real baseline — it was generated from a short local scrape during development.
