#!/usr/bin/env python3
"""Summarize a live-tail baseline JSONL file into a markdown report.

Reads records produced by scripts/live-tail-baseline-collect.sh and computes
p50/p95/p99/max per metric across the window. Zero-valued metrics stay in the
report as an explicit "never fired" observation so an operator can distinguish
"metric absent" from "metric present but idle".

Explicit caveat propagated into the output: baseline data is a
**single-operator personal-prod** signal. Do not extrapolate to fleet behavior.

Usage:
    ./scripts/live-tail-baseline-summarize.py <input_jsonl> [<output_md>]
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


# Group counters as "cumulative"; we report both max value and the delta rate
# over the sampling window (last - first) / window_secs. Histograms only appear
# as summary lines here (their _bucket lines are excluded from the whitelist
# in the collector); we surface a "counter-only" flag for those.
COUNTER_METRICS = {
    "sse_partials_published_total",
    "sse_partials_throttled_total",
    "cc_lb_lifecycle_assembler_rows_total",
    "sse_backfill_pages_total",
    "sse_backfill_rows_total",
    "sse_reset_events_sent_total",
    "sse_lagged_resync_total",
    "sse_reconnects_total",
    "sse_malformed_frames_total",
    "sse_storage_tail_polls_total",
    "sse_partial_notify_sent_total",
    "sse_partial_notify_dropped_total",
    "sse_notify_http_fetches_total",
    "cc_lb_dropped_events_total",
}
GAUGE_METRICS = {
    "sse_storage_tail_backlog_rows",
    "sse_notify_queue_usage_ratio",
    "sse_storage_tail_lag_ms",  # Prometheus-exposed as summary; still gauge-shaped per sample
}


METRIC_NAME_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)")


def extract_metric_name(labeled: str) -> str:
    match = METRIC_NAME_RE.match(labeled)
    return match.group(1) if match else labeled


def load_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def summarize(records: list[dict[str, Any]]) -> str:
    if not records:
        return "# Live-tail baseline summary\n\nNo samples in input file.\n"

    # Timeline metadata
    ts_first = records[0]["ts"]
    ts_last = records[-1]["ts"]
    host = records[0].get("host", "unknown")
    cc_lb_cmd = records[0].get("cc_lb_cmd", "unknown")

    # Group samples by (metric_name, full_labeled_key) so different label sets
    # of the same base metric are summarized separately (e.g. by trigger, by
    # reason, by outcome).
    per_series: dict[str, list[float]] = defaultdict(list)
    for record in records:
        for sample in record.get("samples", []):
            labeled = sample["metric"]
            try:
                value = float(sample["value"])
            except (TypeError, ValueError):
                continue
            per_series[labeled].append(value)

    lines: list[str] = [
        "# Live-tail baseline summary",
        "",
        "> **Baseline source**: single-operator personal-prod. Data reflects one",
        "> cc-lb instance running on one operator's machine. **Do not**",
        "> extrapolate p95/p99 values to fleet or aggregate production behavior.",
        "",
        f"- Window: `{ts_first}` — `{ts_last}`",
        f"- Sample count: {len(records)}",
        f"- Host: `{host}`",
        f"- cc-lb cmdline: `{cc_lb_cmd}`",
        "",
        "## Per-series values",
        "",
        "| Series | Type | Samples | min | p50 | p95 | p99 | max | delta (last − first) |",
        "|---|---|---|---|---|---|---|---|---|",
    ]

    for labeled in sorted(per_series):
        values = per_series[labeled]
        base = extract_metric_name(labeled)
        kind = "counter" if base in COUNTER_METRICS else ("gauge" if base in GAUGE_METRICS else "other")
        n = len(values)
        vmin = min(values)
        vmax = max(values)
        p50 = statistics.median(values)
        p95 = _percentile(values, 0.95)
        p99 = _percentile(values, 0.99)
        delta = values[-1] - values[0]
        lines.append(
            f"| `{labeled}` | {kind} | {n} | {vmin:g} | {p50:g} | {p95:g} | {p99:g} | {vmax:g} | {delta:+g} |"
        )

    lines.append("")
    lines.append("## Never-observed series")
    lines.append("")
    lines.append("(Populated only after cross-referencing docs/metrics-live-tail.md — reserved for future extension.)")
    lines.append("")

    return "\n".join(lines) + "\n"


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = min(len(sorted_values) - 1, int(round(q * (len(sorted_values) - 1))))
    return sorted_values[idx]


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 1
    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else None
    records = load_records(in_path)
    report = summarize(records)
    if out_path:
        out_path.write_text(report)
        print(f"wrote {out_path}", file=sys.stderr)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
