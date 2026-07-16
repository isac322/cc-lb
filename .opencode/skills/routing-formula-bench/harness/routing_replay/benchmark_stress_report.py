from __future__ import annotations

import csv
from pathlib import Path

from routing_replay.benchmark_stress_types import StressReplay
from routing_replay.benchmark_types import BenchmarkSelector


def write_stress_comparison(out_dir: Path, replays: dict[BenchmarkSelector, StressReplay]) -> None:
    rows = tuple(_row(replay, out_dir / selector.value) for selector, replay in replays.items())
    with (out_dir / "comparison.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    table = ["# 5h burst inferred-quota stress comparison", "", "This is inferred quota stress testing from captured snapshot utilization and inferred capacity. It does not claim Anthropic absolute quota or billing parity.", "", "`selector_exclusions` are preserved selector semantics for `actual-choice` or `current` when their nominated upstream is quota-exhausted while another candidate remains eligible; they are neither served nor all-upstream blocks.", "", "| selector | cost | served | blocked | selector exclusions | blocked debit | blocked cost opportunity | exhausted all upstreams | resets | remaining 5h | remaining 7d | artifact |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    table.extend(f"| {row['selector']} | {row['simulated_cost_micros']} | {row['served_requests']} | {row['blocked_requests']} | {row['selector_exclusions']} | {row['blocked_debit_tokens']} | {row['blocked_cost_opportunity_micros']} | {row['exhausted_all_upstreams_count']} | {row['reset_count']} | {row['remaining_5h_tokens']} | {row['remaining_7d_tokens']} | {row['artifact_path']} |" for row in rows)
    (out_dir / "comparison.md").write_text("\n".join(table) + "\n", encoding="utf-8")
    _write_report(out_dir, replays)


def _row(replay: StressReplay, artifact: Path) -> dict[str, str]:
    result = replay.result
    return {
        "selector": replay.selector.value,
        "simulated_cost_micros": str(sum(item.simulated_cost_micros for item in result.decisions)),
        "served_requests": str(len(result.decisions)),
        "blocked_requests": str(len(result.blocked)),
        "selector_exclusions": str(sum(result.exclusions.values())),
        "blocked_debit_tokens": f"{sum(item.simulated_debit_tokens for item in result.blocked):.6f}",
        "blocked_cost_opportunity_micros": str(sum(item.simulated_cost_opportunity_micros for item in result.blocked)),
        "exhausted_all_upstreams_count": str(len(result.blocked)),
        "reset_count": str(result.reset_count),
        "repeat_factor": str(replay.plan.repeat_factor),
        "timestamp_anchor_ms": str(replay.plan.timestamp_anchor_ms),
        "reset_offset_source": replay.plan.reset_offset_source,
        "remaining_5h_tokens": f"{sum(state.capacity - state.used for (_upstream, window), state in result.quota_windows.items() if window == '5h'):.6f}",
        "remaining_7d_tokens": f"{sum(state.capacity - state.used for (_upstream, window), state in result.quota_windows.items() if window == '7d'):.6f}",
        "artifact_path": str(artifact),
    }


def _write_report(out_dir: Path, replays: dict[BenchmarkSelector, StressReplay]) -> None:
    sample = next(iter(replays.values()))
    text = "\n".join(("# 5h burst inferred-quota stress report", "", "This benchmark reshapes and repeats captured successful requests into a deterministic short burst while preserving each upstream's reset offset from its own captured `resets_at_unix_secs` snapshot. It does not claim Anthropic absolute quota parity.", "", "stress_set=5h-burst", f"burst_hours={sample.plan.config.burst_hours}", f"target_multiplier={sample.plan.config.target_multiplier}", f"minimum_repeat_factor={sample.plan.config.minimum_repeat_factor}", f"quota_capacity_multiplier={sample.plan.config.quota_capacity_multiplier}", f"repeat_factor={sample.plan.repeat_factor}", f"timestamp_anchor_ms={sample.plan.timestamp_anchor_ms}", f"reset_offset_source={sample.plan.reset_offset_source}", "earliest_5h_reset_rule=minimum_dynamic_5h_resets_at_then_remaining_5h_tokens_then_simulated_cost_then_debit_then_upstream_id", "blocked_opportunity_rule=deterministic_min_simulated_cost_then_debit_across_exhausted_candidates", "", "Artifacts: `comparison.csv`, `comparison.md`, and each selector's `replay.sqlite`, `replay_summary.csv`, `replay_decisions.csv`, `blocked_requests.csv`, and `quota_exhaustion.csv`.", ""))
    (out_dir / "stress-report.md").write_text(text, encoding="utf-8")
