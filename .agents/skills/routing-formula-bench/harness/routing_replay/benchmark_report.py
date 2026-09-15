from __future__ import annotations

import csv
from pathlib import Path

from routing_replay.benchmark_types import BenchmarkResult, BenchmarkSelector


CANONICAL = (
    BenchmarkSelector.ACTUAL_CHOICE,
    BenchmarkSelector.CURRENT,
    BenchmarkSelector.SMOOTHSTEP_A,
    BenchmarkSelector.LOWERED_GATE,
    BenchmarkSelector.MAX_HIT,
    BenchmarkSelector.MIN_COST,
    BenchmarkSelector.QUOTA_AWARE_MIN_COST,
    BenchmarkSelector.COST_FIRST,
)

STRESS_CANONICAL = (*CANONICAL, BenchmarkSelector.EARLIEST_5H_RESET)


def write_comparison(out_dir: Path, results: dict[BenchmarkSelector, BenchmarkResult]) -> None:
    rows = tuple(_row(selector, result, out_dir / selector.value) for selector, result in results.items())
    with (out_dir / "comparison.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    table = ["# Dynamic quota+cache comparison", "", "| variant | rows | exclusions | simulated cost | debit | hit avg | resets | gini | changed | artifact |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    table.extend(f"| {row['variant']} | {row['rows']} | {row['exclusions']} | {row['simulated_cost_micros']} | {row['simulated_debit_tokens']} | {row['hit_ratio_avg']} | {row['reset_count']} | {row['gini']} | {row['changed_count']} | {row['artifact_path']} |" for row in rows)
    (out_dir / "comparison.md").write_text("\n".join(table) + "\n", encoding="utf-8")


def write_final_report(root: Path, comparison_dir: Path, results: dict[BenchmarkSelector, BenchmarkResult]) -> None:
    ranked = sorted(results.items(), key=lambda item: sum(decision.simulated_cost_micros for decision in item[1].decisions))
    winner, result = ranked[0]
    text = "\n".join(("# dynamic-quota-cache-v1 final report", "", "simulation_mode=dynamic-quota-cache-v1", "", f"Relative inferred-quota winner: `{winner.value}` with simulated cost {sum(item.simulated_cost_micros for item in result.decisions)} micros, average hit ratio {(sum(item.hit_ratio for item in result.decisions) / len(result.decisions) if result.decisions else 0):.6f}, and {result.reset_count} quota resets.", "", "Trust boundaries: this benchmark uses dynamic cache and dynamic quota state, with capacity inference from actual-choice utilization deltas and median fallback, input-side debit only, and deterministic replay timestamps. It does not claim Anthropic-real absolute quota or billing parity.", "", "Variants: actual-choice, current, smoothstep-a (0.35/0.70), lowered-gate (0.25/0.50), max-hit, min-cost, quota-aware-min-cost, cost-first (near-cost tier epsilon 0.05, urgency tiebreak).", "", f"canonical artifacts: `{comparison_dir / 'comparison.csv'}` and `{comparison_dir / 'comparison.md'}`.", "", "Commands: `uv run benchmark_quota_cache.py --source capture_copy.sqlite --selector actual-choice --out-dir dynamic-quota-cache-v1/final-actual-choice`; `uv run benchmark_quota_cache.py --source capture_copy.sqlite --benchmark-set canonical --out-dir dynamic-quota-cache-v1/final-canonical`.", ""))
    root.mkdir(parents=True, exist_ok=True)
    (root / "final-report.md").write_text(text, encoding="utf-8")


def _row(selector: BenchmarkSelector, result: BenchmarkResult, artifact: Path) -> dict[str, str]:
    decisions = result.decisions
    shares = [sum(item.selected_upstream_id == upstream for item in decisions) for upstream in {item.selected_upstream_id for item in decisions}]
    urgency_total = sum(item.quota_urgency for item in decisions)
    l1 = sum(abs(count / len(decisions) - sum(item.quota_urgency for item in decisions if item.selected_upstream_id == upstream) / urgency_total) for upstream, count in {item.selected_upstream_id: sum(other.selected_upstream_id == item.selected_upstream_id for other in decisions) for item in decisions}.items()) if decisions and urgency_total else 0.0
    return {"variant": selector.value, "rows": str(len(decisions)), "exclusions": str(sum(result.exclusions.values())), "simulated_cost_micros": str(sum(item.simulated_cost_micros for item in decisions)), "simulated_debit_tokens": f"{sum(item.simulated_debit_tokens for item in decisions):.6f}", "hit_ratio_avg": f"{sum(item.hit_ratio for item in decisions) / len(decisions) if decisions else 0:.6f}", "hit_ratio_token_weighted": f"{sum(item.cache_read_tokens for item in decisions) / sum(item.simulated_input_side_tokens for item in decisions) if sum(item.simulated_input_side_tokens for item in decisions) else 0:.6f}", "quota_debit_total": f"{sum(item.simulated_debit_tokens for item in decisions):.6f}", "reset_count": str(result.reset_count), "gini": f"{_gini(shares):.6f}", "l1_to_q_target": f"{l1:.6f}", "changed_count": str(sum(item.changed_from_actual for item in decisions)), "artifact_path": str(artifact)}


def _gini(values: list[int]) -> float:
    return sum((2 * index - len(values) - 1) * value for index, value in enumerate(sorted(values), 1)) / (len(values) * sum(values)) if values and sum(values) else 0.0
