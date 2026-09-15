from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from routing_replay.benchmark_engine import replay
from routing_replay.benchmark_select import MIN_COST_ADVANTAGE_THRESHOLD
from routing_replay.benchmark_types import BenchmarkDecision, BenchmarkResult, BenchmarkSelector


@dataclass(frozen=True, slots=True)
class Metric:
    category: str
    scenario: str
    metric: str
    value: str


def run_and_write(source: Path, out_dir: Path, selector: BenchmarkSelector) -> BenchmarkResult:
    result, _capacity = replay(source, selector)
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics = _metrics(result, selector)
    _csv(out_dir / "replay_decisions.csv", result.decisions)
    _summary_csv(out_dir / "replay_summary.csv", metrics)
    _sqlite(out_dir / "replay.sqlite", result, metrics, selector, source)
    return result


def _metrics(result: BenchmarkResult, selector: BenchmarkSelector) -> tuple[Metric, ...]:
    decisions = result.decisions
    total_input = sum(item.simulated_input_side_tokens for item in decisions)
    read = sum(item.cache_read_tokens for item in decisions)
    count = len(decisions)
    costs = sum(item.simulated_cost_micros for item in decisions)
    debit = sum(item.simulated_debit_tokens for item in decisions)
    hits = sum(item.hit_ratio for item in decisions) / count if count else 0.0
    observed_hits = sum(item.observed_hit_ratio for item in decisions) / count if count else 0.0
    shares = _shares(decisions)
    return (
        Metric("coverage", "all", "source_snapshot_rows", str(result.total_rows)),
        Metric("coverage", "all", "replayable_rows", str(count)),
        Metric("coverage", "all", "excluded_rows", str(sum(result.exclusions.values()))),
        Metric("cost", selector.value, "simulated_cost_micros", str(costs)),
        Metric("cost", selector.value, "simulated_debit_tokens", f"{debit:.6f}"),
        Metric("cost", selector.value, "hit_ratio_simulated_avg", f"{hits:.6f}"),
        Metric("cost", selector.value, "hit_ratio_observed_avg", f"{observed_hits:.6f}"),
        Metric("cost", selector.value, "hit_ratio_simulated_token_weighted", f"{read / total_input if total_input else 0:.6f}"),
        Metric("quota", selector.value, "reset_count", str(result.reset_count)),
        Metric("quota", selector.value, "capacity_fallback_count", str(result.capacity_fallback_count)),
        Metric("quota", selector.value, "capacity_missing_count", str(result.capacity_missing_count)),
        Metric("balance", selector.value, "gini", f"{_gini(tuple(shares.values())):.6f}"),
        Metric("balance", selector.value, "l1_to_q_target", f"{_l1_to_urgency_target(decisions):.6f}"),
        Metric("selection", selector.value, "changed_count", str(sum(item.changed_from_actual for item in decisions))),
        Metric("calibration", selector.value, "urgency_rank_top_agreement", f"{sum(item.urgency_rank_top_matches for item in decisions) / count if count else 0:.6f}"),
    )


def _shares(decisions: tuple[BenchmarkDecision, ...]) -> dict[str, int]:
    result: dict[str, int] = {}
    for decision in decisions:
        result[decision.selected_upstream_id] = result.get(decision.selected_upstream_id, 0) + 1
    return result


def _gini(values: tuple[int, ...]) -> float:
    if not values or not sum(values):
        return 0.0
    ordered = sorted(values)
    count = len(ordered)
    return sum((2 * index - count - 1) * value for index, value in enumerate(ordered, 1)) / (count * sum(ordered))


def _l1_to_urgency_target(decisions: tuple[BenchmarkDecision, ...]) -> float:
    shares = _shares(decisions)
    urgency: dict[str, float] = {}
    for decision in decisions:
        urgency[decision.selected_upstream_id] = urgency.get(decision.selected_upstream_id, 0.0) + decision.quota_urgency
    total_urgency = sum(urgency.values())
    if not shares or total_urgency == 0:
        return 0.0
    return sum(abs(count / len(decisions) - urgency.get(upstream, 0.0) / total_urgency) for upstream, count in shares.items())


def _csv(path: Path, decisions: tuple[BenchmarkDecision, ...]) -> None:
    fields = tuple(BenchmarkDecision.__dataclass_fields__)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        writer.writerows(tuple(getattr(item, field) for field in fields) for item in decisions)


def _summary_csv(path: Path, metrics: tuple[Metric, ...]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("category", "scenario", "metric", "value"))
        writer.writerows((item.category, item.scenario, item.metric, item.value) for item in metrics)


def _sqlite(path: Path, result: BenchmarkResult, metrics: tuple[Metric, ...], selector: BenchmarkSelector, source: Path) -> None:
    if path.exists():
        path.unlink()
    with sqlite3.connect(path) as connection:
        connection.executescript("create table replay_meta (key text primary key,value text not null);create table replay_summary (category text,scenario text,metric text,value text);create table replay_decisions (event_id text primary key,request_id text,ts_unix_ms integer,actual_upstream_id text,selected_upstream_id text,selector text,simulated_cost_micros integer,simulated_debit_tokens real,cache_read_tokens integer,simulated_input_side_tokens integer,hit_ratio real,observed_hit_ratio real,quota_urgency real,trace_urgency real,urgency_rank_top_matches integer,changed_from_actual integer)")
        meta = {"simulation_mode": "dynamic-quota-cache-v1", "selector": selector.value, "source_copy": str(source), "quota_5h_gamma": "1.0", "quota_7d_gamma": "1.3", "quota_target_floor": "0.01", "smoothmax_p": "6", "smoothstep_a": "0.35/0.70", "lowered_gate": "0.25/0.50", "min_cost_rule": "deterministic_argmin_simulated_cost_then_debit", "quota_aware_min_cost_threshold": str(MIN_COST_ADVANTAGE_THRESHOLD), "capacity_fallback_count": str(result.capacity_fallback_count), "capacity_missing_count": str(result.capacity_missing_count)}
        connection.executemany("insert into replay_meta values (?,?)", tuple(meta.items()))
        connection.executemany("insert into replay_summary values (?,?,?,?)", tuple((item.category, item.scenario, item.metric, item.value) for item in metrics))
        connection.executemany("insert into replay_decisions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple((item.event_id, item.request_id, item.ts_unix_ms, item.actual_upstream_id, item.selected_upstream_id, item.selector, item.simulated_cost_micros, item.simulated_debit_tokens, item.cache_read_tokens, item.simulated_input_side_tokens, item.hit_ratio, item.observed_hit_ratio, item.quota_urgency, item.trace_urgency, int(item.urgency_rank_top_matches), int(item.changed_from_actual)) for item in result.decisions))
