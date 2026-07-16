from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from routing_replay.benchmark_stress_types import StressReplay
from routing_replay.benchmark_types import BenchmarkDecision


def write_stress_replay(out_dir: Path, replay: StressReplay) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics = _metrics(replay)
    _decisions_csv(out_dir / "replay_decisions.csv", replay.result.decisions)
    _blocked_csv(out_dir / "blocked_requests.csv", replay)
    _quota_csv(out_dir / "quota_exhaustion.csv", replay)
    _summary_csv(out_dir / "replay_summary.csv", metrics)
    _sqlite(out_dir / "replay.sqlite", replay, metrics)


def _metrics(replay: StressReplay) -> tuple[tuple[str, str], ...]:
    result = replay.result
    decisions = result.decisions
    blocked = result.blocked
    return (
        ("served_requests", str(len(decisions))),
        ("blocked_requests", str(len(blocked))),
        ("selector_exclusions", str(sum(result.exclusions.values()))),
        ("exhausted_all_upstreams_count", str(len(blocked))),
        ("simulated_cost_micros", str(sum(item.simulated_cost_micros for item in decisions))),
        ("simulated_debit_tokens", f"{sum(item.simulated_debit_tokens for item in decisions):.6f}"),
        ("blocked_debit_tokens", f"{sum(item.simulated_debit_tokens for item in blocked):.6f}"),
        ("blocked_cost_opportunity_micros", str(sum(item.simulated_cost_opportunity_micros for item in blocked))),
        ("reset_count", str(result.reset_count)),
        ("repeat_factor", str(replay.plan.repeat_factor)),
        ("capacity_fallback_count", str(replay.capacity.fallback_count)),
        ("capacity_missing_count", str(replay.capacity.missing_count)),
    )


def _decisions_csv(path: Path, decisions: tuple[BenchmarkDecision, ...]) -> None:
    fields = tuple(BenchmarkDecision.__dataclass_fields__)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        writer.writerows(tuple(getattr(item, field) for field in fields) for item in decisions)


def _blocked_csv(path: Path, replay: StressReplay) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("event_id", "request_id", "ts_unix_ms", "simulated_debit_tokens", "simulated_cost_opportunity_micros", "reason"))
        writer.writerows((item.event_id, item.request_id, item.ts_unix_ms, item.simulated_debit_tokens, item.simulated_cost_opportunity_micros, item.reason) for item in replay.result.blocked)


def _quota_csv(path: Path, replay: StressReplay) -> None:
    counts = {(item.upstream_id, item.window): item.excluded_request_count for item in replay.result.quota_exhaustions}
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("upstream_id", "window", "capacity_tokens", "used_tokens", "remaining_tokens", "resets_at_unix_secs", "reset_count", "excluded_request_count"))
        writer.writerows((upstream, window, state.capacity, state.used, state.capacity - state.used, state.resets_at, state.reset_count, counts.get((upstream, window), 0)) for (upstream, window), state in sorted(replay.result.quota_windows.items()))


def _summary_csv(path: Path, metrics: tuple[tuple[str, str], ...]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("metric", "value"))
        writer.writerows(metrics)


def _sqlite(path: Path, replay: StressReplay, metrics: tuple[tuple[str, str], ...]) -> None:
    if path.exists():
        path.unlink()
    with sqlite3.connect(path) as connection:
        connection.executescript("create table replay_meta (key text primary key,value text not null);create table replay_summary (metric text,value text);create table replay_decisions (event_id text primary key,request_id text,ts_unix_ms integer,actual_upstream_id text,selected_upstream_id text,selector text,simulated_cost_micros integer,simulated_debit_tokens real,cache_read_tokens integer,simulated_input_side_tokens integer,hit_ratio real,observed_hit_ratio real,quota_urgency real,trace_urgency real,urgency_rank_top_matches integer,changed_from_actual integer);create table blocked_requests (event_id text primary key,request_id text,ts_unix_ms integer,simulated_debit_tokens real,simulated_cost_opportunity_micros integer,reason text);create table quota_exhaustion (upstream_id text,window text,capacity_tokens real,used_tokens real,remaining_tokens real,resets_at_unix_secs integer,reset_count integer,excluded_request_count integer)")
        connection.executemany("insert into replay_meta values (?,?)", tuple(_metadata(replay).items()))
        connection.executemany("insert into replay_summary values (?,?)", metrics)
        connection.executemany("insert into replay_decisions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(_decision_values(item) for item in replay.result.decisions))
        connection.executemany("insert into blocked_requests values (?,?,?,?,?,?)", tuple((item.event_id, item.request_id, item.ts_unix_ms, item.simulated_debit_tokens, item.simulated_cost_opportunity_micros, item.reason) for item in replay.result.blocked))
        counts = {(item.upstream_id, item.window): item.excluded_request_count for item in replay.result.quota_exhaustions}
        connection.executemany("insert into quota_exhaustion values (?,?,?,?,?,?,?,?)", tuple((upstream, window, state.capacity, state.used, state.capacity - state.used, state.resets_at, state.reset_count, counts.get((upstream, window), 0)) for (upstream, window), state in sorted(replay.result.quota_windows.items())))


def _metadata(replay: StressReplay) -> dict[str, str]:
    return {
        "simulation_mode": "dynamic-quota-cache-v1-stress-5h-burst",
        "stress_set": "5h-burst",
        "selector": replay.selector.value,
        "source_copy": str(replay.source),
        "burst_hours": str(replay.plan.config.burst_hours),
        "target_multiplier": str(replay.plan.config.target_multiplier),
        "minimum_repeat_factor": str(replay.plan.config.minimum_repeat_factor),
        "quota_capacity_multiplier": str(replay.plan.config.quota_capacity_multiplier),
        "repeat_factor": str(replay.plan.repeat_factor),
        "timestamp_anchor_ms": str(replay.plan.timestamp_anchor_ms),
        "reset_offset_source": replay.plan.reset_offset_source,
        "blocked_opportunity_rule": "deterministic_min_simulated_cost_then_debit_across_exhausted_candidates",
        "capacity_fallback_count": str(replay.capacity.fallback_count),
        "capacity_missing_count": str(replay.capacity.missing_count),
    }


def _decision_values(item: BenchmarkDecision) -> tuple[str | int | float | bool | None, ...]:
    return (item.event_id, item.request_id, item.ts_unix_ms, item.actual_upstream_id, item.selected_upstream_id, item.selector, item.simulated_cost_micros, item.simulated_debit_tokens, item.cache_read_tokens, item.simulated_input_side_tokens, item.hit_ratio, item.observed_hit_ratio, item.quota_urgency, item.trace_urgency, item.urgency_rank_top_matches, item.changed_from_actual)
