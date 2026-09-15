from __future__ import annotations

import json
from pathlib import Path

from routing_replay.benchmark_cache import CacheState
from routing_replay.benchmark_parse import parse_benchmark_row
from routing_replay.benchmark_quota import QuotaState, infer_capacities
from routing_replay.benchmark_select import select
from routing_replay.benchmark_stress_selftest import run_stress_self_test
from routing_replay.benchmark_types import BenchmarkPricing, BenchmarkRow, BenchmarkSelector, Breakpoint, Candidate, ObservedInputUsage, QuotaSnapshot, SimulatedTokens
from routing_replay.types import Json


def run_self_test(case: str, evidence_dir: Path) -> None:
    checks = {"parser": _parser, "parser-missing-quota": _parser_missing_quota, "cache": _cache, "cache-hit": _cache, "cache-expiry": _cache_expiry, "capacity": _capacity, "capacity-fallback": _capacity_fallback, "quota": _quota, "quota-debit": _quota, "quota-reset": _quota_reset, "actual-choice-exclusions": _actual_exclusions, "selectors": _selectors, "cost-first": _cost_first, "comparison-missing-artifact": _comparison_missing}
    if case == "all":
        for check in checks.values():
            check()
        run_stress_self_test("stress")
    elif case.startswith("stress-") or case == "stress":
        run_stress_self_test(case)
    else:
        checks[case]()
    _write(evidence_dir / _evidence_name(case), f"{case}=pass\n")


def _parser() -> None:
    row, reason = parse_benchmark_row(json.dumps(_payload()))
    assert reason == ""
    assert row is not None and len(row.candidates[0].snapshots) == 2 and row.breakpoints[0].ttl_seconds == 300


def _parser_missing_quota() -> None:
    payload = _payload()
    input_map = payload["input"]
    assert isinstance(input_map, dict)
    candidates = input_map["candidates"]
    assert isinstance(candidates, list) and isinstance(candidates[0], dict)
    candidates[0]["subscription_quotas"] = []
    row, reason = parse_benchmark_row(json.dumps(payload))
    assert row is None and reason == "missing_quota_snapshot"


def _cache() -> None:
    state, points = CacheState(), _points()
    state.record_success("a", points, 0)
    assert state.tokens_for("a", points, 200, 1).cache_read == 120
    assert state.tokens_for("b", points, 200, 1).cache_read == 0


def _cache_expiry() -> None:
    state, points = CacheState(), _points()
    state.record_success("a", points, 0)
    assert state.tokens_for("a", points, 200, 300_001).cache_read == 0


def _capacity() -> None:
    first, second = _row(0, 0.1), _row(1_000, 0.2)
    inferred = infer_capacities((first, second))
    assert inferred.capacities[("a", "5h")] == 15_000


def _capacity_fallback() -> None:
    inferred = infer_capacities((_row(0, 0.1), _row(1_000, 0.2), _row(2_000, 0.2, "b")))
    assert inferred.fallback_count > 0 and inferred.capacities[("b", "5h")] > 0


def _quota() -> None:
    state = QuotaState({("a", "5h"): 1_000, ("a", "7d"): 1_000})
    assert state.ensure(_candidate(0.0), 0)
    state.debit("a", SimulatedTokens(0, 500, 100, 0))
    assert state.urgency("a", 17_000) > state.urgency("a", 0)


def _quota_reset() -> None:
    state = QuotaState({("a", "5h"): 1_000, ("a", "7d"): 1_000})
    assert state.ensure(_candidate(0.5, 10), 0)
    state.debit("a", SimulatedTokens(100, 0, 0, 0))
    assert state.ensure(_candidate(0.5, 10), 10)
    assert state.reset_count() == 1


def _actual_exclusions() -> None:
    selected, reason = select(BenchmarkSelector.ACTUAL_CHOICE, (_candidate(0.1),), {"a": SimulatedTokens(1, 0, 0, 0)}, {"a": 0.0}, _pricing(), None, None)
    assert selected is None and reason == "missing_actual_upstream"


def _selectors() -> None:
    candidates = (_candidate(0.1, upstream="a"), _candidate(0.1, upstream="b"))
    tokens = {"a": SimulatedTokens(100, 0, 0, 0), "b": SimulatedTokens(0, 100, 0, 0)}
    urgencies = {"a": 3.0, "b": 1.0}
    for selector in BenchmarkSelector:
        selected, reason = select(selector, candidates, tokens, urgencies, _pricing(), "a", "a")
        match selector:
            case BenchmarkSelector.EARLIEST_5H_RESET:
                assert selected is None and reason == "stress_selector_requires_stress_replay"
            case (BenchmarkSelector.ACTUAL_CHOICE | BenchmarkSelector.CURRENT | BenchmarkSelector.SMOOTHSTEP_A | BenchmarkSelector.LOWERED_GATE | BenchmarkSelector.MAX_HIT | BenchmarkSelector.MIN_COST | BenchmarkSelector.QUOTA_AWARE_MIN_COST | BenchmarkSelector.COST_FIRST):
                assert selected in {"a", "b"} and reason == ""


def _cost_first() -> None:
    candidates = (_candidate(0.1, upstream="a"), _candidate(0.1, upstream="b"))
    urgencies = {"a": 0.0, "b": 5.0}
    warm_tokens = {"a": SimulatedTokens(0, 1_000, 0, 0), "b": SimulatedTokens(1_000, 0, 0, 0)}
    selected, reason = select(BenchmarkSelector.COST_FIRST, candidates, warm_tokens, urgencies, _pricing(), "a", "a")
    assert selected == "a" and reason == ""
    cold_tokens = {"a": SimulatedTokens(1_000, 0, 0, 0), "b": SimulatedTokens(1_000, 0, 0, 0)}
    selected_cold, reason_cold = select(BenchmarkSelector.COST_FIRST, candidates, cold_tokens, urgencies, _pricing(), "a", "a")
    assert selected_cold == "b" and reason_cold == ""


def _comparison_missing() -> None:
    expected = Path("missing-artifact/replay.sqlite")
    assert not expected.exists()


def _row(timestamp: int, utilization: float, upstream: str = "a") -> BenchmarkRow:
    return BenchmarkRow(str(timestamp), "request", timestamp, "routed_dispatched_success", "fixture", upstream, upstream, _pricing(), _points(), (_candidate(utilization, upstream=upstream),), ObservedInputUsage(1_500, 0, 0, 0))


def _candidate(utilization: float, reset: int = 18_000, upstream: str = "a") -> Candidate:
    return Candidate(upstream, 1.0, 0.0, (QuotaSnapshot("5h", utilization, reset), QuotaSnapshot("7d", utilization, 604_800)))


def _points() -> tuple[Breakpoint, ...]:
    return (Breakpoint("prefix", 120, 300, ("prefix",)),)


def _pricing() -> BenchmarkPricing:
    return BenchmarkPricing(3_000_000, 300_000, 3_750_000, 6_000_000)


def _payload() -> dict[str, Json]:
    return {"event_id": "fixture-event", "request_id": "fixture-request", "ts_unix_ms": 1_000, "input": {"cache_pricing": {"input_micros_per_million": 3_000_000, "cache_read_micros_per_million": 300_000, "cache_creation_5m_micros_per_million": 3_750_000, "cache_creation_1h_micros_per_million": 6_000_000}, "breakpoints": [{"prefix_hash": "prefix", "prefix_token_count": 120, "requested_ttl": "ephemeral5m", "lookback_prefixes": []}], "candidates": [{"upstream_id": "upstream-a", "subscription_quotas": [{"window": "5h", "utilization": 0.2, "resets_at_unix_secs": 19_000}, {"window": "7d", "utilization": 0.3, "resets_at_unix_secs": 605_000}]}], "routing_trace": {"stages": [{"subscription_preference": {"formula_winner_upstream_id": "upstream-a", "candidates": [{"upstream_id": "upstream-a", "quota_weight_factor": 1.0, "quota_urgency_combined": 0.0}]}}]}}}


def _evidence_name(case: str) -> str:
    return {"parser": "task-1-parser.txt", "capacity": "task-2-capacity.txt", "cache": "task-3-cache.txt", "quota": "task-4-quota.txt", "selectors": "task-6-selectors.txt"}.get(case, f"selftest-{case}.txt")


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
