from __future__ import annotations

from routing_replay.benchmark_cache import CacheState
from routing_replay.benchmark_quota import QuotaState
from routing_replay.benchmark_select import _cost
from routing_replay.benchmark_stress import StressConfig, _select_earliest_5h_reset, build_stress_plan, replay_stress
from routing_replay.benchmark_stress_types import ResetDrainOption, StressPlan
from routing_replay.benchmark_types import BenchmarkPricing, BenchmarkRow, BenchmarkSelector, Breakpoint, Candidate, ObservedInputUsage, QuotaSnapshot, SimulatedTokens


def run_stress_self_test(case: str) -> None:
    checks = {
        "stress-reset-skew": _reset_skew,
        "stress-exhaustion-exclusion": _exhaustion_exclusion,
        "stress-all-blocked": _all_blocked,
        "stress-blocked-no-mutation": _blocked_no_mutation,
        "stress-earliest-5h-reset": _earliest_5h_reset_wins,
        "stress-capacity-multiplier": _capacity_multiplier_scales_capacity_not_repeat,
    }
    if case == "stress":
        for check in checks.values():
            check()
        return
    checks[case]()


def _reset_skew() -> None:
    quota = QuotaState(_capacities())
    first, second = _candidate("a", 0.5, 10), _candidate("b", 0.5, 20)
    quota.seed(first, 0, 0)
    quota.seed(second, 0, 0)
    quota.debit("a", _tokens())
    quota.debit("b", _tokens())
    quota.advance((first, second), 15)
    assert quota.used("a", "5h") == 0.0
    assert quota.used("b", "5h") == 600.0
    quota.advance((first, second), 25)
    assert quota.used("b", "5h") == 0.0
    assert quota.reset_count() == 2


def _exhaustion_exclusion() -> None:
    result = replay_stress(_plan((_candidate("a", 1.0), _candidate("b", 0.0))), BenchmarkSelector.ACTUAL_CHOICE)
    assert len(result.decisions) == 1
    assert result.decisions[0].selected_upstream_id == "b"
    assert result.blocked == ()
    assert result.quota_windows[("a", "5h")].used == 1_000.0


def _all_blocked() -> None:
    result = replay_stress(_plan((_candidate("a", 1.0), _candidate("b", 1.0))), BenchmarkSelector.ACTUAL_CHOICE)
    assert len(result.decisions) == 0
    assert len(result.blocked) == 1
    assert result.blocked[0].reason == "all_upstreams_exhausted"


def _blocked_no_mutation() -> None:
    result = replay_stress(_plan((_candidate("a", 1.0), _candidate("b", 1.0))), BenchmarkSelector.ACTUAL_CHOICE)
    assert result.cache_entry_count == 0
    assert result.quota_windows[("a", "5h")].used == 1_000.0
    assert result.quota_windows[("b", "5h")].used == 1_000.0


def _earliest_5h_reset_wins() -> None:
    first, second = _candidate("a", 0.4, 10), _candidate("b", 0.2, 20)
    quota = QuotaState(_capacities())
    assert quota.seed(first, 0, 0)
    assert quota.seed(second, 0, 0)
    pricing = BenchmarkPricing(1_000_000, 100_000, 1_000_000, 1_000_000)
    tokens = {"a": SimulatedTokens(600, 0, 0, 0), "b": SimulatedTokens(0, 1_000, 0, 0)}
    assert quota.can_cover(first, tokens["a"])
    assert quota.can_cover(second, tokens["b"])
    assert tokens["b"].cache_read > tokens["a"].cache_read
    assert _cost(tokens["b"], pricing) < _cost(tokens["a"], pricing)
    options = tuple(
        ResetDrainOption(candidate.upstream_id, state.resets_at, state.capacity - state.used, _cost(tokens[candidate.upstream_id], pricing), tokens[candidate.upstream_id].debit)
        for candidate in (first, second)
        for state in (quota.five_hour_state(candidate.upstream_id),)
    )
    assert _select_earliest_5h_reset(options) == "a"


def _capacity_multiplier_scales_capacity_not_repeat() -> None:
    base = _plan((_candidate("a", 0.0), _candidate("b", 0.0)))
    scaled = build_stress_plan(base.source_rows, _capacities(), StressConfig(1.0, 1.0, 1, 5.0))
    assert scaled.repeat_factor == base.repeat_factor
    assert scaled.capacities[("a", "5h")] == 5_000.0


def _plan(candidates: tuple[Candidate, ...]) -> StressPlan:
    row = BenchmarkRow("event", "request", 0, "routed_dispatched_success", "fixture", "b", "b", _pricing(), _points(), candidates, ObservedInputUsage(1_000, 0, 0, 0))
    return build_stress_plan((row,), _capacities(), StressConfig(1.0, 1.0, 1))


def _candidate(upstream: str, utilization: float, reset: int = 18_000) -> Candidate:
    return Candidate(upstream, 1.0, 0.0, (QuotaSnapshot("5h", utilization, reset), QuotaSnapshot("7d", utilization, 604_800)))


def _capacities() -> dict[tuple[str, str], float]:
    return {(upstream, window): 1_000.0 for upstream in ("a", "b") for window in ("5h", "7d")}


def _points() -> tuple[Breakpoint, ...]:
    return (Breakpoint("prefix", 120, 300, ()),)


def _pricing() -> BenchmarkPricing:
    return BenchmarkPricing(1_000_000, 1_000_000, 1_000_000, 1_000_000)


def _tokens() -> SimulatedTokens:
    return SimulatedTokens(100, 0, 0, 0)
