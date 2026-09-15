from __future__ import annotations

import math
from collections.abc import Callable

from routing_replay.benchmark_types import BenchmarkPricing, BenchmarkSelector, Candidate, SimulatedTokens

MIN_COST_ADVANTAGE_THRESHOLD = 0.05
COST_NEAR_TIE_EPSILON = 0.05


def select(
    selector: BenchmarkSelector, candidates: tuple[Candidate, ...], tokens: dict[str, SimulatedTokens],
    urgencies: dict[str, float], pricing: BenchmarkPricing, actual: str | None, current: str | None,
) -> tuple[str | None, str]:
    available = {candidate.upstream_id for candidate in candidates}
    match selector:
        case BenchmarkSelector.ACTUAL_CHOICE:
            return (actual, "") if actual in available else (None, "missing_actual_upstream")
        case BenchmarkSelector.CURRENT:
            return (current, "") if current in available else (None, "missing_current_upstream")
        case BenchmarkSelector.MAX_HIT:
            return _max_by(candidates, lambda candidate: (_hit(tokens[candidate.upstream_id]), urgencies[candidate.upstream_id])), ""
        case BenchmarkSelector.MIN_COST:
            return min(candidates, key=lambda candidate: (_cost(tokens[candidate.upstream_id], pricing), tokens[candidate.upstream_id].debit, candidate.upstream_id)).upstream_id, ""
        case BenchmarkSelector.QUOTA_AWARE_MIN_COST:
            minimum = min(candidates, key=lambda candidate: (_cost(tokens[candidate.upstream_id], pricing), candidate.upstream_id))
            reference = _cost(tokens[current], pricing) if current in available else max(_cost(tokens[candidate.upstream_id], pricing) for candidate in candidates)
            advantage = (reference - _cost(tokens[minimum.upstream_id], pricing)) / max(reference, 1)
            return (minimum.upstream_id if advantage >= MIN_COST_ADVANTAGE_THRESHOLD else _max_by(candidates, lambda candidate: (urgencies[candidate.upstream_id], _hit(tokens[candidate.upstream_id])))), ""
        case BenchmarkSelector.COST_FIRST:
            minimum = min(_cost(tokens[candidate.upstream_id], pricing) for candidate in candidates)
            near = tuple(candidate for candidate in candidates if _cost(tokens[candidate.upstream_id], pricing) <= (1 + COST_NEAR_TIE_EPSILON) * minimum)
            return _max_by(near, lambda candidate: (urgencies[candidate.upstream_id], -_cost(tokens[candidate.upstream_id], pricing))), ""
        case BenchmarkSelector.EARLIEST_5H_RESET:
            return None, "stress_selector_requires_stress_replay"
        case BenchmarkSelector.SMOOTHSTEP_A:
            return _gated(candidates, tokens, urgencies, 0.35, 0.70), ""
        case BenchmarkSelector.LOWERED_GATE:
            return _gated(candidates, tokens, urgencies, 0.25, 0.50), ""


def _gated(candidates: tuple[Candidate, ...], tokens: dict[str, SimulatedTokens], urgencies: dict[str, float], floor: float, ceiling: float) -> str:
    weights = _normalize(urgencies)
    gate = _smoothstep(_balance(weights), floor, ceiling)
    return _max_by(candidates, lambda candidate: ((1 - gate) * weights[candidate.upstream_id] + gate * _hit(tokens[candidate.upstream_id]), urgencies[candidate.upstream_id]))


def _max_by(candidates: tuple[Candidate, ...], key: Callable[[Candidate], tuple[float, float]]) -> str:
    return max(candidates, key=lambda candidate: (*key(candidate), candidate.upstream_id)).upstream_id


def _cost(tokens: SimulatedTokens, pricing: BenchmarkPricing) -> int:
    return round((tokens.uncached_input * pricing.input_micros_per_million + tokens.cache_read * pricing.cache_read_micros_per_million + tokens.cache_creation_5m * pricing.cache_creation_5m_micros_per_million + tokens.cache_creation_1h * pricing.cache_creation_1h_micros_per_million) / 1_000_000)


def _hit(tokens: SimulatedTokens) -> float:
    total = tokens.uncached_input + tokens.cache_read + tokens.cache_creation_5m + tokens.cache_creation_1h
    return tokens.cache_read / total if total else 0.0


def _normalize(values: dict[str, float]) -> dict[str, float]:
    total = sum(values.values())
    return {key: value / total for key, value in values.items()} if total else {key: 1 / len(values) for key in values}


def _balance(weights: dict[str, float]) -> float:
    if len(weights) < 2:
        return 1.0
    entropy = -sum(value * math.log(value) for value in weights.values() if value > 0)
    return (math.exp(entropy) - 1) / (len(weights) - 1)


def _smoothstep(value: float, floor: float, ceiling: float) -> float:
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 1.0
    progress = (value - floor) / (ceiling - floor)
    return progress * progress * (3 - 2 * progress)
