from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


type Json = None | bool | int | float | str | list[Json] | dict[str, Json]


class Scenario(StrEnum):
    ACTUAL = "historical_actual"
    CURRENT = "current_formula"
    MIXTURE = "mixture_formula"


class Selector(StrEnum):
    MIXTURE = "mixture"
    MAX_HIT = "max-hit"


@dataclass(frozen=True, slots=True)
class Pricing:
    input_micros_per_million: int
    cache_read_micros_per_million: int
    cache_creation_5m_micros_per_million: int
    cache_creation_1h_micros_per_million: int


@dataclass(frozen=True, slots=True)
class CandidateTokens:
    cache_read: int
    cache_creation_5m: int
    cache_creation_1h: int
    uncached: int


@dataclass(frozen=True, slots=True)
class ObservedUsage:
    input_tokens: int
    cache_read: int
    cache_creation_5m: int
    cache_creation_1h: int


@dataclass(frozen=True, slots=True)
class TraceCandidate:
    upstream_id: str
    tier: str
    quota_weight_factor: float
    warning_multiplier: float
    effective_weight: float
    original_index: int


@dataclass(frozen=True, slots=True)
class CacheLookup:
    prefix_hash: str
    content_block_index: int


@dataclass(frozen=True, slots=True)
class CacheBreakpoint:
    prefix_hash: str
    token_count: int
    ttl_seconds: int
    lookups: tuple[CacheLookup, ...] = ()


@dataclass(frozen=True, slots=True)
class ParsedReplayRow:
    event_id: str
    request_id: str
    ts_unix_ms: int
    canonical_model: str
    disposition: str
    pricing: Pricing
    cache_breakpoints: tuple[CacheBreakpoint, ...]
    trace_candidates: tuple[TraceCandidate, ...]
    candidate_tokens: dict[str, CandidateTokens]
    chosen_tier: str
    routing_key: str
    rendezvous_salt_version: str
    actual_upstream_id: str | None
    current_formula_upstream_id: str | None
    observed_usage: ObservedUsage | None


@dataclass(frozen=True, slots=True)
class FormulaConstants:
    affinity_max: float
    low_hit_slope: float
    high_hit_power: float
    price_sensitivity: float
    gamma: float
    cache_confidence_power: float = 1.0
    quota_gate_power: float = 0.0
    quota_balance_floor: float = 0.35
    quota_balance_ceiling: float = 0.7


@dataclass(frozen=True, slots=True)
class WeightedCandidate:
    upstream_id: str
    probability: float
    original_index: int


@dataclass(frozen=True, slots=True)
class ReplayDecision:
    event_id: str
    request_id: str
    ts_unix_ms: int
    canonical_model: str
    disposition: str
    chosen_tier: str
    actual_upstream_id: str | None
    current_formula_upstream_id: str | None
    mixture_upstream_id: str
    alpha: float
    winner_probability: float
    actual_cost_observed_micros: int | None
    current_cost_predicted_micros: int | None
    mixture_cost_predicted_micros: int | None
    mixture_cost_blended_micros: int | None
    mixture_cost_basis: str
    actual_hit_ratio_observed: float | None
    current_hit_ratio_predicted: float | None
    mixture_hit_ratio_predicted: float | None
    current_equals_actual: bool
    mixture_equals_actual: bool
    mixture_equals_current: bool
    q_distribution_json: str
    affinity_json: str
    probability_json: str


@dataclass(frozen=True, slots=True)
class MetricRow:
    category: str
    scenario: str
    metric: str
    value: str


@dataclass(frozen=True, slots=True)
class ReplayRunResult:
    decisions: tuple[ReplayDecision, ...]
    exclusions: dict[str, int]
    total_rows: int
