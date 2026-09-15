from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class BenchmarkSelector(StrEnum):
    ACTUAL_CHOICE = "actual-choice"
    CURRENT = "current"
    SMOOTHSTEP_A = "smoothstep-a"
    LOWERED_GATE = "lowered-gate"
    MAX_HIT = "max-hit"
    MIN_COST = "min-cost"
    QUOTA_AWARE_MIN_COST = "quota-aware-min-cost"
    COST_FIRST = "cost-first"
    EARLIEST_5H_RESET = "earliest-5h-reset"


@dataclass(frozen=True, slots=True)
class BenchmarkPricing:
    input_micros_per_million: int
    cache_read_micros_per_million: int
    cache_creation_5m_micros_per_million: int
    cache_creation_1h_micros_per_million: int


@dataclass(frozen=True, slots=True)
class Breakpoint:
    prefix_hash: str
    token_count: int
    ttl_seconds: int
    lookback_hashes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class QuotaSnapshot:
    window: str
    utilization: float
    resets_at_unix_secs: int


@dataclass(frozen=True, slots=True)
class Candidate:
    upstream_id: str
    current_weight: float
    trace_urgency: float
    snapshots: tuple[QuotaSnapshot, ...]


@dataclass(frozen=True, slots=True)
class ObservedInputUsage:
    input_tokens: int
    cache_read_tokens: int
    cache_creation_5m_tokens: int
    cache_creation_1h_tokens: int


@dataclass(frozen=True, slots=True)
class SimulatedTokens:
    uncached_input: int
    cache_read: int
    cache_creation_5m: int
    cache_creation_1h: int

    @property
    def debit(self) -> float:
        return (
            self.uncached_input
            + self.cache_creation_5m
            + self.cache_creation_1h
            + 0.1 * self.cache_read
        )


@dataclass(frozen=True, slots=True)
class BenchmarkRow:
    event_id: str
    request_id: str
    ts_unix_ms: int
    disposition: str
    canonical_model: str
    actual_upstream_id: str | None
    current_upstream_id: str | None
    pricing: BenchmarkPricing
    breakpoints: tuple[Breakpoint, ...]
    candidates: tuple[Candidate, ...]
    observed_usage: ObservedInputUsage | None


@dataclass(frozen=True, slots=True)
class BenchmarkDecision:
    event_id: str
    request_id: str
    ts_unix_ms: int
    actual_upstream_id: str | None
    selected_upstream_id: str
    selector: str
    simulated_cost_micros: int
    simulated_debit_tokens: float
    cache_read_tokens: int
    simulated_input_side_tokens: int
    hit_ratio: float
    observed_hit_ratio: float
    quota_urgency: float
    trace_urgency: float
    urgency_rank_top_matches: bool
    changed_from_actual: bool


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    decisions: tuple[BenchmarkDecision, ...]
    exclusions: dict[str, int]
    total_rows: int
    capacity_fallback_count: int
    capacity_missing_count: int
    reset_count: int
