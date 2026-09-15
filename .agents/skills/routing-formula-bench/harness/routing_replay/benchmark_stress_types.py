from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from routing_replay.benchmark_quota import CapacityInference, QuotaWindowState
from routing_replay.benchmark_types import BenchmarkDecision, BenchmarkRow, BenchmarkSelector, Candidate


@dataclass(frozen=True, slots=True)
class StressConfig:
    burst_hours: float
    target_multiplier: float
    minimum_repeat_factor: int
    quota_capacity_multiplier: float = 1.0


@dataclass(frozen=True, slots=True)
class CapturedCandidate:
    candidate: Candidate
    observed_at_unix_secs: int


@dataclass(frozen=True, slots=True)
class ResetDrainOption:
    upstream_id: str
    resets_at_unix_secs: int
    remaining_5h_tokens: float
    simulated_cost_micros: int
    simulated_debit_tokens: float


@dataclass(frozen=True, slots=True)
class StressPlan:
    source_rows: tuple[BenchmarkRow, ...]
    capacities: dict[tuple[str, str], float]
    initial_candidates: tuple[CapturedCandidate, ...]
    config: StressConfig
    timestamp_anchor_ms: int
    repeat_factor: int
    reset_offset_source: str


@dataclass(frozen=True, slots=True)
class BlockedRequest:
    event_id: str
    request_id: str
    ts_unix_ms: int
    simulated_debit_tokens: float
    simulated_cost_opportunity_micros: int
    reason: str


@dataclass(frozen=True, slots=True)
class QuotaExhaustion:
    upstream_id: str
    window: str
    excluded_request_count: int


@dataclass(frozen=True, slots=True)
class StressResult:
    decisions: tuple[BenchmarkDecision, ...]
    blocked: tuple[BlockedRequest, ...]
    exclusions: dict[str, int]
    quota_exhaustions: tuple[QuotaExhaustion, ...]
    quota_windows: dict[tuple[str, str], QuotaWindowState]
    cache_entry_count: int
    total_rows: int
    reset_count: int


@dataclass(frozen=True, slots=True)
class StressReplay:
    source: Path
    selector: BenchmarkSelector
    capacity: CapacityInference
    plan: StressPlan
    result: StressResult
