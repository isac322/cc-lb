from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from statistics import median

from routing_replay.benchmark_types import BenchmarkRow, Candidate, QuotaSnapshot, SimulatedTokens

WINDOW_SECONDS = {"5h": 18_000, "7d": 604_800}


@dataclass(frozen=True, slots=True)
class CapacityInference:
    capacities: dict[tuple[str, str], float]
    fallback_count: int
    missing_count: int


@dataclass(slots=True)
class QuotaWindow:  # noqa: MUTABLE_OK
    capacity: float
    used: float
    resets_at: int
    reset_count: int = 0


@dataclass(frozen=True, slots=True)
class QuotaWindowState:
    capacity: float
    used: float
    resets_at: int
    reset_count: int


def observed_debit(row: BenchmarkRow) -> float | None:
    usage = row.observed_usage
    if usage is None:
        return None
    return usage.input_tokens + usage.cache_creation_5m_tokens + usage.cache_creation_1h_tokens + 0.1 * usage.cache_read_tokens


def infer_capacities(rows: tuple[BenchmarkRow, ...]) -> CapacityInference:
    evidence: dict[tuple[str, str], list[float]] = defaultdict(list)
    previous: dict[tuple[str, str], tuple[QuotaSnapshot, float]] = {}
    accrued: dict[tuple[str, str], float] = defaultdict(float)
    for row in rows:
        for candidate in row.candidates:
            for snapshot in candidate.snapshots:
                key = candidate.upstream_id, snapshot.window
                old = previous.get(key)
                if old and snapshot.resets_at_unix_secs == old[0].resets_at_unix_secs:
                    delta = snapshot.utilization - old[0].utilization
                    inferred = accrued[key] / delta if delta > 0 else 0.0
                    if math.isfinite(inferred) and inferred > 0:
                        evidence[key].append(inferred)
                    accrued[key] = 0.0
                previous[key] = (snapshot, row.ts_unix_ms / 1000)
        debit = observed_debit(row)
        if row.actual_upstream_id and debit is not None:
            for candidate in row.candidates:
                if candidate.upstream_id == row.actual_upstream_id:
                    for snapshot in candidate.snapshots:
                        accrued[(candidate.upstream_id, snapshot.window)] += debit
    medians = {window: median(values) for window in WINDOW_SECONDS for values in [tuple(value for (_upstream, key_window), values in evidence.items() if key_window == window for value in values)] if values}
    capacities: dict[tuple[str, str], float] = {}
    fallback = 0
    missing = 0
    for key in previous:
        values = evidence.get(key, [])
        if values:
            capacities[key] = median(values)
        elif key[1] in medians:
            capacities[key] = medians[key[1]]
            fallback += 1
        else:
            missing += 1
    return CapacityInference(capacities, fallback, missing)


class QuotaState:  # noqa: MUTABLE_OK
    def __init__(self, capacities: dict[tuple[str, str], float]) -> None:
        self._capacities = capacities
        self._windows: dict[tuple[str, str], QuotaWindow] = {}

    def ensure(self, candidate: Candidate, now_secs: int) -> bool:
        for snapshot in candidate.snapshots:
            key = candidate.upstream_id, snapshot.window
            if key not in self._windows:
                capacity = self._capacities.get(key)
                if capacity is None:
                    return False
                self._windows[key] = QuotaWindow(capacity, capacity * snapshot.utilization, snapshot.resets_at_unix_secs)
            self._advance(key, now_secs)
        return True

    def seed(self, candidate: Candidate, observed_at_secs: int, timestamp_anchor_secs: int) -> bool:
        for snapshot in candidate.snapshots:
            key = candidate.upstream_id, snapshot.window
            if key not in self._windows:
                capacity = self._capacities.get(key)
                if capacity is None:
                    return False
                self._windows[key] = QuotaWindow(capacity, capacity * snapshot.utilization, timestamp_anchor_secs + snapshot.resets_at_unix_secs - observed_at_secs)
        return True

    def advance(self, candidates: tuple[Candidate, ...], now_secs: int) -> None:
        for candidate in candidates:
            self.ensure(candidate, now_secs)

    def can_cover(self, candidate: Candidate, tokens: SimulatedTokens) -> bool:
        return all(self._windows[candidate.upstream_id, window].capacity - self._windows[candidate.upstream_id, window].used >= tokens.debit for window in WINDOW_SECONDS)

    def exhausted_windows(self, candidate: Candidate, debit: float) -> tuple[str, ...]:
        return tuple(window for window in WINDOW_SECONDS if self._windows[candidate.upstream_id, window].capacity - self._windows[candidate.upstream_id, window].used < debit)

    def used(self, upstream_id: str, window: str) -> float:
        return self._windows[upstream_id, window].used

    def window_states(self) -> dict[tuple[str, str], QuotaWindowState]:
        return {key: QuotaWindowState(value.capacity, value.used, value.resets_at, value.reset_count) for key, value in self._windows.items()}

    def five_hour_state(self, upstream_id: str) -> QuotaWindowState:
        window = self._windows[upstream_id, "5h"]
        return QuotaWindowState(window.capacity, window.used, window.resets_at, window.reset_count)

    def urgency(self, upstream_id: str, now_secs: int) -> float:
        pressures = [self._pressure(self._windows[(upstream_id, window)], window, now_secs) for window in WINDOW_SECONDS]
        return sum(pressure**6 for pressure in pressures) ** (1 / 6)

    def debit(self, upstream_id: str, tokens: SimulatedTokens) -> None:
        for window in WINDOW_SECONDS:
            self._windows[(upstream_id, window)].used += tokens.debit

    def reset_count(self) -> int:
        return sum(window.reset_count for window in self._windows.values())

    def _advance(self, key: tuple[str, str], now_secs: int) -> None:
        window = self._windows[key]
        while now_secs >= window.resets_at:
            window.used = 0.0
            window.resets_at += WINDOW_SECONDS[key[1]]
            window.reset_count += 1

    def _pressure(self, window: QuotaWindow, name: str, now_secs: int) -> float:
        remaining = max(1 - window.used / window.capacity, 0.0)
        time_ratio = min(max((window.resets_at - now_secs) / WINDOW_SECONDS[name], 0.0), 1.0)
        target = max(time_ratio ** (1.0 if name == "5h" else 1.3), 0.01)
        return max(math.log(remaining / target), 0.0) if remaining > 0 else 0.0
