from __future__ import annotations

import math
from collections import Counter
from dataclasses import replace
from pathlib import Path

from routing_replay.benchmark_cache import CacheState
from routing_replay.benchmark_engine import load_rows
from routing_replay.benchmark_quota import CapacityInference, QuotaState, infer_capacities
from routing_replay.benchmark_select import _cost, _hit, select
from routing_replay.benchmark_stress_types import BlockedRequest, CapturedCandidate, QuotaExhaustion, ResetDrainOption, StressConfig, StressPlan, StressReplay, StressResult
from routing_replay.benchmark_types import BenchmarkDecision, BenchmarkRow, BenchmarkSelector, Candidate, SimulatedTokens


RESET_OFFSET_SOURCE = "captured_resets_at_unix_secs_minus_first_candidate_observation_secs"


def run_stress(source: Path, selector: BenchmarkSelector, config: StressConfig) -> StressReplay:
    rows, _exclusions, _total = load_rows(source)
    capacity = infer_capacities(rows)
    plan = build_stress_plan(rows, capacity.capacities, config)
    return StressReplay(source, selector, capacity, plan, replay_stress(plan, selector))


def build_stress_plan(rows: tuple[BenchmarkRow, ...], capacities: dict[tuple[str, str], float], config: StressConfig) -> StressPlan:
    initial = _initial_candidates(rows)
    remaining = sum(_remaining_5h(candidate, capacities) for candidate in initial)
    observed_debit = sum(_observed_debit(row) for row in rows)
    computed_repeat = math.ceil(remaining * config.target_multiplier / observed_debit) if observed_debit else config.minimum_repeat_factor
    repeat_factor = max(config.minimum_repeat_factor, computed_repeat)
    anchor = rows[0].ts_unix_ms if rows else 0
    scaled = {key: capacity * config.quota_capacity_multiplier for key, capacity in capacities.items()}
    return StressPlan(rows, scaled, initial, config, anchor, repeat_factor, RESET_OFFSET_SOURCE)


def replay_stress(plan: StressPlan, selector: BenchmarkSelector) -> StressResult:
    cache, quota = CacheState(), QuotaState(plan.capacities)
    for captured in plan.initial_candidates:
        quota.seed(captured.candidate, captured.observed_at_unix_secs, plan.timestamp_anchor_ms // 1000)
    decisions: list[BenchmarkDecision] = []
    blocked: list[BlockedRequest] = []
    exclusions: Counter[str] = Counter()
    exhaustion_counts: Counter[tuple[str, str]] = Counter()
    for row in _burst_rows(plan):
        outcome = _replay_row(row, selector, cache, quota)
        match outcome:
            case BenchmarkDecision() as decision:
                decisions.append(decision)
            case BlockedRequest() as request:
                blocked.append(request)
                for candidate in row.candidates:
                    for window in quota.exhausted_windows(candidate, request.simulated_debit_tokens):
                        exhaustion_counts[candidate.upstream_id, window] += 1
            case str() as exclusion:
                exclusions[exclusion] += 1
    exhausted = tuple(QuotaExhaustion(upstream, window, count) for (upstream, window), count in sorted(exhaustion_counts.items()))
    return StressResult(tuple(decisions), tuple(blocked), dict(exclusions), exhausted, quota.window_states(), cache.entry_count(), len(plan.source_rows) * plan.repeat_factor, quota.reset_count())


def _initial_candidates(rows: tuple[BenchmarkRow, ...]) -> tuple[CapturedCandidate, ...]:
    captured: dict[str, CapturedCandidate] = {}
    for row in rows:
        for candidate in row.candidates:
            captured.setdefault(candidate.upstream_id, CapturedCandidate(candidate, row.ts_unix_ms // 1000))
    return tuple(captured[key] for key in sorted(captured))


def _remaining_5h(captured: CapturedCandidate, capacities: dict[tuple[str, str], float]) -> float:
    snapshot = next(snapshot for snapshot in captured.candidate.snapshots if snapshot.window == "5h")
    return max(capacities[captured.candidate.upstream_id, "5h"] * (1 - snapshot.utilization), 0.0)


def _observed_debit(row: BenchmarkRow) -> float:
    usage = row.observed_usage
    return 0.0 if usage is None else usage.input_tokens + usage.cache_creation_5m_tokens + usage.cache_creation_1h_tokens + 0.1 * usage.cache_read_tokens


def _burst_rows(plan: StressPlan) -> tuple[BenchmarkRow, ...]:
    total = len(plan.source_rows) * plan.repeat_factor
    if total == 0:
        return ()
    burst_ms = round(plan.config.burst_hours * 3_600_000)
    return tuple(replace(row, event_id=f"{row.event_id}:stress:{index}", request_id=f"{row.request_id}:stress:{index}", ts_unix_ms=plan.timestamp_anchor_ms + min((index * burst_ms) // total, burst_ms - 1)) for index, row in enumerate(plan.source_rows * plan.repeat_factor))


def _replay_row(row: BenchmarkRow, selector: BenchmarkSelector, cache: CacheState, quota: QuotaState) -> BenchmarkDecision | BlockedRequest | str:
    observed = row.observed_usage
    if observed is None:
        return "missing_observed_usage"
    now_secs = row.ts_unix_ms // 1000
    quota.advance(row.candidates, now_secs)
    tokens = {candidate.upstream_id: cache.tokens_for(candidate.upstream_id, row.breakpoints, observed.input_tokens, row.ts_unix_ms) for candidate in row.candidates}
    eligible = tuple(candidate for candidate in row.candidates if quota.can_cover(candidate, tokens[candidate.upstream_id]))
    if not eligible:
        opportunity = min(row.candidates, key=lambda candidate: (_cost(tokens[candidate.upstream_id], row.pricing), tokens[candidate.upstream_id].debit, candidate.upstream_id))
        opportunity_tokens = tokens[opportunity.upstream_id]
        return BlockedRequest(row.event_id, row.request_id, row.ts_unix_ms, opportunity_tokens.debit, _cost(opportunity_tokens, row.pricing), "all_upstreams_exhausted")
    urgencies = {candidate.upstream_id: quota.urgency(candidate.upstream_id, now_secs) for candidate in eligible}
    match selector:
        case BenchmarkSelector.EARLIEST_5H_RESET:
            options = tuple(
                ResetDrainOption(candidate.upstream_id, state.resets_at, state.capacity - state.used, _cost(tokens[candidate.upstream_id], row.pricing), tokens[candidate.upstream_id].debit)
                for candidate in eligible
                for state in (quota.five_hour_state(candidate.upstream_id),)
            )
            selected, reason = _select_earliest_5h_reset(options), ""
        case (BenchmarkSelector.ACTUAL_CHOICE | BenchmarkSelector.CURRENT | BenchmarkSelector.SMOOTHSTEP_A | BenchmarkSelector.LOWERED_GATE | BenchmarkSelector.MAX_HIT | BenchmarkSelector.MIN_COST | BenchmarkSelector.QUOTA_AWARE_MIN_COST | BenchmarkSelector.COST_FIRST):
            selected, reason = select(selector, eligible, tokens, urgencies, row.pricing, row.actual_upstream_id, row.current_upstream_id)
    if selected is None:
        return reason
    selected_tokens = tokens[selected]
    selected_candidate = next(candidate for candidate in eligible if candidate.upstream_id == selected)
    dynamic_top = max(eligible, key=lambda candidate: (urgencies[candidate.upstream_id], candidate.upstream_id)).upstream_id
    trace_top = max(eligible, key=lambda candidate: (candidate.trace_urgency, candidate.upstream_id)).upstream_id
    quota.debit(selected, selected_tokens)
    cache.record_success(selected, row.breakpoints, row.ts_unix_ms)
    observed_total = observed.input_tokens + observed.cache_read_tokens + observed.cache_creation_5m_tokens + observed.cache_creation_1h_tokens
    input_side = selected_tokens.uncached_input + selected_tokens.cache_read + selected_tokens.cache_creation_5m + selected_tokens.cache_creation_1h
    return BenchmarkDecision(row.event_id, row.request_id, row.ts_unix_ms, row.actual_upstream_id, selected, selector.value, _cost(selected_tokens, row.pricing), selected_tokens.debit, selected_tokens.cache_read, input_side, _hit(selected_tokens), observed.cache_read_tokens / observed_total if observed_total else 0.0, urgencies[selected], selected_candidate.trace_urgency, dynamic_top == trace_top, selected != row.actual_upstream_id)


def _select_earliest_5h_reset(options: tuple[ResetDrainOption, ...]) -> str:
    return min(options, key=lambda option: (option.resets_at_unix_secs, option.remaining_5h_tokens, option.simulated_cost_micros, option.simulated_debit_tokens, option.upstream_id)).upstream_id
