from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

from routing_replay.benchmark_cache import CacheState
from routing_replay.benchmark_parse import parse_capture_row
from routing_replay.benchmark_quota import CapacityInference, QuotaState, infer_capacities
from routing_replay.benchmark_select import _cost, _hit, select
from routing_replay.benchmark_types import BenchmarkDecision, BenchmarkResult, BenchmarkRow, BenchmarkSelector
from routing_replay.types import Json


def load_rows(source: Path) -> tuple[tuple[BenchmarkRow, ...], Counter[str], int]:
    parsed: list[BenchmarkRow] = []
    exclusions: Counter[str] = Counter()
    with sqlite3.connect(f"file:{source}?mode=ro", uri=True) as connection:
        total = int(connection.execute("select count(*) from capture_v1").fetchone()[0])
        query = "select event_id,request_id,ts_unix_ms,canonical_model,chosen_upstream_id,disposition,input_tokens,cache_read_input_tokens,cache_creation_5m,cache_creation_1h,payload_json from capture_v1 order by ts_unix_ms,event_id"
        for raw in connection.execute(query):
            row, reason = parse_capture_row(tuple(raw))
            if row is None:
                exclusions[reason] += 1
            elif row.observed_usage is None:
                exclusions["missing_observed_usage"] += 1
            elif row.disposition != "routed_dispatched_success":
                exclusions["non_success_disposition"] += 1
            else:
                parsed.append(row)
    return tuple(parsed), exclusions, total


def replay(source: Path, selector: BenchmarkSelector) -> tuple[BenchmarkResult, CapacityInference]:
    rows, exclusions, total = load_rows(source)
    capacity = infer_capacities(rows)
    cache, quota = CacheState(), QuotaState(capacity.capacities)
    decisions: list[BenchmarkDecision] = []
    for row in rows:
        result = _replay_row(row, selector, cache, quota)
        if isinstance(result, str):
            exclusions[result] += 1
        else:
            decisions.append(result)
    return BenchmarkResult(tuple(decisions), dict(exclusions), total, capacity.fallback_count, capacity.missing_count, quota.reset_count()), capacity


def _replay_row(row: BenchmarkRow, selector: BenchmarkSelector, cache: CacheState, quota: QuotaState) -> BenchmarkDecision | str:
    now_secs = row.ts_unix_ms // 1000
    if not all(quota.ensure(candidate, now_secs) for candidate in row.candidates):
        return "capacity_unavailable"
    if row.observed_usage is None:
        return "missing_observed_usage"
    tokens = {candidate.upstream_id: cache.tokens_for(candidate.upstream_id, row.breakpoints, row.observed_usage.input_tokens, row.ts_unix_ms) for candidate in row.candidates}
    urgencies = {candidate.upstream_id: quota.urgency(candidate.upstream_id, now_secs) for candidate in row.candidates}
    selected, reason = select(selector, row.candidates, tokens, urgencies, row.pricing, row.actual_upstream_id, row.current_upstream_id)
    if selected is None:
        return reason
    selected_tokens = tokens[selected]
    selected_candidate = next(candidate for candidate in row.candidates if candidate.upstream_id == selected)
    dynamic_top = max(row.candidates, key=lambda candidate: (urgencies[candidate.upstream_id], candidate.upstream_id)).upstream_id
    trace_top = max(row.candidates, key=lambda candidate: (candidate.trace_urgency, candidate.upstream_id)).upstream_id
    quota.debit(selected, selected_tokens)
    cache.record_success(selected, row.breakpoints, row.ts_unix_ms)
    observed_total = row.observed_usage.input_tokens + row.observed_usage.cache_read_tokens + row.observed_usage.cache_creation_5m_tokens + row.observed_usage.cache_creation_1h_tokens
    observed_hit = row.observed_usage.cache_read_tokens / observed_total if observed_total else 0.0
    simulated_input_side = selected_tokens.uncached_input + selected_tokens.cache_read + selected_tokens.cache_creation_5m + selected_tokens.cache_creation_1h
    return BenchmarkDecision(row.event_id, row.request_id, row.ts_unix_ms, row.actual_upstream_id, selected, selector.value, _cost(selected_tokens, row.pricing), selected_tokens.debit, selected_tokens.cache_read, simulated_input_side, _hit(selected_tokens), observed_hit, urgencies[selected], selected_candidate.trace_urgency, dynamic_top == trace_top, selected != row.actual_upstream_id)
