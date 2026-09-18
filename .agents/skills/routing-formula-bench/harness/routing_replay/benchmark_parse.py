from __future__ import annotations

import json

from routing_replay.benchmark_types import (
    BenchmarkPricing,
    BenchmarkRow,
    Breakpoint,
    Candidate,
    ObservedInputUsage,
    QuotaSnapshot,
)
from routing_replay.types import Json


def parse_capture_row(row: tuple[Json, ...]) -> tuple[BenchmarkRow | None, str]:
    payload = _map(_json_text(row[10] if len(row) > 10 else None))
    if payload is None:
        return None, "invalid_payload_json"
    input_map = _map(payload.get("input"))
    event_id, request_id, timestamp = _text(row[0]), _text(row[1]), _integer(row[2])
    if input_map is None or event_id is None or request_id is None or timestamp is None:
        return None, "missing_required_columns"
    return _parse_input(
        input_map,
        event_id,
        request_id,
        timestamp,
        _text(row[5]) or "unknown",
        _text(row[3]) or "unknown",
        _text(row[4]),
        _observed(row),
    )


def parse_benchmark_row(payload_text: str) -> tuple[BenchmarkRow | None, str]:
    payload = _map(_json_text(payload_text))
    if payload is None:
        return None, "invalid_payload_json"
    input_map = _map(payload.get("input"))
    event_id, request_id, timestamp = _text(payload.get("event_id")), _text(payload.get("request_id")), _integer(payload.get("ts_unix_ms"))
    if input_map is None or event_id is None or request_id is None or timestamp is None:
        return None, "missing_required_columns"
    return _parse_input(input_map, event_id, request_id, timestamp, "routed_dispatched_success", "fixture", None, None)


def _parse_input(
    input_map: dict[str, Json], event_id: str, request_id: str, timestamp: int, disposition: str,
    model: str, actual: str | None, observed: ObservedInputUsage | None,
) -> tuple[BenchmarkRow | None, str]:
    pricing = _pricing(input_map.get("cache_pricing"))
    if pricing is None:
        return None, "missing_pricing"
    candidates = _candidates(input_map.get("candidates"), input_map.get("routing_trace"))
    if candidates is None:
        return None, "missing_quota_snapshot"
    if not candidates:
        return None, "missing_trace_candidates"
    breakpoints = _breakpoints(input_map.get("breakpoints"))
    if not breakpoints:
        return None, "missing_cache_breakpoints"
    return BenchmarkRow(event_id, request_id, timestamp, disposition, model, actual, _current(input_map.get("routing_trace")), pricing, breakpoints, candidates, observed), ""


def _pricing(value: Json | None) -> BenchmarkPricing | None:
    mapping = _map(value)
    if mapping is None:
        return None
    values = tuple(_integer(mapping.get(key)) for key in (
        "input_micros_per_million", "cache_read_micros_per_million",
        "cache_creation_5m_micros_per_million", "cache_creation_1h_micros_per_million",
    ))
    if any(value is None or value < 0 for value in values):
        return None
    return BenchmarkPricing(values[0] or 0, values[1] or 0, values[2] or 0, values[3] or 0)


def _candidates(value: Json | None, trace_value: Json | None) -> tuple[Candidate, ...] | None:
    trace = _trace_candidates(trace_value)
    if trace is None:
        return None
    parsed: list[Candidate] = []
    for item in _list(value) or []:
        mapping = _map(item)
        upstream = _text(mapping.get("upstream_id")) if mapping else None
        snapshots = _snapshots(mapping.get("subscription_quotas")) if mapping else None
        if upstream is None or snapshots is None:
            return None
        trace_item = trace.get(upstream)
        if trace_item is None:
            continue
        parsed.append(Candidate(upstream, _number(trace_item.get("quota_weight_factor")) or 1.0, _number(trace_item.get("quota_urgency_combined")) or 0.0, snapshots))
    return tuple(parsed)


def _snapshots(value: Json | None) -> tuple[QuotaSnapshot, ...] | None:
    wanted: dict[str, QuotaSnapshot] = {}
    for item in _list(value) or []:
        mapping = _map(item)
        if mapping is None:
            continue
        window, utilization, reset = _text(mapping.get("window")), _number(mapping.get("utilization")), _integer(mapping.get("resets_at_unix_secs"))
        if window in {"5h", "7d"} and utilization is not None and utilization >= 0 and reset is not None:
            wanted[window] = QuotaSnapshot(window, utilization, reset)
    return tuple(wanted[key] for key in ("5h", "7d")) if len(wanted) == 2 else None


def _trace_candidates(value: Json | None) -> dict[str, dict[str, Json]] | None:
    trace = _map(value)
    stages = _list(trace.get("stages")) if trace else None
    if stages is None:
        return None
    for stage in stages:
        stage_map = _map(stage)
        subscription = _map(stage_map.get("subscription_preference")) if stage_map else None
        if subscription is not None:
            return {upstream: mapping for item in _list(subscription.get("candidates")) or [] if (mapping := _map(item)) and (upstream := _text(mapping.get("upstream_id")))}
    return None


def _current(value: Json | None) -> str | None:
    trace = _map(value)
    stages = _list(trace.get("stages")) if trace else ()
    for stage in stages or ():
        stage_map = _map(stage)
        subscription = _map(stage_map.get("subscription_preference")) if stage_map else None
        if subscription is not None:
            return _text(subscription.get("formula_winner_upstream_id"))
    return None


def _breakpoints(value: Json | None) -> tuple[Breakpoint, ...]:
    result: list[Breakpoint] = []
    for item in _list(value) or []:
        mapping = _map(item)
        if mapping is None:
            continue
        prefix = _text(mapping.get("prefix_hash"))
        count = _integer(mapping.get("prefix_token_count"))
        if prefix is not None and count is not None and count > 0:
            lookbacks: list[str] = []
            for lookback in _list(mapping.get("lookback_prefixes")) or []:
                lookback_map = _map(lookback)
                lookback_hash = _text(lookback_map.get("prefix_hash")) if lookback_map else None
                if lookback_hash is not None:
                    lookbacks.append(lookback_hash)
            result.append(Breakpoint(prefix, count, 3600 if _text(mapping.get("requested_ttl")) == "ephemeral1h" else 300, tuple(lookbacks)))
    return tuple(result)


def _observed(row: tuple[Json, ...]) -> ObservedInputUsage | None:
    values = tuple(_integer(row[index] if len(row) > index else None) for index in (6, 7, 8, 9))
    if any(value is None or value < 0 for value in values):
        return None
    return ObservedInputUsage(values[0] or 0, values[1] or 0, values[2] or 0, values[3] or 0)


def _json_text(value: Json | None) -> Json | None:
    if not isinstance(value, str):
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _map(value: Json | None) -> dict[str, Json] | None:
    return value if isinstance(value, dict) else None


def _list(value: Json | None) -> list[Json] | None:
    return value if isinstance(value, list) else None


def _text(value: Json | None) -> str | None:
    return value if isinstance(value, str) else None


def _integer(value: Json | None) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _number(value: Json | None) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None
