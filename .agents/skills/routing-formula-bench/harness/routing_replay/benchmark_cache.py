from __future__ import annotations

from dataclasses import dataclass

from routing_replay.benchmark_types import Breakpoint, SimulatedTokens


@dataclass(frozen=True, slots=True)
class CachedPrefix:
    token_count: int
    expires_at_ms: int


class CacheState:  # noqa: MUTABLE_OK
    def __init__(self) -> None:
        self._by_upstream: dict[str, dict[str, CachedPrefix]] = {}

    def tokens_for(self, upstream_id: str, breakpoints: tuple[Breakpoint, ...], input_tokens: int, now_ms: int) -> SimulatedTokens:
        target = max((breakpoint.token_count for breakpoint in breakpoints), default=0)
        hit = self._best_hit(upstream_id, breakpoints, now_ms)
        hit_tokens = min(hit.token_count, target) if hit else 0
        creation = max(target - hit_tokens, 0)
        one_hour = bool(breakpoints) and breakpoints[-1].ttl_seconds == 3600
        return SimulatedTokens(max(input_tokens - target, 0), hit_tokens, 0 if one_hour else creation, creation if one_hour else 0)

    def record_success(self, upstream_id: str, breakpoints: tuple[Breakpoint, ...], now_ms: int) -> None:
        stored = self._by_upstream.setdefault(upstream_id, {})
        for breakpoint in breakpoints:
            stored[breakpoint.prefix_hash] = CachedPrefix(breakpoint.token_count, now_ms + breakpoint.ttl_seconds * 1000)

    def entry_count(self) -> int:
        return sum(len(prefixes) for prefixes in self._by_upstream.values())

    def _best_hit(self, upstream_id: str, breakpoints: tuple[Breakpoint, ...], now_ms: int) -> CachedPrefix | None:
        cached = self._by_upstream.get(upstream_id, {})
        live = [cached[key] for breakpoint in breakpoints for key in (breakpoint.prefix_hash, *breakpoint.lookback_hashes) if key in cached and cached[key].expires_at_ms > now_ms]
        return max(live, key=lambda prefix: prefix.token_count, default=None)
