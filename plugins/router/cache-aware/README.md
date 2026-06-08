# Cache-Aware Router Plugin

A router plugin for cc-lb that consumes v2 route wire data populated by the host and prefers upstreams predicted to have prompt-cache reuse. Cache observations and scoring inputs live in the host; the plugin only reads each candidate's `cache_score` and returns an upstream choice.

For the protocol-level reference (PDK macros, lifecycle, wire types, registration, limits, anti-patterns), see [docs/plugin-author-guide.md](../../../docs/plugin-author-guide.md).

## Wire version

Register this plugin with a manifest that declares `wire_version = 2` so the host dispatches `route` using `cc_lb_plugin_wire::v2::route::RouteRequest` and includes candidate `cache_score` data:

```toml
name = "cache-aware"
artifact = "target/wasm32-unknown-unknown/release/cache_aware_router.wasm"
wire_version = 2
config = {}
```

The PDK handshake still negotiates the implemented wire functions (`route`, `shape`, and `normalize_error`) at function version `1`; `PluginManifest.wire_version = Some(2)` is the host-side switch that selects v2 route payloads.

## Algorithm

`CacheScoreWire` currently has no literal `warm_match_count` field. The plugin therefore uses the available v2 host prediction as a binary warm-match proxy:

```text
score(candidate) = 1 if candidate.cache_score.predicted_cache_read_tokens > 0
                 = 0 otherwise
```

Pseudocode:

```text
if candidates is empty:
    return no upstream_id

max_score = max(score(candidate))

if max_score == 0:
    return candidates[counter.fetch_add(1, Relaxed) % candidates.len()]

warm_candidates = candidates where score(candidate) == max_score
max_read_tokens = max(candidate.cache_score.predicted_cache_read_tokens for warm_candidates)
best = warm_candidates where predicted_cache_read_tokens == max_read_tokens

if best has one candidate:
    return that candidate

return best[counter.fetch_add(1, Relaxed) % best.len()]
```

Tie-break order:

1. Highest cache score, currently the binary warm indicator above.
2. Highest `predicted_cache_read_tokens`.
3. Stable round-robin using a process-local `AtomicUsize` counter.

When every candidate has score `0` (missing `cache_score` or `predicted_cache_read_tokens == 0`), the plugin falls through to pure round-robin over all candidates. The counter is process-local and resets on plugin/process restart, matching the round-robin router plugin.

## Worked example

Given two candidates:

- Upstream A: conceptual `warm_match_count = 2`, surfaced to this plugin as `predicted_cache_read_tokens = 4096`, so `score = 1`.
- Upstream B: conceptual `warm_match_count = 0`, surfaced as no `cache_score` or `predicted_cache_read_tokens = 0`, so `score = 0`.

Upstream A is selected because its score is higher. If A and B both had `score = 1`, the one with the larger `predicted_cache_read_tokens` would win. If both had score `0`, selection would be `counter % candidates.len()` pure round-robin.

## Config knobs

There are no plugin configuration knobs. The only mutable state is the `AtomicUsize` round-robin counter used for cold fallthrough and exact cache-score ties.

## Build

```bash
rustup target add wasm32-unknown-unknown
cargo build -p cache-aware-router --target wasm32-unknown-unknown --release
```

The wasm artifact lands at `target/wasm32-unknown-unknown/release/cache_aware_router.wasm`.

## Wire surface implemented

- `route` — reads v2 `CandidateWire.cache_score`, prefers warm/read-heavy candidates, and returns `RouteResponse { upstream_id, dialect: SelfReferenced, upstream: AnthropicDirect }`.
- `shape` — concatenates the upstream base URL with the downstream request path and query, mirroring the reference round-robin plugin.
- `normalize_error` — returns `NormalizeErrorResponse { body_base64: None }`, opting into host pass-through on upstream errors.
