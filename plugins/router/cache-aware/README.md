# Cache-Aware Router Filter Plugin

A router filter plugin for cc-lb that consumes v3 `filter` wire data populated by the host and keeps the upstream candidates predicted to have the best prompt-cache reuse. Cache observations and scoring inputs live in the host; the plugin only reads each candidate's `cache_score` and returns per-candidate accept/reject decisions.

For the protocol-level reference (PDK macros, lifecycle, wire types, registration, limits, anti-patterns), see [docs/plugin-author-guide.md](../../../docs/plugin-author-guide.md).

## Wire version

Register this plugin with a manifest that declares `wire_version = 3` so the host dispatches `filter` using `cc_lb_plugin_wire::v3::filter::FilterRequest` and expects `FilterResponse` decisions:

```toml
name = "cache-aware"
artifact = "target/wasm32-unknown-unknown/release/cache_aware_router.wasm"
wire_version = 3
config = { keep_k = 1 }
```

The plugin implements only the v3 `filter` wire function. It does not implement `route`, `shape`, or `normalize_error`; final terminal selection is host responsibility.

## Config

`keep_k` controls how many candidates survive the filter:

```json
{ "keep_k": 3 }
```

Rules:

- Missing `keep_k` defaults to `1`, preserving the old v2 single-pick behavior by allowing exactly one candidate through.
- `keep_k = 0` is clamped to `1`.
- `keep_k` larger than the candidate count keeps every candidate.

## Algorithm

`CacheScoreWire` currently has no literal `warm_match_count` field. The plugin therefore uses the available host prediction as a binary warm-match proxy:

```text
score(candidate) = 1 if candidate.cache_score.predicted_cache_read_tokens > 0
                 = 0 otherwise
```

Pseudocode:

```text
if candidates is empty:
    return no per-candidate results

k = max(config.keep_k or 1, 1)
rank candidates by:
    1. score(candidate) descending
    2. predicted_cache_read_tokens descending
    3. stable input order

keep the top min(k, candidates.len()) candidates
return one result for each input candidate:
    kept candidate: decision = "accept", reason = "top-K by cache_score"
    dropped candidate: decision = "reject", reason = "below K by cache_score"
```

The response preserves input candidate order. Ranking is used only to decide membership in the kept set; the plugin does not emulate or replace host terminal strategy.

## Worked example

Given five candidates and `keep_k = 3`:

- Upstream A: no `cache_score`, so `score = 0`.
- Upstream B: `predicted_cache_read_tokens = 512`, so `score = 1`.
- Upstream C: `predicted_cache_read_tokens = 8192`, so `score = 1`.
- Upstream D: `predicted_cache_read_tokens = 0`, so `score = 0`.
- Upstream E: `predicted_cache_read_tokens = 4096`, so `score = 1`.

The kept set is C, E, and B. The emitted response still has one result per input candidate, in input order.

## Build

```bash
rustup target add wasm32-unknown-unknown
cargo build -p cache-aware-router --target wasm32-unknown-unknown --release
```

The wasm artifact lands at `target/wasm32-unknown-unknown/release/cache_aware_router.wasm`.

## Verification

```bash
cargo test -p cache-aware-router --test v3_behavior
```
