# ADR 0006 — Local prompt-cache simulator and expected cache value routing

- Status: Proposed
- Date: 2026-07-07
- Supersedes: ADR 0004/0005's assumption that exact-prefix observations plus thread memory are sufficient long-term cache-locality signals.
- Related incident data: `.omo/ulw-research/20260707-171508/full-post-v10-snapshot.json` captured 129 post-v10 request rows, 126 cache-positive rows, and 129/129 `request_id`-keyed routing decisions.

## Context

The v10 subscription-preference route used this effective shape inside a quota tier:

```text
effective_weight_i = quota_weight_i * exp(beta * cache_ratio_i)
```

That failed because `cache_ratio_i` was fed by exact local prefix observations only. A cacheable request with no exact local warm prefix produced a creation-only `CacheScore` (`read = 0`, `create > 0`), and that `Some(CacheScore)` masked same-thread provider-usage memory. The router then saw no positive priced cache value, used `request_id` WRH, and scattered cache-positive sessions across upstreams.

The deeper design gap is that a route decision needs an expected cache value, not a binary exact-prefix hit flag. Anthropic exposes provider-side usage after a request (`cache_read_input_tokens`, `cache_creation_input_tokens`, TTL class), but it does not expose the provider's internal prompt-cache hash/key algorithm or the exact prefix hash that was read. A local simulator therefore cannot be a byte-for-byte Anthropic oracle. It can, however, maintain a deterministic proxy-local fingerprint of the same rendered prefix structure and combine it with TTL, lookback, token estimates, and observed provider usage.

Three constraints define the simulator:

1. **Hash matching is byte/content-like, cost is token-priced.** Hit likelihood comes from local prefix fingerprints and cache-entry TTLs. Savings comes from estimated Anthropic billing tokens.
2. **Anthropic's tokenizer is private.** Pre-route token counts must use observed provider token counts when available, otherwise a local estimator corrected by drift metrics.
3. **Prompt caching uses a 20-block lookback.** For a current breakpoint at block `N`, the simulator must consider prefix positions `N`, `N-1`, ..., `N-19`; hashing only the current breakpoint misses valid provider lookback hits.

## Decision

Introduce a local prompt-cache simulator that computes expected cache value per upstream:

```text
expected_cache_value_i =
  P_hit_i * read_tokens_i * (input_price - cache_read_price)
  - (1 - P_hit_i) * create_tokens_i * cache_write_price
```

and feeds subscription preference with:

```text
effective_weight_i = quota_weight_i * exp(beta * normalize(expected_cache_value_i))
```

where `P_hit_i` is a confidence score, not a token-hit ratio. `P_hit_i` is derived from local fingerprint evidence:

```text
P_hit_i = 0.95  if exact local prefix-chain hash matches an unexpired upstream entry
        = 0.60  if no exact hash matches but same-thread/provider usage is recent
        = 0.00  otherwise
```

The confidence constants are deliberately conservative and trace-visible. They are routing priors, not claims about Anthropic internals.

### Local keying

Anthropic's internal key remains undisclosed. The proxy uses its own domain-separated key:

```text
prefix_digest[0] = H("cc-lb-cache-v3" || model || tools || system || tool_choice || thinking)
block_digest[k] = H("message-block" || canonical_message_block[k])
prefix_digest[k] = H("prefix" || prefix_digest[k-1] || block_digest[k])
```

This is a proxy-local fingerprint. It exists to recognize equivalent prefixes across requests and upstreams; it does not attempt to reproduce Anthropic's private key derivation.

### 20-block lookback

For a breakpoint at block `N`, evaluate candidates by array lookup over the prefix chain:

```text
candidate_positions = max(0, N - 19) ..= N
best_hit = longest unexpired cache entry whose prefix_digest[position] exists on upstream_i
```

This avoids rebuilding and rehashing 20 full JSON prefixes. Request processing computes block digests and the prefix chain once, then performs up to 20 hash-store lookups per breakpoint/upstream.

### Token estimates

Each cache entry stores token metadata alongside the local hash:

```text
CacheEntry {
  prefix_digest,
  canonical_model,
  ttl_class,
  expires_at,
  prefix_bytes_len,
  estimated_tokens,
  last_provider_read_tokens,
  last_provider_creation_tokens,
  confidence,
}
```

Token estimate priority:

1. Use provider-observed read/create token counts for the same local prefix digest when available.
2. Use a model-specific local token estimate adjusted by the observed drift histogram/EWMA.
3. Fall back to a bytes-per-token EWMA for the canonical prefix bytes.

### Routing fallback

Until the full v3 simulator is live for all request shapes, candidate cache value uses a value-aware merge:

```text
E_i = value(exact-prefix observation score)
M_i = value(same-thread provider-usage memory)
S_i = simulator expected value if available, else max(E_i, M_i)
```

Exact simulator evidence beats same-thread memory. Same-thread memory only prevents known cache-positive threads from being treated as cold when exact prefix observations are missing or delayed.

## Consequences

### Positive

- Cache-aware routing no longer depends on exact current-breakpoint observations alone.
- The router can model Anthropic's 20-block lookback without 20 full prefix serializations per breakpoint.
- Expected value makes the routing trade-off explicit in money-like units instead of binary warm/cold flags.
- All-cold traffic still spreads by request-id WRH because `P_hit = 0` and no positive cache value exists.
- Trace fields can explain whether the route was driven by exact hash, lookback hash, same-thread memory, or no cache evidence.

### Negative

- The simulator is not an Anthropic internal-key oracle. Eviction, tokenizer drift, concurrency races, and provider-internal cache topology remain uncertainty sources.
- Prefix-chain v3 keys are not backward-compatible with v2 persisted prefix hashes; rollout must support v2 exact observations while v3 warms up.
- Token estimation needs calibration. Poor drift correction can overprice or underprice cache value.
- More trace surface is required: confidence, value, source, lookback distance, and token-estimate source.

### Neutral

- This decision does not require changing the subscription quota tier ordering. Hard quota/tier eviction still happens before cache value affects within-tier selection.
- `cache_read_input_tokens` and `cache_creation_input_tokens` remain the source of truth after a response; simulator output is only a pre-route prediction.

## Alternatives considered

- **Use Anthropic's real hash.** Rejected: Anthropic does not disclose its internal cache key/hash algorithm or rendered-prompt serialization.
- **Hash only the declared breakpoint.** Rejected: misses Anthropic's 20-block lookback behavior and underpredicts hits in growing conversations.
- **Always keep the incumbent upstream.** Rejected as the primary model: it prevents request-id scatter but can freeze the first random owner and hides why a switch is necessary. Incumbent preservation remains a fallback/gate, not the cache-value model.
- **Use provider usage only.** Rejected as the primary model: it is valuable lineage evidence but arrives after routing and does not identify the exact prefix hash read.

## Rollout and QA requirements

- Add unit tests for prefix-chain lookback: a cache entry at `N-19` must match, while `N-20` must miss.
- Add routing tests proving a creation-only exact prefix score does not mask positive same-thread memory.
- Add synthetic replay verification proving that positive same-thread cache evidence prevents request-id scatter when exact-prefix observations are absent.
- Add proxy-path QA after deployment: send a real request through the proxy, inspect selected upstream/routing trace/cache usage fields, and classify provider-side failures separately from proxy routing failures.
