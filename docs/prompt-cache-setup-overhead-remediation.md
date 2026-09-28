# Prompt-cache setup overhead fundamental remediation design

- Status: Implemented, locally verified, and CI passed
- Written: 2026-09-07
- Scope: `cc-lb-engine` proxy hot path and admin-web latency timeline
- Related ADRs: ADR 0007, ADR 0008

## 1. Purpose

Remove excessive `proxy_setup_ms` from the proxy hot path. Change no observable proxy behavior other than the optimization and the new Prometheus instrumentation.

The following values must be completely identical before and after the change.

- Position, TTL, prefix hash, and exact local token count of every cache-control breakpoint
- The 20-content-block lookback candidates and longest-prefix match
- Predicted cache read/creation tokens, cache value, routing winner
- Values recorded in cache observations and request events
- Downstream status/body/headers and the request bytes sent upstream
- Streaming, cancellation, timeout, retry, and error mapping

Omitting breakpoints, approximating tokens, degrading the cache score, or dropping observations is not allowed.

## 2. Repeated setup work

Repeated prompt-cache analysis can dominate setup work before upstream dispatch. Operational measurements and deployment details are omitted from this public design note; the implementation must preserve exact results while reducing repeated serialization and tokenization.

## 3. Current execution point

The proxy hot path executes in `Lifecycle::handle` in the following order.

1. Parse the request body as JSON.
2. Call `request_cache_metadata_from_value`.
3. `analyze_v3_prompt_cache` flattens `tools → system → messages`.
4. Build the structural BLAKE3 prefix chain and lookback candidates.
5. Re-run `PrefixSerializer(blocks[..=index])` for each breakpoint.
6. Run `o200k_base::encode_ordinary(...).len()` on each serialized prefix.
7. After this work finishes, proceed to auth, routing, limit reservation, signer, and dispatch.

The structural hash chain is already linear over the whole request. The repeated cost is the cumulative JSON serialization and BPE tokenization performed per breakpoint.

## 4. Additional correctness constraint discovered

The structural prefix hash and the token-count identity are not the same key.

- The structural block digest excludes `cache_control`.
- The tokenization prefix bytes preserve `cache_control`.

Therefore, an implicit 5m marker and an explicit `ttl: "5m"` can share a structural hash while their serialized token prefixes differ. Token-count identity must follow those exact serialized bytes rather than structural equivalence.

Therefore the existing `prefix_hash` cannot be used as the token-count cache key.

## 5. Chosen solution structure

### 5.1 Keep the proxy-local structural hash

Do not change the existing schema-5 BLAKE3 chain. This key continues to be used for cache affinity, lookback matching, and persisted warm observations. The hash schema version is also unchanged.

### 5.2 Separate exact token-prefix identity

For the token-count cache, use the BLAKE3 digest of the actual `PrefixSerializer` bytes.

```text
TokenPrefixKey = BLAKE3(
    "cc-lb-token-prefix-v1"
    || downstream_credential_scope
    || exact_serialized_prefix_bytes
)
```

The key is the same only when the actual tokenization bytes — including `cache_control`, JSON escaping, source, and model — are identical and the downstream credential scope is also identical. The scope uses `x-api-key` with the same precedence as auth, and only when absent mixes in the `Authorization` header via one-way BLAKE3. The plaintext is never stored or exposed. Do not include the structural invalidator salt or path/index metadata that is not part of the tokenization input.

### 5.3 Exact nested-prefix batch preparation

Do not re-serialize the block tree per breakpoint.

1. Write the `{"content_blocks":[` header once.
2. Serialize each prefix block exactly once with deterministic `serde_json`.
3. Record the byte offset at the end of each breakpoint block.
4. Appending the `],"model":...}` suffix produces a prefix byte-identical to the existing `PrefixSerializer`.
5. Clone the incremental BLAKE3 hasher at each offset to produce the exact `TokenPrefixKey`.

If byte-for-byte equality with the existing direct serializer breaks, do not use the optimized path; fall back to the existing serializer.

### 5.4 Exact incremental token counting

Simple per-block token count summation is forbidden because of BPE boundaries. The chosen fast path preserves the last unstable regex piece returned by the `tiktoken-rs 0.12` ordinary encoder.

- Reuse the stable token count finalized at the previous breakpoint.
- Tokenize only the new block bytes and the previous unstable tail.
- Append each breakpoint's standalone JSON suffix to the unstable tail to compute the exact count.
- On incremental encoder errors or inputs that violate the boundary invariant, fall back to `encode_ordinary(full_prefix).len()`.

Compare every fast-path result differentially against the frozen reference's full-prefix count. The error tolerance is 0.

### 5.5 Bounded exact count cache

Keep a process-local bounded cache.

- key: `TokenPrefixKey`
- value: exact `u64` local token count
- Default capacity: 8,192 entries
- On capacity overflow, evict oldest insertions first
- Eviction only causes a cache miss and recomputation; it does not affect results.

Every requested breakpoint count is still returned. A cache hit skips computation, not functionality.

The cache is isolated per downstream credential scope. Even when different principals/keys send byte-identical prompts, they do not share cache-hit timing. None-auth mode has a single configured principal, so it uses the `unauthenticated` scope.

### 5.6 Cancellation-safe single-flight

Concurrent misses for the same `TokenPrefixKey` compute only once.

- The blocking job completes independently of the request future.
- Even if the leader request is cancelled, waiters and the cache receive the result.
- On panic/failure, clean up the flight entry and permit and wake waiters.
- Failed results are not cached.

### 5.7 Bounded blocking executor

Do not run `Lifecycle::handle`'s prompt-cache analysis directly on a Tokio worker.

- Pass `Arc<Value>` to a `spawn_blocking` job.
- Limit concurrent CPU jobs with `tokio::sync::Semaphore`.
- Default concurrency is `available_parallelism - 1`, minimum 1.
- The queue has no timeout or approximation. Waiting is async and functionality is preserved.
- The sync APIs `preview_route` and `parse_request_cache_breakpoints` keep the existing exact sync analyzer.

Do not change the public signatures of `Lifecycle::new` and `new_with_dynamic_view`. The executor and cache are created as internal private fields. Do not add a production public API just for tests.

### 5.8 Prometheus instrumentation

Expose internal cause analysis via Prometheus, not the request timeline.

- `cc_lb_prompt_cache_analysis_duration_seconds{stage="queue|total|tokenize"}`
- `cc_lb_prompt_cache_token_count_cache_total{result="hit|miss|coalesced"}`
- `cc_lb_prompt_cache_tokenizer_inflight`
- `cc_lb_prompt_cache_tokenized_bytes_total`
- `cc_lb_prompt_cache_tokenized_tokens_total`
- `cc_lb_prompt_cache_tokenizer_fallback_prefixes_total`
- `cc_lb_prompt_cache_analysis_worker_failed_total`

Labels use only the fixed stage/result. Do not add principal, request ID, prefix hash, or model as labels.

### 5.9 Request timeline boundary

The admin-web latency timeline and latency cell show only stages that delay the request.

- Keep `Setup overhead` because it is time that actually delayed dispatch.
- Do not add new prompt-cache micro timings to the timeline.
- The timeline total keeps the existing `duration_ms`. This value is finalized before post-response observation runs, so no separate subtraction is needed.
- Observation pipeline health is viewed only in Prometheus.

## 6. Per-file implementation plan

### Engine

- `crates/cc-lb-engine/src/prompt_cache_simulator.rs`
  - Separate structural analysis from exact prefix batch preparation
  - Keep the frozen reference analyzer
  - Add the optimized analyzer and work statistics
- `crates/cc-lb-engine/src/tokenizer.rs`
  - Add the exact nested-prefix incremental counter
  - Add optimized work statistics separate from the reference path's existing thread-local call counter
- `crates/cc-lb-engine/src/prompt_cache_simulator/optimized.rs`
  - Implement the bounded executor, exact cache, single-flight, and exact prefix batch
- `crates/cc-lb-engine/src/lifecycle.rs`
  - Hold the request JSON as `Arc<Value>`
  - Use the async executor on the proxy hot path
  - Keep the sync preview/parser path
- `crates/cc-lb-engine/src/lib.rs`
  - Register only internal modules; export no new production public API

### Observability

- `crates/cc-lb-observability/src/init.rs`
  - Add metric definitions, descriptions, registration, and initial handles

### Admin web

- `computeStageGroups.ts`
  - Timeline total keeps the existing `duration_ms`
  - Internal post computes only `limit_reconcile_ms`
- `LatencyTimeline.tsx`
  - Stage percentages and unaccounted keep the existing `duration_ms` basis
- `LatencyCell.tsx`
  - Headline/popover percentages keep the existing per-phase denominator

## 7. Failures and fallbacks

- Incremental boundary verification failure: that request uses the existing full-prefix count path
- Incremental encoder or boundary invariant failure: use the existing full-prefix count path
- Blocking task join failure: leave the existing dropped-events metric and error trace, and fall back exactly to the sync analyzer
- Cache eviction: exact recomputation
- Single-flight leader failure: remove the entry, then waiters recompute exactly

No fallback permits approximation or missing breakpoints.

## 8. Completion criteria

- Full `V3PromptCacheAnalysis` equality between the frozen reference and the optimized analyzer
- Identical results across 0/1/3/4/5 breakpoints, mixed TTL, and lookback 19/20 boundaries
- In proxy integration, identical selected upstream, request bytes, status/body, and event/observation
- Tokenized work on the 4-breakpoint representative fixture at near-deepest-prefix level, not cumulative 4-pass
- Exactly 1 actual tokenization job for concurrent identical requests
- Tokio heartbeat persists during a large analysis
- New metrics exposed on the Prometheus endpoint
- Full CI pass
