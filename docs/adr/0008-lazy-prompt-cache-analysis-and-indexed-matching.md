# ADR 0008 — Lazy prompt-cache analysis and indexed content-block matching

- Status: Accepted
- Date: 2026-07-10
- Amends: ADR 0007 (v3 content-block prompt-cache routing). This ADR keeps ADR 0007's routing *semantics* unchanged (flatten order, 20-block lookback, longest content-block prefix wins, cache-positive WRH keying, aggregate-only provider calibration). It changes only the internal *mechanics* — how the analysis is computed, how matches are found, and the hash primitive — for performance and correctness.
- Related: ADR 0002 (JSON library strategy), ADR 0007 (v3 content-block prompt-cache routing).
- Authoritative provider reference: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>.

## Context

Recent production requests spend 30+ seconds in `proxy_setup_ms` with every recorded component timer at or near zero. Direct measurement isolated the cause to prompt-cache request analysis, not database, auth, routing, signing, limits, or plugins.

### Runtime evidence

- Live `request_events_v1`: outliers spent `proxy_setup_ms` in the 36,324–39,994 ms range, while the same requests recorded `auth_ms=0`, `route_ms=0`, `sign_ms=0`, `limit_reserve_ms=0`, `bulkhead_wait_ms=0` and `shape_ms` in the hundreds of ms. The 30+ seconds is charged to setup but to none of the component stages.
- The slow requests carried ~245–259 messages and predicted cache reads above 500k tokens; the process ran at ~94% CPU with accumulated time on Tokio workers (SQLite workers were idle by comparison), i.e. the setup is CPU-bound, not I/O- or lock-bound.
- Toggle proof at constant total payload (128 KiB): one content block took 181–183 ms; splitting the *same bytes* into 128 blocks took 8.7–16.2 s across two runs. Block count, not byte count, drives the cost.

### The primary defect — quadratic cumulative tokenization

`crates/cc-lb-engine/src/prompt_cache_simulator.rs::prefix_token_counts` grows a prefix vector one block at a time and, on every block, re-serializes the *entire accumulated prefix* to JSON and re-tokenizes it from scratch with `PrefixTokenizer::global().count_tokens` (tiktoken, `local_tiktoken_v1`). For N similarly sized blocks this retokenizes prefixes 1..N — effectively `O(N × total_body_bytes)`, quadratic for a request whose blocks grow with the conversation.

This runs inside `analyze_v3_prompt_cache`, invoked once per request via `request_cache_metadata_from_value` at `lifecycle.rs:3518`. That call sits at `lifecycle.rs:1237`, *before* `auth_start` (`lifecycle.rs:1279`), so its cost lands only in `proxy_setup_ms` (`dispatch_started - started`, `lifecycle.rs:1688`) and in no component timer — exactly matching the observed pattern. It is also **unconditional**: it runs for every request with a valid JSON body to populate `ParseCompleted` telemetry, even when prompt-cache routing (`prompt_cache_shadow.enabled` + an observation cache) is disabled.

### Secondary defects (correctness and scaling)

1. **Eager 20-prefix materialization.** For each breakpoint, `analyze_v3_prompt_cache` builds a full `lookback_prefixes` vector (`prompt_cache_simulator.rs:323`) with a `prefix_token_count` on every one of the ≤20 candidates. Only the single matched candidate is ever priced, so up to 19 of every 20 token counts are computed and never used.
2. **Match selection by token count, not structure.** `build_cache_score` (`lifecycle.rs:218-233`) picks the "longest" match by `prefix_token_count` descending. ADR 0007 defines the winner as the longest *content-block position* (`N..N-19`). Token estimates are not a sound ordering key: BPE merges and JSON framing make cumulative estimates non-strictly-monotonic, so the token-max rule can disagree with the structural rule.
3. **Read-time `warm_set_cap` truncation.** `snapshot_for_upstream` (`crates/cc-lb-server/src/prompt_cache_observation_cache.rs:173-214`) scans all of an upstream's entries, sorts by recency, and truncates to `warm_set_cap` (default 32). Recency is not Anthropic's longest-prefix rule, so the true longest match can be truncated away when more than 32 entries are live.
4. **Observation refresh keyed to explicit breakpoints only.** `prompt_cache_observation_context` (`lifecycle.rs:1597`, `540-567`) snapshots only the explicit breakpoint hashes, and `decode_prompt_cache_observations_pure` (`lifecycle.rs:569-656`) matches only those. A cache hit at a non-breakpoint lookback position `N-k` therefore cannot refresh the entry that actually matched, so a warm prefix can be allowed to expire while it is still being read.

### The hash chain is already linear (and stays a forward chain)

Prefix keys are a forward hash chain `H_n = SHA256(tag || H_{n-1} || block_digest_n)` seeded from the canonical model (`prompt_cache_simulator.rs:12-17`, `79-94`, `281-299`). Each block does one constant-size (~85-byte) chaining hash plus one hash of its own content; the whole chain is `O(total_bytes)` computed once, and every prefix key `H_1..H_N` is stored in the `keys` vector. Backward lookback is therefore *array indexing into stored keys* (`lookback_keys`, `100-110`), not re-hashing and not hash "subtraction" (which is impossible for a one-way hash and unnecessary here). Hashing is not the bottleneck. We nonetheless replace SHA-256 with BLAKE3 (below) because it is free to do so, faster, and already a workspace dependency.

## Decision

Separate prompt-cache request analysis into two independent axes, and fix the matching/observation defects while preserving ADR 0007 semantics.

### Axis 1 — Structural analysis and hash-walk matching (cheap, eager)

Structural analysis performs **no tokenization**:

1. Flatten the request once into cacheable blocks in `tools → system → messages` order (unchanged).
2. Compute each block's digest once and extend the forward hash chain once (`O(total_bytes)`), storing every prefix key.
3. For each explicit breakpoint, record its structural lookback candidates as `(prefix_key, content_block_index, lookback_distance, requested_ttl, breakpoint_membership)` — keys and indices only, **no token counts**.

**Breakpoint cap (DoS guard).** Anthropic accepts at most four explicit `cache_control` breakpoints per request. A request declaring more is provider-invalid, so structural analysis first counts cacheable explicit breakpoints directly from the parsed request by reference and stops on the fifth. It returns **no blocks and no breakpoints** (cache-ineligible; the original request bytes are forwarded as-is for the provider to reject) before flattening, cloning, digest serialization, hash-chain construction, or tokenization. `MAX_EXPLICIT_BREAKPOINTS = 4` is also checked after structural collection as a correctness backstop.

Matching is a hash walk over stored keys with early stop:

- Merge every breakpoint's candidates globally by `(prefix_key, content_block_index)`, retaining every membership, and order the resulting prefix groups by `content_block_index` descending.
- For each group, union the TTL classes eligible through all memberships, perform one `O(1)` point lookup, and select the greatest-expiry entry in that union. **Stop at the first prefix group with a hit** — that first hit is, by construction, the longest content-block prefix.
- Preserve every candidate's full membership set: a single deduplicated prefix key that appears in two breakpoints' windows with different TTLs must retain both memberships, because TTL eligibility is asymmetric (a 5m request accepts a 1h or 5m entry; a 1h request requires a 1h entry — `prompt_cache_observation_cache.rs:429-433`). Deduplication must not collapse a prefix to a single TTL or a single anchor.
- **Same-key selection and attribution.** When one prefix key has multiple stored entries (5m and 1h), select the entry with the **greatest expiry** among those that satisfy some eligible membership (preserving the current `warm_entry_for` max-expiry rule, `lifecycle.rs:212`). Attribute the trace only to a membership whose requested TTL the selected entry actually satisfies — never to the shortest-lookback membership if that membership's TTL is ineligible (e.g. a 1h membership when only a 5m entry exists). Ineligible memberships do not produce a hit.

The 20-block window is thus a *hashing/matching* concern (≤20 `O(1)` lookups per breakpoint per upstream, microseconds), fully decoupled from tokenization.

### Axis 2 — Selective tokenization (expensive, lazy, minimized)

Tokenize only the indices that are actually priced, after matching is known:

- The set of indices to tokenize = the explicit breakpoint indices (≤4 typical) ∪ the unique matched indices across all candidate upstreams.
- Candidate construction is explicitly **two-phase**: (1) match every upstream structurally and collect the selected indices; (2) tokenize the union once, memoized by content-block index, then price each upstream. No upstream tokenizes in isolation.
- **Reuse persisted estimates.** A matched warm entry already carries `estimated_prefix_tokens` + `token_estimate_source` (`prompt_cache_observation_cache.rs`). We reuse that count directly to price a matched prefix and never re-tokenize the matched index. This is pure cross-request memoization of our own prior tiktoken estimate on the identical prefix bytes (deterministic, same basis) — it is **not** the rejected read-usage calibration, which would mix Anthropic's real tokens into a tiktoken-based comparison.
  - **Reuse is unconditional because incompatible provenance is unreachable by invariant.** We are the sole writer of `token_estimate_source` (always `local_tiktoken_v1`) and `hydrate_from_store` rejects any stored observation whose `hash_schema_version` differs from the current `HASH_SCHEMA_VERSION` (`prompt_cache_observation_cache.rs`). Therefore every in-memory warm entry carries a current-basis estimate, and the earlier "tokenize only indices with missing or incompatible provenance" branch is dead code — there is no reachable state in which a matched entry's estimate is stale or foreign-basis. Encoding a fallback (e.g. re-pricing against the owning breakpoint's deeper count) would over-price N-k reads for a case that cannot occur, so the fallback and its test are removed.
  - **When re-tokenization would matter (deferred).** The only future state that breaks the invariant is a **change of estimator** (`token_estimate_source` bump, e.g. swapping `local_tiktoken_v1` for a Claude-native tokenizer). At that migration, warm entries written under the old source coexist with the new estimator, and pricing a match against a foreign-basis estimate would mix token bases. Handling that requires re-tokenizing the matched index (or invalidating old-source entries) and is orthogonal to this ADR — it is tracked as separate work to be done **with** the estimator swap, not before it.
- **Honest complexity.** The bound is `O(total_block_bytes)` hashing + `O(Σ serialized-prefix-length(selected index))` tokenization. In the pathological case of `N` upstreams warmed at `N` distinct depths, every prefix is selected and tokenization is still quadratic in bytes; estimate-reuse (above) collapses this to the intended near-linear behavior once v4 warm state exists. The eager, unconditional all-prefix tokenization in `prefix_token_counts` is what this ADR removes — not every theoretical `O(N²)`.
- Token counts are never needed to *select* a winner (that is structural), only to *price* an already-selected match and its downstream creation segments.
- Keep the existing tiktoken estimator (`local_tiktoken_v1`). A constant-factor error cancels in `cache_ratio = value / max_positive_value_in_tier`, so an approximate tokenizer is acceptable; a length/byte proxy is not, because the byte:token ratio varies by content (images, CJK, code, base64) and candidates are differentiated by exactly those differing content segments. The estimator choice is orthogonal to this ADR — the fix is to stop running it `O(N²)` times.

### Scoring by a structural breakpoint fold

Compute the mixed-TTL score by folding the explicit breakpoints in **content-block-index order** (not by sorting on estimated token count, and not via fixed `A/B/C` positions):

- `A` = the matched prefix's content-block position (or start-of-prompt on a miss); `predicted_read = tokens(A)`.
- Because a prompt-cache write covers the cumulative prefix up to a breakpoint, per-TTL creation is defined by the **deepest breakpoint of that TTL that needs a write**, not by summing per-breakpoint segments. A breakpoint before `A` is covered; one after `A` needs a write; one at `A` needs a write only when the matched entry's TTL does not satisfy its requested TTL. Let `last_1h` / `last_5m` be the deepest write-needed breakpoint of each TTL. The 1h baseline is `tokens(A)` only when the matched entry itself is 1h, otherwise zero, so a same-index 5m→1h upgrade prices the full 1h prefix. Then `creation_1h = saturating(tokens(last_1h) - 1h_baseline)`; `boundary = tokens(last_1h)` (or `tokens(A)` when there is no 1h write); `creation_5m = saturating(tokens(last_5m) - boundary)`. Intermediate same-TTL breakpoints add no extra cost, because the prefix is cumulative.
- Subtracting region endpoints (rather than summing per-breakpoint segments) stays correct when estimates are non-monotonic. Worked examples: two 5m breakpoints estimating `100` then `90` → `creation_5m = tokens(last_5m) - 0 = 90` (a token-sort fold would wrongly give `100`); `1h(100)→5m(90)` → `creation_1h = 100`, `creation_5m = saturating(90 - 100) = 0`. It intentionally differs from the current `build_cache_score` (`lifecycle.rs:238-262`), which orders by token count (the token-max bug).
- **Missing-write determination is per `(breakpoint, requested TTL)`, not by position alone.** A breakpoint at index `i` needs a write whenever no live entry at `i` satisfies that breakpoint's TTL — including a *same-index TTL upgrade* (index `i` warm only as 5m, but a 1h breakpoint at `i` still needs a 1h write). The current `prefix_token_count > matched_prefix_tokens` filter (`lifecycle.rs:238`) misses this; the fold must test TTL compatibility at each breakpoint.
- **Reject provider-invalid TTL orderings before scoring.** Anthropic requires longer TTLs before shorter (1h… then 5m…). If a request violates that, mark cache simulation ineligible for it (forward as-is for the provider to reject) rather than manufacturing a routing advantage by reordering. Saturating arithmetic remains only as a guard against estimator non-monotonicity, emitting a drift signal — never silent reordering.

The monetary formula, `CACHE_LOG_BOOST`, cache-positive WRH keying, and quota-tier ordering from ADR 0007 are unchanged. This scoring preserves ADR 0007's **intended structural semantics**; it intentionally differs from the current `build_cache_score`, which selects/orders by token count (the token-max bug).

### Hash primitive: SHA-256 → BLAKE3

Replace SHA-256 with BLAKE3 at all three key-derivation sites (`seed`, `from_block_digests`, `block_digest`). The chain structure, 32-byte output width, and `serde_json::to_vec` (sorted-key, deterministic) hash-input serialization are unchanged — only the compression function changes. Bump the domain-separation tags from `cc-lb-cache-v3:{seed,prefix,block}` to `cc-lb-cache-v4:*` so a schema-4 key can never be confused with a schema-3 one even by accident. BLAKE3 is already a workspace dependency (`Cargo.toml:48`).

- Bump `HASH_SCHEMA_VERSION` from `3` to `4`. Every prefix key changes, so v3 warm entries become inert; `hydrate_from_store` already skips mismatched `hash_schema_version` (`prompt_cache_observation_cache.rs:119`), so the deployment cold-starts for v4 keys with no data corruption. Because the release has not shipped, no backward compatibility is required: no dual-schema read path, no migration bridge — the version filter plus cold start is the whole story.
- Add golden tests covering seed derivation, per-block digest (with `cache_control` excluded), forward-chain extension, canonical-model seeding, and cross-process key stability. Update **all** hard-coded schema-3 fixtures and storage/hash tests (e.g. `tests/hash_golden.rs`) to schema-4 values — do not leave a stale v3 fixture asserting an old key.
- Rationale for keeping a cryptographic (not rolling/reversible) hash: the key routes cache affinity, and a reversible/weak hash lets a client construct preimages to probe "is this prefix warm?" via the routing trace or to force affinity across tenants; a constant, seed-stable, collision/preimage-resistant hash is required for cross-process/replica stability (keys are rehydrated from storage on restart) and to prevent that probing. Collision resistance is not safety-critical here (a collision degrades routing quality and self-heals within TTL; it never corrupts a response or leaks cached content, because Anthropic re-keys on the real bytes), but BLAKE3 provides it for free, so there is no reason to trade it away.

### Remove `warm_set_cap` from matching truth

Delete the read-time recency truncation from the matching path; matching must consider all live entries so the structurally longest match is never dropped. Do **not** relocate the cap to a write-time LRU: provider cache state can legitimately exceed 32 locally observed entries, and evicting by recency would manufacture semantic false misses.

Memory must be bounded without corrupting matching truth:

- **Actually delete expired entries.** `snapshot_for_upstream` currently *filters* expired entries but never removes them (`prompt_cache_observation_cache.rs:173-214`), so the in-memory map grows unbounded. Add real expiry eviction (opportunistic on access + a periodic sweep) so live cardinality is bounded by `unique-creation-rate × TTL`, not by lifetime.
- **Bounded overload mode.** If a `(upstream, model)` partition still exceeds a hard memory ceiling, do **not** keep an arbitrary recency subset (that can select a shorter false winner). Instead either spill exact entries to the already-indexed store for point lookup, or disable cache scoring for that partition entirely and fall back to cache-negative routing. Losing the optimization is acceptable; asserting a false longest match is not.
- Removing `warm_set_cap` also removes it from the config schema, its documentation, and the dynamic-view construction — not just the matching code.

### Response observation carries the selected match

Copy the **immutable routing-time match snapshot** consumed by response handling directly from the chosen candidate: matched prefix key, content-block index, actual stored TTL, estimated tokens, and token provenance. The qualifying membership is used during routing to populate the score/trace but does not need to be retained after the match content index and entry TTL are fixed; response write decisions use those two fields. Do **not** re-snapshot the warm map after routing — a concurrent upsert/expiry between routing and observation would attribute to a different entry than the one that drove the decision. Refreshed expiry is derived from response-start time rather than copied from the old entry.

Response attribution is **inferential**, because Anthropic reports only aggregate `cache_read`/`cache_creation` usage and cannot say which local key hit:

- provider read `> 0` with a predicted match → refresh that predicted matched key as the **best guess** (this is the intended improvement over today, where a non-breakpoint `N-k` hit refreshes nothing). Emit a confidence/agreement signal; make no correctness claim. A wrong guess self-heals within TTL. Do **not** refresh all candidates.
- provider read `> 0` with no predicted match → record an unknown/local-state-missing hit; do not fabricate a key.
- provider read `= 0` with a predicted match → do not refresh; record the prediction disagreement (feeds the drift metric).
- provider creation `> 0` → persist only the explicit breakpoints that the structural fold marked as needing a write (per `(breakpoint, TTL)`). Aggregate usage cannot prove per-breakpoint attribution (ADR 0007), so this too stays explicitly inferential.

**Anchor refreshed expiry to response-start, not response-completion.** `finish_success_response` decodes buffered usage after the whole response (`lifecycle.rs:2033`); computing `now + TTL - grace` there overextends provider cache state by the full generation duration. Use the upstream input-processing / SSE `message_start` time as the expiry anchor.

### ABI / schema / migration

The release has not shipped, so **no backward compatibility is required**: remove fields outright and delete compat shims/fixtures rather than bridging them. No `Option<u64>` transition field, no dual-read path. No wire-version bump is needed for this change — see the "No plugin wire change" bullet below.

- **Remove `prefix_token_count` from the lookback DTOs outright** (`cc_lb_plugin_api::types::CacheLookbackPrefix`, `cc_lb_contract::RequestCacheLookbackPrefix`); lookback carries key/index/distance/TTL-membership only. Retain `prefix_token_count` on explicit breakpoints, on the matched score/trace, and on persisted observations (`estimated_prefix_tokens`).
- **No plugin wire change.** The filter wire struct `FilterRequest` (`crates/cc-lb-plugin-wire/src/v1/mod.rs:191-203`) carries only `cache_pricing` (`CachePricingSummary`) and `candidates` (`UpstreamCandidate`); it does **not** carry the lookback breakpoint DTOs, and neither `CachePricingSummary` nor `UpstreamCandidate` embeds `prefix_token_count`. The one in-tree router plugin (`plugins/router/cache-aware-wasmtime`) reads only `candidate.predicted_cache_read_tokens` (a `CacheScore` field we keep). Removing `prefix_token_count` from the lookback DTOs is therefore a pure in-process Rust type change: **no `WireVersion` bump, no rkyv/schema fingerprint regeneration, no plugin rebuild.** `WireVersion` exposes only `V1` (`crates/cc-lb-plugin-wire/src/schema.rs`) and stays `V1`.
- **No `request_events` migration is needed for this removal.** `request_events_v1` persists only the *matched result* columns (`matched_v3_cache_key`, `matched_content_block_index`, `lookback_distance`, `predicted_cache_read_tokens`, `predicted_cache_creation_tokens_{5m,1h}`, `token_estimate_source`, `cache_value_micros`, … — sqlite migration `0043`, postgres `0073`), all of which we keep. The per-lookback token count is never stored as a column; if the breakpoint list is persisted as JSON anywhere, its serialized shape simply changes with the DTO (no schema migration). The `prompt_cache_observations` store schema is unaffected.
- Update DTO round-trip fixtures, request-event compatibility tests, and simulator conversions to the new shapes; **delete** (do not preserve) any test that only existed to tolerate the old lookback-token-count shape.
- `HASH_SCHEMA_VERSION 3→4` (above) needs no data migration — cold start via the existing version filter.

## Consequences

### Positive

- Setup CPU drops from unconditional `O(N²)` cumulative tokenization to `O(total_bytes)` hashing plus tokenization only of `explicit breakpoints ∪ unique matched indices`; reusing persisted `estimated_prefix_tokens` for matched keys collapses the steady state to near-linear once v4 warm state exists.
- Matching becomes ordered `O(1)` point lookups with early stop instead of full per-upstream entry scans, sorts, and truncation.
- Match selection follows ADR 0007's structural rule (longest content-block prefix), removing the token-max discrepancy in the current `build_cache_score`.
- The selected match is carried immutably into observation, and a non-breakpoint `N-k` hit now refreshes a best-guess entry (inferential) instead of refreshing nothing — warm prefixes stop silently expiring under continuous reads.
- The 20-entry-plus cap regression (longest match truncated by recency) is eliminated, with real expiry eviction bounding memory.

### Negative

- BLAKE3 + `HASH_SCHEMA_VERSION=4` cold-starts v3 warm state on deploy (accepted; no backward compatibility required pre-release).
- The lookback DTO changes shape, but it does not cross the filter wire (`FilterRequest` carries only pricing + candidates), so there is no wire-version bump, no fingerprint regeneration, and no plugin rebuild.
- Removing the read-time cap makes the live matching set unbounded by count; memory now depends on real expiry eviction plus a bounded overload fallback, which must be verified under load.
- Response refresh remains inferential (aggregate provider usage cannot prove which local key hit); it is a best guess with drift telemetry, not a correctness guarantee.
- Worst-case tokenization (N upstreams at N distinct depths) is still superlinear before estimate-reuse warms; this is bounded by explicit breakpoints + unique matched depths, not by total block count.

### Neutral

- Quota-tier ordering, the monetary formula, `CACHE_LOG_BOOST`, cache-positive WRH keying, and deterministic serialization for hash inputs (`serde_json::to_vec`, per ADR 0002/0007) are unchanged.
- The tiktoken estimator and `token_estimate_source = local_tiktoken_v1` are unchanged; any future estimator or usage calibration is a separate versioned change.

## Re-scan findings (setup path)

A final sweep of the `started → dispatch_started` window (`lifecycle.rs:1195-1688`) confirmed there is no second hidden hotspot:

- The request body is parsed once (`RequestBodyView::new`, sonic-rs, `lifecycle.rs:1198`) and reused via borrowed `&Value`; no re-parse.
- `analyze_v3_prompt_cache` runs exactly once per request in the hot path (at `lifecycle.rs:1237`), and unconditionally (not gated by the prompt-cache feature flag) because it feeds `ParseCompleted` telemetry. Making analysis structural-only removes the eager cost from this unconditional path too.
- The second tokenizer call site, `cache_prefix_hash_and_token_count_v2` (`lifecycle.rs:3628-3642`, `count_tokens` at `3638`), is legacy v2 and is referenced only from tests (`tests/hash_golden.rs` and the in-file `#[cfg(test)] mod tests`, `let legacy_v2`/`let legacy_v1` at `4772`/`4778`). It is not on the production hot path.
- `build_cache_score` / `snapshot_for_upstream` run inside the routing timer (`route_ms ≈ 0` on the slow requests), so the per-upstream scan is a scaling/correctness concern addressed here, not the current 30s cause.

## Alternatives considered

- **Keep SHA-256.** Viable (hashing is not the bottleneck), but BLAKE3 is free, faster, already vendored, and lets us bump the schema once alongside the other cutover.
- **Reversible / rolling hash (Rabin-Karp, buzhash) for backward peeling.** Rejected: rolling hashes are reversible but not collision/preimage-resistant, and reversibility is unnecessary because the stored forward chain already gives `O(1)` backward indexing.
- **Collision-resistant incremental hashes (LtHash/MuHash/AdHash).** Rejected: they hash unordered sets, losing the prefix order/position semantics we require, and are heavier than BLAKE3 for no benefit.
- **Byte/char/block-count token proxy.** Rejected: the proxy:token ratio is content-dependent (images, CJK, code, base64), which distorts the read-vs-creation tradeoff and the `exp()` concentration exactly where candidates differ.
- **Provider-usage-only pricing.** Rejected for routing: usage arrives after the route decision and is aggregate-only for multi-breakpoint requests (ADR 0007). It remains the response-time calibration source.
- **Write-time LRU cap.** Rejected: bounds local knowledge by recency and manufactures false misses unrelated to provider cache state.
- **Per-block token summation or incremental tokenizer extension.** Rejected: BPE is non-additive at boundaries, so per-block sums and appended re-tokenization change the estimate.
- **Fast lookup but still tokenize every prefix.** Rejected: preserves the dominant quadratic setup cost.
- **Read-usage calibration for routing** (store Anthropic's actual `cache_read_input_tokens` per matched prefix key and reuse it as the routing estimate on later requests). Rejected. The routing comparison is relative and tier-normalized (`cache_ratio = value / max_positive_value_in_tier`), so a constant estimator factor cancels and the absolute token basis changes neither the ranking nor the cache-positive trigger. Worse, provider read tokens are only ever observed for the prefix that actually hit on the routed upstream — never for the counterfactual matched depths of the other candidates — so calibration would mix a real-token basis (observed prefixes) with a tiktoken basis (cold prefixes) inside the same comparison, breaking the constant-factor cancellation and giving cold candidates a spurious bias. The only absolute-token use is the observation-time cache-threshold filter (`cache_threshold_tokens`, e.g. 1024/4096), which bites only near the boundary for small prefixes (irrelevant to the large requests here) and can read provider usage directly at observation time without a stored calibration cache. Estimator-vs-provider drift is already monitored via the `cc_lb_cache_token_drift` metric. If estimator accuracy ever matters, improve the estimator consistently for all prefixes via a versioned `token_estimate_source`, not per-prefix partial calibration.

## Verification requirements

- **Differential oracle.** A deliberately slow reference that enumerates every breakpoint window, scans every entry, selects by highest structural index with full TTL membership, tokenizes all prefixes, and computes the structural breakpoint fold (index-ordered, per-`(breakpoint, TTL)`); property-based comparison of every optimized upstream match, score, trace, and observation action against it. Do not differential-test against current token-max behavior.
- **Structural properties.** Flatten order; deterministic chain extension; `cache_control` excluded from block digest; `N` and `N-19` hit, `N-20` miss; overlapping windows; duplicate-prefix memberships; up to four breakpoints; valid and invalid mixed-TTL order; write persistence only at explicit breakpoints.
- **Adversarial scoring.** Force token estimates that are equal or decrease with block position and prove the later structural hit still wins; verify the index-ordered per-`(breakpoint, TTL)` write-class fold for all-5m, all-1h, and 1h-then-5m requests (including decreasing-token and same-index-TTL-upgrade cases).
- **Cap regression.** More than 32 live matching entries with the structurally longest match outside the 32 most recent; prove it still wins and that `warm_set_cap` no longer changes routing.
- **Multi-upstream + observation.** Distinct matches per upstream; the selected match carried unchanged into response handling; a non-breakpoint `N-k` hit refreshed; non-selected upstreams untouched; provider read/prediction disagreement handled without fabricated state; buffered and SSE paths equivalent.
- **Performance gates.** Benchmark 1/20/128/~259 blocks at fixed and growing total size, and 1/32/256 upstreams; assert tokenizer-call count equals the number of unique selected indices and that no full entry scan/sort occurs; require approximately linear scaling with input size at fixed breakpoint/upstream counts and a p95 budget anchored to the ~181 ms single-prefix baseline. Prefer instrumentation counters over wall-clock assertions to avoid CI flake.
- **Proxy-path QA.** Through the running proxy (fake-anthropic upstream), deterministic write → lookback-hit including a non-breakpoint `N-k` hit, overlapping mixed-TTL windows, a cache miss/write, and multiple seeded upstream states; verify selected upstream, response status/body, routing trace, `request_events` A/B/C fields, persisted observation key, and refresh expiry. Optionally one minimal real-Anthropic aggregate-usage calibration if credentials permit, classified separately and with temporary credentials cleaned up.
