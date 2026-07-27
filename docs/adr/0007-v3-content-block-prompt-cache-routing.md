# ADR 0007 — v3 content-block prompt-cache routing hard cutover

- Status: Proposed
- Date: 2026-07-08
- Ships with: pending
- Supersedes: ADR 0006's transitional exact-prefix/thread-memory merge, ADR 0005's thread-keyed cache-positive WRH keying, and every v1/v2 prompt-cache routing assumption.
- Research source: `.omo/ulw-research/20260708-v3-cache-research/`.
- Source: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>.
- Retrieved: 2026-07-27.

## Context

The v10 cold-stuck investigation found seven stuck cache-positive cases whose behavior could not be explained by exact-prefix math. The observed pattern was `predicted_cache_read_tokens > 0` with `predicted_cache_creation_tokens_* = 0` in situations where local exact-prefix matching alone should have produced a creation-only score. Cross-checking the frozen local database (`/home/bhyoo/.local/share/cc-lb/storage.sqlite`) showed the active routing input was thread lineage, not exact prefix state.

Thread lineage was valuable as a post-route hint, but it was also the source of the bug class: `select_cache_score_by_value(exact_score, thread_score)` could select a lineage-derived `CacheScore`, and `subscription_preference` used `thread_id` as the WRH routing key whenever a bucket had positive priced cache value. That pinned or scattered traffic by caller session identity rather than by a provider-cacheable prefix. It also made the router appear warm even when the local prefix simulator did not know which provider-cacheable prefix would be read.

Provider research closed the model we need to simulate:

- Anthropic prompt-cache prefix hierarchy is `tools -> system -> messages`.
- Explicit cache writes happen only at cache-control breakpoints.
- Read lookup starts at the breakpoint and walks backward by **content-block position**, checking at most 20 positions including the breakpoint itself (`N, N-1, ..., N-19`).
- The walk is over Anthropic-native cacheable content blocks, not `messages[]` objects and not the number of explicit breakpoints.
- Cache-control placement is valid on tool definitions, `system[]` content blocks, and `messages[].content[]` content blocks. Native docs do not define `cache_control` on a `messages[]` message object itself.
- Top-level automatic caching is a second activation mode. A request-level `cache_control` places a provider-managed breakpoint on the last eligible cacheable block and advances that breakpoint as the conversation grows. It consumes one of the same four breakpoint slots as block-level markers, so automatic caching plus four explicit markers is provider-invalid. It is supported on Claude API / Claude Platform on AWS / Google Cloud / Microsoft Foundry, but not Bedrock.
- Cache state is isolated by workspace on Claude API, Claude Platform on AWS, and Microsoft Foundry. Bedrock and Google Cloud retain organization-level isolation; no provider shares cache state across organizations.
- Provider responses expose aggregate `cache_read_input_tokens` and `cache_creation_input_tokens`; they do not expose Anthropic's internal cache key, hash, tokenizer, eviction topology, or per-breakpoint read/write attribution.

The existing repo surface matches the old model:

- `crates/cc-lb-engine/src/lifecycle.rs` extracts cache breakpoints by recursively finding `cache_control` anywhere under `tools`, `system`, or each message, and derives v2 prefix hashes with `cache_prefix_hash_and_token_count_v2`.
- `crates/cc-lb-engine/src/lifecycle.rs::build_candidates` builds an exact score from warm v2 entries, then asks the prompt-cache cache for a thread-lineage score and calls `select_cache_score_by_value`.
- `crates/cc-lb-engine/src/cache_score_selection.rs` exists only to choose between exact score and thread-lineage score by priced value.
- `crates/cc-lb-server/src/prompt_cache_observation_cache.rs::thread_usage_score` fabricates a routing `CacheScore` from same-thread usage with confidence `0.5` and `ambiguity_reason = "thread_usage_lineage"`.
- `crates/cc-lb-engine/src/builtin_filters/subscription_preference.rs` already prices cache value as read savings minus creation cost, but positive cache value currently switches WRH keying to `thread_id`.
- `prompt_cache_observations` stores v2-style `(upstream_id, canonical_model_id, prefix_hash, ttl_class)` warm entries without v3 content-block index, lookback distance, or token-estimate provenance.

Live provider checks also closed model-id behavior:

- `claude-sonnet-4-5` and `claude-sonnet-4-5-20250929` share prompt-cache state.
- Fabricating a fake dated 4.6+ ID is invalid: `claude-sonnet-4-6-20250929` returns `/count_tokens` 404.
- Therefore canonicalization may use proven alias mappings, but 4.6+ dateless IDs must remain as sent unless a real provider-documented dated ID is known.

## Decision

Hard-cut active prompt-cache routing to v3 content-block semantics. Remove v1/v2 prompt-cache routing and warm-state compatibility rather than keeping a mixed fallback.

### v3 unit of caching

The v3 simulator flattens the request into an Anthropic-compatible cacheable content-block sequence in this order:

```text
tools -> system -> messages
```

The sequence contains every block that is significant to the rendered provider prefix:

- tool definitions in `tools`;
- cacheable content blocks in `system[]`;
- text, image, document, tool_use, tool_result, thinking, redacted_thinking, and tool_reference blocks in `messages[].content[]`.

Thinking, redacted_thinking, and tool_reference blocks contribute to the prefix chain but are not direct breakpoint targets. Empty text blocks are not cacheable. Message objects are not cacheable units; only their content blocks are.

The local key is an Anthropic-compatible **proxy-local simulator key**, not Anthropic's private cache hash. Its job is to recognize equivalent rendered local prefixes and route consistently; it must never be described as the provider's real key.

### v3 read/write model

For each explicit or automatic local breakpoint at content-block index `N`:

```text
lookup candidates = N, N-1, ..., max(0, N-19)
match = longest unexpired upstream entry whose v3 prefix key appears in that window
```

Read prediction uses the longest matched prefix. Write prediction is derived only from missing segments at requested breakpoints. The simulator must **not** persist all lookback candidates as warm entries just because they were checked. Warm entries are created or refreshed from provider-observed writes/reads at breakpoints and their TTL class.

When a request carries at least one `cache_control` marker and produces a server-tool result, Anthropic inserts its own breakpoint on that result. The provider-managed breakpoint always uses the 5-minute TTL, regardless of the TTL on the request's markers. Consequently, `cache_creation.ephemeral_5m_input_tokens` may be non-zero even when every breakpoint cc-lb requested was 1 hour; aggregate creation usage must not be attributed only to the markers cc-lb supplied.

### Storage cutover

Do not migrate v2 warm state. Replace or rebuild `prompt_cache_observations` with a v3 schema and start cold for v3 keys. Rollback is previous binary plus database backup, not an in-process v2 fallback.

The v3 observation schema must store enough to score, trace, and debug the simulator:

- upstream id;
- canonical model id under the canonicalization policy below;
- v3 prefix key;
- TTL class;
- expires-at and last-observed timestamps;
- hash/schema version;
- matched content-block index / prefix block index when known;
- estimated prefix tokens;
- token-estimate source/provenance;
- optional last provider read/create token aggregates used for calibration.

The schema may preserve the table name if migrations drop/recreate or rebuild it, but historical v2 rows are intentionally incompatible. `request_events_v1` v2 cache fields are also in remove/replace scope; this ADR accepts loss of direct historical compatibility for those fields in favor of v3 event fields.

### Routing formula

`subscription_preference` remains the formula winner inside the selected quota tier, but its cache input changes from v2 exact/thread lineage to v3 content-block lookback score only.

For candidate `i`:

```text
v3_match_i = longest live v3 lookback cache entry among N..N-19
predicted_read_tokens_i = matched_prefix_tokens_i
predicted_creation_tokens_5m_i = missing 5m breakpoint segment tokens after matched_prefix_tokens_i
predicted_creation_tokens_1h_i = missing 1h breakpoint segment tokens after matched_prefix_tokens_i

cache_value_micros_i =
  predicted_read_tokens_i * (input_price - cache_read_price)
  - predicted_creation_tokens_5m_i * cache_creation_5m_price
  - predicted_creation_tokens_1h_i * cache_creation_1h_price

cache_ratio_i = clamp(
  cache_value_micros_i / max_positive_cache_value_micros_in_tier,
  -1,
  1
)

effective_weight_i =
  quota_weight_i
  * exp(CACHE_LOG_BOOST * cache_ratio_i)
  * warning_multiplier_i
```

This keeps the current monetary interpretation (`read_savings - creation_cost`) and exponential cache boost, but the score must be produced by v3 lookback state. `select_cache_score_by_value(exact_score, thread_score)` is removed from the active path.

### WRH keying

Cache-positive WRH keying must use the matched v3 cache affinity/prefix key. Cache-negative routing falls back to `request_id`.

```text
routing_key = if max_positive_cache_value_micros_in_tier > 0 {
  bucket_v3_cache_affinity_key
} else {
  request_id
}
```

`bucket_v3_cache_affinity_key` is selected before WRH, so it must not depend on the WRH/formula winner. It is the matched v3 cache key from the candidate with the highest positive `cache_value_micros` in the winning tier. Ties are broken deterministically by longest matched prefix tokens, then smallest lookback distance, then upstream id. The same bucket key is used as the WRH routing key for every candidate in that tier. This replaces the previous bucket-level `thread_id` key without creating a circular dependency between WRH key selection and WRH winner selection.

`thread_id` must not be a routing key source. Trace uses `wrh_key_source = cache_hash | request_id`; `thread_id` is not a valid cache-positive WRH key source under v3.

### Thread lineage

Thread lineage is retained only as analysis data for post-hoc validation. It is not a routing formula input, not a WRH key, not a cache score fallback, and not a tie-breaker.

Any remaining lineage collection code must carry an explicit comment equivalent to:

```text
Analysis-only lineage measurement for v3 post-hoc validation. This must not feed routing, WRH keying, or candidate scoring. Delete after v3 validation proves it is no longer needed.
```

Decision records should still capture lineage would-have-predicted and would-have-picked fields where feasible so the v3 rollout can compare old lineage behavior against the new v3 content-block simulator.

### Model canonicalization

Canonicalize only with known provider-valid mappings. Proven pre-4.6 alias/date pairs may collapse to the same key. Do not invent dated IDs for 4.6+ dateless model IDs; preserve the provider-valid ID as sent unless an official mapping exists.

### Minimum cacheable prompt length

The provider minimum is model-specific and deliberately treated as a lookup table, not a monotonic family rule:

| Minimum tokens | Models |
| --- | --- |
| 512 | Opus 5, Fable 5, Mythos 5 |
| 1024 | Opus 4.8, Sonnet 5, Sonnet 4.6, Sonnet 4.5, Opus 4.1, Opus 4, Sonnet 4 |
| 2048 | Mythos Preview, Opus 4.7, Haiku 3.5 (`claude-3-5-haiku-*`) |
| 4096 | Opus 4.6, Opus 4.5, Haiku 4.5 |

The 1024-token row records the pre-existing default behavior; it is not a threshold correction. The corrected exceptions are Opus 4.7 at 2048 tokens, Opus 5 at 512, Mythos 5 at 512, Mythos Preview at 2048, and Haiku 3.5 at 2048. Haiku 3.5 predates the `claude-<family>-<major>-<minor>` ordering and must be keyed on `claude-3-5-haiku-20241022` / `claude-3-5-haiku-latest`; it is retired on the Claude API but still reachable through Bedrock and Google Cloud upstreams. Unknown canonical model ids use the conservative 1024-token fallback and emit a warning rather than inheriting a neighboring version's threshold. The authoritative list is `cache_threshold_tokens` in `crates/cc-lb-engine/src/model_resolution.rs`; this table is a summary.

### Trace and event fields

Routing traces and request events must expose v3-specific evidence:

- matched v3 cache key;
- lookback distance;
- matched content-block index;
- breakpoint content-block index;
- predicted read tokens;
- predicted 5m/1h creation tokens;
- token-estimate source;
- cache value micros;
- formula winner upstream;
- chosen/kept upstream;
- WRH key source (`cache_hash` or `request_id`);
- lineage would-have-predicted score/upstream, explicitly analysis-only.

Provider usage remains the post-route calibration source. Multi-breakpoint provider usage is aggregate-only and must not be treated as exact per-breakpoint attribution.

## Consequences

### Positive

- Routing aligns with Anthropic's documented 20-content-block lookback instead of exact-current-breakpoint matching.
- Cache-positive WRH locality follows the cacheable prefix key, not caller session identity.
- Lineage can still be used to audit the rollout without silently influencing routing.
- The subscription-preference formula stays explainable in money-like units and retains existing quota/warning behavior.
- v3 starts from a clean schema, avoiding accidental trust in incompatible v2 warm rows.

### Negative

- v3 deployment starts cold because v2 warm state is intentionally dropped.
- Historical analysis that expects v2 request-event cache fields must be updated to v3 fields.
- Anthropic's real tokenizer/hash/cache topology remains undisclosed, so the simulator remains a local approximation calibrated by observed usage.
- A hard cutover removes the safety net of v2 fallback logic; rollback requires binary/database rollback discipline.

### Neutral

- Quota tier ordering is unchanged. Cache value still only affects selection within the winning tier.
- The v3 local key must continue using deterministic `serde_json` serialization. Do not replace hash-input serialization with `sonic_rs` unless a separate ADR proves a safe alternative.
- Provider automatic caching can be represented by local automatic breakpoints, but Bedrock support must remain disabled or explicitly marked unsupported.

## Alternatives considered

- **Keep v2 exact-prefix score and use lineage as fallback.** Rejected because this is the v10 failure mode: lineage could dominate routing without identifying the provider-cacheable prefix.
- **Keep thread-id WRH keying when cache value is positive.** Rejected because positive cache value should create affinity to the cache key, not to an arbitrary caller session id.
- **Migrate v2 warm entries into v3.** Rejected because v2 hashes are over different prefix units and cannot be safely mapped to content-block indices/lookback keys.
- **Hash only explicit breakpoints.** Rejected because Anthropic reads can hit within the 20-block lookback behind a breakpoint.
- **Persist every lookback candidate as warm.** Rejected because lookback lookup does not imply provider wrote those candidates.
- **Use provider usage only.** Rejected for active routing because it arrives after the route decision and is aggregate-only for multi-breakpoint requests.

## Verification requirements

- Unit tests for v3 flattening order and valid cache-control placement: tools, system content blocks, and message content blocks are included; message-object-level cache-control is ignored or rejected according to the implementation decision; empty text blocks are excluded; thinking, redacted_thinking, and tool_reference remain prefix participants but cannot become direct breakpoints.
- Unit tests for lookback: `N` and `N-19` can hit; `N-20` cannot; write persistence only records breakpoint keys, not every lookup candidate.
- Unit tests for model canonicalization: proven aliases share keys; fake 4.6+ dated IDs are not fabricated.
- Storage migration tests for SQLite and Postgres proving v2 rows are dropped/replaced and v3 rows round-trip with block index/token provenance.
- Routing tests proving lineage cannot affect `CacheScore`, formula winner, or WRH key source.
- Subscription-preference tests proving cache-positive routing uses a v3 cache key and cache-negative routing uses `request_id`.
- Event/trace tests proving v3 fields and analysis-only lineage fields are recorded.
- Replay over the frozen v10 stuck class, showing the affected lineage-driven cases are explained by analysis fields and routed by v3 cache-key semantics.
- Proxy-path QA through a running server: issue real Anthropic-compatible requests with explicit cache-control blocks, inspect routing trace/request event fields, and verify provider failures are classified separately from proxy routing behavior.
