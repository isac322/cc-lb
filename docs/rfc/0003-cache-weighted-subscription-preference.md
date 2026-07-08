# RFC-0003: Cache-weighted subscription-preference (v7)

- Feature Name: `cache-weighted-subscription-preference`
- Start Date: 2026-07-06
- Status: Draft
- Related PRs: #350 (v7 rewrite), supersedes prior PR #322 routing effect
- Related ADRs: ADR 0004 (this RFC's decision record), ADR 0003 (WRH shape it extends)
- Superseded in part by: ADR 0005, which restores thread-keyed routing for non-empty `thread_id`, adds bounded same-thread owner memory with hysteresis, and excludes `7d_sonnet`/`7d_opus` from routing criteria.

## Summary

Merge the prompt-cache signal into `subscription-preference`'s within-tier Weighted Rendezvous Hash (WRH) as an exponential multiplier on quota urgency. Delete the standalone `cache_affinity` filter from the default router chain. Bump the salt from `v6` to `v7`.

The resulting WRH weight `effective_weight_i = quota_urgency_i * exp(CACHE_LOG_BOOST * cache_ratio_i)` produces the observed-and-required semantics: a warm cache-holder wins with ~99% share up to ~95% utilisation, a probabilistic crossover with a fresh-quota peer at ~99% utilisation, and hard tier-eviction at `util >= 1.0` handles the final spill.

## Motivation

Two production incidents on `thread-prod-cache-redacted`:

1. **2026-07-05 06:24 UTC scatter** — 50 consecutive turns bounced across four upstreams because `cache_affinity`'s binary `> 0` filter left all four in the pool (BP0 warm on each) and subscription's request-id keying redraw was independent per turn. Estimated waste: ~$25 per 5-turn scatter block.
2. **2026-07-06 02:34 UTC sticky cold** — Once the fix from PR #322 was live, `thread_id`-keyed WRH deterministically re-picked the same (now-cold) upstream on every post-idle turn. Three consecutive $5 cache_creation events with zero cache-read benefit.

Prior fix attempts and why they failed:

- **PR #350 v5:** subscription switched between `thread_id` and `request_id` keying by inspecting `cache_score`. Fixed both incidents but violated the single-concern boundary.
- **PR #350 v6:** cache_affinity became a max-ranker; subscription reverted to pure request-id WRH. Restored separation, but the max-ranker filter dropped every non-max candidate before subscription's tier assessment, so when the max-cache upstream crossed `util >= 1.0` and became tier-blocked there was no fallback available and the request 429'd.

Both filters can be reconciled by placing the cache signal inside subscription's weight, deleting cache_affinity from the default chain, and letting tier-assessment handle the final spill. The remaining engineering question — resolved by consultation with Oracle (bg_bc8e13c4) — is the curve shape.

## Non-goals

- Cross-instance session affinity, prefix-hash-keyed WRH, or a persistent thread-affinity map. Discussed and rejected in prior consultations (bg_63be9089, bg_a5698121, bg_aab7308b).
- Tuning per-principal `CACHE_LOG_BOOST`. Ships as a compile-time constant. Making it configurable is a follow-up if operators want per-tenant curves.
- Rewriting `cache_affinity` for non-default chains that still name it. The type and its trace shape stay in the tree.
- Changing `RequestContext.thread_id` semantics. Header is still extracted, still surfaced in `TierMemory` and traces; only routing use is removed in this v7 RFC. ADR 0005 later supersedes this point by restoring thread-keyed routing with bounded owner memory.
- Wire-level plugin API changes.

## Design

### Formula

Inside `subscription_preference::pick_within_tier`, for each candidate in the current tier bucket:

```rust
let max_cache_tokens = bucket
    .iter()
    .map(|a| a.candidate.cache_score.as_ref()
        .map_or(0, |s| s.predicted_cache_read_tokens))
    .max()
    .unwrap_or(0);

let cache_ratio = if max_cache_tokens > 0 {
    predicted_cache_read_tokens_i as f64 / max_cache_tokens as f64
} else {
    0.0
};

let cache_weight_multiplier = (CACHE_LOG_BOOST * cache_ratio).exp();

let quota_weight = if total_urgency < EPSILON { 1.0 } else { assessment_i.urgency };

let effective_weight = quota_weight * cache_weight_multiplier;

let u = hash_to_open_unit(rendezvous_hash(
    RENDEZVOUS_SALT,
    ctx.request_id.as_str(),
    assessment_i.candidate.upstream_id,
));

let score = if effective_weight > 0.0 {
    -u.ln() / effective_weight
} else {
    f64::INFINITY
};
```

Lowest `score` wins. Existing tiebreak chain (rendezvous hash desc, upstream_id asc, original_index asc) stays.

### The constant

```rust
pub const CACHE_LOG_BOOST: f64 = 9.574_063_128_362_267;   // ln(8100) / 0.94
```

Calibration target: bear-max at `util = 0.99`, `cache_ratio = 1.0` (250K predicted-read) ties Runbear at `util = 0.10`, `cache_ratio = 0.06` (15K incidental BP0 warm), assuming both have equal `remaining_secs` and `capacity_multiplier == 1.0`.

Numeric crossover profile at bear-max cache = 250K, Runbear cache = 15K, both `remaining_secs = 18000`:

| bear util | bear win % | Runbear win % |
|:--:|--:|--:|
| 0.30 | 99.98 | 0.02 |
| 0.90 | 99.01 | 0.99 |
| 0.95 | 96.15 | 3.85 |
| 0.97 | 90.00 | 10.00 |
| **0.99** | **50.00** | **50.00** |
| 0.995 | 20.00 | 80.00 |
| ≥ 1.00 | 0 (tier-evicted) | 100 |

### Salt & version stamping

```rust
pub const SALT_VERSION: &str = "v7";
pub const RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v7:cache-weighted-wrh:2026-07-06";
```

The docstring on `RENDEZVOUS_SALT` gains a v7 entry recording (a) the exponential-multiplier shape, (b) `CACHE_LOG_BOOST`'s calibration target, (c) the incident this replaces.

The `SubscriptionPreferenceTrace.rendezvous_salt_version` field continues to serialise `"v7"` so `request_events_v1.payload.routing_trace.stages[*].subscription_preference.rendezvous_salt_version` queries can distinguish pre-v7 rows.

### Trace payload

`CandidateUrgency` (which is what the WRH exposes per candidate in the subscription trace) gains four fields:

```rust
#[serde(default)]
pub quota_urgency: f64,
#[serde(default)]
pub predicted_cache_read_tokens: u32,
#[serde(default)]
pub cache_ratio: f64,
#[serde(default)]
pub cache_weight_multiplier: f64,
#[serde(default)]
pub effective_weight: f64,
```

The existing `urgency` field stays for backwards compatibility with v6 traces but new writers populate it with `effective_weight` so consumers that only read `urgency` still see the current selection weight. Consumers that need the pre-boost quota value use `quota_urgency`.

`SubscriptionPreferenceTrace` gains no new top-level fields.

The stand-alone `StageDecision.cache_affinity` trace remains legal on the wire (default `None`); it stops being produced only once operators remove `cache_affinity` from the affected principal's chain via the deploy runbook. Existing `request_events_v1` rows with `cache_affinity` populated continue to deserialize correctly.

### Filter chain

There is no code-level `router_chain_default` symbol. The active chain per principal is stored in `plugin_chains_v2` and resolved at DynamicView rebuild time by `crates/cc-lb-server/src/dynamic_view_builder.rs:683`. `BUILTIN_CACHE_AFFINITY_ID` remains a valid builtin so custom chains that keep it still work.

For v7 to take effect on the isac-opencode principal, its `router_chain` row must be updated to remove the `cache_affinity` entry. That is a deploy-time operator action, not a code change in this PR — see the deploy runbook in `.omo/plans/pr350-v7-cache-weighted-subscription.md` Task 12 for the exact admin-API and SQL-migration paths.

`CacheAffinityFilter` and `metadata()` stay in the codebase so admins with hand-authored chains that name `cache_affinity` can still resolve it. The updated `cache_affinity.rs` module docstring notes that keeping `cache_affinity` in a chain neutralises v7's spill semantics — running both re-introduces the v6 tier-eviction bug.

### Tier ordering

Unchanged. `evaluate` still iterates `KnownBase → PartialBase → Overage → UnknownProbe` and returns on the first non-empty bucket. `pick_within_tier` receives only candidates in a single tier, so `max_cache_tokens` is bucket-scoped and lower-tier candidates never influence higher-tier selection.

### `RequestContext.thread_id`

Field retained. Population path (`x-claude-code-session-id` → `x-claude-session-id` → `x-session-affinity` → `x-session-id`) unchanged. In this v7 RFC, `TierMemory` still keys on it for the `previous_tier` observability field, `wrh_session_key` is deleted, and nothing else reads `thread_id` for routing. ADR 0005 supersedes this by using a normalized `thread_id` routing key and owner memory.

### Uniform-fallback interaction

The existing "uniform" branch in `pick_within_tier` fires when `total_urgency < EPSILON` — i.e. every candidate's quota urgency is effectively zero. Under v7, this branch still triggers when quota is uniformly cold; each candidate's `quota_weight` becomes `1.0`, and the cache multiplier applies on top, so a warm cache still decides.

### Feature-flag / rollout

Direct replacement, no runtime flag. The salt bump prevents any hidden dependency on v6 hash values; behaviour differences are visible in the trace via `rendezvous_salt_version`.

## Implementation

### File-level changes

- `crates/cc-lb-engine/src/builtin_filters/subscription_preference.rs`
  - Add `pub const CACHE_LOG_BOOST: f64 = 9.574_063_128_362_267;`
  - Update `SALT_VERSION` to `"v7"` and `RENDEZVOUS_SALT` accordingly; extend the docstring with v7 entry.
  - In `pick_within_tier`, compute `max_cache_tokens` across the bucket; compute per-candidate `cache_ratio`, `cache_weight_multiplier`, `effective_weight`; use `effective_weight` where the current code passes `assessment.urgency` into WRH scoring.
  - In `wrh_key`, replace the `urgency` parameter with `effective_weight` (and keep the uniform-branch `weight = 1.0` behaviour, now applied before the cache multiplier).
  - Populate the new `CandidateUrgency` fields (`quota_urgency`, `predicted_cache_read_tokens`, `cache_ratio`, `cache_weight_multiplier`, `effective_weight`) when building trace rows.
  - Delete `wrh_session_key` and its call site; keep `RequestContext.thread_id` field untouched but remove routing use.
- `crates/cc-lb-engine/src/builtin_filters/cache_affinity.rs`
  - Retained as an opt-in filter for custom chains; `metadata()` docstring notes v7 recommends removing it.
- `crates/cc-lb-server/src/dynamic_view_builder.rs`
  - No source edit. Line 683 already dispatches by ID; `cache_affinity` is dropped only when the per-principal `router_chain` row omits it. Deploy-time operator action documented in the plan.
- `crates/cc-lb-plugin-api/src/types.rs`
  - Extend `CandidateUrgency` with the five new fields (all `#[serde(default)]`, `#[serde(skip_serializing_if = "…is_default")]` where sensible so v6 rows round-trip unchanged).
  - Do NOT touch `SubscriptionPreferenceTrace`, `StageDecision`, or `FilterOutput` top-level shapes.
- `crates/cc-lb-engine/src/builtin_filters/subscription_preference/tests.rs`
  - Add the five acceptance tests listed in `## Testing`.
  - Update or delete v6 tests that asserted request-id-only WRH behaviour (`different_request_ids_spread_across_upstreams`, `same_thread_id_does_not_pin_when_cache_affinity_did_not_narrow_candidates`, `wrh_key_source_is_request_id_even_when_thread_id_present_v6`) — under v7 they become moot (subscription always keys on request_id AND cache-boosts).
- `crates/cc-lb-observability/src/redaction.rs`
  - Extend `subscription_preference_json_len` / `candidate_urgency_json_len` (or equivalent) helpers to account for the new fields so the 4096-byte truncation cap remains honoured. Add a computed-vs-actual length test.

### Tests

Numeric assertions verify the crossover table. Implementation may inline `bucket_from_pair(cache_tokens, util)` fixtures.

1. **`warm_low_util_pins_cache_holder`** — bear util `0.30`, cache 250K; Runbear util `0.10`, cache 15K. Assert bear's `effective_weight ≈ 0.3916393859`, Runbear's `≈ 7.99e-5`, bear win probability `≈ 0.99979596`.
2. **`warm_95_percent_still_pins_cache_holder`** — bear util `0.95`. Assert bear `effective_weight ≈ 0.001998`, Runbear `≈ 7.99e-5`, bear win probability `≈ 0.961538`.
3. **`warm_99_percent_starts_spreading`** — bear util `0.99`. Assert effective weights approximately equal (`|Δ| < 1e-9` within float tolerance), win probability `≈ 0.5`.
4. **`warm_cache_holder_blocked_spills`** — bear `util >= 1.0` (or fresh + status=rejected with no overage). Assert bear absent from the KnownBase bucket via `assess_candidate` returning `None`; Runbear wins outright.
5. **`all_cold_spreads_by_quota_wrh`** — every candidate has `predicted_cache_read_tokens = 0`. Assert `cache_ratio = 0`, `cache_weight_multiplier = 1.0`, `effective_weight == quota_urgency`; distribution matches the existing v6 `four_identical_candidates_distribute_uniformly_across_many_requests` shape.

Plus regression / invariant tests:

6. **`rendezvous_salt_embeds_declared_version`** — existing invariant, updated to check `v7`.
7. **`tier_ordering_never_broken_by_cache_boost`** — synthesise a `PartialBase` candidate with the deepest cache and a `KnownBase` candidate with zero cache; assert `KnownBase` wins regardless of cache boost.
8. **`cache_boost_calibration_at_99_percent`** — verify `CACHE_LOG_BOOST` calibration by asserting equal `effective_weight` at bear (util 0.99, cache_ratio 1.0) vs Runbear (util 0.10, cache_ratio 0.06) at the same reset horizon and capacity multiplier.

### Observability follow-ups

Non-blocking, out of PR #350 scope:

- Dashboard tile: crossover point empirical distribution (`quota_urgency` vs `effective_weight` scatter over 24h).
- Alert: sustained `bear-max util >= 0.99` with 0% peer win rate for 15+ min (indicates calibration drift or `CACHE_LOG_BOOST` needs revision).
- Metric: histogram of `cache_weight_multiplier` per selection decision.

### Rollout

1. Land PR #350 v7 on master (with ADR 0004, this RFC, and tests).
2. Build native aarch64 binary; deploy to `~/.local/bin/cc-lb` with backup of previous.
3. `systemctl --user restart cc-lb.service` (with user approval).
4. Post-deploy verification via `/tmp/pr350_post_deploy_verification.sql`-style queries against local sqlite: (a) recent traces show `rendezvous_salt_version = "v7"`, (b) cold-idle turn cluster spreads across candidates, (c) no sustained cache_creation clumping on a single upstream after 5+ min idles.
5. If crossover point is empirically wrong (e.g. spread begins at 97% instead of 99%), revisit `CACHE_LOG_BOOST` calibration in a follow-up PR — do not hotfix in the deploy window.

## Migration

- Legacy v4/v5/v6 trace rows in `request_events_v1.payload.routing_trace.stages[*]` continue to deserialize (all new fields are `#[serde(default)]`).
- Legacy `router_chain` configurations that list `cache_affinity` still work but no longer receive the v7 semantics. Operator documentation notes this.
- No storage schema migration needed.
- No plugin wire ABI change.

## Rejected alternatives

- **Linear `weight = quota_urgency * (1 + K * cache_ratio)`.** Cannot satisfy both "still pins at 95%" and "spreads before 100%" for any single K. `(1-u)^2` decay demands an exponential compensation.
- **Piecewise/threshold blending.** Introduces discontinuity that is harder to test and reason about; the exponential is smooth and natural.
- **Max-ranker inside subscription** (moved v6 semantics). Only spills at hard tier-eviction (`util >= 1.0`), violates the "spread before 1.0" requirement.
- **Multiplier via `1 / (1 - cache_ratio)` or reciprocal shapes.** Discontinuities at `cache_ratio = 1.0`; requires clamping and produces sharp curve edges.
- **Prefix-hash-keyed WRH (LiteLLM style).** Orthogonal to quota-vs-cache trade-off; addresses cross-client cache-locality which is not the failure mode we have.

## Open questions

- Should `CACHE_LOG_BOOST` be per-principal or per-tier configurable? Defer to post-deployment empirical data.
- Should we still record `wrh_key_source` in the trace? Under v7 it is always `RequestId`; ADR 0005 supersedes this and records `ThreadId` when a non-empty thread id is used.
- Should the tests use a `MockNow` trait or `Instant` fixtures to eliminate flake from `TierMemory` clock-based tests? Existing pattern from v6 tests suffices; revisit if flake appears.
