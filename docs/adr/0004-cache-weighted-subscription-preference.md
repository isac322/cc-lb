# ADR 0004 — Cache-weighted subscription-preference (exponential cache boost, salt v7)

- Status: Proposed
- Date: 2026-07-06
- Ships with: PR #350 (rewrite)
- Supersedes: ADR 0003's within-tier WRH signature; the tier ordering and quota-urgency computation from ADR 0003 stay unchanged.

## Context

Two production incidents on session `ses_example00000000` exposed a common root failure between the `cache_affinity` and `subscription_preference` filters:

1. **2026-07-05 06:24 UTC — scatter.** cache_affinity's `predicted_cache_read_tokens > 0` filter kept every upstream whose shared BP0 (system-prompt) hash was warm — all four candidates for `isac-opencode`. subscription_preference then broke the tie with request-id-keyed WRH, drawing an independent random winner per turn. Fifty consecutive turns bounced across `Example Org`, `example-org`, `example-peer`, `example-secondary-max`. Each switch paid ~250K cache_creation tokens at Opus-write rates.

2. **2026-07-06 02:34–02:48 UTC — sticky cold.** PR #322 keyed subscription's WRH on `thread_id` to fix (1). Once the session went idle past 5 minutes, every candidate's cache expired but `thread_id`-keyed WRH still deterministically re-picked the same (now-cold) upstream every turn. Three consecutive post-idle turns clumped on `example-secondary-max`, each paying ~$5 to re-create the cache.

Two abandoned repairs preceded this decision:

- **PR #350 v5 (rejected):** `subscription_preference` inspected `cache_score` to switch its WRH key between `thread_id` (when any candidate was warm) and `request_id` (when all cold). Fixed both incidents but violated the single-concern boundary — subscription was reasoning about cache.
- **PR #350 v6 (committed to branch, later flagged buggy):** cache_affinity became a max-ranker that dropped every candidate whose `predicted_cache_read_tokens` was below the pool maximum; subscription reverted to pure request-id WRH. Restored separation, but tier-eviction spill was impossible: when the max-cache upstream's utilization crossed 1.0, cache_affinity had already dropped every fallback, so subscription had no candidate left to route to. The blocked upstream still received the request, which then 429'd.

The remaining design question is what curve the cache signal should take when embedded in the subscription weight so that (a) a cache-holding upstream keeps winning as its utilization rises well past normal thresholds, (b) but eventually loses probabilistic weight to a cache-cold candidate before the hard tier-eviction line, so the session redistributes traffic before the cache-holder saturates completely, and (c) at hard tier-eviction (`util >= 1.0` → `HardNegative`) the cache-holder is deterministically removed by tier assessment and the fallback wins outright.

Numerically the constraints are:

- At the 30–90% utilisation band, the cache-holding candidate must dominate — the observed cost of losing a warm 250K-cache pin is roughly $5 per turn, so the WRH crossover at that band should sit above 99%.
- At 95% utilisation the cache-holder should still hold at least a 90% win share.
- Somewhere around 99% utilisation, the fresh-quota candidate should reach 50% win share so that spread begins.
- Between 99% and 100%, the fresh-quota candidate should dominate.
- At `util >= 1.0` the current tier logic already removes the candidate, so no formula change is required for the final spill.

`urgency_quota = (1 - util)^2 / remaining_secs * capacity_multiplier` decays quadratically: at 95% utilisation it is already ~324× smaller than a 10%-utilisation candidate's urgency. A linear cache boost of the form `1 + K * cache_ratio` with any reasonable `K` cannot recover that gap without exploding at low `util`. The transition has to be produced by a factor that compounds against the quadratic decay of quota urgency.

## Decision

Delete the standalone `cache_affinity` filter from the default chain. Move the cache signal into `subscription_preference`'s WRH weight as an **exponential multiplier** on `quota_urgency`, computed inside each tier bucket so cross-tier ordering is unaffected. Bump the salt version from `v6` to `v7`.

```rust
cache_ratio_i = if max_cache_tokens_in_current_tier_bucket > 0 {
    predicted_cache_read_tokens_i / max_cache_tokens_in_current_tier_bucket
} else {
    0.0
}

cache_weight_multiplier_i = exp(CACHE_LOG_BOOST * cache_ratio_i)

effective_weight_i = quota_urgency_i * cache_weight_multiplier_i

score_i = -ln(u_i) / effective_weight_i          // lower wins
```

with

```rust
pub const CACHE_LOG_BOOST: f64 = 9.574_063_128_362_267;   // ln(8100) / 0.94
```

`CACHE_LOG_BOOST` is calibrated so that a candidate with `cache_ratio == 1.0` and utilisation `0.99` (both windows sharing the same reset horizon and capacity multiplier) has an `effective_weight` equal to a peer with `cache_ratio == 0.06` (i.e. only the shared BP0 hash) and utilisation `0.10`. That is the observed 250K-vs-15K, 99%-vs-10% cross-over point the constraints in Context require.

Tier ordering is untouched: within-tier weight comparison never affects which tier wins. The uniform fallback (`weight = 1.0` when total urgency ~ 0) also stays; cache multiplier still applies on top so a cold-quota tie still lets a warm cache decide.

Salt update:

```rust
pub const SALT_VERSION: &str = "v7";
pub const RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v7:cache-weighted-wrh:2026-07-06";
```

The `CacheAffinityFilter` type and its wire trace shape remain in the tree so alternate chains and legacy trace rows still deserialize. There is no code-level default chain to edit: the active chain is stored per-principal in `plugin_chains_v2` and resolved at `crates/cc-lb-server/src/dynamic_view_builder.rs:683`. Removing `cache_affinity` from the isac-opencode principal's chain is a deploy-time operator action documented in `.omo/plans/pr350-v7-cache-weighted-subscription.md` Task 12 (admin API PATCH or one-shot SQLite mutation).

## Consequences

### Positive

- Both 2026-07-05 (scatter) and 2026-07-06 (sticky cold) incidents fold into a single mechanism; the same WRH weight handles both without a special case per direction.
- Subscription no longer needs `thread_id`; PR #322's routing dependency on `x-claude-code-session-id` is retired. The header still populates `RequestContext.thread_id` for observability (tier flip tracking, log correlation) but no longer influences routing.
- Cache-holder spill is smooth. As the cache-holder's utilisation climbs into the 97–99.5% band, the cache-cold peer's win share rises from 10% to 80% before the hard tier-eviction line, so the session begins re-warming a second upstream before the first fully rejects.
- Removes a filter from the default chain; one less stage in the routing pipeline, one less trace row per request.

### Negative

- The exponential multiplier is not obvious to reason about at a glance. Traces must expose all four components (`quota_urgency`, `cache_ratio`, `cache_weight_multiplier`, `effective_weight`) so operator debugging remains tractable.
- `CACHE_LOG_BOOST` is a hard-coded calibration constant tied to specific choices of `HEADROOM_EXPONENT`, canonical reset horizons, and the "99% is the crossover" target. Changing any of those inputs invalidates the calibration; a comment on the constant must call this out.
- Tests that previously locked in cache_affinity's max-ranker behaviour must be rewritten in subscription-preference; the split-filter observability surface changes shape.
- Existing `router_chain` rows in `plugin_chains_v2` that include `cache_affinity` continue to instantiate the filter — no code default was edited. The v7 fix takes effect on the isac-opencode principal only after the deploy runbook applies the admin-API PATCH or SQLite mutation from Task 12 of the implementation plan. Operators on other principals must apply the same step manually.

### Neutral

- Wire ABI unaffected: no new `FilterOutput` fields; new trace fields are additive on `SubscriptionPreferenceTrace` / `CandidateUrgency` with `#[serde(default)]`.
- Existing v6 legacy trace rows continue to deserialize (missing new fields default to zero / None). Historic queries do not break.
- Salt `v5` was skipped; `v6` was implemented and buggy; jumping to `v7` avoids salt reuse.

## Alternatives considered

- **Linear cache boost `weight = quota_urgency * (1 + K * cache_ratio)`.** Rejected: for any single `K`, either the boost is too weak at 95% utilisation (`K ≈ 10` switches at ~95%, violating requirement (a)) or it forces example-org to keep winning past 99.9% utilisation (`K ≫ 10000` blows up in the low-utilisation band and never smoothly hands off). Linear cannot match `(1-u)^2` decay.
- **Max-ranker inside subscription (v6 semantics, moved into the same filter).** Rejected: only spills at `util >= 1.0` and the user explicitly requires spread to begin before 1.0.
- **Piecewise cutoff — max-ranker below some utilisation, blend above.** Rejected: an arbitrary threshold introduces a discontinuity that is harder to defend and complicates tests. The exponential curve is naturally smooth.
- **Prefix-hash-keyed WRH (LiteLLM `prompt_prefix_affinity` style).** Considered in prior consultation (bg_a5698121). Solves shared-cache-locality across multiple clients but does not address the quota-vs-cache trade-off within one client's session. Orthogonal to this decision.
