# ADR 0010: Cost-first deterministic subscription selection

- Status: Accepted
- Date: 2026-07-16

## Context

Within the first non-empty subscription tier, weighted rendezvous hashing let a
warm owner lose to a more expensive candidate based on a routing hash. In an
earlier probabilistic-mixture replay, an owner with probability 0.76 lost
134 of 137 changed rows. Session cache ownership is commonly about ten times
cheaper than re-priming the cache, so this tail leakage is an avoidable cost.

## Decision

Use `cost-first-v1` within the chosen tier only. Existing tier eligibility,
hard negatives, base-signal classification, quota pressure, urgency, and
fallback reasons are unchanged. Provider `allowed_warning` status and finite
`utilization >= surpassed_threshold` remain warning-positive quota observations.

For every candidate with all cache-score and pricing inputs, calculate
`cost_i = uncached * input + create_5m * price_5m + create_1h * price_1h + read * price_read`
in micros. If any cost exists, let `min_cost` be the minimum and retain only:

```text
u128(cost_i) * 100 <= u128(min_cost) * 105
```

Candidates without a cost are excluded while any cost exists. If every cost is
unknown, all candidates remain.

The ranking warning multiplier depends on the selected tier:

```text
KnownBase or PartialBase: 1.0
Overage with any warning-positive base window: 0.20
otherwise: 1.0
```

Base warning observations therefore remain available for classification and
tracing without changing base-tier ranking. The Overage tier retains its
existing warning penalty. Select the maximum lexicographic key:

```text
(tier_urgency * ranking_warning_multiplier, ranking_warning_multiplier,
 -cost when known, -upstream_id, -original_index)
```

This makes a cheap warm owner deterministic, while cold requests tie on cost
and fall through to the use-it-or-lose-it urgency key at zero cost penalty.
Quota-exhausted upstreams remain excluded by existing tier and hard-negative
rules. The trace records `formula_version = cost-first-v1` as the selection
identity and reports the tier-specific ranking multiplier in
`warning_multiplier`.

## Interface cleanup

Because this project has not deployed the WRH-era trace and request-event
interfaces, ADR 0010 removes the dead compatibility surface instead of carrying
optional legacy fields. Removed fields are `WrhKeySource`, `wrh_key_source`,
`rendezvous_salt_version`, capture `salt_version`, `urgency`,
`quota_weight_factor`, `quota_uniform_fallback`, `cache_weight_multiplier`,
`effective_weight`, request-event `quota_cache_multiplier`, and request-event
`quota_effective_weight`. SQLite and Postgres request-event migrations drop the
dead persisted columns.

## Evidence

Dynamic quota-and-cache replay over 1,220 captured production rows measured
95,259,796 micros for cost-first versus 100,069,987 for the current formula
(-4.8%), with hit ratio 0.8642 versus 0.8553. A 5h-burst stress served 259
versus 224 requests at 1x quota and 1,160 versus 881 at 5x quota. At identical
cost, selected-upstream urgency averaged 0.804 versus 0.345 for plain
min-cost, a 2.3x improvement. Capacities were inferred from the replay DB;
these results are not an Anthropic absolute-parity claim.
