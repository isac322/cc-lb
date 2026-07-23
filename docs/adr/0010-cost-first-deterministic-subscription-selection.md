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

Use `cost-first-v2` within the chosen tier only. Existing tier eligibility,
hard negatives, base-signal classification, quota pressure, fallback reasons,
and the v1 cost gate are unchanged.

For every candidate with all cache-score and pricing inputs, calculate
`cost_i = uncached * input + create_5m * price_5m + create_1h * price_1h + read * price_read`
in micros. If any cost exists, let `min_cost` be the minimum and retain only:

```text
u128(cost_i) * 100 <= u128(min_cost) * 105
```

Candidates without a cost are excluded while any cost exists. If every cost is
unknown, all candidates remain.

For warning-positive `KnownBase` and `PartialBase` candidates, retain the
default `0.20` multiplier unless every warning-positive window has pressure
that justifies spending quota before reset. A warning window can relax only
after utilization reaches its explicit `surpassed_threshold`, or `0.98` when
the provider omitted that threshold. Let `P_warning` be the minimum qualifying
pressure across warning-positive windows:

```text
activation = ln(1.10)
ceiling = ln(2.0)
x = clamp((P_warning - activation) / (ceiling - activation), 0, 1)
smoothstep(x) = x^2 * (3 - 2x)
warning_multiplier = 0.20 + 0.60 * smoothstep(x)
```

Using the minimum is a safety gate because every request debits every relevant
quota window. One warned window with no use-it-or-lose-it pressure therefore
keeps the full penalty even if another warned window is near reset. Clean
candidates retain multiplier `1.0`; warned base candidates never exceed `0.80`.
Overage candidates retain the fixed `0.20` warning multiplier because blocked
base quota cannot be consumed through overage.

Select the maximum lexicographic key:

```text
(tier_urgency * warning_multiplier, warning_multiplier,
 -cost when known, -upstream_id, -original_index)
```

This makes a cheap warm owner deterministic, while cold requests tie on cost
and fall through to the use-it-or-lose-it urgency key at zero cost penalty.
Quota-exhausted upstreams remain excluded by existing tier and hard-negative
rules. The trace records `formula_version = cost-first-v2` as the selection
identity.

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

The v2 warning policy was evaluated against a 2,568-row `capture_v1` trace. An
exact trace-state rescore matched the recorded v1 winner on 2,396 rows and
changed none of those calibrated decisions because the sample contained no
qualifying near-reset warning state. A deliberately more aggressive comparison,
which relaxed from combined urgency rather than the minimum warned window,
changed 270 of 1,650 replayable decisions while preserving normal-replay cost
(`267,650,488` micros) and average hit ratio (`0.883550`). Under a 5h-burst
stress it changed 74 selections but served 236 requests versus 239 for the
fixed `0.20` baseline, with 25,134 more inferred quota tokens left unused. This
rejected comparison shows why v2 requires every warned window to qualify
instead of applying a candidate-global relaxation.

For a single 7d warning at 98% utilization inside the 1% pressure floor, v2
raises effective urgency from `0.138629` to `0.554518` while retaining a 20%
warning discount. In a deterministic drain simulation against a clean peer
with pressure `0.15`, v1 consumed none of the extra quota above the 1% target
floor; v2 consumed 58.4% before the warned score fell below the peer.
