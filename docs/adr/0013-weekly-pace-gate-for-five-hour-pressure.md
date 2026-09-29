# ADR 0013: Weekly pace gate for five-hour pressure

- Status: Accepted
- Date: 2026-09-26
- Amends: ADR 0008 (base pressure), ADR 0010 (selection key and formula version)

## Context

ADR 0008 scores each base window independently and combines them with a
smoothmax, so the five-hour window is treated as its own use-it-or-lose-it
deadline. Near every five-hour reset its pressure rises to
`ln(remaining / 0.01)`, up to about 4.6, even when the account still has most of
its weekly quota and plenty of time to use it.

That pressure is not a real loss. The five-hour window refills inside the
shared weekly window, and every unit of work consumes both. Skipping the
current five-hour window only forfeits quota when the weekly quota cannot be
consumed by the five-hour windows that still follow before the weekly reset.

In one deployment this let accounts whose weekly usage was far ahead of pace
take cold requests away from an account with real weekly urgency during the
last ~20–30 minutes of each of their five-hour windows. Prompt-cache ownership
then kept those threads on the wrong account.

cc-lb runs against arbitrary plans and fleets, so the fix must not depend on
plan sizes, observed demand, or constants fitted to one deployment.

## Decision

Keep ADR 0008 pressure for every window, but gate the five-hour term:

```text
L5 = 18_000 s, L7 = 604_800 s
T5 = seconds until the five-hour reset
behind(w) = (1 - util_w) > max(0, T_w - T5) / L7

keep_5h =
  5h snapshot unusable                      -> true
  shared 7d snapshot unusable               -> true
  behind(shared 7d)                         -> true
  behind(any usable relevant scoped weekly) -> true
  otherwise                                 -> false

p5_effective = keep_5h ? p5 : 0
combined     = weighted_smoothmax(p5_effective, weekly_smoothmax(...))
```

"Usable" is the ADR 0008 pressure gate: fresh, finite utilization, and a
future `resets_at`. `quota_urgency_5h` in traces and request events now
records `p5_effective`, so `combined >= quota_urgency_5h` still holds.

Within the near-cost band, the selection key from ADR 0010 becomes:

```text
(tier_urgency * ranking_warning_multiplier, raw_p5, ranking_warning_multiplier,
 -cost when known, -upstream_id, -original_index)
```

`raw_p5` is the ungated five-hour pressure. It only breaks exact urgency ties,
so suppressed candidates still prefer the sooner-refilling window instead of
collapsing onto the lowest upstream ID. The trace `formula_version` is
`cost-first-v2`.

## Why the gate is safe without plan information

Let `r` be the fraction of the weekly quota consumed by one full five-hour
window, and `n = (T7 - T5) / L5` the number of five-hour windows left after the
current one. Those windows can consume at most `r * n` of the weekly quota, so
the current window is only needed when `1 - util_7d > r * n`.

- If `r >= L5 / L7` (a five-hour window can move the weekly meter at all within
  a week), then `r * n >= (T7 - T5) / L7`. Weekly usage at or ahead of linear
  pace therefore guarantees the remaining weekly quota can be consumed later,
  for every such plan. The gate never suppresses a real loss.
- If `r < L5 / L7`, weekly usage produced by the account's own five-hour
  windows cannot reach linear pace, so the gate stays open. Only weekly usage
  that was already present (for example after a plan change) can close it on
  such plans.

The rule uses only snapshot values and structural window lengths. When `r` is
unknown, a snapshot behind pace cannot tell a real five-hour loss from a fake
one: the same snapshot is a real loss for small `r` and no loss for large `r`.
The gate therefore leaves those cases unchanged instead of guessing.

Model-scoped weekly windows drain only with their model's traffic and have no
structural lower bound on `r`, so they may keep the five-hour pressure but never
suppress it on their own.

## Consequences

### Positive

- Accounts ahead of weekly pace no longer take cold requests from accounts that
  can still forfeit weekly quota.
- No new runtime state, configuration, or cross-pod coordination. Missing or
  stale data falls back to ADR 0008 behavior exactly.

### Negative

- Accounts behind weekly pace keep ADR 0008 five-hour pressure, so an idle
  account early in its week can still win near its five-hour reset. Fixing that
  requires knowing `r`, which snapshots do not provide.
- Weekly urgency leaders receive more cold traffic, which increases five-hour
  saturation and cache re-priming somewhat (about +9% forced cold switches in
  synthetic evaluation).
- The gate is a step function; a one-point utilization change can flip it.
- `quota_urgency_5h` history is not comparable across `cost-first-v1` and
  `cost-first-v2` rows.

## Alternatives considered

- **Cap five-hour pressure at a constant or at `p7 + kappa`.** Rejected: the
  constant only separates real from fake losses for a specific plan ratio.
- **Scale five-hour pressure by a prior over `r` (log-uniform).** Rejected: it
  suppressed real losses when `r` is small and increased unserved demand in
  synthetic evaluation.
- **Estimate `r` online per upstream and escalate "critical" accounts.**
  Rejected for now: cold-start and plan-change bias pushed toward
  over-suppression, pod-local state broke deterministic selection, and the
  critical class caused monopolization and saturation.

## Evidence

A synthetic benchmark of 217 environments (plan ratios `1/33.6..1`, fleets of
1–50 accounts, demand-bound and supply-bound load, flat/diurnal/bursty demand,
warmup on/off, plan changes, stale and missing windows, model-scoped weekly
windows) compared the gate with ADR 0008. Across two seeds the gate reduced
realized forfeited-and-unserved work (net -82 and -118 work units, paired sign
test p = 4e-15 and 3e-8) and excess unserved demand by about 60%. Its costs were
the forced cold switch increase noted above and a small regression right after
plan changes.

## Verification

`subscription_preference/tests/weekly_pace_gate.rs` covers the incident shape,
the exact pace boundary, unusable weekly fallbacks, model-scoped keep-only
behavior, and the raw five-hour tiebreak.
