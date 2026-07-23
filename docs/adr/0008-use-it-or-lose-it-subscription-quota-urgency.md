# ADR 0008 — Use-it-or-lose-it subscription quota urgency

- Status: Accepted
- Date: 2026-07-09
- Ships with: pending
- Supersedes: ADR 0003's base-window quota urgency formula and capacity multiplier inside base quota urgency; ADR 0010 amends the warning multiplier used during deterministic cost-first selection.

## Context

`subscription_preference` currently computes each fresh base-window contribution as:

```text
old_U_w = capacity_multiplier(upstream) * (1 - clamp(utilization_w, 0, 1))^2 / remaining_secs_w
quota_weight = max(old_U_5h, old_U_7d)
effective_weight = quota_weight * cache_weight_multiplier(cache_ratio) * warning_multiplier
```

That formula is a seconds burn-pressure score. It grows as reset approaches, but it also rewards raw headroom even when the upstream is already on pace and embeds plan capacity directly into quota urgency. During the quota-lab design pass we separated three concerns:

1. eligibility/safety decides whether a candidate can participate;
2. quota urgency should mean the marginal pressure to use quota before it resets;
3. cache, warning, and WRH scalarization remain outside the quota pressure itself.

Operationally this ADR targets the cache-hit-0 or no-useful-cache path, but the implementation must not break cache-positive behavior. The existing WRH code multiplies quota, cache, and warning factors, so a formula that returns exact zero for on-pace candidates would also nullify cache affinity. The chosen implementation therefore separates the traceable pressure `U` from the WRH quota factor `1 + U`.

Recent local SQLite inspection of the user-service database confirmed that provider warning fields are external Anthropic signals and are persisted in subscription quota latest/checkpoint tables. They are not derived from the quota formula and remain policy-relevant. `surpassed_threshold` is rare, but `allowed_warning` appears often enough that warning demotion should stay.

## Decision

Replace the base-window quota pressure for stable base windows `5h` and `7d`, and the exact `claude-fable-5` model's `7d_fable` window, with:

```text
remaining_ratio_w = 1 - clamp(utilization_w, 0, 1)
time_ratio_w      = clamp((resets_at_w - now) / window_len_secs_w, 0, 1)
target_w          = max(time_ratio_w ^ gamma_w, floor_w)
U_w               = max(0, ln(remaining_ratio_w / target_w))
```

Hardcoded defaults:

```text
window_len_5h = 18_000 seconds
window_len_7d = 604_800 seconds
gamma_5h      = 1.0
gamma_7d      = 1.3
floor_5h      = 0.01
floor_7d      = 0.01
smoothmax_p   = 6.0
w7            = 1.0
```

`7d_fable` uses the same length, gamma, and floor as shared `7d`. `floor` is a near-reset cap on the denominator. It is not a warning threshold and not an early-start threshold. `gamma_7d = 1.3` makes weekly pressure rise earlier and smoother than the linear 5h target. A missing, stale, missing-utilization, missing-reset, or already-elapsed base window contributes `0` to the base pressure vector. For exact Fable, such a scoped observation remains `Unknown` for tier completeness, so missing data produces `PartialBase` rather than being treated as known-neutral.

For exact canonical `claude-fable-5`, first combine shared and scoped weekly pressure symmetrically, then combine 5h and effective weekly pressure:

```text
U_weekly = (U_7d^p + U_fable^p)^(1/p)
quota_pressure = (U_5h^p + (w7 * U_weekly)^p)^(1/p)
```

With `p = 6` and `w7 = 1`:

```text
U_weekly = (U_7d^6 + U_fable^6)^(1/6)
quota_pressure = (U_5h^6 + U_weekly^6)^(1/6)
```

All other models retain the existing two-window weighted smoothmax because `7d_fable` is not relevant to them:

```text
quota_pressure = (U_5h^p + (w7 * U_7d)^p)^(1/p)
```

With the hardcoded defaults:

```text
quota_pressure = (U_5h^6 + U_7d^6)^(1/6)
```

For `KnownBase` and `PartialBase` tier buckets, use the pressure in WRH through a neutral baseline:

```text
quota_weight_factor = if bucket_total_pressure < EPSILON {
    1.0
} else {
    1.0 + quota_pressure
}

effective_weight = quota_weight_factor * cache_weight_multiplier(cache_ratio) * warning_multiplier
score            = -ln(hash_to_open_unit(hash)) / effective_weight
winner           = argmin(score)
```

The `1 + quota_pressure` baseline is load-bearing. It keeps on-pace candidates selectable, lets cache-positive candidates still benefit from cache multiplier, and preserves the existing warning multiplier behavior during uniform fallback. Positive quota pressure still biases WRH toward upstreams whose quota is most likely to be wasted at reset.

`Overage` and `UnknownProbe` are not base-pressure tiers and do not use `1 + quota_pressure`:

```text
overage_bucket_total = sum(existing_overage_urgency_i)
overage_quota_weight_factor_i = if overage_bucket_total < EPSILON {
    1.0
} else {
    existing_overage_urgency_i
}

unknown_probe_quota_weight_factor = 1.0
```

The overage formula, its eligibility, and its uniform fallback therefore remain byte-for-byte behaviorally unchanged. `UnknownProbe` continues to have zero raw urgency and always reaches the existing uniform fallback. Warning-positive base candidates use the reset-aware multiplier defined by ADR 0010.

Remove the old base `capacity_multiplier` from quota urgency. Do not delete or reinterpret `plan_capacity_ratio`; it remains useful for admin analytics and future policy experiments. Overage tier urgency remains out of scope for this ADR and should keep its existing formula and fallback behavior unless a later ADR revisits overage routing.

Keep existing eligibility and safety behavior:

- Fresh `rejected`, fresh `disabled_reason`, and fresh finite `utilization >= 1.0` remain hard negatives.
- Fresh `allowed_warning`, or fresh `allowed` with finite `utilization >= surpassed_threshold`, remains warning-positive.
- Warning-positive candidates remain routable, start with `WARNING_MULTIPLIER = 0.20`, and may relax only through ADR 0010's bounded near-reset policy.
- Tier order remains `KnownBase > PartialBase > Overage > UnknownProbe`.
- Cache multiplier and cache-affinity key selection remain unchanged.

The existing non-Fable subscription-preference salt and trace version remain byte-identical `v11`. Exact canonical `claude-fable-5` uses a scoped salt containing the exact token `v11-fable` and reports `v11-fable` in `rendezvous_salt_version` for base, partial, and unknown-probe selection. Overage selection retains the frozen v10 salt for every model, including Fable.

## Trace and storage decision

The full all-candidate routing trace already lives in `RequestEvent.routing_trace` payload JSON:

```text
routing_trace.stages[*].subscription_preference.candidates[]
```

Extend each `CandidateUrgency` JSON entry with additive fields:

```text
quota_urgency_5h: Option<f64>
quota_urgency_7d: Option<f64>
quota_urgency_combined: Option<f64>
quota_weight_factor: f64
quota_uniform_fallback: bool
```

Keep the existing `urgency` field as the effective WRH weight and keep the existing `quota_urgency` field populated with the combined base pressure for v11 rows. The v11 salt and this ADR define the semantic era.

Field semantics are tier- and era-specific:

- For every v11 or v11-fable `KnownBase` or `PartialBase` candidate, `quota_urgency_5h`, `quota_urgency_7d`, and `quota_urgency_combined` are `Some(value)`. For v11-fable, `quota_urgency_7d` is effective weekly pressure, not raw shared-7d pressure. Missing, stale, malformed, or elapsed windows contribute zero; an unusable scoped Fable observation also remains unknown for tier completeness.
- For v11 `Overage` and `UnknownProbe` candidates, those three base-pressure fields are `None`. `quota_urgency` retains the existing raw overage urgency for `Overage` and is `0.0` for `UnknownProbe`.
- `quota_weight_factor` contains the tier-specific factor actually multiplied into WRH: `1 + combined` or `1.0` for base tiers, existing raw overage urgency or `1.0` for `Overage`, and `1.0` for `UnknownProbe`.
- `quota_uniform_fallback` reports whether that candidate's selected tier bucket used its uniform quota fallback.
- Historical pre-v11 JSON has none of the additive fields. Deserialization must default `quota_weight_factor` to `1.0`, `quota_uniform_fallback` to `false`, and the three `Option<f64>` fields to `None`; consumers must treat these defaults as compatibility placeholders and use `rendezvous_salt_version` to determine semantics.

Add nullable chosen-upstream columns to `request_events_v1` for aggregate queries:

```text
quota_urgency_5h
quota_urgency_7d
quota_urgency_combined
quota_weight_factor
quota_cache_multiplier
quota_warning_multiplier
quota_effective_weight
quota_uniform_fallback
```

These columns store only the selected/kept upstream's values. The payload JSON remains the source for every losing candidate's values. Existing rows are not backfilled.

Selection for these columns is by the terminally resolved/kept upstream ID. If that ID does not match a `CandidateUrgency` entry in the subscription-preference trace, all eight chosen-upstream columns remain `NULL`; values must never be copied from the formula winner or the first candidate as a fallback.

Add SQLite migration `0048_request_events_quota_urgency.sql` with nullable `REAL` columns and a nullable checked `INTEGER` (`0`/`1`) for `quota_uniform_fallback`. Add Postgres migration `0078_request_events_quota_urgency.sql` with nullable `DOUBLE PRECISION` columns and nullable `BOOLEAN`. Neither migration may set non-null defaults or execute an `UPDATE` backfill.

## Consequences

### Positive

- Quota pressure now answers the policy question: “will this quota be wasted if we do not use this upstream before reset?”
- 7d can dominate when weekly waste risk is the binding constraint; 5h is not hard-prioritized over 7d.
- Capacity no longer overrides quota urgency or creates a feedback loop where larger plans receive traffic merely because they are larger.
- Cache-positive and on-pace candidates remain selectable because WRH uses `1 + pressure`, not raw pressure.
- Warning signals stay independent from the formula and continue to protect provider-declared near-surpassed candidates.
- Chosen-upstream columns support cheap aggregate queries, while payload JSON keeps full candidate-level debugging.

### Negative

- Exact Fable routing distribution changes immediately on deployment because its formula and scoped salt change; non-Fable distribution does not move.
- `quota_urgency` historical rows are not directly comparable across v10 and v11 without checking `rendezvous_salt_version`.
- The formula is more complex than old burn pressure; tests must carry golden numeric examples for reviewability.
- `1 + pressure` means quota pressure is a bias on top of a neutral baseline, not an absolute gate. This is intentional, but operators must read `quota_urgency_combined` rather than `effective_weight` to understand raw quota pressure.

### Neutral

- Runtime config remains unchanged.
- Overage fallback remains unchanged.
- Existing subscription quota tables and columns, including `status` and `surpassed_threshold`, remain unchanged.
- Existing cache pricing and cache multiplier constants remain unchanged.

## Alternatives considered

- **Use raw `U` directly as `quota_weight`.** Rejected because on-pace candidates would get effective weight zero, which also nullifies cache multiplier and changes cache-positive semantics.
- **Use a tiny floor such as `max(U, EPSILON)`.** Rejected because it technically avoids infinity but still makes cache multiplier practically irrelevant when another candidate has positive pressure.
- **Keep old capacity multiplier.** Rejected because capacity belongs outside raw quota pressure and can override the use-it-or-lose-it policy.
- **Strict 5h-first with 7d as a tie-breaker.** Rejected because 7d can be the binding reset/waste constraint and should be able to dominate.
- **Raw sum of U5 and U7.** Rejected because a single request consumes both windows; hard summation double-counts more than a max-like smoothmax.
- **Hard max of U5 and U7.** Rejected because it discards secondary pressure when both windows are high.
- **Expose formula knobs immediately.** Rejected for first rollout to keep the production change small and testable.
- **Shadow only.** Rejected by operator decision; this ADR plans a direct replacement with local regression coverage.

## Verification

Implementation must include local regression tests for:

- Exact formula values for representative 5h and 7d scenarios.
- Exact Fable weekly and final pressure values, including smoothmax being greater than max but less than raw sum and degeneration to shared 7d when scoped pressure is zero.
- `gamma_7d = 1.3` producing earlier/smoother weekly pressure than gamma 1.
- Near-reset floor cap: denominator never falls below `0.01`.
- Missing/stale/elapsed base windows contributing zero pressure.
- Missing/stale/malformed/elapsed Fable observations contributing zero while preserving `PartialBase` completeness semantics.
- Capacity ratio changes not changing base quota pressure.
- Cache multiplier outputs unchanged for fixed cache inputs.
- Cache-hot on-pace candidates keeping finite effective weight through `1 + pressure`.
- All-on-pace buckets using uniform quota factor `1.0` while still applying warning multiplier.
- Warning-positive candidates retaining `WARNING_MULTIPLIER = 0.20` while on pace or below the warning relaxation threshold, with ADR 0010 covering near-reset relaxation.
- Two otherwise-equal Fable candidates receiving different pressure, effective weights, and routing distribution from scoped weekly pressure.
- All-candidate payload trace carrying U5/U7/combined/factor fields.
- Historical v10 payload JSON deserializing with the documented compatibility defaults.
- Winner and losing candidates carrying distinct per-candidate trace values.
- Overage-tier scoring remaining identical to v10, including non-uniform urgency differentiation and uniform fallback.
- Chosen-upstream request-event columns round-tripping through SQLite and Postgres adapters.
- Direct SQL assertions proving all eight dedicated columns are written independently of payload JSON, remain `NULL` for a trace/upstream mismatch, and stay `NULL` for pre-migration/historical rows.
- Exact Fable `v11-fable` salt/version and non-Fable fixed v11 winner/hash/trace invariance.
- Overage selection retaining the v10 salt for Fable and non-Fable models.

## Future work

- Expose formula constants as per-principal config after production behavior is understood.
- Add replay comparison tooling over historical SQLite request events if local regression tests are insufficient for future tuning.
- Revisit overage urgency with a separate ADR if overage routing starts depending on the same use-it-or-lose-it semantics.
- Consider admin UI charts over the new chosen-upstream trace columns after enough production data accumulates.
