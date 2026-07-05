# ADR 0003 — Weighted Rendezvous Hash for subscription-preference within-tier selection

- Status: Accepted
- Date: 2026-07-04
- Ships with: PR #312

## Context

The `subscription-preference` router filter (`crates/cc-lb-engine/src/builtin_filters/subscription_preference.rs`) classifies each candidate OAuth upstream into one of four tiers based on subscription quota state — `KnownBase`, `PartialBase`, `Overage`, `UnknownProbe` — then selects a single winner from within the highest non-empty tier.

Before this rewrite, the within-tier selection scored candidates using

```
intra_tier_score = min_headroom + positive_ratio - warning_penalty
```

and picked the highest score. A rendezvous-hash secondary tiebreak was documented as providing load spread, but `f64::total_cmp` on `f64` scores never ties in practice, so the secondary path never fired.

The `isac-opencode` principal has four healthy OAuth upstreams: `bear-max`
(20x), `isac-personal` (20x), `bh322yoo-max` (5x), `Runbear` (5x + team_tier_1
extra_usage). Observed traffic over 200 events showed `bear-max` receiving
78%, `Runbear` 23%, and the remaining two upstreams zero. `bear-max` won every
cache-miss selection deterministically because its 5h utilization (0.05) gave
it the highest `min_headroom`, and no other candidate ever broke the tie.

The concentration masks capacity, provides no load spread when several
candidates are close to full simultaneously (all four at ~90% utilization
should each receive ~25%, not 100/0/0/0), and cannot be tuned against the
capacity multiplier operators actually care about (a Max 20x plan carries
~4× the Sonnet weekly budget of a Max 5x plan).

Simulated `(1-u)^2 / remaining_secs` weighted rendezvous distribution on the
same live snapshot for Opus produced `Runbear` ≈ 55%, others 13–17%. Shadow
evaluation of the rewrite against 100 synthetic requests produced 10 / 38 /
26 / 26 % across the four upstreams, matching the theoretical distribution
within 2σ sampling noise. `bear-max` funneling dropped from 78% to 10%.

## Decision

Rewrite within-tier selection as Vilkonis-style Weighted Rendezvous Hash:

```
urgency_window(w) =
    capacity_multiplier(upstream) * (1 - clamp(util_w, 0, 1))^HEADROOM_EXPONENT
    / remaining_secs_w.max(MIN_REMAIN_SECS)
  when the window is fresh, positive, and resets_at is populated.
  Else the window contributes nothing.

urgency(candidate)          = max_w urgency_window(w)
capacity_multiplier(u)      = min(sqrt(plan_capacity_ratio(u)), CAPACITY_CAP)
weight_i                    = urgency(candidate_i)
u_i                         = fnv1a64_with_fmix64(SALT, request_id, upstream_id_i) ∈ (0, 1)
score_i                     = -ln(u_i) / weight_i
winner                      = argmin(score_i)
```

Constants (all committed in `subscription_preference.rs`):

- `HEADROOM_EXPONENT = 2`
- `CAPACITY_EXPONENT = 0.5` (square root)
- `CAPACITY_CAP = 2.0`
- `UNKNOWN_CAPACITY_RATIO = 1.0`
- `MIN_REMAIN_SECS = 60`
- `EPSILON = 1e-12`
- `OVERAGE_REMAINING_NOMINAL_SECS = 30 * 86_400`
- `OVERAGE_UNKNOWN_WEIGHT = 0.5`
- `RENDEZVOUS_SALT = "cc-lb:subscription-preference:v3:weighted-rendezvous:2026-07-04"`

Overage-tier candidates use a fixed `OVERAGE_REMAINING_NOMINAL_SECS` denominator
and skip the capacity multiplier — overage is a fallback, not a rate-limited
budget window. UnknownProbe candidates use uniform weight.

Base window set is model-aware: always `{5h, 7d}`, plus `7d_sonnet` when the
request model is Sonnet. `7d_opus` remains excluded because cc-lb outbound
requests currently omit the `x-anthropic-billing-header` (`cc_version` /
`cc_entrypoint`) tag that Anthropic requires before it will emit
`anthropic-ratelimit-unified-7d-opus-*` response headers. Header parser at
`crates/cc-lb-engine/src/rate_limit_headers.rs::parse_unified_header_name`
already recognizes the window; the gap is on the outbound side. See P2-9
verification report at
`.omo/plans/window-seven-day-opus-verification.md`.

## Consequences

### Positive

- Load spreads proportionally to `urgency = (1 - util)^2 / remaining_secs`.
  A 4-way tie at high utilization now distributes ~25% each, matching the
  operator's Q3 answer during the design debate.
- Capacity awareness (Max 20x vs Max 5x vs Pro) shifts up to 2× more traffic
  toward higher-tier plans, capped so that a single mega-plan cannot fully
  starve a lower-tier peer that still has headroom.
- Deterministic per-request-id selection preserves cache affinity from the
  upstream `cache-affinity` filter for retry idempotence.
- Fresh + short-remaining candidates gain priority via the `1 / remaining_secs`
  factor, honoring the "use-before-reset-waste" goal from the earlier
  drain-before-lockout rewrite (see commit `1193d9b9`).

### Negative

- Any change to the WRH constants or the salt shifts every request's
  selection. `RENDEZVOUS_SALT` embeds the algorithm version (`v3`) and the
  ship date so bumping the salt is an explicit choice, not an accident.
- The scheme cannot be reasoned about analytically for arbitrary quota
  states. Tests carry the numerical baseline for the four production
  upstreams (`live_snapshot_four_upstreams_produces_expected_wrh_distribution`
  in `subscription_preference/tests.rs`).
- Weight independence in the Vilkonis scheme requires strong avalanche in
  the hash. FNV-1a alone produced correlated hashes for UUIDs differing by
  only trailing bytes, breaking the weight relationship (a heavier candidate
  won 100% of picks). Fixed by chaining Murmur3 `fmix64` on the FNV-1a
  output. Regression is covered by
  `single_candidate_always_wins`,
  `same_request_id_yields_same_winner_across_calls`, and
  `different_request_ids_spread_across_candidates`.

## Operational invariants (review consensus)

- Tier order stays `KnownBase > PartialBase > Overage > UnknownProbe`. Tiers
  never mix. The chosen tier is surfaced via `SubscriptionPreferenceTrace`
  in `RoutingTrace` (see PR #324).
- `overage_in_use = true` always forces the Overage tier and never counts
  against base capacity. `extra_usage` remains an independent axis.
- `stale_rejected_without_reset_blocks = true` — a stale `rejected` snapshot
  without `resets_at` is treated as a hard-negative. This matches the
  operator's Q4 answer: exclude the window rather than fabricate a fallback.
- The capacity multiplier does NOT apply to Overage. Overage tier is the
  spillover pool, not a proportional-share pool.

## Alternatives considered

- **Keep score-based selection, actually break ties**. Would require sorting
  by rendezvous hash on numerically-equal scores. Preserves the pre-existing
  funneling bug because scores are almost never equal on real data.
- **Linear capacity multiplier (`u`) instead of `sqrt(u)`**. Rejected during
  Round-2 of the design debate: a 20x plan would receive 20× the traffic of
  a Pro plan even under moderate load, starving the Pro upstream even when
  it has plenty of headroom.
- **HHI (Herfindahl-Hirschman index) alarm instead of algorithm change**.
  The alarm can catch regression (see P1-5) but cannot itself redistribute
  traffic. Complementary, not substitute.
- **Consistent hashing ring instead of Vilkonis WRH**. Vilkonis fits our
  candidate count (2–8 typical, 20 worst-case) with lower implementation
  cost than a fully-populated hash ring.

## Verification

- `crates/cc-lb-engine/src/builtin_filters/subscription_preference/tests.rs`
  ships 35 tests covering tier gating, window classification, urgency
  numerics, capacity multiplier saturation, WRH edge cases (single
  candidate, zero total weight, same-request-id stability, different-
  request-id spread), and a live-snapshot regression test against the four
  production upstreams.
- Shadow evaluation via redirected `upstream_spec_v1.base_url` on a
  storage-DB clone with 100 synthetic requests reproduced the theoretical
  distribution to within 2σ.
- Post-merge audits `bg_f68592f3` (Q1-Q5 traceability) and `bg_f67127fc`
  (plan compliance) both returned covered/OK.

## Future work (out of scope for this ADR)

- Property-based tests over the WRH selection using `proptest` (task P3-10
  in `.omo/plans/post-deploy-followups.md`).
- Shadow-bench `HEADROOM_EXPONENT` at 2 vs 3 to see whether cubed headroom
  further improves distribution under high-load conditions (P3-11).
- Decision on whether to honor a `7d_opus` window once the collector side is
  verified fresh (P2-9).
- Prometheus tier-share alarm to detect regression back to funneling (P1-5,
  built on `cc_lb_routing_tier_selections_total` from PR #331).

## References

- Design debate: three rounds of parallel background agents recorded in
  session transcript, culminating in oracle synthesizer `bg_c0ed27d9`.
- Plan file: `.omo/plans/subscription-preference-rewrite.md` (gitignored).
- Ship PR: #312 (squash-merged as commit `81a95eeb`, 2026-07-04).
- Post-merge cleanup PR: #314 (aae62e37).
- Follow-up roadmap: `.omo/plans/post-deploy-followups.md` (gitignored).
