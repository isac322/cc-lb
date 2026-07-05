//! Tests for the tier + weighted-rendezvous-hash subscription-preference filter.
//!
//! Sections:
//!
//! - A — Gate / tier selection (kind, hard-negative, overage promotion)
//! - B — Base-window classification state machine (fresh vs stale, disabled)
//! - C — Reset semantics
//! - D — Overage / extra_usage assessment
//! - E — Model relevance (5h + 7d + 7d_sonnet, always excludes 7d_opus)
//! - F — Urgency numerics (headroom exponent, remaining_secs, max over windows)
//! - G — Capacity multiplier (Pro / team_standard / cap saturation / overage)
//! - H — WRH selection: uniform fallback, single candidate, determinism, spread
//! - I — Anti-stampede distribution
//! - J — Live-snapshot regression (four-upstream production fixture)

use bytes::Bytes;
use cc_lb_plugin_api::{PrincipalKind, SubscriptionQuotaDataState};
use http::Method;
use std::collections::HashMap;

use super::*;

const SONNET_MODEL: &str = "claude-sonnet-4-5-20250929";
const OPUS_MODEL: &str = "claude-opus-4-8-20250514";
const HAIKU_MODEL: &str = "claude-haiku-4-5-20251001";
const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";

const T0_SECS: u64 = 1_700_000_000;

// =============================================================================
// Section A — Gate / tier selection
// =============================================================================

#[test]
fn empty_candidate_list_returns_empty() {
    let output = filter_for_model(&[], MODEL_AGNOSTIC);
    assert!(output.kept_upstream_ids.is_empty());
    assert_eq!(output.reason, NO_SUBSCRIPTION_REASON);
}

#[test]
fn only_oauth_all_healthy_picks_from_known_base() {
    let a = healthy_oauth("a", 1);
    let b = healthy_oauth("b", 2);
    let c = healthy_oauth("c", 3);
    let output = filter_for_model(&[a.clone(), b.clone(), c.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids.len(), 1);
    let winner = output.kept_upstream_ids[0];
    assert!(
        winner == a.upstream_id || winner == b.upstream_id || winner == c.upstream_id,
        "winner must come from KnownBase candidates"
    );
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn mixed_kinds_all_oauth_healthy_drops_api_key() {
    let oauth = healthy_oauth("oauth", 1);
    let key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), key], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn all_oauth_hard_negative_picks_api_key() {
    let dead_a = oauth_with(
        "dead-a",
        1,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let dead_b = oauth_with(
        "dead-b",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
        ],
    );
    let key = api_key("api-key", 3);
    let output = filter_for_model(&[dead_a, dead_b, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn all_oauth_hard_negative_no_api_key_fails_open_all_oauth() {
    let dead_a = oauth_with(
        "dead-a",
        1,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let dead_b = oauth_with(
        "dead-b",
        2,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let output = filter_for_model(&[dead_a.clone(), dead_b.clone()], MODEL_AGNOSTIC);
    assert_eq!(
        output.kept_upstream_ids,
        vec![dead_a.upstream_id, dead_b.upstream_id]
    );
    assert_eq!(output.reason, NO_API_KEY_REASON);
}

#[test]
fn base_blocked_overage_ok_picks_overage() {
    let overage_only = oauth_with(
        "overage-only",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );
    let output = filter_for_model(std::slice::from_ref(&overage_only), MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![overage_only.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn base_blocked_overage_blocked_drops_candidate() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn overage_in_use_forces_overage_tier() {
    // Base looks healthy on both windows, but unified reports
    // overage_in_use=true — the Anthropic-side truth is the plan is spent
    // and overage is servicing. A separately-KnownBase candidate must win.
    let ov_in_use = oauth_with(
        "ov-in-use",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.1).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.1).status("allowed").build(),
            fresh(WINDOW_UNIFIED).overage_in_use(true).build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );
    let healthy = healthy_oauth("healthy", 2);
    let output = filter_for_model(&[ov_in_use, healthy.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![healthy.upstream_id]);
}

// =============================================================================
// Section B — Base-window classification state machine
// =============================================================================

#[test]
fn fresh_disabled_reason_is_hard_negative() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.1)
                .status("allowed")
                .disabled("provider disabled")
                .build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

// =============================================================================
// Section C — Reset semantics
// =============================================================================

#[test]
fn stale_rejected_resets_in_future_is_hard_negative() {
    let reset_secs = T0_SECS + 3 * 24 * 3_600;
    let dead = oauth_at_t0(
        "dead",
        1,
        vec![
            stale(WINDOW_SEVEN_DAY)
                .util(1.0)
                .status("rejected")
                .reset_at(reset_secs)
                .build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn stale_rejected_resets_in_past_is_unknown() {
    // Candidate observed 10 days after T0. Reset was 5 days after T0 → past.
    let observed_secs = T0_SECS + 10 * 24 * 3_600;
    let reset_secs = T0_SECS + 5 * 24 * 3_600;
    let alive = UpstreamCandidate {
        observed_at_unix_secs: observed_secs,
        ..oauth_at_t0(
            "alive",
            1,
            vec![
                stale(WINDOW_SEVEN_DAY)
                    .util(1.0)
                    .status("rejected")
                    .reset_at(reset_secs)
                    .build(),
            ],
        )
    };
    let alive_id = alive.upstream_id;
    let key = api_key("k", 2);
    let output = filter_for_model(&[alive, key], MODEL_AGNOSTIC);
    // Base is Unknown → UnknownProbe tier, still selected over API-key.
    assert_eq!(output.kept_upstream_ids, vec![alive_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn stale_rejected_no_resets_at_is_hard_negative() {
    let dead = oauth_at_t0(
        "dead",
        1,
        vec![stale(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

// =============================================================================
// Section D — Overage / extra_usage assessment
// =============================================================================

#[test]
fn extra_usage_enabled_with_remaining_credits_positive_overage_signal() {
    let has_credits = oauth_with(
        "has-credits",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_UNIFIED)
                .extra_usage_enabled(true)
                .extra_usage_limit(100.0)
                .extra_usage_used(20.0)
                .build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[has_credits.clone(), key], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![has_credits.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn extra_usage_disabled_hard_block_overage() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.2).status("allowed").build(),
            fresh(WINDOW_UNIFIED).extra_usage_enabled(false).build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn extra_usage_exhausted_hard_block_overage() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.2).status("allowed").build(),
            fresh(WINDOW_UNIFIED)
                .extra_usage_enabled(true)
                .extra_usage_limit(50.0)
                .extra_usage_used(50.0)
                .build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn fallback_available_false_hard_block_overage() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.2).status("allowed").build(),
            fresh(WINDOW_UNIFIED).fallback_available(false).build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn overage_util_ge_1_fresh_hard_block_overage() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("allowed").build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn extra_usage_stale_ignored_no_block() {
    // Stale overage snapshot with `rejected` status is NOT a fresh block,
    // and the unified snapshot marks fallback_available=true → overage tier
    // stays usable.
    let overage_alive = oauth_with(
        "overage-alive",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            stale(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
            fresh(WINDOW_UNIFIED).fallback_available(true).build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[overage_alive.clone(), key], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![overage_alive.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

// =============================================================================
// Section E — Model relevance
// =============================================================================

#[test]
fn sonnet_request_includes_7d_sonnet_window() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(1.0)
                .status("rejected")
                .build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], SONNET_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn non_sonnet_request_excludes_7d_sonnet_window() {
    let opus_alive = oauth_with(
        "opus",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(1.0)
                .status("rejected")
                .build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[opus_alive.clone(), key], OPUS_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![opus_alive.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn always_ignores_7d_opus_window() {
    for model in [SONNET_MODEL, OPUS_MODEL, HAIKU_MODEL, MODEL_AGNOSTIC] {
        let alive = oauth_with(
            "alive",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY_OPUS)
                    .util(1.0)
                    .status("rejected")
                    .build(),
            ],
        );
        let key = api_key("k", 2);
        let output = filter_for_model(&[alive.clone(), key], model);
        assert_eq!(
            output.kept_upstream_ids,
            vec![alive.upstream_id],
            "model={model} must ignore 7d_opus"
        );
    }
}

// =============================================================================
// Section F — Urgency numerics
// =============================================================================

#[test]
fn runbear_30min_headroom_beats_bearmax_4h_headroom() {
    // Q1: shorter remaining beats longer remaining when headroom is similar.
    // Runbear: util=0.22 remain=30min (1800s) → urgency ∝ (0.78)^2 / 1800 ≈ 3.38e-4.
    // bear:    util=0.05 remain=4h  (14400s) → urgency ∝ (0.95)^2 / 14400 ≈ 6.27e-5.
    // Runbear wins > 80% of the time under WRH.
    let runbear = oauth_at_t0(
        "runbear",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.22)
                .status("allowed")
                .reset_at(T0_SECS + 1800)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.16)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    );
    let bear = oauth_at_t0(
        "bear",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.05)
                .status("allowed")
                .reset_at(T0_SECS + 14_400)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.18)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    );
    let dist = wrh_distribution(&[runbear.clone(), bear.clone()], MODEL_AGNOSTIC, 2000);
    let runbear_share = *dist.get(&runbear.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    assert!(
        runbear_share > 0.75,
        "runbear should win most requests, got share={runbear_share}"
    );
}

#[test]
fn window_urgency_aggregation_uses_max() {
    // Q2: candidate has 5h with tight urgency (util 0.5, remain 60s) plus
    // 7d with essentially zero urgency (util 0.5, remain 1 year). The 5h
    // number must dominate — a competing candidate with only the loose
    // 7d must lose overwhelmingly.
    let tight_5h = oauth_at_t0(
        "tight-5h",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 60)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 365 * 86_400)
                .build(),
        ],
    );
    let loose_only = oauth_at_t0(
        "loose",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 365 * 86_400)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 365 * 86_400)
                .build(),
        ],
    );
    let dist = wrh_distribution(
        &[tight_5h.clone(), loose_only.clone()],
        MODEL_AGNOSTIC,
        1000,
    );
    let tight_share = *dist.get(&tight_5h.upstream_id).unwrap_or(&0) as f64 / 1000.0;
    assert!(
        tight_share > 0.99,
        "tight 5h must dominate via max-over-windows, got share={tight_share}"
    );
}

#[test]
fn resets_at_missing_window_excluded_from_urgency() {
    // Q4: two identical candidates except one has resets_at populated on 5h
    // and the other doesn't. Both windows still classify as
    // CurrentPositive (fresh + allowed), so both stay in KnownBase. The
    // one without resets_at contributes 0 urgency → fallback path
    // makes them equal only through the uniform floor; but the with-reset
    // candidate has a strictly positive urgency, so it wins the WRH.
    let with_reset = oauth_at_t0(
        "with-reset",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 3600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    );
    let no_reset = oauth_at_t0(
        "no-reset",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.5).status("allowed").build(),
        ],
    );
    let dist = wrh_distribution(
        &[with_reset.clone(), no_reset.clone()],
        MODEL_AGNOSTIC,
        1000,
    );
    let with_reset_share = *dist.get(&with_reset.upstream_id).unwrap_or(&0) as f64 / 1000.0;
    assert!(
        with_reset_share > 0.99,
        "with-reset should win — no-reset urgency is zero, so with-reset dominates WRH; got share={with_reset_share}"
    );
}

#[test]
fn high_util_short_remaining_loses_to_low_util_long_remaining() {
    // Q5: A (util 0.10 remain 4h) vs B (util 0.90 remain 5min).
    // Urgency A ∝ (0.9)^2 / 14400 ≈ 5.63e-5.
    // Urgency B ∝ (0.1)^2 / 300 ≈ 3.33e-5.
    // A should win the majority of picks; (1-u)^2 collapses B's contribution.
    let low_util_long = oauth_at_t0(
        "low-util-long",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.1)
                .status("allowed")
                .reset_at(T0_SECS + 14_400)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.1)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    );
    let high_util_short = oauth_at_t0(
        "high-util-short",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.9)
                .status("allowed")
                .reset_at(T0_SECS + 300)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.9)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    );
    let dist = wrh_distribution(
        &[low_util_long.clone(), high_util_short.clone()],
        MODEL_AGNOSTIC,
        2000,
    );
    let low_share = *dist.get(&low_util_long.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    assert!(
        low_share > 0.55,
        "low-util long remaining must beat high-util short remaining; got share={low_share}"
    );
}

// =============================================================================
// Section G — Capacity multiplier
// =============================================================================

#[test]
fn pro_plan_ratio_gives_capacity_multiplier_1() {
    // Pro (ratio=1.0) → multiplier=1.0. Compare against a candidate whose
    // ratio is None (defaults to UNKNOWN_CAPACITY_RATIO=1.0). Under
    // identical util/remain, they should tie → uniform pick ~50/50.
    let pro = with_plan(
        oauth_at_t0(
            "pro",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 3600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 6 * 86_400)
                    .build(),
            ],
        ),
        1.0,
    );
    let unknown = oauth_at_t0(
        "unknown",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 3600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.5)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    );
    let dist = wrh_distribution(&[pro.clone(), unknown.clone()], MODEL_AGNOSTIC, 2000);
    let pro_share = *dist.get(&pro.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    assert!(
        (0.40..=0.60).contains(&pro_share),
        "pro (ratio=1.0) and unknown (ratio defaulting to 1.0) should split evenly; got pro share={pro_share}"
    );
}

#[test]
fn team_standard_between_pro_and_saturated() {
    // team_standard ratio=1.25 → multiplier=sqrt(1.25)≈1.118.
    // vs pro (ratio=1.0 → mult=1.0). At identical util/remaining,
    // team wins ~52.8% (1.118 / (1.118 + 1.0)).
    let pro = with_plan(
        oauth_at_t0(
            "pro",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 3600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 6 * 86_400)
                    .build(),
            ],
        ),
        1.0,
    );
    let team = with_plan(
        oauth_at_t0(
            "team",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 3600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 6 * 86_400)
                    .build(),
            ],
        ),
        1.25,
    );
    let dist = wrh_distribution(&[pro.clone(), team.clone()], MODEL_AGNOSTIC, 2000);
    let team_share = *dist.get(&team.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    assert!(
        (0.47..=0.60).contains(&team_share),
        "team_standard (~52.8% expected) should sit between pro and saturated; got team share={team_share}"
    );
}

#[test]
fn equal_util_equal_remaining_unequal_plan_saturates_capacity_cap() {
    // Ratio=5 and ratio=20 both saturate the sqrt-then-cap at 2.0.
    // Their urgencies are numerically identical → distribution is ~50/50.
    let five_x = with_plan(
        oauth_at_t0(
            "5x",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.3)
                    .status("allowed")
                    .reset_at(T0_SECS + 3600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.3)
                    .status("allowed")
                    .reset_at(T0_SECS + 6 * 86_400)
                    .build(),
            ],
        ),
        5.0,
    );
    let twenty_x = with_plan(
        oauth_at_t0(
            "20x",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.3)
                    .status("allowed")
                    .reset_at(T0_SECS + 3600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.3)
                    .status("allowed")
                    .reset_at(T0_SECS + 6 * 86_400)
                    .build(),
            ],
        ),
        20.0,
    );
    let dist = wrh_distribution(&[five_x.clone(), twenty_x.clone()], MODEL_AGNOSTIC, 2000);
    let five_share = *dist.get(&five_x.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    assert!(
        (0.42..=0.58).contains(&five_share),
        "5x and 20x should tie at capacity cap; got 5x share={five_share}"
    );
}

#[test]
fn overage_tier_does_not_apply_capacity_multiplier() {
    // Two overage-only candidates with identical overage util but very
    // different plan_capacity_ratio. Since overage-tier urgency does NOT
    // include the multiplier, they must split ~50/50.
    let small = with_plan(
        oauth_at_t0(
            "small",
            1,
            vec![
                fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
                fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
            ],
        ),
        1.0,
    );
    let big = with_plan(
        oauth_at_t0(
            "big",
            2,
            vec![
                fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
                fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
            ],
        ),
        20.0,
    );
    let dist = wrh_distribution(&[small.clone(), big.clone()], MODEL_AGNOSTIC, 2000);
    let small_share = *dist.get(&small.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    assert!(
        (0.42..=0.58).contains(&small_share),
        "overage tier ignores capacity multiplier; got small share={small_share}"
    );
}

// =============================================================================
// Section H — WRH selection: uniform fallback, single, determinism, spread
// =============================================================================

#[test]
fn zero_total_weight_falls_back_to_uniform() {
    // Every candidate has a base-positive window but no resets_at, so
    // urgency=0 across the board. WRH must fall back to uniform and
    // spread traffic evenly (~25% per candidate over enough samples).
    let mk = |name: &str, seed: u8| {
        oauth_at_t0(
            name,
            seed,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.5).status("allowed").build(),
            ],
        )
    };
    let a = mk("a", 1);
    let b = mk("b", 2);
    let c = mk("c", 3);
    let d = mk("d", 4);
    let dist = wrh_distribution(
        &[a.clone(), b.clone(), c.clone(), d.clone()],
        MODEL_AGNOSTIC,
        4000,
    );
    for candidate in [&a, &b, &c, &d] {
        let share = *dist.get(&candidate.upstream_id).unwrap_or(&0) as f64 / 4000.0;
        assert!(
            (0.20..=0.30).contains(&share),
            "uniform fallback should split evenly; {} share={share}",
            candidate.name
        );
    }
}

#[test]
fn single_candidate_always_wins() {
    let only = healthy_oauth("only", 1);
    for request_id in 0..50 {
        let output = SubscriptionPreferenceFilter::new()
            .filter(
                &ctx_with_request_id(MODEL_AGNOSTIC, &format!("req-{request_id}")),
                &principal(),
                std::slice::from_ref(&only),
            )
            .unwrap();
        assert_eq!(output.kept_upstream_ids, vec![only.upstream_id]);
    }
}

#[test]
fn same_request_id_yields_same_winner_across_calls() {
    let a = healthy_oauth("a", 1);
    let b = healthy_oauth("b", 2);
    let c = healthy_oauth("c", 3);
    let winners: Vec<Uuid> = (0..10)
        .map(|_| {
            let out = SubscriptionPreferenceFilter::new()
                .filter(
                    &ctx_with_request_id(MODEL_AGNOSTIC, "req-stable"),
                    &principal(),
                    &[a.clone(), b.clone(), c.clone()],
                )
                .unwrap();
            out.kept_upstream_ids[0]
        })
        .collect();
    let first = winners[0];
    for w in &winners[1..] {
        assert_eq!(*w, first, "same request_id must produce same winner");
    }
}

#[test]
fn different_request_ids_spread_across_candidates() {
    // 3 identical KnownBase candidates + no resets_at → urgency all zero →
    // uniform → distribution shows all three getting picked as request_id varies.
    let a = oauth_at_t0(
        "a",
        1,
        vec![fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build()],
    );
    let b = oauth_at_t0(
        "b",
        2,
        vec![fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build()],
    );
    let c = oauth_at_t0(
        "c",
        3,
        vec![fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build()],
    );
    let dist = wrh_distribution(&[a.clone(), b.clone(), c.clone()], MODEL_AGNOSTIC, 1500);
    for candidate in [&a, &b, &c] {
        let share = *dist.get(&candidate.upstream_id).unwrap_or(&0) as f64 / 1500.0;
        assert!(
            (0.25..=0.42).contains(&share),
            "candidate {} did not receive a fair share of picks: {share}",
            candidate.name
        );
    }
}

// =============================================================================
// Section I — Anti-stampede distribution (Q3)
// =============================================================================

#[test]
fn four_identical_candidates_distribute_uniformly_across_many_requests() {
    // Q3: four candidates at util≈0.9 remain≈30min → uniform-ish weights →
    // distribution should be within a tight band around 25% per candidate.
    let mk = |name: &str, seed: u8| {
        oauth_at_t0(
            name,
            seed,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.9)
                    .status("allowed")
                    .reset_at(T0_SECS + 1800)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.5)
                    .status("allowed")
                    .reset_at(T0_SECS + 6 * 86_400)
                    .build(),
            ],
        )
    };
    let candidates = vec![mk("a", 1), mk("b", 2), mk("c", 3), mk("d", 4)];
    let ids: Vec<Uuid> = candidates.iter().map(|c| c.upstream_id).collect();
    let dist = wrh_distribution(&candidates, MODEL_AGNOSTIC, 4000);
    for id in ids {
        let share = *dist.get(&id).unwrap_or(&0) as f64 / 4000.0;
        assert!(
            (0.20..=0.30).contains(&share),
            "identical candidates must split ~evenly under Q3; got share={share}"
        );
    }
}

// =============================================================================
// Section J — Live-snapshot regression (four-upstream production fixture)
// =============================================================================
//
// Baked from /tmp/quota-latest.json captured on 2026-07-04. The current-code
// algorithm funnelled 78% of traffic to bear-max. The rewritten algorithm
// must produce the (much fairer) weighted-rendezvous distribution:
// Runbear ≈ 55%, isac-personal ≈ 16%, bh322yoo-max ≈ 15%, bear-max ≈ 13%.

fn live_now_secs() -> u64 {
    1_783_166_568
}

fn live_snapshot_bear_max() -> UpstreamCandidate {
    let now = live_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "bear-max",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.05)
                    .status("allowed")
                    .reset_at(1_783_180_199)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.18)
                    .status("allowed")
                    .reset_at(1_783_623_599)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 20.0)
}

fn live_snapshot_isac_personal() -> UpstreamCandidate {
    let now = live_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "isac-personal",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.13)
                    .status("allowed")
                    .reset_at(1_783_176_000)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.37)
                    .status("allowed")
                    .reset_at(1_783_612_800)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 20.0)
}

fn live_snapshot_bh322yoo_max() -> UpstreamCandidate {
    let now = live_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "bh322yoo-max",
            3,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.0)
                    .status("allowed")
                    .reset_at(1_783_180_199)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.23)
                    .status("allowed")
                    .reset_at(1_783_223_999)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 5.0)
}

fn live_snapshot_runbear() -> UpstreamCandidate {
    let now = live_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "Runbear",
            4,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.22)
                    .status("allowed")
                    .reset_at(1_783_168_799)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.16)
                    .status("allowed")
                    .reset_at(1_783_522_799)
                    .build(),
                fresh(WINDOW_OVERAGE)
                    .util(0.1517)
                    .status("allowed")
                    .extra_usage_enabled(true)
                    .extra_usage_limit(30_000.0)
                    .extra_usage_used(4551.0)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 5.0)
}

#[test]
fn live_snapshot_four_upstreams_produces_expected_wrh_distribution() {
    let candidates = vec![
        live_snapshot_bear_max(),
        live_snapshot_isac_personal(),
        live_snapshot_bh322yoo_max(),
        live_snapshot_runbear(),
    ];
    let ids: HashMap<&str, Uuid> = candidates
        .iter()
        .map(|c| (c.name.as_str(), c.upstream_id))
        .collect();
    let bear_id = ids["bear-max"];
    let isac_id = ids["isac-personal"];
    let bh322_id = ids["bh322yoo-max"];
    let runbear_id = ids["Runbear"];

    let dist = wrh_distribution(&candidates, MODEL_AGNOSTIC, 2000);
    let share = |id: Uuid| *dist.get(&id).unwrap_or(&0) as f64 / 2000.0;

    // Expected shares (see fixture): Runbear ~55.4%, isac ~16.3%, bh322 ~14.9%, bear ~13.4%.
    let runbear_share = share(runbear_id);
    let isac_share = share(isac_id);
    let bh322_share = share(bh322_id);
    let bear_share = share(bear_id);

    assert!(
        (0.48..=0.63).contains(&runbear_share),
        "runbear share {runbear_share} outside expected 48-63%"
    );
    assert!(
        (0.10..=0.22).contains(&isac_share),
        "isac share {isac_share} outside expected 10-22%"
    );
    assert!(
        (0.09..=0.21).contains(&bh322_share),
        "bh322 share {bh322_share} outside expected 9-21%"
    );
    assert!(
        (0.08..=0.20).contains(&bear_share),
        "bear share {bear_share} outside expected 8-20%"
    );
    // Sanity: total sums to 1 (no dropped candidates).
    let total = runbear_share + isac_share + bh322_share + bear_share;
    assert!(
        (0.995..=1.005).contains(&total),
        "shares must sum to 1, got {total}"
    );
}

// =============================================================================
// Section K — SubscriptionPreferenceTrace attachment
// =============================================================================

#[test]
fn known_base_win_attaches_trace_with_chosen_tier() {
    let a = healthy_oauth("a", 1);
    let b = healthy_oauth("b", 2);
    let output = filter_for_model(&[a.clone(), b.clone()], MODEL_AGNOSTIC);

    let trace = output
        .subscription_preference
        .expect("KnownBase win must attach subscription_preference trace");
    assert_eq!(
        trace.chosen_tier,
        cc_lb_plugin_api::SubscriptionTier::KnownBase
    );
    assert_eq!(trace.candidates.len(), 2);
    for candidate in &trace.candidates {
        assert_eq!(
            candidate.tier,
            cc_lb_plugin_api::SubscriptionTier::KnownBase
        );
        assert!(
            candidate.urgency > 0.0,
            "KnownBase candidate urgency must be positive, got {}",
            candidate.urgency
        );
    }
}

#[test]
fn no_subscription_returns_no_trace() {
    let key = api_key("only-api-key", 1);
    let output = filter_for_model(&[key], MODEL_AGNOSTIC);
    assert!(
        output.subscription_preference.is_none(),
        "no-subscription path must not attach a trace"
    );
}

#[test]
fn all_oauth_hard_negative_no_api_key_fails_open_without_trace() {
    let a = oauth_with(
        "dead-a",
        1,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let b = oauth_with(
        "dead-b",
        2,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let output = filter_for_model(&[a, b], MODEL_AGNOSTIC);

    assert_eq!(output.reason, NO_API_KEY_REASON);
    assert!(
        output.subscription_preference.is_none(),
        "fail-open path must not attach a trace"
    );
}

// =============================================================================
// Helpers
// =============================================================================

fn filter_for_model(candidates: &[UpstreamCandidate], canonical_model: &str) -> FilterOutput {
    SubscriptionPreferenceFilter::new()
        .filter(&ctx(canonical_model), &principal(), candidates)
        .expect("builtin filter cannot fail")
}

/// Run the filter across a fixed sequence of synthetic request IDs and
/// return a histogram of picked upstream IDs.
fn wrh_distribution(
    candidates: &[UpstreamCandidate],
    canonical_model: &str,
    trials: usize,
) -> HashMap<Uuid, usize> {
    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let mut counts: HashMap<Uuid, usize> = HashMap::new();
    for i in 0..trials {
        let ctx = ctx_with_request_id(canonical_model, &format!("req-{i}"));
        let output = filter.filter(&ctx, &principal, candidates).unwrap();
        if let Some(&winner) = output.kept_upstream_ids.first() {
            *counts.entry(winner).or_insert(0) += 1;
        }
    }
    counts
}

fn ctx(canonical_model: &str) -> RequestContext {
    ctx_with_request_id(canonical_model, "req")
}

fn ctx_with_request_id(canonical_model: &str, request_id: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: canonical_model.to_owned(),
    }
}

fn principal() -> Principal {
    Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    }
}

fn upstream_id(seed: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = seed;
    Uuid::from_bytes(bytes)
}

fn oauth_with(
    name: &str,
    id_seed: u8,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: upstream_id(id_seed),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: quotas,
        observed_at_unix_secs: 0,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }
}

fn oauth_at_t0(
    name: &str,
    id_seed: u8,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        observed_at_unix_secs: T0_SECS,
        ..oauth_with(name, id_seed, quotas)
    }
}

fn healthy_oauth(name: &str, id_seed: u8) -> UpstreamCandidate {
    oauth_at_t0(
        name,
        id_seed,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .status("allowed")
                .reset_at(T0_SECS + 3600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .status("allowed")
                .reset_at(T0_SECS + 6 * 86_400)
                .build(),
        ],
    )
}

fn api_key(name: &str, id_seed: u8) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: upstream_id(id_seed),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 0,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }
}

fn with_plan(mut candidate: UpstreamCandidate, ratio: f64) -> UpstreamCandidate {
    candidate.plan_capacity_ratio = Some(ratio);
    candidate
}

#[derive(Clone, Debug)]
struct SnapBuilder {
    inner: SubscriptionQuotaCandidateSnapshot,
}

impl SnapBuilder {
    fn build(self) -> SubscriptionQuotaCandidateSnapshot {
        self.inner
    }

    fn util(mut self, value: f64) -> Self {
        self.inner.utilization = Some(value);
        self
    }

    fn status(mut self, value: &str) -> Self {
        self.inner.status = Some(value.to_owned());
        self
    }

    fn disabled(mut self, reason: &str) -> Self {
        self.inner.disabled_reason = Some(reason.to_owned());
        self
    }

    fn reset_at(mut self, t: u64) -> Self {
        self.inner.resets_at_unix_secs = Some(t);
        self
    }

    fn overage_in_use(mut self, value: bool) -> Self {
        self.inner.overage_in_use = Some(value);
        self
    }

    fn fallback_available(mut self, value: bool) -> Self {
        self.inner.fallback_available = Some(value);
        self
    }

    fn extra_usage_enabled(mut self, value: bool) -> Self {
        self.inner.extra_usage_enabled = Some(value);
        self
    }

    fn extra_usage_limit(mut self, value: f64) -> Self {
        self.inner.extra_usage_monthly_limit = Some(value);
        self
    }

    fn extra_usage_used(mut self, value: f64) -> Self {
        self.inner.extra_usage_used_credits = Some(value);
        self
    }
}

fn fresh(window: &str) -> SnapBuilder {
    SnapBuilder {
        inner: blank_snapshot(window, SubscriptionQuotaDataState::Fresh),
    }
}

fn stale(window: &str) -> SnapBuilder {
    SnapBuilder {
        inner: blank_snapshot(window, SubscriptionQuotaDataState::Stale),
    }
}

fn blank_snapshot(
    window: &str,
    state: SubscriptionQuotaDataState,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state,
        source: Some("header".to_owned()),
        utilization: None,
        status: None,
        resets_at_unix_secs: None,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: None,
        max_staleness_secs: 60,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}
