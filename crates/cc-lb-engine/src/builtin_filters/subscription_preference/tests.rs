//! Tests for the tier + weighted-rendezvous-hash subscription-preference filter.
//!
//! Sections:
//!
//! - A — Gate / tier selection (kind, hard-negative, overage promotion)
//! - B — Base-window classification state machine (fresh vs stale, disabled)
//! - C — Reset semantics
//! - D — Overage / extra_usage assessment
//! - E — Model relevance (5h + 7d, excludes unstable model-specific windows)
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
const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";

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
fn sonnet_request_excludes_7d_sonnet_window() {
    let sonnet_alive = oauth_with(
        "sonnet",
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
    let output = filter_for_model(&[sonnet_alive.clone(), key], SONNET_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![sonnet_alive.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
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
// Session-affinity regressions (guard the v8 policy that keys WRH on non-empty
// thread_id with request_id fallback; see ADR 0005).
// =============================================================================

#[test]
fn same_thread_id_without_cache_uses_request_id_and_spreads_when_request_ids_vary() {
    // Given: four healthy live-snapshot upstreams and one non-empty thread_id
    // shared across 2000 turns with distinct request_ids.
    // When: none of the candidates has a priced live-cache value.
    // Then: same-session identity alone must not pin all-cold routing; the WRH
    // key falls back to request_id and spreads across the healthy pool.
    let candidates = vec![
        live_snapshot_bear_max(),
        live_snapshot_isac_personal(),
        live_snapshot_bh322yoo_max(),
        live_snapshot_runbear(),
    ];
    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let thread_id = "thread-prod-cache-redacted";
    let mut winners: HashMap<Uuid, usize> = HashMap::new();
    for i in 0..2000 {
        let ctx = ctx_with_thread_id(MODEL_AGNOSTIC, &format!("req-{i}"), thread_id);
        let output = filter.filter(&ctx, &principal, &candidates).unwrap();
        let trace = output
            .subscription_preference
            .as_ref()
            .expect("subscription-alive path must emit trace");
        assert_eq!(
            trace.wrh_key_source,
            WrhKeySource::RequestId,
            "all-cold subscription routing must ignore thread_id as a pinning key"
        );
        *winners
            .entry(*output.kept_upstream_ids.first().unwrap())
            .or_insert(0) += 1;
    }
    assert!(
        winners.len() >= 3,
        "same-thread all-cold turns must spread across request_id WRH; landed on {} upstreams",
        winners.len()
    );
}

#[test]
fn different_request_ids_spread_across_upstreams() {
    // Given: same four healthy candidates as the golden distribution test.
    // When: each trial carries a distinct request_id (thread_id absent),
    //   so v8 WRH falls back to a fresh independent per-request key.
    // Then: the aggregate distribution must cover multiple upstreams —
    //   proving pure-request-id keying keeps load spread across the pool.
    let candidates = vec![
        live_snapshot_bear_max(),
        live_snapshot_isac_personal(),
        live_snapshot_bh322yoo_max(),
        live_snapshot_runbear(),
    ];
    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let mut winners: HashMap<Uuid, usize> = HashMap::new();
    for i in 0..2000 {
        let ctx = ctx_with_request_id(MODEL_AGNOSTIC, &format!("req-{i}"));
        let output = filter.filter(&ctx, &principal, &candidates).unwrap();
        *winners
            .entry(*output.kept_upstream_ids.first().unwrap())
            .or_insert(0) += 1;
    }
    assert!(
        winners.len() >= 3,
        "expected request_id fallback WRH to spread across ≥3 upstreams over 2000 distinct request_ids; landed on {} upstreams",
        winners.len()
    );
    let min_share = winners.values().copied().min().unwrap_or(0) as f64 / 2000.0;
    assert!(
        min_share >= 0.03,
        "no upstream should be starved across per-request draws; min share was {min_share}",
    );
}

#[test]
fn missing_thread_id_falls_back_to_request_id() {
    // Given: two healthy oauth upstreams, and a ctx where thread_id is
    //   unset (mimicking a stateless / warmup / non-opencode client).
    // When: the same ctx (same request_id, no thread_id) is filtered twice.
    // Then: the winner must be deterministic (fallback path is stable),
    //   proving that removing the thread_id header does not regress the
    //   old behaviour that the previous tests already cover.
    let candidates = vec![healthy_oauth("a", 1), healthy_oauth("b", 2)];
    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-stateless");
    let w1 = *filter
        .filter(&ctx, &principal, &candidates)
        .unwrap()
        .kept_upstream_ids
        .first()
        .unwrap();
    let w2 = *filter
        .filter(&ctx, &principal, &candidates)
        .unwrap()
        .kept_upstream_ids
        .first()
        .unwrap();
    assert_eq!(
        w1, w2,
        "fallback path must be deterministic for a fixed request_id"
    );
}

#[test]
fn unknown_cache_pricing_omits_cache_terms_without_cache_boost() {
    // Given: one candidate advertises a deep live cache, but the request carries
    // the default unknown pricing snapshot.
    let cached = with_live_cache(healthy_oauth_candidate("cached", 1), 500_000);
    let peer = healthy_oauth_candidate("peer", 2);

    // When: subscription-preference scores both candidates.
    let out = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_unknown_cache_pricing(MODEL_AGNOSTIC, "req-unknown-price"),
            &principal(),
            &[cached.clone(), peer],
        )
        .unwrap();

    // Then: unknown pricing does not become cache_loss_unknown or hidden owner
    // retention; it simply removes cache economics from the weight.
    let trace = out.subscription_preference.expect("trace present");
    let cached_urgency = candidate_urgency_for(&trace, cached.upstream_id);
    assert_eq!(cached_urgency.cache_ratio, 0.0);
    assert!((cached_urgency.cache_weight_multiplier - 1.0).abs() < CROSSOVER_TOLERANCE);
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
        thread_id: None,
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: canonical_model.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn ctx_with_thread_id(canonical_model: &str, request_id: &str, thread_id: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        thread_id: Some(thread_id.to_owned()),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: canonical_model.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn ctx_with_unknown_cache_pricing(canonical_model: &str, request_id: &str) -> RequestContext {
    RequestContext {
        cache_pricing: cc_lb_plugin_api::CachePricingSummary::default(),
        ..ctx_with_request_id(canonical_model, request_id)
    }
}

fn test_cache_pricing() -> cc_lb_plugin_api::CachePricingSummary {
    cc_lb_plugin_api::CachePricingSummary {
        status: "known".to_owned(),
        input_micros_per_million: Some(5_000_000),
        cache_creation_5m_micros_per_million: Some(6_250_000),
        cache_creation_1h_micros_per_million: Some(10_000_000),
        cache_read_micros_per_million: Some(500_000),
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

/// Attach a live prompt-cache observation with the given
/// `predicted_cache_read_tokens` to a candidate. Used by v7 crossover
/// tests to synthesise a bucket where cache-hit depth varies per
/// candidate; the max within the bucket drives the exponential boost.
fn with_live_cache(mut candidate: UpstreamCandidate, read_tokens: u32) -> UpstreamCandidate {
    candidate.cache_score = Some(cc_lb_plugin_api::types::CacheScore {
        predicted_cache_read_tokens: read_tokens,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: Some(0),
        confidence: 1.0,
        ambiguity_reason: None,
    });
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

// =============================================================================
// Section K — Observability trace: wrh_key_source and salt version
// =============================================================================

fn healthy_oauth_candidate(name: &str, id_seed: u8) -> UpstreamCandidate {
    oauth_with(
        name,
        id_seed,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(0.10)
                .reset_at(T0_SECS + 3_600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.10)
                .reset_at(T0_SECS + 604_800)
                .build(),
        ],
    )
}

#[test]
fn rendezvous_salt_embeds_declared_version() {
    let embed = format!(":{SALT_VERSION}:");
    assert!(
        RENDEZVOUS_SALT.contains(&embed),
        "RENDEZVOUS_SALT `{RENDEZVOUS_SALT}` must embed SALT_VERSION `{SALT_VERSION}` verbatim; \
         they are bumped together per the salt-version invariant",
    );
}

#[test]
fn wrh_key_source_is_thread_id_when_thread_has_positive_priced_cache_value() {
    // Given: a threaded request where at least one candidate has priced live-cache value.
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        with_live_cache(healthy_oauth_candidate("upstream-a", 1), 500_000),
        healthy_oauth_candidate("upstream-b", 2),
    ];

    // When: subscription-preference scores the cache-warm bucket.
    let ctx = ctx_with_thread_id(SONNET_MODEL, "req-1", "thread-A");
    let output = filter.filter(&ctx, &principal(), &candidates).unwrap();
    let trace = output
        .subscription_preference
        .expect("subscription-alive path must emit trace");

    // Then: thread_id is used only because current facts include positive cache value.
    assert_eq!(
        trace.wrh_key_source,
        WrhKeySource::ThreadId,
        "cache-warm subscription routing should key on thread_id when present"
    );
    assert_eq!(
        trace.rendezvous_salt_version.as_deref(),
        Some(SALT_VERSION),
        "trace must stamp the current WRH salt version so post-hoc queries can \
         distinguish algorithm changes from state changes"
    );
}

#[test]
fn wrh_key_source_falls_back_to_request_id_when_thread_id_absent() {
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        healthy_oauth_candidate("upstream-a", 1),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let ctx = ctx_with_request_id(SONNET_MODEL, "req-1");
    let output = filter.filter(&ctx, &principal(), &candidates).unwrap();
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(trace.wrh_key_source, WrhKeySource::RequestId);
}

#[test]
fn wrh_key_source_falls_back_to_request_id_when_thread_id_is_empty_string() {
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        healthy_oauth_candidate("upstream-a", 1),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let ctx = ctx_with_thread_id(SONNET_MODEL, "req-1", "");
    let output = filter.filter(&ctx, &principal(), &candidates).unwrap();
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(
        trace.wrh_key_source,
        WrhKeySource::RequestId,
        "empty thread_id must be treated as absent so an upstream that \
         populates the header with an empty string does not accidentally pin"
    );
}

#[test]
fn previous_tier_is_none_on_every_turn() {
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        healthy_oauth_candidate("upstream-a", 1),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let principal = principal();
    let ctx_turn_1 = ctx_with_thread_id(SONNET_MODEL, "req-1", "thread-A");
    let out_1 = filter.filter(&ctx_turn_1, &principal, &candidates).unwrap();
    let trace_1 = out_1.subscription_preference.expect("turn-1 trace present");
    assert!(
        trace_1.previous_tier.is_none(),
        "first turn on a thread has no prior tier record"
    );

    let ctx_turn_2 = ctx_with_thread_id(SONNET_MODEL, "req-2", "thread-A");
    let out_2 = filter.filter(&ctx_turn_2, &principal, &candidates).unwrap();
    let trace_2 = out_2.subscription_preference.expect("turn-2 trace present");
    assert!(trace_2.previous_tier.is_none());
}

#[test]
fn previous_tier_is_not_shared_across_threads() {
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        healthy_oauth_candidate("upstream-a", 1),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let principal = principal();
    let ctx_a1 = ctx_with_thread_id(SONNET_MODEL, "req-a1", "thread-A");
    let _ = filter.filter(&ctx_a1, &principal, &candidates).unwrap();

    let ctx_b1 = ctx_with_thread_id(SONNET_MODEL, "req-b1", "thread-B");
    let out_b1 = filter.filter(&ctx_b1, &principal, &candidates).unwrap();
    let trace_b1 = out_b1.subscription_preference.expect("trace present");
    assert!(
        trace_b1.previous_tier.is_none(),
        "thread-B first turn must not see thread-A's tier record"
    );
}

#[test]
fn oversized_thread_id_uses_bounded_routing_key() {
    // Given: a caller-controlled session id much larger than the routing key cap.
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        with_live_cache(healthy_oauth_candidate("upstream-a", 1), 500_000),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let principal = principal();
    let oversized_thread_id = "session-".repeat(MAX_THREAD_ROUTING_KEY_BYTES);

    // When: two turns use the same oversized session id.
    let first = filter
        .filter(
            &ctx_with_thread_id(SONNET_MODEL, "req-long-1", &oversized_thread_id),
            &principal,
            &candidates,
        )
        .unwrap();
    let first_trace = first.subscription_preference.expect("first trace present");
    let second = filter
        .filter(
            &ctx_with_thread_id(SONNET_MODEL, "req-long-2", &oversized_thread_id),
            &principal,
            &candidates,
        )
        .unwrap();
    let second_trace = second
        .subscription_preference
        .expect("second trace present");

    // Then: routing still treats it as ThreadId when cache value is live.
    assert_eq!(first_trace.wrh_key_source, WrhKeySource::ThreadId);
    assert_eq!(second_trace.wrh_key_source, WrhKeySource::ThreadId);
}

#[test]
fn stateless_requests_never_populate_previous_tier() {
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        healthy_oauth_candidate("upstream-a", 1),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let principal = principal();
    for i in 0..5 {
        let ctx = ctx_with_request_id(SONNET_MODEL, &format!("req-{i}"));
        let out = filter.filter(&ctx, &principal, &candidates).unwrap();
        let trace = out.subscription_preference.expect("trace present");
        assert!(
            trace.previous_tier.is_none(),
            "iteration {i}: stateless (no thread_id) request must never surface previous_tier",
        );
    }
}

// =============================================================================
// Section L — v7 cache-weighted WRH: exponential cache boost with 99% crossover.
// See docs/adr/0004-cache-weighted-subscription-preference.md.
// =============================================================================

use crate::builtin_filters::subscription_preference::CACHE_LOG_BOOST;

const CROSSOVER_TOLERANCE: f64 = 1e-9;
const FIVE_HOUR_RESET_SECS: u64 = 18_000;
const SEVEN_DAY_RESET_SECS: u64 = 604_800;

fn healthy_known_base_at_util(name: &str, id_seed: u8, util: f64) -> UpstreamCandidate {
    oauth_at_t0(
        name,
        id_seed,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(util)
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(util)
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
                .build(),
        ],
    )
}

fn candidate_urgency_for(
    trace: &cc_lb_plugin_api::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> &cc_lb_plugin_api::CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|c| c.upstream_id == upstream_id)
        .expect("candidate urgency must be present in trace")
}

fn expected_quota_urgency(util: f64) -> f64 {
    (1.0 - util).powi(2) / (FIVE_HOUR_RESET_SECS as f64)
}

fn expected_cache_weight_multiplier(cache_ratio: f64) -> f64 {
    (CACHE_LOG_BOOST * cache_ratio).exp()
}

#[test]
fn warm_low_util_pins_cache_holder() {
    // Given: bear-max util 0.30 with 250K cache; Runbear util 0.10 with 15K cache.
    // When: subscription filter scores the candidates.
    // Then: bear effective_weight ≈ 0.3916, Runbear ≈ 7.99e-5; bear wins ≈99.98% of draws.
    let bear = with_live_cache(healthy_known_base_at_util("bear-max", 1, 0.30), 250_000);
    let runbear = with_live_cache(healthy_known_base_at_util("runbear", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), runbear.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let bear_urg = candidate_urgency_for(&trace, bear.upstream_id);
    let runbear_urg = candidate_urgency_for(&trace, runbear.upstream_id);
    let bear_expected = expected_quota_urgency(0.30) * expected_cache_weight_multiplier(1.0);
    let runbear_expected =
        expected_quota_urgency(0.10) * expected_cache_weight_multiplier(15_000.0 / 250_000.0);
    assert!(
        (bear_urg.effective_weight - bear_expected).abs() < CROSSOVER_TOLERANCE,
        "bear effective_weight {} vs expected {}",
        bear_urg.effective_weight,
        bear_expected,
    );
    assert!(
        (runbear_urg.effective_weight - runbear_expected).abs() < CROSSOVER_TOLERANCE,
        "runbear effective_weight {} vs expected {}",
        runbear_urg.effective_weight,
        runbear_expected,
    );
    assert!(
        bear_urg.effective_weight > runbear_urg.effective_weight * 1000.0,
        "at util 0.30 bear-max must dominate (effective_weight ratio >= 1000×) to win >99.9%"
    );
}

#[test]
fn warm_95_percent_still_pins_cache_holder() {
    // v7 crossover point ≈ 0.99. At util 0.95 bear-max must still dominate ≈96% win share.
    let bear = with_live_cache(healthy_known_base_at_util("bear-max", 1, 0.95), 250_000);
    let runbear = with_live_cache(healthy_known_base_at_util("runbear", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), runbear.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let bear_urg = candidate_urgency_for(&trace, bear.upstream_id);
    let runbear_urg = candidate_urgency_for(&trace, runbear.upstream_id);
    let bear_expected = expected_quota_urgency(0.95) * expected_cache_weight_multiplier(1.0);
    let runbear_expected =
        expected_quota_urgency(0.10) * expected_cache_weight_multiplier(15_000.0 / 250_000.0);
    assert!(
        (bear_urg.effective_weight - bear_expected).abs() < CROSSOVER_TOLERANCE,
        "bear effective_weight {} vs expected {}",
        bear_urg.effective_weight,
        bear_expected,
    );
    assert!(
        (runbear_urg.effective_weight - runbear_expected).abs() < CROSSOVER_TOLERANCE,
        "runbear effective_weight {} vs expected {}",
        runbear_urg.effective_weight,
        runbear_expected,
    );
    let ratio = bear_urg.effective_weight / runbear_urg.effective_weight;
    assert!(
        ratio > 20.0,
        "at util 0.95 bear-max effective_weight/runbear ratio must exceed 20 (≈96%+ win); got {ratio}"
    );
}

#[test]
fn warm_99_percent_starts_spreading_at_crossover() {
    // Calibration target: at bear util 0.99 with cache_ratio 1.0, effective_weight
    // equals Runbear at util 0.10 with cache_ratio 0.06. |Δ| below crossover
    // tolerance is the invariant CACHE_LOG_BOOST is calibrated for.
    let bear = with_live_cache(healthy_known_base_at_util("bear-max", 1, 0.99), 250_000);
    let runbear = with_live_cache(healthy_known_base_at_util("runbear", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), runbear.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let bear_urg = candidate_urgency_for(&trace, bear.upstream_id);
    let runbear_urg = candidate_urgency_for(&trace, runbear.upstream_id);
    assert!(
        (bear_urg.effective_weight - runbear_urg.effective_weight).abs() < CROSSOVER_TOLERANCE,
        "at util 0.99 bear-max effective_weight ({}) must equal Runbear ({}) within {}",
        bear_urg.effective_weight,
        runbear_urg.effective_weight,
        CROSSOVER_TOLERANCE,
    );
}

#[test]
fn warm_cache_holder_blocked_spills_to_fresh_quota_peer() {
    // Given: bear-max carries deep cache but its 5h window is fresh + status=rejected
    //   (util 1.0), so tier assessment marks it HardNegative with no overage.
    //   Runbear is KnownBase with light cache.
    // When: subscription-preference filters.
    // Then: bear is not present in the KnownBase bucket; Runbear wins outright.
    let bear = with_live_cache(
        oauth_at_t0(
            "bear-max",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .status("rejected")
                    .util(1.0)
                    .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .status("rejected")
                    .util(1.0)
                    .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
                    .build(),
            ],
        ),
        250_000,
    );
    let runbear = with_live_cache(healthy_known_base_at_util("runbear", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), runbear.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    assert!(
        trace
            .candidates
            .iter()
            .all(|c| c.upstream_id != bear.upstream_id),
        "hard-blocked bear-max must be excluded from the assessed bucket"
    );
    assert_eq!(
        out.kept_upstream_ids,
        vec![runbear.upstream_id],
        "with bear-max hard-blocked, Runbear must be the sole winner even though its cache is shallow"
    );
}

#[test]
fn usage_warm_owner_beats_moderate_quota_disadvantage() {
    // Given: a warm owner has the same production-shape disadvantage observed in
    // ses_0c30 (~1.33x lower quota urgency), but strong provider usage lineage.
    let owner = with_live_cache(
        healthy_known_base_at_util("isac-personal", 1, 0.22),
        590_000,
    );
    let quota_peer = healthy_known_base_at_util("bear-max", 2, 0.10);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_thread_id(MODEL_AGNOSTIC, "req-1", "ses-warm-owner");

    // When: subscription-preference scores the same warm thread.
    let out = filter
        .filter(&ctx, &principal(), &[owner.clone(), quota_peer.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let owner_urg = candidate_urgency_for(&trace, owner.upstream_id);
    let peer_urg = candidate_urgency_for(&trace, quota_peer.upstream_id);

    // Then: cache bonus multiplies the existing quota weight instead of allowing
    // a modest quota advantage to recreate a hot Anthropic prompt cache elsewhere.
    assert_eq!(trace.wrh_key_source, WrhKeySource::ThreadId);
    assert!(
        peer_urg.quota_urgency / owner_urg.quota_urgency > 1.30,
        "fixture must preserve the observed moderate quota disadvantage"
    );
    assert!(
        owner_urg.effective_weight > peer_urg.effective_weight,
        "warm owner effective weight must beat moderate quota disadvantage"
    );
    assert_eq!(out.kept_upstream_ids, vec![owner.upstream_id]);
}

#[test]
fn severe_quota_pressure_can_override_usage_cache_owner() {
    // Given: a cache owner is nearly exhausted while a peer has plenty of quota.
    let owner = with_live_cache(
        healthy_known_base_at_util("isac-personal", 1, 0.9999),
        590_000,
    );
    let quota_peer = healthy_known_base_at_util("bear-max", 2, 0.10);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_thread_id(MODEL_AGNOSTIC, "req-1", "ses-severe-quota");

    // When: subscription-preference scores the same warm thread.
    let out = filter
        .filter(&ctx, &principal(), &[owner.clone(), quota_peer.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let owner_urg = candidate_urgency_for(&trace, owner.upstream_id);
    let peer_urg = candidate_urgency_for(&trace, quota_peer.upstream_id);

    // Then: cache locality is not an absolute v9-style pin; severe quota pressure
    // can still select the fresh peer.
    assert_eq!(trace.wrh_key_source, WrhKeySource::ThreadId);
    assert!(
        peer_urg.effective_weight > owner_urg.effective_weight,
        "fresh quota peer must beat a near-exhausted cache owner"
    );
    assert_eq!(out.kept_upstream_ids, vec![quota_peer.upstream_id]);
}

#[test]
fn all_cold_reduces_to_pure_quota_wrh_distribution() {
    // v7 must collapse to v6 uniform-quota behaviour when no candidate has any cache
    // signal (cache_ratio=0 across the pool → multiplier=1 → effective_weight=quota_urgency).
    let candidates = vec![
        live_snapshot_bear_max(),
        live_snapshot_isac_personal(),
        live_snapshot_bh322yoo_max(),
        live_snapshot_runbear(),
    ];
    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter.filter(&ctx, &principal, &candidates).unwrap();
    let trace = out.subscription_preference.expect("trace present");
    assert_eq!(trace.wrh_key_source, WrhKeySource::RequestId);
    for candidate in &trace.candidates {
        assert_eq!(candidate.predicted_cache_read_tokens, 0);
        assert_eq!(candidate.cache_ratio, 0.0);
        assert!(
            (candidate.cache_weight_multiplier - 1.0).abs() < CROSSOVER_TOLERANCE,
            "cold candidate must have cache_weight_multiplier=1.0, got {}",
            candidate.cache_weight_multiplier,
        );
        assert!(
            (candidate.effective_weight - candidate.quota_urgency).abs() < CROSSOVER_TOLERANCE,
            "cold candidate effective_weight ({}) must equal quota_urgency ({})",
            candidate.effective_weight,
            candidate.quota_urgency,
        );
    }
}

#[test]
fn tier_ordering_never_broken_by_cache_boost() {
    // A PartialBase candidate with the deepest possible cache must NEVER win over
    // a KnownBase candidate with no cache. Cache boost operates strictly within tier.
    let known_base = healthy_known_base_at_util("known-base", 1, 0.10);
    // PartialBase: 5h fresh+allowed, 7d MISSING (positive_count=1, total=2 → PartialBase).
    let partial_base = with_live_cache(
        oauth_at_t0(
            "partial-base",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .status("allowed")
                    .util(0.10)
                    .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
                    .build(),
            ],
        ),
        500_000,
    );
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[known_base.clone(), partial_base])
        .unwrap();
    assert_eq!(
        out.kept_upstream_ids,
        vec![known_base.upstream_id],
        "KnownBase must always beat PartialBase regardless of cache depth"
    );
}

#[test]
fn cache_boost_calibration_at_99_percent() {
    // Direct assertion of the CACHE_LOG_BOOST calibration target. See ADR 0004.
    let bear_quota = expected_quota_urgency(0.99);
    let runbear_quota = expected_quota_urgency(0.10);
    let bear_effective = bear_quota * expected_cache_weight_multiplier(1.0);
    let runbear_effective = runbear_quota * expected_cache_weight_multiplier(15_000.0 / 250_000.0);
    assert!(
        (bear_effective - runbear_effective).abs() < CROSSOVER_TOLERANCE,
        "CACHE_LOG_BOOST={} miscalibrated: bear (util=0.99, ratio=1.0) effective_weight {} \
         must equal Runbear (util=0.10, ratio=0.06) {} within {}",
        CACHE_LOG_BOOST,
        bear_effective,
        runbear_effective,
        CROSSOVER_TOLERANCE,
    );
}
