//! Tests for the tier + cost-first subscription-preference filter.
//!
//! Sections:
//!
//! - A — Gate / tier selection (kind, hard-negative, overage promotion)
//! - B — Base-window classification state machine (fresh vs stale, disabled)
//! - C — Reset semantics
//! - D — Overage / extra_usage assessment
//! - E — Model relevance (5h + 7d, excludes unstable model-specific windows,
//!   plus the Fable-scoped 7d_fable window only for claude-fable-5)
//! - F — ADR 0008 pressure numerics and frozen v10 overage urgency
//! - G — Capacity-ratio independence across pressure and deterministic selection
//! - H — Cost-first selection: stable ties, single candidate, determinism
//! - I — Anti-stampede deterministic selection
//! - J — Live-snapshot regression (four-upstream production fixture)

use bytes::Bytes;
use cc_lb_domain::{PrincipalKind, SubscriptionQuotaDataState};
use cc_lb_routing::RoutingContext;
use http::Method;
use std::collections::HashMap;

use super::*;

mod cost_first;
mod fable_pressure;
mod fable_resetless;
mod fable_uniform_classification;

const SONNET_MODEL: &str = "claude-sonnet-4-5-20250929";
const OPUS_MODEL: &str = "claude-opus-4-8-20250514";
const HAIKU_MODEL: &str = "claude-haiku-4-5-20251001";
const DATED_FABLE_LIKE_MODEL: &str = "claude-fable-5-20260701";
const FUTURE_FABLE_LIKE_MODEL: &str = "claude-fable-6";
const UNKNOWN_MODEL: &str = "claude-unknown-model";
const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";
const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";

const T0_SECS: u64 = 1_700_000_000;
const FIVE_HOUR_RESET_SECS: u64 = 18_000;
const SEVEN_DAY_RESET_SECS: u64 = 604_800;

fn candidate_urgency_for(
    trace: &cc_lb_domain::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> &cc_lb_domain::CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .expect("candidate urgency must be present in trace")
}

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

fn healthy_oauth_candidate(name: &str, id_seed: u8) -> UpstreamCandidate {
    healthy_known_base_at_util(name, id_seed, 0.10)
}

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
fn all_oauth_hard_negative_no_api_key_fails_closed() {
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
    let output = filter_for_model(&[dead_a, dead_b], MODEL_AGNOSTIC);
    assert!(output.kept_upstream_ids.is_empty());
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
fn stale_exhausted_utilization_with_future_reset_is_hard_negative() {
    let dead = oauth_at_t0(
        "dead",
        1,
        vec![
            stale(WINDOW_SEVEN_DAY)
                .util(1.0)
                .status("allowed")
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
                .build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn stale_disabled_window_with_future_reset_is_hard_negative() {
    let dead = oauth_at_t0(
        "dead",
        1,
        vec![
            stale(WINDOW_SEVEN_DAY)
                .util(0.2)
                .status("allowed")
                .disabled("subscription_disabled")
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
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
fn stale_positive_overage_signals_do_not_authorize_exhausted_base() {
    let cases = [
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            stale(WINDOW_OVERAGE).util(0.2).status("allowed").build(),
        ],
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            stale(WINDOW_UNIFIED).fallback_available(true).build(),
        ],
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            stale(WINDOW_UNIFIED).overage_in_use(true).build(),
        ],
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            stale(WINDOW_UNIFIED)
                .extra_usage_enabled(true)
                .extra_usage_limit(100.0)
                .extra_usage_used(20.0)
                .build(),
        ],
    ];

    for (index, quotas) in cases.into_iter().enumerate() {
        let candidate = oauth_with("stale-overage", index as u8 + 1, quotas);
        let key = api_key("k", 100 + index as u8);
        let output = filter_for_model(&[candidate, key.clone()], MODEL_AGNOSTIC);
        assert_eq!(
            output.kept_upstream_ids,
            vec![key.upstream_id],
            "stale positive case {index} must not authorize overage"
        );
    }
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
fn sonnet_request_blocks_exhausted_7d_sonnet_when_observed() {
    let sonnet_exhausted = oauth_with(
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
    let output = filter_for_model(&[sonnet_exhausted, key.clone()], SONNET_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
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
fn opus_request_blocks_exhausted_7d_opus_when_observed() {
    let opus_exhausted = oauth_with(
        "opus",
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
    let output = filter_for_model(&[opus_exhausted, key.clone()], OPUS_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn non_opus_requests_ignore_7d_opus_window() {
    for model in [SONNET_MODEL, HAIKU_MODEL, MODEL_AGNOSTIC] {
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

#[test]
fn absent_optional_scoped_windows_are_noop() {
    for (model, window) in [
        (SONNET_MODEL, WINDOW_SEVEN_DAY_SONNET),
        (OPUS_MODEL, WINDOW_SEVEN_DAY_OPUS),
    ] {
        let alive = oauth_with(
            "alive",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
                absent(window).build(),
            ],
        );
        let output = filter_for_model(std::slice::from_ref(&alive), model);
        assert_eq!(output.kept_upstream_ids, vec![alive.upstream_id]);
        assert_eq!(
            output
                .subscription_preference
                .expect("subscription trace")
                .chosen_tier,
            cc_lb_domain::SubscriptionTier::KnownBase,
            "model={model} absent optional scoped quota must be a no-op"
        );
    }
}

#[test]
fn unobserved_optional_scoped_window_remains_partial_base() {
    let candidate = oauth_with(
        "unobserved-opus",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            missing(WINDOW_SEVEN_DAY_OPUS).build(),
        ],
    );

    let assessment = assess_candidate(
        &candidate,
        0,
        &relevant_base_windows(OPUS_MODEL),
        &FilterConfig::default(),
    )
    .expect("candidate remains assessable");

    assert_eq!(assessment.tier, Tier::PartialBase);
}

#[test]
fn fable_request_blocks_oauth_with_exhausted_7d_fable_window() {
    // Given: shared quota is healthy, but the Fable-scoped weekly quota is
    // confirmed exhausted for an OAuth candidate with no usable overage.
    let fable_exhausted = oauth_with(
        "fable-exhausted",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(1.0)
                .status("rejected")
                .reset_at(T0_SECS + 302_400)
                .build(),
        ],
    );
    let key = api_key("k", 2);

    // When: the request targets a canonical Fable model.
    let output = filter_for_model(&[fable_exhausted, key.clone()], FABLE_MODEL);

    // Then: the exhausted OAuth candidate is blocked and the API key wins.
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn non_fable_requests_ignore_exhausted_7d_fable_window() {
    for model in [
        SONNET_MODEL,
        OPUS_MODEL,
        HAIKU_MODEL,
        DATED_FABLE_LIKE_MODEL,
        FUTURE_FABLE_LIKE_MODEL,
        UNKNOWN_MODEL,
    ] {
        // Given: shared quota is healthy and only the Fable-scoped window is
        // confirmed exhausted.
        let oauth = oauth_with(
            "shared-quota-healthy",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY_FABLE)
                    .util(1.0)
                    .status("rejected")
                    .build(),
            ],
        );
        let key = api_key("k", 2);

        // When: the request targets a non-Fable canonical model.
        let output = filter_for_model(&[oauth.clone(), key], model);

        // Then: 7d_fable is irrelevant and healthy shared quota keeps OAuth.
        assert_eq!(
            output.kept_upstream_ids,
            vec![oauth.upstream_id],
            "model={model} must ignore 7d_fable"
        );
        assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
    }
}

#[test]
fn model_scoped_weekly_pressure_is_used_without_shared_seven_day() {
    for (model, window) in [
        (SONNET_MODEL, WINDOW_SEVEN_DAY_SONNET),
        (OPUS_MODEL, WINDOW_SEVEN_DAY_OPUS),
    ] {
        let available = oauth_at_t0(
            "available",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.2)
                    .status("allowed")
                    .reset_at(T0_SECS + 9_000)
                    .build(),
                fresh(window)
                    .util(0.25)
                    .status("allowed")
                    .reset_at(T0_SECS + 302_400)
                    .build(),
            ],
        );
        let nearly_exhausted = oauth_at_t0(
            "nearly-exhausted",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.2)
                    .status("allowed")
                    .reset_at(T0_SECS + 9_000)
                    .build(),
                fresh(window)
                    .util(0.98)
                    .status("allowed")
                    .reset_at(T0_SECS + 302_400)
                    .build(),
            ],
        );
        let windows = relevant_base_windows(model);
        let available_assessment =
            assess_candidate(&available, 0, &windows, &FilterConfig::default())
                .expect("available scoped-weekly candidate is assessable");
        let nearly_exhausted_assessment =
            assess_candidate(&nearly_exhausted, 1, &windows, &FilterConfig::default())
                .expect("nearly exhausted scoped-weekly candidate is assessable");

        assert!(
            available_assessment.quota_urgency_7d > nearly_exhausted_assessment.quota_urgency_7d,
            "model={model} must include the observed {window} window in weekly pressure"
        );
    }
}

// =============================================================================
// Section F — Urgency numerics
// =============================================================================

#[test]
fn quota_pressure_golden_values() {
    // Given: representative half-window, combined-window, and floor-bound snapshots.
    let five_hour = oauth_at_t0(
        "five-hour",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 9_000)
                .build(),
        ],
    );
    let seven_day = oauth_at_t0(
        "seven-day",
        2,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
        ],
    );
    let combined = oauth_at_t0(
        "combined",
        3,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 9_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
        ],
    );
    let floor_bound = oauth_at_t0(
        "floor-bound",
        4,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.0)
                .status("allowed")
                .reset_at(T0_SECS + 1)
                .build(),
        ],
    );
    let config = FilterConfig::default();

    // When: base pressure is assessed for each ADR golden scenario.
    let five_hour_pressure = assess_candidate(&five_hour, 0, &[WINDOW_FIVE_HOUR], &config)
        .expect("five-hour candidate is assessable")
        .quota_urgency_combined;
    let seven_day_pressure = assess_candidate(&seven_day, 0, &[WINDOW_SEVEN_DAY], &config)
        .expect("seven-day candidate is assessable")
        .quota_urgency_combined;
    let combined_pressure =
        assess_candidate(&combined, 0, &[WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY], &config)
            .expect("combined candidate is assessable")
            .quota_urgency_combined;
    let floor_pressure = assess_candidate(&floor_bound, 0, &[WINDOW_FIVE_HOUR], &config)
        .expect("floor-bound candidate is assessable")
        .quota_urgency_combined;
    let weekly_gamma_one_pressure = pressure_from_ratios(
        0.75,
        0.5,
        BaseWindowPressureConfig {
            window_len_secs: SEVEN_DAY_WINDOW_LEN_SECS,
            gamma: 1.0,
            target_floor: SEVEN_DAY_TARGET_FLOOR,
        },
    );

    // Then: the values match ADR 0008, including the linear-gamma control.
    assert!((five_hour_pressure - 0.405_465_108_108_164_4).abs() <= 1e-12);
    assert!((seven_day_pressure - 0.613_409_262_276_148).abs() <= 1e-12);
    assert!((weekly_gamma_one_pressure - 0.405_465_108_108_164_4).abs() <= 1e-12);
    assert!((combined_pressure - 0.621_654_592_904_933_6).abs() <= 1e-12);
    assert!((floor_pressure - 4.605_170_185_988_092).abs() <= 1e-12);
}

#[test]
fn assessment_preserves_base_pressure_components() {
    // Given: a KnownBase candidate with distinct 5h and 7d pressures.
    let candidate = oauth_at_t0(
        "base",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 9_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
        ],
    );

    // When: the private assessment is built once at the candidate boundary.
    let assessment = assess_candidate(
        &candidate,
        0,
        &[WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY],
        &FilterConfig::default(),
    )
    .expect("base candidate is assessable");

    // Then: both components and their combined pressure remain available internally.
    assert_eq!(assessment.tier, Tier::KnownBase);
    assert!((assessment.quota_urgency_5h - 0.405_465_108_108_164_4).abs() <= 1e-12);
    assert!((assessment.quota_urgency_7d - 0.613_409_262_276_148).abs() <= 1e-12);
    assert!((assessment.quota_urgency_combined - 0.621_654_592_904_933_6).abs() <= 1e-12);
    assert_eq!(assessment.overage_urgency, 0.0);
}

#[test]
fn assessment_preserves_overage_raw_urgency_separately() {
    // Given: a base-blocked candidate with usable overage.
    let candidate = oauth_at_t0(
        "overage",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.25).status("allowed").build(),
        ],
    );

    // When: the candidate is assessed into the Overage tier.
    let assessment = assess_candidate(
        &candidate,
        0,
        &[WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY],
        &FilterConfig::default(),
    )
    .expect("overage candidate is assessable");

    // Then: raw v10 overage urgency is separate and base components are zero.
    assert_eq!(assessment.tier, Tier::Overage);
    assert_eq!(assessment.quota_urgency_5h, 0.0);
    assert_eq!(assessment.quota_urgency_7d, 0.0);
    assert_eq!(assessment.quota_urgency_combined, 0.0);
    assert_eq!(assessment.overage_urgency, overage_urgency(Some(0.25)));
}

#[test]
fn quota_pressure_zero_for_unusable_windows_and_full_utilization() {
    // Given: every unusable-window shape plus one usable control window.
    let cases = [
        oauth_at_t0("missing", 1, Vec::new()),
        oauth_at_t0(
            "stale",
            2,
            vec![
                stale(WINDOW_FIVE_HOUR)
                    .util(0.25)
                    .reset_at(T0_SECS + 9_000)
                    .build(),
            ],
        ),
        oauth_at_t0(
            "missing-utilization",
            3,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .status("allowed")
                    .reset_at(T0_SECS + 9_000)
                    .build(),
            ],
        ),
        oauth_at_t0(
            "missing-reset",
            4,
            vec![fresh(WINDOW_FIVE_HOUR).util(0.25).status("allowed").build()],
        ),
        oauth_at_t0(
            "elapsed",
            5,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.25)
                    .status("allowed")
                    .reset_at(T0_SECS)
                    .build(),
            ],
        ),
        oauth_at_t0(
            "non-finite",
            6,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(f64::NAN)
                    .status("allowed")
                    .reset_at(T0_SECS + 9_000)
                    .build(),
            ],
        ),
        oauth_at_t0(
            "full",
            7,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(1.0)
                    .status("allowed")
                    .reset_at(T0_SECS + 9_000)
                    .build(),
            ],
        ),
    ];
    let usable = oauth_at_t0(
        "usable",
        8,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 9_000)
                .build(),
        ],
    );
    let config = FilterConfig::default();

    // When: every case is reduced to its base-window pressure contribution.
    let pressures = cases.map(|candidate| {
        assess_candidate(&candidate, 0, &[WINDOW_FIVE_HOUR], &config)
            .map_or(0.0, |assessment| assessment.quota_urgency_combined)
    });
    let usable_pressure = assess_candidate(&usable, 0, &[WINDOW_FIVE_HOUR], &config)
        .expect("usable candidate is assessable")
        .quota_urgency_combined;

    // Then: unusable/full windows are zero and the control proves real v11 math ran.
    assert_eq!(pressures, [0.0; 7]);
    assert!((usable_pressure - 0.405_465_108_108_164_4).abs() <= 1e-12);
}

#[test]
fn quota_pressure_ignores_capacity_ratio() {
    // Given: identical pressure inputs attached to radically different plan capacities.
    let quota = || {
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 9_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
        ]
    };
    let small = with_plan(oauth_at_t0("small", 1, quota()), 1.0);
    let large = with_plan(oauth_at_t0("large", 2, quota()), 20.0);
    let config = FilterConfig::default();

    // When: base pressure is assessed independently for each candidate.
    let small_pressure =
        assess_candidate(&small, 0, &[WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY], &config)
            .expect("small-plan candidate is assessable")
            .quota_urgency_combined;
    let large_pressure =
        assess_candidate(&large, 0, &[WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY], &config)
            .expect("large-plan candidate is assessable")
            .quota_urgency_combined;

    // Then: plan capacity cannot alter raw base pressure.
    assert_eq!(small_pressure, large_pressure);
}

#[test]
fn short_reset_underuse_has_greater_pressure_and_share() {
    // Q1: ADR 0008 pressure prefers quota at greater risk of expiring unused.
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
    let output = filter_for_model(&[runbear.clone(), bear.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let runbear_pressure = candidate_urgency_for(&trace, runbear.upstream_id).quota_urgency;
    let bear_pressure = candidate_urgency_for(&trace, bear.upstream_id).quota_urgency;
    assert!(runbear_pressure > bear_pressure);
    assert_eq!(output.kept_upstream_ids, vec![runbear.upstream_id]);
}

#[test]
fn combined_pressure_is_dominated_by_tight_window() {
    // Q2: ADR 0008 smoothmax remains dominated by the tight 5h pressure.
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
    let output = filter_for_model(&[tight_5h.clone(), loose_only], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let tight_pressure = candidate_urgency_for(&trace, tight_5h.upstream_id).quota_urgency;
    assert!(tight_pressure > 0.0);
    assert_eq!(output.kept_upstream_ids, vec![tight_5h.upstream_id]);
}

#[test]
fn resets_at_missing_window_excluded_from_urgency() {
    // Q4: two identical candidates except one has resets_at populated on 5h
    // and the other doesn't. Both windows still classify as
    // CurrentPositive (fresh + allowed), so both stay in KnownBase. The
    // one without resets_at contributes zero pressure but remains selectable
    // through ADR 0008's neutral factor.
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
    let output = filter_for_model(&[with_reset.clone(), no_reset.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let with_reset_weight = candidate_urgency_for(&trace, with_reset.upstream_id);
    let no_reset_weight = candidate_urgency_for(&trace, no_reset.upstream_id);
    assert!(with_reset_weight.quota_urgency > 0.0);
    assert_eq!(no_reset_weight.quota_urgency, 0.0);
    assert_eq!(output.kept_upstream_ids, vec![with_reset.upstream_id]);
}

#[test]
fn high_util_short_remaining_has_greater_pressure_and_share() {
    // Q5: ADR 0008 compares remaining quota against the time-shaped target.
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
    let output = filter_for_model(
        &[low_util_long.clone(), high_util_short.clone()],
        MODEL_AGNOSTIC,
    );
    let trace = output.subscription_preference.expect("trace present");
    let low_pressure = candidate_urgency_for(&trace, low_util_long.upstream_id).quota_urgency;
    let high_pressure = candidate_urgency_for(&trace, high_util_short.upstream_id).quota_urgency;
    assert!(high_pressure > low_pressure);
    assert_eq!(output.kept_upstream_ids, vec![high_util_short.upstream_id]);
}

// =============================================================================
// Section G — Capacity ratio is metadata only under ADR 0008
// =============================================================================

#[test]
fn plan_capacity_ratio_does_not_change_base_pressure() {
    // Given: identical base snapshots with an explicit ratio and without one.
    let explicit_ratio = with_plan(
        oauth_at_t0(
            "explicit-ratio",
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
        20.0,
    );
    let absent_ratio = oauth_at_t0(
        "absent-ratio",
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

    // When: ADR 0008 assesses and scores the base bucket.
    let output = filter_for_model(
        &[explicit_ratio.clone(), absent_ratio.clone()],
        MODEL_AGNOSTIC,
    );
    let trace = output.subscription_preference.expect("trace present");
    let explicit = candidate_urgency_for(&trace, explicit_ratio.upstream_id);
    let absent = candidate_urgency_for(&trace, absent_ratio.upstream_id);

    // Then: capacity metadata cannot alter any base-pressure component.
    assert!(explicit.quota_urgency > 0.0);
    assert_eq!(explicit.quota_urgency_5h, absent.quota_urgency_5h);
    assert_eq!(explicit.quota_urgency_7d, absent.quota_urgency_7d);
    assert_eq!(
        explicit.quota_urgency_combined,
        absent.quota_urgency_combined
    );
    assert_eq!(explicit.quota_urgency, absent.quota_urgency);
}

#[test]
fn plan_capacity_ratio_does_not_change_base_selection() {
    // Given: a non-uniform base bucket and the same IDs/snapshots with swapped ratios.
    let on_pace = healthy_known_base_at_util("on-pace", 1, 0.90);
    let urgent = oauth_at_t0(
        "urgent",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.10)
                .status("allowed")
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.10)
                .status("allowed")
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS / 2)
                .build(),
        ],
    );
    let original_ratios = [
        with_plan(on_pace.clone(), 1.0),
        with_plan(urgent.clone(), 20.0),
    ];
    let swapped_ratios = [with_plan(on_pace, 20.0), with_plan(urgent, 1.0)];

    // When: both fixtures route the same deterministic request sequence.
    let original_selection = selection_counts(&original_ratios, MODEL_AGNOSTIC, 2000);
    let swapped_selection = selection_counts(&swapped_ratios, MODEL_AGNOSTIC, 2000);

    // Then: plan capacity metadata cannot change a single routing outcome.
    assert_eq!(
        original_selection, swapped_selection,
        "ADR 0008 selection must be independent of plan_capacity_ratio"
    );
}

#[test]
fn plan_capacity_ratio_does_not_change_overage_urgency_or_selection() {
    // Given: identical overage candidates routed with original and swapped plan ratios.
    let first = oauth_at_t0(
        "first",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );
    let second = oauth_at_t0(
        "second",
        2,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );
    let original_ratios = [
        with_plan(first.clone(), 1.0),
        with_plan(second.clone(), 20.0),
    ];
    let swapped_ratios = [with_plan(first, 20.0), with_plan(second, 1.0)];

    // When: the original fixture is scored and both route the same request sequence.
    let output = filter_for_model(&original_ratios, MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let first_weight = candidate_urgency_for(&trace, original_ratios[0].upstream_id);
    let second_weight = candidate_urgency_for(&trace, original_ratios[1].upstream_id);
    let original_selection = selection_counts(&original_ratios, MODEL_AGNOSTIC, 2000);
    let swapped_selection = selection_counts(&swapped_ratios, MODEL_AGNOSTIC, 2000);

    // Then: capacity metadata affects neither overage urgency nor winners.
    assert_eq!(first_weight.quota_urgency, second_weight.quota_urgency);
    assert_eq!(
        original_selection, swapped_selection,
        "overage selection must be independent of plan_capacity_ratio"
    );
}

#[test]
fn overage_urgency_characterization_remains_v10() {
    // Given: a readable overage utilization and the existing unknown fallback.
    let expected_known = 0.75f64.powi(HEADROOM_EXPONENT) / (OVERAGE_REMAINING_NOMINAL_SECS as f64);

    // When: the unchanged overage urgency formula evaluates both inputs.
    let known = overage_urgency(Some(0.25));
    let unknown = overage_urgency(None);

    // Then: v10 overage headroom and fallback behavior remain characterized.
    assert_eq!(known, expected_known);
    assert_eq!(unknown, OVERAGE_UNKNOWN_WEIGHT);
}

#[test]
fn base_uniform_quota_urgency_is_zero() {
    // Given: two on-pace base candidates whose pressure sum is below EPSILON.
    let candidates = vec![
        healthy_known_base_at_util("a", 1, 0.0),
        healthy_known_base_at_util("b", 2, 0.0),
    ];

    // When: the filter scores their bucket.
    let output = filter_for_model(&candidates, MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");

    // Then: cost-first trace exposes zero urgency and neutral warning state.
    for candidate in &trace.candidates {
        assert_eq!(candidate.quota_urgency, 0.0);
        assert_eq!(candidate.warning_multiplier, 1.0);
    }
}

#[test]
fn base_non_uniform_quota_urgency_tracks_positive_pressure() {
    // Given: a base bucket containing distinct positive combined pressures.
    let candidates = vec![
        oauth_at_t0(
            "urgent",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.10)
                    .status("allowed")
                    .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.10)
                    .status("allowed")
                    .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS / 2)
                    .build(),
            ],
        ),
        healthy_known_base_at_util("less-urgent", 2, 0.90),
    ];

    // When: the filter scores their bucket.
    let output = filter_for_model(&candidates, MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");

    // Then: the urgent candidate carries positive pressure while its on-pace peer stays neutral.
    assert!(candidate_urgency_for(&trace, upstream_id(1)).quota_urgency > 0.0);
    assert_eq!(
        candidate_urgency_for(&trace, upstream_id(2)).quota_urgency,
        0.0
    );
}

#[test]
fn base_warning_signals_do_not_change_positive_pressure_selection() {
    // Given: a pressured base candidate and the same candidate after each
    // provider warning transition.
    let allowed = oauth_at_t0(
        "transitioned",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.10)
                .status("allowed")
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.10)
                .status("allowed")
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS / 2)
                .build(),
        ],
    );
    let mut explicit_warning = allowed.clone();
    explicit_warning.subscription_quotas[0].status = Some("allowed_warning".to_owned());
    let mut threshold_warning = allowed.clone();
    threshold_warning.subscription_quotas[0].surpassed_threshold = Some(0.05);
    let peer = oauth_at_t0(
        "peer",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.50)
                .status("allowed")
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.50)
                .status("allowed")
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS / 2)
                .build(),
        ],
    );

    // When: the allowed and warning-positive forms compete with the same peer.
    let allowed_output = filter_for_model(&[allowed.clone(), peer.clone()], MODEL_AGNOSTIC);
    for warning_candidate in [explicit_warning, threshold_warning] {
        let warning_output =
            filter_for_model(&[warning_candidate.clone(), peer.clone()], MODEL_AGNOSTIC);
        let trace = warning_output
            .subscription_preference
            .expect("subscription trace");
        let warning_weight = candidate_urgency_for(&trace, warning_candidate.upstream_id);

        // Then: warning remains classified and observable in quota state, but
        // base ranking uses a neutral multiplier and keeps the allowed winner.
        assert_eq!(
            warning_output.kept_upstream_ids,
            allowed_output.kept_upstream_ids
        );
        assert_eq!(
            warning_weight.tier,
            cc_lb_domain::SubscriptionTier::KnownBase
        );
        assert!(warning_weight.quota_urgency > 0.0);
        assert_eq!(warning_weight.warning_multiplier, 1.0);
    }

    let partial_allowed = oauth_at_t0(
        "partial-transitioned",
        3,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.10)
                .status("allowed")
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                .build(),
        ],
    );
    let mut partial_explicit_warning = partial_allowed.clone();
    partial_explicit_warning.subscription_quotas[0].status = Some("allowed_warning".to_owned());
    let mut partial_threshold_warning = partial_allowed.clone();
    partial_threshold_warning.subscription_quotas[0].surpassed_threshold = Some(0.05);
    let partial_peer = oauth_at_t0(
        "partial-peer",
        4,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.30)
                .status("allowed")
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                .build(),
        ],
    );
    let partial_allowed_output =
        filter_for_model(&[partial_allowed, partial_peer.clone()], MODEL_AGNOSTIC);

    for warning_candidate in [partial_explicit_warning, partial_threshold_warning] {
        let warning_output = filter_for_model(
            &[warning_candidate.clone(), partial_peer.clone()],
            MODEL_AGNOSTIC,
        );
        let trace = warning_output
            .subscription_preference
            .expect("subscription trace");
        let warning_weight = candidate_urgency_for(&trace, warning_candidate.upstream_id);

        assert_eq!(
            warning_output.kept_upstream_ids,
            partial_allowed_output.kept_upstream_ids
        );
        assert_eq!(
            warning_weight.tier,
            cc_lb_domain::SubscriptionTier::PartialBase
        );
        assert!(warning_weight.quota_urgency > 0.0);
        assert_eq!(warning_weight.warning_multiplier, 1.0);
    }
}

#[test]
fn overage_warning_multiplier_and_selection_remain_unchanged() {
    // Given: otherwise-equal overage candidates whose blocked base windows
    // differ only by a provider warning on the still-positive 5h window.
    let warned = oauth_at_t0(
        "warned-overage",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.10)
                .status("allowed_warning")
                .build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.25).status("allowed").build(),
        ],
    );
    let clean = oauth_at_t0(
        "clean-overage",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.10).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.25).status("allowed").build(),
        ],
    );

    // When: cost-first compares the two candidates in the Overage tier.
    let output = filter_for_model(&[warned.clone(), clean.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("subscription trace");
    let warned_weight = candidate_urgency_for(&trace, warned.upstream_id);
    let clean_weight = candidate_urgency_for(&trace, clean.upstream_id);

    // Then: the existing overage-only warning penalty remains active.
    assert_eq!(warned_weight.tier, cc_lb_domain::SubscriptionTier::Overage);
    assert_eq!(clean_weight.tier, cc_lb_domain::SubscriptionTier::Overage);
    assert_eq!(warned_weight.warning_multiplier, OVERAGE_WARNING_MULTIPLIER);
    assert_eq!(clean_weight.warning_multiplier, 1.0);
    assert_eq!(output.kept_upstream_ids, vec![clean.upstream_id]);
}

#[test]
fn overage_non_uniform_urgency_matches_v10() {
    // Given: two overage candidates with distinct v10 raw urgencies.
    let low_util = oauth_at_t0(
        "low-util",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.25).status("allowed").build(),
        ],
    );
    let high_util = oauth_at_t0(
        "high-util",
        2,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.75).status("allowed").build(),
        ],
    );
    // When: the filter scores the overage bucket.
    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(MODEL_AGNOSTIC, "overage-cost-first"),
            &principal(),
            &[low_util.clone(), high_util.clone()],
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");
    // Then: the trace keeps urgency observability while cost-first ranks it deterministically.
    assert!(candidate_urgency_for(&trace, low_util.upstream_id).quota_urgency > 0.0);
    assert!(candidate_urgency_for(&trace, high_util.upstream_id).quota_urgency > 0.0);
    assert_eq!(output.kept_upstream_ids, vec![upstream_id(1)]);
}

#[test]
fn overage_uniform_tiebreak_matches_cost_first() {
    // Given: two allowed overage candidates whose v10 urgency sum is below EPSILON.
    let first = oauth_at_t0(
        "first",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE)
                .util(0.999_999)
                .status("allowed")
                .build(),
        ],
    );
    let second = oauth_at_t0(
        "second",
        2,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE)
                .util(0.999_999)
                .status("allowed")
                .build(),
        ],
    );
    // When: the filter scores the overage bucket.
    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(MODEL_AGNOSTIC, "overage-uniform-cost-first"),
            &principal(),
            &[first.clone(), second.clone()],
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");
    // Then: equal all-None candidates use the stable upstream-ID tiebreak.
    assert_eq!(first.upstream_id, upstream_id(1));
    assert_eq!(second.upstream_id, upstream_id(2));
    for candidate in &trace.candidates {
        assert!(candidate.quota_urgency < EPSILON);
    }
    assert_eq!(output.kept_upstream_ids, vec![upstream_id(1)]);
}

#[test]
fn unknown_probe_quota_urgency_is_zero() {
    // Given: two OAuth candidates with no usable quota snapshots.
    let candidates = vec![oauth_at_t0("a", 1, vec![]), oauth_at_t0("b", 2, vec![])];

    // When: the filter scores the UnknownProbe bucket.
    let output = filter_for_model(&candidates, MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");

    // Then: unknown probes always expose neutral quota urgency.
    assert_eq!(
        trace.chosen_tier,
        cc_lb_domain::SubscriptionTier::UnknownProbe
    );
    for candidate in &trace.candidates {
        assert_eq!(candidate.quota_urgency, 0.0);
    }
}

// =============================================================================
// Section H — Cost-first selection: stable ties, single, determinism
// =============================================================================

#[test]
fn zero_total_weight_uses_upstream_id_tiebreak() {
    // Every candidate has a base-positive window but no resets_at, so
    // urgency=0 across the board. Cost-first falls back to stable upstream ID order.
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
    assert_eq!(
        filter_for_model(&[a.clone(), b, c, d], MODEL_AGNOSTIC).kept_upstream_ids,
        vec![a.upstream_id]
    );
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
fn different_request_ids_do_not_change_cost_first_winner() {
    // 3 identical KnownBase candidates + no resets_at -> urgency all zero.
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
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = [a.clone(), b, c];
    assert_eq!(
        filter
            .filter(
                &ctx_with_request_id(MODEL_AGNOSTIC, "one"),
                &principal(),
                &candidates
            )
            .unwrap()
            .kept_upstream_ids,
        filter
            .filter(
                &ctx_with_request_id(MODEL_AGNOSTIC, "two"),
                &principal(),
                &candidates
            )
            .unwrap()
            .kept_upstream_ids
    );
}

// =============================================================================
// Section I — Anti-stampede deterministic selection (Q3)
// =============================================================================

#[test]
fn four_identical_candidates_distribute_uniformly_across_many_requests() {
    // Q3: four candidates at util around 0.9 and remain around 30min stay deterministic.
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
    let dist = selection_counts(&candidates, MODEL_AGNOSTIC, 4000);
    assert_eq!(dist.len(), 1);
    assert_eq!(dist[&candidates[0].upstream_id], 4000);
}

// =============================================================================
// Section J — Live-snapshot regression (four-upstream production fixture)
// =============================================================================
//
// Baked from /tmp/quota-latest.json captured on 2026-07-04. The current-code
// algorithm funnelled 78% of traffic to bear-max. Cost-first-v1 now chooses a
// single deterministic cheapest/pressure-aware winner for identical inputs.

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
fn live_snapshot_four_upstreams_produces_cost_first_winner() {
    let candidates = vec![
        live_snapshot_bear_max(),
        live_snapshot_isac_personal(),
        live_snapshot_bh322yoo_max(),
        live_snapshot_runbear(),
    ];
    let dist = selection_counts(&candidates, MODEL_AGNOSTIC, 2000);
    let winner = filter_for_model(&candidates, MODEL_AGNOSTIC).kept_upstream_ids[0];
    assert_eq!(dist.len(), 1);
    assert_eq!(dist[&winner], 2000);
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
    assert_eq!(trace.chosen_tier, cc_lb_domain::SubscriptionTier::KnownBase);
    assert_eq!(trace.candidates.len(), 2);
    for candidate in &trace.candidates {
        assert_eq!(candidate.tier, cc_lb_domain::SubscriptionTier::KnownBase);
        assert!(candidate.quota_urgency >= 0.0);
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
fn all_oauth_hard_negative_no_api_key_fails_closed_without_trace() {
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
        "fail-closed path must not attach a trace"
    );
}

// =============================================================================
// Session-affinity regressions for cost-first cache affinity.
// =============================================================================

#[test]
fn same_thread_id_without_cache_keeps_cost_first_winner_when_request_ids_vary() {
    // Given: four healthy live-snapshot upstreams and one non-empty thread_id
    // shared across 2000 turns with distinct request_ids.
    // When: none of the candidates has a priced live-cache value.
    // Then: same-session identity alone does not change the cost-first winner.
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
        assert!(output.subscription_preference.is_some());
        *winners
            .entry(*output.kept_upstream_ids.first().unwrap())
            .or_insert(0) += 1;
    }
    assert_eq!(winners.len(), 1);
}

#[test]
fn different_request_ids_keep_cost_first_winner() {
    // Given: same four healthy candidates as the golden distribution test.
    // When: each trial carries a distinct request_id (thread_id absent).
    // Then: cost-first selection stays deterministic across request IDs.
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
    assert_eq!(winners.len(), 1);
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
fn selection_counts(
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

fn ctx(canonical_model: &str) -> RoutingContext {
    ctx_with_request_id(canonical_model, "req")
}

fn ctx_with_request_id(canonical_model: &str, request_id: &str) -> RoutingContext {
    RoutingContext {
        request_id: request_id.to_owned(),
        thread_id: None,
        requested_service_tier: None,
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: canonical_model.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn ctx_with_thread_id(canonical_model: &str, request_id: &str, thread_id: &str) -> RoutingContext {
    RoutingContext {
        request_id: request_id.to_owned(),
        thread_id: Some(thread_id.to_owned()),
        requested_service_tier: None,
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: canonical_model.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn ctx_with_unknown_cache_pricing(canonical_model: &str, request_id: &str) -> RoutingContext {
    RoutingContext {
        cache_pricing: cc_lb_domain::CachePricingSummary::default(),
        ..ctx_with_request_id(canonical_model, request_id)
    }
}

fn test_cache_pricing() -> cc_lb_domain::CachePricingSummary {
    cc_lb_domain::CachePricingSummary {
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
/// `predicted_cache_read_tokens` to a candidate. Cache-factor tests vary this
/// value per candidate; the maximum positive cache value in the bucket
/// normalizes each exponential multiplier.
fn with_live_cache(mut candidate: UpstreamCandidate, read_tokens: u32) -> UpstreamCandidate {
    let cache_key = format!("v3-cache-{}", candidate.upstream_id);
    candidate.cache_score = Some(cc_lb_domain::CacheScore {
        predicted_cache_read_tokens: read_tokens,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: Some(0),
        confidence: 1.0,
        ambiguity_reason: None,
        matched_v3_cache_key: Some(cache_key),
        breakpoint_content_block_index: Some(0),
        matched_content_block_index: Some(0),
        lookback_distance: Some(0),
        token_estimate_source: Some("test".to_owned()),
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

fn absent(window: &str) -> SnapBuilder {
    SnapBuilder {
        inner: blank_snapshot(window, SubscriptionQuotaDataState::Absent),
    }
}

fn missing(window: &str) -> SnapBuilder {
    SnapBuilder {
        inner: blank_snapshot(window, SubscriptionQuotaDataState::Unobserved),
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
