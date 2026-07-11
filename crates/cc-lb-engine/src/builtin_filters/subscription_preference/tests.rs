//! Tests for the tier + weighted-rendezvous-hash subscription-preference filter.
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
//! - G — Capacity-ratio independence across pressure, weight, and distribution
//! - H — WRH selection: uniform fallback, single candidate, determinism, spread
//! - I — Anti-stampede distribution
//! - J — Live-snapshot regression (four-upstream production fixture)

use bytes::Bytes;
use cc_lb_plugin_api::{PrincipalKind, SubscriptionQuotaDataState};
use http::Method;
use std::collections::HashMap;

use super::*;

mod fable_pressure;
mod fable_salt;

const SONNET_MODEL: &str = "claude-sonnet-4-5-20250929";
const OPUS_MODEL: &str = "claude-opus-4-8-20250514";
const HAIKU_MODEL: &str = "claude-haiku-4-5-20251001";
const DATED_FABLE_LIKE_MODEL: &str = "claude-fable-5-20260701";
const FUTURE_FABLE_LIKE_MODEL: &str = "claude-fable-6";
const UNKNOWN_MODEL: &str = "claude-unknown-model";
const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";
const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";
const V10_RENDEZVOUS_SALT_ORACLE: &str =
    "cc-lb:subscription-preference:v10:symmetric-cache-value:2026-07-07";
const V11_RENDEZVOUS_SALT_ORACLE: &str =
    "cc-lb:subscription-preference:v11:use-it-or-lose-it-quota-pressure:2026-07-10";

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
    let example_org = oauth_at_t0(
        "example-org",
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
    let example_peer = oauth_at_t0(
        "example-peer",
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
    let dist = wrh_distribution(&[Example Org.clone(), bear.clone()], MODEL_AGNOSTIC, 2000);
    let runbear_share = *dist.get(&Example Org.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    let output = filter_for_model(&[Example Org.clone(), bear.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let runbear_pressure = candidate_urgency_for(&trace, Example Org.upstream_id).quota_urgency;
    let bear_pressure = candidate_urgency_for(&trace, bear.upstream_id).quota_urgency;
    assert!(runbear_pressure > bear_pressure);
    assert!(
        (0.65..=0.78).contains(&runbear_share),
        "ADR 0008 neutral factors should give Example Org a majority without starving bear; got share={runbear_share}"
    );
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
    let dist = wrh_distribution(
        &[tight_5h.clone(), loose_only.clone()],
        MODEL_AGNOSTIC,
        1000,
    );
    let tight_share = *dist.get(&tight_5h.upstream_id).unwrap_or(&0) as f64 / 1000.0;
    let output = filter_for_model(&[tight_5h.clone(), loose_only], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let tight_pressure = candidate_urgency_for(&trace, tight_5h.upstream_id).quota_urgency;
    assert!(tight_pressure > 0.0);
    assert!(
        (0.75..=0.90).contains(&tight_share),
        "the neutral baseline keeps the loose peer selectable while tight 5h pressure dominates; got share={tight_share}"
    );
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
    let dist = wrh_distribution(
        &[with_reset.clone(), no_reset.clone()],
        MODEL_AGNOSTIC,
        1000,
    );
    let with_reset_share = *dist.get(&with_reset.upstream_id).unwrap_or(&0) as f64 / 1000.0;
    let output = filter_for_model(&[with_reset.clone(), no_reset.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let with_reset_weight = candidate_urgency_for(&trace, with_reset.upstream_id);
    let no_reset_weight = candidate_urgency_for(&trace, no_reset.upstream_id);
    assert!(with_reset_weight.quota_urgency > 0.0);
    assert_eq!(no_reset_weight.quota_urgency, 0.0);
    assert_eq!(no_reset_weight.effective_weight, 1.0);
    assert!(
        (0.58..=0.72).contains(&with_reset_share),
        "with-reset pressure should bias WRH without zeroing the no-reset peer; got share={with_reset_share}"
    );
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
    let dist = wrh_distribution(
        &[low_util_long.clone(), high_util_short.clone()],
        MODEL_AGNOSTIC,
        2000,
    );
    let low_share = *dist.get(&low_util_long.upstream_id).unwrap_or(&0) as f64 / 2000.0;
    let output = filter_for_model(
        &[low_util_long.clone(), high_util_short.clone()],
        MODEL_AGNOSTIC,
    );
    let trace = output.subscription_preference.expect("trace present");
    let low_pressure = candidate_urgency_for(&trace, low_util_long.upstream_id).quota_urgency;
    let high_pressure = candidate_urgency_for(&trace, high_util_short.upstream_id).quota_urgency;
    assert!(high_pressure > low_pressure);
    assert!(
        (0.22..=0.36).contains(&low_share),
        "ADR 0008 should favor the short-reset candidate while the neutral baseline keeps both selectable; got share={low_share}"
    );
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
fn plan_capacity_ratio_does_not_change_base_effective_weight() {
    // Given: identical positive-pressure candidates with different plan ratios.
    let first = with_plan(
        oauth_at_t0(
            "first",
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
    let second = with_plan(
        oauth_at_t0(
            "second",
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

    // When: the filter composes quota, cache, and warning factors.
    let output = filter_for_model(&[first.clone(), second.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let first_weight = candidate_urgency_for(&trace, first.upstream_id);
    let second_weight = candidate_urgency_for(&trace, second.upstream_id);

    // Then: plan ratio changes neither the quota factor nor effective weight.
    assert!(!first_weight.quota_uniform_fallback);
    assert_eq!(
        first_weight.quota_weight_factor,
        second_weight.quota_weight_factor
    );
    assert_eq!(first_weight.cache_weight_multiplier, 1.0);
    assert_eq!(second_weight.cache_weight_multiplier, 1.0);
    assert_eq!(first_weight.warning_multiplier, 1.0);
    assert_eq!(second_weight.warning_multiplier, 1.0);
    assert_eq!(
        first_weight.effective_weight,
        second_weight.effective_weight
    );
}

#[test]
fn plan_capacity_ratio_does_not_change_base_distribution() {
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
    let original_distribution = wrh_distribution(&original_ratios, MODEL_AGNOSTIC, 2000);
    let swapped_distribution = wrh_distribution(&swapped_ratios, MODEL_AGNOSTIC, 2000);

    // Then: plan capacity metadata cannot change a single routing outcome.
    assert_eq!(
        original_distribution, swapped_distribution,
        "ADR 0008 distribution must be independent of plan_capacity_ratio"
    );
}

#[test]
fn plan_capacity_ratio_does_not_change_overage_weight_or_distribution() {
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
    let original_distribution = wrh_distribution(&original_ratios, MODEL_AGNOSTIC, 2000);
    let swapped_distribution = wrh_distribution(&swapped_ratios, MODEL_AGNOSTIC, 2000);

    // Then: capacity metadata affects neither v10 overage weight nor winners.
    assert_eq!(first_weight.quota_urgency, second_weight.quota_urgency);
    assert_eq!(
        first_weight.quota_weight_factor,
        second_weight.quota_weight_factor
    );
    assert_eq!(
        first_weight.effective_weight,
        second_weight.effective_weight
    );
    assert_eq!(
        original_distribution, swapped_distribution,
        "overage distribution must be independent of plan_capacity_ratio"
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
fn cache_hot_on_pace_effective_weight_is_finite() {
    // Given: an on-pace KnownBase bucket with one cache-hot candidate.
    let cached = with_live_cache(
        oauth_at_t0(
            "cached",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.0).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.0).status("allowed").build(),
            ],
        ),
        250_000,
    );
    let peer = oauth_at_t0(
        "peer",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.0).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.0).status("allowed").build(),
        ],
    );

    // When: the filter scores the uniform-pressure bucket.
    let output = filter_for_model(&[cached.clone(), peer], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let cached_weight = candidate_urgency_for(&trace, cached.upstream_id);

    // Then: the neutral factor keeps the cache multiplier finite and effective.
    assert_eq!(cached_weight.quota_urgency, 0.0);
    assert_eq!(cached_weight.quota_weight_factor, 1.0);
    assert!(cached_weight.effective_weight.is_finite());
    assert_eq!(
        cached_weight.effective_weight,
        cached_weight.quota_weight_factor
            * cached_weight.cache_weight_multiplier
            * cached_weight.warning_multiplier
    );
}

#[test]
fn base_uniform_quota_factor_is_one() {
    // Given: two on-pace base candidates whose pressure sum is below EPSILON.
    let candidates = vec![
        healthy_known_base_at_util("a", 1, 0.0),
        healthy_known_base_at_util("b", 2, 0.0),
    ];

    // When: the filter scores their bucket.
    let output = filter_for_model(&candidates, MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");

    // Then: effective weight exposes the neutral quota factor of 1.0.
    for candidate in &trace.candidates {
        assert_eq!(candidate.quota_urgency, 0.0);
        assert_eq!(candidate.quota_weight_factor, 1.0);
        assert!(candidate.quota_uniform_fallback);
        assert_eq!(candidate.cache_weight_multiplier, 1.0);
        assert_eq!(candidate.warning_multiplier, 1.0);
        assert_eq!(candidate.effective_weight, 1.0);
    }
}

#[test]
fn base_non_uniform_quota_factor_is_one_plus_pressure() {
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

    // Then: each base factor is its own 1 + combined pressure.
    for candidate in &trace.candidates {
        let expected = 1.0 + candidate.quota_urgency;
        assert!(
            (candidate.effective_weight - expected).abs() <= 1e-12,
            "effective weight {} must equal 1 + combined pressure {}",
            candidate.effective_weight,
            candidate.quota_urgency,
        );
    }
}

#[test]
fn warning_multiplier_is_point_two_in_mixed_pressure_bucket() {
    // Given: one warning-positive candidate in a non-uniform base bucket.
    let warning = oauth_at_t0(
        "warning",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.10)
                .status("allowed_warning")
                .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS / 2)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.10)
                .status("allowed")
                .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS / 2)
                .build(),
        ],
    );
    let peer = healthy_known_base_at_util("peer", 2, 0.90);

    // When: the filter scores their bucket.
    let output = filter_for_model(&[warning.clone(), peer], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let warning_weight = candidate_urgency_for(&trace, warning.upstream_id);

    // Then: warning remains a final 0.20 multiplier on the base quota factor.
    assert_eq!(warning_weight.warning_multiplier, WARNING_MULTIPLIER);
    let expected = (1.0 + warning_weight.quota_urgency) * WARNING_MULTIPLIER;
    assert!((warning_weight.effective_weight - expected).abs() <= 1e-12);
}

#[test]
fn warning_multiplier_is_point_two_in_uniform_pressure_bucket() {
    // Given: a warning-positive candidate in an all-on-pace base bucket.
    let warning = oauth_at_t0(
        "warning",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.0)
                .status("allowed_warning")
                .build(),
            fresh(WINDOW_SEVEN_DAY).util(0.0).status("allowed").build(),
        ],
    );
    let peer = oauth_at_t0(
        "peer",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.0).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.0).status("allowed").build(),
        ],
    );

    // When: the filter scores their uniform bucket.
    let output = filter_for_model(&[warning.clone(), peer], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let warning_weight = candidate_urgency_for(&trace, warning.upstream_id);

    // Then: the neutral quota factor still receives the unchanged 0.20 warning multiplier.
    assert_eq!(warning_weight.quota_urgency, 0.0);
    assert_eq!(warning_weight.warning_multiplier, WARNING_MULTIPLIER);
    assert_eq!(warning_weight.effective_weight, WARNING_MULTIPLIER);
}

#[test]
fn overage_non_uniform_weight_matches_v10() {
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
    let request_id = "overage-v10-non-uniform";

    // When: the filter scores the overage bucket.
    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(MODEL_AGNOSTIC, request_id),
            &principal(),
            &[low_util.clone(), high_util.clone()],
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");
    let low_expected = 0.5625 / 2_592_000.0;
    let high_expected = 0.0625 / 2_592_000.0;
    let low_score = -hash_to_open_unit(rendezvous_hash(
        V10_RENDEZVOUS_SALT_ORACLE,
        request_id,
        low_util.upstream_id,
    ))
    .ln()
        / low_expected;
    let high_score = -hash_to_open_unit(rendezvous_hash(
        V10_RENDEZVOUS_SALT_ORACLE,
        request_id,
        high_util.upstream_id,
    ))
    .ln()
        / high_expected;

    // Then: weights and winner are byte-for-byte v10 raw-urgency behavior.
    let low_trace = candidate_urgency_for(&trace, low_util.upstream_id);
    let high_trace = candidate_urgency_for(&trace, high_util.upstream_id);
    assert_eq!(low_trace.quota_weight_factor, low_expected);
    assert_eq!(low_trace.effective_weight, low_expected);
    assert_eq!(high_trace.quota_weight_factor, high_expected);
    assert_eq!(high_trace.effective_weight, high_expected);
    assert!(low_score < high_score, "v10 salt must select UUID seed 1");
    assert_eq!(output.kept_upstream_ids, vec![upstream_id(1)]);
}

#[test]
fn overage_uniform_fallback_matches_v10() {
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
    let request_id = "overage-v10-uniform";

    // When: the filter scores the overage bucket.
    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(MODEL_AGNOSTIC, request_id),
            &principal(),
            &[first.clone(), second.clone()],
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");
    let first_v10_hash = rendezvous_hash(V10_RENDEZVOUS_SALT_ORACLE, request_id, first.upstream_id);
    let second_v10_hash =
        rendezvous_hash(V10_RENDEZVOUS_SALT_ORACLE, request_id, second.upstream_id);
    let first_v11_hash = rendezvous_hash(V11_RENDEZVOUS_SALT_ORACLE, request_id, first.upstream_id);
    let second_v11_hash =
        rendezvous_hash(V11_RENDEZVOUS_SALT_ORACLE, request_id, second.upstream_id);

    // Then: v10 uniform fallback uses factor 1.0 and the raw hash tie-break outcome.
    assert_eq!(first.upstream_id, upstream_id(1));
    assert_eq!(second.upstream_id, upstream_id(2));
    assert!(
        second_v10_hash > first_v10_hash,
        "v10 salt must select UUID seed 2"
    );
    assert!(
        first_v11_hash > second_v11_hash,
        "v11 salt must select UUID seed 1 for this distinguishing fixture"
    );
    for candidate in &trace.candidates {
        assert!(candidate.quota_urgency < EPSILON);
        assert_eq!(candidate.quota_weight_factor, 1.0);
        assert_eq!(candidate.effective_weight, 1.0);
        assert!(candidate.quota_uniform_fallback);
    }
    assert_eq!(output.kept_upstream_ids, vec![upstream_id(2)]);
}

#[test]
fn unknown_probe_quota_factor_is_one() {
    // Given: two OAuth candidates with no usable quota snapshots.
    let candidates = vec![oauth_at_t0("a", 1, vec![]), oauth_at_t0("b", 2, vec![])];

    // When: the filter scores the UnknownProbe bucket.
    let output = filter_for_model(&candidates, MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");

    // Then: unknown probes always use the neutral factor 1.0.
    assert_eq!(
        trace.chosen_tier,
        cc_lb_plugin_api::SubscriptionTier::UnknownProbe
    );
    for candidate in &trace.candidates {
        assert_eq!(candidate.quota_urgency, 0.0);
        assert_eq!(candidate.effective_weight, 1.0);
    }
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
// algorithm funnelled 78% of traffic to example-org. The rewritten algorithm
// must produce the (much fairer) weighted-rendezvous distribution:
// Example Org ≈ 55%, example-peer ≈ 16%, example-secondary-max ≈ 15%, example-org ≈ 13%.

fn synthetic_now_secs() -> u64 {
    1_000_000
}

fn example_snapshot_example_org() -> UpstreamCandidate {
    let now = synthetic_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "example-org",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.10)
                    .status("allowed")
                    .reset_at(12_000)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.20)
                    .status("allowed")
                    .reset_at(450_000)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 20.0)
}

fn example_org_snapshot() -> UpstreamCandidate {
    let now = synthetic_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "example-org",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.15)
                    .status("allowed")
                    .reset_at(now + 9_000)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.40)
                    .status("allowed")
                    .reset_at(now + 450_000)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 20.0)
}

fn example_secondary_max_snapshot() -> UpstreamCandidate {
    let now = synthetic_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "example-secondary-max",
            3,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.00)
                    .status("allowed")
                    .reset_at(now + 12_000)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.25)
                    .status("allowed")
                    .reset_at(now + 60_000)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 5.0)
}

fn example_fourth_snapshot() -> UpstreamCandidate {
    let now = synthetic_now_secs();
    let candidate = UpstreamCandidate {
        observed_at_unix_secs: now,
        ..oauth_with(
            "example-fourth",
            4,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.20)
                    .status("allowed")
                    .reset_at(now + 2_000)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.15)
                    .status("allowed")
                    .reset_at(now + 360_000)
                    .build(),
                fresh(WINDOW_OVERAGE)
                    .util(0.15)
                    .status("allowed")
                    .extra_usage_enabled(true)
                    .extra_usage_limit(40_000.0)
                    .extra_usage_used(6_000.0)
                    .build(),
            ],
        )
    };
    with_plan(candidate, 5.0)
}

#[test]
fn example_snapshot_four_upstreams_produces_expected_wrh_distribution() {
    let candidates = vec![
        example_snapshot_example_org(),
        example_snapshot_example_peer(),
        example_snapshot_example_secondary_max(),
        example_snapshot_runbear(),
    ];
    let ids: HashMap<&str, Uuid> = candidates
        .iter()
        .map(|c| (c.name.as_str(), c.upstream_id))
        .collect();
    let bear_id = ids["example-org"];
    let isac_id = ids["example-peer"];
    let bh322_id = ids["example-secondary-max"];
    let runbear_id = ids["Example Org"];

    let dist = wrh_distribution(&candidates, MODEL_AGNOSTIC, 2000);
    let share = |id: Uuid| *dist.get(&id).unwrap_or(&0) as f64 / 2000.0;
    let runbear_share = share(runbear_id);
    let isac_share = share(isac_id);
    let bh322_share = share(bh322_id);
    let bear_share = share(bear_id);
    let trace = filter_for_model(&candidates, MODEL_AGNOSTIC)
        .subscription_preference
        .expect("trace present");
    let total_weight: f64 = trace
        .candidates
        .iter()
        .map(|candidate| candidate.effective_weight)
        .sum();
    for (name, id, observed_share) in [
        ("Example Org", runbear_id, runbear_share),
        ("example-peer", isac_id, isac_share),
        ("example-secondary-max", bh322_id, bh322_share),
        ("example-org", bear_id, bear_share),
    ] {
        let expected_share = candidate_urgency_for(&trace, id).effective_weight / total_weight;
        assert!(
            (observed_share - expected_share).abs() <= 0.10,
            "{name} share {observed_share} must track ADR 0008 WRH weight share {expected_share}"
        );
    }
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
        example_snapshot_example_org(),
        example_snapshot_example_peer(),
        example_snapshot_example_secondary_max(),
        example_snapshot_runbear(),
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
        example_snapshot_example_org(),
        example_snapshot_example_peer(),
        example_snapshot_example_secondary_max(),
        example_snapshot_runbear(),
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
    assert!((cached_urgency.cache_weight_multiplier - 1.0).abs() < WEIGHT_TOLERANCE);
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
/// `predicted_cache_read_tokens` to a candidate. Cache-factor tests vary this
/// value per candidate; the maximum positive cache value in the bucket
/// normalizes each exponential multiplier.
fn with_live_cache(mut candidate: UpstreamCandidate, read_tokens: u32) -> UpstreamCandidate {
    let cache_key = format!("v3-cache-{}", candidate.upstream_id);
    candidate.cache_score = Some(cc_lb_plugin_api::types::CacheScore {
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
fn wrh_key_source_is_cache_hash_when_candidate_has_positive_priced_cache_value() {
    // Given: a threaded request where at least one candidate has priced v3 cache value.
    let filter = SubscriptionPreferenceFilter::new();
    let warm = with_live_cache(healthy_oauth_candidate("upstream-a", 1), 500_000);
    let expected_cache_key = warm
        .cache_score
        .as_ref()
        .and_then(|score| score.matched_v3_cache_key.clone())
        .expect("v3 cache key");
    let candidates = vec![warm, healthy_oauth_candidate("upstream-b", 2)];

    // When: subscription-preference scores the cache-warm bucket.
    let ctx = ctx_with_thread_id(SONNET_MODEL, "req-1", "thread-A");
    let output = filter.filter(&ctx, &principal(), &candidates).unwrap();
    let trace = output
        .subscription_preference
        .expect("subscription-alive path must emit trace");

    // Then: cache-positive routing uses the matched v3 key, never the thread id.
    assert_eq!(
        trace.wrh_key_source,
        WrhKeySource::CacheHash,
        "cache-warm subscription routing should key on the matched v3 cache hash"
    );
    assert_eq!(
        trace.bucket_v3_cache_affinity_key.as_deref(),
        Some(expected_cache_key.as_str())
    );
    assert_eq!(
        trace.rendezvous_salt_version.as_deref(),
        Some(SALT_VERSION),
        "trace must stamp the current WRH salt version so post-hoc queries can \
         distinguish algorithm changes from state changes"
    );
}

#[test]
fn thread_id_does_not_change_cache_positive_routing_decision() {
    let filter = SubscriptionPreferenceFilter::new();
    let candidates = vec![
        with_live_cache(healthy_oauth_candidate("upstream-a", 1), 500_000),
        healthy_oauth_candidate("upstream-b", 2),
    ];
    let principal = principal();

    let first = filter
        .filter(
            &ctx_with_thread_id(SONNET_MODEL, "req-same", "thread-A"),
            &principal,
            &candidates,
        )
        .unwrap();
    let second = filter
        .filter(
            &ctx_with_thread_id(SONNET_MODEL, "req-same", "thread-B"),
            &principal,
            &candidates,
        )
        .unwrap();
    let first_trace = first.subscription_preference.expect("first trace present");
    let second_trace = second
        .subscription_preference
        .expect("second trace present");

    assert_eq!(first.kept_upstream_ids, second.kept_upstream_ids);
    assert_eq!(first_trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(second_trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(
        first_trace.bucket_v3_cache_affinity_key,
        second_trace.bucket_v3_cache_affinity_key
    );
    assert_eq!(
        first_trace.formula_winner_upstream_id,
        second_trace.formula_winner_upstream_id
    );
    assert_eq!(first_trace.kept_upstream_id, second_trace.kept_upstream_id);
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
    let oversized_thread_id = "session-".repeat(256);

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

    // Then: routing ignores thread_id size and uses bounded v3 cache keys.
    assert_eq!(first_trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(second_trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(
        first_trace.bucket_v3_cache_affinity_key,
        second_trace.bucket_v3_cache_affinity_key
    );
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
// Section L — ADR 0008 quota factor composed with the ADR 0004 cache multiplier.
// =============================================================================

use crate::builtin_filters::subscription_preference::CACHE_LOG_BOOST;

const WEIGHT_TOLERANCE: f64 = 1e-9;
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

fn expected_cache_weight_multiplier(cache_ratio: f64) -> f64 {
    (CACHE_LOG_BOOST * cache_ratio).exp()
}

#[test]
fn effective_weight_is_quota_factor_times_cache_and_warning_multipliers() {
    // Given: example-org util 0.30 with 250K cache; Example Org util 0.10 with 15K cache.
    // When: subscription filter scores the candidates.
    // Then: ADR 0008's neutral quota factor preserves the cache multiplier.
    let bear = with_live_cache(healthy_known_base_at_util("example-org", 1, 0.30), 250_000);
    let Example Org = with_live_cache(healthy_known_base_at_util("Example Org", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), Example Org.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let bear_urg = candidate_urgency_for(&trace, bear.upstream_id);
    let runbear_urg = candidate_urgency_for(&trace, Example Org.upstream_id);
    let bear_expected = bear_urg.quota_weight_factor
        * bear_urg.cache_weight_multiplier
        * bear_urg.warning_multiplier;
    let runbear_expected = runbear_urg.quota_weight_factor
        * runbear_urg.cache_weight_multiplier
        * runbear_urg.warning_multiplier;
    assert!(
        (bear_urg.cache_weight_multiplier - expected_cache_weight_multiplier(1.0)).abs()
            < WEIGHT_TOLERANCE
    );
    assert!(
        (runbear_urg.cache_weight_multiplier
            - expected_cache_weight_multiplier(15_000.0 / 250_000.0))
        .abs()
            < WEIGHT_TOLERANCE
    );
    assert!(
        (bear_urg.effective_weight - bear_expected).abs() < WEIGHT_TOLERANCE,
        "bear effective_weight {} vs expected {}",
        bear_urg.effective_weight,
        bear_expected,
    );
    assert!(
        (runbear_urg.effective_weight - runbear_expected).abs() < WEIGHT_TOLERANCE,
        "Example Org effective_weight {} vs expected {}",
        runbear_urg.effective_weight,
        runbear_expected,
    );
    assert!(
        bear_urg.effective_weight > runbear_urg.effective_weight * 1000.0,
        "cache-hot candidate must retain the larger composed effective weight"
    );
}

#[test]
fn on_pace_quota_factor_is_one_before_cache_multiplier() {
    // Given: two on-pace candidates with different cache values.
    let bear = with_live_cache(healthy_known_base_at_util("example-org", 1, 0.95), 250_000);
    let Example Org = with_live_cache(healthy_known_base_at_util("Example Org", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), Example Org.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let bear_urg = candidate_urgency_for(&trace, bear.upstream_id);
    let runbear_urg = candidate_urgency_for(&trace, Example Org.upstream_id);

    // Then: ADR 0008 supplies factor 1.0 before the independent cache multiplier.
    assert_eq!(bear_urg.quota_urgency, 0.0);
    assert_eq!(runbear_urg.quota_urgency, 0.0);
    assert_eq!(bear_urg.quota_weight_factor, 1.0);
    assert_eq!(runbear_urg.quota_weight_factor, 1.0);
    assert_eq!(bear_urg.effective_weight, bear_urg.cache_weight_multiplier);
    assert_eq!(
        runbear_urg.effective_weight,
        runbear_urg.cache_weight_multiplier
    );
}

#[test]
fn cache_multiplier_ratio_depends_on_cache_value_not_on_pace_utilization() {
    // Given: on-pace candidates at different utilization and cache ratios.
    let bear = with_live_cache(healthy_known_base_at_util("example-org", 1, 0.99), 250_000);
    let Example Org = with_live_cache(healthy_known_base_at_util("Example Org", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), Example Org.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let bear_urg = candidate_urgency_for(&trace, bear.upstream_id);
    let runbear_urg = candidate_urgency_for(&trace, Example Org.upstream_id);
    assert_eq!(bear_urg.quota_weight_factor, 1.0);
    assert_eq!(runbear_urg.quota_weight_factor, 1.0);
    let ratio = bear_urg.effective_weight / runbear_urg.effective_weight;
    assert!(
        (ratio - 8100.0).abs() < WEIGHT_TOLERANCE,
        "on-pace factor 1.0 must leave the cache multiplier ratio at 8100; got {ratio}",
    );
}

#[test]
fn warm_cache_holder_blocked_spills_to_fresh_quota_peer() {
    // Given: example-org carries deep cache but its 5h window is fresh + status=rejected
    //   (util 1.0), so tier assessment marks it HardNegative with no overage.
    //   Example Org is KnownBase with light cache.
    // When: subscription-preference filters.
    // Then: bear is not present in the KnownBase bucket; Example Org wins outright.
    let bear = with_live_cache(
        oauth_at_t0(
            "example-org",
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
    let Example Org = with_live_cache(healthy_known_base_at_util("Example Org", 2, 0.10), 15_000);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_request_id(MODEL_AGNOSTIC, "req-1");
    let out = filter
        .filter(&ctx, &principal(), &[bear.clone(), Example Org.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    assert!(
        trace
            .candidates
            .iter()
            .all(|c| c.upstream_id != bear.upstream_id),
        "hard-blocked example-org must be excluded from the assessed bucket"
    );
    assert_eq!(
        out.kept_upstream_ids,
        vec![Example Org.upstream_id],
        "with example-org hard-blocked, Example Org must be the sole winner even though its cache is shallow"
    );
}

#[test]
fn usage_warm_owner_wins_when_quota_factors_are_neutral() {
    // Given: an on-pace warm owner and peer, with strong provider usage lineage.
    let owner = with_live_cache(
        healthy_known_base_at_util("example-peer", 1, 0.20),
        590_000,
    );
    let quota_peer = healthy_known_base_at_util("example-org", 2, 0.10);
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_thread_id(MODEL_AGNOSTIC, "req-1", "ses-warm-owner");

    // When: subscription-preference scores the same warm thread.
    let out = filter
        .filter(&ctx, &principal(), &[owner.clone(), quota_peer.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let owner_urg = candidate_urgency_for(&trace, owner.upstream_id);
    let peer_urg = candidate_urgency_for(&trace, quota_peer.upstream_id);

    // Then: cache bonus multiplies the neutral quota factor instead of recreating
    // a hot Anthropic prompt cache elsewhere.
    assert_eq!(trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(owner_urg.quota_urgency, 0.0);
    assert_eq!(peer_urg.quota_urgency, 0.0);
    assert!(
        owner_urg.effective_weight > peer_urg.effective_weight,
        "warm owner effective weight must win when quota factors are neutral"
    );
    assert_eq!(out.kept_upstream_ids, vec![owner.upstream_id]);
}

#[test]
fn severe_quota_pressure_can_override_usage_cache_owner() {
    // Given: a cache owner is on pace while a near-reset peer risks wasting quota.
    let owner = with_live_cache(
        healthy_known_base_at_util("example-peer", 1, 0.9999),
        100_000,
    );
    let quota_peer = with_live_cache(
        oauth_at_t0(
            "example-org",
            3,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.0)
                    .status("allowed")
                    .reset_at(T0_SECS + 60)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.0)
                    .status("allowed")
                    .reset_at(T0_SECS + 60)
                    .build(),
            ],
        ),
        95_000,
    );
    let filter = SubscriptionPreferenceFilter::new();
    let ctx = ctx_with_thread_id(MODEL_AGNOSTIC, "req-1", "ses-severe-quota");

    // When: subscription-preference scores the same warm thread.
    let out = filter
        .filter(&ctx, &principal(), &[owner.clone(), quota_peer.clone()])
        .unwrap();
    let trace = out.subscription_preference.expect("trace present");
    let owner_urg = candidate_urgency_for(&trace, owner.upstream_id);
    let peer_urg = candidate_urgency_for(&trace, quota_peer.upstream_id);

    // Then: cache locality remains multiplicative, while severe positive pressure
    // can still select the use-it-or-lose-it peer.
    assert_eq!(trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(owner_urg.quota_urgency, 0.0);
    assert!(peer_urg.quota_urgency > 4.0);
    assert!(
        peer_urg.effective_weight > owner_urg.effective_weight,
        "fresh quota peer must beat a near-exhausted cache owner"
    );
    assert_eq!(out.kept_upstream_ids, vec![quota_peer.upstream_id]);
}

#[test]
fn all_cold_reduces_to_pure_quota_wrh_distribution() {
    // Without cache signal, ADR 0008 effective weight is exactly 1 + base pressure.
    let candidates = vec![
        example_snapshot_example_org(),
        example_snapshot_example_peer(),
        example_snapshot_example_secondary_max(),
        example_snapshot_runbear(),
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
            (candidate.cache_weight_multiplier - 1.0).abs() < WEIGHT_TOLERANCE,
            "cold candidate must have cache_weight_multiplier=1.0, got {}",
            candidate.cache_weight_multiplier,
        );
        assert!(
            (candidate.effective_weight - (1.0 + candidate.quota_urgency)).abs() < WEIGHT_TOLERANCE,
            "cold candidate effective_weight ({}) must equal 1 + quota_urgency ({})",
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
fn cache_multiplier_calibration_ratio_is_8100() {
    // Given: the retained ADR 0004 cache-ratio calibration points.
    let cache_hot_multiplier = expected_cache_weight_multiplier(1.0);
    let cache_peer_multiplier = expected_cache_weight_multiplier(15_000.0 / 250_000.0);

    // When: their current multiplicative ratio is evaluated without quota inputs.
    let multiplier_ratio = cache_hot_multiplier / cache_peer_multiplier;

    // Then: the cache factor alone retains the calibrated 8100 ratio.
    assert!(
        (multiplier_ratio - 8100.0).abs() < WEIGHT_TOLERANCE,
        "CACHE_LOG_BOOST={} must produce multiplier ratio 8100, got {} within {}",
        CACHE_LOG_BOOST,
        multiplier_ratio,
        WEIGHT_TOLERANCE,
    );
}
