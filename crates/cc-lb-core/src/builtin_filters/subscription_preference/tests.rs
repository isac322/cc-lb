use bytes::Bytes;
use cc_lb_plugin_api::{PrincipalKind, SubscriptionQuotaDataState};
use http::Method;

use super::*;

const SONNET_MODEL: &str = "claude-sonnet-4-5-20250929";
const OPUS_MODEL: &str = "claude-opus-4-8-20250514";
const HAIKU_MODEL: &str = "claude-haiku-4-5-20251001";
const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";

const T0_SECS: u64 = 1_700_000_000;
const T0_MILLIS: u64 = T0_SECS * 1_000;
const FIVE_HOUR_HORIZON_SECS: u64 = 2 * 3_600;
const LONG_HORIZON_SECS: u64 = 24 * 3_600;

// =============================================================================
// Section A — filter-level output (kept_upstream_ids + reason)
// =============================================================================

#[test]
fn keeps_subscription_when_quota_appears_alive() {
    let oauth = oauth_with("oauth", 1, vec![fresh(WINDOW_FIVE_HOUR).util(0.7).build()]);
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_api_key_when_subscription_quota_is_exhausted() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build()],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_api_key_when_one_window_is_exhausted_and_others_missing() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build(),
            missing(WINDOW_SEVEN_DAY).build(),
            missing(WINDOW_OVERAGE).build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn treats_unknown_subscription_quota_as_alive() {
    let oauth = oauth_with("oauth", 1, Vec::new());
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_exhausted_subscription_when_no_api_key_exists() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(1.0)
                .status("rejected")
                .disabled("quota exhausted")
                .build(),
        ],
    );
    let output = filter_for_model(std::slice::from_ref(&oauth), MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, NO_API_KEY_REASON);
}

#[test]
fn keeps_api_keys_when_no_subscription_candidates_exist() {
    let first = api_key("first", 1);
    let second = api_key("second", 2);
    let output = filter_for_model(&[first.clone(), second.clone()], MODEL_AGNOSTIC);

    assert_eq!(
        output.kept_upstream_ids,
        vec![first.upstream_id, second.upstream_id]
    );
    assert_eq!(output.reason, NO_SUBSCRIPTION_REASON);
}

// =============================================================================
// Section B — exhaustion gates per window class (regressions preserved)
// =============================================================================

#[test]
fn keeps_api_key_when_seven_day_window_is_fresh_exhausted_even_if_five_hour_is_alive() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_api_key_when_seven_day_window_is_stale_exhausted() {
    let observed_millis = T0_MILLIS;
    let reset_secs = T0_SECS + 3 * 24 * 3_600;
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .observed_millis(observed_millis)
                .build(),
            stale(WINDOW_SEVEN_DAY)
                .util(1.0)
                .status("rejected")
                .reset_at(reset_secs)
                .observed_millis(observed_millis)
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn ignores_stale_seven_day_exhaustion_when_reset_has_already_passed() {
    let observed_millis = T0_MILLIS;
    let reset_secs = T0_SECS + 5 * 24 * 3_600;
    let candidate_observed_secs = T0_SECS + 10 * 24 * 3_600;
    let oauth = UpstreamCandidate {
        observed_at_unix_secs: candidate_observed_secs,
        ..oauth_with(
            "oauth",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.2)
                    .observed_millis(observed_millis)
                    .build(),
                stale(WINDOW_SEVEN_DAY)
                    .util(1.0)
                    .status("rejected")
                    .reset_at(reset_secs)
                    .observed_millis(observed_millis)
                    .build(),
            ],
        )
    };
    let api_key = api_key("api-key", 2);
    let oauth_id = oauth.upstream_id;
    let output = filter_for_model(&[oauth, api_key], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_api_key_when_seven_day_sonnet_window_is_exhausted_for_sonnet_request() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.4).build(),
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(1.0)
                .status("exceeded")
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_subscription_when_seven_day_sonnet_is_exhausted_for_opus_request() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.4).build(),
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(1.0)
                .status("exceeded")
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], OPUS_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn ignores_seven_day_opus_window_even_for_opus_request() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.4).build(),
            fresh(WINDOW_SEVEN_DAY_OPUS)
                .util(1.0)
                .status("exceeded")
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], OPUS_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_api_key_when_shared_seven_day_is_exhausted_for_opus_request() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_SEVEN_DAY_OPUS).util(0.1).build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], OPUS_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_subscription_when_only_seven_day_sonnet_is_exhausted_for_haiku_request() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.4).build(),
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(1.0)
                .status("exceeded")
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], HAIKU_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn ignores_unknown_window_labels() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).build(),
            fresh("unknown_made_up_window")
                .util(1.0)
                .status("rejected")
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn five_hour_and_shared_seven_day_exhaustion_gates_routing_for_every_model() {
    for model in [SONNET_MODEL, OPUS_MODEL, HAIKU_MODEL] {
        for exhausted_window in [WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY] {
            let oauth = oauth_with(
                "oauth",
                1,
                vec![fresh(exhausted_window).util(1.0).status("rejected").build()],
            );
            let api_key = api_key("api-key", 2);
            let output = filter_for_model(&[oauth, api_key.clone()], model);

            assert_eq!(
                output.kept_upstream_ids,
                vec![api_key.upstream_id],
                "model={model} exhausted={exhausted_window} should drop OAuth"
            );
            assert_eq!(
                output.reason, API_KEY_FALLBACK_REASON,
                "model={model} exhausted={exhausted_window} expected API key fallback"
            );
        }
    }
}

#[test]
fn pre_filters_seven_day_exhaustion_independently_per_candidate() {
    let exhausted = oauth_with(
        "oauth-exhausted",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
        ],
    );
    let alive = oauth_with(
        "oauth-alive",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.5).build(),
        ],
    );
    let api_key = api_key("api-key", 3);
    let output = filter_for_model(&[exhausted, alive.clone(), api_key], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![alive.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

// =============================================================================
// Section C — algorithm spec scenarios (S1 .. S12)
// =============================================================================

#[test]
fn s1_load_balance_regime_picks_smaller_upstream_id_on_tie() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .reset_at(T0_SECS + 8 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .reset_at(T0_SECS + 8 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a.clone(), b], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn s2_near_lockout_with_imminent_reset_beats_low_utilization_candidate() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.95)
                .reset_at(T0_SECS + 30 * 60)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn s3_near_lockout_with_distant_reset_loses_to_imminent_reset_high_headroom() {
    // A: 5h=0.95 short-window drain, but reset is outside the 5h horizon
    //    → only the lockout-pressure credit fires.
    // B: 5h=0.2 with reset inside the 5h horizon → strong waste credit on
    //    the unused 80% of the window.
    // Spec says B wins because soon-wasted 80% beats slowly-stranded 5%.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.95)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .reset_at(T0_SECS + 30 * 60)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn s4_use_before_waste_prefers_imminent_reset_with_large_headroom() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.1)
                .reset_at(T0_SECS + 5 * 60)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.1)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn s5_sonnet_request_respects_per_model_seven_day_in_score() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(0.9)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(0.2)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], SONNET_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn s6_opus_request_ignores_seven_day_sonnet_in_score() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(0.9)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(0.2)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a.clone(), b], OPUS_MODEL);
    // Per-model sonnet window dropped → both u_max collapse to 0.3.
    // Smaller upstream_id wins tie-break.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn s7_stale_long_window_high_utilization_still_penalizes_in_score() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            stale(WINDOW_SEVEN_DAY)
                .util(0.95)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.4)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn s8_stale_long_window_with_passed_reset_falls_back_to_unknown_baseline() {
    let candidate_observed = T0_SECS + 10 * 24 * 3_600;
    let a = oauth_with_observed(
        "a",
        1,
        candidate_observed,
        vec![
            stale(WINDOW_SEVEN_DAY)
                .util(0.95)
                .reset_at(T0_SECS + 5 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.4)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    // A's stale long signal is discarded (passed reset) → A scores at
    // UNKNOWN_UTIL=0.5; B scores at 0.4; B wins.
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn s9_candidate_with_no_trusted_signal_loses_to_known_healthy() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            missing(WINDOW_FIVE_HOUR).build(),
            missing(WINDOW_SEVEN_DAY).build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.3)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn s10_missing_reset_time_collapses_to_load_balance_tiebreak() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn s11_bit_identical_candidates_break_tie_by_original_index() {
    let first = oauth_with_observed(
        "twin",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.3)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let second = UpstreamCandidate { ..first.clone() };

    let output = filter_for_model(&[first.clone(), second], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![first.upstream_id]);
}

#[test]
fn s12_equally_urgent_candidates_break_tie_by_upstream_id() {
    let snapshot = fresh(WINDOW_FIVE_HOUR)
        .util(0.99)
        .reset_at(T0_SECS + 60)
        .observed_millis(T0_MILLIS)
        .build();
    let a = oauth_with_observed("a", 1, T0_SECS, vec![snapshot.clone()]);
    let b = oauth_with_observed("b", 2, T0_SECS, vec![snapshot]);

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

// =============================================================================
// Section D — edge cases: malformed numerics, observation anchors, weird states
// =============================================================================

#[test]
fn nan_utilization_treated_as_no_signal() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util_raw(Some(f64::NAN))
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    // A has no trusted signal (NaN dropped) → u_max=0.5 unknown baseline.
    // B has trusted signal at 0.5. Tie on score collapses to
    // has_trusted_signal: true beats false → B wins.
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn negative_utilization_treated_as_no_signal() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util_raw(Some(-0.5))
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.4)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    // B's trusted 0.4 < A's UNKNOWN_UTIL 0.5 → B wins on raw score.
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn utilization_above_one_treated_as_exhaustion_on_fresh_short_window() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util_raw(Some(1.7))
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[a, api_key.clone()], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn utilization_above_one_on_stale_short_window_neither_scores_nor_exhausts() {
    // Stale 5h is not trusted for exhaustion or scoring; u=1.5 has no effect.
    // The candidate stays alive but contributes no score signal — defers
    // to UNKNOWN_UTIL=0.5. A separate trusted candidate at 0.4 wins.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            stale(WINDOW_FIVE_HOUR)
                .util_raw(Some(1.5))
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.4)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn allowed_warning_status_does_not_exhaust() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .status("allowed_warning")
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[a.clone(), api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn disabled_reason_on_fresh_short_window_exhausts() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.1)
                .disabled("operator disabled")
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[a, api_key.clone()], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
}

#[test]
fn utilization_exactly_zero_with_imminent_reset_creates_strong_waste_credit() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.0)
                .reset_at(T0_SECS + 60)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn candidate_with_observed_at_zero_uses_snapshot_observation() {
    let a = oauth_with_observed(
        "a",
        1,
        0,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.95)
                .reset_at(T0_SECS - 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        0,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.4)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    // A: snapshot observed_at = T0_SECS, resets at T0_SECS-3600 → reset
    // already passed. For a Fresh long window the score-trust rule is
    // strict (Fresh state alone), so utilization stays trusted but the
    // reset urgency contribution is zero. u_max(A)=0.95.
    // B: u_max=0.4. B wins.
    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn snapshot_observed_at_zero_uses_candidate_observed_at() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.4)
                .reset_at(T0_SECS - 10)
                .observed_millis(0)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![fresh(WINDOW_FIVE_HOUR).util(0.3).observed_millis(0).build()],
    );

    // Snapshot millis=0 is ignored, candidate observed_at wins.
    // A's reset is in the past relative to candidate observed_at → no urgency.
    // Raw u_max: A=0.4, B=0.3. B wins.
    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn empty_candidate_list_returns_no_subscription_kept_ids() {
    let output = filter_for_model(&[], MODEL_AGNOSTIC);
    assert!(output.kept_upstream_ids.is_empty());
    assert_eq!(output.reason, NO_SUBSCRIPTION_REASON);
}

#[test]
fn case_insensitive_model_match_for_sonnet_does_not_apply() {
    // model_is_sonnet uses simple substring contains — uppercase should NOT
    // match because the host normalises canonical model ids to lowercase.
    // We pin this behaviour so a future "Sonnet" id never silently flips
    // routing.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_SONNET)
                .util(1.0)
                .status("exceeded")
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(std::slice::from_ref(&a), "CLAUDE-SONNET-4-5");
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

// =============================================================================
// Section E — properties: determinism, totality, no-panic, subsumption
// =============================================================================

#[test]
fn selection_is_deterministic_across_input_permutations() {
    let snapshot_a = fresh(WINDOW_FIVE_HOUR)
        .util(0.3)
        .reset_at(T0_SECS + 4 * 3_600)
        .observed_millis(T0_MILLIS)
        .build();
    let snapshot_b = fresh(WINDOW_FIVE_HOUR)
        .util(0.4)
        .reset_at(T0_SECS + 4 * 3_600)
        .observed_millis(T0_MILLIS)
        .build();
    let snapshot_c = fresh(WINDOW_FIVE_HOUR)
        .util(0.5)
        .reset_at(T0_SECS + 4 * 3_600)
        .observed_millis(T0_MILLIS)
        .build();
    let a = oauth_with_observed("a", 1, T0_SECS, vec![snapshot_a]);
    let b = oauth_with_observed("b", 2, T0_SECS, vec![snapshot_b]);
    let c = oauth_with_observed("c", 3, T0_SECS, vec![snapshot_c]);

    let permutations = [
        vec![a.clone(), b.clone(), c.clone()],
        vec![a.clone(), c.clone(), b.clone()],
        vec![b.clone(), a.clone(), c.clone()],
        vec![b.clone(), c.clone(), a.clone()],
        vec![c.clone(), a.clone(), b.clone()],
        vec![c.clone(), b.clone(), a.clone()],
    ];

    for perm in &permutations {
        let output = filter_for_model(perm, MODEL_AGNOSTIC);
        assert_eq!(
            output.kept_upstream_ids,
            vec![a.upstream_id],
            "perm={:?} should always pick A (lowest u_max)",
            perm.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn selection_picks_exactly_one_oauth_when_any_alive() {
    let alive = (1..=5).map(|i| {
        oauth_with_observed(
            &format!("oauth-{i}"),
            i,
            T0_SECS,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.1 * f64::from(i))
                    .observed_millis(T0_MILLIS)
                    .build(),
            ],
        )
    });
    let mut candidates: Vec<UpstreamCandidate> = alive.collect();
    candidates.push(api_key("api-key", 99));

    let output = filter_for_model(&candidates, MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids.len(), 1);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn trusted_exhausted_candidate_never_wins_when_alive_exists() {
    let exhausted = oauth_with_observed(
        "dead",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(1.0)
                .status("rejected")
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let alive = oauth_with_observed(
        "alive",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.99)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[exhausted, alive.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![alive.upstream_id]);
}

#[test]
fn load_balance_subsumption_no_urgency_picks_lowest_u_max() {
    // Every candidate is below DRAIN_START AND every reset is outside its
    // urgency horizon → urgency=0 for all → algorithm reduces to the
    // existing min(max applicable utilization) policy.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.7)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.5)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let c = oauth_with_observed(
        "c",
        3,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.8)
                .reset_at(T0_SECS + 4 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone(), c], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn fresher_observation_breaks_tie_when_pressure_is_equal() {
    let snapshot = fresh(WINDOW_FIVE_HOUR).util(0.4).build();
    let stale_oauth = oauth_with_observed("stale", 1, 1_000, vec![snapshot.clone()]);
    let fresh_oauth = oauth_with_observed("fresh", 2, 2_000, vec![snapshot]);

    let output = filter_for_model(&[stale_oauth, fresh_oauth.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![fresh_oauth.upstream_id]);
}

// =============================================================================
// Section F — boundary behaviour of the scoring constants
// =============================================================================

#[test]
fn drain_pressure_is_zero_at_threshold_exactly() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.85)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.9)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn drain_pressure_kicks_in_just_above_threshold() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.90)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.99)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    // B has heavy drain credit on its near-lockout 5h that beats A's clean
    // base utilization 0.90. Without lockout pressure, A would win on
    // pure base score.
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn reset_urgency_is_zero_at_horizon_boundary() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.3)
                .reset_at(T0_SECS + FIVE_HOUR_HORIZON_SECS)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.4)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a.clone(), b], MODEL_AGNOSTIC);
    // Reset exactly on horizon → urgency=0 → load-balance regime →
    // lower u_max (A=0.3) wins.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn reset_urgency_is_positive_inside_horizon() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.4)
                .reset_at(T0_SECS + FIVE_HOUR_HORIZON_SECS - 1)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.4)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    // A has a (tiny) positive waste credit; B has none. A wins.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn five_hour_window_uses_two_hour_reset_horizon() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.1)
                .reset_at(T0_SECS + 3 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.3)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a.clone(), b], MODEL_AGNOSTIC);
    // 3h reset on 5h window is outside the 2h short-window horizon →
    // no urgency. A still wins on base because 0.1 < 0.3.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn long_window_uses_twenty_four_hour_reset_horizon() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.1)
                .reset_at(T0_SECS + 12 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.1)
                .reset_at(T0_SECS + 3 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    // A's reset (12h) is inside 24h horizon → waste credit fires.
    // B's reset (72h) is outside → no credit. A wins.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn long_window_just_outside_horizon_has_no_urgency() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.4)
                .reset_at(T0_SECS + LONG_HORIZON_SECS)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.5)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a.clone(), b], MODEL_AGNOSTIC);
    // Exactly on horizon → urgency=0 → A wins purely on lower base.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn urgency_credit_is_capped_at_maximum() {
    // Both candidates' raw urgency credits exceed MAX_URGENCY_CREDIT (A ≈ 2.33,
    // B ≈ 2.15), so the cap flattens both to 2.0. Lower base utilization then
    // breaks the score tie — without the cap A's deeper urgency would have
    // dragged its score below B's.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.99)
                .reset_at(T0_SECS + 10)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.98)
                .reset_at(T0_SECS + 10)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn long_window_does_not_receive_short_drain_credit() {
    // 7d near lockout but Fresh: spec says only 5h/overage/unified receive
    // SHORT_DRAIN_WEIGHT credit. Long-window scarcity is encoded as base
    // utilization only.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.97)
                .reset_at(T0_SECS + 6 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.5)
                .reset_at(T0_SECS + 6 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn overage_window_receives_short_drain_credit() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_OVERAGE)
                .util(0.96)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_OVERAGE)
                .util(0.5)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    // A's drain credit beats B's lower base — same shape as the 5h case.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn unified_window_receives_short_drain_credit() {
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_UNIFIED)
                .util(0.97)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_UNIFIED)
                .util(0.5)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn unknown_baseline_beats_near_lockout_known() {
    // S9 variant: known candidate is at u=0.99 with no urgency-eligible
    // reset → score ≈ 0.99. Unknown baseline candidate scores 0.5.
    let known = oauth_with_observed(
        "known",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.99)
                .reset_at(T0_SECS + 6 * 24 * 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let unknown = oauth_with_observed(
        "unknown",
        2,
        T0_SECS,
        vec![missing(WINDOW_FIVE_HOUR).build()],
    );

    let output = filter_for_model(&[known, unknown.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![unknown.upstream_id]);
}

// =============================================================================
// Section G — coverage gaps surfaced by spec review
// =============================================================================

#[test]
fn urgency_uses_max_window_credit_not_sum() {
    // A has two Fresh applicable windows each with a small drain credit.
    // If the implementation summed credits across windows, A's combined
    // urgency would outscore B's clean low utilization. Spec says max,
    // so B (with lower base and no urgency) must win.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.91)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_OVERAGE)
                .util(0.91)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.85)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn seven_day_opus_window_ignored_in_score_not_only_exhaustion() {
    // Opus request. A has very-low 7d_opus (would be attractive if scored)
    // but a higher shared 7d. B has the opposite. Spec says 7d_opus is
    // ignored entirely → B wins because its shared 7d=0.3 beats A's 0.6.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_OPUS)
                .util(0.1)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.6)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY_OPUS)
                .util(0.9)
                .observed_millis(T0_MILLIS)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.3)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], OPUS_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

#[test]
fn missing_snapshot_with_all_exhausted_fields_does_not_exhaust() {
    let oauth = oauth_with_observed(
        "oauth",
        1,
        T0_SECS,
        vec![SubscriptionQuotaCandidateSnapshot {
            window: WINDOW_SEVEN_DAY.to_owned(),
            state: SubscriptionQuotaDataState::Missing,
            source: None,
            utilization: Some(1.0),
            status: Some("rejected".to_owned()),
            resets_at_unix_secs: None,
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: Some("operator force-disabled".to_owned()),
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            observed_at_unix_millis: None,
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn stale_short_window_with_rejected_status_does_not_exhaust() {
    // Stale 5h is not trusted for exhaustion regardless of which exhaustion
    // indicator fires (utilization, status, disabled_reason). Pins
    // exhaustion property item 11 for the short-window stale path.
    let oauth = oauth_with_observed(
        "oauth",
        1,
        T0_SECS,
        vec![
            stale(WINDOW_FIVE_HOUR)
                .util(0.4)
                .status("rejected")
                .disabled("provider transient block")
                .reset_at(T0_SECS + 3_600)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn reset_drain_credit_applies_to_long_window_near_lockout() {
    // 7d windows do not receive short_drain credit, but they DO receive
    // reset_drain + waste credit. Verifies the long-window urgency path.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.95)
                .reset_at(T0_SECS + 60)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.7)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[b, a.clone()], MODEL_AGNOSTIC);
    // A's reset_drain (~0.4) pulls score below B's clean 0.7.
    assert_eq!(output.kept_upstream_ids, vec![a.upstream_id]);
}

#[test]
fn stale_long_window_without_reset_remains_trusted_for_scoring() {
    // Pin the spec ambiguity: stale long windows with NO reset timestamp
    // are trusted for scoring (and for exhaustion). The looser reading
    // wins over the literal "future-reset only" reading. If this contract
    // is ever tightened, this test must flip.
    let a = oauth_with_observed(
        "a",
        1,
        T0_SECS,
        vec![
            stale(WINDOW_SEVEN_DAY)
                .util(0.95)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );
    let b = oauth_with_observed(
        "b",
        2,
        T0_SECS,
        vec![
            fresh(WINDOW_SEVEN_DAY)
                .util(0.4)
                .observed_millis(T0_MILLIS)
                .build(),
        ],
    );

    let output = filter_for_model(&[a, b.clone()], MODEL_AGNOSTIC);
    // A's u_max=0.95 comes from the trusted stale snapshot; B's=0.4 wins.
    assert_eq!(output.kept_upstream_ids, vec![b.upstream_id]);
}

// =============================================================================
// Section H — helpers and builders
// =============================================================================

fn filter_for_model(candidates: &[UpstreamCandidate], canonical_model: &str) -> FilterOutput {
    SubscriptionPreferenceFilter::new()
        .filter(&ctx(canonical_model), &principal(), candidates)
        .expect("builtin filter cannot fail")
}

fn ctx(canonical_model: &str) -> RequestContext {
    RequestContext {
        request_id: "req".to_owned(),
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
    oauth_with_observed(name, id_seed, 0, quotas)
}

fn oauth_with_observed(
    name: &str,
    id_seed: u8,
    observed_at_unix_secs: u64,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: upstream_id(id_seed),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: quotas,
        observed_at_unix_secs,
        cache_score: None,
        base_url: None,
    }
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
    }
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

    fn util_raw(mut self, value: Option<f64>) -> Self {
        self.inner.utilization = value;
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

    fn observed_millis(mut self, t: u64) -> Self {
        self.inner.observed_at_unix_millis = Some(t);
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

fn missing(window: &str) -> SnapBuilder {
    let mut snapshot = blank_snapshot(window, SubscriptionQuotaDataState::Missing);
    snapshot.source = None;
    snapshot.observed_at_unix_millis = None;
    SnapBuilder { inner: snapshot }
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
