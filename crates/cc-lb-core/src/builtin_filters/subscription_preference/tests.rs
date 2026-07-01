//! Tests for the tier-based subscription-preference filter.
//!
//! The test suite is organised around the algorithm's structure:
//!
//! - Section A — filter-level output (kept_upstream_ids + reason)
//! - Section B — tier selection (base > overage > probe > dead)
//! - Section C — base-window classification state machine
//! - Section D — overage assessment
//! - Section E — reset semantics
//! - Section F — intra-tier score correctness
//! - Section G — deterministic tie-break
//! - Section H — model-relevance (sonnet, opus, unknown windows)
//! - Section I — production regression: base plan > overage-rejected
//! - Section J — helpers and builders (bottom of file)

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

// =============================================================================
// Section A — filter-level output (kept_upstream_ids + reason)
// =============================================================================

#[test]
fn a1_keeps_subscription_when_base_is_healthy() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.3).status("allowed").build(),
        ],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth.clone(), api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn a2_keeps_api_key_when_every_oauth_is_dead() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let api_key = api_key("api-key", 2);
    let output = filter_for_model(&[oauth, api_key.clone()], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn a3_keeps_exhausted_oauth_when_no_api_key_exists() {
    let oauth = oauth_with(
        "oauth",
        1,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let output = filter_for_model(std::slice::from_ref(&oauth), MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, NO_API_KEY_REASON);
}

#[test]
fn a4_keeps_api_keys_when_no_subscription_candidates_exist() {
    let first = api_key("first", 1);
    let second = api_key("second", 2);
    let output = filter_for_model(&[first.clone(), second.clone()], MODEL_AGNOSTIC);

    assert_eq!(
        output.kept_upstream_ids,
        vec![first.upstream_id, second.upstream_id]
    );
    assert_eq!(output.reason, NO_SUBSCRIPTION_REASON);
}

#[test]
fn a5_empty_candidate_list_returns_no_subscription() {
    let output = filter_for_model(&[], MODEL_AGNOSTIC);
    assert!(output.kept_upstream_ids.is_empty());
    assert_eq!(output.reason, NO_SUBSCRIPTION_REASON);
}

#[test]
fn a6_only_one_upstream_id_returned_when_selecting_from_tier() {
    // Two OAuth candidates, one clearly better; one API-key. Exactly one OAuth
    // wins — never both.
    let better = oauth_with(
        "better",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.1).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let worse = oauth_with(
        "worse",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.6).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.7).status("allowed").build(),
        ],
    );
    let api_key = api_key("api-key", 3);
    let output = filter_for_model(&[better.clone(), worse, api_key], MODEL_AGNOSTIC);

    assert_eq!(output.kept_upstream_ids.len(), 1);
    assert_eq!(output.kept_upstream_ids, vec![better.upstream_id]);
}

// =============================================================================
// Section B — tier selection order
// =============================================================================

#[test]
fn b1_known_base_wins_over_partial_base() {
    let known = oauth_with(
        "known",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.4).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.4).status("allowed").build(),
        ],
    );
    let partial = oauth_with(
        "partial",
        2,
        vec![fresh(WINDOW_FIVE_HOUR).util(0.1).status("allowed").build()],
        // WINDOW_SEVEN_DAY missing → Unknown → partial-base tier
    );
    let output = filter_for_model(&[known.clone(), partial], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![known.upstream_id]);
}

#[test]
fn b2_partial_base_wins_over_overage_fallback() {
    // partial: 5h positive, 7d unknown (no snapshot). Overage snapshot rejected.
    let partial = oauth_with(
        "partial",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
        ],
    );
    // overage-only: both bases rejected, overage healthy.
    let overage_only = oauth_with(
        "overage-only",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );
    let output = filter_for_model(&[partial.clone(), overage_only], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![partial.upstream_id]);
}

#[test]
fn b3_overage_wins_over_unknown_probe() {
    // overage-only: base rejected, overage healthy.
    let overage_only = oauth_with(
        "overage-only",
        1,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );
    // probe: no base data at all.
    let probe = oauth_with("probe", 2, Vec::new());
    let output = filter_for_model(&[overage_only.clone(), probe], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![overage_only.upstream_id]);
}

#[test]
fn b4_unknown_probe_wins_over_dead() {
    let probe = oauth_with("probe", 1, Vec::new());
    let dead = oauth_with(
        "dead",
        2,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let output = filter_for_model(&[dead, probe.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![probe.upstream_id]);
}

#[test]
fn b5_dead_only_falls_back_to_api_key() {
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
fn b6_overage_in_use_flag_alone_proves_base_blocked() {
    // Base signals only have low utilization — no explicit rejection — but
    // unified reports overage_in_use=true. Filter should mark base as
    // proven blocked and require overage.ok to keep this candidate.
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
    let healthy = oauth_with(
        "healthy",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.4).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.4).status("allowed").build(),
        ],
    );
    let output = filter_for_model(&[ov_in_use, healthy.clone()], MODEL_AGNOSTIC);
    // healthy is KnownBase, ov_in_use is Overage → healthy wins.
    assert_eq!(output.kept_upstream_ids, vec![healthy.upstream_id]);
}

// =============================================================================
// Section C — base-window classification state machine
// =============================================================================

#[test]
fn c1_fresh_disabled_reason_is_hard_negative() {
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

#[test]
fn c2_fresh_allowed_warning_is_positive_but_penalised() {
    let warned = oauth_with(
        "warned",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .status("allowed_warning")
                .build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let clean = oauth_with(
        "clean",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let output = filter_for_model(&[warned, clean.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![clean.upstream_id]);
}

#[test]
fn c3_fresh_no_status_uses_utilization_gate() {
    // util=0.99 → still positive (below 1.0)
    let just_below = oauth_with(
        "just-below",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.99).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[just_below.clone(), key], MODEL_AGNOSTIC);
    // still alive
    assert_eq!(output.kept_upstream_ids, vec![just_below.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn c4_fresh_no_status_at_or_above_one_is_hard_negative() {
    let dead = oauth_with(
        "dead",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn c5_missing_or_absent_snapshot_is_unknown() {
    // Base windows entirely missing → probe tier.
    let probe = oauth_with(
        "probe",
        1,
        vec![
            missing(WINDOW_FIVE_HOUR).build(),
            missing(WINDOW_SEVEN_DAY).build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[probe.clone(), key], MODEL_AGNOSTIC);
    // Probe tier wins because it's non-dead.
    assert_eq!(output.kept_upstream_ids, vec![probe.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn c6_nan_utilization_is_unknown_not_positive() {
    // NaN util + no status + Fresh → Unknown (not Positive, not HardNegative).
    // With only 5h Unknown and 7d Fresh allowed → PartialBase.
    let partial = oauth_with(
        "partial",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util_raw(Some(f64::NAN)).build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let known = oauth_with(
        "known",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.5).status("allowed").build(),
        ],
    );
    let output = filter_for_model(&[partial, known.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![known.upstream_id]);
}

// =============================================================================
// Section D — overage assessment
// =============================================================================

#[test]
fn d1_fresh_overage_rejected_blocks_overage_bucket() {
    // Base rejected, overage rejected → dead → falls back to api key.
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
}

#[test]
fn d2_fallback_available_false_blocks_overage() {
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
    // fallback_available=false wins over overage_snap positive → overage blocked.
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn d3_extra_usage_credits_positive_keeps_overage_alive() {
    // Base rejected, no overage snapshot, but extra_usage credits > 0 on
    // unified → overage tier eligible.
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
fn d4_extra_usage_disabled_blocks_overage() {
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
    // extra_usage_enabled=false + hard_overage_block_wins → dead.
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn d5_stale_overage_rejected_does_not_block() {
    // Base rejected, overage snapshot is STALE + rejected → overage not
    // fresh-blocked. If overage has another positive signal (fallback
    // available on unified), overage tier is usable.
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
// Section E — reset semantics
// =============================================================================

#[test]
fn e1_stale_rejected_with_future_reset_still_blocks() {
    let observed_millis = T0_MILLIS;
    let reset_secs = T0_SECS + 3 * 24 * 3_600;
    let dead = UpstreamCandidate {
        observed_at_unix_secs: T0_SECS,
        ..oauth_with(
            "dead",
            1,
            vec![
                stale(WINDOW_SEVEN_DAY)
                    .util(1.0)
                    .status("rejected")
                    .reset_at(reset_secs)
                    .observed_millis(observed_millis)
                    .build(),
            ],
        )
    };
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn e2_stale_rejected_with_expired_reset_downgrades_to_unknown() {
    // observed candidate time is 10 days after T0; reset was 5 days after T0
    // → resets_at ≤ now → rejection expired → Unknown.
    let observed_millis = T0_MILLIS;
    let reset_secs = T0_SECS + 5 * 24 * 3_600;
    let candidate_observed_secs = T0_SECS + 10 * 24 * 3_600;
    let alive = UpstreamCandidate {
        observed_at_unix_secs: candidate_observed_secs,
        ..oauth_with(
            "alive",
            1,
            vec![
                stale(WINDOW_SEVEN_DAY)
                    .util(1.0)
                    .status("rejected")
                    .reset_at(reset_secs)
                    .observed_millis(observed_millis)
                    .build(),
            ],
        )
    };
    let alive_id = alive.upstream_id;
    let key = api_key("k", 2);
    let output = filter_for_model(&[alive, key], MODEL_AGNOSTIC);
    // Base is now unknown → probe tier keeps alive.
    assert_eq!(output.kept_upstream_ids, vec![alive_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn e3_stale_rejected_without_reset_still_blocks_by_default() {
    // No resets_at, stale rejected → hard_negative under default config.
    let dead = UpstreamCandidate {
        observed_at_unix_secs: T0_SECS,
        ..oauth_with(
            "dead",
            1,
            vec![stale(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
        )
    };
    let key = api_key("k", 2);
    let output = filter_for_model(&[dead, key.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn e4_imminent_reset_does_not_unblock_currently_rejected() {
    // A currently rejected candidate whose 7d resets in 30 seconds still
    // loses to a healthy alternative — reset-imminent is never an unblock.
    let reset_in_30s = T0_SECS + 30;
    let rejected_soon_reset = UpstreamCandidate {
        observed_at_unix_secs: T0_SECS,
        ..oauth_with(
            "reject-soon-reset",
            1,
            vec![
                fresh(WINDOW_SEVEN_DAY)
                    .util(1.0)
                    .status("rejected")
                    .reset_at(reset_in_30s)
                    .build(),
            ],
        )
    };
    let healthy = UpstreamCandidate {
        observed_at_unix_secs: T0_SECS,
        ..oauth_with(
            "healthy",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.5).status("allowed").build(),
            ],
        )
    };
    let output = filter_for_model(&[rejected_soon_reset, healthy.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![healthy.upstream_id]);
}

// =============================================================================
// Section F — intra-tier score correctness
// =============================================================================

#[test]
fn f1_base_tier_prefers_lower_base_utilization() {
    let low = oauth_with(
        "low",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.1).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
        ],
    );
    let high = oauth_with(
        "high",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.6).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.7).status("allowed").build(),
        ],
    );
    let output = filter_for_model(&[low.clone(), high], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![low.upstream_id]);
}

#[test]
fn f2_base_score_ignores_overage_signal() {
    // Both bases identical, one candidate has a very healthy overage snapshot,
    // the other has overage util=0.9. Overage MUST NOT influence base tier
    // ranking.
    let a = oauth_with(
        "a",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(0.05).status("allowed").build(),
        ],
    );
    let b = oauth_with(
        "b",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(0.9).status("allowed").build(),
        ],
    );
    // Bases tie; overage is invisible to base tier score → tiebreak decides.
    // Assert that the picked candidate is deterministic regardless of order.
    let picked_1 = filter_for_model(&[a.clone(), b.clone()], MODEL_AGNOSTIC).kept_upstream_ids;
    let picked_2 = filter_for_model(&[b, a], MODEL_AGNOSTIC).kept_upstream_ids;
    assert_eq!(picked_1, picked_2);
}

#[test]
fn f3_warning_status_loses_to_allowed_when_headroom_ties() {
    let warned = oauth_with(
        "warned",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.3)
                .status("allowed_warning")
                .build(),
            fresh(WINDOW_SEVEN_DAY).util(0.3).status("allowed").build(),
        ],
    );
    let clean = oauth_with(
        "clean",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.3).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.3).status("allowed").build(),
        ],
    );
    let output = filter_for_model(&[warned, clean.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![clean.upstream_id]);
}

// =============================================================================
// Section G — deterministic tie-break
// =============================================================================

#[test]
fn g1_selection_is_deterministic_across_input_permutations() {
    // Three healthy OAuths with identical utilization → intra-tier score
    // is identical → rendezvous hash decides. The winner must be the same
    // regardless of input order.
    let make = |name: &str, seed: u8| {
        oauth_with(
            name,
            seed,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.3).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.3).status("allowed").build(),
            ],
        )
    };
    let a = make("a", 1);
    let b = make("b", 2);
    let c = make("c", 3);

    let mut winners = Vec::new();
    for order in [
        [a.clone(), b.clone(), c.clone()],
        [a.clone(), c.clone(), b.clone()],
        [b.clone(), a.clone(), c.clone()],
        [b.clone(), c.clone(), a.clone()],
        [c.clone(), a.clone(), b.clone()],
        [c.clone(), b.clone(), a.clone()],
    ] {
        let output = filter_for_model(&order, MODEL_AGNOSTIC);
        winners.push(output.kept_upstream_ids);
    }
    let first = winners[0].clone();
    for w in &winners[1..] {
        assert_eq!(w, &first, "tiebreak must be permutation-invariant");
    }
}

#[test]
fn g2_upstream_id_breaks_final_tie() {
    // Same score, force a rendezvous collision by using an identical
    // request_id and comparing two candidates with different UUIDs.
    // Rendezvous hash still varies with UUID bytes, so this test asserts
    // determinism (not lex order) — the deterministic result must be stable.
    let low = oauth_with(
        "a",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.5).status("allowed").build(),
        ],
    );
    let high = oauth_with(
        "b",
        200,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.5).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.5).status("allowed").build(),
        ],
    );
    let winner_1 = filter_for_model(&[low.clone(), high.clone()], MODEL_AGNOSTIC)
        .kept_upstream_ids
        .into_iter()
        .next()
        .unwrap();
    let winner_2 = filter_for_model(&[high, low], MODEL_AGNOSTIC)
        .kept_upstream_ids
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(winner_1, winner_2, "same winner regardless of input order");
}

// =============================================================================
// Section H — model-relevance
// =============================================================================

#[test]
fn h1_sonnet_request_treats_seven_day_sonnet_as_relevant() {
    // Sonnet request, 7d_sonnet is Fresh + rejected → hard_negative.
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
fn h2_opus_request_ignores_seven_day_sonnet_signal() {
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
fn h3_seven_day_opus_window_is_never_counted() {
    // Even on an opus request the 7d_opus window is ignored — the base is
    // healthy on 5h+7d.
    let opus_alive = oauth_with(
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
    let output = filter_for_model(&[opus_alive.clone(), key], OPUS_MODEL);
    assert_eq!(output.kept_upstream_ids, vec![opus_alive.upstream_id]);
}

#[test]
fn h4_shared_seven_day_still_blocks_every_model() {
    for model in [SONNET_MODEL, OPUS_MODEL, HAIKU_MODEL, MODEL_AGNOSTIC] {
        let dead = oauth_with(
            "dead",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            ],
        );
        let key = api_key("k", 2);
        let output = filter_for_model(&[dead, key.clone()], model);
        assert_eq!(
            output.kept_upstream_ids,
            vec![key.upstream_id],
            "model={model} should treat 7d rejection as blocking"
        );
    }
}

#[test]
fn h5_unknown_window_labels_are_ignored() {
    // "unknown_bucket" is not a recognised base or overage label → ignored.
    let alive = oauth_with(
        "alive",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh("unknown_bucket").util(1.0).status("rejected").build(),
        ],
    );
    let key = api_key("k", 2);
    let output = filter_for_model(&[alive.clone(), key], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![alive.upstream_id]);
}

// =============================================================================
// Section I — production regression: base plan > overage-rejected
// =============================================================================
//
// Reproduces the exact production scenario that motivated the rewrite:
//
// - example-org: 7d api util=1.0 status=rejected (base truly exhausted)
// - Example Org: 7d api util=1.0 status=rejected (base truly exhausted)
// - example-peer: 7d util=0.47 allowed, overage header status=rejected
// - example-secondary-max: 7d util=0.35 allowed, overage header status=rejected
//
// The old algorithm treated overage-rejected as candidate-exhausted, marked
// every candidate dead, kept all 4, and let first-pick route to example-org
// (lowest UUID). The new algorithm must select from example-peer /
// example-secondary-max via base score.

#[test]
fn i1_production_bug_case_selects_from_base_healthy_candidates() {
    let example_org = oauth_with(
        "example-org",
        1,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let Example Org = oauth_with(
        "Example Org",
        2,
        vec![fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build()],
    );
    let example_peer = oauth_with(
        "example-peer",
        3,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.0).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.47).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
        ],
    );
    let example-secondary = oauth_with(
        "example-secondary-max",
        4,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.26).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.35).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
        ],
    );

    let output = filter_for_model(
        &[
            example_org.clone(),
            Example Org.clone(),
            example_peer.clone(),
            example-secondary.clone(),
        ],
        MODEL_AGNOSTIC,
    );

    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
    assert_eq!(output.kept_upstream_ids.len(), 1);
    let chosen = output.kept_upstream_ids[0];
    assert!(
        chosen == example_peer.upstream_id || chosen == example-secondary.upstream_id,
        "must pick from base-healthy candidates, got {chosen:?}"
    );
    assert_ne!(chosen, example_org.upstream_id);
    assert_ne!(chosen, Example Org.upstream_id);
}

#[test]
fn i2_production_bug_case_picks_lowest_base_utilization_at_tie() {
    // Both example-peer and example-secondary are KnownBase → intra-tier score by
    // headroom. example-secondary has 5h=0.26/7d=0.35 → min headroom = 0.65.
    // example-peer has 5h=0.0/7d=0.47 → min headroom = 0.53.
    // Score = min_headroom + positive_ratio(1.0) = 1.65 vs 1.53 → example-secondary wins.
    let example_peer = oauth_with(
        "example-peer",
        3,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.0).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.47).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
        ],
    );
    let example-secondary = oauth_with(
        "example-secondary-max",
        4,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.26).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.35).status("allowed").build(),
            fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
        ],
    );
    let output = filter_for_model(&[example_peer, example-secondary.clone()], MODEL_AGNOSTIC);
    assert_eq!(output.kept_upstream_ids, vec![example-secondary.upstream_id]);
}

#[test]
fn i3_all_bases_dead_but_one_has_healthy_overage_uses_that_one() {
    // If (for a different production shape) 3 of 4 have dead base + dead
    // overage, and 1 has dead base + healthy overage → that last one wins
    // via the overage tier.
    let base_and_overage_dead = |name: &str, seed: u8| {
        oauth_with(
            name,
            seed,
            vec![
                fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
                fresh(WINDOW_OVERAGE).util(1.0).status("rejected").build(),
            ],
        )
    };
    let dead_a = base_and_overage_dead("dead-a", 1);
    let dead_b = base_and_overage_dead("dead-b", 2);
    let dead_c = base_and_overage_dead("dead-c", 3);
    let overage_alive = oauth_with(
        "overage-alive",
        4,
        vec![
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE).util(0.3).status("allowed").build(),
        ],
    );

    let output = filter_for_model(
        &[dead_a, dead_b, dead_c, overage_alive.clone()],
        MODEL_AGNOSTIC,
    );
    assert_eq!(output.kept_upstream_ids, vec![overage_alive.upstream_id]);
}

// =============================================================================
// Section J — helpers and builders
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
    UpstreamCandidate {
        upstream_id: upstream_id(id_seed),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: quotas,
        observed_at_unix_secs: 0,
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
