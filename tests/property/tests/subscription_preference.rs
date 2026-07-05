#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use bytes::Bytes;
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_plugin_api::{
    FilterPlugin, Principal, PrincipalKind, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, UpstreamCandidate, UpstreamKind,
};
use http::Method;
use proptest::prelude::*;

const STRUCTURAL_CASES: u32 = 256;
const STATISTICAL_CASES: u32 = 1;
const SAMPLE_COUNT: u32 = 1024;
const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";
const WINDOW_FIVE_HOUR: &str = "5h";
const WINDOW_SEVEN_DAY: &str = "7d";
const T0_SECS: u64 = 1_700_000_000;
const REQUEST_ID_SEED: u64 = 0x9f7b_d241_a17c_38e9;
const MAX_WEIGHT: f64 = 100.0;

proptest! {
    #![proptest_config(structural_config())]

    #[test]
    fn single_candidate_winner(request_id in request_id_strategy(), util_basis in 0u32..=900) {
        let candidate = candidate_with_util("only", 1, scaled_util(util_basis));
        let output = SubscriptionPreferenceFilter::new()
            .filter(
                &make_context(&request_id, MODEL_AGNOSTIC),
                &make_principal(),
                std::slice::from_ref(&candidate),
            )
            .expect("builtin filter cannot fail");

        prop_assert_eq!(output.kept_upstream_ids, vec![candidate.upstream_id]);
    }
}

proptest! {
    #![proptest_config(structural_config())]

    #[test]
    fn deterministic_under_fixed_request_id(
        request_id in request_id_strategy(),
        first_util in 0u32..=900,
        second_util in 0u32..=900,
        third_util in 0u32..=900,
    ) {
        let filter = SubscriptionPreferenceFilter::new();
        let candidates = vec![
            candidate_with_util("a", 1, scaled_util(first_util)),
            candidate_with_util("b", 2, scaled_util(second_util)),
            candidate_with_util("c", 3, scaled_util(third_util)),
        ];
        let first = winner_bytes(&filter, &request_id, &candidates);

        for _ in 0..16 {
            prop_assert_eq!(winner_bytes(&filter, &request_id, &candidates), first);
        }
    }
}

proptest! {
    #![proptest_config(statistical_config())]

    #[test]
    fn uniform_fallback_when_total_urgency_is_zero(seed in Just(REQUEST_ID_SEED)) {
        let filter = SubscriptionPreferenceFilter::new();
        let candidates = vec![
            missing_candidate("a", 1),
            missing_candidate("b", 2),
            missing_candidate("c", 3),
            missing_candidate("d", 4),
        ];
        let mut state = seed;
        let mut winners = BTreeSet::new();

        for _ in 0..SAMPLE_COUNT {
            let request_id = next_request_id(&mut state);
            winners.insert(winner_bytes(&filter, &request_id, &candidates));
        }

        for candidate in &candidates {
            prop_assert!(
                winners.contains(&candidate.upstream_id.into_bytes()),
                "candidate {} was never selected",
                candidate.name,
            );
        }
    }
}

proptest! {
    #![proptest_config(structural_config())]

    #[test]
    fn equal_urgency_tiebreak_is_deterministic(request_id in request_id_strategy(), util_basis in 0u32..=900) {
        let filter = SubscriptionPreferenceFilter::new();
        let util = scaled_util(util_basis);
        let first_candidate = candidate_with_util("a", 1, util);
        let second_candidate = candidate_with_util("b", 2, util);
        let candidates = vec![first_candidate.clone(), second_candidate.clone()];
        let reversed = vec![second_candidate, first_candidate];
        let first = winner_bytes(&filter, &request_id, &candidates);

        for _ in 0..16 {
            prop_assert_eq!(winner_bytes(&filter, &request_id, &candidates), first);
        }
        prop_assert_eq!(winner_bytes(&filter, &request_id, &reversed), first);
    }
}

proptest! {
    #![proptest_config(statistical_config())]

    #[test]
    fn weight_proportional_distribution_at_1024_samples(
        first_weight in 1u32..=100,
        second_weight in 1u32..=100,
    ) {
        let filter = SubscriptionPreferenceFilter::new();
        let candidates = vec![
            candidate_with_headroom("a", 1, weight_headroom(first_weight)),
            candidate_with_headroom("b", 2, weight_headroom(second_weight)),
        ];
        let target = candidates[0].upstream_id.into_bytes();
        let mut state = REQUEST_ID_SEED;
        let mut wins = 0u32;

        for _ in 0..SAMPLE_COUNT {
            let request_id = next_request_id(&mut state);
            if winner_bytes(&filter, &request_id, &candidates) == target {
                wins += 1;
            }
        }

        let actual_share = f64::from(wins) / f64::from(SAMPLE_COUNT);
        let expected_share = f64::from(first_weight) / f64::from(first_weight + second_weight);
        prop_assert!(
            (actual_share - expected_share).abs() <= 0.10,
            "expected share {expected_share}, got {actual_share} for weights {first_weight}:{second_weight}",
        );
    }
}

proptest! {
    #![proptest_config(structural_config())]

    #[test]
    fn zero_remaining_time_windows_do_not_contribute(
        request_id in request_id_strategy(),
        first_util in 0u32..=900,
        second_util in 0u32..=900,
    ) {
        let filter = SubscriptionPreferenceFilter::new();
        let elapsed = vec![
            elapsed_candidate("a", 1, scaled_util(first_util)),
            elapsed_candidate("b", 2, scaled_util(second_util)),
        ];
        let missing_resets = vec![
            no_reset_candidate("a", 1, scaled_util(first_util)),
            no_reset_candidate("b", 2, scaled_util(second_util)),
        ];

        prop_assert_eq!(
            winner_bytes(&filter, &request_id, &elapsed),
            winner_bytes(&filter, &request_id, &missing_resets),
        );
    }
}

fn request_id_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-z0-9_-]{1,64}").expect("request id regex compiles")
}

fn structural_config() -> ProptestConfig {
    ProptestConfig {
        cases: STRUCTURAL_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn statistical_config() -> ProptestConfig {
    ProptestConfig {
        cases: STATISTICAL_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn scaled_util(value: u32) -> f64 {
    f64::from(value) / 1_000.0
}

fn weight_headroom(weight: u32) -> f64 {
    (f64::from(weight) / MAX_WEIGHT).sqrt()
}

fn candidate_with_util(name: &str, id_seed: u8, utilization: f64) -> UpstreamCandidate {
    oauth_candidate(
        name,
        id_seed,
        vec![
            fresh_allowed(WINDOW_FIVE_HOUR, utilization, Some(T0_SECS + 3_600)),
            fresh_allowed(WINDOW_SEVEN_DAY, utilization, Some(T0_SECS + 6 * 86_400)),
        ],
    )
}

fn candidate_with_headroom(name: &str, id_seed: u8, headroom: f64) -> UpstreamCandidate {
    let utilization = 1.0 - headroom;
    oauth_candidate(
        name,
        id_seed,
        vec![
            fresh_allowed(WINDOW_FIVE_HOUR, utilization, Some(T0_SECS + 60)),
            fresh_allowed(WINDOW_SEVEN_DAY, utilization, Some(T0_SECS + 60)),
        ],
    )
}

fn missing_candidate(name: &str, id_seed: u8) -> UpstreamCandidate {
    oauth_candidate(
        name,
        id_seed,
        vec![missing(WINDOW_FIVE_HOUR), missing(WINDOW_SEVEN_DAY)],
    )
}

fn elapsed_candidate(name: &str, id_seed: u8, utilization: f64) -> UpstreamCandidate {
    oauth_candidate(
        name,
        id_seed,
        vec![
            fresh_allowed(WINDOW_FIVE_HOUR, utilization, Some(T0_SECS)),
            fresh_allowed(WINDOW_SEVEN_DAY, utilization, Some(T0_SECS - 1)),
        ],
    )
}

fn no_reset_candidate(name: &str, id_seed: u8, utilization: f64) -> UpstreamCandidate {
    oauth_candidate(
        name,
        id_seed,
        vec![
            fresh_allowed(WINDOW_FIVE_HOUR, utilization, None),
            fresh_allowed(WINDOW_SEVEN_DAY, utilization, None),
        ],
    )
}

fn oauth_candidate(
    name: &str,
    id_seed: u8,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: upstream_id_string(id_seed)
            .parse()
            .expect("generated upstream id must be a valid UUID"),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: quotas,
        observed_at_unix_secs: T0_SECS,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }
}

fn upstream_id_string(seed: u8) -> String {
    format!("00000000-0000-0000-0000-0000000000{seed:02x}")
}

fn fresh_allowed(
    window: &str,
    utilization: f64,
    resets_at_unix_secs: Option<u64>,
) -> SubscriptionQuotaCandidateSnapshot {
    let mut snapshot = blank_snapshot(window, SubscriptionQuotaDataState::Fresh);
    snapshot.utilization = Some(utilization);
    snapshot.status = Some("allowed".to_owned());
    snapshot.resets_at_unix_secs = resets_at_unix_secs;
    snapshot
}

fn missing(window: &str) -> SubscriptionQuotaCandidateSnapshot {
    blank_snapshot(window, SubscriptionQuotaDataState::Missing)
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

fn winner_bytes(
    filter: &SubscriptionPreferenceFilter,
    request_id: &str,
    candidates: &[UpstreamCandidate],
) -> [u8; 16] {
    let output = filter
        .filter(
            &make_context(request_id, MODEL_AGNOSTIC),
            &make_principal(),
            candidates,
        )
        .expect("builtin filter cannot fail");
    output.kept_upstream_ids[0].into_bytes()
}

fn make_context(request_id: &str, model: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: model.to_owned(),
    }
}

fn make_principal() -> Principal {
    Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: Default::default(),
    }
}

fn next_request_id(state: &mut u64) -> String {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    format!("wrh-{:016x}", splitmix64(*state))
}

fn splitmix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
