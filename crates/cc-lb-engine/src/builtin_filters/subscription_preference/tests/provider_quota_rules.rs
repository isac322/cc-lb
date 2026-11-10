use super::*;

// QA: provider window selection, all four tiers, hard negatives, absence vs
// unknown observations, stale reset semantics, neutral warnings, pressure and
// weekly pace gating, API-key fallback, pinned candidates, and failover parity.
const PROVIDERS: [(UpstreamKind, UpstreamKind, &str, [&str; 2]); 3] = [
    (
        UpstreamKind::OpenaiOauth,
        UpstreamKind::OpenaiApiKey,
        "gpt-5",
        [WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY],
    ),
    (
        UpstreamKind::GeminiOauth,
        UpstreamKind::GeminiApiKey,
        "gemini-2.5-pro",
        [WINDOW_GEMINI_FIVE_HOUR, WINDOW_GEMINI_SEVEN_DAY],
    ),
    (
        UpstreamKind::GeminiOauth,
        UpstreamKind::GeminiApiKey,
        "claude-sonnet-4",
        [WINDOW_THIRD_PARTY_FIVE_HOUR, WINDOW_THIRD_PARTY_SEVEN_DAY],
    ),
];

fn provider_oauth(
    id: u8,
    kind: UpstreamKind,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        kind,
        ..oauth_at_t0("provider", id, quotas)
    }
}

fn healthy_windows(windows: [&str; 2]) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        fresh(windows[0])
            .util(0.2)
            .status("allowed")
            .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
            .build(),
        fresh(windows[1])
            .util(0.2)
            .status("allowed")
            .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
            .build(),
    ]
}

#[test]
fn exhausted_oauth_loses_to_healthy_oauth_even_with_more_cache_reads() {
    for (oauth_kind, _, model, windows) in PROVIDERS {
        for negative in [
            fresh(windows[0]).util(1.0).status("allowed").build(),
            fresh(windows[0]).util(0.1).status("rejected").build(),
            fresh(windows[0])
                .util(0.1)
                .status("allowed")
                .disabled("disabled by provider")
                .build(),
        ] {
            let exhausted = with_live_cache(provider_oauth(1, oauth_kind, vec![negative]), 100_000);
            let healthy = provider_oauth(2, oauth_kind, healthy_windows(windows));
            let output = filter_for_model(&[exhausted.clone(), healthy.clone()], model);
            assert_eq!(output.kept_upstream_ids, vec![healthy.upstream_id]);
            let trace = output.subscription_preference.expect("tier trace");
            assert_eq!(trace.chosen_tier, cc_lb_domain::SubscriptionTier::KnownBase);
            assert_eq!(trace.candidates.len(), 1);
            assert!(subscription_hard_blocked(model, &exhausted));
            assert!(!subscription_hard_blocked(model, &healthy));
        }
    }
}

#[test]
fn all_oauth_exhausted_uses_api_key_or_fails_closed() {
    for (oauth_kind, key_kind, model, windows) in PROVIDERS {
        let first = provider_oauth(1, oauth_kind, vec![fresh(windows[0]).util(1.0).build()]);
        let second = provider_oauth(
            2,
            oauth_kind,
            vec![fresh(windows[1]).status("rejected").build()],
        );
        let key = other_provider("key", 3, key_kind);
        let output = filter_for_model(&[first.clone(), second.clone(), key.clone()], model);
        assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
        assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
        assert!(output.subscription_preference.is_none());

        let output = filter_for_model(&[first.clone(), second], model);
        assert!(output.kept_upstream_ids.is_empty());
        assert_eq!(output.reason, NO_API_KEY_REASON);

        let output = filter_for_model(std::slice::from_ref(&first), model);
        assert_eq!(output.kept_upstream_ids, vec![first.upstream_id]);
        assert_eq!(output.reason, SINGLE_CANDIDATE_PRESERVED_REASON);
    }
}

#[test]
fn provider_candidates_follow_all_four_tiers_before_api_key_fallback() {
    for (oauth_kind, key_kind, model, windows) in PROVIDERS {
        let known = provider_oauth(4, oauth_kind, healthy_windows(windows));
        let partial = provider_oauth(
            3,
            oauth_kind,
            vec![
                fresh(windows[0]).util(0.2).build(),
                missing(windows[1]).build(),
            ],
        );
        let overage = provider_oauth(
            2,
            oauth_kind,
            vec![
                fresh(windows[0]).util(1.0).build(),
                fresh(WINDOW_OVERAGE).status("allowed").util(0.2).build(),
            ],
        );
        let unknown = provider_oauth(1, oauth_kind, Vec::new());
        let key = other_provider("key", 5, key_kind);
        let mut candidates = vec![known, partial, overage, unknown, key];
        for tier in [
            cc_lb_domain::SubscriptionTier::KnownBase,
            cc_lb_domain::SubscriptionTier::PartialBase,
            cc_lb_domain::SubscriptionTier::Overage,
            cc_lb_domain::SubscriptionTier::UnknownProbe,
        ] {
            let output = filter_for_model(&candidates, model);
            assert_eq!(output.kept_upstream_ids, vec![candidates[0].upstream_id]);
            assert_eq!(
                output
                    .subscription_preference
                    .expect("tier trace")
                    .chosen_tier,
                tier
            );
            candidates.remove(0);
        }
    }
}

#[test]
fn openai_credits_keep_exhausted_base_candidate_in_overage_tier() {
    let credits = cc_lb_domain::SubscriptionQuotaCredits {
        has_credits: Some(true),
        unlimited: Some(false),
        limit_reached: Some(false),
        balance: Some("62500.0000000000".to_owned()),
        approx_local_messages: Some([10, 20]),
        approx_cloud_messages: None,
    };
    let candidate = provider_oauth(
        1,
        UpstreamKind::OpenaiOauth,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE)
                .fallback_available(true)
                .credits(credits)
                .build(),
        ],
    );
    let key = other_provider("key", 2, UpstreamKind::OpenaiApiKey);

    let output = filter_for_model(&[candidate.clone(), key], "gpt-5");

    assert_eq!(output.kept_upstream_ids, vec![candidate.upstream_id]);
    assert_eq!(
        output
            .subscription_preference
            .expect("subscription preference trace")
            .chosen_tier,
        cc_lb_domain::SubscriptionTier::Overage
    );
}

#[test]
fn openai_credit_limit_reached_still_blocks_overage() {
    let candidate = provider_oauth(
        1,
        UpstreamKind::OpenaiOauth,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE)
                .fallback_available(true)
                .credits(cc_lb_domain::SubscriptionQuotaCredits {
                    has_credits: Some(true),
                    unlimited: Some(true),
                    limit_reached: Some(true),
                    balance: None,
                    approx_local_messages: None,
                    approx_cloud_messages: None,
                })
                .build(),
        ],
    );
    let key = other_provider("key", 2, UpstreamKind::OpenaiApiKey);

    let output = filter_for_model(&[candidate, key.clone()], "gpt-5");

    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}

#[test]
fn codex_credit_flags_preserve_base_tiers_and_require_fresh_overage_availability() {
    for (has_credits, unlimited, expected_overage) in [
        (Some(true), Some(false), true),
        (Some(false), Some(true), true),
        (Some(false), Some(false), false),
        (None, None, false),
    ] {
        let credits = cc_lb_domain::SubscriptionQuotaCredits {
            has_credits,
            unlimited,
            limit_reached: None,
            balance: Some("62500.0000000000".to_owned()),
            approx_local_messages: None,
            approx_cloud_messages: None,
        };
        let mut credit_snapshot = fresh(WINDOW_OVERAGE).credits(credits.clone()).build();
        credit_snapshot.fallback_available = credits.fallback_available();
        let key = other_provider("key", 2, UpstreamKind::OpenaiApiKey);
        let mut healthy = healthy_windows([WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY]);
        healthy.push(credit_snapshot.clone());
        let base = provider_oauth(1, UpstreamKind::OpenaiOauth, healthy);
        let output = filter_for_model(&[base.clone(), key.clone()], "gpt-5");
        assert_eq!(output.kept_upstream_ids, vec![base.upstream_id]);
        assert_eq!(
            output.subscription_preference.unwrap().chosen_tier,
            cc_lb_domain::SubscriptionTier::KnownBase
        );

        let mut exhausted = provider_oauth(
            1,
            UpstreamKind::OpenaiOauth,
            vec![
                absent(WINDOW_FIVE_HOUR).build(),
                fresh(WINDOW_SEVEN_DAY).util(1.0).build(),
                credit_snapshot,
            ],
        );
        let output = filter_for_model(&[exhausted.clone(), key.clone()], "gpt-5");
        assert_eq!(
            output.kept_upstream_ids,
            vec![if expected_overage {
                exhausted.upstream_id
            } else {
                key.upstream_id
            }],
            "has_credits={has_credits:?}, unlimited={unlimited:?}"
        );
        if expected_overage {
            assert_eq!(
                output.subscription_preference.unwrap().chosen_tier,
                cc_lb_domain::SubscriptionTier::Overage
            );
        }
        exhausted.subscription_quotas[2].state = SubscriptionQuotaDataState::Stale;
        let output = filter_for_model(&[exhausted.clone(), key], "gpt-5");
        assert_eq!(output.kept_upstream_ids, vec![upstream_id(2)]);
        let pinned = filter_for_model(std::slice::from_ref(&exhausted), "gpt-5");
        assert_eq!(pinned.kept_upstream_ids, vec![exhausted.upstream_id]);
        assert_eq!(pinned.reason, SINGLE_CANDIDATE_PRESERVED_REASON);
    }
}

#[test]
fn openai_credit_header_does_not_clear_usage_poll_limit_restriction() {
    let candidate = provider_oauth(
        1,
        UpstreamKind::OpenaiOauth,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(1.0).status("rejected").build(),
            fresh(WINDOW_SEVEN_DAY).util(1.0).status("rejected").build(),
            fresh(WINDOW_OVERAGE)
                .fallback_available(false)
                .credits(cc_lb_domain::SubscriptionQuotaCredits {
                    has_credits: Some(true),
                    unlimited: Some(false),
                    limit_reached: Some(true),
                    balance: None,
                    approx_local_messages: None,
                    approx_cloud_messages: None,
                })
                .build(),
            fresh(WINDOW_UNIFIED)
                .fallback_available(true)
                .credits(cc_lb_domain::SubscriptionQuotaCredits {
                    has_credits: Some(true),
                    unlimited: Some(false),
                    limit_reached: None,
                    balance: Some("62500.0000000000".to_owned()),
                    approx_local_messages: None,
                    approx_cloud_messages: None,
                })
                .build(),
        ],
    );
    let key = other_provider("key", 2, UpstreamKind::OpenaiApiKey);

    let output = filter_for_model(&[candidate, key.clone()], "gpt-5");

    assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
}
#[test]
fn openai_pro_absent_five_hour_is_known_base_but_unobserved_is_partial() {
    let weekly = fresh(WINDOW_SEVEN_DAY)
        .util(0.25)
        .status("allowed")
        .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
        .build();
    for (five_hour, tier) in [
        (
            absent(WINDOW_FIVE_HOUR).build(),
            cc_lb_domain::SubscriptionTier::KnownBase,
        ),
        (
            missing(WINDOW_FIVE_HOUR).build(),
            cc_lb_domain::SubscriptionTier::PartialBase,
        ),
    ] {
        let pro = provider_oauth(
            1,
            UpstreamKind::OpenaiOauth,
            vec![five_hour, weekly.clone()],
        );
        let key = other_provider("key", 2, UpstreamKind::OpenaiApiKey);
        let output = filter_for_model(&[pro.clone(), key], "gpt-5-pro");
        assert_eq!(output.kept_upstream_ids, vec![pro.upstream_id]);
        let trace = output.subscription_preference.expect("Pro tier trace");
        assert_eq!(trace.chosen_tier, tier);
        assert_eq!(trace.candidates[0].quota_urgency_5h, Some(0.0));
    }
}

#[test]
fn gemini_model_family_ignores_the_other_familys_exhaustion() {
    for (model, selected, other) in [
        (
            "gemini-2.5-pro",
            GEMINI_BASE_WINDOWS,
            THIRD_PARTY_BASE_WINDOWS,
        ),
        (
            "claude-sonnet-4",
            THIRD_PARTY_BASE_WINDOWS,
            GEMINI_BASE_WINDOWS,
        ),
        (
            "claude-fable-5",
            THIRD_PARTY_BASE_WINDOWS,
            GEMINI_BASE_WINDOWS,
        ),
        (
            "gpt-oss-120b",
            THIRD_PARTY_BASE_WINDOWS,
            GEMINI_BASE_WINDOWS,
        ),
    ] {
        let mut quotas = healthy_windows(selected);
        quotas.extend(other.map(|window| fresh(window).util(1.0).status("rejected").build()));
        let oauth = provider_oauth(1, UpstreamKind::GeminiOauth, quotas);
        let key = other_provider("key", 2, UpstreamKind::GeminiApiKey);
        let output = filter_for_model(&[oauth.clone(), key], model);
        assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
        assert_eq!(
            output
                .subscription_preference
                .expect("selected-family trace")
                .chosen_tier,
            cc_lb_domain::SubscriptionTier::KnownBase
        );
        assert!(!subscription_hard_blocked(model, &oauth));
    }
}

#[test]
fn selected_gemini_family_exhaustion_falls_back_despite_other_family_headroom() {
    for (model, selected, other) in [
        (
            "gemini-2.5-pro",
            GEMINI_BASE_WINDOWS,
            THIRD_PARTY_BASE_WINDOWS,
        ),
        (
            "claude-sonnet-4",
            THIRD_PARTY_BASE_WINDOWS,
            GEMINI_BASE_WINDOWS,
        ),
        (
            "gpt-oss-120b",
            THIRD_PARTY_BASE_WINDOWS,
            GEMINI_BASE_WINDOWS,
        ),
    ] {
        let mut quotas = healthy_windows(other);
        quotas.extend(selected.map(|window| fresh(window).util(1.0).status("rejected").build()));
        let oauth = provider_oauth(1, UpstreamKind::GeminiOauth, quotas);
        let key = other_provider("key", 2, UpstreamKind::GeminiApiKey);
        let output = filter_for_model(&[oauth.clone(), key.clone()], model);
        assert_eq!(output.kept_upstream_ids, vec![key.upstream_id]);
        assert!(subscription_hard_blocked(model, &oauth));
    }
}

#[test]
fn unknown_gemini_model_family_treats_both_families_as_unknown() {
    for utilization in [0.1, 1.0] {
        let quotas = GEMINI_BASE_WINDOWS
            .into_iter()
            .chain(THIRD_PARTY_BASE_WINDOWS)
            .map(|window| fresh(window).util(utilization).build())
            .collect();
        let oauth = provider_oauth(1, UpstreamKind::GeminiOauth, quotas);
        let key = other_provider("key", 2, UpstreamKind::GeminiApiKey);
        let output = filter_for_model(&[oauth.clone(), key], "unidentified-model");
        assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
        assert_eq!(
            output
                .subscription_preference
                .expect("unknown-family trace")
                .chosen_tier,
            cc_lb_domain::SubscriptionTier::UnknownProbe
        );
        assert!(!subscription_hard_blocked("unidentified-model", &oauth));
    }
}

#[test]
fn provider_stale_observations_use_existing_reset_semantics() {
    for (oauth_kind, key_kind, model, windows) in PROVIDERS {
        for (snapshot, blocked) in [
            (stale(windows[1]).util(0.2).status("allowed").build(), false),
            (
                stale(windows[1]).util(1.0).reset_at(T0_SECS + 1).build(),
                true,
            ),
            (
                stale(windows[1])
                    .status("rejected")
                    .reset_at(T0_SECS + 1)
                    .build(),
                true,
            ),
            (
                stale(windows[1])
                    .disabled("disabled")
                    .reset_at(T0_SECS + 1)
                    .build(),
                true,
            ),
            (stale(windows[1]).status("rejected").build(), true),
            (
                stale(windows[1])
                    .util(1.0)
                    .status("rejected")
                    .reset_at(T0_SECS)
                    .build(),
                false,
            ),
        ] {
            let oauth = provider_oauth(1, oauth_kind, vec![snapshot]);
            let key = other_provider("key", 2, key_kind);
            let output = filter_for_model(&[oauth.clone(), key.clone()], model);
            assert_eq!(
                output.kept_upstream_ids,
                vec![if blocked {
                    key.upstream_id
                } else {
                    oauth.upstream_id
                }]
            );
            assert_eq!(subscription_hard_blocked(model, &oauth), blocked);
            if !blocked {
                let trace = output.subscription_preference.expect("unknown stale trace");
                assert_eq!(
                    trace.chosen_tier,
                    cc_lb_domain::SubscriptionTier::UnknownProbe
                );
                assert_eq!(trace.candidates[0].quota_urgency, 0.0);
            }
        }
    }
}

#[test]
fn provider_base_warnings_are_ranking_neutral() {
    for (oauth_kind, _, model, windows) in PROVIDERS {
        let allowed = provider_oauth(1, oauth_kind, healthy_windows(windows));
        let peer = provider_oauth(2, oauth_kind, healthy_windows(windows));
        let baseline = filter_for_model(&[peer.clone(), allowed.clone()], model);
        for threshold_warning in [false, true] {
            let mut warning = allowed.clone();
            if threshold_warning {
                warning.subscription_quotas[0].surpassed_threshold = Some(0.1);
            } else {
                warning.subscription_quotas[0].status = Some("allowed_warning".to_owned());
            }
            let output = filter_for_model(&[peer.clone(), warning], model);
            assert_eq!(output.kept_upstream_ids, baseline.kept_upstream_ids);
            assert_eq!(
                output.subscription_preference,
                baseline.subscription_preference
            );
        }
    }
}

#[test]
fn provider_pressure_and_weekly_pace_match_anthropic_numerically() {
    // With weekly quota behind pace, five-hour pressure selects the imminent
    // refill. With weekly quota ahead, that pressure is suppressed as before.
    for weekly_utilization in [0.0, 0.9] {
        let urgent = oauth_at_t0(
            "urgent",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.0)
                    .status("allowed")
                    .reset_at(T0_SECS + 600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(weekly_utilization)
                    .status("allowed")
                    .reset_at(T0_SECS + 2 * 86_400)
                    .build(),
            ],
        );
        let peer = healthy_oauth_candidate("peer", 1);
        let baseline = filter_for_model(&[peer.clone(), urgent.clone()], MODEL_AGNOSTIC);
        let trace = baseline
            .subscription_preference
            .as_ref()
            .expect("Anthropic trace");
        let urgent_pressure = candidate_urgency_for(trace, urgent.upstream_id)
            .quota_urgency_5h
            .unwrap();
        assert_eq!(urgent_pressure > 0.0, weekly_utilization == 0.0);
        for (oauth_kind, _, model, windows) in PROVIDERS {
            let convert = |mut candidate: UpstreamCandidate| {
                candidate.kind = oauth_kind;
                candidate.subscription_quotas[0].window = windows[0].to_owned();
                candidate.subscription_quotas[1].window = windows[1].to_owned();
                candidate
            };
            let output = filter_for_model(&[convert(peer.clone()), convert(urgent.clone())], model);
            assert_eq!(output.kept_upstream_ids, baseline.kept_upstream_ids);
            assert_eq!(
                output.subscription_preference,
                baseline.subscription_preference
            );
        }
    }
}
