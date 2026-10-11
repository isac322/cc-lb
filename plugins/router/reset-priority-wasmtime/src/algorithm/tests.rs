// Test code is allowed the panicking conveniences the library itself is not:
// an `expect` or an index here fails one test with a message, whereas in the
// guest it would trap the plugin mid-request.
#![allow(
    clippy::wildcard_imports,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
use super::*;

const NOW: u64 = 1_700_000_000;
const HOUR: u64 = 3_600;
const DAY: u64 = 24 * HOUR;

fn window(state: DataState, utilization: f64, resets_in_secs: u64) -> QuotaWindow {
    QuotaWindow {
        state,
        utilization: Some(utilization),
        resets_at_unix_secs: Some(NOW + resets_in_secs),
        observed_at_unix_secs: Some(NOW),
        rejected: false,
    }
}

fn rejected_window(state: DataState, utilization: f64, resets_in_secs: u64) -> QuotaWindow {
    QuotaWindow {
        rejected: true,
        ..window(state, utilization, resets_in_secs)
    }
}

fn candidate<'a>(
    upstream_id: &'a str,
    kind: &'a str,
    quotas: CandidateQuotas<'a>,
    predicted_cache_read_tokens: u32,
) -> CandidateView<'a> {
    CandidateView {
        upstream_id,
        kind,
        observed_at_unix_secs: NOW,
        predicted_cache_read_tokens,
        quotas,
    }
}

fn oauth<'a>(upstream_id: &'a str, quotas: CandidateQuotas<'a>) -> CandidateView<'a> {
    candidate(upstream_id, KIND_ANTHROPIC_OAUTH, quotas, 0)
}

fn decide_for<'a>(thread_id: Option<&'a str>, candidates: &'a [CandidateView<'a>]) -> Decision {
    decide(&RequestView {
        thread_id,
        now_unix_secs: NOW,
        candidates,
    })
}

fn accepted_ids(decision: &Decision) -> Vec<&str> {
    decision.accepted.iter().map(String::as_str).collect()
}

fn reason_for<'a>(decision: &'a Decision, upstream_id: &str) -> &'a str {
    decision
        .per_candidate
        .iter()
        .find(|(id, _, _)| id == upstream_id)
        .map(|(_, _, reason)| reason.as_str())
        .expect("candidate present")
}

// -- The rule from the video -------------------------------------------------

#[test]
fn earliest_deadline_with_quota_left_is_burned_first() {
    // Account A resets tomorrow and still has 20% left; account B resets in four
    // days with 80% left. The video's rule: burn A.
    let a_five = window(DataState::Fresh, 0.1, 4 * HOUR);
    let a_week = window(DataState::Fresh, 0.8, DAY);
    let b_five = window(DataState::Fresh, 0.1, 4 * HOUR);
    let b_week = window(DataState::Fresh, 0.2, 4 * DAY);
    let candidates = [
        oauth(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            CandidateQuotas {
                five_hour: Some(&a_five),
                shared_weekly: Some(&a_week),
                model_weekly: None,
            },
        ),
        oauth(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            CandidateQuotas {
                five_hour: Some(&b_five),
                shared_weekly: Some(&b_week),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(
        accepted_ids(&decision),
        vec!["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]
    );
    assert!(
        reason_for(&decision, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
            .starts_with("reject:lower_priority_or_affinity")
    );
}

#[test]
fn even_spread_is_not_used_when_deadlines_differ() {
    // Both accounts are untouched, but one resets far sooner. A round-robin
    // router would alternate; this one must not.
    let near = window(DataState::Fresh, 0.0, 2 * DAY);
    let far = window(DataState::Fresh, 0.0, 6 * DAY);
    let five_near = window(DataState::Fresh, 0.0, 4 * HOUR);
    let five_far = window(DataState::Fresh, 0.0, 4 * HOUR);
    let candidates = [
        oauth(
            "near",
            CandidateQuotas {
                five_hour: Some(&five_near),
                shared_weekly: Some(&near),
                model_weekly: None,
            },
        ),
        oauth(
            "far",
            CandidateQuotas {
                five_hour: Some(&five_far),
                shared_weekly: Some(&far),
                model_weekly: None,
            },
        ),
    ];

    for _ in 0..8 {
        assert_eq!(accepted_ids(&decide_for(None, &candidates)), vec!["near"]);
    }
}

#[test]
fn exhausted_weekly_window_is_rejected_and_the_peer_is_used() {
    let dead = window(DataState::Fresh, 1.0, DAY);
    let alive = window(DataState::Fresh, 0.4, 5 * DAY);
    let five = window(DataState::Fresh, 0.2, 3 * HOUR);
    let candidates = [
        oauth(
            "dead",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&dead),
                model_weekly: None,
            },
        ),
        oauth(
            "alive",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&alive),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["alive"]);
    assert_eq!(reason_for(&decision, "dead"), "reject:weekly_exhausted");
}

#[test]
fn provider_rejection_blocks_even_below_full_utilization() {
    let rejected = rejected_window(DataState::Fresh, 0.3, 2 * DAY);
    let healthy = window(DataState::Fresh, 0.5, 5 * DAY);
    let five = window(DataState::Fresh, 0.1, 3 * HOUR);
    let candidates = [
        oauth(
            "rejected",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&rejected),
                model_weekly: None,
            },
        ),
        oauth(
            "healthy",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&healthy),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["healthy"]);
    assert_eq!(reason_for(&decision, "rejected"), "reject:weekly_exhausted");
}

#[test]
fn stale_rejection_stops_blocking_after_its_reset_passes() {
    // Stale snapshot, reset already in the past: the rejection it described has
    // expired, so the account is usable again.
    let stale_expired = QuotaWindow {
        state: DataState::Stale,
        utilization: Some(1.0),
        resets_at_unix_secs: Some(NOW - HOUR),
        observed_at_unix_secs: Some(NOW - 2 * HOUR),
        rejected: true,
    };
    let five = window(DataState::Fresh, 0.1, 3 * HOUR);
    let candidates = [oauth(
        "revived",
        CandidateQuotas {
            five_hour: Some(&five),
            shared_weekly: Some(&stale_expired),
            model_weekly: None,
        },
    )];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["revived"]);
}

// -- Caveat 1: do not walk into the five-hour wall ---------------------------

#[test]
fn a_cold_session_prefers_five_hour_headroom_within_the_weekly_band() {
    // Comparable weekly urgency and no cache hit: start on the roomy account.
    let urgent_week = window(DataState::Fresh, 0.2, DAY);
    let urgent_five = window(DataState::Fresh, 0.97, 2 * HOUR);
    let relaxed_week = window(DataState::Fresh, 0.2, DAY);
    let relaxed_five = window(DataState::Fresh, 0.05, 4 * HOUR);
    let candidates = [
        oauth(
            "urgent",
            CandidateQuotas {
                five_hour: Some(&urgent_five),
                shared_weekly: Some(&urgent_week),
                model_weekly: None,
            },
        ),
        oauth(
            "relaxed",
            CandidateQuotas {
                five_hour: Some(&relaxed_five),
                shared_weekly: Some(&relaxed_week),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["relaxed"]);
}

#[test]
fn an_exhausted_five_hour_window_is_blocked_even_without_a_peer() {
    let blocked_five = window(DataState::Fresh, 1.0, HOUR);
    let week = window(DataState::Fresh, 0.2, DAY);
    let candidates = [oauth(
        "solo",
        CandidateQuotas {
            five_hour: Some(&blocked_five),
            shared_weekly: Some(&week),
            model_weekly: None,
        },
    )];

    let decision = decide_for(None, &candidates);

    assert!(decision.accepted.is_empty());
    assert_eq!(reason_for(&decision, "solo"), "reject:five_hour_exhausted");
}

#[test]
fn weekly_urgency_outranks_five_hour_headroom() {
    // The weekly deadline wins even with only 5% of five-hour quota left.
    let behind_week = window(DataState::Fresh, 0.8, DAY);
    let behind_five = window(DataState::Fresh, 0.95, HOUR);
    let ahead_week = window(DataState::Fresh, 0.1, 5 * DAY);
    let ahead_five = window(DataState::Fresh, 0.0, 4 * HOUR);
    let candidates = [
        oauth(
            "behind_pace",
            CandidateQuotas {
                five_hour: Some(&behind_five),
                shared_weekly: Some(&behind_week),
                model_weekly: None,
            },
        ),
        oauth(
            "ahead_of_pace",
            CandidateQuotas {
                five_hour: Some(&ahead_five),
                shared_weekly: Some(&ahead_week),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);
    assert_eq!(accepted_ids(&decision), vec!["behind_pace"]);

    // With both five-hour windows comfortable, the deadline term decides: the
    // account that resets tomorrow still holding 20% of its week must be
    // drained first, even though its peer has more quota and more time.
    let behind_five = window(DataState::Fresh, 0.2, 4 * HOUR);
    let ahead_five = window(DataState::Fresh, 0.2, 4 * HOUR);
    let candidates = [
        oauth(
            "behind_pace",
            CandidateQuotas {
                five_hour: Some(&behind_five),
                shared_weekly: Some(&behind_week),
                model_weekly: None,
            },
        ),
        oauth(
            "ahead_of_pace",
            CandidateQuotas {
                five_hour: Some(&ahead_five),
                shared_weekly: Some(&ahead_week),
                model_weekly: None,
            },
        ),
    ];
    let decision = decide_for(None, &candidates);
    assert_eq!(accepted_ids(&decision), vec!["behind_pace"]);
}

#[test]
fn five_hour_reset_cannot_override_weekly_urgency() {
    // An imminent five-hour reset cannot displace greater weekly urgency.
    let behind_week = window(DataState::Fresh, 0.9, 5 * DAY);
    let behind_five = window(DataState::Fresh, 0.5, 30 * 60);
    let ahead_week = window(DataState::Fresh, 0.2, 5 * DAY);
    let ahead_five = window(DataState::Fresh, 0.0, 5 * HOUR - 6 * 60);
    let candidates = [
        oauth(
            "behind",
            CandidateQuotas {
                five_hour: Some(&behind_five),
                shared_weekly: Some(&behind_week),
                model_weekly: None,
            },
        ),
        oauth(
            "ahead",
            CandidateQuotas {
                five_hour: Some(&ahead_five),
                shared_weekly: Some(&ahead_week),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["ahead"]);
}

#[test]
fn all_accounts_five_hour_exhausted_keep_nothing() {
    // The downstream filter also rejects these accounts: do not report a
    // winner that cannot survive the chain.
    let soon = window(DataState::Fresh, 1.0, HOUR);
    let soon_week = window(DataState::Fresh, 0.3, DAY);
    let later = window(DataState::Fresh, 1.0, 4 * HOUR);
    let later_week = window(DataState::Fresh, 0.3, 4 * DAY);
    let candidates = [
        oauth(
            "soon",
            CandidateQuotas {
                five_hour: Some(&soon),
                shared_weekly: Some(&soon_week),
                model_weekly: None,
            },
        ),
        oauth(
            "later",
            CandidateQuotas {
                five_hour: Some(&later),
                shared_weekly: Some(&later_week),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert!(decision.accepted.is_empty());
}

// -- Caveat 2: session and account affinity ---------------------------------

#[test]
fn a_thread_is_pinned_to_one_account_and_the_choice_is_stable() {
    // Two accounts with identical, untouched weekly windows: urgency ties, so
    // affinity decides. Each thread must keep landing on its own account.
    let five = window(DataState::Fresh, 0.0, 4 * HOUR);
    let week = window(DataState::Fresh, 0.0, 5 * DAY);
    let quotas = CandidateQuotas {
        five_hour: Some(&five),
        shared_weekly: Some(&week),
        model_weekly: None,
    };
    let candidates = [oauth("account-a", quotas), oauth("account-b", quotas)];

    let first = decide_for(Some("thread-1"), &candidates);
    let second = decide_for(Some("thread-1"), &candidates);
    assert_eq!(accepted_ids(&first), accepted_ids(&second));

    // Different threads distribute across the tied accounts rather than all
    // piling onto one.
    let mut seen: Vec<String> = Vec::new();
    for index in 0..32 {
        let thread = format!("thread-{index}");
        let decision = decide_for(Some(&thread), &candidates);
        let id = decision.accepted[0].clone();
        if !seen.contains(&id) {
            seen.push(id);
        }
    }
    assert_eq!(seen.len(), 2, "threads should spread across tied accounts");
}

#[test]
fn a_warm_cache_keeps_the_thread_on_its_account() {
    // Same weekly deadline, so both sit inside the affinity band. The thread's
    // prompt cache lives on "warm": predicted cache reads must win over the
    // marginally higher raw urgency of "cold".
    let warm_week = window(DataState::Fresh, 0.30, 5 * DAY);
    let cold_week = window(DataState::Fresh, 0.25, 5 * DAY);
    let five = window(DataState::Fresh, 0.1, 4 * HOUR);
    let candidates = [
        candidate(
            "cold",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&cold_week),
                model_weekly: None,
            },
            0,
        ),
        candidate(
            "warm",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&warm_week),
                model_weekly: None,
            },
            240_000,
        ),
    ];

    let decision = decide_for(Some("thread-9"), &candidates);

    assert_eq!(accepted_ids(&decision), vec!["warm"]);
}

#[test]
fn affinity_does_not_override_a_clearly_better_deadline() {
    // "cold" expires tomorrow against "warm"'s five days, which is far outside
    // the near-tie band, so expiry wins over cache warmth.
    let cold_week = window(DataState::Fresh, 0.1, DAY);
    let warm_week = window(DataState::Fresh, 0.1, 5 * DAY);
    let five = window(DataState::Fresh, 0.1, 4 * HOUR);
    let candidates = [
        candidate(
            "cold",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&cold_week),
                model_weekly: None,
            },
            0,
        ),
        candidate(
            "warm",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&warm_week),
                model_weekly: None,
            },
            240_000,
        ),
    ];

    let decision = decide_for(Some("thread-9"), &candidates);

    assert_eq!(accepted_ids(&decision), vec!["cold"]);
}

// -- Fallbacks and pass-through ---------------------------------------------

#[test]
fn api_key_candidates_pass_through_when_a_subscription_is_chosen() {
    let week = window(DataState::Fresh, 0.2, DAY);
    let five = window(DataState::Fresh, 0.1, 3 * HOUR);
    let candidates = [
        oauth(
            "sub",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&week),
                model_weekly: None,
            },
        ),
        candidate(
            "api-key",
            "anthropic_api_key",
            CandidateQuotas::default(),
            0,
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["api-key", "sub"]);
    assert_eq!(
        reason_for(&decision, "api-key"),
        "pass_through_non_subscription"
    );
}

#[test]
fn every_subscription_exhausted_leaves_only_the_api_key_fallback() {
    let dead = window(DataState::Fresh, 1.0, DAY);
    let five = window(DataState::Fresh, 0.1, 3 * HOUR);
    let candidates = [
        oauth(
            "dead",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&dead),
                model_weekly: None,
            },
        ),
        candidate(
            "api-key",
            "anthropic_api_key",
            CandidateQuotas::default(),
            0,
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["api-key"]);
}

#[test]
fn every_subscription_exhausted_without_a_fallback_keeps_nothing() {
    // No API-key fallback and every weekly window the provider has already
    // rejected: the plugin keeps nothing and the host answers with
    // route_no_upstream_after_filter. The built-in filter downstream reaches
    // the same conclusion on its own, so there is no fallback to add here.
    let dead = window(DataState::Fresh, 1.0, DAY);
    let five = window(DataState::Fresh, 0.1, 3 * HOUR);
    let candidates = [
        oauth(
            "dead-a",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&dead),
                model_weekly: None,
            },
        ),
        oauth(
            "dead-b",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&dead),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert!(decision.accepted.is_empty());
    assert_eq!(reason_for(&decision, "dead-a"), "reject:weekly_exhausted");
}

#[test]
fn candidates_without_quota_data_are_usable_and_spread_by_thread() {
    let candidates = [
        oauth("unknown-a", CandidateQuotas::default()),
        oauth("unknown-b", CandidateQuotas::default()),
    ];

    let mut seen: Vec<String> = Vec::new();
    for index in 0..32 {
        let thread = format!("thread-{index}");
        let decision = decide_for(Some(&thread), &candidates);
        assert_eq!(decision.accepted.len(), 1);
        let id = decision.accepted[0].clone();
        if !seen.contains(&id) {
            seen.push(id);
        }
    }
    assert_eq!(seen.len(), 2, "unknown accounts should still spread");
}

#[test]
fn an_account_with_almost_no_weekly_quota_left_loses_to_one_with_room() {
    let bare = window(DataState::Fresh, 0.999, DAY);
    let roomy = window(DataState::Fresh, 0.4, 5 * DAY);
    let five = window(DataState::Fresh, 0.1, 4 * HOUR);
    let candidates = [
        oauth(
            "bare",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&bare),
                model_weekly: None,
            },
        ),
        oauth(
            "roomy",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&roomy),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["roomy"]);
}

#[test]
fn model_scoped_weekly_window_blocks_only_its_own_model_family() {
    let opus_dead = window(DataState::Fresh, 1.0, 2 * DAY);
    let shared = window(DataState::Fresh, 0.1, 4 * DAY);
    let five = window(DataState::Fresh, 0.1, 4 * HOUR);
    let candidates = [
        oauth(
            "opus-capped",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&shared),
                model_weekly: Some(&opus_dead),
            },
        ),
        oauth(
            "healthy",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&shared),
                model_weekly: None,
            },
        ),
    ];

    let decision = decide_for(None, &candidates);

    assert_eq!(accepted_ids(&decision), vec!["healthy"]);
    assert_eq!(
        reason_for(&decision, "opus-capped"),
        "reject:weekly_exhausted"
    );
}

#[test]
fn model_window_label_matches_the_requested_model_family() {
    assert_eq!(
        model_weekly_window("claude-opus-5-5-20260101"),
        Some(WINDOW_SEVEN_DAY_OPUS)
    );
    assert_eq!(
        model_weekly_window("claude-sonnet-4-6-20260201"),
        Some(WINDOW_SEVEN_DAY_SONNET)
    );
    assert_eq!(
        model_weekly_window("claude-fable-5"),
        Some(WINDOW_SEVEN_DAY_FABLE)
    );
    assert_eq!(model_weekly_window("claude-haiku-4-5"), None);
}

#[test]
fn decisions_are_deterministic_for_identical_input() {
    let five = window(DataState::Fresh, 0.3, 3 * HOUR);
    let week_a = window(DataState::Fresh, 0.2, 2 * DAY);
    let week_b = window(DataState::Fresh, 0.25, 2 * DAY + HOUR);
    let candidates = [
        oauth(
            "a",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&week_a),
                model_weekly: None,
            },
        ),
        oauth(
            "b",
            CandidateQuotas {
                five_hour: Some(&five),
                shared_weekly: Some(&week_b),
                model_weekly: None,
            },
        ),
    ];

    let first = decide_for(Some("t"), &candidates);
    for _ in 0..16 {
        assert_eq!(first, decide_for(Some("t"), &candidates));
    }
}

#[test]
fn the_clock_comes_from_the_quota_snapshots_not_the_candidate_stamp() {
    // The live shape: the host stamps candidates with the *rate-limit* cache
    // time and leaves it at 0 when no rate-limit snapshot exists, while the
    // quota windows carry real observation times. If the rule trusted the
    // candidate stamp, "now" would be 0, every reset would look decades away,
    // every time fraction would clamp to 1.0 and the deadline term would vanish
    // — the decision would degrade to "most quota left wins".
    let now = 1_700_000_000u64;
    let near_five = QuotaWindow {
        state: DataState::Fresh,
        utilization: Some(0.0),
        resets_at_unix_secs: Some(now + 600),
        observed_at_unix_secs: Some(now),
        rejected: false,
    };
    let near_week = QuotaWindow {
        state: DataState::Fresh,
        utilization: Some(0.8),
        resets_at_unix_secs: Some(now + 100_000),
        observed_at_unix_secs: Some(now),
        rejected: false,
    };
    let soon_week = QuotaWindow {
        state: DataState::Fresh,
        utilization: Some(0.1),
        resets_at_unix_secs: Some(now + 25_000),
        observed_at_unix_secs: Some(now),
        rejected: false,
    };

    let mut later = oauth(
        "later",
        CandidateQuotas {
            five_hour: Some(&near_five),
            shared_weekly: Some(&near_week),
            model_weekly: None,
        },
    );
    later.observed_at_unix_secs = 0;
    let mut sooner = oauth(
        "sooner",
        CandidateQuotas {
            five_hour: Some(&near_five),
            shared_weekly: Some(&soon_week),
            model_weekly: None,
        },
    );
    sooner.observed_at_unix_secs = 0;

    let decision = decide_for(None, &[later, sooner]);

    assert_eq!(accepted_ids(&decision), vec!["sooner"]);
    let reason = reason_for(&decision, "sooner");
    assert!(
        reason.contains("5h_reset_in=600s"),
        "the clock must come from the snapshots, got: {reason}"
    );
    assert!(
        reason.contains("7d_reset_in=25000s"),
        "the clock must come from the snapshots, got: {reason}"
    );
}

#[test]
fn routing_clock_supersedes_stale_observation_stamps() {
    // Wire V2 supplies the host routing clock. On an idle account the newest
    // quota snapshot can be an hour old, so counting down from the observation
    // stamp reports a reset far later than it is — and the deadline term is
    // exactly what decides between two accounts in that state. The clock wins;
    // the observation estimate remains only a floor.
    let observed = NOW - HOUR;
    let five = QuotaWindow {
        state: DataState::Fresh,
        utilization: Some(0.0),
        resets_at_unix_secs: Some(NOW + 600),
        observed_at_unix_secs: Some(observed),
        rejected: false,
    };
    let week = QuotaWindow {
        state: DataState::Fresh,
        utilization: Some(0.5),
        resets_at_unix_secs: Some(NOW + 5 * DAY),
        observed_at_unix_secs: Some(observed),
        rejected: false,
    };

    let mut account = oauth(
        "stale-observation",
        CandidateQuotas {
            five_hour: Some(&five),
            shared_weekly: Some(&week),
            model_weekly: None,
        },
    );
    account.observed_at_unix_secs = observed;

    let decision = decide_for(None, &[account]);

    let reason = reason_for(&decision, "stale-observation");
    assert!(
        reason.contains("5h_reset_in=600s"),
        "the host routing clock must win over the observation stamp, got: {reason}"
    );
}

#[test]
fn observed_at_is_taken_from_the_freshest_candidate() {
    // The host stamps every candidate with its observation time; the newest one
    // is the best available "now".
    let five = window(DataState::Fresh, 0.0, 4 * HOUR);
    let week = window(DataState::Fresh, 0.0, 5 * DAY);
    let quotas = CandidateQuotas {
        five_hour: Some(&five),
        shared_weekly: Some(&week),
        model_weekly: None,
    };
    let mut stale = oauth("stale", quotas);
    stale.observed_at_unix_secs = NOW - DAY;
    let candidates = [stale, oauth("fresh", quotas)];

    let decision = decide_for(None, &candidates);

    assert_eq!(decision.accepted.len(), 1);
    assert!(reason_for(&decision, "fresh").starts_with("accept:reset_priority"));
}

// -- Brute force: invariants over the whole decision space ------------------

/// One account shape in the sweep below.
#[derive(Clone, Copy, Debug)]
struct Shape {
    util: f64,
    week_in: u64,
    week_state: DataState,
    five_in: u64,
    five_state: DataState,
}

/// The exhaustion rule as the module documents it, restated independently here
/// so the sweep can assert "nothing viable is ever silently dropped" without
/// calling the code it is checking.
fn appears_blocked(shape: &Shape, now: u64) -> bool {
    if shape.util < 1.0 {
        return false;
    }
    let weekly_blocked = match shape.week_state {
        DataState::Fresh => true,
        DataState::Stale => now + shape.week_in > now,
        DataState::Absent | DataState::Unobserved => false,
    };
    let five_hour_blocked = match shape.five_state {
        DataState::Fresh => true,
        DataState::Stale => now + shape.five_in > now,
        DataState::Absent | DataState::Unobserved => false,
    };
    weekly_blocked || five_hour_blocked
}

fn windows_for(shape: &Shape, now: u64) -> (QuotaWindow, QuotaWindow) {
    let week = QuotaWindow {
        state: shape.week_state,
        utilization: Some(shape.util),
        resets_at_unix_secs: Some(now + shape.week_in),
        observed_at_unix_secs: Some(now),
        rejected: false,
    };
    let five = QuotaWindow {
        state: shape.five_state,
        utilization: Some(shape.util),
        resets_at_unix_secs: Some(now + shape.five_in),
        observed_at_unix_secs: Some(now),
        rejected: false,
    };
    (week, five)
}

#[test]
fn invariants_hold_across_a_brute_force_grid() {
    let utils = [0.0, 0.5, 0.9, 0.99, 1.0];
    let week_ins = [600_u64, DAY, 6 * DAY];
    let week_states = [DataState::Fresh, DataState::Stale, DataState::Absent];
    let five_ins = [60_u64, 9_000];
    let five_states = [DataState::Fresh, DataState::Stale];

    let mut shapes: Vec<Shape> = Vec::new();
    for util in utils {
        for week_in in week_ins {
            for week_state in week_states {
                for five_in in five_ins {
                    for five_state in five_states {
                        shapes.push(Shape {
                            util,
                            week_in,
                            week_state,
                            five_in,
                            five_state,
                        });
                    }
                }
            }
        }
    }

    let mut checked = 0_u32;
    for a in &shapes {
        for b in &shapes {
            let (week_a, five_a) = windows_for(a, NOW);
            let (week_b, five_b) = windows_for(b, NOW);
            let quotas_a = CandidateQuotas {
                five_hour: Some(&five_a),
                shared_weekly: Some(&week_a),
                model_weekly: None,
            };
            let quotas_b = CandidateQuotas {
                five_hour: Some(&five_b),
                shared_weekly: Some(&week_b),
                model_weekly: None,
            };
            let candidates = [
                oauth("a", quotas_a),
                oauth("b", quotas_b),
                candidate(
                    "api-key",
                    "anthropic_api_key",
                    CandidateQuotas::default(),
                    0,
                ),
            ];

            for thread in [None, Some("thread")] {
                let first = decide_for(thread, &candidates);
                let second = decide_for(thread, &candidates);
                let context = format!("a={a:?} b={b:?} thread={thread:?}");

                assert_eq!(first, second, "not deterministic: {context}");
                assert_eq!(first.per_candidate.len(), 3, "one verdict each: {context}");

                // Non-subscription candidates always survive.
                assert!(
                    first.accepted.iter().any(|id| id == "api-key"),
                    "api key dropped: {context}"
                );

                // At most one subscription is ever kept.
                let kept: Vec<&str> = first
                    .accepted
                    .iter()
                    .map(String::as_str)
                    .filter(|id| *id != "api-key")
                    .collect();
                assert!(kept.len() <= 1, "kept {kept:?}: {context}");

                // Nothing that looks exhausted may be kept...
                for id in &kept {
                    let shape = if *id == "a" { a } else { b };
                    assert!(
                        !appears_blocked(shape, NOW),
                        "kept an exhausted account {id}: {context}"
                    );
                }
                // ...and nothing viable may be dropped.
                let any_viable = !appears_blocked(a, NOW) || !appears_blocked(b, NOW);
                assert_eq!(
                    kept.len(),
                    usize::from(any_viable),
                    "viable account was dropped: {context}"
                );

                // Verdicts are well formed and match the accept list.
                for (id, verdict, reason) in &first.per_candidate {
                    assert!(*verdict == "accept" || *verdict == "reject", "{context}");
                    assert!(!reason.is_empty(), "empty reason for {id}: {context}");
                    assert_eq!(
                        first.accepted.iter().any(|accepted_id| accepted_id == id),
                        *verdict == "accept",
                        "verdict/accept mismatch for {id}: {context}"
                    );
                }
            }
            checked += 1;
        }
    }
    assert!(checked > 10_000, "swept only {checked} pairs");
}

#[test]
fn an_earlier_deadline_with_more_quota_always_wins() {
    // No five-hour windows, so the weekly term decides alone: the account that
    // resets sooner while holding more of its week must always be chosen.
    for (util_a, util_b) in [(0.1, 0.5), (0.4, 0.9), (0.0, 0.95)] {
        for (week_a, week_b) in [(DAY, 4 * DAY), (2 * DAY, 6 * DAY), (HOUR, 5 * DAY)] {
            let sooner_week = window(DataState::Fresh, util_a, week_a);
            let later_week = window(DataState::Fresh, util_b, week_b);
            let candidates = [
                oauth(
                    "a",
                    CandidateQuotas {
                        five_hour: None,
                        shared_weekly: Some(&sooner_week),
                        model_weekly: None,
                    },
                ),
                oauth(
                    "b",
                    CandidateQuotas {
                        five_hour: None,
                        shared_weekly: Some(&later_week),
                        model_weekly: None,
                    },
                ),
            ];

            let decision = decide_for(None, &candidates);
            assert_eq!(
                accepted_ids(&decision),
                vec!["a"],
                "a=({util_a}, {week_a}s) b=({util_b}, {week_b}s)"
            );
        }
    }
}

#[test]
fn five_hour_rejection_cannot_hide_a_healthy_runner_up() {
    let rejected = rejected_window(DataState::Fresh, 0.2, HOUR);
    let soon = window(DataState::Fresh, 0.1, DAY);
    let later = window(DataState::Fresh, 0.1, 5 * DAY);
    let candidates = [
        oauth(
            "rejected",
            CandidateQuotas {
                five_hour: Some(&rejected),
                shared_weekly: Some(&soon),
                model_weekly: None,
            },
        ),
        oauth(
            "healthy",
            CandidateQuotas {
                five_hour: None,
                shared_weekly: Some(&later),
                model_weekly: None,
            },
        ),
    ];
    let decision = decide_for(None, &candidates);
    assert_eq!(accepted_ids(&decision), vec!["healthy"]);
    assert_eq!(
        reason_for(&decision, "rejected"),
        "reject:five_hour_exhausted"
    );
}

#[test]
fn stale_rejection_without_reset_cannot_hide_a_healthy_runner_up() {
    let rejected = QuotaWindow {
        resets_at_unix_secs: None,
        ..rejected_window(DataState::Stale, 0.2, HOUR)
    };
    let soon = window(DataState::Fresh, 0.1, DAY);
    let later = window(DataState::Fresh, 0.1, 5 * DAY);
    let candidates = [
        oauth(
            "rejected",
            CandidateQuotas {
                five_hour: None,
                shared_weekly: Some(&soon),
                model_weekly: Some(&rejected),
            },
        ),
        oauth(
            "healthy",
            CandidateQuotas {
                five_hour: None,
                shared_weekly: Some(&later),
                model_weekly: None,
            },
        ),
    ];
    assert_eq!(
        accepted_ids(&decide_for(None, &candidates)),
        vec!["healthy"]
    );
}

#[test]
fn headroom_uses_the_most_constrained_weekly_window() {
    let shared = window(DataState::Fresh, 0.1, DAY);
    let model = window(DataState::Fresh, 0.999, DAY);
    let roomy = window(DataState::Fresh, 0.5, 5 * DAY);
    let candidates = [
        oauth(
            "model-bare",
            CandidateQuotas {
                five_hour: None,
                shared_weekly: Some(&shared),
                model_weekly: Some(&model),
            },
        ),
        oauth(
            "roomy",
            CandidateQuotas {
                five_hour: None,
                shared_weekly: Some(&roomy),
                model_weekly: None,
            },
        ),
    ];
    assert_eq!(accepted_ids(&decide_for(None, &candidates)), vec!["roomy"]);
}

#[test]
fn stale_five_hour_utilization_does_not_demote_an_account_after_reset() {
    let expired = window(DataState::Stale, 0.95, 0);
    let soon = window(DataState::Fresh, 0.1, DAY);
    let later = window(DataState::Fresh, 0.1, 5 * DAY);
    let candidates = [
        oauth(
            "reset",
            CandidateQuotas {
                five_hour: Some(&expired),
                shared_weekly: Some(&soon),
                model_weekly: None,
            },
        ),
        oauth(
            "later",
            CandidateQuotas {
                five_hour: None,
                shared_weekly: Some(&later),
                model_weekly: None,
            },
        ),
    ];
    assert_eq!(accepted_ids(&decide_for(None, &candidates)), vec!["reset"]);
}

#[test]
fn a_warm_session_uses_its_nearly_spent_five_hour_window() {
    let week = window(DataState::Fresh, 0.2, 3 * DAY);
    let tight = window(DataState::Fresh, 0.99, 4 * HOUR);
    let roomy = window(DataState::Fresh, 0.1, 60);
    let candidates = [
        candidate(
            "warm",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&tight),
                shared_weekly: Some(&week),
                model_weekly: None,
            },
            240_000,
        ),
        oauth(
            "cold",
            CandidateQuotas {
                five_hour: Some(&roomy),
                shared_weekly: Some(&week),
                model_weekly: None,
            },
        ),
    ];
    for thread in [None, Some("existing-session")] {
        let decision = decide_for(thread, &candidates);
        assert_eq!(accepted_ids(&decision), vec!["warm"]);
        assert!(reason_for(&decision, "warm").contains("cache_read_tokens=240000"));
    }
}

#[test]
fn a_warm_session_switches_accounts_only_when_its_window_is_exhausted() {
    let week = window(DataState::Fresh, 0.2, 3 * DAY);
    let mut owner_five = window(DataState::Fresh, 0.99, HOUR);
    let peer_five = window(DataState::Fresh, 0.1, HOUR);
    for (utilization, expected) in [(0.99, "warm"), (1.0, "peer")] {
        owner_five.utilization = Some(utilization);
        let candidates = [
            candidate(
                "warm",
                KIND_ANTHROPIC_OAUTH,
                CandidateQuotas {
                    five_hour: Some(&owner_five),
                    shared_weekly: Some(&week),
                    model_weekly: None,
                },
                240_000,
            ),
            oauth(
                "peer",
                CandidateQuotas {
                    five_hour: Some(&peer_five),
                    shared_weekly: Some(&week),
                    model_weekly: None,
                },
            ),
        ];
        assert_eq!(
            accepted_ids(&decide_for(Some("session"), &candidates)),
            vec![expected]
        );
    }
}

#[test]
fn weekly_urgency_wins_even_with_a_warm_peer_and_an_imminent_five_hour_reset() {
    let soon_week = window(DataState::Fresh, 0.2, DAY);
    let later_week = window(DataState::Fresh, 0.2, 5 * DAY);
    let comfortable = window(DataState::Fresh, 0.1, 4 * HOUR);
    let imminent = window(DataState::Fresh, 0.0, 60);
    let candidates = [
        oauth(
            "weekly-priority",
            CandidateQuotas {
                five_hour: Some(&comfortable),
                shared_weekly: Some(&soon_week),
                model_weekly: None,
            },
        ),
        candidate(
            "warm-peer",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&imminent),
                shared_weekly: Some(&later_week),
                model_weekly: None,
            },
            240_000,
        ),
    ];
    assert_eq!(
        accepted_ids(&decide_for(Some("session"), &candidates)),
        vec!["weekly-priority"]
    );
}

#[test]
fn largest_cache_hit_wins_over_five_hour_headroom() {
    let week = window(DataState::Fresh, 0.2, 3 * DAY);
    let tight = window(DataState::Fresh, 0.99, HOUR);
    let roomy = window(DataState::Fresh, 0.1, HOUR);
    let candidates = [
        candidate(
            "large-hit",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&tight),
                shared_weekly: Some(&week),
                model_weekly: None,
            },
            240_000,
        ),
        candidate(
            "small-hit",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&roomy),
                shared_weekly: Some(&week),
                model_weekly: None,
            },
            1_000,
        ),
    ];
    assert_eq!(
        accepted_ids(&decide_for(Some("session"), &candidates)),
        vec!["large-hit"]
    );
}

#[test]
fn unknown_weekly_quota_still_allows_cache_affinity() {
    let tight = window(DataState::Fresh, 0.99, HOUR);
    let roomy = window(DataState::Fresh, 0.1, HOUR);
    let candidates = [
        candidate(
            "warm",
            KIND_ANTHROPIC_OAUTH,
            CandidateQuotas {
                five_hour: Some(&tight),
                ..CandidateQuotas::default()
            },
            240_000,
        ),
        oauth(
            "cold",
            CandidateQuotas {
                five_hour: Some(&roomy),
                ..CandidateQuotas::default()
            },
        ),
    ];
    assert_eq!(
        accepted_ids(&decide_for(Some("session"), &candidates)),
        vec!["warm"]
    );
}

#[test]
fn candidate_order_does_not_change_the_session_owner() {
    let week = window(DataState::Fresh, 0.2, 3 * DAY);
    let quotas = CandidateQuotas {
        shared_weekly: Some(&week),
        ..CandidateQuotas::default()
    };
    let a = oauth("a", quotas);
    let b = oauth("b", quotas);
    for thread in [None, Some("session"), Some("")] {
        assert_eq!(
            accepted_ids(&decide_for(thread, &[a, b])),
            accepted_ids(&decide_for(thread, &[b, a])),
        );
    }
}
