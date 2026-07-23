use cc_lb_domain::CacheScore;

use super::*;

#[derive(Clone, Copy)]
struct CacheTokenEstimate {
    uncached_input: u32,
    create_5m: u32,
    create_1h: u32,
    read: u32,
}

fn with_cache_tokens(
    mut candidate: UpstreamCandidate,
    estimate: CacheTokenEstimate,
) -> UpstreamCandidate {
    candidate.cache_score = Some(CacheScore {
        predicted_cache_read_tokens: estimate.read,
        predicted_cache_creation_tokens_5m: estimate.create_5m,
        predicted_cache_creation_tokens_1h: estimate.create_1h,
        predicted_uncached_input_tokens: estimate.uncached_input,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: None,
        confidence: 1.0,
        ambiguity_reason: None,
        matched_v3_cache_key: None,
        breakpoint_content_block_index: None,
        matched_content_block_index: None,
        lookback_distance: None,
        token_estimate_source: None,
    });
    candidate
}

fn urgent_candidate(name: &str, seed: u8) -> UpstreamCandidate {
    oauth_at_t0(
        name,
        seed,
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
    )
}

fn on_pace_candidate(name: &str, seed: u8) -> UpstreamCandidate {
    healthy_known_base_at_util(name, seed, 0.95)
}

fn warning_on_pace_candidate(name: &str, seed: u8) -> UpstreamCandidate {
    oauth_at_t0(
        name,
        seed,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.0)
                .status("allowed_warning")
                .build(),
            fresh(WINDOW_SEVEN_DAY).util(0.0).status("allowed").build(),
        ],
    )
}

fn winner(candidates: &[UpstreamCandidate], request_id: &str) -> Uuid {
    SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(MODEL_AGNOSTIC, request_id),
            &principal(),
            candidates,
        )
        .expect("builtin filter cannot fail")
        .kept_upstream_ids[0]
}

#[test]
fn warm_owner_cost_wins_over_more_urgent_expensive_candidate() {
    // Given: the warm owner is less costly despite cache creation work, while its peer is urgent.
    let owner = with_cache_tokens(
        on_pace_candidate("owner", 1),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 100_000,
            read: 100_000,
        },
    );
    let expensive = with_cache_tokens(
        urgent_candidate("expensive", 2),
        CacheTokenEstimate {
            uncached_input: 250_000,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        },
    );

    // When: request identities vary without changing the candidates.
    for request_number in 0..64 {
        // Then: cost-first always keeps the lower-cost warm owner.
        assert_eq!(
            winner(
                &[owner.clone(), expensive.clone()],
                &format!("warm-{request_number}")
            ),
            owner.upstream_id
        );
    }
}

#[test]
fn equal_cold_costs_choose_highest_tier_urgency() {
    // Given: cold candidates with identical known zero input cost and different urgency.
    let low = with_cache_tokens(on_pace_candidate("low", 1), CacheTokenEstimate::zero());
    let high = with_cache_tokens(urgent_candidate("high", 2), CacheTokenEstimate::zero());

    // When: request identities vary without changing the candidates.
    for request_number in 0..64 {
        // Then: the high-urgency candidate wins every time.
        assert_eq!(
            winner(
                &[low.clone(), high.clone()],
                &format!("equal-{request_number}")
            ),
            high.upstream_id
        );
    }
}

#[test]
fn costs_within_five_percent_choose_higher_urgency() {
    // Given: a 100-micro cheap candidate and a 105-micro urgent candidate.
    let cheap = with_cache_tokens(
        on_pace_candidate("cheap", 1),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 200,
        },
    );
    let urgent = with_cache_tokens(
        urgent_candidate("urgent", 2),
        CacheTokenEstimate {
            uncached_input: 21,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        },
    );

    // When: the costs fall exactly on the five-percent near-cost boundary.
    let selected = winner(&[cheap, urgent.clone()], "near-cost");

    // Then: urgency decides within the near-cost tier.
    assert_eq!(selected, urgent.upstream_id);
}

#[test]
fn costs_more_than_five_percent_choose_cheaper_candidate() {
    // Given: a 100-micro on-pace candidate and a 106-micro urgent candidate.
    let cheap = with_cache_tokens(
        on_pace_candidate("cheap", 1),
        CacheTokenEstimate {
            uncached_input: 20,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        },
    );
    let expensive = with_cache_tokens(
        urgent_candidate("expensive", 2),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 212,
        },
    );

    // When: the expensive candidate sits outside the near-cost tier.
    let selected = winner(&[cheap.clone(), expensive], "cost-gap");

    // Then: cost excludes the urgent but too-expensive candidate.
    assert_eq!(selected, cheap.upstream_id);
}

#[test]
fn known_cost_excludes_unknown_cost_candidate() {
    // Given: one candidate has an estimated cost and an urgent peer has no cost data.
    let known = with_cache_tokens(
        on_pace_candidate("known", 1),
        CacheTokenEstimate {
            uncached_input: 20,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        },
    );
    let unknown = urgent_candidate("unknown", 2);

    // When: request identities vary with the same mixed-cost bucket.
    for request_number in 0..64 {
        // Then: the candidate without a cost cannot enter the near-cost tier.
        assert_eq!(
            winner(
                &[known.clone(), unknown.clone()],
                &format!("mixed-{request_number}")
            ),
            known.upstream_id
        );
    }
}

#[test]
fn all_unknown_costs_choose_highest_effective_urgency() {
    // Given: neither candidate has enough pricing input for a cost estimate.
    let low = on_pace_candidate("low", 1);
    let high = urgent_candidate("high", 2);

    // When: the all-None fallback ranks candidates by effective urgency.
    let selected = winner(&[low, high.clone()], "all-none");

    // Then: the high-urgency candidate wins.
    assert_eq!(selected, high.upstream_id);
}

#[test]
fn warning_multiplier_breaks_zero_urgency_tie() {
    // Given: both candidates are on pace, but one carries the provider warning signal.
    let warned = warning_on_pace_candidate("warned", 1);
    let unwarned = on_pace_candidate("unwarned", 2);

    // When: request identities vary while urgency remains zero for both candidates.
    for request_number in 0..64 {
        // Then: the full warning multiplier ranks the unwarned candidate first.
        assert_eq!(
            winner(
                &[warned.clone(), unwarned.clone()],
                &format!("warning-{request_number}"),
            ),
            unwarned.upstream_id
        );
    }
}

#[test]
fn near_reset_warning_relaxes_without_removing_the_warning_penalty() {
    // Given: a warned weekly window has 2% remaining near reset, while a clean
    // peer has lower raw pressure. Both candidates have identical input cost.
    let warned = with_cache_tokens(
        oauth_at_t0(
            "warned-near-reset",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.95)
                    .status("allowed")
                    .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.98)
                    .status("allowed_warning")
                    .reset_at(T0_SECS + 4 * 60 * 60)
                    .build(),
            ],
        ),
        CacheTokenEstimate::zero(),
    );
    let clean = with_cache_tokens(
        oauth_at_t0(
            "clean-peer",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.50)
                    .status("allowed")
                    .reset_at(T0_SECS + 2 * 60 * 60)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.95)
                    .status("allowed")
                    .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
                    .build(),
            ],
        ),
        CacheTokenEstimate::zero(),
    );

    // When: the candidates are compared inside the same tier and cost bucket.
    let output = filter_for_model(&[warned.clone(), clean], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let warned_trace = candidate_urgency_for(&trace, warned.upstream_id);

    // Then: the near-reset warning relaxes only to 0.80, which is enough to
    // drain expiring quota while preserving a penalty against a clean peer.
    assert!((warned_trace.warning_multiplier - 0.80).abs() < 1e-12);
    assert_eq!(output.kept_upstream_ids, vec![warned.upstream_id]);
}

#[test]
fn far_reset_warning_keeps_the_full_penalty() {
    // Given: the same 98%-utilized warning is still far enough from reset that
    // it has no use-it-or-lose-it pressure.
    let warned = with_cache_tokens(
        oauth_at_t0(
            "warned-far-reset",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.95)
                    .status("allowed")
                    .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.98)
                    .status("allowed_warning")
                    .reset_at(T0_SECS + 24 * 60 * 60)
                    .build(),
            ],
        ),
        CacheTokenEstimate::zero(),
    );
    let clean = with_cache_tokens(
        oauth_at_t0(
            "clean-peer",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.50)
                    .status("allowed")
                    .reset_at(T0_SECS + 2 * 60 * 60)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.95)
                    .status("allowed")
                    .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
                    .build(),
            ],
        ),
        CacheTokenEstimate::zero(),
    );

    // When: the warning has no pressure-derived release.
    let output = filter_for_model(&[warned.clone(), clean.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let warned_trace = candidate_urgency_for(&trace, warned.upstream_id);

    // Then: the existing 0.20 penalty and clean-peer preference remain intact.
    assert_eq!(warned_trace.warning_multiplier, WARNING_MULTIPLIER);
    assert_eq!(output.kept_upstream_ids, vec![clean.upstream_id]);
}

#[test]
fn least_urgent_warning_window_gates_relaxation() {
    // Given: one warning window is near reset, but another warning window has
    // no pressure. Every request would debit both windows.
    let warned = with_cache_tokens(
        oauth_at_t0(
            "mixed-warning-pressure",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.98)
                    .status("allowed_warning")
                    .reset_at(T0_SECS + FIVE_HOUR_RESET_SECS)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.98)
                    .status("allowed_warning")
                    .reset_at(T0_SECS + 4 * 60 * 60)
                    .build(),
            ],
        ),
        CacheTokenEstimate::zero(),
    );
    let clean = with_cache_tokens(
        oauth_at_t0(
            "clean-peer",
            2,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .util(0.50)
                    .status("allowed")
                    .reset_at(T0_SECS + 2 * 60 * 60)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .util(0.95)
                    .status("allowed")
                    .reset_at(T0_SECS + SEVEN_DAY_RESET_SECS)
                    .build(),
            ],
        ),
        CacheTokenEstimate::zero(),
    );

    // When: the candidate-level warning policy considers both warned windows.
    let output = filter_for_model(&[warned.clone(), clean.clone()], MODEL_AGNOSTIC);
    let trace = output.subscription_preference.expect("trace present");
    let warned_trace = candidate_urgency_for(&trace, warned.upstream_id);

    // Then: the zero-pressure 5h warning keeps the full penalty, preventing the
    // urgent 7d window from spending a more constrained warned quota.
    assert_eq!(warned_trace.warning_multiplier, WARNING_MULTIPLIER);
    assert_eq!(output.kept_upstream_ids, vec![clean.upstream_id]);
}

#[test]
fn identical_inputs_choose_the_same_winner_repeatedly() {
    // Given: a fixed near-cost bucket and request identity.
    let cheap = with_cache_tokens(
        on_pace_candidate("cheap", 1),
        CacheTokenEstimate {
            uncached_input: 20,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        },
    );
    let urgent = with_cache_tokens(
        urgent_candidate("urgent", 2),
        CacheTokenEstimate {
            uncached_input: 21,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        },
    );

    // When: the unchanged input is selected repeatedly.
    let first = winner(&[cheap.clone(), urgent.clone()], "repeatable");
    let winners = (0..16)
        .map(|_| winner(&[cheap.clone(), urgent.clone()], "repeatable"))
        .collect::<Vec<_>>();

    // Then: every call chooses the same cost-first winner.
    assert!(winners.iter().all(|winner| *winner == first));
}

impl CacheTokenEstimate {
    const fn zero() -> Self {
        Self {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 0,
        }
    }
}
