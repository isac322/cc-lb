use cc_lb_domain::{
    CachePricingSummary, CacheScore, NativeCachePricing, NativeCachePricingComponent,
    NativeCachePricingProvider, NativeCachePricingRate, NativeCachePricingStatus,
};

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
fn winner_with_pricing(
    candidates: &[UpstreamCandidate],
    request_id: &str,
    cache_pricing: CachePricingSummary,
) -> Uuid {
    let mut ctx = ctx_with_request_id(MODEL_AGNOSTIC, request_id);
    ctx.cache_pricing = cache_pricing;
    SubscriptionPreferenceFilter::new()
        .filter(&ctx, &principal(), candidates)
        .expect("builtin filter cannot fail")
        .kept_upstream_ids[0]
}

fn native_pricing(input: Option<u64>, read: Option<u64>) -> CachePricingSummary {
    let rates = vec![
        NativeCachePricingRate {
            component: NativeCachePricingComponent::OrdinaryInput,
            lifetime: None,
            status: input
                .map(|_| NativeCachePricingStatus::Known)
                .unwrap_or(NativeCachePricingStatus::Unknown),
            micros_per_million: input,
        },
        NativeCachePricingRate {
            component: NativeCachePricingComponent::CachedInputRead,
            lifetime: None,
            status: read
                .map(|_| NativeCachePricingStatus::Known)
                .unwrap_or(NativeCachePricingStatus::Unknown),
            micros_per_million: read,
        },
    ];
    CachePricingSummary {
        status: "unknown".to_owned(),
        input_micros_per_million: None,
        cache_creation_5m_micros_per_million: None,
        cache_creation_1h_micros_per_million: None,
        cache_read_micros_per_million: None,
        native: Some(NativeCachePricing {
            version: 1,
            provider: NativeCachePricingProvider::OpenAi,
            rates,
        }),
    }
}

fn native_observation(
    candidate: UpstreamCandidate,
    estimate: CacheTokenEstimate,
) -> UpstreamCandidate {
    let mut candidate = with_cache_tokens(candidate, estimate);
    candidate
        .cache_score
        .as_mut()
        .expect("cache score")
        .ambiguity_reason = Some("provider_native_observation".to_owned());
    candidate
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
fn native_observation_respects_the_five_percent_cost_gate() {
    // Given: a native warm candidate and an urgent candidate with no observed
    // warmth, the shared native baseline makes the urgent candidate costlier.
    let warm = native_observation(
        on_pace_candidate("warm", 1),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 212,
        },
    );
    let cold = native_observation(urgent_candidate("cold", 2), CacheTokenEstimate::zero());

    // Then: urgency cannot override a cost gap above five percent.
    assert_eq!(
        winner_with_pricing(
            &[warm.clone(), cold],
            "native-cost-gap",
            native_pricing(Some(5_000_000), Some(500_000)),
        ),
        warm.upstream_id
    );
}

#[test]
fn native_warmth_uses_shared_baseline_and_lowers_cost() {
    // Given: native observations with 100k versus 1k cached tokens and the
    // same native rates.
    let warm = native_observation(
        on_pace_candidate("warm", 1),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 100_000,
        },
    );
    let shallow = native_observation(
        urgent_candidate("shallow", 2),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 1_000,
        },
    );
    let pricing = native_pricing(Some(5_000_000), Some(500_000));
    let baseline_tokens = 100_000;
    let warm_cost = estimate_candidate_input_cost_micros(&warm, &pricing, baseline_tokens)
        .expect("known native rates");
    let shallow_cost = estimate_candidate_input_cost_micros(&shallow, &pricing, baseline_tokens)
        .expect("known native rates");

    // Then: the larger warm observation is cheaper and wins outside the gate.
    assert!(warm_cost < shallow_cost);
    assert!(u128::from(shallow_cost) * 100 > u128::from(warm_cost) * 105);
    assert_eq!(
        winner_with_pricing(&[warm.clone(), shallow], "native-warmth-direction", pricing),
        warm.upstream_id
    );
}

#[test]
fn known_native_read_cost_uses_native_input_and_read_rates() {
    let candidate = native_observation(
        on_pace_candidate("native", 1),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 0,
            create_1h: 0,
            read: 1_000_000,
        },
    );
    assert_eq!(
        estimate_candidate_input_cost_micros(
            &candidate,
            &native_pricing(Some(2_000_000), Some(100_000)),
            1_000_000,
        ),
        Some(100_000)
    );
}

#[test]
fn missing_native_rate_keeps_cost_neutral() {
    let candidate = native_observation(
        on_pace_candidate("native", 1),
        CacheTokenEstimate {
            uncached_input: 0,
            create_5m: 10_000,
            create_1h: 10_000,
            read: 1_000_000,
        },
    );
    let pricing = native_pricing(Some(2_000_000), None);
    assert_eq!(
        estimate_candidate_input_cost_micros(&candidate, &pricing, 1_000_000),
        None
    );
    assert_eq!(cache_read_savings_ratio(&candidate, &pricing), 0.0);
    assert_eq!(candidate_cache_value_micros(&candidate, &pricing), None);
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
fn base_warning_does_not_change_zero_urgency_tie() {
    // Given: two on-pace base candidates, with one candidate transitioning from
    // allowed to either provider warning form.
    let allowed = on_pace_candidate("transitioned", 1);
    let mut explicit_warning = allowed.clone();
    explicit_warning.subscription_quotas[0].status = Some("allowed_warning".to_owned());
    let mut threshold_warning = allowed.clone();
    threshold_warning.subscription_quotas[0].surpassed_threshold = Some(0.90);
    let peer = on_pace_candidate("peer", 2);

    // When: request identities vary across the otherwise identical base buckets.
    for request_number in 0..64 {
        let request_id = format!("warning-{request_number}");
        let allowed_winner = winner(&[allowed.clone(), peer.clone()], &request_id);
        for warned in [&explicit_warning, &threshold_warning] {
            let warned_winner = winner(&[(*warned).clone(), peer.clone()], &request_id);

            // Then: neither warning form can change the deterministic zero-U tie.
            assert_eq!(warned_winner, allowed_winner);
        }
    }
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
