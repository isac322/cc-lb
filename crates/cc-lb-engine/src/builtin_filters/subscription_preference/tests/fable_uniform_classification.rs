use super::*;

#[test]
fn fable_observation_tier_uses_uniform_base_classification() {
    let cases = [
        (
            stale(WINDOW_SEVEN_DAY_FABLE)
                .util(0.5)
                .status("rejected")
                .reset_at(T0_SECS + 302_400),
            Tier::Overage,
        ),
        (
            stale(WINDOW_SEVEN_DAY_FABLE).util(0.5).status("rejected"),
            Tier::Overage,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .status("allowed")
                .reset_at(T0_SECS + 302_400),
            Tier::KnownBase,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE).util(0.5).status("allowed"),
            Tier::KnownBase,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE).util(0.5).status("rejected"),
            Tier::Overage,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.5)
                .status("rejected")
                .reset_at(T0_SECS),
            Tier::Overage,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE).util(1.0).status("allowed"),
            Tier::Overage,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(1.0)
                .status("allowed")
                .reset_at(T0_SECS),
            Tier::Overage,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.5)
                .status("allowed")
                .disabled("policy"),
            Tier::Overage,
        ),
        (
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.5)
                .status("allowed")
                .disabled("policy")
                .reset_at(T0_SECS),
            Tier::Overage,
        ),
    ];
    for (scoped, expected_tier) in cases {
        let candidate = candidate_with_fable(
            scoped.build(),
            Some(fresh(WINDOW_OVERAGE).util(0.5).status("allowed").build()),
        );
        let assessment = assess_candidate(
            &candidate,
            0,
            &relevant_base_windows(FABLE_MODEL),
            &FilterConfig::default(),
        )
        .expect("Fable candidate is assessable");

        assert_eq!(assessment.tier, expected_tier);
    }
}

#[test]
fn stale_nonrejected_fable_remains_partial_base() {
    let candidate = candidate_with_fable(
        stale(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("allowed")
            .build(),
        None,
    );

    let assessment = assess_candidate(
        &candidate,
        0,
        &relevant_base_windows(FABLE_MODEL),
        &FilterConfig::default(),
    )
    .expect("partial Fable candidate is assessable");

    assert_eq!(assessment.tier, Tier::PartialBase);
}

#[test]
fn absent_fable_window_is_noop() {
    let candidate = oauth_at_t0(
        "absent-fable",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
            absent(WINDOW_SEVEN_DAY_FABLE).build(),
        ],
    );

    let assessment = assess_candidate(
        &candidate,
        0,
        &relevant_base_windows(FABLE_MODEL),
        &FilterConfig::default(),
    )
    .expect("candidate with a proven-absent Fable window is assessable");

    assert_eq!(assessment.tier, Tier::KnownBase);
}

#[test]
fn unobserved_fable_window_remains_partial_base() {
    let candidate = oauth_at_t0(
        "unobserved-fable",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
        ],
    );

    let assessment = assess_candidate(
        &candidate,
        0,
        &relevant_base_windows(FABLE_MODEL),
        &FilterConfig::default(),
    )
    .expect("candidate without a Fable observation is assessable");

    assert_eq!(assessment.tier, Tier::PartialBase);
}

#[test]
fn fable_hard_negatives_with_live_reset_retain_overage_transition() {
    for scoped in [
        stale(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("rejected")
            .reset_at(T0_SECS + 302_400),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("rejected")
            .reset_at(T0_SECS + 302_400),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(1.0)
            .status("allowed")
            .reset_at(T0_SECS + 302_400),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("allowed")
            .disabled("policy")
            .reset_at(T0_SECS + 302_400),
    ] {
        let overage = fresh(WINDOW_OVERAGE).util(0.5).status("allowed").build();
        let candidate = candidate_with_fable(scoped.build(), Some(overage));
        let assessment = assess_candidate(
            &candidate,
            0,
            &relevant_base_windows(FABLE_MODEL),
            &FilterConfig::default(),
        )
        .expect("valid hard negative uses overage");

        assert_eq!(assessment.tier, Tier::Overage);
        assert_eq!(assessment.overage_urgency, overage_urgency(Some(0.5)));
    }
}

fn candidate_with_fable(
    scoped: SubscriptionQuotaCandidateSnapshot,
    overage: Option<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    let mut quotas = vec![
        fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
        fresh(WINDOW_SEVEN_DAY)
            .util(0.25)
            .status("allowed")
            .reset_at(T0_SECS + 302_400)
            .build(),
        scoped,
    ];
    quotas.extend(overage);
    oauth_at_t0("fable", 1, quotas)
}
