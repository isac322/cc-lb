use super::*;

#[test]
fn resetless_allowed_fable_counts_as_known_base() {
    let candidate = oauth_at_t0(
        "resetless-fable",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.2)
                .status("allowed")
                .reset_at(T0_SECS + 9_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.0)
                .status("allowed")
                .build(),
        ],
    );

    let assessment = assess_candidate(
        &candidate,
        0,
        &relevant_base_windows(FABLE_MODEL),
        &FilterConfig::default(),
    )
    .expect("resetless Fable candidate is assessable");

    assert_eq!(assessment.tier, Tier::KnownBase);
}

#[test]
fn resetless_allowed_fable_does_not_increase_combined_urgency() {
    let base_quotas = vec![
        fresh(WINDOW_FIVE_HOUR)
            .util(0.2)
            .status("allowed")
            .reset_at(T0_SECS + 9_000)
            .build(),
        fresh(WINDOW_SEVEN_DAY)
            .util(0.25)
            .status("allowed")
            .reset_at(T0_SECS + 302_400)
            .build(),
    ];
    let without_fable = oauth_at_t0("without-fable", 1, base_quotas.clone());
    let mut resetless_fable_quotas = base_quotas;
    resetless_fable_quotas.push(
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(0.0)
            .status("allowed")
            .build(),
    );
    let with_resetless_fable = oauth_at_t0("with-resetless-fable", 2, resetless_fable_quotas);

    let without_fable_assessment = assess_candidate(
        &without_fable,
        0,
        &relevant_base_windows(FABLE_MODEL),
        &FilterConfig::default(),
    )
    .expect("candidate without Fable is assessable");
    let with_resetless_fable_assessment = assess_candidate(
        &with_resetless_fable,
        0,
        &relevant_base_windows(FABLE_MODEL),
        &FilterConfig::default(),
    )
    .expect("candidate with resetless Fable is assessable");

    assert_eq!(
        with_resetless_fable_assessment.quota_urgency_combined,
        without_fable_assessment.quota_urgency_combined
    );
}
