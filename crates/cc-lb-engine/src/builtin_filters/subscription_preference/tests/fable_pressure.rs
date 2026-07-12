use super::*;

#[test]
fn fable_pressure_uses_nested_weekly_smoothmax() {
    let candidate = oauth_at_t0(
        "fable",
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
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.10)
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
    .expect("Fable candidate is assessable");
    let shared_weekly = 0.613_409_262_276_148_f64;
    let fable_weekly = 0.795_730_819_070_102_7_f64;
    let expected_weekly =
        (shared_weekly.powf(SMOOTHMAX_P) + fable_weekly.powf(SMOOTHMAX_P)).powf(1.0 / SMOOTHMAX_P);
    let expected_combined = (0.405_465_108_108_164_4_f64.powf(SMOOTHMAX_P)
        + expected_weekly.powf(SMOOTHMAX_P))
    .powf(1.0 / SMOOTHMAX_P);

    assert!(expected_weekly > shared_weekly.max(fable_weekly));
    assert!(expected_weekly < shared_weekly + fable_weekly);
    assert!((assessment.quota_urgency_7d - expected_weekly).abs() <= 1e-12);
    assert!((assessment.quota_urgency_combined - expected_combined).abs() <= 1e-12);
}

#[test]
fn zero_fable_pressure_degenerates_to_shared_weekly_pressure() {
    let candidate = oauth_at_t0(
        "fable",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.0).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.25)
                .status("allowed")
                .reset_at(T0_SECS + 302_400)
                .build(),
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.90)
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
    .expect("Fable candidate is assessable");

    assert!((assessment.quota_urgency_7d - 0.613_409_262_276_148).abs() <= 1e-12);
}

#[test]
fn unusable_fable_observation_is_partial_and_contributes_zero_pressure() {
    let cases = [
        stale(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("rejected")
            .reset_at(T0_SECS + 302_400),
        stale(WINDOW_SEVEN_DAY_FABLE).util(0.5).status("rejected"),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .status("allowed")
            .reset_at(T0_SECS + 302_400),
        fresh(WINDOW_SEVEN_DAY_FABLE).util(0.5).status("allowed"),
        fresh(WINDOW_SEVEN_DAY_FABLE).util(0.5).status("rejected"),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("rejected")
            .reset_at(T0_SECS),
        fresh(WINDOW_SEVEN_DAY_FABLE).util(1.0).status("allowed"),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(1.0)
            .status("allowed")
            .reset_at(T0_SECS),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("allowed")
            .disabled("policy"),
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .util(0.5)
            .status("allowed")
            .disabled("policy")
            .reset_at(T0_SECS),
    ];
    for scoped in cases {
        let candidate = candidate_with_fable(scoped.build(), None);
        let assessment = assess_candidate(
            &candidate,
            0,
            &relevant_base_windows(FABLE_MODEL),
            &FilterConfig::default(),
        )
        .expect("partial Fable candidate is assessable");

        assert_eq!(assessment.tier, Tier::PartialBase);
        assert!((assessment.quota_urgency_7d - 0.613_409_262_276_148).abs() <= 1e-12);
        assert_eq!(assessment.overage_urgency, 0.0);
    }
}

#[test]
fn valid_fable_hard_negatives_retain_overage_transition() {
    for scoped in [
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

#[test]
fn fable_pressure_changes_effective_weight_and_distribution() {
    let candidate = |name, seed, fable_util| {
        oauth_at_t0(
            name,
            seed,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY_FABLE)
                    .util(fable_util)
                    .status("allowed")
                    .reset_at(T0_SECS + 60_480)
                    .build(),
            ],
        )
    };
    let pressured = candidate("pressured", 1, 0.10);
    let on_pace = candidate("on-pace", 2, 0.95);

    let output = filter_for_model(&[pressured.clone(), on_pace.clone()], FABLE_MODEL);
    let trace = output.subscription_preference.expect("trace present");
    let pressured_trace = candidate_urgency_for(&trace, pressured.upstream_id);
    let on_pace_trace = candidate_urgency_for(&trace, on_pace.upstream_id);
    let distribution = wrh_distribution(&[pressured.clone(), on_pace.clone()], FABLE_MODEL, 2_000);

    assert!(pressured_trace.quota_urgency > on_pace_trace.quota_urgency);
    assert!(pressured_trace.effective_weight > on_pace_trace.effective_weight);
    assert!(distribution[&pressured.upstream_id] > distribution[&on_pace.upstream_id]);
}

#[test]
fn fable_warning_pressure_keeps_warning_multiplier() {
    let candidate = oauth_at_t0(
        "warning",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.1)
                .status("allowed_warning")
                .reset_at(T0_SECS + 60_480)
                .build(),
        ],
    );

    let output = filter_for_model(std::slice::from_ref(&candidate), FABLE_MODEL);
    let trace = output.subscription_preference.expect("trace present");
    let candidate_trace = candidate_urgency_for(&trace, candidate.upstream_id);

    assert!(candidate_trace.quota_urgency_7d.expect("weekly pressure") > 0.0);
    assert_eq!(candidate_trace.warning_multiplier, WARNING_MULTIPLIER);
    assert!(
        (candidate_trace.effective_weight
            - candidate_trace.quota_weight_factor * WARNING_MULTIPLIER)
            .abs()
            <= 1e-12
    );
}

#[test]
fn non_exact_fable_models_ignore_scoped_pressure() {
    for model in [
        SONNET_MODEL,
        OPUS_MODEL,
        HAIKU_MODEL,
        DATED_FABLE_LIKE_MODEL,
        FUTURE_FABLE_LIKE_MODEL,
        UNKNOWN_MODEL,
    ] {
        let candidate = oauth_at_t0(
            "control",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR).util(0.9).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY).util(0.9).status("allowed").build(),
                fresh(WINDOW_SEVEN_DAY_FABLE)
                    .util(0.1)
                    .status("allowed")
                    .reset_at(T0_SECS + 60_480)
                    .build(),
            ],
        );

        let output = filter_for_model(std::slice::from_ref(&candidate), model);
        let trace = output.subscription_preference.expect("trace present");
        let candidate_trace = candidate_urgency_for(&trace, candidate.upstream_id);

        assert_eq!(candidate_trace.quota_urgency_7d, Some(0.0), "model={model}");
        assert_eq!(
            candidate_trace.quota_urgency_combined,
            Some(0.0),
            "model={model}"
        );
    }
}
