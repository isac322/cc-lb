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
fn fable_pressure_changes_quota_urgency_and_selection() {
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
    let selected = filter_for_model(&[pressured.clone(), on_pace.clone()], FABLE_MODEL);

    assert!(pressured_trace.quota_urgency > on_pace_trace.quota_urgency);
    assert_eq!(selected.kept_upstream_ids, vec![pressured.upstream_id]);
}

#[test]
fn fable_warning_preserves_scoped_pressure_without_base_penalty() {
    let allowed = oauth_at_t0(
        "allowed",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY).util(0.2).status("allowed").build(),
            fresh(WINDOW_SEVEN_DAY_FABLE)
                .util(0.1)
                .status("allowed")
                .reset_at(T0_SECS + 60_480)
                .build(),
        ],
    );
    let mut warning = allowed.clone();
    warning.subscription_quotas[2].status = Some("allowed_warning".to_owned());

    let allowed_output = filter_for_model(std::slice::from_ref(&allowed), FABLE_MODEL);
    let warning_output = filter_for_model(std::slice::from_ref(&warning), FABLE_MODEL);
    let allowed_trace = allowed_output
        .subscription_preference
        .expect("trace present");
    let warning_trace = warning_output
        .subscription_preference
        .expect("trace present");
    let allowed_candidate = candidate_urgency_for(&allowed_trace, allowed.upstream_id);
    let warning_candidate = candidate_urgency_for(&warning_trace, warning.upstream_id);

    assert!(warning_candidate.quota_urgency_7d.expect("weekly pressure") > 0.0);
    assert_eq!(
        warning_candidate.quota_urgency_7d,
        allowed_candidate.quota_urgency_7d
    );
    assert_eq!(warning_candidate.warning_multiplier, 1.0);
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
