use super::*;

const HOUR: u64 = 3_600;

fn assess_for_model(candidate: &UpstreamCandidate, model: &str) -> (f64, f64) {
    let assessment = assess_candidate(
        candidate,
        0,
        &relevant_base_windows(model),
        &FilterConfig::default(),
    )
    .expect("candidate is assessable");
    (
        assessment.quota_urgency_5h,
        assessment.five_hour_pressure_raw,
    )
}

fn five_hour_expiring_soon(
    weekly: SubscriptionQuotaCandidateSnapshot,
) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        fresh(WINDOW_FIVE_HOUR)
            .util(0.0)
            .status("allowed")
            .reset_at(T0_SECS + 600)
            .build(),
        weekly,
    ]
}

#[test]
fn ahead_of_pace_weekly_account_cannot_steal_with_expiring_five_hour_window() {
    // Given: the incident shape. "weekly-behind" still has most of its weekly
    // quota left 21h before the weekly reset, while "weekly-ahead" has already
    // used 74% of its weekly quota with 88h left and its 5h window resets soon.
    let weekly_behind = oauth_at_t0(
        "weekly-behind",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.58)
                .status("allowed")
                .reset_at(T0_SECS + 1_152)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.17)
                .status("allowed")
                .reset_at(T0_SECS + 76_752)
                .build(),
        ],
    );
    let weekly_ahead = oauth_at_t0(
        "weekly-ahead",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.02)
                .status("allowed")
                .reset_at(T0_SECS + 1_152)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .util(0.74)
                .status("allowed")
                .reset_at(T0_SECS + 317_952)
                .build(),
        ],
    );

    // When: the filter ranks both cold candidates.
    let output = filter_for_model(
        &[weekly_ahead.clone(), weekly_behind.clone()],
        MODEL_AGNOSTIC,
    );

    // Then: the ahead-of-pace account's 5h pressure is suppressed and the
    // account that can still forfeit weekly quota wins.
    let trace = output.subscription_preference.expect("trace present");
    let ahead = candidate_urgency_for(&trace, weekly_ahead.upstream_id);
    let behind = candidate_urgency_for(&trace, weekly_behind.upstream_id);
    assert_eq!(ahead.quota_urgency_5h, Some(0.0));
    assert!(behind.quota_urgency > ahead.quota_urgency);
    assert_eq!(output.kept_upstream_ids, vec![weekly_behind.upstream_id]);
    assert_eq!(trace.formula_version.as_deref(), Some("cost-first-v2"));
}

#[test]
fn gate_boundary_is_linear_weekly_pace_to_end_of_current_five_hour_window() {
    // Given: 84h between the end of the current 5h window and the weekly
    // reset, so linear pace leaves exactly half of the weekly quota.
    let weekly_reset = T0_SECS + 600 + 84 * HOUR;
    let behind = oauth_at_t0(
        "behind",
        1,
        five_hour_expiring_soon(
            fresh(WINDOW_SEVEN_DAY)
                .util(0.49)
                .status("allowed")
                .reset_at(weekly_reset)
                .build(),
        ),
    );
    let at_pace = oauth_at_t0(
        "at-pace",
        2,
        five_hour_expiring_soon(
            fresh(WINDOW_SEVEN_DAY)
                .util(0.50)
                .status("allowed")
                .reset_at(weekly_reset)
                .build(),
        ),
    );

    // When / Then: strictly behind pace keeps the pressure; at pace suppresses
    // it while the raw value stays available for tiebreaks.
    let (behind_effective, behind_raw) = assess_for_model(&behind, MODEL_AGNOSTIC);
    let (at_pace_effective, at_pace_raw) = assess_for_model(&at_pace, MODEL_AGNOSTIC);
    assert!(behind_effective > 0.0);
    assert_eq!(behind_effective, behind_raw);
    assert_eq!(at_pace_effective, 0.0);
    assert_eq!(at_pace_raw, behind_raw);
}

#[test]
fn unusable_shared_weekly_keeps_five_hour_pressure() {
    // Given: weekly usage far ahead of pace, but the weekly snapshot cannot
    // be trusted (stale) or has no reset time.
    let weekly_reset = T0_SECS + 6 * 86_400;
    let stale_weekly = oauth_at_t0(
        "stale-weekly",
        1,
        five_hour_expiring_soon(
            stale(WINDOW_SEVEN_DAY)
                .util(0.9)
                .status("allowed")
                .reset_at(weekly_reset)
                .build(),
        ),
    );
    let resetless_weekly = oauth_at_t0(
        "resetless-weekly",
        2,
        five_hour_expiring_soon(fresh(WINDOW_SEVEN_DAY).util(0.9).status("allowed").build()),
    );

    // When / Then: the gate falls back to the unchanged 5h pressure.
    for candidate in [&stale_weekly, &resetless_weekly] {
        let (effective, raw) = assess_for_model(candidate, MODEL_AGNOSTIC);
        assert!(raw > 0.0);
        assert_eq!(effective, raw);
    }
}

#[test]
fn model_scoped_weekly_window_can_keep_but_never_suppress_five_hour_pressure() {
    let weekly_reset = T0_SECS + 6 * 86_400;
    let weekly = |window: &str, util: f64| {
        fresh(window)
            .util(util)
            .status("allowed")
            .reset_at(weekly_reset)
            .build()
    };
    let five_hour = || {
        fresh(WINDOW_FIVE_HOUR)
            .util(0.0)
            .status("allowed")
            .reset_at(T0_SECS + 600)
            .build()
    };
    // Given: shared weekly ahead of pace, Fable-scoped weekly behind pace.
    let scoped_behind = oauth_at_t0(
        "scoped-behind",
        1,
        vec![
            five_hour(),
            weekly(WINDOW_SEVEN_DAY, 0.9),
            weekly(WINDOW_SEVEN_DAY_FABLE, 0.1),
        ],
    );
    // Given: shared weekly behind pace, Fable-scoped weekly ahead of pace.
    let scoped_ahead = oauth_at_t0(
        "scoped-ahead",
        2,
        vec![
            five_hour(),
            weekly(WINDOW_SEVEN_DAY, 0.1),
            weekly(WINDOW_SEVEN_DAY_FABLE, 0.9),
        ],
    );

    // Then: a relevant scoped window behind pace keeps the pressure.
    let (effective, raw) = assess_for_model(&scoped_behind, FABLE_MODEL);
    assert!(raw > 0.0);
    assert_eq!(effective, raw);
    // Then: the scoped window is ignored for requests it does not govern.
    let (effective, _) = assess_for_model(&scoped_behind, MODEL_AGNOSTIC);
    assert_eq!(effective, 0.0);
    // Then: a scoped window ahead of pace never suppresses on its own.
    let (effective, raw) = assess_for_model(&scoped_ahead, FABLE_MODEL);
    assert!(raw > 0.0);
    assert_eq!(effective, raw);
}

#[test]
fn raw_five_hour_pressure_breaks_ties_between_suppressed_candidates() {
    // Given: two ahead-of-pace candidates with zero effective urgency. The
    // lower upstream ID would win the ID tiebreak, but its 5h window refills
    // much later.
    let weekly_reset = T0_SECS + 6 * 86_400;
    let weekly = || {
        fresh(WINDOW_SEVEN_DAY)
            .util(0.9)
            .status("allowed")
            .reset_at(weekly_reset)
            .build()
    };
    let refills_late = oauth_at_t0(
        "refills-late",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.0)
                .status("allowed")
                .reset_at(T0_SECS + 4 * HOUR)
                .build(),
            weekly(),
        ],
    );
    let refills_soon = oauth_at_t0(
        "refills-soon",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .util(0.0)
                .status("allowed")
                .reset_at(T0_SECS + 300)
                .build(),
            weekly(),
        ],
    );

    // When: the filter ranks the tied bucket.
    let output = filter_for_model(
        &[refills_late.clone(), refills_soon.clone()],
        MODEL_AGNOSTIC,
    );

    // Then: both are suppressed and the sooner-refilling window wins.
    let trace = output.subscription_preference.expect("trace present");
    for candidate in &trace.candidates {
        assert_eq!(candidate.quota_urgency, 0.0);
    }
    assert_eq!(output.kept_upstream_ids, vec![refills_soon.upstream_id]);
}
