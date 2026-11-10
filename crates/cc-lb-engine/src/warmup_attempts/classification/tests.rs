use super::*;
use cc_lb_quota::rate_limit_headers::UnifiedQuotaObservation;
use cc_lb_storage_api::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
const SCHEDULED_FOR: i64 = 1_782_412_800;
const FIVE_HOUR_RESET: i64 = 1_782_430_800;
const SEVEN_DAY_RESET: i64 = 1_783_017_600;

#[derive(Clone, Copy)]
struct ExpectedAttempt {
    outcome: WarmupAttemptOutcome,
    cycle_key: Option<i64>,
}

const SEVEN_DAY_SKIPPED: ExpectedAttempt = ExpectedAttempt {
    outcome: WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted),
    cycle_key: Some(SEVEN_DAY_RESET),
};
const FIVE_HOUR_REDUNDANT: ExpectedAttempt = ExpectedAttempt {
    outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
    cycle_key: Some(FIVE_HOUR_RESET),
};
const MISSING_CYCLE_KEY: ExpectedAttempt = ExpectedAttempt {
    outcome: WarmupAttemptOutcome::TransientFailure(
        WarmupTransientFailureReason::RateLimitedCycleKeyMissing,
    ),
    cycle_key: None,
};

#[test]
fn classifies_429_with_future_seven_day_reset_as_skipped() {
    let observations = [quota_observation(
        SubscriptionQuotaWindow::SevenDay,
        SEVEN_DAY_RESET,
        Some(SubscriptionQuotaStatus::Rejected),
        None,
    )];

    assert_429(&observations, SEVEN_DAY_SKIPPED);
}

#[test]
fn classifies_429_with_fable_weekly_and_no_shared_seven_day_as_skipped() {
    let observations = [quota_observation(
        SubscriptionQuotaWindow::SevenDayFable,
        SEVEN_DAY_RESET,
        Some(SubscriptionQuotaStatus::Rejected),
        None,
    )];

    assert_429(&observations, SEVEN_DAY_SKIPPED);
}

#[test]
fn classifies_429_preserving_shared_seven_day_precedence() {
    let observations = [
        quota_observation(
            SubscriptionQuotaWindow::SevenDayFable,
            SEVEN_DAY_RESET + 10_000,
            Some(SubscriptionQuotaStatus::Rejected),
            None,
        ),
        quota_observation(
            SubscriptionQuotaWindow::SevenDay,
            SEVEN_DAY_RESET,
            Some(SubscriptionQuotaStatus::Rejected),
            None,
        ),
    ];

    assert_429(&observations, SEVEN_DAY_SKIPPED);
}

#[test]
fn classifies_429_with_future_five_hour_and_seven_day_reset_as_seven_day_skipped() {
    let observations = [
        quota_observation(
            SubscriptionQuotaWindow::FiveHour,
            FIVE_HOUR_RESET,
            None,
            None,
        ),
        quota_observation(
            SubscriptionQuotaWindow::SevenDay,
            SEVEN_DAY_RESET,
            None,
            Some(1.0),
        ),
    ];

    assert_429(&observations, SEVEN_DAY_SKIPPED);
}

#[test]
fn classifies_429_with_only_future_five_hour_reset_as_active_window() {
    let observations = [quota_observation(
        SubscriptionQuotaWindow::FiveHour,
        FIVE_HOUR_RESET,
        None,
        None,
    )];

    assert_429(&observations, FIVE_HOUR_REDUNDANT);
}

#[test]
fn classifies_429_with_allowed_seven_day_reset_as_five_hour_active() {
    let observations = [
        quota_observation(
            SubscriptionQuotaWindow::FiveHour,
            FIVE_HOUR_RESET,
            Some(SubscriptionQuotaStatus::Allowed),
            Some(0.1),
        ),
        quota_observation(
            SubscriptionQuotaWindow::SevenDay,
            SEVEN_DAY_RESET,
            Some(SubscriptionQuotaStatus::Allowed),
            Some(0.25),
        ),
    ];

    assert_429(&observations, FIVE_HOUR_REDUNDANT);
}

#[test]
fn classifies_429_without_relevant_reset_as_missing_cycle_key() {
    assert_429(&[], MISSING_CYCLE_KEY);
}

#[test]
fn success_without_target_headers_preserves_the_candidate_cycle() {
    for kind in [
        UpstreamKind::AnthropicOauth,
        UpstreamKind::OpenaiOauth,
        UpstreamKind::GeminiOauth,
    ] {
        for expected_cycle_key in [None, Some(FIVE_HOUR_RESET)] {
            let fields = response_attempt_fields(
                kind,
                StatusCode::OK,
                &[],
                expected_cycle_key,
                SCHEDULED_FOR,
                SCHEDULED_FOR,
                None,
            );

            assert_eq!(
                fields.outcome,
                WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive)
            );
            assert_eq!(
                fields.cycle_key,
                Some(expected_cycle_key.unwrap_or(SCHEDULED_FOR)),
                "kind={kind:?}",
            );
        }
    }
}

#[test]
fn successful_response_without_reset_is_authoritative_despite_exhaustion_observation() {
    for (kind, weekly) in [
        (
            UpstreamKind::AnthropicOauth,
            SubscriptionQuotaWindow::SevenDay,
        ),
        (UpstreamKind::OpenaiOauth, SubscriptionQuotaWindow::SevenDay),
        (
            UpstreamKind::GeminiOauth,
            SubscriptionQuotaWindow::GeminiSevenDay,
        ),
    ] {
        let observations = [UnifiedQuotaObservation {
            window: weekly,
            status: Some(SubscriptionQuotaStatus::Rejected),
            utilization: Some(1.0),
            ..UnifiedQuotaObservation::default()
        }];
        let fields = response_attempt_fields(
            kind,
            StatusCode::OK,
            &observations,
            Some(SCHEDULED_FOR),
            SCHEDULED_FOR,
            SCHEDULED_FOR,
            None,
        );
        assert_eq!(
            fields.outcome,
            WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
            "kind={kind:?}",
        );
        assert_eq!(fields.cycle_key, Some(SCHEDULED_FOR));
    }
}

#[test]
fn success_with_five_hour_headers_advances_the_cycle() {
    let observations = [quota_observation(
        SubscriptionQuotaWindow::FiveHour,
        FIVE_HOUR_RESET,
        Some(SubscriptionQuotaStatus::Allowed),
        Some(0.0),
    )];
    let fields = response_attempt_fields(
        UpstreamKind::AnthropicOauth,
        StatusCode::OK,
        &observations,
        Some(SCHEDULED_FOR),
        SCHEDULED_FOR,
        SCHEDULED_FOR,
        None,
    );

    assert_eq!(
        fields.outcome,
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced)
    );
    assert_eq!(fields.cycle_key, Some(FIVE_HOUR_RESET));
}

#[test]
fn success_with_only_seven_day_headers_advances_weekly_only_accounts() {
    for kind in [UpstreamKind::AnthropicOauth, UpstreamKind::OpenaiOauth] {
        let observations = [quota_observation(
            SubscriptionQuotaWindow::SevenDay,
            SEVEN_DAY_RESET,
            Some(SubscriptionQuotaStatus::Allowed),
            Some(0.01),
        )];
        let fields = response_attempt_fields(
            kind,
            StatusCode::OK,
            &observations,
            Some(SCHEDULED_FOR),
            SCHEDULED_FOR,
            SCHEDULED_FOR,
            None,
        );
        assert_eq!(
            fields.outcome,
            WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced)
        );
        assert_eq!(fields.cycle_key, Some(SEVEN_DAY_RESET));
    }
}

#[test]
fn success_chooses_earliest_future_target_and_ignores_expired_resets() {
    for kind in [UpstreamKind::AnthropicOauth, UpstreamKind::OpenaiOauth] {
        let observations = [
            quota_observation(
                SubscriptionQuotaWindow::FiveHour,
                SCHEDULED_FOR - 1,
                None,
                None,
            ),
            quota_observation(
                SubscriptionQuotaWindow::SevenDay,
                SEVEN_DAY_RESET,
                None,
                None,
            ),
        ];
        let fields = response_attempt_fields(
            kind,
            StatusCode::OK,
            &observations,
            Some(SCHEDULED_FOR),
            SCHEDULED_FOR,
            SCHEDULED_FOR,
            None,
        );
        assert_eq!(fields.cycle_key, Some(SEVEN_DAY_RESET));
        let observations = [
            quota_observation(
                SubscriptionQuotaWindow::FiveHour,
                FIVE_HOUR_RESET,
                None,
                None,
            ),
            quota_observation(
                SubscriptionQuotaWindow::SevenDay,
                FIVE_HOUR_RESET - 1,
                None,
                None,
            ),
        ];
        let fields = response_attempt_fields(
            kind,
            StatusCode::OK,
            &observations,
            Some(SCHEDULED_FOR),
            SCHEDULED_FOR,
            SCHEDULED_FOR,
            None,
        );
        assert_eq!(fields.cycle_key, Some(FIVE_HOUR_RESET - 1));
    }
}

#[test]
fn gemini_success_uses_only_gemini_group_resets() {
    let observations = [
        quota_observation(
            SubscriptionQuotaWindow::GeminiFiveHour,
            FIVE_HOUR_RESET,
            None,
            None,
        ),
        quota_observation(
            SubscriptionQuotaWindow::GeminiSevenDay,
            SEVEN_DAY_RESET,
            None,
            None,
        ),
        quota_observation(
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            FIVE_HOUR_RESET - 1,
            None,
            None,
        ),
    ];
    let fields = response_attempt_fields(
        UpstreamKind::GeminiOauth,
        StatusCode::OK,
        &observations,
        Some(SCHEDULED_FOR),
        SCHEDULED_FOR,
        SCHEDULED_FOR,
        None,
    );
    assert_eq!(fields.cycle_key, Some(FIVE_HOUR_RESET));
}

#[test]
fn gemini_429_with_active_target_is_transient() {
    let observations = [
        quota_observation(
            SubscriptionQuotaWindow::GeminiFiveHour,
            FIVE_HOUR_RESET,
            None,
            None,
        ),
        quota_observation(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            SEVEN_DAY_RESET,
            Some(SubscriptionQuotaStatus::Rejected),
            Some(1.0),
        ),
    ];
    let fields = response_attempt_fields(
        UpstreamKind::GeminiOauth,
        StatusCode::TOO_MANY_REQUESTS,
        &observations,
        None,
        SCHEDULED_FOR,
        SCHEDULED_FOR,
        None,
    );
    assert_eq!(fields.outcome, MISSING_CYCLE_KEY.outcome);
    assert_eq!(fields.cycle_key, None);
}

#[test]
fn gemini_429_exhausted_weekly_is_skipped() {
    let observations = [quota_observation(
        SubscriptionQuotaWindow::GeminiSevenDay,
        SEVEN_DAY_RESET,
        Some(SubscriptionQuotaStatus::Rejected),
        Some(1.0),
    )];
    let fields = response_attempt_fields(
        UpstreamKind::GeminiOauth,
        StatusCode::TOO_MANY_REQUESTS,
        &observations,
        None,
        SCHEDULED_FOR,
        SCHEDULED_FOR,
        None,
    );
    assert_eq!(fields.outcome, SEVEN_DAY_SKIPPED.outcome);
    assert_eq!(fields.cycle_key, Some(SEVEN_DAY_RESET));
}

#[test]
fn idle_uses_five_hour_reset_for_normal_account() {
    let latest = [
        quota_sample(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaSampleKind::Sample,
            20,
            Some(FIVE_HOUR_RESET),
        ),
        quota_sample(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaSampleKind::Sample,
            10,
            Some(SEVEN_DAY_RESET),
        ),
    ];

    assert_eq!(
        super::super::idle_secs_since_prev_window_from_latest(&latest, SEVEN_DAY_RESET),
        Some(SEVEN_DAY_RESET - FIVE_HOUR_RESET)
    );
}

#[test]
fn idle_uses_seven_day_reset_only_for_explicitly_absent_five_hour() {
    let latest = [
        quota_sample(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaSampleKind::Absent,
            20,
            None,
        ),
        quota_sample(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaSampleKind::Sample,
            10,
            Some(SEVEN_DAY_RESET),
        ),
    ];

    assert_eq!(
        super::super::idle_secs_since_prev_window_from_latest(&latest, SEVEN_DAY_RESET + 100),
        Some(100)
    );
}

#[test]
fn idle_does_not_use_seven_day_reset_for_unobserved_five_hour() {
    let latest = [quota_sample(
        SubscriptionQuotaWindow::SevenDay,
        SubscriptionQuotaSampleKind::Sample,
        10,
        Some(SEVEN_DAY_RESET),
    )];

    assert_eq!(
        super::super::idle_secs_since_prev_window_from_latest(&latest, SEVEN_DAY_RESET + 100),
        None
    );
}

fn quota_sample(
    window: SubscriptionQuotaWindow,
    sample_kind: SubscriptionQuotaSampleKind,
    observed_at_unix_millis: u64,
    resets_at_unix_secs: Option<i64>,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id: uuid::Uuid::new_v4(),
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind,
        observed_at_unix_millis,
        sample_id: uuid::Uuid::new_v4(),
        utilization: None,
        status: None,
        resets_at_unix_secs: resets_at_unix_secs.and_then(|value| u64::try_from(value).ok()),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

#[test]
fn openai_429_with_active_target_is_transient() {
    let observations = [quota_observation(
        SubscriptionQuotaWindow::SevenDay,
        SEVEN_DAY_RESET,
        Some(SubscriptionQuotaStatus::Allowed),
        Some(0.2),
    )];
    let fields = response_attempt_fields(
        UpstreamKind::OpenaiOauth,
        StatusCode::TOO_MANY_REQUESTS,
        &observations,
        None,
        SCHEDULED_FOR,
        SCHEDULED_FOR,
        None,
    );
    assert_eq!(fields.outcome, MISSING_CYCLE_KEY.outcome);
    assert_eq!(fields.cycle_key, None);
}

fn assert_429(observations: &[UnifiedQuotaObservation], expected: ExpectedAttempt) {
    let fields = response_attempt_fields(
        UpstreamKind::AnthropicOauth,
        StatusCode::TOO_MANY_REQUESTS,
        observations,
        None,
        SCHEDULED_FOR,
        SCHEDULED_FOR,
        None,
    );

    assert_eq!(fields.outcome, expected.outcome);
    assert_eq!(fields.cycle_key, expected.cycle_key);
}

fn quota_observation(
    window: SubscriptionQuotaWindow,
    resets_at_unix_secs: i64,
    status: Option<SubscriptionQuotaStatus>,
    utilization: Option<f64>,
) -> UnifiedQuotaObservation {
    UnifiedQuotaObservation {
        window,
        utilization,
        status,
        resets_at_unix_secs: u64::try_from(resets_at_unix_secs).ok(),
        ..UnifiedQuotaObservation::default()
    }
}
