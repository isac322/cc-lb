use super::*;
use cc_lb_quota::rate_limit_headers::UnifiedQuotaObservation;
use cc_lb_storage_api::SubscriptionQuotaStatus;

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

fn assert_429(observations: &[UnifiedQuotaObservation], expected: ExpectedAttempt) {
    let fields = response_attempt_fields(
        StatusCode::TOO_MANY_REQUESTS,
        observations,
        None,
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
