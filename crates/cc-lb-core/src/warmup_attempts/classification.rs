use cc_lb_storage_api::{
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, WarmupAttemptOutcome,
    WarmupPermanentFailureReason, WarmupSkipReason, WarmupSuccessReason,
    WarmupTransientFailureReason,
};
use http::StatusCode;

use super::{WarmupAttemptExecution, WarmupAttemptExecutionResult};

#[derive(Clone, Debug)]
pub(super) struct AttemptFields {
    pub(super) outcome: WarmupAttemptOutcome,
    pub(super) http_status: Option<i32>,
    pub(super) cycle_key: Option<i64>,
    pub(super) error_detail: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseClassification {
    SuccessNewCycle {
        cycle_key: i64,
    },
    WindowAlreadyActive {
        cycle_key: i64,
    },
    SevenDayExhausted {
        cycle_key: i64,
    },
    Transient {
        reason: WarmupTransientFailureReason,
    },
    Permanent {
        reason: WarmupPermanentFailureReason,
    },
}

pub(super) fn attempt_fields(execution: &WarmupAttemptExecution<'_>) -> AttemptFields {
    match execution.result {
        WarmupAttemptExecutionResult::Response {
            status,
            observations,
            error_detail,
        } => response_attempt_fields(
            status,
            observations,
            execution.expected_cycle_key,
            execution.scheduled_for_unix_secs,
            error_detail,
        ),
        WarmupAttemptExecutionResult::TransientFailure {
            reason,
            http_status,
            error_detail,
        } => AttemptFields {
            outcome: WarmupAttemptOutcome::TransientFailure(reason),
            http_status: http_status.map(status_to_i32),
            cycle_key: None,
            error_detail: error_detail.map(ToOwned::to_owned),
        },
        WarmupAttemptExecutionResult::PermanentFailure {
            reason,
            http_status,
            error_detail,
        } => AttemptFields {
            outcome: WarmupAttemptOutcome::PermanentFailure(reason),
            http_status: http_status.map(status_to_i32),
            cycle_key: None,
            error_detail: error_detail.map(ToOwned::to_owned),
        },
        WarmupAttemptExecutionResult::Skipped {
            reason,
            cycle_key,
            error_detail,
        } => AttemptFields {
            outcome: WarmupAttemptOutcome::Skipped(reason),
            http_status: None,
            cycle_key,
            error_detail: error_detail.map(ToOwned::to_owned),
        },
        WarmupAttemptExecutionResult::PreflightActiveWindow { cycle_key } => AttemptFields {
            outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
            http_status: None,
            cycle_key: Some(cycle_key),
            error_detail: None,
        },
    }
}

fn response_attempt_fields(
    status: StatusCode,
    observations: &[crate::UnifiedQuotaObservation],
    expected_cycle_key: Option<i64>,
    scheduled_for_unix_secs: i64,
    error_detail: Option<&str>,
) -> AttemptFields {
    let candidate_cycle_key = expected_cycle_key.unwrap_or(scheduled_for_unix_secs);
    let http_status = Some(status_to_i32(status));
    let owned_detail = error_detail.map(ToOwned::to_owned);
    match classify_response(status, observations, candidate_cycle_key) {
        ResponseClassification::SuccessNewCycle { cycle_key } => {
            let success_reason = match expected_cycle_key {
                Some(expected) if cycle_key > expected => WarmupSuccessReason::CycleAdvanced,
                Some(_) | None => WarmupSuccessReason::WindowAlreadyActive,
            };
            AttemptFields {
                outcome: WarmupAttemptOutcome::Success(success_reason),
                http_status,
                cycle_key: Some(cycle_key),
                error_detail: owned_detail,
            }
        }
        ResponseClassification::WindowAlreadyActive { cycle_key } => AttemptFields {
            outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
            http_status,
            cycle_key: Some(cycle_key),
            error_detail: owned_detail,
        },
        ResponseClassification::SevenDayExhausted { cycle_key } => AttemptFields {
            outcome: WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted),
            http_status,
            cycle_key: Some(cycle_key),
            error_detail: owned_detail,
        },
        ResponseClassification::Transient { reason } => AttemptFields {
            outcome: WarmupAttemptOutcome::TransientFailure(reason),
            http_status,
            cycle_key: None,
            error_detail: owned_detail,
        },
        ResponseClassification::Permanent { reason } => AttemptFields {
            outcome: WarmupAttemptOutcome::PermanentFailure(reason),
            http_status,
            cycle_key: None,
            error_detail: owned_detail,
        },
    }
}

fn classify_response(
    status: StatusCode,
    observations: &[crate::UnifiedQuotaObservation],
    candidate_cycle_key: i64,
) -> ResponseClassification {
    if status.is_success() {
        return ResponseClassification::SuccessNewCycle {
            cycle_key: five_hour_cycle_key(observations).unwrap_or(candidate_cycle_key),
        };
    }
    match status {
        StatusCode::TOO_MANY_REQUESTS => {
            if let Some(cycle_key) = seven_day_exhausted_cycle_key(observations)
                && cycle_key > candidate_cycle_key
            {
                return ResponseClassification::SevenDayExhausted { cycle_key };
            }

            match five_hour_cycle_key(observations) {
                Some(cycle_key) if cycle_key > candidate_cycle_key => {
                    ResponseClassification::WindowAlreadyActive { cycle_key }
                }
                Some(_) | None => ResponseClassification::Transient {
                    reason: WarmupTransientFailureReason::RateLimitedCycleKeyMissing,
                },
            }
        }
        StatusCode::UNAUTHORIZED => ResponseClassification::Permanent {
            reason: WarmupPermanentFailureReason::AuthFailed,
        },
        StatusCode::FORBIDDEN => ResponseClassification::Permanent {
            reason: WarmupPermanentFailureReason::Forbidden,
        },
        StatusCode::BAD_REQUEST => ResponseClassification::Permanent {
            reason: WarmupPermanentFailureReason::BadRequest,
        },
        StatusCode::NOT_FOUND => ResponseClassification::Permanent {
            reason: WarmupPermanentFailureReason::NotFound,
        },
        status if status.is_server_error() => ResponseClassification::Transient {
            reason: WarmupTransientFailureReason::Upstream5xx,
        },
        _ => ResponseClassification::Transient {
            reason: WarmupTransientFailureReason::NetworkError,
        },
    }
}

fn five_hour_cycle_key(observations: &[crate::UnifiedQuotaObservation]) -> Option<i64> {
    cycle_key_for_window(observations, SubscriptionQuotaWindow::FiveHour)
}

fn seven_day_exhausted_cycle_key(observations: &[crate::UnifiedQuotaObservation]) -> Option<i64> {
    const SEVEN_DAY_WINDOWS: [SubscriptionQuotaWindow; 3] = [
        SubscriptionQuotaWindow::SevenDay,
        SubscriptionQuotaWindow::SevenDaySonnet,
        SubscriptionQuotaWindow::SevenDayOpus,
    ];

    SEVEN_DAY_WINDOWS
        .into_iter()
        .find_map(|window| exhausted_cycle_key_for_window(observations, window))
}

fn exhausted_cycle_key_for_window(
    observations: &[crate::UnifiedQuotaObservation],
    window: SubscriptionQuotaWindow,
) -> Option<i64> {
    observations
        .iter()
        .find(|observation| {
            observation.window == window && quota_observation_is_exhausted(observation)
        })
        .and_then(|observation| observation.resets_at_unix_secs)
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

fn quota_observation_is_exhausted(observation: &crate::UnifiedQuotaObservation) -> bool {
    observation.status == Some(SubscriptionQuotaStatus::Rejected)
        || observation
            .utilization
            .is_some_and(|utilization| utilization >= 1.0)
}

fn cycle_key_for_window(
    observations: &[crate::UnifiedQuotaObservation],
    window: SubscriptionQuotaWindow,
) -> Option<i64> {
    observations
        .iter()
        .find(|observation| observation.window == window)
        .and_then(|observation| observation.resets_at_unix_secs)
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

fn status_to_i32(status: StatusCode) -> i32 {
    i32::from(status.as_u16())
}

#[cfg(test)]
mod tests;
