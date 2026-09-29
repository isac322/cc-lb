use cc_lb_clock::{Clock, unix_secs};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::WarmupPermanentFailureReason;

use crate::warmup::execute::WarmupAttemptExecutionResult;

use super::request::{WarmupDispatchAttempt, WarmupDispatchResult};

pub(super) fn response_is_auth_failed(attempt: &WarmupDispatchAttempt) -> bool {
    matches!(
        attempt.result,
        WarmupDispatchResult::Response { status, .. } if status == http::StatusCode::UNAUTHORIZED
    )
}

pub(super) fn execution_result_from_dispatch_attempt(
    attempt: &WarmupDispatchAttempt,
) -> WarmupAttemptExecutionResult<'_> {
    match &attempt.result {
        WarmupDispatchResult::Response {
            status,
            observations,
        } => WarmupAttemptExecutionResult::Response {
            status: *status,
            observations,
            error_detail: None,
        },
        WarmupDispatchResult::TransientFailure {
            reason,
            error_detail,
        } => WarmupAttemptExecutionResult::TransientFailure {
            reason: *reason,
            http_status: None,
            error_detail: Some(error_detail.as_str()),
        },
        WarmupDispatchResult::PermanentFailure {
            reason,
            error_detail,
        } => WarmupAttemptExecutionResult::PermanentFailure {
            reason: *reason,
            http_status: None,
            error_detail: Some(error_detail.as_str()),
        },
    }
}

pub(super) fn transient_attempt_error(
    attempt: &WarmupDispatchAttempt,
    http_status: Option<i32>,
) -> String {
    match &attempt.result {
        WarmupDispatchResult::Response { status, .. } => {
            format!("warmup returned transient status {status}")
        }
        WarmupDispatchResult::TransientFailure { error_detail, .. } => error_detail.clone(),
        WarmupDispatchResult::PermanentFailure { .. } => http_status
            .map(|status| format!("warmup returned transient status {status}"))
            .unwrap_or_else(|| "warmup returned transient failure".to_owned()),
    }
}

pub(super) fn pre_request_error_reason(
    error: &SchedulerError,
) -> Option<WarmupPermanentFailureReason> {
    let detail = error.to_string();
    if detail.contains("oauth decrypt failed") {
        return Some(WarmupPermanentFailureReason::CredentialDecryptFailed);
    }
    if detail.contains("missing oauth credentials") {
        return Some(WarmupPermanentFailureReason::OauthCredentialsMissing);
    }
    None
}

pub(super) fn cycle_key_i64(cycle_key: u64) -> SchedulerResult<i64> {
    i64::try_from(cycle_key)
        .map_err(|_| SchedulerError::Job("warmup cycle key exceeds i64".to_owned()))
}

pub(super) fn now_unix_secs_i64(clock: &dyn Clock) -> SchedulerResult<i64> {
    i64::try_from(unix_secs(clock.now()))
        .map_err(|_| SchedulerError::Job("current unix timestamp exceeds i64".to_owned()))
}
