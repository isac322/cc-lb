use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::WarmupAttemptReason;

use crate::scheduler_dispatch::time::now_unix_secs;
use crate::warmup::execute::WarmupAttemptExecutionResult;
use crate::warmup::{WarmupAbandonReason, WarmupResult, classify_response};

use super::WarmupDispatchAttempt;

pub(super) fn response_is_auth_failed(
    attempt: &WarmupDispatchAttempt,
    expected_cycle_key: i64,
) -> bool {
    match attempt {
        WarmupDispatchAttempt::Response {
            status,
            observations,
        } => matches!(
            classify_response(*status, observations, expected_cycle_key),
            WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::AuthFailed)
        ),
        WarmupDispatchAttempt::TransientFailure { .. }
        | WarmupDispatchAttempt::PermanentFailure { .. } => false,
    }
}

pub(super) fn execution_result_from_dispatch_attempt(
    attempt: &WarmupDispatchAttempt,
) -> WarmupAttemptExecutionResult<'_> {
    match attempt {
        WarmupDispatchAttempt::Response {
            status,
            observations,
        } => WarmupAttemptExecutionResult::Response {
            status: *status,
            observations,
            error_detail: None,
        },
        WarmupDispatchAttempt::TransientFailure {
            reason,
            error_detail,
        } => WarmupAttemptExecutionResult::TransientFailure {
            reason: *reason,
            http_status: None,
            error_detail: Some(error_detail.as_str()),
        },
        WarmupDispatchAttempt::PermanentFailure {
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
    match attempt {
        WarmupDispatchAttempt::Response { status, .. } => {
            format!("warmup returned transient status {status}")
        }
        WarmupDispatchAttempt::TransientFailure { error_detail, .. } => error_detail.clone(),
        WarmupDispatchAttempt::PermanentFailure { .. } => http_status
            .map(|status| format!("warmup returned transient status {status}"))
            .unwrap_or_else(|| "warmup returned transient failure".to_owned()),
    }
}

pub(super) fn pre_request_error_reason(error: &SchedulerError) -> Option<WarmupAttemptReason> {
    let detail = error.to_string();
    if detail.contains("oauth decrypt failed") {
        return Some(WarmupAttemptReason::CredentialDecryptFailed);
    }
    if detail.contains("missing oauth credentials") {
        return Some(WarmupAttemptReason::OauthCredentialsMissing);
    }
    None
}

pub(super) fn cycle_key_i64(cycle_key: u64) -> SchedulerResult<i64> {
    i64::try_from(cycle_key)
        .map_err(|_| SchedulerError::Job("warmup cycle key exceeds i64".to_owned()))
}

pub(super) fn now_unix_secs_i64() -> SchedulerResult<i64> {
    i64::try_from(now_unix_secs())
        .map_err(|_| SchedulerError::Job("current unix timestamp exceeds i64".to_owned()))
}
