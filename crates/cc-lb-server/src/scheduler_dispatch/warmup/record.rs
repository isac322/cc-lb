use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_storage_api::{
    UpstreamRecord, WarmupAttemptTrigger, WarmupDispatchKind, WarmupPermanentFailureReason,
    WarmupSkipReason,
};

use crate::warmup::execute::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};

use super::SchedulerDispatch;
use super::support::{cycle_key_i64, now_unix_secs_i64};

impl SchedulerDispatch {
    pub(super) async fn record_warmup_skip(
        &self,
        upstream: &UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
        reason: WarmupSkipReason,
        cycle_key: Option<i64>,
        error_detail: Option<&str>,
    ) {
        let attempted_at_unix_secs = now_unix_secs_i64(&*self.clock).unwrap_or(expected_cycle_key);
        execute_warmup_attempt(WarmupAttemptExecution {
            storage: self.storage.as_ref(),
            upstream,
            scheduled_for_unix_secs: cycle_key_i64(job.cycle_key).unwrap_or(expected_cycle_key),
            trigger: WarmupAttemptTrigger::Scheduled,
            replica_id: self.replica_id,
            lease_holder: None,
            expected_cycle_key: Some(expected_cycle_key),
            attempted_at_unix_secs,
            completed_at_unix_secs: Some(attempted_at_unix_secs),
            dispatch_kind: WarmupDispatchKind::NotDispatched,
            result: WarmupAttemptExecutionResult::Skipped {
                reason,
                cycle_key,
                error_detail,
            },
        })
        .await;
    }

    pub(super) async fn record_warmup_window_already_active(
        &self,
        upstream: &UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
        cycle_key: i64,
    ) {
        let attempted_at_unix_secs = now_unix_secs_i64(&*self.clock).unwrap_or(expected_cycle_key);
        execute_warmup_attempt(WarmupAttemptExecution {
            storage: self.storage.as_ref(),
            upstream,
            scheduled_for_unix_secs: cycle_key_i64(job.cycle_key).unwrap_or(expected_cycle_key),
            trigger: WarmupAttemptTrigger::Scheduled,
            replica_id: self.replica_id,
            lease_holder: None,
            expected_cycle_key: Some(expected_cycle_key),
            attempted_at_unix_secs,
            completed_at_unix_secs: Some(attempted_at_unix_secs),
            dispatch_kind: WarmupDispatchKind::NotDispatched,
            result: WarmupAttemptExecutionResult::PreflightActiveWindow { cycle_key },
        })
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn record_warmup_failure(
        &self,
        upstream: &UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
        attempted_at_unix_secs: i64,
        reason: WarmupPermanentFailureReason,
        error_detail: String,
        lease_holder: Option<&str>,
        dispatch_kind: WarmupDispatchKind,
    ) {
        execute_warmup_attempt(WarmupAttemptExecution {
            storage: self.storage.as_ref(),
            upstream,
            scheduled_for_unix_secs: cycle_key_i64(job.cycle_key).unwrap_or(expected_cycle_key),
            trigger: WarmupAttemptTrigger::Scheduled,
            replica_id: self.replica_id,
            lease_holder,
            expected_cycle_key: Some(expected_cycle_key),
            attempted_at_unix_secs,
            completed_at_unix_secs: Some(
                now_unix_secs_i64(&*self.clock).unwrap_or(attempted_at_unix_secs),
            ),
            dispatch_kind,
            result: WarmupAttemptExecutionResult::PermanentFailure {
                reason,
                http_status: None,
                error_detail: Some(error_detail.as_str()),
            },
        })
        .await;
    }
}
