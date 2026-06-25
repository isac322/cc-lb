use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStatusUpdate, UpstreamStore};
use cc_lb_storage_api::{WarmupAttemptOutcome, WarmupAttemptReason, WarmupAttemptTrigger};

use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::{now_unix_millis, now_unix_secs};
use crate::warmup::execute::{WarmupAttemptExecution, execute_warmup_attempt};

mod quota;
mod record;
mod request;
mod scheduling;
mod support;

use quota::{
    WarmupPreflightSkip, quota_preflight_skip_from_snapshots, warmup_preflight_skip_reason,
};
use request::WarmupDispatchAttempt;
use scheduling::next_warmup_task;
use support::{
    cycle_key_i64, execution_result_from_dispatch_attempt, now_unix_secs_i64,
    pre_request_error_reason, response_is_auth_failed, transient_attempt_error,
};

use super::SchedulerDispatch;

impl SchedulerDispatch {
    pub(super) async fn dispatch_warmup(
        &self,
        job: UpstreamWarmupJob,
    ) -> SchedulerResult<JobOutcome> {
        let mut upstream = match UpstreamStore::get_by_id(self.storage.as_ref(), job.upstream_id)
            .await
            .map_err(storage_scheduler_error)?
        {
            Some(upstream) => upstream,
            None => return Ok(JobOutcome::Skip),
        };
        let expected_cycle_key = cycle_key_i64(job.cycle_key)?;
        if let Some(reason) = warmup_preflight_skip_reason(&upstream) {
            self.record_warmup_skip(&upstream, job, expected_cycle_key, reason, None, None)
                .await;
            return Ok(JobOutcome::Skip);
        }
        if let Some(skip) = self.quota_preflight_skip(upstream.id) {
            self.record_warmup_skip(
                &upstream,
                job,
                expected_cycle_key,
                skip.reason,
                skip.cycle_key,
                None,
            )
            .await;
            if let Some(cycle_key) = skip.cycle_key {
                self.write_next_warmup_after_quota_skip(upstream.id, cycle_key)
                    .await?;
            }
            return Ok(JobOutcome::Skip);
        }
        self.fire_warmup(&mut upstream, job, expected_cycle_key)
            .await
    }

    fn quota_preflight_skip(&self, upstream_id: uuid::Uuid) -> Option<WarmupPreflightSkip> {
        let view = self.dynamic_view.load();
        let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
        drop(view);
        let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
            upstream_id,
            now_unix_millis(),
            max_staleness_secs,
        );
        quota_preflight_skip_from_snapshots(&snapshots, now_unix_secs_i64().ok()?)
    }

    async fn fire_warmup(
        &self,
        upstream: &mut UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
    ) -> SchedulerResult<JobOutcome> {
        let attempted_at_unix_secs = now_unix_secs_i64()?;
        let lease_holder = self.replica_id.map(|id| format!("scheduler:{id}"));
        if let Err(error) = self.ensure_fresh_oauth_token(upstream).await {
            if let Some(reason) = pre_request_error_reason(&error) {
                self.record_warmup_failure(
                    upstream,
                    job,
                    expected_cycle_key,
                    attempted_at_unix_secs,
                    reason,
                    error.to_string(),
                    lease_holder.as_deref(),
                )
                .await;
                return Ok(JobOutcome::Done);
            }
            return Err(error);
        }

        let initial_attempt = self.dispatch_warmup_request(upstream).await;
        let final_attempt = if response_is_auth_failed(&initial_attempt, expected_cycle_key) {
            match self.force_refresh_oauth_token(upstream).await {
                Ok(true) => self.dispatch_warmup_request(upstream).await,
                Ok(false) => initial_attempt,
                Err(error) => {
                    tracing::warn!(upstream_id = %upstream.id, %error, "warmup oauth force-refresh failed");
                    WarmupDispatchAttempt::PermanentFailure {
                        reason: WarmupAttemptReason::OauthRefreshFailed,
                        error_detail: error.to_string(),
                    }
                }
            }
        } else {
            initial_attempt
        };

        let completed_at_unix_secs = Some(now_unix_secs_i64()?);
        let record = execute_warmup_attempt(WarmupAttemptExecution {
            storage: self.storage.as_ref(),
            upstream,
            scheduled_for_unix_secs: expected_cycle_key,
            trigger: WarmupAttemptTrigger::Scheduled,
            replica_id: self.replica_id,
            lease_holder: lease_holder.as_deref(),
            expected_cycle_key: Some(expected_cycle_key),
            attempted_at_unix_secs,
            completed_at_unix_secs,
            result: execution_result_from_dispatch_attempt(&final_attempt),
        })
        .await;

        match record.outcome {
            WarmupAttemptOutcome::SuccessFresh | WarmupAttemptOutcome::SuccessRedundant => {
                let response_cycle_key = record.cycle_key.unwrap_or(expected_cycle_key);
                self.write_status_after_warmup_success(upstream.id, response_cycle_key)
                    .await?;
                match final_attempt {
                    WarmupDispatchAttempt::Response { observations, .. } => {
                        self.record_warmup_observations(upstream.id, observations)?;
                    }
                    WarmupDispatchAttempt::TransientFailure { .. }
                    | WarmupDispatchAttempt::PermanentFailure { .. } => {}
                }
                Ok(JobOutcome::Done)
            }
            WarmupAttemptOutcome::TransientFailure => Err(SchedulerError::Job(
                transient_attempt_error(&final_attempt, record.http_status),
            )),
            WarmupAttemptOutcome::PermanentFailure => {
                if let Some(reason) = record.reason {
                    tracing::warn!(upstream_id = %upstream.id, reason = ?reason, "warmup cycle abandoned");
                }
                Ok(JobOutcome::Done)
            }
            WarmupAttemptOutcome::Skipped
                if record.reason == Some(WarmupAttemptReason::SevenDayQuotaExhausted) =>
            {
                if let Some(seven_day_resets_at) = record.cycle_key {
                    self.write_next_warmup_after_quota_skip(upstream.id, seven_day_resets_at)
                        .await?;
                    if let WarmupDispatchAttempt::Response { observations, .. } = final_attempt {
                        self.record_warmup_observations(upstream.id, observations)?;
                    }
                }
                Ok(JobOutcome::Skip)
            }
            WarmupAttemptOutcome::Skipped => Ok(JobOutcome::Skip),
        }
    }

    async fn write_status_after_warmup_success(
        &self,
        upstream_id: uuid::Uuid,
        response_resets_at_unix_secs: i64,
    ) -> SchedulerResult<()> {
        let response_resets_at_unix_secs =
            u64::try_from(response_resets_at_unix_secs).map_err(|_| {
                SchedulerError::Job("warmup response reset time before unix epoch".to_owned())
            })?;
        let status = UpstreamStatusUpdate {
            last_warmup_at_unix_secs: Some(Some(now_unix_secs())),
            ..UpstreamStatusUpdate::default()
        };
        if let Err(error) =
            UpstreamStore::set_status(self.storage.as_ref(), upstream_id, status).await
        {
            tracing::warn!(upstream_id = %upstream_id, %error, "warmup status writeback failed; admin UI may show stale data");
        }

        let task = next_warmup_task(upstream_id, response_resets_at_unix_secs);
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn write_next_warmup_after_quota_skip(
        &self,
        upstream_id: uuid::Uuid,
        resets_at_unix_secs: i64,
    ) -> SchedulerResult<()> {
        let resets_at_unix_secs = u64::try_from(resets_at_unix_secs)
            .map_err(|_| SchedulerError::Job("warmup reset time before unix epoch".to_owned()))?;
        let task = next_warmup_task(upstream_id, resets_at_unix_secs);
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
