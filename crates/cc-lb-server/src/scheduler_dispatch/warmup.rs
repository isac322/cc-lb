use cc_lb_clock::unix_secs;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStatusUpdate, UpstreamStore};
use cc_lb_storage_api::{
    WarmupAttemptOutcome, WarmupAttemptTrigger, WarmupLane, WarmupPermanentFailureReason,
    WarmupSkipReason,
};

use crate::scheduler_dispatch::http::decrypt_bundle;
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::warmup::execute::{WarmupAttemptExecution, execute_warmup_attempt};

mod quota;
mod record;
mod request;
mod scheduling;
mod support;

/// Re-check delay for a lane whose quota group is reported absent.
const QUOTA_GROUP_ABSENT_RECHECK_SECS: i64 = 5 * 60 * 60;

use quota::{
    WarmupPreflightReject, WarmupQuotaPreflightDecision, next_warmup_evaluation_after_success,
    quota_preflight_decision_from_snapshots, warmup_preflight_reject,
};
use request::{WarmupDispatchAttempt, WarmupDispatchResult};
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
        if let Some(reject) = warmup_preflight_reject(&upstream) {
            match reject {
                WarmupPreflightReject::Skip(reason) => {
                    self.record_warmup_skip(&upstream, job, expected_cycle_key, reason, None, None)
                        .await;
                }
                WarmupPreflightReject::PermanentFailure(reason) => {
                    self.record_warmup_failure(
                        &upstream,
                        job,
                        expected_cycle_key,
                        now_unix_secs_i64(&*self.clock).unwrap_or(expected_cycle_key),
                        reason,
                        format!("warmup preflight rejected: {reason:?}"),
                        None,
                        cc_lb_storage_api::WarmupDispatchKind::NotDispatched,
                    )
                    .await;
                }
            }
            return Ok(JobOutcome::Skip);
        }
        // Every warm-up credential serves the primary group. Decrypt failures
        // fall through to the request path, which records the credential failure.
        if job.lane != WarmupLane::Primary
            && let Ok(bundle) = decrypt_bundle(&upstream, self.aead.as_ref())
            && !job.lane.credential_supports(upstream.kind, &bundle)
        {
            self.record_warmup_skip(
                &upstream,
                job,
                expected_cycle_key,
                WarmupSkipReason::QuotaGroupUnavailable,
                None,
                Some("credential profile does not serve this quota group"),
            )
            .await;
            return Ok(JobOutcome::Skip);
        }
        match self.quota_preflight_decision(&upstream, job.lane) {
            WarmupQuotaPreflightDecision::ActiveWindow { cycle_key } => {
                self.record_warmup_window_already_active(
                    &upstream,
                    job,
                    expected_cycle_key,
                    cycle_key,
                )
                .await;
                self.write_next_warmup_after_quota_skip(upstream.id, job.lane, cycle_key)
                    .await?;
                return Ok(JobOutcome::Done);
            }
            WarmupQuotaPreflightDecision::SevenDayQuotaExhausted { cycle_key } => {
                self.record_warmup_skip(
                    &upstream,
                    job,
                    expected_cycle_key,
                    WarmupSkipReason::SevenDayQuotaExhausted,
                    Some(cycle_key),
                    None,
                )
                .await;
                self.write_next_warmup_after_quota_skip(upstream.id, job.lane, cycle_key)
                    .await?;
                return Ok(JobOutcome::Skip);
            }
            WarmupQuotaPreflightDecision::TargetsAbsent => {
                self.record_warmup_skip(
                    &upstream,
                    job,
                    expected_cycle_key,
                    WarmupSkipReason::QuotaGroupUnavailable,
                    None,
                    Some("every target quota window is reported absent"),
                )
                .await;
                let recheck_at_unix_secs = now_unix_secs_i64(&*self.clock)?
                    .saturating_add(QUOTA_GROUP_ABSENT_RECHECK_SECS);
                self.write_next_warmup_after_quota_skip(
                    upstream.id,
                    job.lane,
                    recheck_at_unix_secs,
                )
                .await?;
                return Ok(JobOutcome::Skip);
            }
            WarmupQuotaPreflightDecision::SendWarmup | WarmupQuotaPreflightDecision::Unknown => {}
        }
        self.fire_warmup(&mut upstream, job, expected_cycle_key)
            .await
    }

    fn quota_preflight_decision(
        &self,
        upstream: &UpstreamRecord,
        lane: WarmupLane,
    ) -> WarmupQuotaPreflightDecision {
        let view = self.dynamic_view.load();
        let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
        drop(view);
        let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis(&*self.clock),
            max_staleness_secs,
        );
        let Ok(now_unix_secs) = now_unix_secs_i64(&*self.clock) else {
            return WarmupQuotaPreflightDecision::Unknown;
        };
        quota_preflight_decision_from_snapshots(upstream.kind, lane, &snapshots, now_unix_secs)
    }

    async fn fire_warmup(
        &self,
        upstream: &mut UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
    ) -> SchedulerResult<JobOutcome> {
        let attempted_at_unix_secs = now_unix_secs_i64(&*self.clock)?;
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
                    cc_lb_storage_api::WarmupDispatchKind::NotDispatched,
                )
                .await;
                return Ok(JobOutcome::Done);
            }
            return Err(error);
        }

        let initial_attempt = self.dispatch_warmup_request(upstream, job.lane).await;
        let final_attempt = if response_is_auth_failed(&initial_attempt) {
            match self.force_refresh_oauth_token(upstream).await {
                Ok(true) => self.dispatch_warmup_request(upstream, job.lane).await,
                Ok(false) => initial_attempt,
                Err(error) => {
                    tracing::warn!(upstream_id = %upstream.id, %error, "warmup oauth force-refresh failed");
                    WarmupDispatchAttempt {
                        dispatch_kind: initial_attempt.dispatch_kind,
                        result: WarmupDispatchResult::PermanentFailure {
                            reason: WarmupPermanentFailureReason::OauthRefreshFailed,
                            error_detail: error.to_string(),
                        },
                    }
                }
            }
        } else {
            initial_attempt
        };

        let completed_at_unix_secs = Some(now_unix_secs_i64(&*self.clock)?);
        let record = execute_warmup_attempt(WarmupAttemptExecution {
            storage: self.storage.as_ref(),
            upstream,
            lane: job.lane,
            scheduled_for_unix_secs: expected_cycle_key,
            trigger: WarmupAttemptTrigger::Scheduled,
            replica_id: self.replica_id,
            lease_holder: lease_holder.as_deref(),
            expected_cycle_key: Some(expected_cycle_key),
            attempted_at_unix_secs,
            completed_at_unix_secs,
            dispatch_kind: final_attempt.dispatch_kind,
            result: execution_result_from_dispatch_attempt(&final_attempt),
        })
        .await;

        match record.outcome {
            WarmupAttemptOutcome::Success(_) => {
                let now_unix_secs = now_unix_secs_i64(&*self.clock)?;
                let view = self.dynamic_view.load();
                let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
                drop(view);
                let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
                    upstream.id,
                    now_unix_millis(&*self.clock),
                    max_staleness_secs,
                );
                let observations = match &final_attempt.result {
                    WarmupDispatchResult::Response { observations, .. } => observations.as_slice(),
                    _ => &[],
                };
                let next_evaluation = next_warmup_evaluation_after_success(
                    upstream.kind,
                    job.lane,
                    &snapshots,
                    observations,
                    now_unix_secs,
                );
                self.write_status_after_warmup_success(upstream.id, job.lane, next_evaluation)
                    .await?;
                if let WarmupDispatchResult::Response { observations, .. } = final_attempt.result {
                    self.record_warmup_observations(upstream.id, observations)?;
                }
                Ok(JobOutcome::Done)
            }
            WarmupAttemptOutcome::TransientFailure(_) => Err(SchedulerError::Job(
                transient_attempt_error(&final_attempt, record.http_status),
            )),
            WarmupAttemptOutcome::PermanentFailure(reason) => {
                tracing::warn!(upstream_id = %upstream.id, ?reason, "warmup cycle abandoned");
                Ok(JobOutcome::Done)
            }
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted) => {
                if let Some(seven_day_resets_at) = record.cycle_key {
                    self.write_next_warmup_after_quota_skip(
                        upstream.id,
                        job.lane,
                        seven_day_resets_at,
                    )
                    .await?;
                    if let WarmupDispatchResult::Response { observations, .. } =
                        final_attempt.result
                    {
                        self.record_warmup_observations(upstream.id, observations)?;
                    }
                }
                Ok(JobOutcome::Skip)
            }
            WarmupAttemptOutcome::Skipped(_) => Ok(JobOutcome::Skip),
        }
    }

    async fn write_status_after_warmup_success(
        &self,
        upstream_id: uuid::Uuid,
        lane: WarmupLane,
        next_evaluation_at_unix_secs: i64,
    ) -> SchedulerResult<()> {
        let next_evaluation_at_unix_secs =
            u64::try_from(next_evaluation_at_unix_secs).map_err(|_| {
                SchedulerError::Job("warmup next evaluation before unix epoch".to_owned())
            })?;
        let status = UpstreamStatusUpdate {
            last_warmup_at_unix_secs: Some(Some(unix_secs(self.clock.now()))),
            ..UpstreamStatusUpdate::default()
        };
        if let Err(error) =
            UpstreamStore::set_status(self.storage.as_ref(), upstream_id, status).await
        {
            tracing::warn!(upstream_id = %upstream_id, %error, "warmup status writeback failed; admin UI may show stale data");
        }

        let task = next_warmup_task(upstream_id, next_evaluation_at_unix_secs, lane);
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn write_next_warmup_after_quota_skip(
        &self,
        upstream_id: uuid::Uuid,
        lane: WarmupLane,
        resets_at_unix_secs: i64,
    ) -> SchedulerResult<()> {
        let resets_at_unix_secs = u64::try_from(resets_at_unix_secs)
            .map_err(|_| SchedulerError::Job("warmup reset time before unix epoch".to_owned()))?;
        let task = next_warmup_task(upstream_id, resets_at_unix_secs, lane);
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
