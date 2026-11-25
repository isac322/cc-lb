use crate::scheduler_dispatch::http::decrypt_bundle;
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::warmup::execute::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};
use cc_lb_clock::{Clock, unix_millis, unix_secs};
use cc_lb_control::WarmupWindowVerifier;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::warmup::{ManualWarmupJob, UpstreamWarmupJob};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStatusUpdate, UpstreamStore};
use cc_lb_storage_api::{
    WarmupAttemptOutcome, WarmupAttemptTrigger, WarmupLane, WarmupPermanentFailureReason,
    WarmupSkipReason,
};

mod quota;
mod record;
pub(super) mod request;
mod scheduling;
mod support;
mod verify;

/// Re-check delay for a lane whose quota group is reported absent.
const QUOTA_GROUP_ABSENT_RECHECK_SECS: i64 = 5 * 60 * 60;

use quota::{
    WarmupPreflightReject, WarmupQuotaPreflightDecision, next_warmup_evaluation_after_success,
    quota_preflight_decision_from_snapshots, target_window_resets, warmup_preflight_reject,
};
use request::{WarmupDispatchAttempt, WarmupDispatchResult};
use scheduling::next_warmup_task;
use support::{
    cycle_key_i64, execution_result_from_dispatch_attempt, now_unix_secs_i64,
    pre_request_error_reason, response_is_auth_failed, transient_attempt_error,
};

use super::SchedulerDispatch;

fn clock_secs_and_millis(clock: &dyn Clock) -> Option<(i64, u64)> {
    let now = clock.now();
    let secs = i64::try_from(unix_secs(now)).ok()?;
    let millis = unix_millis(now).min(u128::from(u64::MAX)) as u64;
    Some((secs, millis))
}

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
        match self.quota_preflight_decision(&upstream, job.lane).await {
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

    async fn quota_preflight_decision(
        &self,
        upstream: &UpstreamRecord,
        lane: WarmupLane,
    ) -> WarmupQuotaPreflightDecision {
        let view = self.dynamic_view.load();
        let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
        drop(view);
        let Some((now_unix_secs, now_unix_millis)) = clock_secs_and_millis(&*self.clock) else {
            return WarmupQuotaPreflightDecision::Unknown;
        };
        let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        );
        let decision =
            quota_preflight_decision_from_snapshots(upstream.kind, lane, &snapshots, now_unix_secs);
        if decision != WarmupQuotaPreflightDecision::Unknown {
            return decision;
        }
        if let Err(error) = self
            .subscription_quota_cache
            .hydrate_from_store_unless_raced(&self.stores, &[upstream.id])
            .await
        {
            tracing::warn!(
                upstream_id = %upstream.id,
                %error,
                "warmup quota preflight DB hydration failed; falling back to unknown",
            );
            return WarmupQuotaPreflightDecision::Unknown;
        }
        let Some((now_unix_secs, now_unix_millis)) = clock_secs_and_millis(&*self.clock) else {
            return WarmupQuotaPreflightDecision::Unknown;
        };
        let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        );
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

        // The resets each target window reported before the request, so the
        // post-request check can tell a new window from one already running.
        let previous_resets = {
            let view = self.dynamic_view.load();
            let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
            drop(view);
            let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
                upstream.id,
                now_unix_millis(&*self.clock),
                max_staleness_secs,
            );
            target_window_resets(upstream.kind, job.lane, &snapshots)
        };
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

        let request_completed_at_unix_secs = now_unix_secs_i64(&*self.clock)?;
        // The job has a 60 s timeout; the verification wait plus poll must fit.
        let can_verify_window =
            request_completed_at_unix_secs.saturating_sub(attempted_at_unix_secs) <= 20;
        let window_check = if can_verify_window
            && upstream.kind == cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth
            && matches!(
                &final_attempt.result,
                WarmupDispatchResult::Response { status, .. } if status.is_success()
            ) {
            Some(
                self.verify_warmup_window(
                    upstream,
                    job.lane,
                    &previous_resets,
                    request_completed_at_unix_secs,
                )
                .await,
            )
        } else {
            None
        };
        let verified_observations = match window_check.as_ref() {
            Some(cc_lb_control::WarmupWindowCheck::Started { observations }) => {
                Some(observations.as_slice())
            }
            _ => None,
        };
        let not_started_error_detail = match window_check.as_ref() {
            Some(cc_lb_control::WarmupWindowCheck::NotStarted { windows }) => {
                let windows = windows
                    .iter()
                    .map(|window| window.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let status = match &final_attempt.result {
                    WarmupDispatchResult::Response { status, .. } => *status,
                    _ => unreachable!("not-started verification requires a response"),
                };
                Some(format!(
                    "warm-up returned {status} but {windows} did not start"
                ))
            }
            _ => None,
        };
        let execution_result = if let Some(error_detail) = not_started_error_detail.as_deref() {
            let WarmupDispatchResult::Response { status, .. } = &final_attempt.result else {
                unreachable!("not-started verification requires a response");
            };
            WarmupAttemptExecutionResult::PermanentFailure {
                reason: WarmupPermanentFailureReason::WindowNotStarted,
                http_status: Some(*status),
                error_detail: Some(error_detail),
            }
        } else {
            execution_result_from_dispatch_attempt(&final_attempt, verified_observations)
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
            result: execution_result,
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
                let observations = match window_check.as_ref() {
                    Some(cc_lb_control::WarmupWindowCheck::Started { observations }) => {
                        observations.as_slice()
                    }
                    _ => match &final_attempt.result {
                        WarmupDispatchResult::Response { observations, .. } => {
                            observations.as_slice()
                        }
                        _ => &[],
                    },
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
                if !matches!(
                    window_check.as_ref(),
                    Some(cc_lb_control::WarmupWindowCheck::Started { .. })
                ) && let WarmupDispatchResult::Response { observations, .. } =
                    final_attempt.result
                {
                    self.record_warmup_observations(upstream.id, observations)?;
                }
                Ok(JobOutcome::Done)
            }
            WarmupAttemptOutcome::TransientFailure(_) => {
                let detail = transient_attempt_error(&final_attempt, record.http_status);
                tracing::warn!(
                    upstream_id = %upstream.id,
                    lane = ?job.lane,
                    http_status = record.http_status,
                    detail = %detail,
                    "warmup transient failure; rescheduling via RetryClass",
                );
                Ok(JobOutcome::Retry { delay: None })
            }
            WarmupAttemptOutcome::PermanentFailure(reason) => {
                if matches!(
                    window_check.as_ref(),
                    Some(cc_lb_control::WarmupWindowCheck::NotStarted { .. })
                ) {
                    let now_unix_secs = now_unix_secs_i64(&*self.clock)?;
                    let view = self.dynamic_view.load();
                    let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
                    drop(view);
                    let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
                        upstream.id,
                        now_unix_millis(&*self.clock),
                        max_staleness_secs,
                    );
                    let next_evaluation = next_warmup_evaluation_after_success(
                        upstream.kind,
                        job.lane,
                        &snapshots,
                        &[],
                        now_unix_secs,
                    );
                    self.write_next_warmup_after_quota_skip(upstream.id, job.lane, next_evaluation)
                        .await?;
                    return Ok(JobOutcome::Done);
                }
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

    pub(super) async fn dispatch_manual_warmup(
        &self,
        job: ManualWarmupJob,
    ) -> SchedulerResult<JobOutcome> {
        let dispatch_now_unix_secs = unix_secs(self.clock.now());
        if job
            .expires_at_unix_secs
            .is_some_and(|expires_at| dispatch_now_unix_secs >= expires_at)
        {
            tracing::info!(
                target: "warmup",
                upstream_id = %job.upstream_id,
                lane = job.lane.as_str(),
                holder = %job.holder,
                action = "split_manual_warmup_expired_before_start",
            );
            cc_lb_control::record_manual_warmup_expired_before_start(
                self.storage.as_ref(),
                job.upstream_id,
                job.lane,
                dispatch_now_unix_secs,
                job.nonce,
                "manual warmup expired in scheduler queue before a worker started it".to_owned(),
            )
            .await
            .map_err(storage_scheduler_error)?;
            return Ok(JobOutcome::Skip);
        }
        let Some(mut upstream) = UpstreamStore::get_by_id(self.storage.as_ref(), job.upstream_id)
            .await
            .map_err(storage_scheduler_error)?
        else {
            return Ok(JobOutcome::Skip);
        };
        let attempted_at_unix_secs = now_unix_secs_i64(&*self.clock)?;
        if !upstream.warmup_enabled {
            let record = execute_warmup_attempt(WarmupAttemptExecution {
                storage: self.storage.as_ref(),
                upstream: &upstream,
                lane: job.lane,
                scheduled_for_unix_secs: attempted_at_unix_secs,
                trigger: WarmupAttemptTrigger::Manual,
                replica_id: self.replica_id,
                lease_holder: Some(job.holder.as_str()),
                expected_cycle_key: Some(job.candidate_cycle_key),
                attempted_at_unix_secs,
                completed_at_unix_secs: Some(attempted_at_unix_secs),
                dispatch_kind: cc_lb_storage_api::WarmupDispatchKind::NotDispatched,
                result: WarmupAttemptExecutionResult::Skipped {
                    reason: WarmupSkipReason::UpstreamDisabled,
                    cycle_key: None,
                    error_detail: None,
                },
            })
            .await;
            let now_u64 = unix_secs(self.clock.now());
            cc_lb_control::record_manual_warmup_result(
                self.storage.as_ref(),
                upstream.id,
                job.lane,
                now_u64,
                cc_lb_control::ManualWarmupJobResult {
                    nonce: job.nonce,
                    outcome: record.outcome,
                    cycle_key: record.cycle_key,
                    error_detail: record.error_detail,
                    expires_at_unix_secs: now_u64
                        .saturating_add(cc_lb_control::SPLIT_BRIDGE_RESULT_TTL_SECS),
                },
            )
            .await
            .map_err(storage_scheduler_error)?;
            return Ok(JobOutcome::Done);
        }

        if let Err(error) = self.ensure_fresh_oauth_token(&mut upstream).await {
            if let Some(reason) = pre_request_error_reason(&error) {
                let error_detail = error.to_string();
                let record = execute_warmup_attempt(WarmupAttemptExecution {
                    storage: self.storage.as_ref(),
                    upstream: &upstream,
                    lane: job.lane,
                    scheduled_for_unix_secs: attempted_at_unix_secs,
                    trigger: WarmupAttemptTrigger::Manual,
                    replica_id: self.replica_id,
                    lease_holder: Some(job.holder.as_str()),
                    expected_cycle_key: Some(job.candidate_cycle_key),
                    attempted_at_unix_secs,
                    completed_at_unix_secs: Some(now_unix_secs_i64(&*self.clock)?),
                    dispatch_kind: cc_lb_storage_api::WarmupDispatchKind::NotDispatched,
                    result: WarmupAttemptExecutionResult::PermanentFailure {
                        reason,
                        http_status: None,
                        error_detail: Some(error_detail.as_str()),
                    },
                })
                .await;
                let now_u64 = unix_secs(self.clock.now());
                cc_lb_control::record_manual_warmup_result(
                    self.storage.as_ref(),
                    upstream.id,
                    job.lane,
                    now_u64,
                    cc_lb_control::ManualWarmupJobResult {
                        nonce: job.nonce,
                        outcome: record.outcome,
                        cycle_key: record.cycle_key,
                        error_detail: record.error_detail,
                        expires_at_unix_secs: now_u64
                            .saturating_add(cc_lb_control::SPLIT_BRIDGE_RESULT_TTL_SECS),
                    },
                )
                .await
                .map_err(storage_scheduler_error)?;
                return Ok(JobOutcome::Done);
            }
            return Err(error);
        }

        let previous_resets = {
            let view = self.dynamic_view.load();
            let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
            drop(view);
            let snapshots = self.subscription_quota_cache.snapshot_for_upstream(
                upstream.id,
                now_unix_millis(&*self.clock),
                max_staleness_secs,
            );
            target_window_resets(upstream.kind, job.lane, &snapshots)
        };
        let initial_attempt = self.dispatch_warmup_request(&upstream, job.lane).await;
        let final_attempt = if response_is_auth_failed(&initial_attempt) {
            match self.force_refresh_oauth_token(&mut upstream).await {
                Ok(true) => self.dispatch_warmup_request(&upstream, job.lane).await,
                Ok(false) => initial_attempt,
                Err(error) => {
                    tracing::warn!(upstream_id = %upstream.id, %error, "manual warmup oauth force-refresh failed");
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

        let request_completed_at_unix_secs = now_unix_secs_i64(&*self.clock)?;
        let can_verify_window =
            request_completed_at_unix_secs.saturating_sub(attempted_at_unix_secs) <= 20;
        let window_check = if can_verify_window
            && upstream.kind == cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth
            && matches!(
                &final_attempt.result,
                WarmupDispatchResult::Response { status, .. } if status.is_success()
            ) {
            Some(
                self.verify_warmup_window(
                    &upstream,
                    job.lane,
                    &previous_resets,
                    request_completed_at_unix_secs,
                )
                .await,
            )
        } else {
            None
        };
        let verified_observations = match window_check.as_ref() {
            Some(cc_lb_control::WarmupWindowCheck::Started { observations }) => {
                Some(observations.as_slice())
            }
            _ => None,
        };
        let not_started_error_detail = match window_check.as_ref() {
            Some(cc_lb_control::WarmupWindowCheck::NotStarted { windows }) => {
                let windows = windows
                    .iter()
                    .map(|window| window.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let status = match &final_attempt.result {
                    WarmupDispatchResult::Response { status, .. } => *status,
                    _ => unreachable!("not-started verification requires a response"),
                };
                Some(format!(
                    "warm-up returned {status} but {windows} did not start"
                ))
            }
            _ => None,
        };
        let execution_result = if let Some(error_detail) = not_started_error_detail.as_deref() {
            let WarmupDispatchResult::Response { status, .. } = &final_attempt.result else {
                unreachable!("not-started verification requires a response");
            };
            WarmupAttemptExecutionResult::PermanentFailure {
                reason: WarmupPermanentFailureReason::WindowNotStarted,
                http_status: Some(*status),
                error_detail: Some(error_detail),
            }
        } else {
            execution_result_from_dispatch_attempt(&final_attempt, verified_observations)
        };
        let completed_at_unix_secs = Some(now_unix_secs_i64(&*self.clock)?);
        let record = execute_warmup_attempt(WarmupAttemptExecution {
            storage: self.storage.as_ref(),
            upstream: &upstream,
            lane: job.lane,
            scheduled_for_unix_secs: attempted_at_unix_secs,
            trigger: WarmupAttemptTrigger::Manual,
            replica_id: self.replica_id,
            lease_holder: Some(job.holder.as_str()),
            expected_cycle_key: Some(job.candidate_cycle_key),
            attempted_at_unix_secs,
            completed_at_unix_secs,
            dispatch_kind: final_attempt.dispatch_kind,
            result: execution_result,
        })
        .await;

        let result_now_unix_secs = unix_secs(self.clock.now());
        if matches!(record.outcome, WarmupAttemptOutcome::Success(_)) {
            let status = UpstreamStatusUpdate {
                last_warmup_at_unix_secs: Some(Some(result_now_unix_secs)),
                ..UpstreamStatusUpdate::default()
            };
            if let Err(error) =
                UpstreamStore::set_status(self.storage.as_ref(), upstream.id, status).await
            {
                tracing::warn!(upstream_id = %upstream.id, %error, "manual warmup status writeback failed");
            }
        }
        if !matches!(
            window_check.as_ref(),
            Some(cc_lb_control::WarmupWindowCheck::Started { .. })
        ) && let WarmupDispatchResult::Response { observations, .. } = final_attempt.result
            && let Err(error) = self.record_warmup_observations(upstream.id, observations)
        {
            // Persist `ManualWarmupJobResult` even if the async DB sink is full/closed so
            // split-admin callers receive the classified outcome (in-memory cache is updated).
            tracing::warn!(
                upstream_id = %upstream.id,
                %error,
                "manual warmup quota observation publication failed; preserving classified warmup result"
            );
        }

        cc_lb_control::record_manual_warmup_result(
            self.storage.as_ref(),
            upstream.id,
            job.lane,
            result_now_unix_secs,
            cc_lb_control::ManualWarmupJobResult {
                nonce: job.nonce,
                outcome: record.outcome,
                cycle_key: record.cycle_key,
                error_detail: record.error_detail,
                expires_at_unix_secs: result_now_unix_secs
                    .saturating_add(cc_lb_control::SPLIT_BRIDGE_RESULT_TTL_SECS),
            },
        )
        .await
        .map_err(storage_scheduler_error)?;
        Ok(JobOutcome::Done)
    }
}

#[cfg(test)]
mod tests;
