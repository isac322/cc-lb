#[allow(deprecated)]
use cc_lb_core::subscription_quota_events::unified_observation_to_record;
use cc_lb_core::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};
use cc_lb_storage_api::{UpstreamStatusUpdate, WarmupAttemptReason, WarmupAttemptTrigger};
use http::HeaderMap;

use crate::scheduler_dispatch::http::{decrypt_bundle, upstream_base_url};
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::{now_unix_millis, now_unix_secs};
use crate::warmup::execute::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};
use crate::warmup::request::WarmupRequestAttempt;
use crate::warmup::{
    WarmupAbandonReason, WarmupResult, classify_response, dispatch_warmup_attempt, stable_jitter_ms,
};

use super::SchedulerDispatch;

const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;
const POST_RESET_GUARD_SECS: u64 = 30;

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
            self.record_warmup_skip(&upstream, job, expected_cycle_key, reason, None)
                .await;
            return Ok(JobOutcome::Skip);
        }
        self.fire_warmup(&mut upstream, job, expected_cycle_key)
            .await
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
            cc_lb_storage_api::WarmupAttemptOutcome::SuccessFresh
            | cc_lb_storage_api::WarmupAttemptOutcome::SuccessRedundant => {
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
            cc_lb_storage_api::WarmupAttemptOutcome::TransientFailure => Err(SchedulerError::Job(
                transient_attempt_error(&final_attempt, record.http_status),
            )),
            cc_lb_storage_api::WarmupAttemptOutcome::PermanentFailure => {
                if let Some(reason) = record.reason {
                    tracing::warn!(upstream_id = %upstream.id, reason = ?reason, "warmup cycle abandoned");
                }
                Ok(JobOutcome::Done)
            }
            cc_lb_storage_api::WarmupAttemptOutcome::Skipped => Ok(JobOutcome::Skip),
        }
    }

    async fn record_warmup_skip(
        &self,
        upstream: &UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
        reason: WarmupAttemptReason,
        error_detail: Option<&str>,
    ) {
        let attempted_at_unix_secs = now_unix_secs_i64().unwrap_or(expected_cycle_key);
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
            result: WarmupAttemptExecutionResult::Skipped {
                reason,
                error_detail,
            },
        })
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_warmup_failure(
        &self,
        upstream: &UpstreamRecord,
        job: UpstreamWarmupJob,
        expected_cycle_key: i64,
        attempted_at_unix_secs: i64,
        reason: WarmupAttemptReason,
        error_detail: String,
        lease_holder: Option<&str>,
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
            completed_at_unix_secs: Some(now_unix_secs_i64().unwrap_or(attempted_at_unix_secs)),
            result: WarmupAttemptExecutionResult::PermanentFailure {
                reason,
                http_status: None,
                error_detail: Some(error_detail.as_str()),
            },
        })
        .await;
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

        let jitter_secs = stable_jitter_ms(upstream_id, response_resets_at_unix_secs) / 1_000;
        let run_at_unix_secs = response_resets_at_unix_secs
            .saturating_add(POST_RESET_GUARD_SECS)
            .saturating_add(jitter_secs);
        let job = UpstreamWarmupJob::new(upstream_id, response_resets_at_unix_secs);
        let task = SchedulerPushTask {
            args: AdaptiveJob::Warmup(job),
            idempotency_key: Some(job.idempotency_key(response_resets_at_unix_secs)),
            run_at_unix_secs: Some(run_at_unix_secs),
        };
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn ensure_fresh_oauth_token(&self, upstream: &mut UpstreamRecord) -> SchedulerResult<()> {
        let Some(lazy_refresher) = self.lazy_refresher.as_ref() else {
            return Ok(());
        };
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        if bundle
            .expires_at_unix_secs
            .saturating_sub(TOKEN_REFRESH_LOOKAHEAD_SECS)
            > now_unix_secs()
        {
            return Ok(());
        }
        if let Err(error) = lazy_refresher.refresh_one(upstream.id).await {
            tracing::warn!(upstream_id = %upstream.id, %error, "warmup proactive oauth refresh failed");
            return Ok(());
        }
        if let Some(refreshed) = UpstreamStore::get_by_id(self.storage.as_ref(), upstream.id)
            .await
            .map_err(storage_scheduler_error)?
        {
            *upstream = refreshed;
        }
        Ok(())
    }

    async fn force_refresh_oauth_token(
        &self,
        upstream: &mut UpstreamRecord,
    ) -> SchedulerResult<bool> {
        let Some(lazy_refresher) = self.lazy_refresher.as_ref() else {
            return Ok(false);
        };
        lazy_refresher
            .refresh_one(upstream.id)
            .await
            .map_err(|error| SchedulerError::Job(error.to_string()))?;
        if let Some(refreshed) = UpstreamStore::get_by_id(self.storage.as_ref(), upstream.id)
            .await
            .map_err(storage_scheduler_error)?
        {
            *upstream = refreshed;
        }
        Ok(true)
    }

    async fn dispatch_warmup_request(&self, upstream: &UpstreamRecord) -> WarmupDispatchAttempt {
        if upstream.warmup_dialect_plugin.is_some()
            && let Some(lazy_refresher) = self.lazy_refresher.as_ref()
        {
            let outcome = match crate::warmup::dialect::dispatch_warmup_with_dialect(
                self.runtime.as_ref(),
                self.stores.as_ref(),
                self.data_dir.as_ref(),
                self.aead.clone(),
                lazy_refresher.clone(),
                upstream,
                &self.http,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) if error.is_transient() => {
                    return WarmupDispatchAttempt::TransientFailure {
                        reason: WarmupAttemptReason::DialectPluginTransient,
                        error_detail: error.to_string(),
                    };
                }
                Err(error) => {
                    tracing::warn!(error = %error, reason = WarmupAbandonReason::DialectPlugin.as_str(), "warmup dialect failed permanently");
                    return WarmupDispatchAttempt::PermanentFailure {
                        reason: WarmupAttemptReason::DialectPluginFailed,
                        error_detail: error.to_string(),
                    };
                }
            };
            return WarmupDispatchAttempt::Response {
                status: outcome.status,
                observations: parse_headers(&outcome.headers),
            };
        }
        let bundle = match decrypt_bundle(upstream, self.aead.as_ref()) {
            Ok(bundle) => bundle,
            Err(error) => {
                return WarmupDispatchAttempt::PermanentFailure {
                    reason: WarmupAttemptReason::CredentialDecryptFailed,
                    error_detail: error.to_string(),
                };
            }
        };
        let base_url = match upstream_base_url(upstream) {
            Ok(base_url) => base_url,
            Err(error) => {
                return WarmupDispatchAttempt::PermanentFailure {
                    reason: WarmupAttemptReason::RequestBuildFailed,
                    error_detail: error.to_string(),
                };
            }
        };
        let replica_id = self
            .replica_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_owned());
        match dispatch_warmup_attempt(&self.http, &bundle.access_token, &base_url, &replica_id)
            .await
        {
            WarmupRequestAttempt::Response {
                status,
                observations,
            } => WarmupDispatchAttempt::Response {
                status,
                observations,
            },
            WarmupRequestAttempt::RequestBuildFailed { error } => {
                WarmupDispatchAttempt::PermanentFailure {
                    reason: WarmupAttemptReason::RequestBuildFailed,
                    error_detail: error,
                }
            }
            WarmupRequestAttempt::NetworkError { error } => {
                WarmupDispatchAttempt::TransientFailure {
                    reason: WarmupAttemptReason::NetworkError,
                    error_detail: error,
                }
            }
        }
    }

    fn record_warmup_observations(
        &self,
        upstream_id: uuid::Uuid,
        observations: Vec<UnifiedQuotaObservation>,
    ) -> SchedulerResult<()> {
        let observed_at_unix_millis = now_unix_millis();
        for observation in observations {
            let record =
                unified_observation_to_record(upstream_id, observation, observed_at_unix_millis);
            self.subscription_quota_cache
                .upsert_observation(upstream_id, &record);
            self.subscription_quota_sink
                .enqueue(record)
                .map_err(|error| SchedulerError::Job(error.to_string()))?;
        }
        Ok(())
    }
}

enum WarmupDispatchAttempt {
    Response {
        status: http::StatusCode,
        observations: Vec<UnifiedQuotaObservation>,
    },
    TransientFailure {
        reason: WarmupAttemptReason,
        error_detail: String,
    },
    PermanentFailure {
        reason: WarmupAttemptReason,
        error_detail: String,
    },
}

fn warmup_preflight_skip_reason(upstream: &UpstreamRecord) -> Option<WarmupAttemptReason> {
    if upstream.deleted_at_unix_secs.is_some() {
        return Some(WarmupAttemptReason::UpstreamDeleted);
    }
    if upstream.kind != UpstreamKind::AnthropicOauth
        || !upstream.enabled
        || !upstream.warmup_enabled
    {
        return Some(WarmupAttemptReason::UpstreamDisabled);
    }
    if upstream.oauth_credentials.is_none() {
        return Some(WarmupAttemptReason::OauthCredentialsMissing);
    }
    None
}

fn response_is_auth_failed(attempt: &WarmupDispatchAttempt, expected_cycle_key: i64) -> bool {
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

fn execution_result_from_dispatch_attempt(
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

fn transient_attempt_error(attempt: &WarmupDispatchAttempt, http_status: Option<i32>) -> String {
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

fn pre_request_error_reason(error: &SchedulerError) -> Option<WarmupAttemptReason> {
    let detail = error.to_string();
    if detail.contains("oauth decrypt failed") {
        return Some(WarmupAttemptReason::CredentialDecryptFailed);
    }
    if detail.contains("missing oauth credentials") {
        return Some(WarmupAttemptReason::OauthCredentialsMissing);
    }
    None
}

fn cycle_key_i64(cycle_key: u64) -> SchedulerResult<i64> {
    i64::try_from(cycle_key)
        .map_err(|_| SchedulerError::Job("warmup cycle key exceeds i64".to_owned()))
}

fn now_unix_secs_i64() -> SchedulerResult<i64> {
    i64::try_from(now_unix_secs())
        .map_err(|_| SchedulerError::Job("current unix timestamp exceeds i64".to_owned()))
}

fn parse_headers(headers: &HeaderMap) -> Vec<UnifiedQuotaObservation> {
    parse_anthropic_unified_headers(headers)
}
