#[allow(deprecated)]
use cc_lb_core::subscription_quota_events::unified_observation_to_record;
use cc_lb_core::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::warmup::{UpstreamWarmupJob, UpstreamWarmupJobHandler};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{EntityJob, SchedulerPushTask};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::UpstreamStatusUpdate;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};
use http::HeaderMap;

use crate::scheduler_dispatch::http::{decrypt_bundle, upstream_base_url};
use crate::scheduler_dispatch::outcomes::warmup_outcome;
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::{now_unix_millis, now_unix_secs};
use crate::warmup::{
    WarmupAbandonReason, WarmupResult, classify_response, dispatch_warmup, stable_jitter_ms,
};

use super::SchedulerDispatch;

const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;
const POST_RESET_GUARD_SECS: u64 = 30;

impl SchedulerDispatch {
    pub(super) async fn dispatch_warmup(
        &self,
        job: UpstreamWarmupJob,
    ) -> SchedulerResult<JobOutcome> {
        warmup_outcome(
            UpstreamWarmupJobHandler::new()
                .handle(
                    job,
                    now_unix_secs(),
                    |upstream_id| self.upstream_is_warmup_live(upstream_id),
                    |job| self.fire_warmup(job),
                )
                .await?,
        )
    }

    async fn upstream_is_warmup_live(&self, upstream_id: uuid::Uuid) -> SchedulerResult<bool> {
        let upstream = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .map_err(storage_scheduler_error)?;
        Ok(upstream.as_ref().is_some_and(is_warmup_live))
    }

    async fn fire_warmup(&self, job: UpstreamWarmupJob) -> SchedulerResult<()> {
        let mut upstream = UpstreamStore::get_by_id(self.storage.as_ref(), job.upstream_id)
            .await
            .map_err(storage_scheduler_error)?
            .ok_or_else(|| SchedulerError::Job("warmup upstream disappeared".to_owned()))?;
        let cycle_key = i64::try_from(job.cycle_key)
            .map_err(|_| SchedulerError::Job("warmup cycle key exceeds i64".to_owned()))?;
        self.ensure_fresh_oauth_token(&mut upstream).await?;
        let (status, observations) = self.dispatch_warmup_request(&upstream).await?;
        let result = classify_response(status, &observations, cycle_key);
        let (final_status, final_observations, final_result) = if matches!(
            result,
            WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::AuthFailed)
        ) {
            match self.force_refresh_oauth_token(&mut upstream).await {
                Ok(true) => {
                    let (status, observations) = self.dispatch_warmup_request(&upstream).await?;
                    let result = classify_response(status, &observations, cycle_key);
                    (status, observations, result)
                }
                Ok(false) => (status, observations, result),
                Err(error) => {
                    tracing::warn!(upstream_id = %upstream.id, %error, "warmup oauth force-refresh failed");
                    (status, observations, result)
                }
            }
        } else {
            (status, observations, result)
        };
        match final_result {
            WarmupResult::Success {
                cycle_key: response_cycle_key,
            }
            | WarmupResult::WindowAlreadyActive {
                cycle_key: response_cycle_key,
            } => {
                self.write_status_after_warmup_success(upstream.id, response_cycle_key)
                    .await?;
                self.record_warmup_observations(upstream.id, final_observations)
            }
            WarmupResult::RetryableTransient => Err(SchedulerError::Job(format!(
                "warmup returned transient status {final_status}"
            ))),
            WarmupResult::AbandonCyclePermanent(reason) => {
                tracing::warn!(upstream_id = %upstream.id, reason = reason.as_str(), "warmup cycle abandoned");
                Ok(())
            }
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

        let jitter_secs = stable_jitter_ms(upstream_id, response_resets_at_unix_secs) / 1_000;
        let run_at_unix_secs = response_resets_at_unix_secs
            .saturating_add(POST_RESET_GUARD_SECS)
            .saturating_add(jitter_secs);
        let job = UpstreamWarmupJob::new(upstream_id, response_resets_at_unix_secs);
        let task = SchedulerPushTask {
            args: EntityJob::Warmup(job),
            idempotency_key: Some(job.idempotency_key(response_resets_at_unix_secs)),
            run_at_unix_secs: Some(run_at_unix_secs),
        };
        match self.backend.push_entity_task(task).await {
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

    async fn dispatch_warmup_request(
        &self,
        upstream: &UpstreamRecord,
    ) -> SchedulerResult<(http::StatusCode, Vec<UnifiedQuotaObservation>)> {
        if upstream.warmup_dialect_plugin.is_some()
            && let Some(lazy_refresher) = self.lazy_refresher.as_ref()
        {
            let outcome = crate::warmup::dialect::dispatch_warmup_with_dialect(
                self.runtime.as_ref(),
                self.stores.as_ref(),
                self.data_dir.as_ref(),
                self.aead.clone(),
                lazy_refresher.clone(),
                upstream,
                &self.http,
            )
            .await
            .map_err(map_warmup_dialect_error)?;
            return Ok((outcome.status, parse_headers(&outcome.headers)));
        }
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        let base_url = upstream_base_url(upstream)?;
        let replica_id = self
            .replica_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_owned());
        Ok(dispatch_warmup(&self.http, &bundle.access_token, &base_url, &replica_id).await)
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

fn is_warmup_live(upstream: &UpstreamRecord) -> bool {
    upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.enabled
        && upstream.warmup_enabled
        && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
}

fn parse_headers(headers: &HeaderMap) -> Vec<UnifiedQuotaObservation> {
    parse_anthropic_unified_headers(headers)
}

fn map_warmup_dialect_error(error: crate::warmup::dialect::WarmupDispatchError) -> SchedulerError {
    match error {
        crate::warmup::dialect::WarmupDispatchError::Http(_)
        | crate::warmup::dialect::WarmupDispatchError::Storage(_)
        | crate::warmup::dialect::WarmupDispatchError::Materialize(_) => {
            SchedulerError::Job(error.to_string())
        }
        crate::warmup::dialect::WarmupDispatchError::MissingPlugin
        | crate::warmup::dialect::WarmupDispatchError::RegistryNotFound(_)
        | crate::warmup::dialect::WarmupDispatchError::RegistryUnsupportedSlot { .. }
        | crate::warmup::dialect::WarmupDispatchError::Instantiate(_)
        | crate::warmup::dialect::WarmupDispatchError::BodySerialize(_)
        | crate::warmup::dialect::WarmupDispatchError::Shape(_)
        | crate::warmup::dialect::WarmupDispatchError::Signer(_)
        | crate::warmup::dialect::WarmupDispatchError::RequestBuild(_) => {
            tracing::warn!(error = %error, reason = WarmupAbandonReason::DialectPlugin.as_str(), "warmup dialect failed permanently");
            SchedulerError::Job(error.to_string())
        }
    }
}
