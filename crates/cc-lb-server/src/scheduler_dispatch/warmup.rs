use cc_lb_core::subscription_quota_events::unified_observation_to_record;
use cc_lb_core::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::idempotency::WarmupEffectsStore;
use cc_lb_scheduler::jobs::warmup::{UpstreamWarmupJob, UpstreamWarmupJobHandler};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::SchedulerBackend;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};
use http::HeaderMap;

use crate::scheduler_dispatch::http::{decrypt_bundle, upstream_base_url};
use crate::scheduler_dispatch::outcomes::warmup_outcome;
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::{now_unix_millis, now_unix_secs};
use crate::warmup::{WarmupAbandonReason, WarmupResult, classify_response, dispatch_warmup};

use super::SchedulerDispatch;

impl SchedulerDispatch {
    pub(super) async fn dispatch_warmup(
        &self,
        job: UpstreamWarmupJob,
    ) -> SchedulerResult<JobOutcome> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => warmup_outcome(
                UpstreamWarmupJobHandler::new(WarmupEffectsStore::new(sqlite.pool.clone()))
                    .handle(
                        job,
                        now_unix_secs(),
                        |upstream_id| self.upstream_is_warmup_live(upstream_id),
                        |job| self.fire_warmup(job),
                    )
                    .await?,
            ),
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => warmup_outcome(
                UpstreamWarmupJobHandler::new(WarmupEffectsStore::new(postgres.pool.clone()))
                    .handle(
                        job,
                        now_unix_secs(),
                        |upstream_id| self.upstream_is_warmup_live(upstream_id),
                        |job| self.fire_warmup(job),
                    )
                    .await?,
            ),
        }
    }

    async fn upstream_is_warmup_live(&self, upstream_id: uuid::Uuid) -> SchedulerResult<bool> {
        let upstream = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .map_err(storage_scheduler_error)?;
        Ok(upstream.as_ref().is_some_and(is_warmup_live))
    }

    async fn fire_warmup(&self, job: UpstreamWarmupJob) -> SchedulerResult<()> {
        let upstream = UpstreamStore::get_by_id(self.storage.as_ref(), job.upstream_id)
            .await
            .map_err(storage_scheduler_error)?
            .ok_or_else(|| SchedulerError::Job("warmup upstream disappeared".to_owned()))?;
        let cycle_key = i64::try_from(job.cycle_key)
            .map_err(|_| SchedulerError::Job("warmup cycle key exceeds i64".to_owned()))?;
        let (status, observations) = self.dispatch_warmup_request(&upstream).await?;
        match classify_response(status, &observations, cycle_key) {
            WarmupResult::Success { cycle_key: _ }
            | WarmupResult::WindowAlreadyActive { cycle_key: _ } => {
                self.record_warmup_observations(upstream.id, observations)
            }
            WarmupResult::RetryableTransient => Err(SchedulerError::Job(format!(
                "warmup returned transient status {status}"
            ))),
            WarmupResult::AbandonCyclePermanent(reason) => {
                tracing::warn!(upstream_id = %upstream.id, reason = reason.as_str(), "warmup cycle abandoned");
                Ok(())
            }
        }
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
