#[allow(deprecated)]
use cc_lb_core::subscription_quota_events::unified_observation_to_record;
use cc_lb_core::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::idempotency::WarmupEffectsStore;
use cc_lb_scheduler::jobs::warmup::{UpstreamWarmupJob, UpstreamWarmupJobHandler};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::SchedulerBackend;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::UpstreamStatusUpdate;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};
use chrono::{DateTime, TimeZone, Utc};
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
const FIVE_HOURS_SECS: i64 = 5 * 3600;

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
                self.write_status_after_warmup_success(upstream.id, cycle_key, response_cycle_key)
                    .await;
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
        candidate_cycle_key: i64,
        response_cycle_key: i64,
    ) {
        let Some(next_warmup_at) =
            next_warmup_at_after_success(upstream_id, candidate_cycle_key, response_cycle_key)
        else {
            tracing::warn!(upstream_id = %upstream_id, cycle_key = candidate_cycle_key, "could not compute next_warmup_at; admin status will lag");
            return;
        };
        #[allow(deprecated)]
        let status = UpstreamStatusUpdate {
            next_warmup_at: Some(Some(next_warmup_at)),
            last_warmup_cycle_key: Some(Some(response_cycle_key.max(candidate_cycle_key))),
            last_warmup_at_unix_secs: Some(Some(now_unix_secs())),
            ..UpstreamStatusUpdate::default()
        };
        if let Err(error) =
            UpstreamStore::set_status(self.storage.as_ref(), upstream_id, status).await
        {
            tracing::warn!(upstream_id = %upstream_id, %error, "warmup status writeback failed; admin UI may show stale data");
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

const POST_RESET_GUARD_SECS: i64 = 30;

fn next_warmup_at_after_success(
    upstream_id: uuid::Uuid,
    candidate_cycle_key: i64,
    response_cycle_key: i64,
) -> Option<DateTime<Utc>> {
    let schedule_anchor = if response_cycle_key > candidate_cycle_key {
        response_cycle_key
    } else {
        candidate_cycle_key.saturating_add(FIVE_HOURS_SECS)
    };
    let anchor_u64 = u64::try_from(schedule_anchor).ok()?;
    let jitter_ms = stable_jitter_ms(upstream_id, anchor_u64);
    let base = Utc.timestamp_opt(schedule_anchor, 0).single()?;
    base.checked_add_signed(chrono::Duration::seconds(POST_RESET_GUARD_SECS))
        .and_then(|value| {
            value.checked_add_signed(chrono::Duration::milliseconds(jitter_ms as i64))
        })
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
