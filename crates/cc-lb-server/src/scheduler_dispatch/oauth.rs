use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::idempotency::{
    OAuthRefreshClaimsStore, OAuthUsagePollCursorsStore, OAuthUsagePollScheduleConfig,
};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{OAuthRefreshJob, OAuthRefreshJobHandler};
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    OAuthUsagePollHandler, OAuthUsagePollJob, OAuthUsagePollObservation,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{EntityJob, SchedulerBackend};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};

use crate::scheduler_dispatch::http::{decrypt_bundle, fetch_usage, request_refresh};
use crate::scheduler_dispatch::storage::{StorageHandle, storage_scheduler_error};
use crate::scheduler_dispatch::time::{now_unix_millis, now_unix_secs};
use crate::scheduler_dispatch::usage::observe_usage_body;

use super::SchedulerDispatch;

impl SchedulerDispatch {
    pub(super) async fn dispatch_oauth_refresh(
        &self,
        job: OAuthRefreshJob,
    ) -> SchedulerResult<JobOutcome> {
        let replica_id = self
            .replica_id
            .ok_or_else(|| SchedulerError::Job("scheduler replica id is unavailable".to_owned()))?;
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                OAuthRefreshJobHandler::new(
                    OAuthRefreshClaimsStore::new(sqlite.pool.clone()),
                    StorageHandle::new(self.storage.clone()),
                    replica_id,
                )
                .handle(
                    job,
                    now_unix_secs(),
                    |upstream| self.refresh_upstream(upstream),
                    |metadata_job| self.enqueue_metadata_refresh(metadata_job),
                )
                .await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                OAuthRefreshJobHandler::new(
                    OAuthRefreshClaimsStore::new(postgres.pool.clone()),
                    StorageHandle::new(self.storage.clone()),
                    replica_id,
                )
                .handle(
                    job,
                    now_unix_secs(),
                    |upstream| self.refresh_upstream(upstream),
                    |metadata_job| self.enqueue_metadata_refresh(metadata_job),
                )
                .await
            }
        }
    }

    pub(super) async fn dispatch_oauth_usage_poll(
        &self,
        job: OAuthUsagePollJob,
    ) -> SchedulerResult<JobOutcome> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                OAuthUsagePollHandler::new(
                    OAuthUsagePollCursorsStore::new(sqlite.pool.clone()),
                    OAuthUsagePollScheduleConfig::default(),
                )
                .handle(job, now_unix_secs(), |job| self.poll_usage(job))
                .await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                OAuthUsagePollHandler::new(
                    OAuthUsagePollCursorsStore::new(postgres.pool.clone()),
                    OAuthUsagePollScheduleConfig::default(),
                )
                .handle(job, now_unix_secs(), |job| self.poll_usage(job))
                .await
            }
        }
    }

    async fn refresh_upstream(
        &self,
        upstream: UpstreamRecord,
    ) -> SchedulerResult<EncryptedOAuthTokens> {
        let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
        let response = request_refresh(
            &self.http,
            self.oauth_cfg.as_ref(),
            &self.cancel,
            &bundle.refresh_token,
        )
        .await?;
        let scopes = response
            .scope
            .as_deref()
            .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
            .unwrap_or(bundle.scopes);
        let updated = OAuthTokenBundle {
            access_token: response.access_token,
            refresh_token: response.refresh_token.unwrap_or(bundle.refresh_token),
            expires_at_unix_secs: now_unix_secs().saturating_add(response.expires_in),
            scopes,
        };
        EncryptedOAuthTokens::encrypt(self.aead.as_ref(), &updated, upstream.id.as_bytes())
            .map_err(|error| SchedulerError::Job(error.to_string()))
    }

    async fn enqueue_metadata_refresh(&self, job: MetadataRefreshJob) -> SchedulerResult<()> {
        self.backend.push_job(EntityJob::MetadataRefresh(job)).await
    }

    async fn poll_usage(
        &self,
        job: OAuthUsagePollJob,
    ) -> SchedulerResult<OAuthUsagePollObservation> {
        let Some(upstream) = UpstreamStore::get_by_id(self.storage.as_ref(), job.upstream_id)
            .await
            .map_err(storage_scheduler_error)?
        else {
            return Ok(OAuthUsagePollObservation::Skip);
        };
        if upstream.kind != UpstreamKind::AnthropicOauth
            || !upstream.enabled
            || upstream.deleted_at_unix_secs.is_some()
        {
            return Ok(OAuthUsagePollObservation::Skip);
        }
        let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
        let observed_at_unix_secs = now_unix_secs();
        let response = match fetch_usage(
            &self.http,
            &bundle.access_token,
            "cc-lb scheduler oauth usage poller",
            &self.cancel,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, error = %error, "oauth usage poll failed");
                return Ok(OAuthUsagePollObservation::NetworkFailure {
                    observed_at_unix_secs,
                });
            }
        };
        if response.status == http::StatusCode::TOO_MANY_REQUESTS {
            return Ok(OAuthUsagePollObservation::Throttled {
                observed_at_unix_secs,
            });
        }
        if !response.status.is_success() {
            return Ok(OAuthUsagePollObservation::StatusFailure {
                observed_at_unix_secs,
                status: response.status.as_u16(),
            });
        }
        let observed_at_unix_millis = now_unix_millis();
        observe_usage_body(
            upstream.id,
            &response.body,
            observed_at_unix_millis,
            &self.subscription_quota_sink,
            self.subscription_quota_cache.as_ref(),
        )?;
        Ok(OAuthUsagePollObservation::Success {
            observed_at_unix_secs,
            window_start_unix_millis: observed_at_unix_millis,
            window_end_unix_millis: observed_at_unix_millis,
        })
    }
}
