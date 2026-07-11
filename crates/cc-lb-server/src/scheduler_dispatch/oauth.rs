use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_engine::clock::unix_secs;
use cc_lb_oauth_protocol::{ExistingTokenParts, refreshed_token_parts};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshJob, OAuthRefreshJobHandler, RefreshedOAuthTokens,
};
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};

use crate::scheduler_dispatch::http::{decrypt_bundle, fetch_usage, request_refresh};
use crate::scheduler_dispatch::storage::{StorageHandle, storage_scheduler_error};
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::scheduler_dispatch::usage::observe_usage_body;

use super::SchedulerDispatch;

const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;

impl SchedulerDispatch {
    pub(super) async fn dispatch_oauth_refresh(
        &self,
        job: OAuthRefreshJob,
    ) -> SchedulerResult<JobOutcome> {
        let replica_id = self
            .replica_id
            .ok_or_else(|| SchedulerError::Job("scheduler replica id is unavailable".to_owned()))?;
        OAuthRefreshJobHandler::new(StorageHandle::new(self.storage.clone()), replica_id)
            .handle(
                job,
                unix_secs(self.clock.now()),
                |upstream| self.refresh_upstream(upstream),
                |metadata_job| self.enqueue_metadata_refresh(metadata_job),
                |upstream_id, expires_at_unix_secs| {
                    self.push_next_oauth_refresh_task(upstream_id, expires_at_unix_secs)
                },
            )
            .await
    }

    async fn refresh_upstream(
        &self,
        upstream: UpstreamRecord,
    ) -> SchedulerResult<RefreshedOAuthTokens> {
        let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
        let response = request_refresh(
            &self.http,
            self.oauth_cfg.as_ref(),
            &self.cancel,
            &bundle.refresh_token,
        )
        .await?;
        let refreshed = refreshed_token_parts(
            ExistingTokenParts {
                refresh_token: bundle.refresh_token,
                scopes: bundle.scopes,
            },
            response,
            unix_secs(self.clock.now()),
        );
        let updated = OAuthTokenBundle {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            expires_at_unix_secs: refreshed.expires_at_unix_secs,
            scopes: refreshed.scopes,
        };
        let encrypted_tokens =
            EncryptedOAuthTokens::encrypt(self.aead.as_ref(), &updated, upstream.id.as_bytes())
                .map_err(|error| SchedulerError::Job(error.to_string()))?;
        Ok(RefreshedOAuthTokens {
            encrypted_tokens,
            expires_at_unix_secs: updated.expires_at_unix_secs,
        })
    }

    async fn enqueue_metadata_refresh(&self, job: MetadataRefreshJob) -> SchedulerResult<()> {
        self.backend
            .push_job(AdaptiveJob::MetadataRefresh(job))
            .await
    }

    async fn push_next_oauth_refresh_task(
        &self,
        upstream_id: uuid::Uuid,
        expires_at_unix_secs: u64,
    ) -> SchedulerResult<()> {
        let job = OAuthRefreshJob::new(upstream_id);
        let idempotency_key = job.idempotency_key(expires_at_unix_secs);
        let task = SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(job),
            idempotency_key: Some(idempotency_key),
            run_at_unix_secs: Some(OAuthRefreshJob::run_at_for_expires_at(expires_at_unix_secs)),
            max_attempts: None,
        };
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn poll_usage(
        &self,
        upstream_id: uuid::Uuid,
        _traceparent: Option<&str>,
    ) -> SchedulerResult<OAuthUsagePollObservation> {
        let Some(mut upstream) = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .map_err(storage_scheduler_error)?
        else {
            return Ok(OAuthUsagePollObservation::Skip);
        };
        if !should_poll_oauth_usage(&upstream) {
            return Ok(OAuthUsagePollObservation::Skip);
        }
        self.ensure_fresh_usage_token(&mut upstream).await?;
        let observed_at_unix_secs = unix_secs(self.clock.now());
        let first_response = match self.fetch_usage_with_current_token(&upstream).await {
            FetchOutcome::Response(response) => response,
            FetchOutcome::Network => {
                return Ok(OAuthUsagePollObservation::NetworkFailure {
                    observed_at_unix_secs,
                });
            }
        };
        let response = if first_response.status == http::StatusCode::UNAUTHORIZED {
            match self.force_refresh_usage_token(&mut upstream).await {
                Ok(true) => match self.fetch_usage_with_current_token(&upstream).await {
                    FetchOutcome::Response(retry_response) => retry_response,
                    FetchOutcome::Network => {
                        return Ok(OAuthUsagePollObservation::NetworkFailure {
                            observed_at_unix_secs,
                        });
                    }
                },
                Ok(false) => first_response,
                Err(error) => {
                    tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage force-refresh failed");
                    first_response
                }
            }
        } else {
            first_response
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
        let observed_at_unix_millis = now_unix_millis(&*self.clock);
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

    async fn fetch_usage_with_current_token(&self, upstream: &UpstreamRecord) -> FetchOutcome {
        let bundle = match decrypt_bundle(upstream, self.aead.as_ref()) {
            Ok(bundle) => bundle,
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage decrypt bundle failed");
                return FetchOutcome::Network;
            }
        };
        match fetch_usage(
            &self.http,
            &bundle.access_token,
            "cc-lb scheduler oauth usage poller",
            &self.cancel,
        )
        .await
        {
            Ok(response) => FetchOutcome::Response(response),
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage poll fetch failed");
                FetchOutcome::Network
            }
        }
    }

    async fn ensure_fresh_usage_token(&self, upstream: &mut UpstreamRecord) -> SchedulerResult<()> {
        let Some(lazy_refresher) = self.lazy_refresher.as_ref() else {
            return Ok(());
        };
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        if bundle
            .expires_at_unix_secs
            .saturating_sub(TOKEN_REFRESH_LOOKAHEAD_SECS)
            > unix_secs(self.clock.now())
        {
            return Ok(());
        }
        if let Err(error) = lazy_refresher.refresh_one(upstream.id).await {
            tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage proactive refresh failed");
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

    async fn force_refresh_usage_token(
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
}

enum FetchOutcome {
    Response(crate::scheduler_dispatch::http::UsageFetchResponse),
    Network,
}

fn should_poll_oauth_usage(upstream: &UpstreamRecord) -> bool {
    upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
}

#[cfg(test)]
mod tests;
