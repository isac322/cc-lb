use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_engine::clock::unix_secs;
use cc_lb_oauth_protocol::{ExistingTokenParts, refreshed_token_parts};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshJob, OAuthRefreshJobHandler, RefreshOutcome, RefreshedOAuthTokens,
};
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};

use cc_lb_control::anthropic_metadata::{
    CEDAR_EMBER_FENCE_RECOVERY_AFTER_MILLIS, CedarEmberIdentityRecord, CedarEmberPollRecord,
    CedarEmberStatus, cedar_ember_epoch_meta_key, cedar_ember_fence_started_at_millis,
    cedar_ember_identity_meta_key, cedar_ember_meta_key, fetch_oauth_profile_at,
    make_metadata_http_client, settled_cedar_ember_epoch,
};

use crate::scheduler_dispatch::http::{
    decrypt_bundle, fetch_usage, request_refresh, upstream_base_url,
};
use crate::scheduler_dispatch::storage::{StorageHandle, storage_scheduler_error};
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::scheduler_dispatch::usage::observe_usage_body;

use super::SchedulerDispatch;

const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;

/// Coarse time bucket that re-keys long-lived metadata refreshes. Anthropic
/// refreshing access tokens live ~8 hours, so `oauth_token_generation` — the
/// discriminator in `MetadataRefreshJob::idempotency_key` — advances about
/// once per 8 hours on the refreshing path. No existing constant names that
/// lifetime (it arrives in the token endpoint's `expires_in`), so it is
/// pinned here: 8 * 60 * 60.
const LONG_LIVED_METADATA_REFRESH_BUCKET_SECS: u64 = 28_800;

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

    async fn refresh_upstream(&self, upstream: UpstreamRecord) -> SchedulerResult<RefreshOutcome> {
        let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
        if bundle.never_refresh {
            // Refreshing a 365-day token makes Anthropic revoke it and issue an
            // 8-hour token instead; long-lived credentials must never reach the
            // token endpoint.
            return Ok(RefreshOutcome::NotRefreshable);
        }
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
                refresh_token_expires_at_unix_secs: bundle.refresh_token_expires_at_unix_secs,
                scopes: bundle.scopes,
            },
            response,
            unix_secs(self.clock.now()),
        );
        let updated = OAuthTokenBundle {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            expires_at_unix_secs: refreshed.expires_at_unix_secs,
            refresh_token_expires_at_unix_secs: refreshed.refresh_token_expires_at_unix_secs,
            scopes: refreshed.scopes,
            never_refresh: bundle.never_refresh,
        };
        let encrypted_tokens =
            EncryptedOAuthTokens::encrypt(self.aead.as_ref(), &updated, upstream.id.as_bytes())
                .map_err(|error| SchedulerError::Job(error.to_string()))?;
        Ok(RefreshOutcome::Refreshed(RefreshedOAuthTokens {
            encrypted_tokens,
            expires_at_unix_secs: updated.expires_at_unix_secs,
        }))
    }

    async fn enqueue_metadata_refresh(&self, job: MetadataRefreshJob) -> SchedulerResult<()> {
        self.backend
            .push_job(AdaptiveJob::MetadataRefresh(job))
            .await
    }

    /// Enqueues a metadata refresh for a long-lived credential, which never
    /// reaches the token-refresh path that normally schedules this job. A
    /// long-lived credential's `oauth_token_generation` is frozen, so the
    /// generation in `MetadataRefreshJob::idempotency_key` cannot rotate the
    /// key — and the unique index on `(job_type, idempotency_key)` covers
    /// `Done` rows until housekeeping reaps them (`dlq_retention_days`,
    /// default 30 days). Appending a coarse time bucket rotates the key on
    /// the same ~8-hour cadence at which a refreshing credential's
    /// generation advances, while repeated 60-second usage-poll ticks inside
    /// one bucket still collapse into a single job. The job payload keeps
    /// the real generation so the handler's stale check still applies, and a
    /// `Conflict` means this bucket's refresh is already queued — not an
    /// error.
    async fn enqueue_metadata_refresh_for_long_lived(
        &self,
        upstream: &UpstreamRecord,
        traceparent: Option<&str>,
    ) -> SchedulerResult<()> {
        let mut job = MetadataRefreshJob::new(upstream.id, upstream.oauth_token_generation);
        job.traceparent = traceparent.map(str::to_owned);
        let bucket = unix_secs(self.clock.now()) / LONG_LIVED_METADATA_REFRESH_BUCKET_SECS;
        let idempotency_key = format!("{}:{bucket}", job.idempotency_key());
        let task = SchedulerPushTask {
            args: AdaptiveJob::MetadataRefresh(job),
            idempotency_key: Some(idempotency_key),
            run_at_unix_secs: None,
            max_attempts: None,
        };
        match self.backend.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Clears a previously recorded apply error once the credential proves
    /// usable again, mirroring `complete_refresh` clearing the error on a
    /// successful token refresh.
    async fn clear_recorded_apply_error(&self, upstream: &UpstreamRecord) -> SchedulerResult<()> {
        if upstream.last_apply_error.is_none() {
            return Ok(());
        }
        UpstreamStore::set_last_apply_error(self.storage.as_ref(), upstream.id, None)
            .await
            .map_err(storage_scheduler_error)
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
        traceparent: Option<&str>,
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
        // The invalidation epoch is captured before the provider request is
        // issued: a claim that lands mid-flight bumps the epoch, and the
        // snapshot this response produces is then unservable regardless of
        // write ordering or replica clock skew.
        let epoch = self.cedar_ember_epoch(upstream.id).await;
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
        let cedar_ember = observe_usage_body(
            upstream.id,
            &response.body,
            observed_at_unix_millis,
            &self.subscription_quota_sink,
            self.subscription_quota_cache.as_ref(),
        )?;
        if let Some(epoch) = epoch {
            let fingerprint = upstream.oauth_credential_fingerprint().unwrap_or_default();
            self.persist_cedar_ember(
                upstream.id,
                fingerprint,
                epoch,
                cedar_ember.clone(),
                observed_at_unix_millis,
            )
            .await;
            if cedar_ember.is_some() {
                self.ensure_cedar_ember_identity(&upstream, fingerprint)
                    .await;
            }
        }
        self.clear_recorded_apply_error(&upstream).await?;
        if upstream.oauth_never_refresh {
            self.enqueue_metadata_refresh_for_long_lived(&upstream, traceparent)
                .await?;
        }
        Ok(OAuthUsagePollObservation::Success {
            observed_at_unix_secs,
            window_start_unix_millis: observed_at_unix_millis,
            window_end_unix_millis: observed_at_unix_millis,
        })
    }

    /// Reads the claim-invalidation epoch. `Some(inner)` is the settled epoch
    /// to embed in the snapshot (`None` when no claim has ever run); `None`
    /// — a live claim fence, an unreadable value, a lost recovery race, or a
    /// failed read — fails closed: the coupon write is skipped entirely.
    ///
    /// A well-formed fence older than [`CEDAR_EMBER_FENCE_RECOVERY_AFTER_MILLIS`]
    /// belongs to an abandoned claim (cancelled handler or failed settle
    /// write). The poll settles it with a fresh epoch via compare-and-put and
    /// only then issues its provider request, so the snapshot it stamps with
    /// that epoch is observed after any consumption the abandoned claim may
    /// have caused. The claim POST itself is never replayed.
    async fn cedar_ember_epoch(&self, upstream_id: uuid::Uuid) -> Option<Option<uuid::Uuid>> {
        let key = cedar_ember_epoch_meta_key(upstream_id);
        let raw = match self.storage.get_meta_value(&key).await {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember epoch read failed; skipping coupon write");
                return None;
            }
        };
        if let Some(epoch) = settled_cedar_ember_epoch(raw.as_deref()) {
            return Some(epoch);
        }
        let raw = raw?;
        let expired = cedar_ember_fence_started_at_millis(&raw).is_some_and(|started| {
            now_unix_millis(&*self.clock).saturating_sub(started)
                >= CEDAR_EMBER_FENCE_RECOVERY_AFTER_MILLIS
        });
        if !expired {
            tracing::info!(%upstream_id, "cedar_ember epoch not settled; skipping coupon write");
            return None;
        }
        let recovered = uuid::Uuid::new_v4();
        match self
            .storage
            .compare_and_put_meta_value(&key, Some(&raw), &recovered.to_string())
            .await
        {
            Ok(true) => {
                tracing::warn!(%upstream_id, "settled abandoned cedar_ember claim fence");
                Some(Some(recovered))
            }
            Ok(false) => {
                tracing::info!(%upstream_id, "cedar_ember fence changed during recovery; skipping coupon write");
                None
            }
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember fence recovery failed; skipping coupon write");
                None
            }
        }
    }

    /// Persists the polled `cedar_ember` block so every replica — not just
    /// the one that polled — can serve it. Best-effort: a failed write is
    /// retried by the next tick.
    async fn persist_cedar_ember(
        &self,
        upstream_id: uuid::Uuid,
        credential_fingerprint: u64,
        epoch: Option<uuid::Uuid>,
        status: Option<CedarEmberStatus>,
        observed_at_unix_millis: u64,
    ) {
        let record = CedarEmberPollRecord {
            credential_fingerprint,
            epoch,
            observed_at_unix_millis,
            status,
        };
        let value = match serde_json::to_string(&record) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember snapshot serialize failed");
                return;
            }
        };
        if let Err(error) = self
            .storage
            .put_meta_value(&cedar_ember_meta_key(upstream_id), &value)
            .await
        {
            tracing::warn!(%upstream_id, %error, "cedar_ember snapshot write failed");
        }
    }

    /// Fetches the OAuth identity once per credential — only when a coupon
    /// exists and no identity is bound to this fingerprint yet. The claim
    /// endpoint still re-verifies the live profile; this record only fills
    /// the GET's account/org fields.
    async fn ensure_cedar_ember_identity(
        &self,
        upstream: &UpstreamRecord,
        credential_fingerprint: u64,
    ) {
        let upstream_id = upstream.id;
        let key = cedar_ember_identity_meta_key(upstream_id);
        match self.storage.get_meta_value(&key).await {
            Ok(Some(raw)) => {
                let bound = serde_json::from_str::<CedarEmberIdentityRecord>(&raw)
                    .map(|record| record.credential_fingerprint == credential_fingerprint)
                    .unwrap_or(false);
                if bound {
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember identity read failed");
                return;
            }
        }
        let bundle = match decrypt_bundle(upstream, self.aead.as_ref()) {
            Ok(bundle) => bundle,
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember identity decrypt failed");
                return;
            }
        };
        let base_url = match upstream_base_url(upstream) {
            Ok(base_url) => base_url,
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember identity base url invalid");
                return;
            }
        };
        let client = make_metadata_http_client();
        let profile = match fetch_oauth_profile_at(
            &client,
            &base_url,
            &bundle.access_token,
            "cc-lb scheduler oauth usage poller",
            &self.cancel,
        )
        .await
        {
            Ok(profile) => profile,
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember identity profile fetch failed");
                return;
            }
        };
        let (Some(account_id), Some(organization_id)) = (
            profile.account.and_then(|account| account.uuid),
            profile
                .organization
                .and_then(|organization| organization.uuid),
        ) else {
            tracing::warn!(%upstream_id, "cedar_ember identity profile missing account/org uuid");
            return;
        };
        let record = CedarEmberIdentityRecord {
            credential_fingerprint,
            account_id,
            organization_id,
        };
        let value = match serde_json::to_string(&record) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember identity serialize failed");
                return;
            }
        };
        if let Err(error) = self.storage.put_meta_value(&key, &value).await {
            tracing::warn!(%upstream_id, %error, "cedar_ember identity write failed");
        }
    }

    async fn fetch_usage_with_current_token(&self, upstream: &UpstreamRecord) -> FetchOutcome {
        let bundle = match decrypt_bundle(upstream, self.aead.as_ref()) {
            Ok(bundle) => bundle,
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage decrypt bundle failed");
                return FetchOutcome::Network;
            }
        };
        let base_url = match upstream_base_url(upstream) {
            Ok(base_url) => base_url,
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage base url invalid");
                return FetchOutcome::Network;
            }
        };
        match fetch_usage(
            &self.http,
            &base_url,
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
        if bundle.never_refresh {
            // Long-lived credentials are never refreshed.
            return Ok(());
        }
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
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        if bundle.never_refresh {
            // Long-lived credentials are never refreshed; a 401 is terminal and
            // must be recorded durably so the upstream surfaces as broken.
            let reason = "status_401".to_owned();
            UpstreamStore::set_last_apply_error(
                self.storage.as_ref(),
                upstream.id,
                Some(reason.clone()),
            )
            .await
            .map_err(storage_scheduler_error)?;
            upstream.last_apply_error = Some(reason);
            return Ok(false);
        }
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
