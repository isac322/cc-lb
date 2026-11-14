use bytes::Bytes;
use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_clock::unix_secs;
use cc_lb_oauth_protocol::{
    ANTIGRAVITY_OAUTH_USER_AGENT, ExistingTokenParts, GeminiOAuthProfile, OAuthAccountIdentity,
    gemini_identity_from_responses, gemini_identity_is_bound, is_terminal_refresh_failure,
    oauth_account_from_bootstrap, openai_identity_from_token, refresh_requires_reconnect,
    refreshed_token_parts,
};
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
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, StorageError, UpstreamRecord, UpstreamStatusUpdate,
    UpstreamStore,
};

use cc_lb_control::anthropic_compat::{
    CLAUDE_CODE_LATEST_VERSION_FALLBACK, CLAUDE_CODE_LATEST_VERSION_KEY, claude_code_user_agent,
};

use cc_lb_control::anthropic_metadata::{
    CedarEmberIdentityRecord, CedarEmberPollRecord, CedarEmberStatus,
    cedar_ember_identity_meta_key, cedar_ember_meta_key, fetch_oauth_profile_at,
    make_metadata_http_client,
};
use cc_lb_control::gemini_period_unknown_quota::{
    GeminiRawQuotaBinding, persist_gemini_period_unknown_quota,
};

use crate::scheduler_dispatch::http::{
    RefreshRequestError, decrypt_bundle, fetch_gemini_account_metadata, fetch_gemini_quota,
    fetch_usage, gemini_summary_groups, request_refresh, upstream_base_url,
};
use crate::scheduler_dispatch::storage::{StorageHandle, storage_scheduler_error};
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::scheduler_dispatch::usage::parse_usage_body;

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
        if refresh_requires_reconnect(upstream.last_apply_error.as_deref()) {
            return Ok(RefreshOutcome::NotRefreshable);
        }
        let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
        if bundle.never_refresh {
            // Access-only imports and Anthropic long-lived grants must never
            // reach a token endpoint, including direct scheduler dispatch.
            return Ok(RefreshOutcome::NotRefreshable);
        }
        if bundle
            .refresh_token_expires_at_unix_secs
            .is_some_and(|deadline| deadline <= unix_secs(self.clock.now()))
        {
            self.record_reconnect_required(&upstream, "refresh_token_expired")
                .await?;
            return Ok(RefreshOutcome::NotRefreshable);
        }
        let response = match request_refresh(
            &self.http,
            self.oauth_cfg.as_ref(),
            &self.config,
            upstream.kind,
            bundle.oauth_profile,
            &self.cancel,
            &bundle.refresh_token,
        )
        .await
        {
            Ok(response) => response,
            Err(RefreshRequestError::Endpoint { status, body })
                if is_terminal_refresh_failure(status.as_u16(), &body) =>
            {
                let reason = if status == http::StatusCode::UNAUTHORIZED {
                    "status_401"
                } else {
                    "status_400"
                };
                self.record_reconnect_required(&upstream, reason).await?;
                return Ok(RefreshOutcome::NotRefreshable);
            }
            Err(RefreshRequestError::Endpoint { status, .. }) => {
                return Err(SchedulerError::Job(format!(
                    "oauth token endpoint returned {status}"
                )));
            }
            Err(RefreshRequestError::Other(error)) => return Err(error),
        };
        let account_id = response.account_id.clone().or(bundle.account_id.clone());
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
            account_id,
            project_id: bundle.project_id,
            oauth_profile: bundle.oauth_profile,
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

    async fn record_reconnect_required(
        &self,
        upstream: &UpstreamRecord,
        reason: &str,
    ) -> SchedulerResult<()> {
        match UpstreamStore::set_status(
            self.storage.as_ref(),
            upstream.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(reason.to_owned())),
                expected_oauth_token_generation: Some(upstream.oauth_token_generation),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
        {
            Ok(()) | Err(StorageError::Conflict { .. }) => Ok(()),
            Err(error) => Err(storage_scheduler_error(error)),
        }
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
        if !should_poll_oauth_usage(&upstream) {
            return Ok(OAuthUsagePollObservation::Skip);
        }
        let is_anthropic = upstream.kind == UpstreamKind::AnthropicOauth;
        let is_antigravity = upstream.kind == UpstreamKind::GeminiOauth
            && decrypt_bundle(&upstream, self.aead.as_ref())
                .ok()
                .is_some_and(|bundle| bundle.oauth_profile == GeminiOAuthProfile::Antigravity);
        let user_agent = if is_anthropic {
            self.claude_code_user_agent().await?
        } else if is_antigravity {
            ANTIGRAVITY_OAUTH_USER_AGENT.to_owned()
        } else {
            String::new()
        };
        let fence = self.capture_usage_poll_fence(&upstream).await?;
        let poll_sequence = self.subscription_quota_cache.begin_poll();
        let observed_at_unix_secs = unix_secs(self.clock.now());
        let first_response = match self
            .fetch_usage_with_current_token(&upstream, &user_agent)
            .await
        {
            FetchOutcome::Response(response) => response,
            FetchOutcome::Network => {
                return Ok(OAuthUsagePollObservation::NetworkFailure {
                    observed_at_unix_secs,
                });
            }
        };
        let response = if first_response.status == http::StatusCode::UNAUTHORIZED {
            match self.force_refresh_usage_token(&mut upstream).await {
                Ok(true) => match self
                    .fetch_usage_with_current_token(&upstream, &user_agent)
                    .await
                {
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
        let openai_inventory = self
            .fetch_openai_reset_inventory(&upstream, &fence, &response.body)
            .await?;
        self.validate_usage_poll_fence(&upstream, &fence).await?;
        let observed_at_unix_millis = now_unix_millis(&*self.clock);
        let (mut records, cedar_ember, affected_main_published, raw_updates) = if is_anthropic {
            let (records, cedar_ember) =
                parse_usage_body(upstream.id, &response.body, observed_at_unix_millis)?;
            (
                records,
                cedar_ember,
                true,
                super::usage::GeminiRawUpdates::default(),
            )
        } else {
            let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
            let (records, affected_main_published, raw_updates) =
                super::usage::parse_provider_usage_body(
                    super::usage::ProviderUsageContext {
                        upstream_id: upstream.id,
                        kind: upstream.kind,
                        profile: bundle.oauth_profile,
                        project_id: bundle.project_id.as_deref(),
                    },
                    &response.body,
                    observed_at_unix_millis,
                )?;
            (records, None, affected_main_published, raw_updates)
        };
        if upstream.kind == UpstreamKind::OpenaiOauth {
            let order = self
                .subscription_quota_cache
                .credit_request_order(poll_sequence);
            for record in &mut records {
                if let Some(credits) = record.credits.as_mut() {
                    credits.stamp_observations(
                        observed_at_unix_millis,
                        cc_lb_domain::CreditSource::Api,
                    );
                    credits.stamp_request_order(order);
                }
            }
        }
        self.publish_usage_poll(&upstream, &fence, poll_sequence, &records)
            .await?;
        if upstream.kind == UpstreamKind::GeminiOauth {
            let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
            let binding = GeminiRawQuotaBinding::from_credential(
                bundle.account_id.as_deref(),
                bundle.project_id.as_deref(),
                upstream.oauth_token_generation,
            );
            self.validate_usage_poll_fence(&upstream, &fence).await?;
            if let Err(error) = persist_gemini_period_unknown_quota(
                self.storage.as_ref(),
                upstream.id,
                binding,
                raw_updates.gemini,
                raw_updates.third_party,
            )
            .await
            {
                tracing::warn!(upstream_id = %upstream.id, %error, "Gemini period-unknown quota write failed");
            }
        }
        if !is_anthropic {
            let bundle = decrypt_bundle(&upstream, self.aead.as_ref())?;
            self.enrich_provider_account_metadata(
                &upstream,
                &bundle,
                &response.body,
                observed_at_unix_millis,
            )
            .await;
        }
        if is_anthropic
            && self
                .validate_usage_poll_fence(&upstream, &fence)
                .await
                .is_ok()
        {
            let fingerprint = upstream.oauth_credential_fingerprint().unwrap_or_default();
            self.persist_cedar_ember(
                upstream.id,
                fingerprint,
                fence.epoch(),
                cedar_ember.clone(),
                observed_at_unix_millis,
            )
            .await;
            if cedar_ember.is_some() {
                self.ensure_cedar_ember_identity(&upstream, fingerprint, &user_agent)
                    .await;
            }
        }
        if let Some(record) = openai_inventory {
            self.persist_openai_reset_inventory(&upstream, &fence, &record)
                .await;
        }
        if is_anthropic && upstream.oauth_never_refresh {
            self.enqueue_metadata_refresh_for_long_lived(&upstream, traceparent)
                .await?;
        }
        Ok(OAuthUsagePollObservation::Success {
            observed_at_unix_secs,
            window_start_unix_millis: observed_at_unix_millis,
            window_end_unix_millis: observed_at_unix_millis,
            affected_main_quota_published: affected_main_published,
        })
    }

    /// Display metadata is independent of quota observation success. Failed
    /// enrichment never changes credentials, quotas, or the poll outcome.
    async fn enrich_provider_account_metadata(
        &self,
        upstream: &UpstreamRecord,
        bundle: &OAuthTokenBundle,
        usage_body: &[u8],
        observed_at_unix_millis: u64,
    ) {
        let expected_generation = upstream.oauth_token_generation;
        let metadata = match self
            .storage
            .get_upstream_subscription_metadata(upstream.id)
            .await
        {
            Ok(metadata) => metadata,
            Err(_) => return,
        };
        let mut identity = bound_provider_identity(
            upstream.kind,
            bundle,
            metadata.as_ref(),
            expected_generation,
        );
        let changed = match upstream.kind {
            UpstreamKind::OpenaiOauth => {
                let Ok(usage) = serde_json::from_slice::<serde_json::Value>(usage_body) else {
                    return;
                };
                merge_openai_usage_identity(&mut identity, bundle, &usage)
            }
            UpstreamKind::GeminiOauth => {
                let missing = identity.plan.is_none()
                    || (identity.account_name.is_none() && identity.plan_name.is_none());
                let recent = metadata.as_ref().is_some_and(|record| {
                    observed_at_unix_millis.saturating_sub(
                        u64::try_from(record.observed_at_unix_millis).unwrap_or_default(),
                    ) < LONG_LIVED_METADATA_REFRESH_BUCKET_SECS * 1_000
                });
                if recent && !missing {
                    return;
                }
                let (load, userinfo) =
                    fetch_gemini_account_metadata(&self.http, &self.config, bundle, &self.cancel)
                        .await;
                merge_gemini_account_metadata(
                    &mut identity,
                    bundle,
                    load.as_ref(),
                    userinfo.as_ref(),
                )
            }
            _ => false,
        };
        if !changed {
            return;
        }
        let mut bootstrap = serde_json::json!({"oauth_account": identity});
        if upstream.kind == UpstreamKind::GeminiOauth
            && bundle
                .account_id
                .as_deref()
                .is_none_or(|email| email.trim().is_empty())
        {
            bootstrap["oauth_account_generation"] = serde_json::json!(expected_generation);
        }
        let record = cc_lb_storage_api::UpstreamSubscriptionMetadataRecord {
            upstream_id: upstream.id,
            organization_uuid: None,
            organization_role: None,
            workspace_role: None,
            observed_at_unix_millis: i64::try_from(observed_at_unix_millis).unwrap_or(i64::MAX),
            last_error: None,
            raw_roles: None,
            raw_bootstrap: Some(bootstrap.to_string()),
        };
        #[cfg(test)]
        tests::before_provider_metadata_write().await;
        if let Err(error) = self
            .storage
            .compare_and_put_upstream_subscription_metadata(
                &record,
                expected_generation,
                metadata.as_ref(),
            )
            .await
        {
            tracing::warn!(upstream_id = %upstream.id, %error, "provider display metadata write failed");
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
        user_agent: &str,
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
            user_agent,
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

    /// Mirrors the metadata-refresh runner but reads the latest channel: the
    /// daily compat-refreshed Claude Code latest version, or the pinned
    /// latest-release fallback before the first refresh.
    async fn claude_code_user_agent(&self) -> SchedulerResult<String> {
        let version = AnthropicCompatibilityKvStore::get_compatibility_kv(
            self.storage.as_ref(),
            CLAUDE_CODE_LATEST_VERSION_KEY,
        )
        .await
        .map_err(storage_scheduler_error)?
        .map(|record| record.value)
        .unwrap_or_else(|| CLAUDE_CODE_LATEST_VERSION_FALLBACK.to_owned());
        Ok(claude_code_user_agent(&version))
    }

    async fn fetch_usage_with_current_token(
        &self,
        upstream: &UpstreamRecord,
        user_agent: &str,
    ) -> FetchOutcome {
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
        let summary_response = match fetch_usage(
            &self.http,
            &base_url,
            upstream.kind,
            bundle.oauth_profile,
            &bundle.access_token,
            bundle.account_id.as_deref(),
            bundle.project_id.as_deref(),
            user_agent,
            &self.cancel,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage poll fetch failed");
                return FetchOutcome::Network;
            }
        };
        if upstream.kind == UpstreamKind::GeminiOauth {
            let user_agent = match bundle.oauth_profile {
                GeminiOAuthProfile::GeminiCli => user_agent,
                GeminiOAuthProfile::Antigravity => {
                    cc_lb_oauth_protocol::ANTIGRAVITY_OAUTH_USER_AGENT
                }
            };
            return self
                .complete_gemini_usage(
                    &base_url,
                    &bundle.access_token,
                    bundle.project_id.as_deref(),
                    user_agent,
                    summary_response,
                )
                .await;
        }
        FetchOutcome::Response(summary_response)
    }

    async fn complete_gemini_usage(
        &self,
        base_url: &url::Url,
        access_token: &str,
        project_id: Option<&str>,
        user_agent: &str,
        summary_response: crate::scheduler_dispatch::http::UsageFetchResponse,
    ) -> FetchOutcome {
        if summary_response.status == http::StatusCode::UNAUTHORIZED
            || summary_response.status == http::StatusCode::TOO_MANY_REQUESTS
        {
            return FetchOutcome::Response(summary_response);
        }
        let present = if summary_response.status.is_success() {
            gemini_summary_groups(&summary_response.body).unwrap_or((false, false))
        } else {
            (false, false)
        };
        let mut fallback = serde_json::Map::new();
        let mut fallback_succeeded = false;
        if !present.0 || !present.1 {
            match fetch_gemini_quota(
                &self.http,
                base_url,
                access_token,
                project_id,
                user_agent,
                &self.cancel,
            )
            .await
            {
                Ok(response) if response.status == http::StatusCode::UNAUTHORIZED => {
                    return FetchOutcome::Response(response);
                }
                Ok(response) if response.status.is_success() => {
                    match sonic_rs::from_slice::<serde_json::Value>(&response.body) {
                        Ok(value) if value.is_object() => {
                            if !present.0 {
                                fallback.insert("gemini".to_owned(), value.clone());
                            }
                            if !present.1 {
                                fallback.insert("third_party".to_owned(), value);
                            }
                            fallback_succeeded = true;
                        }
                        Ok(_) => {
                            tracing::warn!(status = %response.status, "Gemini quota fallback was not a JSON object")
                        }
                        Err(error) => {
                            tracing::warn!(status = %response.status, line = error.line(), column = error.column(), "Gemini quota fallback JSON parse failed")
                        }
                    }
                }
                Ok(response) => {
                    tracing::warn!(status = %response.status, "Gemini quota fallback failed")
                }
                Err(error) => tracing::warn!(%error, "Gemini quota fallback fetch failed"),
            }
        }
        if !summary_response.status.is_success() && !fallback_succeeded {
            return FetchOutcome::Response(summary_response);
        }
        let summary = if summary_response.status.is_success() {
            serde_json::from_slice(&summary_response.body).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };
        let body = match serde_json::to_vec(&serde_json::json!({
            "summary": summary,
            "fallback": fallback,
        })) {
            Ok(body) => Bytes::from(body),
            Err(error) => {
                tracing::warn!(%error, "Gemini quota response envelope encode failed");
                return FetchOutcome::Network;
            }
        };
        FetchOutcome::Response(crate::scheduler_dispatch::http::UsageFetchResponse {
            status: http::StatusCode::OK,
            body,
        })
    }

    async fn ensure_fresh_usage_token(&self, upstream: &mut UpstreamRecord) -> SchedulerResult<()> {
        if refresh_requires_reconnect(upstream.last_apply_error.as_deref()) {
            return Ok(());
        }
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        if bundle.never_refresh {
            // Long-lived credentials are never refreshed.
            return Ok(());
        }
        if bundle
            .refresh_token_expires_at_unix_secs
            .is_some_and(|deadline| deadline <= unix_secs(self.clock.now()))
        {
            self.record_reconnect_required(upstream, "refresh_token_expired")
                .await?;
        } else {
            if bundle
                .expires_at_unix_secs
                .saturating_sub(TOKEN_REFRESH_LOOKAHEAD_SECS)
                > unix_secs(self.clock.now())
            {
                return Ok(());
            }
            let Some(lazy_refresher) = self.lazy_refresher.as_ref() else {
                return Ok(());
            };
            if let Err(error) = lazy_refresher.refresh_one(upstream.id).await {
                tracing::warn!(upstream_id = %upstream.id, %error, "oauth usage proactive refresh failed");
            }
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
        if refresh_requires_reconnect(upstream.last_apply_error.as_deref()) {
            return Ok(false);
        }
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        let reason = if bundle.never_refresh {
            // A long-lived credential cannot rotate after a usage endpoint 401.
            Some("status_401")
        } else if bundle
            .refresh_token_expires_at_unix_secs
            .is_some_and(|deadline| deadline <= unix_secs(self.clock.now()))
        {
            Some("refresh_token_expired")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.record_reconnect_required(upstream, reason).await?;
            if let Some(refreshed) = UpstreamStore::get_by_id(self.storage.as_ref(), upstream.id)
                .await
                .map_err(storage_scheduler_error)?
            {
                *upstream = refreshed;
            }
            return Ok(false);
        }
        let Some(lazy_refresher) = self.lazy_refresher.as_ref() else {
            return Ok(false);
        };
        let generation = upstream.oauth_token_generation;
        let result = lazy_refresher.refresh_one(upstream.id).await;
        if let Some(refreshed) = UpstreamStore::get_by_id(self.storage.as_ref(), upstream.id)
            .await
            .map_err(storage_scheduler_error)?
        {
            *upstream = refreshed;
        }
        if refresh_requires_reconnect(upstream.last_apply_error.as_deref()) {
            return Ok(false);
        }
        if let Err(error) = result
            && upstream.oauth_token_generation == generation
        {
            return Err(SchedulerError::Job(error.to_string()));
        }
        Ok(true)
    }
}

enum FetchOutcome {
    Response(crate::scheduler_dispatch::http::UsageFetchResponse),
    Network,
}

fn bound_provider_identity(
    kind: UpstreamKind,
    bundle: &OAuthTokenBundle,
    metadata: Option<&cc_lb_storage_api::UpstreamSubscriptionMetadataRecord>,
    credential_generation: u64,
) -> OAuthAccountIdentity {
    let (identity, metadata_generation) = oauth_account_from_bootstrap(
        metadata.and_then(|metadata| metadata.raw_bootstrap.as_deref()),
    );
    let bound = match kind {
        UpstreamKind::OpenaiOauth => bundle
            .account_id
            .as_ref()
            .is_some_and(|account| identity.account_id.as_ref() == Some(account)),
        UpstreamKind::GeminiOauth => gemini_identity_is_bound(
            &identity,
            bundle.account_id.as_deref(),
            bundle.project_id.as_deref(),
            metadata_generation,
            credential_generation,
        ),
        _ => false,
    };
    if bound {
        identity
    } else {
        OAuthAccountIdentity::default()
    }
}

fn metadata_text(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn merge_openai_usage_identity(
    identity: &mut OAuthAccountIdentity,
    bundle: &OAuthTokenBundle,
    usage: &serde_json::Value,
) -> bool {
    let Some(account_id) = bundle.account_id.as_deref().filter(|id| !id.is_empty()) else {
        return false;
    };
    if usage
        .get("account_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        != Some(account_id)
    {
        return false;
    }
    let claims = openai_identity_from_token(&bundle.access_token);
    if claims.account_id.as_deref() == Some(account_id) {
        identity.account_name = identity.account_name.take().or(claims.account_name);
        identity.account_email = identity.account_email.take().or(claims.account_email);
        identity.plan = identity.plan.take().or(claims.plan);
    }
    identity.account_id = Some(account_id.to_owned());
    if let Some(email) = metadata_text(usage.get("email")) {
        identity.account_email = Some(email);
    }
    if let Some(plan) = metadata_text(usage.get("plan_type")) {
        identity.plan = Some(plan);
    }
    identity.plan_name = None;
    true
}

fn merge_gemini_account_metadata(
    identity: &mut OAuthAccountIdentity,
    bundle: &OAuthTokenBundle,
    load: Option<&serde_json::Value>,
    userinfo: Option<&serde_json::Value>,
) -> bool {
    let email = bundle
        .account_id
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty());
    let Some(project) = bundle
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|project| !project.is_empty())
    else {
        return false;
    };
    let update = gemini_identity_from_responses(
        load.unwrap_or(&serde_json::Value::Null),
        userinfo.unwrap_or(&serde_json::Value::Null),
    );
    if update
        .account_email
        .as_deref()
        .zip(email)
        .is_some_and(|(provided, expected)| !provided.eq_ignore_ascii_case(expected))
        || update
            .project_id
            .as_deref()
            .is_some_and(|provided| provided != project)
    {
        return false;
    }
    let project_matches = update.project_id.as_deref() == Some(project);
    let requested_cli_project = bundle.oauth_profile == GeminiOAuthProfile::GeminiCli
        && update.project_id.is_none()
        && load.is_some_and(serde_json::Value::is_object);
    if email.is_none() && !project_matches && !requested_cli_project {
        return false;
    }
    let mut changed = false;
    if update.account_email.is_some() {
        identity.account_email = update.account_email;
        if let Some(name) = update.account_name {
            identity.account_name = Some(name);
        }
        changed = true;
    }
    // CLI requests explicitly carry the stored project. Code Assist may omit
    // that project in a standard-tier response, as Gemini CLI setup.ts allows.
    if (project_matches || requested_cli_project)
        && let Some(plan) = update.plan
    {
        if identity.plan.as_ref() != Some(&plan) || update.plan_name.is_some() {
            identity.plan_name = update.plan_name;
        }
        identity.plan = Some(plan);
        changed = true;
    }
    if changed {
        identity.account_email = identity
            .account_email
            .take()
            .or_else(|| email.map(str::to_owned));
        identity.project_id = Some(project.to_owned());
    }
    changed
}

fn should_poll_oauth_usage(upstream: &UpstreamRecord) -> bool {
    matches!(
        upstream.kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
        && !refresh_requires_reconnect(upstream.last_apply_error.as_deref())
}

#[cfg(test)]
mod tests;
