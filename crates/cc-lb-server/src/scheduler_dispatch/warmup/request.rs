use cc_lb_clock::unix_secs;
use cc_lb_quota::unified_observation_to_sample;
use cc_lb_quota::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::{
    UpstreamKind, UpstreamRecord, UpstreamStore, WarmupDispatchKind, WarmupLane,
    WarmupPermanentFailureReason, WarmupTransientFailureReason,
};
use http::HeaderMap;

use crate::scheduler_dispatch::http::decrypt_bundle;
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::warmup::request::{
    ProviderWarmupDispatchParams, WarmupRequestAttempt, dispatch_provider_warmup_attempt,
};

use super::SchedulerDispatch;

const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;

pub(super) struct WarmupDispatchAttempt {
    pub(super) dispatch_kind: WarmupDispatchKind,
    pub(super) result: WarmupDispatchResult,
}

pub(super) enum WarmupDispatchResult {
    Response {
        status: http::StatusCode,
        observations: Vec<UnifiedQuotaObservation>,
        error_detail: Option<String>,
    },
    TransientFailure {
        reason: WarmupTransientFailureReason,
        error_detail: String,
    },
    PermanentFailure {
        reason: WarmupPermanentFailureReason,
        error_detail: String,
    },
}

impl SchedulerDispatch {
    pub(super) async fn ensure_fresh_oauth_token(
        &self,
        upstream: &mut UpstreamRecord,
    ) -> SchedulerResult<()> {
        let Some(lazy_refresher) = self.lazy_refresher.as_ref() else {
            return Ok(());
        };
        if !self.bundle_needs_refresh(upstream)? {
            return Ok(());
        }
        // Another lane of the same upstream may have refreshed the credential
        // since this job loaded it; recheck the stored copy before refreshing so
        // concurrent lanes do not issue a second token-endpoint request.
        if let Some(current) = UpstreamStore::get_by_id(self.storage.as_ref(), upstream.id)
            .await
            .map_err(storage_scheduler_error)?
        {
            *upstream = current;
            if !self.bundle_needs_refresh(upstream)? {
                return Ok(());
            }
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

    fn bundle_needs_refresh(&self, upstream: &UpstreamRecord) -> SchedulerResult<bool> {
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        Ok(bundle
            .expires_at_unix_secs
            .saturating_sub(TOKEN_REFRESH_LOOKAHEAD_SECS)
            <= unix_secs(self.clock.now()))
    }

    pub(super) async fn force_refresh_oauth_token(
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

    pub(super) async fn dispatch_warmup_request(
        &self,
        upstream: &UpstreamRecord,
        lane: WarmupLane,
    ) -> WarmupDispatchAttempt {
        if upstream.warmup_dialect_plugin.is_some() {
            let outcome = match crate::warmup::dialect::dispatch_warmup_with_dialect(
                crate::warmup::dialect::WarmupDialectDispatchParams {
                    runtime: self.runtime.as_ref(),
                    stores: self.stores.as_ref(),
                    data_dir: self.data_dir.as_ref(),
                    aead: self.aead.clone(),
                    lazy_refresher: self.lazy_refresher.clone(),
                    upstream,
                    lane,
                    http: &self.http,
                    clock: self.clock.clone(),
                    config: self.config.as_ref(),
                },
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) if error.is_transient() => {
                    return WarmupDispatchAttempt {
                        dispatch_kind: WarmupDispatchKind::DialectPlugin,
                        result: WarmupDispatchResult::TransientFailure {
                            reason: WarmupTransientFailureReason::DialectPluginTransient,
                            error_detail: error.to_string(),
                        },
                    };
                }
                Err(error) => {
                    tracing::warn!(error = %error, reason = "dialect_plugin_failed", "warmup dialect failed permanently");
                    return WarmupDispatchAttempt {
                        dispatch_kind: WarmupDispatchKind::DialectPlugin,
                        result: WarmupDispatchResult::PermanentFailure {
                            reason: WarmupPermanentFailureReason::DialectPluginFailed,
                            error_detail: error.to_string(),
                        },
                    };
                }
            };
            return WarmupDispatchAttempt {
                dispatch_kind: WarmupDispatchKind::DialectPlugin,
                result: WarmupDispatchResult::Response {
                    status: outcome.status,
                    observations: match upstream.kind {
                        UpstreamKind::OpenaiOauth => {
                            cc_lb_quota::parse_openai_codex_quota_headers(&outcome.headers)
                        }
                        UpstreamKind::GeminiOauth => Vec::new(),
                        _ => parse_headers(&outcome.headers),
                    },
                    error_detail: None,
                },
            };
        }
        match dispatch_provider_warmup_attempt(ProviderWarmupDispatchParams {
            http: &self.http,
            upstream,
            lane,
            aead: self.aead.as_ref(),
            config: self.config.as_ref(),
            clock: self.clock.clone(),
        })
        .await
        {
            WarmupRequestAttempt::Response {
                status,
                observations,
                error_detail,
                ..
            } => WarmupDispatchAttempt {
                dispatch_kind: WarmupDispatchKind::Http,
                result: WarmupDispatchResult::Response {
                    status,
                    observations,
                    error_detail,
                },
            },
            WarmupRequestAttempt::CredentialDecryptFailed { error } => WarmupDispatchAttempt {
                dispatch_kind: WarmupDispatchKind::NotDispatched,
                result: WarmupDispatchResult::PermanentFailure {
                    reason: WarmupPermanentFailureReason::CredentialDecryptFailed,
                    error_detail: error,
                },
            },
            WarmupRequestAttempt::RequestBuildFailed { error } => WarmupDispatchAttempt {
                dispatch_kind: WarmupDispatchKind::NotDispatched,
                result: WarmupDispatchResult::PermanentFailure {
                    reason: WarmupPermanentFailureReason::RequestBuildFailed,
                    error_detail: error,
                },
            },
            WarmupRequestAttempt::NetworkError { error } => WarmupDispatchAttempt {
                dispatch_kind: WarmupDispatchKind::Http,
                result: WarmupDispatchResult::TransientFailure {
                    reason: WarmupTransientFailureReason::NetworkError,
                    error_detail: error,
                },
            },
        }
    }

    pub(super) fn record_warmup_observations(
        &self,
        upstream_id: uuid::Uuid,
        observations: Vec<UnifiedQuotaObservation>,
    ) -> SchedulerResult<()> {
        let observed_at_unix_millis = now_unix_millis(&*self.clock);
        for observation in observations {
            let record =
                unified_observation_to_sample(upstream_id, observation, observed_at_unix_millis);
            self.subscription_quota_cache
                .upsert_observation(upstream_id, &record);
            self.subscription_quota_sink
                .enqueue(record)
                .map_err(|error| SchedulerError::Job(error.to_string()))?;
        }
        Ok(())
    }
}

fn parse_headers(headers: &HeaderMap) -> Vec<UnifiedQuotaObservation> {
    parse_anthropic_unified_headers(headers)
}
