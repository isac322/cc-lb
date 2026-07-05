use cc_lb_engine::clock::unix_secs;
#[allow(deprecated)]
use cc_lb_engine::subscription_quota_events::unified_observation_to_record;
use cc_lb_engine::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::{
    UpstreamRecord, UpstreamStore, WarmupDispatchKind, WarmupPermanentFailureReason,
    WarmupTransientFailureReason,
};
use http::HeaderMap;

use crate::scheduler_dispatch::http::{decrypt_bundle, upstream_base_url};
use crate::scheduler_dispatch::storage::storage_scheduler_error;
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::warmup::request::WarmupRequestAttempt;
use crate::warmup::{WarmupAbandonReason, dispatch_warmup_attempt};

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
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        if bundle
            .expires_at_unix_secs
            .saturating_sub(TOKEN_REFRESH_LOOKAHEAD_SECS)
            > unix_secs(self.clock.now())
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
    ) -> WarmupDispatchAttempt {
        if upstream.warmup_dialect_plugin.is_some()
            && let Some(lazy_refresher) = self.lazy_refresher.as_ref()
        {
            let outcome = match crate::warmup::dialect::dispatch_warmup_with_dialect(
                crate::warmup::dialect::WarmupDialectDispatchParams {
                    runtime: self.runtime.as_ref(),
                    stores: self.stores.as_ref(),
                    data_dir: self.data_dir.as_ref(),
                    aead: self.aead.clone(),
                    lazy_refresher: lazy_refresher.clone(),
                    upstream,
                    http: &self.http,
                    clock: self.clock.clone(),
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
                    tracing::warn!(error = %error, reason = WarmupAbandonReason::DialectPlugin.as_str(), "warmup dialect failed permanently");
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
                    observations: parse_headers(&outcome.headers),
                },
            };
        }
        let bundle = match decrypt_bundle(upstream, self.aead.as_ref()) {
            Ok(bundle) => bundle,
            Err(error) => {
                return WarmupDispatchAttempt {
                    dispatch_kind: WarmupDispatchKind::NotDispatched,
                    result: WarmupDispatchResult::PermanentFailure {
                        reason: WarmupPermanentFailureReason::CredentialDecryptFailed,
                        error_detail: error.to_string(),
                    },
                };
            }
        };
        let base_url = match upstream_base_url(upstream) {
            Ok(base_url) => base_url,
            Err(error) => {
                return WarmupDispatchAttempt {
                    dispatch_kind: WarmupDispatchKind::NotDispatched,
                    result: WarmupDispatchResult::PermanentFailure {
                        reason: WarmupPermanentFailureReason::RequestBuildFailed,
                        error_detail: error.to_string(),
                    },
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
            } => WarmupDispatchAttempt {
                dispatch_kind: WarmupDispatchKind::Http,
                result: WarmupDispatchResult::Response {
                    status,
                    observations,
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

fn parse_headers(headers: &HeaderMap) -> Vec<UnifiedQuotaObservation> {
    parse_anthropic_unified_headers(headers)
}
