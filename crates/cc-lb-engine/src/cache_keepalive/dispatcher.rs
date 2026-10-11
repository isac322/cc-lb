use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_control::dynamic_view::DynamicViewHolder;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_storage_api::UpstreamStore;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, Signer,
    UpstreamDialect, shape_request,
};
use http::StatusCode;
use http_body_util::BodyExt;
use serde::Deserialize;
use url::Url;

use crate::attempt_rail::{AttemptIntent, ResponseAccountingGuard, Signed};
use crate::lifecycle::UpstreamDispatch;

use super::request_snapshot::RequestSnapshot;
use super::scheduler::{
    DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher, RenewalFinalization,
    RenewalUsage,
};

const DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);
const ERROR_BODY_PREFIX_BYTES: usize = 512;

pub struct AnthropicKeepaliveDispatcher {
    dynamic_view: Arc<DynamicViewHolder>,
    upstream_store: Arc<dyn UpstreamStore>,
    http_client: Arc<dyn UpstreamDispatch>,
    timeout: Duration,
    activity: Option<Arc<crate::upstream_activity::UpstreamActivity>>,
}

impl AnthropicKeepaliveDispatcher {
    pub fn new(
        dynamic_view: Arc<DynamicViewHolder>,
        upstream_store: Arc<dyn UpstreamStore>,
        http_client: Arc<dyn UpstreamDispatch>,
    ) -> Self {
        Self {
            dynamic_view,
            upstream_store,
            http_client,
            timeout: DISPATCH_TIMEOUT,
            activity: None,
        }
    }

    pub fn with_upstream_activity(
        mut self,
        activity: Arc<crate::upstream_activity::UpstreamActivity>,
    ) -> Self {
        self.activity = Some(activity);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn keepalive_signing_input(
        &self,
        snapshot: &RequestSnapshot,
        upstream: &UpstreamRecord,
        source_ref_id: &str,
    ) -> Result<(Arc<dyn Signer>, ShapedRequest), String> {
        let upstream_api = direct_anthropic_upstream(upstream)?;
        let signer_factory = self
            .dynamic_view
            .load()
            .signer_factory
            .with_router_choice(upstream.name.clone());
        let signer = signer_factory
            .build(&upstream_api)
            .await
            .map_err(|source| source.to_string())?;
        let keepalive_body = snapshot
            .build_keepalive_body()
            .map_err(|source| source.to_string())?;
        let target_url = resolve_keepalive_url(snapshot, upstream)?;
        let shaped = shaped_request_from_snapshot(
            snapshot,
            keepalive_body,
            &upstream_api,
            target_url,
            source_ref_id,
        )?;
        Ok((signer, shaped))
    }

    async fn dispatch_signed(
        &self,
        signed: Signed<'_>,
    ) -> Result<(StatusCode, Bytes, Duration), String> {
        let mut response = signed
            .dispatch(self.http_client.as_ref())
            .await
            .map_err(|source| source.to_string())?;
        let _activity = response
            .extensions_mut()
            .remove::<Arc<crate::upstream_activity::ActivityGuard>>();
        let cache_anchor_at = Instant::now();
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|source| source.to_string())?
            .to_bytes();
        Ok((status, body, cache_anchor_at.elapsed()))
    }
}

#[async_trait]
impl KeepaliveDispatcher for AnthropicKeepaliveDispatcher {
    async fn dispatch(
        &self,
        snapshot: &RequestSnapshot,
        context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        let upstream = match self.upstream_store.get_by_id(snapshot.upstream_id).await {
            Ok(Some(upstream)) if upstream.deleted_at_unix_secs.is_none() && upstream.enabled => {
                upstream
            }
            Ok(Some(_)) | Ok(None) => return DispatchOutcome::Error("upstream gone".to_owned()),
            Err(source) => return DispatchOutcome::Error(source.to_string()),
        };
        if let Err(error) = keepalive_upstream_supported(&upstream) {
            return DispatchOutcome::UnsupportedProvider(error);
        }

        let parsed: Option<serde_json::Value> = sonic_rs::from_slice(&snapshot.body).ok();
        let metadata =
            crate::lifecycle::request_cache_metadata_from_value(&snapshot.headers, parsed.as_ref());
        let model = parsed
            .as_ref()
            .and_then(|value| value.get("model"))
            .and_then(serde_json::Value::as_str)
            .map(crate::model_resolution::canonical_model_id)
            .unwrap_or_default();
        let owner = context
            .principal_id()
            .zip(metadata.observed_session_id.as_ref())
            .map(|(principal, session)| (principal.to_owned(), session.clone()));
        let ttl = match snapshot.ttl {
            cc_lb_storage_api::CacheTtl::Ttl5m => 300,
            cc_lb_storage_api::CacheTtl::Ttl1h => 3600,
        };
        let activity = if let Some(registry) = &self.activity {
            let view = self.dynamic_view.load();
            registry
                .initialize(
                    view.prompt_cache_observation_store_opt().map(AsRef::as_ref),
                    &[snapshot.upstream_id],
                    model,
                )
                .await;
            let Some(activity) = registry.claim(snapshot.upstream_id, model, owner, false, ttl)
            else {
                return DispatchOutcome::Error(
                    "upstream is protected by another conversation".to_owned(),
                );
            };
            Some(activity)
        } else {
            None
        };

        let (signer, shaped) = match self
            .keepalive_signing_input(snapshot, &upstream, context.source_ref_id())
            .await
        {
            Ok(input) => input,
            Err(error) => return DispatchOutcome::Error(error),
        };

        let dispatch_started = Instant::now();
        let (dispatch_result, accounting_guard) = match context.into_reservation() {
            Some(reservation) => {
                let reserved = AttemptIntent::from_reservation(reservation).into_reserved();
                let signed = match reserved
                    .begin_attempt()
                    .with_activity(activity.clone())
                    .sign(signer.as_ref(), shaped)
                    .await
                {
                    Ok(signed) => signed,
                    Err(error) => return DispatchOutcome::Error(error.to_string()),
                };
                (
                    tokio::time::timeout(self.timeout, self.dispatch_signed(signed)).await,
                    reserved.into_response_accounting_guard(),
                )
            }
            None => {
                let reserved = AttemptIntent::observe_only().into_reserved();
                let signed = match reserved
                    .begin_attempt()
                    .with_activity(activity.clone())
                    .sign(signer.as_ref(), shaped)
                    .await
                {
                    Ok(signed) => signed,
                    Err(error) => return DispatchOutcome::Error(error.to_string()),
                };
                (
                    tokio::time::timeout(self.timeout, self.dispatch_signed(signed)).await,
                    reserved.into_response_accounting_guard(),
                )
            }
        };

        if let (Some(activity), Ok(Ok((StatusCode::OK, body, cache_anchor_age)))) =
            (&activity, &dispatch_result)
            && let Ok(parsed) = sonic_rs::from_slice::<KeepaliveResponseBody>(body)
            && let Some(usage) = parsed.usage
            && (usage.cache_read_input_tokens > 0 || usage.cache_creation_input_tokens > 0)
            && let Some(registry) = &self.activity
        {
            activity.observe(
                registry
                    .now()
                    .saturating_sub(cache_anchor_age.as_secs())
                    .saturating_add(ttl),
            );
        }
        if let (Some(activity), Ok(Ok((status, _, _)))) = (&activity, &dispatch_result)
            && matches!(
                *status,
                StatusCode::UNAUTHORIZED | StatusCode::TOO_MANY_REQUESTS
            )
        {
            activity.reject();
        }

        match dispatch_result {
            Ok(Ok((StatusCode::OK, body, cache_anchor_age))) => classify_success_body(
                &body,
                cache_anchor_age,
                dispatch_started.elapsed(),
                accounting_guard,
            ),
            Ok(Ok((status, body, _))) => {
                DispatchOutcome::Error(format!("status={} body={}", status, body_prefix(&body)))
            }
            Ok(Err(source)) => DispatchOutcome::Error(source),
            Err(source) => DispatchOutcome::Error(source.to_string()),
        }
    }
}

fn keepalive_upstream_supported(upstream: &UpstreamRecord) -> Result<(), String> {
    match upstream.kind {
        UpstreamKind::AnthropicApiKey => Err(
            "AnthropicApiKey keep-alive is disabled until storage-backed signing is available; downstream keys cannot be replayed to Anthropic".to_owned(),
        ),
        UpstreamKind::AnthropicOauth => Ok(()),
    }
}

fn direct_anthropic_upstream(upstream: &UpstreamRecord) -> Result<Upstream, String> {
    match upstream.kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth => {
            Ok(Upstream::AnthropicDirect {
                base_url: upstream.base_url.clone(),
            })
        }
    }
}

fn resolve_keepalive_url(
    snapshot: &RequestSnapshot,
    upstream: &UpstreamRecord,
) -> Result<Url, String> {
    let Some(base_url) = upstream.base_url.as_ref() else {
        return Ok(snapshot.url.clone());
    };
    let mut resolved = base_url
        .join(snapshot.url.path())
        .map_err(|source| format!("resolve keep-alive url: {source}"))?;
    resolved.set_query(snapshot.url.query());
    Ok(resolved)
}

fn shaped_request_from_snapshot(
    snapshot: &RequestSnapshot,
    body: Bytes,
    upstream: &Upstream,
    target_url: Url,
    source_ref_id: &str,
) -> Result<ShapedRequest, String> {
    let dialect = SnapshotDialect {
        url: target_url.clone(),
    };
    let context = DialectShapeContext {
        request_id: source_ref_id.to_owned(),
        downstream_headers: snapshot.headers.clone(),
        method: snapshot.method.clone(),
        path: target_url.path().to_owned(),
        query: target_url.query().map(ToOwned::to_owned),
        body_bytes: body,
    };
    let principal = Principal {
        id: snapshot.upstream_id.to_string(),
        kind: PrincipalKind::ApiKey,
    };
    shape_request(&dialect, &context, upstream, &principal).map_err(|source| source.to_string())
}

struct SnapshotDialect {
    url: Url,
}

impl UpstreamDialect for SnapshotDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            self.url.clone(),
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

#[derive(Deserialize, Default)]
struct KeepaliveResponseUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens_5m: u64,
    #[serde(default)]
    cache_creation_input_tokens_1h: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
}

#[derive(Deserialize)]
struct KeepaliveResponseBody {
    usage: Option<KeepaliveResponseUsage>,
}

fn classify_success_body(
    body: &[u8],
    cache_anchor_age: Duration,
    duration: Duration,
    accounting_guard: ResponseAccountingGuard,
) -> DispatchOutcome {
    match sonic_rs::from_slice::<KeepaliveResponseBody>(body) {
        Ok(parsed) => {
            let usage = parsed.usage.unwrap_or_default();
            let finalization = RenewalFinalization {
                usage: RenewalUsage {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    cache_creation_input_tokens: usage.cache_creation_input_tokens,
                    cache_creation_input_tokens_5m: usage.cache_creation_input_tokens_5m,
                    cache_creation_input_tokens_1h: usage.cache_creation_input_tokens_1h,
                    cache_read_input_tokens: usage.cache_read_input_tokens,
                },
                status: StatusCode::OK.as_u16(),
                duration,
                accounting_guard,
            };
            if finalization.usage.cache_read_input_tokens > 0 {
                DispatchOutcome::CacheHit {
                    cache_anchor_age,
                    finalization,
                }
            } else {
                DispatchOutcome::CacheMiss { finalization }
            }
        }
        Err(source) => DispatchOutcome::Error(source.to_string()),
    }
}

fn body_prefix(body: &[u8]) -> String {
    let prefix_len = body.len().min(ERROR_BODY_PREFIX_BYTES);
    String::from_utf8_lossy(&body[..prefix_len]).into_owned()
}

#[cfg(test)]
mod tests;
