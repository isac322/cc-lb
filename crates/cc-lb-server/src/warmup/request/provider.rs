use std::sync::Arc;

use bytes::Bytes;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_clock::ClockHandle;
use cc_lb_config::Config;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_quota::{UnifiedQuotaObservation, parse_openai_codex_quota_headers};
use cc_lb_storage_api::{UpstreamKind, UpstreamRecord, WarmupLane};
use cc_lb_upstream::{DialectShapeContext, Signer, UpstreamDialect, shape_request, sign_request};
use http::{HeaderMap, HeaderValue, Method, Request};
use http_body_util::Full;
use serde_json::json;
use uuid::Uuid;

use crate::dynamic_view_builder::build_subscription_signer;

use super::{
    WarmupHttpClient, WarmupRequestAttempt, dispatch_anthropic_warmup_attempt,
    dispatch_http_request,
};

pub struct ProviderWarmupDispatchParams<'a> {
    pub http: &'a WarmupHttpClient,
    pub upstream: &'a UpstreamRecord,
    pub lane: WarmupLane,
    pub aead: &'a AeadService,
    pub config: &'a Config,
    pub clock: ClockHandle,
}

/// Sends the smallest native inference request through the proxy's own shaper and signer.
pub async fn dispatch_provider_warmup_attempt(
    params: ProviderWarmupDispatchParams<'_>,
) -> WarmupRequestAttempt {
    match build_and_dispatch(params).await {
        Ok(attempt) => attempt,
        Err(error) => WarmupRequestAttempt::RequestBuildFailed { error },
    }
}

async fn build_and_dispatch(
    params: ProviderWarmupDispatchParams<'_>,
) -> Result<WarmupRequestAttempt, String> {
    let upstream = params.upstream;
    if !WarmupLane::lanes_for_kind(upstream.kind).contains(&params.lane) {
        return Err(unsupported_lane_error(upstream.kind, params.lane));
    }
    let bundle = match upstream.oauth_credentials.as_ref() {
        Some(credentials) => match credentials.decrypt(params.aead, upstream.id.as_bytes()) {
            Ok(bundle) => bundle,
            Err(_) => {
                return Ok(WarmupRequestAttempt::CredentialDecryptFailed {
                    error: "oauth decrypt failed".to_owned(),
                });
            }
        },
        None => {
            return Ok(WarmupRequestAttempt::CredentialDecryptFailed {
                error: "missing oauth credentials".to_owned(),
            });
        }
    };
    if upstream.kind == UpstreamKind::AnthropicOauth {
        let base_url = upstream.base_url.clone().unwrap_or_else(|| {
            url::Url::parse("https://api.anthropic.com/").expect("valid Anthropic URL")
        });
        return Ok(
            dispatch_anthropic_warmup_attempt(params.http, &bundle.access_token, &base_url).await,
        );
    }
    let dialect = cc_lb_engine::lifecycle::native_route_dialect(upstream);
    let (request, signer) =
        build_provider_warmup_request(&params, &bundle, dialect.as_ref()).await?;
    let parse_observations: fn(&HeaderMap) -> Vec<UnifiedQuotaObservation> = match upstream.kind {
        UpstreamKind::OpenaiOauth => parse_openai_codex_quota_headers,
        _ => |_| Vec::new(),
    };
    Ok(dispatch_http_request(
        params.http,
        request,
        parse_observations,
        Some(signer.as_ref()),
    )
    .await)
}

/// The selected dialect sees the native provider body before OAuth signing/wrapping.
pub(crate) async fn build_provider_warmup_request(
    params: &ProviderWarmupDispatchParams<'_>,
    bundle: &OAuthTokenBundle,
    dialect: &dyn UpstreamDialect,
) -> Result<(Request<Full<Bytes>>, Arc<dyn Signer>), String> {
    let upstream = params.upstream;
    let (api, path, body) = match (upstream.kind, params.lane) {
        (UpstreamKind::OpenaiOauth, WarmupLane::Primary) => (
            Upstream::OpenaiDirect {
                base_url: upstream.base_url.clone(),
            },
            "/v1/responses".to_owned(),
            json!({
                "model": params.config.warmup.openai_model,
                "stream": true,
                "store": false,
                "instructions": "Reply with a single period.",
                "input": [{"role": "user", "content": "."}],
            }),
        ),
        (UpstreamKind::GeminiOauth, lane) => (
            Upstream::GeminiDirect {
                base_url: upstream.base_url.clone(),
            },
            format!(
                "/v1beta/models/{}:generateContent",
                match lane {
                    WarmupLane::Primary => &params.config.warmup.gemini_model,
                    WarmupLane::ThirdParty => &params.config.warmup.gemini_third_party_model,
                }
            ),
            json!({
                "contents": [{"role": "user", "parts": [{"text": "."}]}],
                "generationConfig": {"maxOutputTokens": 1},
            }),
        ),
        (UpstreamKind::OpenaiOauth, lane) => {
            return Err(unsupported_lane_error(upstream.kind, lane));
        }
        _ => return Err("provider warmup requires OpenAI or Gemini OAuth".to_owned()),
    };
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    if upstream.kind == UpstreamKind::OpenaiOauth {
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
    }
    let context = DialectShapeContext {
        request_id: Uuid::new_v4().to_string(),
        downstream_headers: headers,
        method: Method::POST,
        path,
        query: None,
        body_bytes: Bytes::from(serde_json::to_vec(&body).map_err(|error| error.to_string())?),
    };
    let principal = Principal {
        id: upstream.id.to_string(),
        kind: PrincipalKind::OAuthSubject,
    };
    let shaped =
        shape_request(dialect, &context, &api, &principal).map_err(|error| error.to_string())?;
    let signer = build_subscription_signer(
        upstream,
        bundle,
        params.clock.clone(),
        params.config.oauth.antigravity.as_ref(),
    )
    .map_err(|error| error.to_string())?;
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .map_err(|error| signer.scrub_secrets(&error.to_string()).into_owned())?;
    let mut builder = Request::builder()
        .method(signed.method().clone())
        .uri(signed.url().as_str());
    for (name, value) in signed.headers() {
        builder = builder.header(name, value);
    }
    let request = builder
        .body(Full::new(signed.body().clone()))
        .map_err(|error| error.to_string())?;
    Ok((request, signer))
}

fn unsupported_lane_error(kind: UpstreamKind, lane: WarmupLane) -> String {
    format!(
        "warmup lane {} is not available for {} upstreams",
        lane.as_str(),
        kind.as_str()
    )
}

#[cfg(test)]
mod tests;
