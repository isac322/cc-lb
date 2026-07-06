use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_plugin_api::{
    ApiKeyAwareSignerFactory, DialectError, Principal, PrincipalKind, RequestContext,
    ShapedRequest, ShapedRequestBuilder, SignedRequest, Upstream, UpstreamDialect, shape_request,
    sign_request,
};
use cc_lb_storage_api::UpstreamStore;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use http::header::AUTHORIZATION;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use serde::Deserialize;
use url::Url;

use crate::lifecycle::UpstreamDispatch;

use super::request_snapshot::RequestSnapshot;
use super::scheduler::{DispatchOutcome, KeepaliveDispatcher};

const DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);
const ERROR_BODY_PREFIX_BYTES: usize = 512;

pub struct AnthropicKeepaliveDispatcher {
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    upstream_store: Arc<dyn UpstreamStore>,
    http_client: Arc<dyn UpstreamDispatch>,
    timeout: Duration,
}

impl AnthropicKeepaliveDispatcher {
    pub fn new(
        signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
        upstream_store: Arc<dyn UpstreamStore>,
        http_client: Arc<dyn UpstreamDispatch>,
    ) -> Self {
        Self {
            signer_factory,
            upstream_store,
            http_client,
            timeout: DISPATCH_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn signed_keepalive_request(
        &self,
        snapshot: &RequestSnapshot,
        upstream: &UpstreamRecord,
    ) -> Result<SignedRequest, String> {
        let upstream_api = direct_anthropic_upstream(upstream)?;
        let signer_factory = self
            .signer_factory
            .with_router_choice(downstream_api_key(&snapshot.headers), upstream.name.clone());
        let signer = signer_factory
            .build(&upstream_api)
            .await
            .map_err(|source| source.to_string())?;
        let keepalive_body = snapshot
            .build_keepalive_body()
            .map_err(|source| source.to_string())?;
        let shaped = shaped_request_from_snapshot(snapshot, keepalive_body, &upstream_api)?;
        sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|source| source.to_string())
    }

    async fn dispatch_signed(&self, signed: SignedRequest) -> Result<(StatusCode, Bytes), String> {
        let response = self
            .http_client
            .dispatch(signed)
            .await
            .map_err(|source| source.to_string())?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|source| source.to_string())?
            .to_bytes();
        Ok((status, body))
    }
}

#[async_trait]
impl KeepaliveDispatcher for AnthropicKeepaliveDispatcher {
    async fn dispatch(&self, snapshot: &RequestSnapshot) -> DispatchOutcome {
        let upstream = match self.upstream_store.get_by_id(snapshot.upstream_id).await {
            Ok(Some(upstream)) if upstream.deleted_at_unix_secs.is_none() && upstream.enabled => {
                upstream
            }
            Ok(Some(_)) | Ok(None) => return DispatchOutcome::Error("upstream gone".to_owned()),
            Err(source) => return DispatchOutcome::Error(source.to_string()),
        };

        let signed = match self.signed_keepalive_request(snapshot, &upstream).await {
            Ok(signed) => signed,
            Err(error) => return DispatchOutcome::Error(error),
        };

        match tokio::time::timeout(self.timeout, self.dispatch_signed(signed)).await {
            Ok(Ok((StatusCode::OK, body))) => classify_success_body(&body),
            Ok(Ok((status, body))) => {
                DispatchOutcome::Error(format!("status={} body={}", status, body_prefix(&body)))
            }
            Ok(Err(source)) => DispatchOutcome::Error(source),
            Err(source) => DispatchOutcome::Error(source.to_string()),
        }
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

fn shaped_request_from_snapshot(
    snapshot: &RequestSnapshot,
    body: Bytes,
    upstream: &Upstream,
) -> Result<ShapedRequest, String> {
    let dialect = SnapshotDialect {
        url: snapshot.url.clone(),
    };
    let ctx = RequestContext {
        request_id: format!("cache-keepalive-{}", snapshot.upstream_id),
        thread_id: None,
        downstream_headers: snapshot.headers.clone(),
        method: snapshot.method.clone(),
        path: snapshot.url.path().to_owned(),
        query: snapshot.url.query().map(ToOwned::to_owned),
        body_bytes: body,
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
    };
    let principal = Principal {
        id: snapshot.upstream_id.to_string(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    shape_request(&dialect, &ctx, upstream, &principal).map_err(|source| source.to_string())
}

struct SnapshotDialect {
    url: Url,
}

impl UpstreamDialect for SnapshotDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            self.url.clone(),
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }
}

fn downstream_api_key(headers: &HeaderMap) -> String {
    if let Some(value) = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    {
        return value.to_owned();
    }

    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, credential)| {
            scheme.eq_ignore_ascii_case("Bearer") && !credential.is_empty()
        })
        .map(|(_, credential)| credential.to_owned())
        .unwrap_or_default()
}

#[derive(Deserialize)]
struct KeepaliveResponseUsage {
    #[serde(default)]
    cache_read_input_tokens: u64,
}

#[derive(Deserialize)]
struct KeepaliveResponseBody {
    usage: Option<KeepaliveResponseUsage>,
}

fn classify_success_body(body: &[u8]) -> DispatchOutcome {
    match sonic_rs::from_slice::<KeepaliveResponseBody>(body) {
        Ok(parsed)
            if parsed
                .usage
                .as_ref()
                .is_some_and(|usage| usage.cache_read_input_tokens > 0) =>
        {
            DispatchOutcome::CacheHit
        }
        Ok(_) => DispatchOutcome::CacheMiss,
        Err(source) => DispatchOutcome::Error(source.to_string()),
    }
}

fn body_prefix(body: &[u8]) -> String {
    let prefix_len = body.len().min(ERROR_BODY_PREFIX_BYTES);
    String::from_utf8_lossy(&body[..prefix_len]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_successful_cache_read_as_hit() {
        let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":1234,"input_tokens":0,"output_tokens":0}}"#;

        let outcome = classify_success_body(body);

        assert!(matches!(outcome, DispatchOutcome::CacheHit));
    }

    #[test]
    fn classifies_zero_cache_read_as_miss() {
        let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":0,"input_tokens":0,"output_tokens":0}}"#;

        let outcome = classify_success_body(body);

        assert!(matches!(outcome, DispatchOutcome::CacheMiss));
    }

    #[test]
    fn classifies_missing_usage_as_miss() {
        let body = br#"{"content":[],"stop_reason":"max_tokens"}"#;

        let outcome = classify_success_body(body);

        assert!(matches!(outcome, DispatchOutcome::CacheMiss));
    }
}
