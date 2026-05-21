#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use cc_lb_plugin_api::{
    shape_request, Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder,
    Upstream, UpstreamDialect,
};
use cc_lb_signer_gcp::{GcpOAuthSigner, GcpToken, StaticGcpTokenProvider};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method};

pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

pub fn scopes() -> Vec<String> {
    vec![CLOUD_PLATFORM_SCOPE.to_owned()]
}

pub fn token(value: &str, ttl_secs: u64) -> GcpToken {
    GcpToken::new(
        value,
        SystemTime::now() + Duration::from_secs(ttl_secs),
        scopes(),
    )
}

pub fn signer(provider: Arc<StaticGcpTokenProvider>) -> GcpOAuthSigner {
    GcpOAuthSigner::with_scopes(scopes(), provider)
}

pub fn shaped_request() -> ShapedRequest {
    shaped_request_with_headers(HeaderMap::new())
}

pub fn shaped_request_with_headers(headers: HeaderMap) -> ShapedRequest {
    let ctx = request_context(Bytes::from_static(b"{}"), HeaderMap::new());
    shape_request(
        &TestDialect { headers },
        &ctx,
        &Upstream::Vertex {
            project: "p".to_owned(),
            region: "us-central1".to_owned(),
        },
        &principal(),
    )
    .expect("shape request")
}

pub fn request_context(body_bytes: Bytes, downstream_headers: HeaderMap) -> RequestContext {
    RequestContext {
        request_id: "req-gcp-test".to_owned(),
        downstream_headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes,
    }
}

pub fn principal() -> Principal {
    Principal {
        id: "principal-gcp".to_owned(),
        kind: PrincipalKind::WorkloadIdentity,
        claims: serde_json::Map::new(),
    }
}

pub fn anthropic_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers
}

pub fn messages_body(stream: bool) -> Bytes {
    Bytes::from(format!(
        r#"{{"model":"claude-3-5-sonnet@20240620","max_tokens":64,"messages":[],"stream":{stream}}}"#
    ))
}

struct TestDialect {
    headers: HeaderMap,
}

impl UpstreamDialect for TestDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        Ok(builder.shaped_request(
            "https://us-central1-aiplatform.googleapis.com/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude-3-5-sonnet@20240620:rawPredict"
                .parse()
                .expect("url"),
            Method::POST,
            self.headers.clone(),
            Bytes::from_static(br#"{"anthropic_version":"vertex-2023-10-16"}"#),
        ))
    }

    fn normalize_error(&self, _status: http::StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}
