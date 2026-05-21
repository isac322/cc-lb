use axum::body::{to_bytes, Body};
use bytes::Bytes;
use cc_lb_plugin_api::{
    sign_request, Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder,
    SignerFactory, Upstream, UpstreamDialect,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use fake_anthropic::{app, AppConfig};
use http::{HeaderMap, Method, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

struct E2EDialect;

impl UpstreamDialect for E2EDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        Ok(builder.shaped_request(
            "http://127.0.0.1/v1/messages".parse().expect("url"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#),
        ))
    }

    fn normalize_error(&self, _status: http::StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

#[tokio::test]
async fn fake_anthropic_accepts_signed_request() {
    let ctx = RequestContext {
        request_id: "req-e2e".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let shaped =
        cc_lb_plugin_api::shape_request(&E2EDialect, &ctx, &Upstream::AnthropicDirect, &principal)
            .expect("shape request");

    let signer = AnthropicKeySignerFactory::new("sk-ant-test-key")
        .build(&Upstream::AnthropicDirect)
        .await
        .expect("signer");
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("signed request");

    let app = app(AppConfig::default());
    let (url, method, headers, body) = signed.into_parts();
    let mut builder = Request::builder().method(method).uri(url.path());
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }

    let response = app
        .oneshot(builder.body(Body::from(body)).expect("request builds"))
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(value["type"], "message");
}
