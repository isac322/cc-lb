use axum::body::{Body, to_bytes};
use bytes::Bytes;
use cc_lb_plugin_api::{Principal, PrincipalKind, RequestContext, Upstream};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, SignerFactory,
    UpstreamDialect, shape_request, sign_request,
};
use fake_anthropic::{AppConfig, app};
use http::{HeaderMap, Method, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

struct E2EDialect;

impl UpstreamDialect for E2EDialect {
    fn shape(
        &self,
        _context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            "http://127.0.0.1/v1/messages".parse().expect("url"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#),
        ))
    }
}

#[tokio::test]
async fn fake_anthropic_accepts_signed_request() {
    let ctx = RequestContext {
        request_id: "req-e2e".to_owned(),
        thread_id: None,
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
        cache_pricing: cc_lb_domain::CachePricingSummary::default(),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let shaped = shape_request(
        &E2EDialect,
        &ctx.dialect_shape_context(),
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .expect("shape request");

    let signer = AnthropicKeySignerFactory::new("sk-ant-test-key")
        .build(&Upstream::AnthropicDirect { base_url: None })
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
