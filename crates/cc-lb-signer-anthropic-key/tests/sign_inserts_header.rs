use bytes::Bytes;
use cc_lb_plugin_api::{
    Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder, SignerFactory,
    Upstream, UpstreamDialect, sign_request,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use http::header::{AUTHORIZATION, USER_AGENT};
use http::{HeaderMap, HeaderValue, Method};

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("test-agent"));
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer original"));
        Ok(builder.shaped_request(
            "https://api.anthropic.com/v1/messages"
                .parse()
                .expect("url"),
            Method::POST,
            headers,
            Bytes::from_static(br#"{"model":"claude-3-5-sonnet-20241022"}"#),
        ))
    }
}

#[tokio::test]
async fn sign_inserts_header() {
    let ctx = RequestContext {
        request_id: "req-1".to_owned(),
        thread_id: None,
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
        cache_pricing: cc_lb_plugin_api::CachePricingSummary::default(),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let shaped = cc_lb_plugin_api::shape_request(
        &DirectDialect,
        &ctx,
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

    assert_eq!(signed.method(), &Method::POST);
    assert_eq!(
        signed.url().as_str(),
        "https://api.anthropic.com/v1/messages"
    );
    assert_eq!(
        signed.body(),
        &Bytes::from_static(br#"{"model":"claude-3-5-sonnet-20241022"}"#)
    );
    assert_eq!(
        signed
            .headers()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok()),
        Some("sk-ant-test-key")
    );
    assert_eq!(
        signed
            .headers()
            .get(USER_AGENT)
            .and_then(|value| value.to_str().ok()),
        Some("test-agent")
    );
    assert_eq!(
        signed
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer original")
    );
}
