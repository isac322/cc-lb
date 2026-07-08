use bytes::Bytes;
use cc_lb_plugin_api::{
    Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder, SignerFactory,
    Upstream, UpstreamDialect, sign_request,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use http::{HeaderMap, Method};

struct BodyDialect;

impl UpstreamDialect for BodyDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        Ok(builder.shaped_request(
            "https://api.anthropic.com/v1/messages"
                .parse()
                .expect("url"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(b"\x00\x01binary body\xff"),
        ))
    }
}

#[tokio::test]
async fn sign_does_not_modify_body() {
    let ctx = RequestContext {
        request_id: "req-2".to_owned(),
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
        &BodyDialect,
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

    assert_eq!(
        signed.body(),
        &Bytes::from_static(b"\x00\x01binary body\xff")
    );
}
