use bytes::Bytes;
use cc_lb_plugin_api::{
    sign_request, Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder,
    SignerFactory, Upstream, UpstreamDialect,
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

    fn normalize_error(&self, _status: http::StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

#[tokio::test]
async fn sign_does_not_modify_body() {
    let ctx = RequestContext {
        request_id: "req-2".to_owned(),
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
        cc_lb_plugin_api::shape_request(&BodyDialect, &ctx, &Upstream::AnthropicDirect, &principal)
            .expect("shape request");

    let signer = AnthropicKeySignerFactory::new("sk-ant-test-key")
        .build(&Upstream::AnthropicDirect)
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
