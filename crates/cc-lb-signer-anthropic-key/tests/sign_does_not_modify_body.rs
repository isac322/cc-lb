use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, SignerFactory,
    UpstreamDialect, shape_request, sign_request,
};
use http::{HeaderMap, Method};

struct BodyDialect;

impl UpstreamDialect for BodyDialect {
    fn shape(
        &self,
        _context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
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
    let ctx = DialectShapeContext {
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
    };
    let shaped = shape_request(
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
