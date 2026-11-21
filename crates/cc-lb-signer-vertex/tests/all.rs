use bytes::Bytes;
use cc_lb_domain::{
    Principal, PrincipalKind, Upstream, VertexApiVersion, VertexTarget, VertexUpstreamConfig,
};
use cc_lb_signer_vertex::{VertexCredentialSource, VertexSigner};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
    shape_request, sign_request,
};
use http::Method;
use http::header::{AUTHORIZATION, HeaderMap, HeaderValue};

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer inherited"));
        headers.insert("x-api-key", HeaderValue::from_static("stale-key"));
        headers.insert(
            "x-goog-user-project",
            HeaderValue::from_static("stale-project"),
        );
        Ok(builder.shaped_request(
            "https://gemini.example/v1beta/models/gemini-2.5-flash:generateContent?alt=json"
                .parse()
                .expect("url"),
            Method::POST,
            headers,
            Bytes::from_static(br#"{"contents":[]}"#),
        ))
    }
}

#[tokio::test]
async fn qa_url_1_public_signer_routes_url_and_strips_client_credentials() {
    let context = DialectShapeContext {
        request_id: "request-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1beta/models/gemini-2.5-flash:generateContent".to_owned(),
        query: Some("alt=json".to_owned()),
        body_bytes: Bytes::from_static(b"{}"),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
    };
    let upstream = Upstream::GeminiDirect { base_url: None };
    let shaped =
        shape_request(&DirectDialect, &context, &upstream, &principal).expect("shape request");
    let signer = VertexSigner::new(
        "vertex-upstream",
        cc_lb_domain::UpstreamKind::VertexApiKey,
        VertexUpstreamConfig {
            target: VertexTarget::Regional {
                project_id: "demo-project".into(),
                location: "us-central1".into(),
            },
            api_version: VertexApiVersion::V1Beta1,
        },
        None,
        None,
        VertexCredentialSource::ApiKey(HeaderValue::from_static("vertex-key")),
    );

    let signed = sign_request(&signer, shaped).await.expect("sign request");
    assert_eq!(
        signed.url().as_str(),
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/demo-project/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent?alt=json"
    );
    assert_eq!(
        signed.headers().get("x-goog-api-key"),
        Some(&HeaderValue::from_static("vertex-key"))
    );
    assert!(!signed.headers().contains_key(AUTHORIZATION));
    assert!(!signed.headers().contains_key("x-api-key"));
    assert!(!signed.headers().contains_key("x-goog-user-project"));
}
