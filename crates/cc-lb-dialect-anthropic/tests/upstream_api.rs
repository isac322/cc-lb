use bytes::Bytes;
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_upstream::{DialectShapeContext, shape_request};
use http::{HeaderMap, HeaderValue, Method};

#[test]
fn direct_shape_matches_proxy_observable_with_narrow_context() {
    // Given
    let body = Bytes::from_static(
        br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hello"}]}"#,
    );
    let mut downstream_headers = HeaderMap::new();
    downstream_headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    downstream_headers.insert("content-type", HeaderValue::from_static("application/json"));
    downstream_headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    let context = DialectShapeContext {
        request_id: "req-direct-observable".to_owned(),
        downstream_headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: Some("stream=true&trace=abc".to_owned()),
        body_bytes: body.clone(),
    };
    let principal = Principal {
        id: "principal-direct".to_owned(),
        kind: PrincipalKind::ApiKey,
    };

    // When
    let shaped = shape_request(
        &AnthropicDirectDialect::default(),
        &context,
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .expect("direct shape should succeed");

    // Then
    let observable = format!(
        "url={} method={} anthropic-version={} content-type={} x-api-key={} body={}",
        shaped.url(),
        shaped.method(),
        shaped.headers()["anthropic-version"]
            .to_str()
            .expect("utf8"),
        shaped.headers()["content-type"].to_str().expect("utf8"),
        shaped.headers()["x-api-key"].to_str().expect("utf8"),
        String::from_utf8_lossy(shaped.body()),
    );
    println!("{observable}");
    assert_eq!(
        observable,
        "url=https://api.anthropic.com/v1/messages?stream=true&trace=abc method=POST anthropic-version=2023-06-01 content-type=application/json x-api-key=sk-ant-test body={\"model\":\"claude-3-5-sonnet-20241022\",\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}]}"
    );
}
