mod common;

use bytes::Bytes;
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_plugin_api::{shape_request, Upstream};
use http::Method;

#[test]
fn direct_shape_preserves_method_headers_and_body_bytes() {
    let body = Bytes::from_static(
        br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hello"}]}"#,
    );
    let ctx = common::request_context(
        Method::POST,
        "/v1/messages",
        Some("stream=true&trace=abc"),
        body.clone(),
        common::anthropic_headers(),
    );

    let shaped = shape_request(
        &AnthropicDirectDialect,
        &ctx,
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect("direct shape should succeed");

    assert_eq!(
        shaped.url().as_str(),
        "https://api.anthropic.com/v1/messages?stream=true&trace=abc"
    );
    assert_eq!(shaped.method(), &Method::POST);
    assert_eq!(shaped.headers(), &ctx.downstream_headers);
    assert_eq!(shaped.body(), &body);
}

#[test]
fn direct_shape_rejects_wrong_upstream_variant() {
    let ctx = common::request_context(
        Method::POST,
        "/v1/messages",
        None,
        Bytes::from_static(b"{}"),
        common::anthropic_headers(),
    );
    let upstream = Upstream::CustomAnthropicSpec {
        base_url: "https://gw.example/api".parse().expect("valid test url"),
    };

    let err = shape_request(
        &AnthropicDirectDialect,
        &ctx,
        &upstream,
        &common::principal(),
    )
    .expect_err("wrong upstream must be rejected");

    assert!(err.to_string().contains("AnthropicDirect"));
}
