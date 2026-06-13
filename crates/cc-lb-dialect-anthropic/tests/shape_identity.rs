mod common;

use bytes::Bytes;
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_plugin_api::{Upstream, shape_request};
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
        &AnthropicDirectDialect::default(),
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
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
