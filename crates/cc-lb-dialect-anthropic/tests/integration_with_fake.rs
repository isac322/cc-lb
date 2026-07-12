mod common;

use bytes::Bytes;
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_domain::Upstream;
use cc_lb_upstream::shape_request;
use http::Method;

#[test]
fn direct_shape_body_identity_for_relay_to_fake_anthropic() {
    let client_body_bytes = Bytes::from_static(
        br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":[{"type":"text","text":"byte identity"}]}],"stream":true}"#,
    );
    let ctx = common::request_context(
        Method::POST,
        "/v1/messages",
        None,
        client_body_bytes.clone(),
        common::anthropic_headers(),
    );

    let shaped = shape_request(
        &AnthropicDirectDialect::default(),
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
        &common::principal(),
    )
    .expect("direct shape should succeed");

    let upstream_body_bytes = shaped.body().clone();
    assert_eq!(client_body_bytes, upstream_body_bytes);
    println!(
        "direct_identity byte_equal=true client_body_len={} shaped_body_len={} note=network relay is owned by core dispatcher; this crate proves the bytes handed to fake-anthropic remain unchanged",
        client_body_bytes.len(),
        upstream_body_bytes.len()
    );
}
