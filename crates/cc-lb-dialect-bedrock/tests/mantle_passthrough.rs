mod common;

use bytes::Bytes;
use cc_lb_dialect_bedrock::BedrockMantleDialect;
use cc_lb_plugin_api::{shape_request, Upstream, UpstreamDialect};
use http::StatusCode;

#[test]
fn mantle_preserves_anthropic_shape_body_and_headers() {
    let body = Bytes::from_static(
        br#"{"model":"claude-3-5-sonnet-20241022","messages":[],"stream":true}"#,
    );
    let headers = common::anthropic_headers();
    let ctx = common::request_context(body.clone(), headers.clone());

    let shaped = shape_request(
        &BedrockMantleDialect,
        &ctx,
        &Upstream::BedrockMantle {
            region: "us-east-1".to_owned(),
        },
        &common::principal(),
    )
    .expect("mantle shape succeeds");

    println!(
        "MantlePassthrough bytes_in={} bytes_out={} url={}",
        body.len(),
        shaped.body().len(),
        shaped.url()
    );
    assert_eq!(shaped.body(), &body);
    assert_eq!(shaped.headers(), &headers);
    assert_eq!(
        shaped.url().as_str(),
        "https://bedrock-mantle.us-east-1.api.aws/anthropic/v1/messages"
    );
    assert!(BedrockMantleDialect
        .normalize_error(StatusCode::BAD_REQUEST, &Bytes::from_static(b"{}"))
        .is_none());
}
