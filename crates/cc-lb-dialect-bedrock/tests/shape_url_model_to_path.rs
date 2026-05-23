mod common;

use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use cc_lb_plugin_api::{DialectError, Upstream, shape_request};

#[test]
fn model_moves_to_bedrock_runtime_path_and_colon_suffix_is_preserved() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());

    let shaped = shape_request(
        &BedrockRuntimeDialect::default(),
        &ctx,
        &Upstream::BedrockRuntime {
            region: "us-east-1".to_owned(),
        },
        &common::principal(),
    )
    .expect("runtime shape succeeds");

    assert_eq!(
        shaped.url().as_str(),
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3-5-sonnet-20241022-v2:0/invoke"
    );
    let body: serde_json::Value = serde_json::from_slice(shaped.body()).expect("json body");
    assert!(body.get("model").is_none());
}

#[test]
fn streaming_shape_uses_response_stream_endpoint() {
    let ctx = common::request_context(common::messages_body(true), common::anthropic_headers());

    let shaped = shape_request(
        &BedrockRuntimeDialect::default(),
        &ctx,
        &Upstream::BedrockRuntime {
            region: "us-west-2".to_owned(),
        },
        &common::principal(),
    )
    .expect("runtime shape succeeds");

    assert!(
        shaped.url().as_str().ends_with(
            "/model/anthropic.claude-3-5-sonnet-20241022-v2:0/invoke-with-response-stream"
        )
    );
    assert_eq!(
        shaped
            .headers()
            .get("accept")
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.amazon.eventstream")
    );
}

#[test]
fn model_must_be_present_and_string() {
    for body in [r#"{"messages":[]}"#, r#"{"model":7,"messages":[]}"#] {
        let ctx = common::request_context(
            bytes::Bytes::from(body.to_owned()),
            common::anthropic_headers(),
        );
        let err = shape_request(
            &BedrockRuntimeDialect::default(),
            &ctx,
            &Upstream::BedrockRuntime {
                region: "us-east-1".to_owned(),
            },
            &common::principal(),
        )
        .expect_err("invalid model must fail");

        assert!(matches!(err, DialectError::UnsupportedRequest { .. }));
        assert!(err.to_string().contains("model"));
    }
}
