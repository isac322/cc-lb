mod common;

use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use cc_lb_plugin_api::{Upstream, shape_request};

#[test]
fn anthropic_beta_header_becomes_ordered_body_array() {
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
    let body: serde_json::Value = serde_json::from_slice(shaped.body()).expect("json body");

    assert_eq!(
        body["anthropic_beta"],
        serde_json::json!(["oauth-2025-04-20", "files-api-2025-04-14"])
    );
    assert!(shaped.headers().get("anthropic-beta").is_none());
}
