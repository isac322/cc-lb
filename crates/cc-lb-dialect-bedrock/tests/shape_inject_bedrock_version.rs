mod common;

use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use cc_lb_plugin_api::{shape_request, Upstream};

#[test]
fn bedrock_anthropic_version_is_forced_in_body() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());

    let shaped = shape_request(
        &BedrockRuntimeDialect,
        &ctx,
        &Upstream::BedrockRuntime {
            region: "us-east-1".to_owned(),
        },
        &common::principal(),
    )
    .expect("runtime shape succeeds");
    let body: serde_json::Value = serde_json::from_slice(shaped.body()).expect("json body");

    assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
}
