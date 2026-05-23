mod common;

use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use cc_lb_plugin_api::{Upstream, shape_request};

#[test]
fn provider_version_and_auth_headers_are_stripped_before_sigv4() {
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

    assert!(shaped.headers().get("anthropic-version").is_none());
    assert!(shaped.headers().get("anthropic-beta").is_none());
    assert!(shaped.headers().get("x-api-key").is_none());
    assert!(shaped.headers().get("authorization").is_none());
    assert_eq!(
        shaped
            .headers()
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok()),
        Some("127.0.0.1")
    );
    assert_eq!(
        shaped
            .headers()
            .get("accept")
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
}
