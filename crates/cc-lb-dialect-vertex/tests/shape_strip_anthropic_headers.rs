mod common;

use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::shape_request;

#[test]
fn anthropic_auth_and_version_headers_are_stripped() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());

    let shaped = shape_request(
        &VertexDialect,
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");
    let headers = shaped.headers();

    assert!(headers.get("anthropic-version").is_none());
    assert!(headers.get("anthropic-beta").is_none());
    assert!(headers.get("x-api-key").is_none());
    assert!(headers.get("authorization").is_none());
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        headers
            .get("user-agent")
            .and_then(|value| value.to_str().ok()),
        Some("claude-cli/2.1.75")
    );
    assert_eq!(
        headers
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok()),
        Some("127.0.0.1")
    );
}
