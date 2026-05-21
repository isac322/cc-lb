mod common;

use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::shape_request;

#[test]
fn vertex_anthropic_version_is_forced_in_body() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());

    let shaped = shape_request(
        &VertexDialect::default(),
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");
    let body: serde_json::Value = serde_json::from_slice(shaped.body()).expect("json body");

    assert_eq!(body["anthropic_version"], "vertex-2023-10-16");
}

#[test]
fn body_anthropic_beta_is_preserved() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());

    let shaped = shape_request(
        &VertexDialect::default(),
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");
    let body: serde_json::Value = serde_json::from_slice(shaped.body()).expect("json body");

    assert_eq!(
        body["anthropic_beta"],
        serde_json::json!(["fine-grained-tool-streaming-2025-05-14"])
    );
}
