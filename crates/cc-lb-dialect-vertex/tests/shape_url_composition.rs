mod common;

use bytes::Bytes;
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::{shape_request, DialectError, Upstream};
use http::HeaderValue;

#[test]
fn raw_predict_url_uses_project_region_model_and_raw_suffix() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());

    let shaped = shape_request(
        &VertexDialect::default(),
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");

    assert_eq!(
        shaped.url().as_str(),
        "https://us-central1-aiplatform.googleapis.com/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude-3-5-sonnet@20240620:rawPredict"
    );
    let body: serde_json::Value = serde_json::from_slice(shaped.body()).expect("json body");
    assert!(body.get("model").is_none());
}

#[test]
fn stream_true_body_uses_stream_raw_predict_suffix() {
    let ctx = common::request_context(common::messages_body(true), common::anthropic_headers());

    let shaped = shape_request(
        &VertexDialect::default(),
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");

    assert!(shaped
        .url()
        .as_str()
        .ends_with("/models/claude-3-5-sonnet@20240620:streamRawPredict"));
}

#[test]
fn event_stream_accept_header_uses_stream_raw_predict_suffix() {
    let mut headers = common::anthropic_headers();
    headers.append(
        "accept",
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    let ctx = common::request_context(common::messages_body(false), headers);

    let shaped = shape_request(
        &VertexDialect::default(),
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");

    assert!(shaped
        .url()
        .as_str()
        .ends_with("/models/claude-3-5-sonnet@20240620:streamRawPredict"));
}

#[test]
fn model_must_be_present_and_string() {
    for body in [r#"{"messages":[]}"#, r#"{"model":7,"messages":[]}"#] {
        let ctx =
            common::request_context(Bytes::from(body.to_owned()), common::anthropic_headers());
        let err = shape_request(
            &VertexDialect::default(),
            &ctx,
            &common::vertex_upstream(),
            &common::principal(),
        )
        .expect_err("invalid model must fail");

        assert!(matches!(err, DialectError::UnsupportedRequest { .. }));
        assert!(err.to_string().contains("model"));
    }
}

#[test]
fn wrong_upstream_variant_is_rejected() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());
    let err = shape_request(
        &VertexDialect::default(),
        &ctx,
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect_err("wrong upstream must fail");

    assert!(matches!(err, DialectError::UpstreamMismatch { .. }));
}
