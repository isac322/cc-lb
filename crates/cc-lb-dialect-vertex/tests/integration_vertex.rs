mod common;

use axum::body::{to_bytes, Body};
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::shape_request;
use fake_vertex::{app, AppConfig};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn shaped_request_reaches_fake_vertex_as_anthropic_message() {
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());
    let shaped = shape_request(
        &VertexDialect,
        &ctx,
        &common::vertex_upstream(),
        &common::principal(),
    )
    .expect("vertex shape succeeds");
    let mut builder = Request::builder()
        .method(shaped.method().clone())
        .uri(shaped.url().path());
    for (name, value) in shaped.headers() {
        builder = builder.header(name, value);
    }
    let request = builder
        .header("authorization", "Bearer ya29.test")
        .body(Body::from(shaped.body().clone()))
        .expect("request builds");

    let response = app(AppConfig::default())
        .oneshot(request)
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");

    assert_eq!(body["type"], "message");
    assert_eq!(body["model"], "claude-3-5-sonnet@20240620");
    assert_eq!(
        body["content"][0]["text"],
        "fake vertex fixture response HELLO"
    );
    println!(
        "vertex_e2e status=200 anthropic_type={} model={} output_tokens={}",
        body["type"].as_str().unwrap_or("missing"),
        body["model"].as_str().unwrap_or("missing"),
        body["usage"]["output_tokens"].as_u64().unwrap_or(0)
    );
}
