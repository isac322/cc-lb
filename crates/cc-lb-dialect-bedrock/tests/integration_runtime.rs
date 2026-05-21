mod common;

use axum::body::{to_bytes, Body};
use cc_lb_dialect_bedrock::{convert_eventstream_to_sse_bytes, BedrockRuntimeDialect};
use cc_lb_plugin_api::{shape_request, Upstream};
use fake_bedrock_runtime::{app, AppConfig};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn shaped_runtime_request_converts_fake_eventstream_to_sse() {
    let ctx = common::request_context(common::messages_body(true), common::anthropic_headers());
    let shaped = shape_request(
        &BedrockRuntimeDialect::default(),
        &ctx,
        &Upstream::BedrockRuntime {
            region: "us-east-1".to_owned(),
        },
        &common::principal(),
    )
    .expect("runtime shape succeeds");

    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method(shaped.method())
                .uri(shaped.url().path())
                .header("authorization", common::valid_auth_header())
                .body(Body::from(shaped.body().clone()))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let sse = convert_eventstream_to_sse_bytes(&body).expect("event-stream converts");
    let sse_text = String::from_utf8(sse.to_vec()).expect("sse is utf8");
    let delta_count = sse_text.matches("event: content_block_delta").count();
    let message_stop_count = sse_text.matches("event: message_stop").count();

    println!(
        "ConvertedSseEvents={delta_count} MessageStopEvents={message_stop_count} SseBytes={}",
        sse_text.len()
    );
    assert_eq!(delta_count, 50);
    assert_eq!(message_stop_count, 1);
}
