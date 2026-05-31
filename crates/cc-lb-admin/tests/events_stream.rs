mod config_admin_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_config::Config;
use config_admin_common::{TOKEN, app, authed_bytes, temp_storage, test_state};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn events_stream_opens_with_sse_content_type() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .expect("content-type")
        .to_str()
        .unwrap();
    assert!(
        content_type.starts_with("text/event-stream"),
        "expected SSE content-type, got {content_type}",
    );
}

#[tokio::test]
async fn events_stream_first_byte_is_connected_comment() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let mut body = response.into_body();
    let frame = body.frame().await.expect("first frame").expect("frame ok");
    let bytes = frame.into_data().expect("data frame");
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.starts_with(": connected"),
        "expected ': connected' initial SSE comment, got {text:?}",
    );
}

#[tokio::test]
async fn events_stream_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/events/stream", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
