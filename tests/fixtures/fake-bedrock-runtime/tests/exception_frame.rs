use aws_eventstream_codec::decode_messages;
use axum::body::{Body, to_bytes};
use fake_bedrock_runtime::{AppConfig, app};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn model_stream_error_exception_frame_is_distinguishable() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/model/m/invoke-with-response-stream")
                .header("x-fake-mode", "ModelStreamErrorException")
                .body(Body::from("{}"))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let decoded = decode_messages(&body).expect("event-stream decodes");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].header_str(":message-type"), Some("exception"));
    assert_eq!(
        decoded[0].header_str(":exception-type"),
        Some("ModelStreamErrorException")
    );
}
