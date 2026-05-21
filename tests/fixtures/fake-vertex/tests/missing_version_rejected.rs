use axum::body::{to_bytes, Body};
use fake_vertex::{app, AppConfig};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn missing_vertex_anthropic_version_is_rejected() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude:rawPredict")
                .header("authorization", "Bearer ya29.test")
                .body(Body::from(r#"{"messages":[],"max_tokens":10}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["error"]["code"], 400);
    assert_eq!(body["error"]["status"], "INVALID_ARGUMENT");
}
