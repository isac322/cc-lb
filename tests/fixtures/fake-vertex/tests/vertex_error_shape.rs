use axum::body::{to_bytes, Body};
use fake_vertex::{app, AppConfig};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn bad_version_returns_vertex_error_shape_not_anthropic_error() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude:rawPredict")
                .header("authorization", "Bearer ya29.test")
                .body(Body::from(r#"{"anthropic_version":"2023-06-01","messages":[]}"#))
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
    assert!(body.get("type").is_none());
}

#[tokio::test]
async fn missing_or_non_ya29_auth_returns_vertex_unauthenticated() {
    let app = app(AppConfig::default());
    let missing = request(app.clone(), None).await;
    assert_eq!(missing.0, StatusCode::UNAUTHORIZED);
    assert_eq!(missing.1["error"]["status"], "UNAUTHENTICATED");

    let wrong = request(app, Some("Bearer sk-ant-test")).await;
    assert_eq!(wrong.0, StatusCode::UNAUTHORIZED);
    assert_eq!(wrong.1["error"]["code"], 401);
}

async fn request(app: axum::Router, auth: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude:rawPredict");
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    let response = app
        .oneshot(
            builder
                .body(Body::from(
                    r#"{"anthropic_version":"vertex-2023-10-16","messages":[]}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    (status, serde_json::from_slice(&body).expect("json body"))
}
