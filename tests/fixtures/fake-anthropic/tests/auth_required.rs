use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn missing_auth_returns_anthropic_error_shape() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .body(Body::from("{}"))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().contains_key("request-id"));

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "authentication_error");
}

#[tokio::test]
async fn invalid_auth_is_rejected_and_bearer_sk_ant_is_accepted() {
    let invalid = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("x-api-key", "not-a-key")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);

    let accepted = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer sk-ant-test")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    assert_eq!(accepted.status(), StatusCode::OK);
}
