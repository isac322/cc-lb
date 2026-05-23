use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn record_whitelist_headers() {
    let app = app(AppConfig::default());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("X-Organization-UUID", "org-123")
                .header("X-Trusted-Device-Token", "device-456")
                .body(Body::from(
                    r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);

    let last_request = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__last_request")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(last_request.status(), StatusCode::OK);

    let body = to_bytes(last_request.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");

    assert_eq!(body["x_api_key"], "sk-ant-test");
    assert_eq!(body["headers"]["x-organization-uuid"], "org-123");
    assert_eq!(body["headers"]["x-trusted-device-token"], "device-456");
    assert_eq!(
        body["headers"].as_object().expect("headers object").len(),
        2
    );
}
