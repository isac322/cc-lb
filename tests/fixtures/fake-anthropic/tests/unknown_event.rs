use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn tamper_unknown_event_mode_emits_foo_event() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("x-fake-mode", "tamper-unknown-event")
                .body(Body::from(
                    r#"{"model":"c","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let text = String::from_utf8(body.to_vec()).expect("utf8 sse");

    assert!(text.contains("event: foo"));
    assert!(text.contains("data: {\"x\":1}"));
    assert!(text.contains("event: message_stop"));
}
