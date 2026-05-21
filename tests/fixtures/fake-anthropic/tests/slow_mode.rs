use std::time::Instant;

use axum::body::Body;
use fake_anthropic::{app, AppConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn slow_mode_rate_limits_first_sse_frame() {
    let response = app(AppConfig {
        slow_mode_bps: 512,
        files_cap_bytes: 104_857_600,
    })
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header("x-api-key", "sk-ant-test")
            .header("x-fake-mode", "slow")
            .body(Body::from(
                r#"{"model":"c","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#,
            ))
            .expect("request builds"),
    )
    .await
    .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let started = Instant::now();
    let mut body = response.into_body();
    let frame = body
        .frame()
        .await
        .expect("first frame exists")
        .expect("first frame is ok");
    let elapsed = started.elapsed();
    let bytes = frame.into_data().expect("data frame");
    let rate = bytes.len() as f64 / elapsed.as_secs_f64();

    assert!(bytes.len() > 64);
    assert!(
        rate <= 1024.0,
        "first SSE frame emitted too quickly: {rate:.2} B/s over {elapsed:?}"
    );
}
