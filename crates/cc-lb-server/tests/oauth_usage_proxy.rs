use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_config::Config;
use cc_lb_server::app::build_app_for_testing;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn api_oauth_usage_returns_anthropic_usage_shape() {
    let clock: cc_lb_core::ClockHandle = std::sync::Arc::new(cc_lb_core::SystemClock);
    let app = build_app_for_testing(Config::default(), clock.clone())
        .await
        .expect("build app");
    let request = Request::builder()
        .method("GET")
        .uri("/api/oauth/usage")
        .body(Body::empty())
        .expect("request builds");

    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("usage route");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("usage json");
    assert!(json.get("5h").is_some());
    assert!(json.get("7d").is_some());
    assert!(json.get("five_hour").is_none());
    assert!(json.get("windows").is_none());
    assert!(json.get("caveats").is_none());
}
