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
async fn t2__api_oauth_usage_returns_anthropic_usage_shape() {
    let app = build_app_for_testing(Config::default(), cc_lb_testkit::fixed_clock(1_700_000_000))
        .await
        .expect("build app");
    let request = Request::builder()
        .method("GET")
        .uri("/api/oauth/usage")
        .body(Body::empty())
        .expect("request builds");

    let response = app.router.oneshot(request).await.expect("usage route");

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
