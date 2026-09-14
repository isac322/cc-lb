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
async fn t2__readyz_503_when_no_upstream_ready() {
    let app = build_app_for_testing(Config::default(), cc_lb_testkit::fixed_clock(1_700_000_000))
        .await
        .expect("build app");

    let response = app
        .router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .expect("ready request"),
        )
        .await
        .expect("readyz");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("ready body")
        .to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("ready json");
    assert_eq!(json["ready"], false);
    assert_eq!(json["reason"], "no_ready_upstream");
}
