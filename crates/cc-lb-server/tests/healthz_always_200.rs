mod healthcheck_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_server::app::build_app_for_testing;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn healthz_always_200() {
    let (upstream_addr, _upstream) = healthcheck_common::spawn_upstream(StatusCode::OK).await;
    let config = healthcheck_common::config_for_upstream(upstream_addr, 5);
    let app = build_app_for_testing(config).expect("build app");

    let request = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Body::empty())
        .expect("health request");

    let response = app.router.clone().oneshot(request).await.expect("healthz");
    println!("/healthz status={}", response.status());
    assert_eq!(response.status(), StatusCode::OK);

    let body = response
        .into_body()
        .collect()
        .await
        .expect("health body")
        .to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("health json");
    assert_eq!(json["status"], "ok");
    assert!(json["uptime_seconds"].is_u64());
    for key in ["version", "git_sha", "build_time", "rustc", "features"] {
        assert!(json["build"][key].is_string(), "missing build key {key}");
    }
}
