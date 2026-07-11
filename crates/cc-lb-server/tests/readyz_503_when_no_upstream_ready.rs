#![cfg(any())]

use crate::healthcheck_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_server::app::build_app_for_testing;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn readyz_503_when_no_upstream_ready() {
    let clock: cc_lb_engine::ClockHandle = std::sync::Arc::new(cc_lb_engine::SystemClock);
    let (upstream_addr, _upstream) =
        healthcheck_common::spawn_upstream(StatusCode::INTERNAL_SERVER_ERROR).await;
    let config = healthcheck_common::config_for_upstream(upstream_addr, 1);
    let app = build_app_for_testing(config, clock.clone())
        .await
        .expect("build app");

    let warmup = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header("x-api-key", "sk-ant-test")
        .body(Body::empty())
        .expect("warmup request");
    let warmup_response = app.router.clone().oneshot(warmup).await.expect("warmup");
    println!("warmup status={}", warmup_response.status());

    let request = Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(Body::empty())
        .expect("ready request");

    let response = app.router.clone().oneshot(request).await.expect("readyz");
    println!("/readyz status={}", response.status());
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
