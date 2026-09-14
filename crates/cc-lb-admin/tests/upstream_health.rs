use crate::config_admin_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn test_state() -> AdminState {
    config_admin_common::test_state_without_storage()
}

#[tokio::test]
async fn t2__upstream_health_current_admin_health_smoke() {
    let response = router(test_state())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert!(body["git_sha"].is_string());
    assert!(body["uptime_secs"].is_number());
}
