use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    AdminState {
        storage: None,
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config: Arc::new(Config::default()),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_snapshot_health() {
    let app = router(test_state());

    let req = Request::builder()
        .method("GET")
        .uri("/admin/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let mut json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    json["uptime_secs"] = serde_json::json!(0);
    json["version"] = serde_json::json!("0.1.0");
    json["git_sha"] = serde_json::json!("unknown");

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}
