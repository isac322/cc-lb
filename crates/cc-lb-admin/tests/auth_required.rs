use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
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
async fn test_auth_required() {
    let app = router(test_state());

    let endpoints = vec![
        ("/admin/principals", "GET"),
        ("/admin/principals/alice/quota", "GET"),
        ("/admin/principals/alice/quota/override", "POST"),
        ("/admin/audit", "GET"),
        ("/admin/upstreams", "GET"),
        ("/admin/upstreams/test/health", "GET"),
        ("/admin/upstreams/test/drain", "POST"),
        ("/admin/plugins", "GET"),
        ("/admin/killswitch", "POST"),
        ("/admin/killswitch", "DELETE"),
        ("/admin/oauth/start", "POST"),
        ("/admin/oauth/complete", "POST"),
        ("/admin/oauth/status", "GET"),
        ("/admin/config/current", "GET"),
        ("/admin/config/schema", "GET"),
        ("/admin/config/draft", "GET"),
        ("/admin/config/draft", "PUT"),
        ("/admin/config/draft/validate", "POST"),
        ("/admin/config/apply", "POST"),
        ("/admin/config/history", "GET"),
        ("/admin/config/diff?from_revision=1&to_revision=2", "GET"),
        ("/admin/config/reload", "POST"),
        ("/admin/dashboard/summary", "GET"),
        ("/admin/usage", "GET"),
        ("/admin/principals/alice/usage", "GET"),
        ("/admin/principals/alice/limits", "GET"),
        ("/admin/events/recent", "GET"),
        ("/admin/events/stream", "GET"),
    ];

    for (path, method) in endpoints {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Endpoint {} should require auth",
            path
        );
    }
}

#[tokio::test]
async fn test_auth_success() {
    let app = router(test_state());

    let req = Request::builder()
        .method("GET")
        .uri("/admin/principals")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
