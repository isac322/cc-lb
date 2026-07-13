use crate::admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_token: Some("test-token".to_string()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

#[tokio::test]
async fn test_auth_required() {
    let app = router(test_state());

    let endpoints = vec![
        ("/admin/principals/alice/usage", "GET"),
        ("/admin/principals/alice/limits", "GET"),
        ("/admin/principals/alice/keys/key-1", "GET"),
        ("/admin/principals/alice/keys/key-1/revoke", "POST"),
        ("/admin/principals/alice/keys/key-1/disable", "POST"),
        ("/admin/principals/alice/keys/key-1/enable", "POST"),
        ("/admin/principals/alice/keys/key-1/usage", "GET"),
        ("/admin/audit", "GET"),
        ("/admin/status", "GET"),
        ("/admin/killswitch", "POST"),
        ("/admin/killswitch", "DELETE"),
        ("/admin/config/current", "GET"),
        ("/admin/config/reload", "POST"),
        ("/admin/scheduler/status", "GET"),
        ("/admin/scheduler/failures", "GET"),
        ("/admin/v1/status", "GET"),
        ("/admin/v1/upstreams", "GET"),
        ("/admin/v1/upstreams", "POST"),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001",
            "GET",
        ),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001/enable",
            "POST",
        ),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001/oauth/start",
            "POST",
        ),
        ("/admin/v1/principals", "GET"),
        ("/admin/v1/principals", "POST"),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001",
            "GET",
        ),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001/disable",
            "POST",
        ),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001/plugin-chain",
            "GET",
        ),
        ("/admin/v1/plugins/registry", "GET"),
        ("/admin/v1/plugins/registry", "POST"),
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
        .uri("/admin/config/current")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
