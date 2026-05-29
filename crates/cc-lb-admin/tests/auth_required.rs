mod admin_test_common;

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
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_auth_required() {
    let app = router(test_state());

    let endpoints = vec![
        ("/admin/principals", "GET"),
        ("/admin/principals/alice/limits", "GET"),
        ("/admin/principals/alice/keys", "GET"),
        ("/admin/principals/alice/keys", "POST"),
        ("/admin/principals/alice/keys/key-1/revoke", "POST"),
        ("/admin/audit", "GET"),
        ("/admin/upstreams", "GET"),
        ("/admin/upstreams/test/drain", "POST"),
        ("/admin/killswitch", "POST"),
        ("/admin/killswitch", "DELETE"),
        ("/admin/oauth/start", "POST"),
        ("/admin/oauth/complete", "POST"),
        ("/admin/config/current", "GET"),
        ("/admin/config/reload", "POST"),
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
