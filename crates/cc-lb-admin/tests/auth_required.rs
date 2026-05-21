use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::Config;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    AdminState {
        storage: None,
        quota_manager: None,
        lifecycle: None,
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
        ("/admin/principals/alice/quota", "GET"),
        ("/admin/principals/alice/quota/override", "POST"),
        ("/admin/audit", "GET"),
        ("/admin/upstreams", "GET"),
        ("/admin/upstreams/test/drain", "POST"),
        ("/admin/killswitch", "POST"),
        ("/admin/killswitch", "DELETE"),
        ("/admin/oauth/start", "POST"),
        ("/admin/oauth/complete", "POST"),
        ("/admin/config/current", "GET"),
        ("/admin/config/reload", "POST"),
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
