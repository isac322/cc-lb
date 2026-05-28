use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    AdminState {
        storage: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: cc_lb_core::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            Arc::new(
                arc_swap::ArcSwap::from(
                    cc_lb_core::api_keys::principal_view::PrincipalView::from_config(
                        &Config::default(),
                    )
                    .expect("principal view builds"),
                ),
            ),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view: Arc::new(arc_swap::ArcSwap::from(
            cc_lb_core::api_keys::principal_view::PrincipalView::from_config(
                &cc_lb_admin::CurrentConfig::current_config((Arc::new(Config::default())).as_ref()),
            )
            .expect("principal view builds"),
        )),
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
