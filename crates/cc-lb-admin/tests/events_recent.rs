use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::api_keys::{
    concurrent_guard::KeyConcurrencyManager, limit_engine::LimitEngine,
    principal_view::PrincipalView,
};
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&config, std::collections::HashMap::new()).expect("principal view builds"),
    ));
    AdminState {
        storage: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            principal_view.clone(),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view,
        config: Arc::new(config),
        admin_token: Some("test-token".to_owned()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn events_recent_current_admin_health_smoke() {
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
}
