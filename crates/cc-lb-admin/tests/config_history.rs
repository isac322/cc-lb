mod config_admin_common;

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
    let principal_view = Arc::new(ArcSwap::from(PrincipalView::from_config(&config)));
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
async fn config_history_route_returns_applied_history() {
    let (_dir, storage) = config_admin_common::temp_storage();
    let config = Config::default();
    storage
        .append_config_history(
            7,
            toml::to_string_pretty(&config).unwrap(),
            1234,
            cc_lb_storage_redb::HistorySummary {
                upstreams: config.upstreams.len(),
                principals: config.principals.len(),
                plugin_count: 0,
                tls_enabled: false,
            },
        )
        .unwrap();
    let app = config_admin_common::app(config_admin_common::test_state(config, Some(storage)));

    let (status, _, json, _) =
        config_admin_common::authed_json(app, "GET", "/admin/config/history?limit=1", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["history"][0]["revision"], 7);
    assert_eq!(json["history"][0]["applied_at_unix_secs"], 1234);
}

#[tokio::test]
async fn config_history_current_admin_config_smoke() {
    let response = router(test_state())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/config/current")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
