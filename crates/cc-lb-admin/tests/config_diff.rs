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
        key_store: None,
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
async fn config_diff_route_returns_history_difference() {
    let (_dir, storage) = config_admin_common::temp_storage();
    let mut from_config = Config::default();
    from_config.body.messages_cap_bytes = 100;
    let mut to_config = Config::default();
    to_config.body.messages_cap_bytes = 200;
    for (revision, config) in [(1, &from_config), (2, &to_config)] {
        storage
            .append_config_history(
                revision,
                toml::to_string_pretty(config).unwrap(),
                1000 + revision,
                cc_lb_storage_redb::HistorySummary {
                    upstreams: config.upstreams.len(),
                    principals: config.principals.len(),
                    plugin_count: 0,
                    tls_enabled: false,
                },
            )
            .unwrap();
    }
    let app = config_admin_common::app(config_admin_common::test_state(
        Config::default(),
        Some(storage),
    ));

    let (status, _, json, _) = config_admin_common::authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=2",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["from"], 1);
    assert_eq!(json["to"], 2);
    assert!(json["diff"].as_array().unwrap().iter().any(|item| {
        item["path"] == "body.messages_cap_bytes" && item["from"] == 100 && item["to"] == 200
    }));
}

#[tokio::test]
async fn config_diff_current_admin_config_smoke() {
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
