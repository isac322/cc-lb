mod admin_test_common;

mod config_admin_common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
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
        config: Arc::new(config),
        admin_token: Some("test-token".to_owned()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
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
                upstreams: 0,
                principals: 0,
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
