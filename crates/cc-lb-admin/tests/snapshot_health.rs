use crate::admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        dynamic_view_rebinder: None,
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
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
