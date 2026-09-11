use crate::admin_test_common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use http_body_util::BodyExt;
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
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
async fn t2__upstream_health_current_admin_health_smoke() {
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
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert!(body["git_sha"].is_string());
    assert!(body["uptime_secs"].is_number());
}
