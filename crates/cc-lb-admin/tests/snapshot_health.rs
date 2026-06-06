mod admin_test_common;

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
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        admin_token: Some("test-token".to_string()),
        lazy_refresher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
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
