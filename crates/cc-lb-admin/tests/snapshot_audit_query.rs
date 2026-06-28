mod admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_storage_api::AuditEntry;
use cc_lb_storage_api::AuditStore;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_token: Some("test-token".to_string()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        clock: Arc::new(cc_lb_core::SystemClock),
    }
}

#[tokio::test]
async fn test_snapshot_audit_query() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(temp_dir.path(), "test.sqlite").await;

    let entry = AuditEntry {
        ts: 1000,
        request_id: "req_1".to_string(),
        principal_id: "alice".to_string(),
        route: "test".to_string(),
        upstream: "test".to_string(),
        model: None,
        status: 200,
        input_tokens: Some(10),
        output_tokens: Some(10),
        duration_ms: 100,
        agent_label: None,
        ..Default::default()
    };
    storage.append_audit(&entry).await.unwrap();

    let app = router(test_state(storage));

    let req = Request::builder()
        .method("GET")
        .uri("/admin/audit")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}
