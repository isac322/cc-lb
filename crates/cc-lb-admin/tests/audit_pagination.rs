mod admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_storage_redb::{AuditEntry, Storage};
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
        admin_token: Some("test-token".to_string()),
        lazy_refresher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_audit_pagination() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let master_key = [0u8; 32];
    let storage = Arc::new(Storage::open(&db_path, master_key).unwrap());

    for i in 0..10 {
        let entry = AuditEntry {
            ts: 1000 + i,
            request_id: format!("req_{}", i),
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
        storage.append_audit(&entry).unwrap();
    }

    let app = router(test_state(storage));

    let req = Request::builder()
        .method("GET")
        .uri("/admin/audit?limit=5")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["entries"].as_array().unwrap().len(), 5);

    let req = Request::builder()
        .method("GET")
        .uri("/admin/audit?since=1002&until=1005")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0]["ts"], 1002);
    assert_eq!(entries[3]["ts"], 1005);
}
