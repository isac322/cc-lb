use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::{AuditEntry, RedbStorage};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state(storage: Arc<RedbStorage>) -> AdminState {
    AdminState {
        storage,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config: Arc::new(Config::default()),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_snapshot_audit_query() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&db_path).unwrap());

    let entry = AuditEntry {
        ts: 1000,
        request_id: "req_1".to_string(),
        principal_id: "alice".to_string(),
        route: "test".to_string(),
        upstream: "test".to_string(),
        model: None,
        status: 200,
        input_tokens: 10,
        output_tokens: 10,
        duration_ms: 100,
        agent_label: None,
        kind: None,
        payload: None,
    };
    storage.append_audit(&entry).unwrap();

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
