use crate::admin_test_common;

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
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        dynamic_view_rebinder: None,
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
async fn test_audit_pagination() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(temp_dir.path(), "test.sqlite").await;

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
        storage.append_audit(&entry).await.unwrap();
    }

    let app = router(test_state(storage));

    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?principal_id=alice&limit=5")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["entries"].as_array().unwrap().len(), 5);
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries[0]["ts"], 1009);
    assert_eq!(entries[4]["ts"], 1005);

    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?principal_id=alice&since=1002&until=1005")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0]["ts"], 1005);
    assert_eq!(entries[3]["ts"], 1002);

    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?principal_id=alice&after=1005&limit=2")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["ts"], 1009);
    assert_eq!(entries[1]["ts"], 1008);
}

#[tokio::test]
async fn audit_returns_the_most_recent_200_matching_entries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(temp_dir.path(), "recent.sqlite").await;

    for i in 0..263 {
        storage
            .append_audit(&AuditEntry {
                ts: 10_000 + i,
                request_id: format!("bulk-{i}"),
                principal_id: "bulk-principal".to_owned(),
                route: "test".to_owned(),
                upstream: "test".to_owned(),
                status: 200,
                duration_ms: 1,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let app = router(test_state(storage));
    let request = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?until=10262&limit=200")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 200);
    assert_eq!(entries.first().unwrap()["request_id"], "bulk-262");
    assert_eq!(entries.last().unwrap()["request_id"], "bulk-63");
}

#[tokio::test]
async fn audit_admin_only_keeps_older_admin_actions_visible() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(temp_dir.path(), "admin_only.sqlite").await;

    // Reproduces the observed window failure: one older admin action followed
    // by more newer non-admin rows than the query limit.
    storage
        .append_audit(&AuditEntry {
            ts: 10_000,
            request_id: "older-admin-action".to_owned(),
            principal_id: "qa-safety-principal".to_owned(),
            route: "/admin/v1/principals".to_owned(),
            upstream: "admin".to_owned(),
            status: 200,
            admin_action: Some("principal_update".to_owned()),
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..250 {
        storage
            .append_audit(&AuditEntry {
                ts: 10_001 + i,
                request_id: format!("nonadmin-{i}"),
                principal_id: "qa-safety-principal".to_owned(),
                route: "/v1/messages".to_owned(),
                upstream: "upstream".to_owned(),
                status: 429,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let app = router(test_state(storage));

    let request = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?principal_id=qa-safety-principal&admin_only=true&limit=200")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|entry| entry["request_id"] == "older-admin-action"),
        "admin_only must surface the older admin action past newer non-admin rows"
    );
    assert!(entries.iter().all(|entry| !entry["admin_action"].is_null()));

    // The default query keeps returning non-admin rows.
    let request = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?principal_id=qa-safety-principal&limit=200")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 200);
    assert!(entries.iter().all(|entry| entry["admin_action"].is_null()));
}
