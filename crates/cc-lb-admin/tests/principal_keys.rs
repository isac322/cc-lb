mod admin_test_common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_storage_api::AuditStore;
use serde_json::json;
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
        scheduler: None,
        admin_token: Some("test-token".to_owned()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_engine::SystemClock),
    }
}

#[tokio::test]
async fn principal_keys_current_admin_principals_smoke() {
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

#[tokio::test]
async fn revoked_key_list_preserves_key_id_last4_and_audit_rows() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": "keys", "kind": "machine", "allowed_models": [], "default_limits": [] }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();

    let (status, _, issued) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal_id}/keys"),
            json!({ "label": "qa-revoked-shape" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let key_id = issued["key_id"].as_str().unwrap();
    let plaintext = issued["plaintext_key"].as_str().unwrap();
    let expected_last4 = &plaintext[plaintext.len() - 4..];

    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal_id}/keys/{key_id}/revoke"),
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/principals/{principal_id}/keys?status=revoked"
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    let keys = body["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["key_id"], key_id);
    assert_eq!(keys[0]["last_4"], expected_last4);

    wait_for_audit_action(&server.storage, "principal_key_issue", key_id).await;
    wait_for_audit_action(&server.storage, "principal_key_revoke", key_id).await;
}

async fn wait_for_audit_action(
    storage: &std::sync::Arc<cc_lb_storage_sqlite::SqliteStorage>,
    needle: &str,
    key_id: &str,
) {
    for _ in 0..20 {
        let entries = storage.query_audit(None, 0, u64::MAX, 100).await.unwrap();
        if entries.iter().any(|entry| {
            entry.api_key_id.as_deref() == Some(key_id)
                && entry
                    .admin_action
                    .as_deref()
                    .is_some_and(|action| action.contains(needle))
        }) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("audit action {needle} for key {key_id} was not persisted");
}
