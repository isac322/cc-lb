use crate::admin_test_common;
use axum::http::{HeaderMap, HeaderValue, StatusCode};

use std::sync::Arc;

use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_storage_api::{AuditStore, RequestEvent, RequestEventStore};
use serde_json::json;

#[tokio::test]
async fn key_issue_rejects_removed_kind_fields() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": "strict-key-issue", "kind": "human", "allowed_models": [], "default_limits": [] }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();

    for removed in ["principal_kind", "upstream_kind"] {
        let mut request = json!({ "label": "must-reject" });
        request[removed] = json!("machine");
        let (status, _, body) = server
            .client
            .post_json(
                &format!("/admin/v1/principals/{principal_id}/keys"),
                request,
            )
            .await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"], "validation_failed");
        assert!(body["message"].as_str().unwrap().contains(removed));
    }
}

#[tokio::test]
async fn key_issue_accepts_omitted_and_empty_labels_and_authenticates() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": "optional-key-label", "kind": "machine", "allowed_models": [], "default_limits": [] }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();
    admin_test_common::set_dynamic_principal(&server.dynamic_view, principal_id);
    let authn = BuiltinAuthn::new(
        admin_test_common::key_store(server.storage.clone()),
        Arc::new(cc_lb_clock::SystemClock),
    );

    for request in [json!({}), json!({ "label": "" })] {
        let (status, _, issued) = server
            .client
            .post_json(
                &format!("/admin/v1/principals/{principal_id}/keys"),
                request,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "issue response: {issued}");
        let expected_key_id = issued["key_id"].as_str().unwrap().to_owned();

        let plaintext = issued["plaintext_key"].as_str().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_bytes(plaintext.as_bytes()).unwrap(),
        );
        let view = server.dynamic_view.load();
        let authenticated = authn
            .authenticate(&headers, &view.principal_view)
            .await
            .expect("empty-label key authenticates");
        assert_eq!(authenticated.principal_id, principal_id);
        assert_eq!(authenticated.key_id, expected_key_id);
    }

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{principal_id}/keys"))
        .await;
    assert_eq!(status, StatusCode::OK);
    let keys = body["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().all(|key| key["label"].is_null()));
}

#[tokio::test]
async fn key_issue_maps_invalid_nonempty_labels_to_bad_request() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": "invalid-key-label", "kind": "machine", "allowed_models": [], "default_limits": [] }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();

    for (label, reason_fragment) in [("bad\0label", "NUL"), ("system.reserved-label", "system")] {
        let (status, _, body) = server
            .client
            .post_json(
                &format!("/admin/v1/principals/{principal_id}/keys"),
                json!({ "label": label }),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_input");
        assert_eq!(body["field"], "label");
        assert!(
            body["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains(reason_fragment))
        );
    }

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{principal_id}/keys"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["keys"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn revoked_key_list_preserves_key_id_last4_and_audit_rows() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": "keys", "kind": "human", "allowed_models": [], "default_limits": [] }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();
    admin_test_common::set_dynamic_principal(&server.dynamic_view, principal_id);

    let (status, _, issued) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal_id}/keys"),
            json!({ "label": "qa-revoked-shape" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(issued.get("principal_kind").is_none());
    assert!(issued.get("upstream_kind").is_none());
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
    assert!(keys[0].get("principal_kind").is_none());
    assert!(keys[0].get("upstream_kind").is_none());

    wait_for_audit_action(&server.storage, "principal_key_issue", key_id).await;
    wait_for_audit_action(&server.storage, "principal_key_revoke", key_id).await;
}

#[tokio::test]
async fn legacy_key_routes_record_concrete_actor_aware_audits_without_secrets() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({
                "name": "legacy-key-audit",
                "kind": "machine",
                "allowed_models": [],
                "default_limits": []
            }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();
    admin_test_common::set_dynamic_principal(&server.dynamic_view, principal_id);

    let (status, _, issued) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal_id}/keys"),
            json!({ "label": "legacy-audit" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let key_id = issued["key_id"].as_str().unwrap();

    let key_route = format!("/admin/principals/{principal_id}/keys/{key_id}");
    let (status, _, key) = server.client.get(&key_route).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(key["id"], key_id);
    assert_eq!(key["principal_id"], principal_id);
    assert!(key.get("plaintext_key").is_none());
    assert!(key.get("principal_kind").is_none());
    assert!(key.get("upstream_kind").is_none());

    for (operation, action, expected_key_status) in [
        ("disable", "principal_key_disable", "disabled"),
        ("enable", "principal_key_enable", "active"),
        ("revoke", "principal_key_revoke", "revoked"),
    ] {
        let route = format!("{key_route}/{operation}");
        let (status, _, body) = server.client.post_json(&route, json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({ "status": "ok" }));

        let (status, _, key) = server.client.get(&key_route).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(key["status"], expected_key_status);

        let entries = server
            .storage
            .query_audit(Some(principal_id), 0, u64::MAX, 100)
            .await
            .unwrap();
        let entry = entries
            .iter()
            .find(|entry| {
                entry.admin_action.as_deref() == Some(action)
                    && entry.api_key_id.as_deref() == Some(key_id)
            })
            .unwrap_or_else(|| panic!("missing {action} audit for key {key_id}"));
        assert_eq!(entry.route, route);
        assert_eq!(entry.principal_id, principal_id);
        assert_eq!(entry.status, StatusCode::OK.as_u16());
        assert_eq!(entry.actor_authority.as_deref(), Some("static-token"));
        assert_eq!(entry.actor_subject.as_deref(), Some("test-static-token"));
        assert_eq!(entry.actor_kind.as_deref(), Some("break_glass"));
        assert!(entry.payload.is_none());
    }
}

#[tokio::test]
async fn principal_key_usage_uses_persisted_request_events() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, principal) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": "key-usage", "kind": "machine", "allowed_models": [], "default_limits": [] }),
        )
        .await;
    let principal_id = principal["id"].as_str().unwrap();
    admin_test_common::set_dynamic_principal(&server.dynamic_view, principal_id);

    let (status, _, issued) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal_id}/keys"),
            json!({ "label": "usage" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let key_id = issued["key_id"].as_str().unwrap();
    let event_ts = cc_lb_clock::unix_secs(std::time::SystemTime::now());
    let event_ts_ms = event_ts.saturating_mul(1_000);

    server
        .storage
        .append_request_event(&RequestEvent {
            ts: event_ts,
            ts_ms: Some(event_ts_ms),
            request_id: "req-admin-key-usage-a".to_owned(),
            event_id: Some("0193a7b8-1234-7e2f-9012-adminkey0001".to_owned()),
            principal_id: Some(principal_id.to_owned()),
            key_id: Some(key_id.to_owned()),
            input_tokens: Some(10),
            cache_creation_input_tokens: Some(20),
            cache_read_input_tokens: Some(30),
            output_tokens: Some(40),
            cost_usd_micros: Some(50),
            status: 200,
            duration_ms: 10,
            ..Default::default()
        })
        .await
        .expect("append request event");

    let (status, _, keys_body) = server
        .client
        .get(&format!("/admin/v1/principals/{principal_id}/keys"))
        .await;
    assert_eq!(status, StatusCode::OK);
    let keys = keys_body["keys"].as_array().unwrap();
    let key = keys
        .iter()
        .find(|entry| entry["key_id"] == key_id)
        .expect("issued key is listed");
    assert_eq!(key["last_used_at_unix_secs"], event_ts);

    let (status, _, usage_body) = server
        .client
        .get(&format!(
            "/admin/principals/{principal_id}/keys/{key_id}/usage?range=7d&step=1d"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "usage response: {usage_body}");
    let observed = usage_body["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["request_count"] == 1)
        .expect("usage bucket with request");
    assert_eq!(observed["input_tokens"], 60);
    assert_eq!(observed["output_tokens"], 40);
    assert_eq!(observed["cost_usd_micros"], 50);
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
