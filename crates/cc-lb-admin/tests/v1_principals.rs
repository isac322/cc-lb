use crate::admin_test_common;

use std::time::Duration;

use axum::http::{StatusCode, header};
use cc_lb_storage_api::{
    AuditQueryScope, AuditStore, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind,
    WasmBlob, WasmRegistryEntryInput,
};
use serde_json::{Value, json};

#[tokio::test]
async fn create_201_with_etag_and_location() {
    let server = admin_test_common::spawn_admin_server().await;

    let (status, headers, body) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({
                "name": "alpha",
                "kind": "machine",
                "allowed_models": ["claude-3-*"],
                "default_limits": []
            }),
        )
        .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["name"], "alpha");
    assert_eq!(body["kind"], "machine");
    assert_eq!(body["enabled"], true);
    let revision = body["revision"].as_u64().unwrap();
    let id = body["id"].as_str().unwrap();
    assert_eq!(
        server
            .client
            .header_str(&headers, header::LOCATION.as_str()),
        format!("/admin/v1/principals/{id}")
    );
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        format!("W/\"{revision}\"")
    );
}

#[tokio::test]
async fn list_paginates_with_x_total_count() {
    let server = admin_test_common::spawn_admin_server().await;
    for name in ["alpha", "bravo", "charlie"] {
        server
            .client
            .post_json(
                "/admin/v1/principals",
                json!({ "name": name, "kind": "machine", "allowed_models": [], "default_limits": [] }),
            )
            .await;
    }

    let (status, headers, body) = server
        .client
        .get("/admin/v1/principals?after=1&limit=1")
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(server.client.header_str(&headers, "x-total-count"), "3");
    assert_eq!(body["principals"].as_array().unwrap().len(), 1);
    assert_eq!(body["principals"][0]["name"], "bravo");
}

#[tokio::test]
async fn get_returns_etag_with_weak_revision() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let revision = created["revision"].as_u64().unwrap();

    let (status, headers, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], id);
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        format!("W/\"{revision}\"")
    );
}

#[tokio::test]
async fn get_allowed_models_returns_state_with_etag() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, headers, created) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({
                "name": "model-reader",
                "kind": "machine",
                "allowed_models": ["claude-haiku-*"],
                "default_limits": []
            }),
        )
        .await;
    let id = created_id(&created);
    let etag = server.client.header_str(&headers, header::ETAG.as_str());

    let (status, headers, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}/allowed_models"))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["models"], json!(["claude-haiku-*"]));
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        etag
    );
}

#[tokio::test]
async fn update_correct_if_match_bumps_revision() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server.client.header_str(&headers, header::ETAG.as_str());

    let (status, headers, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-renamed", "allowed_upstreams": ["11111111-1111-1111-1111-111111111111"] }),
            Some(etag),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "alpha-renamed");
    assert_eq!(
        body["allowed_upstreams"],
        json!(["11111111-1111-1111-1111-111111111111"])
    );
    let revision = body["revision"].as_u64().unwrap();
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        format!("W/\"{revision}\"")
    );
}

#[tokio::test]
async fn update_endpoints_audit_distinct_concrete_routes() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, headers, created) = create_principal(&server.client, "audit-routes").await;
    let id = created_id(&created);
    let etag = server.client.header_str(&headers, header::ETAG.as_str());
    let principal_route = format!("/admin/v1/principals/{id}");
    let allowed_models_route = format!("{principal_route}/allowed_models");

    let (status, _, updated) = server
        .client
        .put_json(
            &principal_route,
            json!({ "name": "audit-routes-renamed" }),
            Some(etag),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = server
        .client
        .put_json(
            &allowed_models_route,
            json!({
                "models": ["claude-audit-*"],
                "expected_revision": updated["revision"]
            }),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let entries = server
        .storage
        .query_recent_audit(AuditQueryScope::Principal(&id), 0, u64::MAX, 20, false)
        .await
        .unwrap();
    assert!(entries.iter().any(|entry| {
        entry.route == principal_route
            && entry.admin_action.as_deref().is_some_and(|action| {
                action.contains("principal_update") && action.contains("name")
            })
    }));
    assert!(entries.iter().any(|entry| {
        entry.route == allowed_models_route
            && entry.admin_action.as_deref().is_some_and(|action| {
                action.contains("principal_update") && action.contains("allowed_models")
            })
    }));

    assert!(
        entries
            .iter()
            .all(|entry| entry.route != "/admin/v1/principals/{id}")
    );
}

#[tokio::test]
async fn allowed_upstreams_round_trips_through_create_patch_and_get() {
    let server = admin_test_common::spawn_admin_server().await;
    let first_upstream = "11111111-1111-1111-1111-111111111111";
    let second_upstream = "22222222-2222-2222-2222-222222222222";
    let (_, headers, created) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({
                "name": "alpha",
                "kind": "machine",
                "allowed_models": [],
                "allowed_upstreams": [first_upstream],
                "default_limits": []
            }),
        )
        .await;
    let id = created_id(&created);
    let etag = server.client.header_str(&headers, header::ETAG.as_str());

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allowed_upstreams"], json!([first_upstream]));

    let (status, headers, body) = server
        .client
        .json(
            "PATCH",
            &format!("/admin/v1/principals/{id}"),
            Some(json!({ "allowed_upstreams": [second_upstream] })),
            &[(header::IF_MATCH.as_str(), etag)],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allowed_upstreams"], json!([second_upstream]));
    let revision = body["revision"].as_u64().unwrap();

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allowed_upstreams"], json!([second_upstream]));
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        format!("W/\"{revision}\"")
    );
}

#[tokio::test]
async fn update_stale_if_match_returns_conflict() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (status, _, first_update) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-one" }),
            Some(&etag),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let current_revision = first_update["revision"].as_u64().unwrap();

    let (status, _, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-two" }),
            Some(&etag),
        )
        .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "storage_conflict");
    assert!(current_revision > created["revision"].as_u64().unwrap());
}

#[tokio::test]
async fn update_without_if_match_returns_428() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, _, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);

    let (status, _, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-renamed" }),
            None,
        )
        .await;

    assert_eq!(status, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(body["error"], "if_match_required");
}

#[tokio::test]
async fn enable_disable_persists_and_audits() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, headers, body) = server
        .client
        .post_with_if_match(&format!("/admin/v1/principals/{id}/disable"), &etag)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["enabled"], false);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, _, body) = server
        .client
        .post_with_if_match(&format!("/admin/v1/principals/{id}/enable"), &etag)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["enabled"], true);
    assert!(body["revision"].as_u64().unwrap() > 0);

    let entries = server
        .storage
        .query_recent_audit(AuditQueryScope::Principal(&id), 0, u64::MAX, 20, false)
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .filter_map(|entry| entry.admin_action.as_deref())
            .any(|action| action.contains("principal_update") && action.contains("enabled"))
    );
}

#[tokio::test]
async fn delete_principal_cascades_owned_plugin_chains() {
    let server = admin_test_common::spawn_admin_server().await;
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let principal_route = format!("/admin/v1/principals/{id}");
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();
    let principal_id = id.parse().unwrap();
    let (registry, _) = server
        .storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [7; 32],
                bytes: b"wasm".to_vec(),
                size_bytes: 4,
            },
            WasmRegistryEntryInput {
                name: "filter-plugin".to_owned(),
                version: None,
                original_filename: "filter.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1,
                uploaded_by_admin_id: principal_id,
                description: "filter plugin".to_owned(),
                usage: "test fixture".to_owned(),
                hook_metadata: Default::default(),
                supported_slots: Vec::new(),
            },
        )
        .await
        .unwrap();
    server
        .storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlotKind::Router,
            order: 1000,
            wasm_registry_id: registry.id,
            config: json!({}),
        })
        .await
        .unwrap();

    let (status, _, body) = server.client.delete(&principal_route, &etag).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, Value::Null);
    assert!(
        server
            .storage
            .list_chain_for_principal(principal_id, PluginSlotKind::Router)
            .await
            .unwrap()
            .is_empty()
    );

    let (status, _, body) = server.client.get(&principal_route).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown_principal");

    let entries = server
        .storage
        .query_recent_audit(AuditQueryScope::Principal(&id), 0, u64::MAX, 20, false)
        .await
        .unwrap();
    assert!(entries.iter().any(|entry| {
        entry.route == principal_route
            && entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.starts_with("principal_delete"))
    }));
}

#[tokio::test]
async fn recreate_after_delete_returns_created_then_name_conflict() {
    let server = admin_test_common::spawn_admin_server().await;
    let (status, headers, first) = create_principal(&server.client, "primary").await;
    assert_eq!(status, StatusCode::CREATED);
    let first_id = created_id(&first);
    let first_etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, _, body) = server
        .client
        .delete(&format!("/admin/v1/principals/{first_id}"), &first_etag)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, Value::Null);

    let (status, _, body) = server.client.get("/admin/v1/principals").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["principals"]
            .as_array()
            .unwrap()
            .iter()
            .all(|principal| principal["id"] != first_id)
    );

    let (status, _, second) = create_principal(&server.client, "primary").await;
    assert_eq!(status, StatusCode::CREATED);
    let second_id = created_id(&second);
    assert_ne!(second_id, first_id);

    let (status, _, conflict) = create_principal(&server.client, "primary").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"], "principal_name_conflict");
    assert_eq!(conflict["existing_principal_id"], second_id);
}

#[tokio::test]
async fn concurrent_same_name_create_has_one_winner() {
    let server = admin_test_common::spawn_admin_server().await;
    let (left, right) = tokio::join!(
        create_principal(&server.client, "concurrent-name"),
        create_principal(&server.client, "concurrent-name")
    );
    let (created, conflict) = if left.0 == StatusCode::CREATED {
        (&left, &right)
    } else {
        (&right, &left)
    };

    assert_eq!(created.0, StatusCode::CREATED);
    assert_eq!(conflict.0, StatusCode::CONFLICT);
    assert_eq!(conflict.2["error"], "principal_name_conflict");
    assert_eq!(conflict.2["existing_principal_id"], created.2["id"]);
}

async fn create_principal(
    client: &admin_test_common::AdminClient,
    name: &str,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": name, "kind": "machine", "allowed_models": [], "default_limits": [] }),
        )
        .await
}

fn created_id(body: &Value) -> String {
    body["id"].as_str().unwrap().to_owned()
}
