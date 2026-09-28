use crate::config_admin_common;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use cc_lb_config::Config;
use cc_lb_storage_api::{
    AuditStore, BUILTIN_SUBSCRIPTION_PREFERENCE_ID, PluginChainEntryInput, PluginRegistryStore,
    PluginSlotKind, PrincipalCreate, PrincipalKind, PrincipalStore, WasmBlob,
    WasmRegistryEntryInput, sparse_order,
};
use config_admin_common::{TOKEN, app, authed_json, temp_storage, test_state};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn registry_list_paginates() {
    let (_dir, storage) = temp_storage().await;
    seed_registry(&storage, 1, "plugin-page-a").await;
    let _second = seed_registry(&storage, 3, "plugin-page-b").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, headers, body) =
        request_json(app, "GET", "/admin/v1/plugins/registry?limit=1", None, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-total-count").unwrap(), "3");
    assert_eq!(body["entries"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn registry_list_exposes_builtin_subscription_preference() {
    let (_dir, storage) = temp_storage().await;
    let uploaded = seed_registry(&storage, 24, "plugin-metadata-null").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) =
        request_json(app, "GET", "/admin/v1/plugins/registry", None, None).await;

    assert_eq!(status, StatusCode::OK);
    let subscription_preference = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string())
        .expect("builtin subscription-preference entry is listed");
    assert_eq!(subscription_preference["name"], "subscription-preference");
    assert_eq!(subscription_preference["kind"], "filter");
    assert!(
        subscription_preference
            .get("wire_version")
            .is_none_or(Value::is_null)
    );
    assert_eq!(subscription_preference["is_builtin"], true);
    let uploaded_entry = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == uploaded.id.to_string())
        .expect("uploaded plugin entry is listed");
    assert!(uploaded_entry.get("metadata").is_none_or(Value::is_null));
}

#[tokio::test]
async fn builtin_registry_update_and_delete_return_409() {
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (patch_status, _, patch_body) = request_json(
        app.clone(),
        "PATCH",
        &format!("/admin/v1/plugins/registry/{BUILTIN_SUBSCRIPTION_PREFERENCE_ID}"),
        Some(json!({ "label": "nope" })),
        Some("W/\"0\""),
    )
    .await;
    let (delete_status, _, delete_body) = request_json(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{BUILTIN_SUBSCRIPTION_PREFERENCE_ID}"),
        None,
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(patch_status, StatusCode::CONFLICT);
    assert_eq!(patch_body["error"], "builtin_plugin_immutable");
    assert_eq!(delete_status, StatusCode::CONFLICT);
    assert_eq!(delete_body["error"], "builtin_plugin_immutable");
}

#[tokio::test]
async fn registry_get_returns_etag_with_revision() {
    let (_dir, storage) = temp_storage().await;
    let entry = seed_registry(&storage, 3, "plugin-get").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, headers, body) = request_json(
        app,
        "GET",
        &format!("/admin/v1/plugins/registry/{}", entry.id),
        None,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("etag").unwrap(), "W/\"0\"");
    assert_eq!(body["revision"], 0);
}

#[tokio::test]
async fn registry_patch_label_with_if_match_bumps_revision() {
    let (_dir, storage) = temp_storage().await;
    let entry = seed_registry(&storage, 4, "plugin-label").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, headers, body) = request_json(
        app,
        "PATCH",
        &format!("/admin/v1/plugins/registry/{}", entry.id),
        Some(json!({ "label": "Production" })),
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("etag").unwrap(), "W/\"1\"");
    assert_eq!(body["label"], "Production");
    assert_eq!(body["revision"], 1);
}

#[tokio::test]
async fn registry_patch_label_stale_if_match_returns_409() {
    let (_dir, storage) = temp_storage().await;
    let entry = seed_registry(&storage, 5, "plugin-label-stale").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "PATCH",
        &format!("/admin/v1/plugins/registry/{}", entry.id),
        Some(json!({ "label": "Stale" })),
        Some("W/\"99\""),
    )
    .await;

    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(body, json!({ "error": "stale_revision", "current": 0 }));
}

#[tokio::test]
async fn registry_delete_when_unreferenced_deletes_blob_and_cache_file() {
    let (dir, storage) = temp_storage().await;
    let entry = seed_registry(&storage, 6, "plugin-delete-registry").await;
    let data_dir = dir.path().join("data");
    let cache_dir = data_dir.join("plugins/wasm/cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let cache_path = cache_dir.join(format!("{}.wasm", hex_sha256(entry.sha256)));
    std::fs::write(&cache_path, b"cached wasm").unwrap();
    let mut config = Config::default();
    config.runtime.data_dir = Some(data_dir);
    let app = app(test_state(config, Some(storage.clone())));

    let (status, _, _) = request_bytes(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{}", entry.id),
        None,
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        storage
            .get_blob_bytes(entry.sha256)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!cache_path.exists());
}

#[tokio::test]
async fn registry_delete_cascade_blocks_when_chain_references_it() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-cascade").await;
    let entry = seed_registry(&storage, 7, "plugin-cascade").await;
    let chain_entry = seed_chain(&storage, principal_id, entry.id, sparse_order::STEP).await;
    let app = app(test_state(Config::default(), Some(storage.clone())));

    let (status, _, body) = request_json(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{}", entry.id),
        None,
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "plugin_registry_referenced");
    assert_eq!(body["id"], chain_entry.id.to_string());
    assert!(
        storage
            .get_blob_bytes(entry.sha256)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn registry_references_endpoint_lists_chain_and_cascade_delete_removes_it() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-reference-preview").await;
    let entry = seed_registry(&storage, 37, "plugin-reference-preview").await;
    let chain_entry = seed_chain(&storage, principal_id, entry.id, sparse_order::STEP).await;
    let app = app(test_state(Config::default(), Some(storage.clone())));

    let (status, _, preview) = request_json(
        app.clone(),
        "GET",
        &format!("/admin/v1/plugins/registry/{}/references", entry.id),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body={preview}");
    assert_eq!(preview["refcount"], 1);
    assert_eq!(preview["references"].as_array().unwrap().len(), 1);
    assert_eq!(
        preview["references"][0]["chain_entry_id"],
        chain_entry.id.to_string()
    );

    let fingerprint = preview["reference_fingerprint"]
        .as_str()
        .expect("fingerprint string");
    let (status, _, deleted) = request_json_with_reference_fingerprint(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{}?cascade=references", entry.id),
        Some("W/\"0\""),
        fingerprint,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body={deleted}");
    assert_eq!(deleted["deleted"]["id"], entry.id.to_string());
    assert_eq!(deleted["removed_references"].as_array().unwrap().len(), 1);
    assert!(
        storage
            .list_chain_for_principal(principal_id, PluginSlotKind::Router)
            .await
            .unwrap()
            .iter()
            .all(|chain| chain.wasm_registry_id != entry.id)
    );
    assert!(
        storage
            .get_blob_bytes(entry.sha256)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn registry_cascade_delete_rejects_stale_reference_fingerprint() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-stale-fingerprint").await;
    let entry = seed_registry(&storage, 38, "plugin-stale-fingerprint").await;
    seed_chain(&storage, principal_id, entry.id, sparse_order::STEP).await;
    let app = app(test_state(Config::default(), Some(storage.clone())));

    let (status, _, body) = request_json_with_reference_fingerprint(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{}?cascade=references", entry.id),
        Some("W/\"0\""),
        "0000000000000000000000000000000000000000000000000000000000000000",
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "body={body}");
    assert_eq!(body["error"], "references_changed");
    assert!(
        storage
            .list_chain_for_principal(principal_id, PluginSlotKind::Router)
            .await
            .unwrap()
            .iter()
            .any(|chain| chain.wasm_registry_id == entry.id)
    );
    assert!(
        storage
            .get_blob_bytes(entry.sha256)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn registry_delete_stale_if_match_returns_412_with_current_revision() {
    let (_dir, storage) = temp_storage().await;
    let entry = seed_registry(&storage, 14, "plugin-delete-stale").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{}", entry.id),
        None,
        Some("W/\"99\""),
    )
    .await;

    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(body, json!({ "error": "stale_revision", "current": 0 }));
}

#[tokio::test]
async fn chain_insert_position_last_uses_next_after() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-last").await;
    let entry =
        seed_registry_with_slots(&storage, 8, "plugin-last", vec![PluginSlotKind::Router]).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, first, _) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "router", "wasm_registry_id": entry.id })),
    )
    .await;
    let (_, _, second, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "router", "wasm_registry_id": entry.id, "position": "last" })),
    )
    .await;

    assert_eq!(first["order"], sparse_order::STEP);
    assert_eq!(second["order"], sparse_order::STEP * 2);
}

#[tokio::test]
async fn chain_insert_position_before_uses_sparse_between() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-before").await;
    let entry = seed_registry(&storage, 9, "plugin-before").await;
    let first = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let second = seed_chain(&storage, principal_id, entry.id, 2000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "router",
            "wasm_registry_id": entry.id,
            "position": { "before": second.id }
        })),
    )
    .await;

    assert_eq!(first.order, 1000);
    assert_eq!(inserted["order"], 1500);
}

#[tokio::test]
async fn chain_insert_position_before_seeded_builtin_uses_sparse_order() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-before-seeded-builtin").await;
    let entry = seed_registry(&storage, 79, "plugin-before-seeded-builtin").await;
    let builtin = storage
        .list_chain_for_principal(principal_id, PluginSlotKind::Router)
        .await
        .unwrap()
        .into_iter()
        .find(|chain| chain.wasm_registry_id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .unwrap();
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "router",
            "wasm_registry_id": entry.id,
            "position": { "before": builtin.id }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(inserted["order"], -sparse_order::STEP);
}

#[tokio::test]
async fn chain_insert_position_first_uses_min_minus_step() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-first").await;
    let entry = seed_registry(&storage, 10, "plugin-first").await;
    seed_chain(&storage, principal_id, entry.id, 2000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "router",
            "wasm_registry_id": entry.id,
            "position": "first"
        })),
    )
    .await;

    assert_eq!(inserted["order"], -sparse_order::STEP);
}

#[tokio::test]
async fn chain_insert_accepts_builtin_subscription_preference_registry_id() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-builtin-subscription-preference").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "router",
            "wasm_registry_id": BUILTIN_SUBSCRIPTION_PREFERENCE_ID
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["slot"], "router");
    assert_eq!(
        body["wasm_registry_id"],
        BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string()
    );
    assert!(body.get("wire_version").is_none_or(Value::is_null));

    let (status, _, chain, _) = authed_json(
        app,
        "GET",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain?slot=router"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = chain["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|entry| entry["id"] == body["id"]
        && entry["wasm_registry_id"] == BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string()));
}

#[tokio::test]
async fn plugin_chain_rejects_non_canonical_slot_names() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-slot-aliases").await;
    let app = app(test_state(Config::default(), Some(storage)));

    for slot in ["Router", "filter", "observe", "sign", "build_signer"] {
        let (status, _, _) = request_bytes(
            app.clone(),
            "GET",
            &format!("/admin/v1/principals/{principal_id}/plugin-chain?slot={slot}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "slot={slot}");
    }
}

#[tokio::test]
async fn chain_insert_unknown_principal_returns_400() {
    let (_dir, storage) = temp_storage().await;
    let entry = seed_registry(&storage, 19, "plugin-unknown-principal").await;
    let unknown_principal = Uuid::new_v4();
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{unknown_principal}/plugin-chain"),
        Some(json!({ "slot": "router", "wasm_registry_id": entry.id })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unknown_principal");
    assert_eq!(body["id"], unknown_principal.to_string());
}

#[tokio::test]
async fn chain_insert_rejects_plugin_not_advertising_target_slot() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-unsupported-slot").await;
    let shape_only = seed_registry_with_slots(
        &storage,
        77,
        "shape-only-plugin",
        vec![PluginSlotKind::Shape],
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "router", "wasm_registry_id": shape_only.id })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unsupported_slot");
    assert_eq!(body["plugin_name"], "shape-only-plugin");
    assert_eq!(body["slot"], "router");
}

#[tokio::test]
async fn insert_chain_duplicate_router_returns_201_and_lists_both_entries() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-router-singleton").await;
    let entry = seed_registry(&storage, 22, "plugin-router-singleton").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlotKind::Router,
        entry.id,
        sparse_order::STEP,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "router", "wasm_registry_id": entry.id, "position": "last" })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let second_id = body["id"].as_str().unwrap().to_owned();
    assert_ne!(second_id, first.id.to_string());
    assert_eq!(body["slot"], "router");
    assert_eq!(body["order"], sparse_order::STEP * 2);

    let (status, _, chain, _) = authed_json(
        app,
        "GET",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain?slot=router"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = chain["entries"].as_array().unwrap();
    let matching_entries: Vec<_> = entries
        .iter()
        .filter(|chain| chain["wasm_registry_id"] == entry.id.to_string())
        .collect();
    assert_eq!(matching_entries.len(), 2);
    let first_id = first.id.to_string();
    assert!(
        matching_entries
            .iter()
            .any(|chain| chain["id"].as_str() == Some(first_id.as_str()))
    );
    assert!(
        matching_entries
            .iter()
            .any(|chain| chain["id"].as_str() == Some(second_id.as_str()))
    );
}

#[tokio::test]
async fn insert_chain_duplicate_shape_returns_409_slot_singleton() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-shape-singleton").await;
    let entry = seed_registry(&storage, 23, "plugin-shape-singleton").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlotKind::Shape,
        entry.id,
        sparse_order::STEP,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "shape", "wasm_registry_id": entry.id, "position": "last" })),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "slot_singleton");
    assert_eq!(body["existing_entry_id"], first.id.to_string());
}

#[tokio::test]
async fn chain_update_empty_body_rejected_400() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-empty-update").await;
    let entry = seed_registry(&storage, 11, "plugin-empty-update").await;
    let chain = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "PUT",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        Some(json!({})),
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "empty_update");
}

#[tokio::test]
async fn chain_update_stale_if_match_returns_412_with_current_revision() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-update-stale").await;
    let entry = seed_registry(&storage, 18, "plugin-update-stale").await;
    let chain = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "PUT",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        Some(json!({ "config": { "enabled": true } })),
        Some("W/\"99\""),
    )
    .await;

    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(body, json!({ "error": "stale_revision", "current": 0 }));
}

#[tokio::test]
async fn chain_delete_forwards_if_match_to_storage_and_returns_204() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-delete-chain").await;
    let entry = seed_registry(&storage, 15, "plugin-delete-chain").await;
    let chain = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let app = app(test_state(Config::default(), Some(storage.clone())));

    let (status, headers, bytes) = request_bytes(
        app,
        "DELETE",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        None,
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(headers.get("etag").is_none());
    assert!(bytes.is_empty());
    assert!(
        storage
            .list_chain_for_principal(principal_id, PluginSlotKind::Router)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.id != chain.id)
    );
}

#[tokio::test]
async fn chain_delete_stale_if_match_returns_412_with_current_revision() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-delete-stale").await;
    let entry = seed_registry(&storage, 16, "plugin-delete-stale").await;
    let chain = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "DELETE",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        None,
        Some("W/\"99\""),
    )
    .await;

    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(body, json!({ "error": "stale_revision", "current": 0 }));
}

#[tokio::test]
async fn chain_delete_malformed_if_match_returns_400() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-delete-malformed").await;
    let entry = seed_registry(&storage, 17, "plugin-delete-malformed").await;
    let chain = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "DELETE",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        None,
        Some("not-a-revision"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_if_match");
}

#[tokio::test]
async fn chain_reorder_then_needs_rebalance_returns_409() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-reorder").await;
    let entry = seed_registry(&storage, 12, "plugin-reorder").await;
    let first = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let second = seed_chain(&storage, principal_id, entry.id, 2000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain/reorder"),
        Some(json!({ "entries": [
            { "id": first.id, "order": 1000, "expected_revision": first.revision },
            { "id": second.id, "order": 1001, "expected_revision": second.revision }
        ] })),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "needs_rebalance");
}

#[tokio::test]
async fn reorder_invalid_order_after_stage_returns_409() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-reorder-invalid-order").await;
    let entry = seed_registry(&storage, 21, "plugin-reorder-invalid-order").await;
    let first = seed_chain(&storage, principal_id, entry.id, 100).await;
    let second = seed_chain(&storage, principal_id, entry.id, 200).await;
    seed_chain(&storage, principal_id, entry.id, 300).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain/reorder"),
        Some(json!({ "entries": [
            { "id": first.id, "order": 100, "expected_revision": first.revision },
            { "id": second.id, "order": 299, "expected_revision": second.revision }
        ] })),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "invalid_order");
}

#[tokio::test]
async fn chain_rebalance_evens_spacing_and_returns_new_orders() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-rebalance").await;
    let entry = seed_registry(&storage, 13, "plugin-rebalance").await;
    seed_chain(&storage, principal_id, entry.id, 1000).await;
    seed_chain(&storage, principal_id, entry.id, 1001).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain/rebalance?slot=router"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["entries"][0]["order"], 1000);
    assert_eq!(body["entries"][1]["order"], 2000);
}

#[tokio::test]
async fn chain_rebalance_emits_chain_audit() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-rebalance-audit").await;
    let entry = seed_registry(&storage, 20, "plugin-rebalance-audit").await;
    seed_chain(&storage, principal_id, entry.id, 1000).await;
    seed_chain(&storage, principal_id, entry.id, 1001).await;
    let app = app(test_state(Config::default(), Some(storage.clone())));

    let (status, _, _, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain/rebalance?slot=router"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let expected_route = format!("/admin/v1/principals/{principal_id}/plugin-chain/rebalance");
    assert!(
        storage
            .query_audit(None, 0, u64::MAX, 20)
            .await
            .unwrap()
            .iter()
            .any(|entry| {
                entry.route == expected_route
                    && entry.admin_action.as_deref().is_some_and(|action| {
                        action
                            == format!(
                                "plugin_chain_update(principal={principal_id}, slots=router)"
                            )
                    })
            })
    );
}

async fn request_json(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let (status, headers, bytes) = request_bytes(app, method, uri, body, if_match).await;
    let json = serde_json::from_slice(&bytes).unwrap();
    (status, headers, json)
}

async fn request_json_with_reference_fingerprint(
    app: axum::Router,
    method: &str,
    uri: &str,
    if_match: Option<&str>,
    reference_fingerprint: &str,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .header("X-Reference-Fingerprint", reference_fingerprint);
    if let Some(if_match) = if_match {
        builder = builder.header("If-Match", if_match);
    }
    let response = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap();
    (status, headers, json)
}

async fn request_bytes(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> (StatusCode, HeaderMap, bytes::Bytes) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {TOKEN}"));
    if let Some(if_match) = if_match {
        builder = builder.header("If-Match", if_match);
    }
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&value).unwrap())
        }
        None => Body::empty(),
    };
    let response = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, bytes)
}

async fn seed_principal(storage: &cc_lb_storage_sqlite::SqliteStorage, name: &str) -> Uuid {
    storage
        .create(
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1_800_000_000,
        )
        .await
        .unwrap()
        .id
}

async fn seed_registry_with_slots(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    seed: u8,
    name: &str,
    slots: Vec<PluginSlotKind>,
) -> cc_lb_storage_api::WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; seed as usize],
                size_bytes: seed as u64,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                version: None,
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: format!("{name} description"),
                usage: "test fixture".to_owned(),
                hook_metadata: Default::default(),
                supported_slots: slots,
            },
        )
        .await
        .unwrap();
    entry
}

async fn seed_registry(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    seed: u8,
    name: &str,
) -> cc_lb_storage_api::WasmRegistryEntry {
    seed_registry_with_slots(
        storage,
        seed,
        name,
        vec![PluginSlotKind::Router, PluginSlotKind::Shape],
    )
    .await
}

async fn seed_chain(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: Uuid,
    wasm_registry_id: Uuid,
    order: i64,
) -> cc_lb_storage_api::PluginChainEntry {
    seed_chain_with_slot(
        storage,
        principal_id,
        PluginSlotKind::Router,
        wasm_registry_id,
        order,
    )
    .await
}

async fn seed_chain_with_slot(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: Uuid,
    slot: PluginSlotKind,
    wasm_registry_id: Uuid,
    order: i64,
) -> cc_lb_storage_api::PluginChainEntry {
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot,
            order,
            wasm_registry_id,
            config: json!({}),
        })
        .await
        .unwrap()
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}
