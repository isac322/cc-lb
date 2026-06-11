mod config_admin_common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use cc_lb_config::Config;
use cc_lb_core::spawn_audit_writer;
use cc_lb_storage_api::{
    BUILTIN_CACHE_AFFINITY_ID, PluginChainEntryInput, PluginRegistryStore, PluginSlot,
    PrincipalCreate, PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
    sparse_order,
};
use config_admin_common::{TOKEN, app, authed_json, temp_storage, test_state};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn registry_list_paginates() {
    let (_dir, storage) = temp_storage();
    seed_registry(&storage, 1, "plugin-page-a").await;
    let _second = seed_registry(&storage, 2, "plugin-page-b").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, headers, body) =
        request_json(app, "GET", "/admin/v1/plugins/registry?limit=1", None, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-total-count").unwrap(), "2");
    assert_eq!(body["entries"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn registry_list_exposes_builtin_cache_affinity() {
    let (_dir, storage) = temp_storage();
    let uploaded = seed_registry(&storage, 24, "plugin-metadata-null").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) =
        request_json(app, "GET", "/admin/v1/plugins/registry", None, None).await;

    assert_eq!(status, StatusCode::OK);
    let builtin = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == BUILTIN_CACHE_AFFINITY_ID.to_string())
        .expect("builtin cache-affinity entry is listed");
    assert_eq!(builtin["name"], "cache-affinity");
    assert_eq!(builtin["kind"], "filter");
    assert_eq!(builtin["wire_version"], 3);
    assert_eq!(builtin["is_builtin"], true);
    assert_eq!(
        builtin["metadata"]["purpose"],
        "Prefer upstreams whose prompt cache is already warm for this request."
    );
    assert_eq!(
        builtin["metadata"]["keeps"],
        "Candidates with a positive prefill_cache_score (the upstream has already cached the prefix)."
    );
    assert_eq!(
        builtin["metadata"]["drops"],
        "Candidates with zero cache score — only when at least one candidate is a cache hit; otherwise nothing is dropped."
    );
    assert_eq!(
        builtin["metadata"]["empty_behavior"],
        "Never drops everything. Falls back to passing all candidates through when no cache hit exists."
    );
    assert_eq!(
        builtin["metadata"]["examples"],
        json!([
            "5 candidates, 2 with positive cache score → keep the 2 hits.",
            "5 candidates, all with zero cache score → pass all 5 through.",
            "Exactly 1 candidate → no change."
        ])
    );
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
    let (_dir, storage) = temp_storage();
    let app = app(test_state(Config::default(), Some(storage)));

    let (patch_status, _, patch_body) = request_json(
        app.clone(),
        "PATCH",
        &format!("/admin/v1/plugins/registry/{BUILTIN_CACHE_AFFINITY_ID}"),
        Some(json!({ "label": "nope" })),
        Some("W/\"0\""),
    )
    .await;
    let (delete_status, _, delete_body) = request_json(
        app,
        "DELETE",
        &format!("/admin/v1/plugins/registry/{BUILTIN_CACHE_AFFINITY_ID}"),
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
    let (_dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
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
    let (dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-cascade").await;
    let entry = seed_registry(&storage, 7, "plugin-cascade").await;
    seed_chain(&storage, principal_id, entry.id, sparse_order::STEP).await;
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
    assert_eq!(body["id"], entry.id.to_string());
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
    let (_dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-last").await;
    let entry = seed_registry(&storage, 8, "plugin-last").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, first, _) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "ObservabilityHook", "wasm_registry_id": entry.id })),
    )
    .await;
    let (_, _, second, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "ObservabilityHook", "wasm_registry_id": entry.id, "position": "last" })),
    )
    .await;

    assert_eq!(first["order"], sparse_order::STEP);
    assert_eq!(second["order"], sparse_order::STEP * 2);
}

#[tokio::test]
async fn chain_insert_position_before_uses_sparse_between() {
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-before").await;
    let entry = seed_registry(&storage, 9, "plugin-before").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        1000,
    )
    .await;
    let second = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        2000,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "ObservabilityHook",
            "wasm_registry_id": entry.id,
            "position": { "before": second.id }
        })),
    )
    .await;

    assert_eq!(first.order, 1000);
    assert_eq!(inserted["order"], 1500);
}

#[tokio::test]
async fn chain_insert_position_first_uses_min_minus_step() {
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-first").await;
    let entry = seed_registry(&storage, 10, "plugin-first").await;
    seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        2000,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "ObservabilityHook",
            "wasm_registry_id": entry.id,
            "position": "first"
        })),
    )
    .await;

    assert_eq!(inserted["order"], 1000);
}

#[tokio::test]
async fn chain_insert_unknown_principal_returns_400() {
    let (_dir, storage) = temp_storage();
    let entry = seed_registry(&storage, 19, "plugin-unknown-principal").await;
    let unknown_principal = Uuid::new_v4();
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{unknown_principal}/plugin-chain"),
        Some(json!({ "slot": "Router", "wasm_registry_id": entry.id })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unknown_principal");
    assert_eq!(body["id"], unknown_principal.to_string());
}

#[tokio::test]
async fn insert_chain_duplicate_router_returns_201_and_lists_both_entries() {
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-router-singleton").await;
    let entry = seed_registry(&storage, 22, "plugin-router-singleton").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::Router,
        entry.id,
        sparse_order::STEP,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "Router", "wasm_registry_id": entry.id, "position": "last" })),
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
        &format!("/admin/v1/principals/{principal_id}/plugin-chain?slot=Router"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = chain["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    let first_id = first.id.to_string();
    assert!(
        entries
            .iter()
            .any(|entry| entry["id"].as_str() == Some(first_id.as_str()))
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry["id"].as_str() == Some(second_id.as_str()))
    );
}

#[tokio::test]
async fn insert_chain_duplicate_shape_returns_409_slot_singleton() {
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-shape-singleton").await;
    let entry = seed_registry(&storage, 23, "plugin-shape-singleton").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::Shape,
        entry.id,
        sparse_order::STEP,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "Shape", "wasm_registry_id": entry.id, "position": "last" })),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "slot_singleton");
    assert_eq!(body["existing_entry_id"], first.id.to_string());
}

#[tokio::test]
async fn chain_update_empty_body_rejected_400() {
    let (_dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-update-stale").await;
    let entry = seed_registry(&storage, 18, "plugin-update-stale").await;
    let chain = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body) = request_json(
        app,
        "PUT",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        Some(json!({ "sse_per_event": true })),
        Some("W/\"99\""),
    )
    .await;

    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(body, json!({ "error": "stale_revision", "current": 0 }));
}

#[tokio::test]
async fn chain_delete_forwards_if_match_to_storage_and_returns_204() {
    let (_dir, storage) = temp_storage();
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
            .list_chain_for_principal(principal_id, PluginSlot::Router)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn chain_delete_stale_if_match_returns_412_with_current_revision() {
    let (_dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
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
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-reorder").await;
    let entry = seed_registry(&storage, 12, "plugin-reorder").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        1000,
    )
    .await;
    let second = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        2000,
    )
    .await;
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
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-reorder-invalid-order").await;
    let entry = seed_registry(&storage, 21, "plugin-reorder-invalid-order").await;
    let first = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        100,
    )
    .await;
    let second = seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        200,
    )
    .await;
    seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        300,
    )
    .await;
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
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-rebalance").await;
    let entry = seed_registry(&storage, 13, "plugin-rebalance").await;
    seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        1000,
    )
    .await;
    seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        1001,
    )
    .await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!(
            "/admin/v1/principals/{principal_id}/plugin-chain/rebalance?slot=ObservabilityHook"
        ),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["entries"][0]["order"], 1000);
    assert_eq!(body["entries"][1]["order"], 2000);
}

#[tokio::test]
async fn chain_rebalance_emits_chain_audit() {
    let (_dir, storage) = temp_storage();
    let (audit_sink, audit_writer) = spawn_audit_writer(storage.clone(), 64);
    let principal_id = seed_principal(&storage, "principal-rebalance-audit").await;
    let entry = seed_registry(&storage, 20, "plugin-rebalance-audit").await;
    seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        1000,
    )
    .await;
    seed_chain_with_slot(
        &storage,
        principal_id,
        PluginSlot::ObservabilityHook,
        entry.id,
        1001,
    )
    .await;
    let mut state = test_state(Config::default(), Some(storage.clone()));
    state.audit_sink = Some(Arc::new(audit_sink));
    let app = app(state);

    let (status, _, _, _) = authed_json(
        app,
        "POST",
        &format!(
            "/admin/v1/principals/{principal_id}/plugin-chain/rebalance?slot=ObservabilityHook"
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let saw_audit = storage
            .query_audit(None, 0, u64::MAX, 20)
            .unwrap()
            .iter()
            .any(|entry| {
                entry.admin_action.as_deref().is_some_and(|action| {
                    action == format!("plugin_chain_update(principal={principal_id}, slots=observability_hook)")
                })
            });
        if saw_audit {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("plugin_chain_update audit entry not observed within 5s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    audit_writer.abort();
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

async fn seed_principal(storage: &cc_lb_storage_redb::RedbStorage, name: &str) -> Uuid {
    storage
        .create(
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            1_800_000_000,
        )
        .await
        .unwrap()
        .id
}

async fn seed_registry(
    storage: &cc_lb_storage_redb::RedbStorage,
    seed: u8,
    name: &str,
) -> cc_lb_storage_api::WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; seed as usize],
                size_bytes: seed as u64,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
            },
        )
        .await
        .unwrap();
    entry
}

async fn seed_chain(
    storage: &cc_lb_storage_redb::RedbStorage,
    principal_id: Uuid,
    wasm_registry_id: Uuid,
    order: i64,
) -> cc_lb_storage_api::PluginChainEntry {
    seed_chain_with_slot(
        storage,
        principal_id,
        PluginSlot::Router,
        wasm_registry_id,
        order,
    )
    .await
}

async fn seed_chain_with_slot(
    storage: &cc_lb_storage_redb::RedbStorage,
    principal_id: Uuid,
    slot: PluginSlot,
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
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            wire_version: None,
        })
        .await
        .unwrap()
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}
