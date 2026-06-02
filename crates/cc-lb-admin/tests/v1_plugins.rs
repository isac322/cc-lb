mod config_admin_common;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use cc_lb_config::Config;
use cc_lb_storage_api::{
    PluginChainEntryInput, PluginRegistryStore, PluginSlot, PrincipalCreate, PrincipalKind,
    PrincipalStore, WasmBlob, WasmRegistryEntryInput, sparse_order,
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

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "stale_revision");
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
    assert!(storage.get_blob_bytes(entry.sha256).await.unwrap().is_some());
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
        Some(json!({ "slot": "Router", "wasm_registry_id": entry.id })),
    )
    .await;
    let (_, _, second, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({ "slot": "Router", "wasm_registry_id": entry.id, "position": "last" })),
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
    let first = seed_chain(&storage, principal_id, entry.id, 1000).await;
    let second = seed_chain(&storage, principal_id, entry.id, 2000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
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
    seed_chain(&storage, principal_id, entry.id, 2000).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (_, _, inserted, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
            "wasm_registry_id": entry.id,
            "position": "first"
        })),
    )
    .await;

    assert_eq!(inserted["order"], 1000);
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
async fn chain_rebalance_evens_spacing_and_returns_new_orders() {
    let (_dir, storage) = temp_storage();
    let principal_id = seed_principal(&storage, "principal-rebalance").await;
    let entry = seed_registry(&storage, 13, "plugin-rebalance").await;
    seed_chain(&storage, principal_id, entry.id, 1000).await;
    seed_chain(&storage, principal_id, entry.id, 1001).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain/rebalance?slot=Router"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["entries"][0]["order"], 1000);
    assert_eq!(body["entries"][1]["order"], 2000);
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
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlot::Router,
            order,
            wasm_registry_id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
        })
        .await
        .unwrap()
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}
