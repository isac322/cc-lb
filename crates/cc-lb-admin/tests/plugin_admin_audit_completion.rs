//! Plugin admin routes: the label PATCH and WASM GC write attributed audit
//! rows, an audit-write failure after a committed plugin change still rebuilds
//! the serving view, and a client disconnect after the commit does not abandon
//! the rest of the mutation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_admin::DynamicViewRebinder;
use cc_lb_config::Config;
use cc_lb_control::{DynamicView, DynamicViewBuilder, DynamicViewHolder};
use cc_lb_storage_api::{
    AuditEntry, AuditQueryScope, AuditStore, PluginChainEntryInput, PluginRegistryStore,
    PluginSlotKind, PrincipalCreate, PrincipalKind, PrincipalStore, WasmBlob,
    WasmRegistryEntryInput,
};
use cc_lb_storage_sqlite::SqliteStorage;
use config_admin_common::{TOKEN, app, temp_storage, test_state};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::sync::{Mutex, oneshot};
use tower::ServiceExt;
use uuid::Uuid;

use crate::config_admin_common;

const AUDIT_WRITE_FAILED_BODY: &[u8] = br#"{"error":"audit_write_failed"}"#;
const GC_ROUTE: &str = "/admin/v1/plugins/wasm/gc";

/// What the rebuild that produced a given view generation read from storage.
#[derive(Clone, Debug, Default, PartialEq)]
struct Observed {
    label: Option<String>,
    chain_configs: Vec<Value>,
}

type Gate = (oneshot::Sender<()>, oneshot::Receiver<()>);

/// Builds each new view from the resident one and records the committed plugin
/// state it saw, keyed by the generation it produced. When armed, one rebuild
/// reports that it started and waits for a release signal.
struct RecordingRebinder {
    storage: Arc<SqliteStorage>,
    holder: Arc<DynamicViewHolder>,
    registry_id: Uuid,
    principal_id: Uuid,
    observed: Mutex<HashMap<u64, Observed>>,
    gate: Mutex<Option<Gate>>,
}

impl RecordingRebinder {
    async fn arm_gate(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.gate.lock().await = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    async fn observed_at(&self, generation: u64) -> Option<Observed> {
        self.observed.lock().await.get(&generation).cloned()
    }
}

#[async_trait]
impl DynamicViewRebinder for RecordingRebinder {
    async fn rebuild_dynamic_view(
        &self,
        _current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let gate = self.gate.lock().await.take();
        if let Some((entered, release)) = gate {
            let _ = entered.send(());
            release.await?;
        }
        let label = self
            .storage
            .get_registry_entry_by_id(self.registry_id)
            .await?
            .and_then(|entry| entry.label);
        let chain_configs = self
            .storage
            .list_chain_for_principal(self.principal_id, PluginSlotKind::Router)
            .await?
            .into_iter()
            // Principal creation also seeds the built-in subscription-preference
            // entry; only the entry under test is observed.
            .filter(|entry| entry.wasm_registry_id == self.registry_id)
            .map(|entry| entry.config)
            .collect();
        let view = DynamicViewBuilder::from_view(&self.holder.load()).build();
        self.observed.lock().await.insert(
            view.generation,
            Observed {
                label,
                chain_configs,
            },
        );
        Ok(view)
    }
}

struct Harness {
    _dir: tempfile::TempDir,
    storage: Arc<SqliteStorage>,
    app: axum::Router,
    holder: Arc<DynamicViewHolder>,
    rebinder: Arc<RecordingRebinder>,
    registry_id: Uuid,
    chain_id: Uuid,
}

async fn harness() -> Harness {
    let (dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "plugin-audit-principal").await;
    let registry = seed_registry(&storage, 61, "plugin-audit").await;
    let chain = storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlotKind::Router,
            order: 1000,
            wasm_registry_id: registry.id,
            config: json!({}),
        })
        .await
        .unwrap();
    let mut state = test_state(Config::default(), Some(storage.clone()));
    let holder = state.dynamic_view.clone();
    let rebinder = Arc::new(RecordingRebinder {
        storage: storage.clone(),
        holder: holder.clone(),
        registry_id: registry.id,
        principal_id,
        observed: Mutex::new(HashMap::new()),
        gate: Mutex::new(None),
    });
    state.dynamic_view_rebinder = Some(rebinder.clone());
    Harness {
        _dir: dir,
        storage,
        app: app(state),
        holder,
        rebinder,
        registry_id: registry.id,
        chain_id: chain.id,
    }
}

fn request(method: &str, uri: &str, body: Option<Value>, if_match: Option<&str>) -> Request<Body> {
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
    builder.body(body).unwrap()
}

async fn send(
    app: &axum::Router,
    request: Request<Body>,
) -> (StatusCode, axum::http::HeaderMap, bytes::Bytes) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, bytes)
}

async fn audit_rows_for(storage: &SqliteStorage, route: &str) -> Vec<AuditEntry> {
    storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 1000, false)
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.route == route)
        .collect()
}

async fn fail_audit_inserts(storage: &SqliteStorage) {
    sqlx::query(
        "CREATE TRIGGER fail_admin_audit BEFORE INSERT ON audit_log_v1 \
         BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END",
    )
    .execute(storage.pool())
    .await
    .unwrap();
}

fn assert_test_token_actor(row: &AuditEntry) {
    assert_eq!(row.actor_authority.as_deref(), Some("static-token"));
    assert_eq!(row.actor_subject.as_deref(), Some("test-static-token"));
    assert!(row.actor.is_some());
    assert!(row.actor_kind.is_some());
}

async fn seed_principal(storage: &SqliteStorage, name: &str) -> Uuid {
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

async fn seed_registry(
    storage: &SqliteStorage,
    seed: u8,
    name: &str,
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
                supported_slots: vec![PluginSlotKind::Router, PluginSlotKind::Shape],
            },
        )
        .await
        .unwrap();
    entry
}

async fn seed_orphan_blob(storage: &SqliteStorage, sha256: [u8; 32]) {
    sqlx::query(
        "INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(sha256.as_slice())
    .bind(b"\0asm\x01\0\0\0".as_slice())
    .bind(8_i64)
    .bind(1_800_000_000_i64)
    .execute(storage.pool())
    .await
    .unwrap();
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test]
async fn registry_label_patch_writes_one_attributed_audit_row() {
    let harness = harness().await;
    let route = format!("/admin/v1/plugins/registry/{}", harness.registry_id);

    let (status, _, bytes) = send(
        &harness.app,
        request(
            "PATCH",
            &route,
            Some(json!({ "label": "Production" })),
            Some("W/\"0\""),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["label"], "Production");

    let rows = audit_rows_for(&harness.storage, &route).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(
        row.admin_action.as_deref(),
        Some(format!("plugin_registry_label_update(id={})", harness.registry_id).as_str())
    );
    assert_eq!(row.status, 200);
    assert_test_token_actor(row);
    assert_eq!(
        row.payload,
        Some(json!({
            "wasm_registry_id": harness.registry_id.to_string(),
            "label": "Production",
        }))
    );

    let (status, _, _) = send(
        &harness.app,
        request(
            "PATCH",
            &route,
            Some(json!({ "label": null })),
            Some("W/\"1\""),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored = harness
        .storage
        .get_registry_entry_by_id(harness.registry_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.label, None);
    let rows = audit_rows_for(&harness.storage, &route).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().any(|row| {
        row.payload
            == Some(json!({
                "wasm_registry_id": harness.registry_id.to_string(),
                "label": null,
            }))
    }));
}

#[tokio::test]
async fn registry_label_patch_audit_failure_returns_500_and_rebuilds_view() {
    let harness = harness().await;
    let route = format!("/admin/v1/plugins/registry/{}", harness.registry_id);
    let generation_before = harness.holder.generation();
    fail_audit_inserts(&harness.storage).await;

    let (status, headers, bytes) = send(
        &harness.app,
        request(
            "PATCH",
            &route,
            Some(json!({ "label": "Production" })),
            Some("W/\"0\""),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(bytes.as_ref(), AUDIT_WRITE_FAILED_BODY);
    assert!(headers.get("etag").is_none());
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());

    let stored = harness
        .storage
        .get_registry_entry_by_id(harness.registry_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.label.as_deref(), Some("Production"));
    assert_eq!(stored.revision, 1);
    assert!(audit_rows_for(&harness.storage, &route).await.is_empty());

    let generation = harness.holder.generation();
    assert!(
        generation > generation_before,
        "serving view was not rebuilt"
    );
    assert_eq!(
        harness
            .rebinder
            .observed_at(generation)
            .await
            .map(|observed| observed.label),
        Some(Some("Production".to_owned()))
    );
}

#[tokio::test]
async fn chain_update_audit_failure_returns_500_and_rebuilds_view_with_committed_chain() {
    let harness = harness().await;
    let route = format!("/admin/v1/plugin-chain-entries/{}", harness.chain_id);
    let generation_before = harness.holder.generation();
    fail_audit_inserts(&harness.storage).await;

    let (status, headers, bytes) = send(
        &harness.app,
        request(
            "PUT",
            &route,
            Some(json!({ "config": { "enabled": true } })),
            Some("W/\"0\""),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(bytes.as_ref(), AUDIT_WRITE_FAILED_BODY);
    assert!(headers.get("etag").is_none());
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());
    assert!(audit_rows_for(&harness.storage, &route).await.is_empty());

    let generation = harness.holder.generation();
    assert!(
        generation > generation_before,
        "serving view was not rebuilt"
    );
    assert_eq!(
        harness
            .rebinder
            .observed_at(generation)
            .await
            .map(|observed| observed.chain_configs),
        Some(vec![json!({ "enabled": true })])
    );
}

#[tokio::test]
async fn chain_update_finishes_audit_and_rebuild_after_client_disconnect() {
    let harness = harness().await;
    let route = format!("/admin/v1/plugin-chain-entries/{}", harness.chain_id);
    let generation_before = harness.holder.generation();
    let (entered, release) = harness.rebinder.arm_gate().await;

    let mut in_flight = Box::pin(harness.app.clone().oneshot(request(
        "PUT",
        &route,
        Some(json!({ "config": { "enabled": true } })),
        Some("W/\"0\""),
    )));
    tokio::select! {
        _ = &mut in_flight => panic!("request finished while its rebuild was gated"),
        result = entered => result.unwrap(),
    }
    // The client disconnects: the request future is dropped mid-rebuild.
    drop(in_flight);
    // On a cancelled continuation the gate receiver is gone; ignore that here
    // and let the generation assertion below report it.
    let _ = release.send(());
    // The rebuild holds the holder's rebuild lock until it installs its view.
    drop(harness.holder.lock_rebuild().await);

    let generation = harness.holder.generation();
    assert!(
        generation > generation_before,
        "rebuild was abandoned with the request"
    );
    assert_eq!(
        harness
            .rebinder
            .observed_at(generation)
            .await
            .map(|observed| observed.chain_configs),
        Some(vec![json!({ "enabled": true })])
    );
    let rows = audit_rows_for(&harness.storage, &route).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(
        rows[0]
            .admin_action
            .as_deref()
            .is_some_and(|action| action.starts_with("plugin_chain_update("))
    );
}

#[tokio::test]
async fn wasm_gc_writes_one_audit_row_listing_removed_blobs() {
    let (dir, storage) = temp_storage().await;
    let orphan_sha = [0x5a_u8; 32];
    seed_orphan_blob(&storage, orphan_sha).await;
    let data_dir = dir.path().join("data");
    let cache_dir = data_dir.join("plugins/wasm/cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let cache_path = cache_dir.join(format!("{}.wasm", hex_sha256(orphan_sha)));
    std::fs::write(&cache_path, b"cached wasm").unwrap();
    let mut config = Config::default();
    config.runtime.data_dir = Some(data_dir);
    let app = app(test_state(config, Some(storage.clone())));

    let (status, _, bytes) = send(&app, request("POST", GC_ROUTE, None, None)).await;

    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body,
        json!({ "removed": [hex_sha256(orphan_sha)], "count": 1 })
    );
    assert!(storage.get_blob_bytes(orphan_sha).await.unwrap().is_none());
    assert!(!cache_path.exists());

    let rows = audit_rows_for(&storage, GC_ROUTE).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(
        row.admin_action.as_deref(),
        Some("plugin_wasm_gc(removed=1)")
    );
    assert_eq!(row.status, 200);
    assert_test_token_actor(row);
    assert_eq!(
        row.payload,
        Some(json!({ "removed": [hex_sha256(orphan_sha)] }))
    );
}

#[tokio::test]
async fn wasm_gc_removes_every_orphan_and_audits_them_once() {
    let (dir, storage) = temp_storage().await;
    let first = [0x5c_u8; 32];
    let second = [0x5d_u8; 32];
    seed_orphan_blob(&storage, first).await;
    seed_orphan_blob(&storage, second).await;
    let mut config = Config::default();
    config.runtime.data_dir = Some(dir.path().join("data"));
    let app = app(test_state(config, Some(storage.clone())));

    let (status, _, bytes) = send(&app, request("POST", GC_ROUTE, None, None)).await;

    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["count"], 2);
    let mut reported: Vec<String> = body["removed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sha| sha.as_str().unwrap().to_owned())
        .collect();
    reported.sort();
    assert_eq!(reported, vec![hex_sha256(first), hex_sha256(second)]);
    assert!(storage.get_blob_bytes(first).await.unwrap().is_none());
    assert!(storage.get_blob_bytes(second).await.unwrap().is_none());

    let rows = audit_rows_for(&storage, GC_ROUTE).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        rows[0].admin_action.as_deref(),
        Some("plugin_wasm_gc(removed=2)")
    );
    assert_eq!(rows[0].payload, Some(json!({ "removed": body["removed"] })));
}

#[tokio::test]
async fn wasm_gc_without_orphans_still_writes_audit_row() {
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state(Config::default(), Some(storage.clone())));

    let (status, _, bytes) = send(&app, request("POST", GC_ROUTE, None, None)).await;

    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body, json!({ "removed": [], "count": 0 }));

    let rows = audit_rows_for(&storage, GC_ROUTE).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(
        row.admin_action.as_deref(),
        Some("plugin_wasm_gc(removed=0)")
    );
    assert_eq!(row.status, 200);
    assert_test_token_actor(row);
    assert_eq!(row.payload, Some(json!({ "removed": [] })));
}

#[tokio::test]
async fn wasm_gc_audit_failure_returns_500_after_cleanup() {
    let (dir, storage) = temp_storage().await;
    let orphan_sha = [0x5b_u8; 32];
    seed_orphan_blob(&storage, orphan_sha).await;
    let mut config = Config::default();
    config.runtime.data_dir = Some(dir.path().join("data"));
    let app = app(test_state(config, Some(storage.clone())));
    fail_audit_inserts(&storage).await;

    let (status, _, bytes) = send(&app, request("POST", GC_ROUTE, None, None)).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(bytes.as_ref(), AUDIT_WRITE_FAILED_BODY);
    assert!(storage.get_blob_bytes(orphan_sha).await.unwrap().is_none());
    assert!(audit_rows_for(&storage, GC_ROUTE).await.is_empty());
}
