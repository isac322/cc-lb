use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_admin::AdminPorts;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_config::Config;
use cc_lb_control::{ApplyStatus, DynamicViewBuilder, UpstreamStatusEntry, UpstreamStatusSnapshot};
use cc_lb_domain::ReplicaIdentity;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate, PrincipalKind,
    PrincipalStore, UpstreamCreate, UpstreamStore, WasmBlob, WasmRegistryEntryInput,
};
use config_admin_common::{app, authed_json, temp_storage, test_state};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn status_reflects_in_memory_dynamic_view_generation_and_replica_id() {
    let (_dir, storage) = temp_storage().await;
    let mut state = test_state(Config::default(), Some(storage.clone()));
    let replica = ReplicaIdentity { id: Uuid::new_v4() };
    state.lifecycle = Some(AdminPorts {
        replica_identity: Some(replica.clone()),
        ..AdminPorts::default()
    });
    bump_dynamic_generation(&state, UpstreamStatusSnapshot::default());

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/v1/status", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 2);
    assert_eq!(body["replica_id"], replica.id.to_string());
}

#[tokio::test]
async fn status_shows_partial_failure_when_upstream_marked_error_in_snapshot() {
    let (_dir, storage) = temp_storage().await;
    let upstream = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "status-error-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .unwrap();
    let state = test_state(Config::default(), Some(storage));
    bump_dynamic_generation(&state, snapshot_with_upstream_error(&upstream.name));

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/v1/status", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["upstreams"][0]["id"], upstream.id.to_string());
    assert_eq!(body["upstreams"][0]["name"], upstream.name);
    assert_eq!(body["upstreams"][0]["status"], "error");
    assert_eq!(body["upstreams"][0]["last_apply_error"], "apply failed");
}

#[tokio::test]
async fn export_contains_no_plaintext_oauth_tokens() {
    let (_dir, storage) = temp_storage().await;
    let upstream = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "oauth-export-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .unwrap();
    storage
        .store_oauth_tokens(
            upstream.id,
            upstream.revision,
            EncryptedOAuthTokens::from_ciphertext(b"plain-alpha plain-renew".to_vec()),
            false,
        )
        .await
        .unwrap();

    let (status, _, body, bytes) = authed_json(
        app(test_state(Config::default(), Some(storage))),
        "GET",
        "/admin/v1/export",
        None,
    )
    .await;

    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["upstreams"][0]["oauth_credentials_present"], true);
    assert!(!text.contains("plain-alpha"));
    assert!(!text.contains("plain-renew"));
}

#[tokio::test]
async fn export_schema_version_field_present_and_equals_1() {
    let (_dir, storage) = temp_storage().await;

    let (status, _, body, _) = authed_json(
        app(test_state(Config::default(), Some(storage))),
        "GET",
        "/admin/v1/export",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["schema_version"], 1);
}

#[tokio::test]
async fn export_round_trips_through_stable_key_ordering() {
    let (_dir, storage) = temp_storage().await;
    seed_upstream(&storage, "bravo").await;
    seed_upstream(&storage, "alpha").await;
    let principal_id = seed_principal(&storage, "principal-alpha").await;
    let registry = seed_registry(&storage, 9, "plugin-alpha").await;
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlotKind::Router,
            order: 1000,
            wasm_registry_id: registry.id,
            config: json!({ "zeta": 1, "alpha": { "zeta": true, "alpha": false } }),
        })
        .await
        .unwrap();

    let (status, _, mut body, _) = authed_json(
        app(test_state(Config::default(), Some(storage))),
        "GET",
        "/admin/v1/export",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    body["exported_at_unix_secs"] = json!(1_800_000_000_u64);
    let first = serde_json::to_string(&body).unwrap();
    let round_tripped: Value = serde_json::from_str(&first).unwrap();
    let second = serde_json::to_string(&round_tripped).unwrap();
    assert_eq!(first, second);
    insta::assert_json_snapshot!(body);
}

fn bump_dynamic_generation(state: &cc_lb_admin::AdminState, snapshot: UpstreamStatusSnapshot) {
    let current = state.dynamic_view.load();
    let next = DynamicViewBuilder::from_view(&current)
        .upstream_status_snapshot(Arc::new(snapshot))
        .build();
    state.dynamic_view.store(next);
}

fn snapshot_with_upstream_error(name: &str) -> UpstreamStatusSnapshot {
    UpstreamStatusSnapshot {
        entries: HashMap::from([(
            name.to_owned(),
            UpstreamStatusEntry {
                status: ApplyStatus::Error,
                last_apply_error: Some("apply failed".to_owned()),
                last_apply_at_unix_secs: 1_800_000_001,
            },
        )]),
        applied_at_unix_secs: 1_800_000_001,
        revision_hash: 0,
    }
}

async fn seed_upstream(storage: &cc_lb_storage_sqlite::SqliteStorage, name: &str) {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .unwrap();
}

async fn seed_principal(storage: &cc_lb_storage_sqlite::SqliteStorage, name: &str) -> Uuid {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["claude-3-*".to_owned()],
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
    storage: &cc_lb_storage_sqlite::SqliteStorage,
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
                schema_hash: None,
                name: name.to_owned(),
                version: None,
                original_filename: format!("{name}.wasm"),
                label: Some("fixture".to_owned()),
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: format!("{name} description"),
                usage: "test fixture".to_owned(),
                hook_metadata: Default::default(),
                supported_slots: Vec::new(),
            },
        )
        .await
        .unwrap();
    entry
}
