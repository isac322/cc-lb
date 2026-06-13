use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_plugin_api::TerminalStrategy;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::{
    PluginChainEntry, PluginChainEntryInput, PluginRegistryStore, PluginSlot, PrincipalCreate,
    PrincipalKind, PrincipalStore, PrincipalUpdate, WasmBlob, WasmRegistryEntry,
    WasmRegistryEntryInput,
};
use cc_lb_storage_redb::Storage;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn storage_fixture(seed: u8) -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        Storage::open(&dir.path().join("dynamic-view-pipeline.redb"), [seed; 32]).expect("storage"),
    );
    (dir, storage)
}

fn stores(storage: Arc<Storage>) -> Stores {
    Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage,
        audit: None,
        plugin_registry_repo: None,
    }
}

async fn create_principal(storage: &Storage, name: &str) -> cc_lb_storage_api::PrincipalRecord {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
        },
        1,
    )
    .await
    .expect("principal created")
}

async fn set_terminal_strategy(
    storage: &Storage,
    principal: &cc_lb_storage_api::PrincipalRecord,
    terminal: TerminalStrategy,
) -> cc_lb_storage_api::PrincipalRecord {
    PrincipalStore::update(
        storage,
        principal.id,
        principal.revision,
        PrincipalUpdate {
            router_terminal_strategy: Some(terminal),
            ..PrincipalUpdate::default()
        },
        2,
    )
    .await
    .expect("principal update succeeds")
    .expect("principal exists")
}

async fn build_view(
    stores: &Stores,
    runtime: &ExtismRuntime,
    data_dir: &std::path::Path,
) -> Arc<cc_lb_core::DynamicView> {
    build_dynamic_view(
        stores,
        &AnthropicOAuthConfig::default(),
        Arc::new(AeadService::from_master_key([1; 32])),
        None,
        0,
        runtime,
        data_dir,
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
    )
    .await
    .expect("dynamic view builds")
}

async fn register_plugin(storage: &Storage, name: &str, wat: impl AsRef<str>) -> WasmRegistryEntry {
    let wat = wat.as_ref();
    let bytes = wat::parse_str(wat).expect("wat parses");
    let sha256 = sha256(&bytes);
    let blob = WasmBlob {
        sha256,
        size_bytes: bytes.len() as u64,
        bytes,
        parse_validated_at_unix_secs: 1,
    };
    let input = WasmRegistryEntryInput {
        name: name.to_owned(),
        original_filename: format!("{name}.wasm"),
        label: None,
        uploaded_at_unix_secs: 1,
        uploaded_by_admin_id: Uuid::new_v4(),
        supported_slots: Vec::new(),
    };
    storage
        .persist_wasm_upload(blob, input)
        .await
        .expect("wasm persisted")
        .0
}

async fn insert_router_entry(
    storage: &Storage,
    principal_id: Uuid,
    plugin: &WasmRegistryEntry,
    order: i64,
) -> PluginChainEntry {
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlot::Router,
            order,
            wasm_registry_id: plugin.id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            wire_version: Some(3),
        })
        .await
        .expect("router chain entry inserted")
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut out = [0_u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn filter_wat(name: &str) -> String {
    format!(
        r#"(module
            (func $filter (result i32) (i32.const 0))
            (export "filter" (func $filter))
            (export "marker-{name}" (func $filter)))"#
    )
}

fn observe_only_wat() -> &'static str {
    r#"(module (func (export "observe") (result i32) (i32.const 0)))"#
}

#[tokio::test]
async fn router_pipeline_instantiates_ordered_filters_and_terminal() {
    let (dir, storage) = storage_fixture(31);
    let stores = stores(storage.clone());
    let principal = create_principal(&storage, "principal-a").await;
    let principal = set_terminal_strategy(&storage, &principal, TerminalStrategy::Random).await;
    let late = register_plugin(&storage, "late-filter", filter_wat("late-filter")).await;
    let early = register_plugin(&storage, "early-filter", filter_wat("early-filter")).await;
    let late_entry = insert_router_entry(&storage, principal.id, &late, 200).await;
    let early_entry = insert_router_entry(&storage, principal.id, &early, 100).await;
    let runtime = ExtismRuntime::new();

    let view = build_view(&stores, &runtime, dir.path()).await;

    let spec = view
        .principal_view
        .get("principal-a")
        .expect("principal is cached");
    let pipeline = spec.resolved_pipeline(None);
    assert_eq!(pipeline.terminal, TerminalStrategy::Random);
    assert!(pipeline.instantiation_error.is_none());
    assert_eq!(pipeline.user_filters.len(), 2);
    assert_eq!(pipeline.user_filters[0].plugin_name(), "early-filter");
    assert_eq!(pipeline.user_filters[0].plugin_id(), early_entry.id);
    assert_eq!(pipeline.user_filters[1].plugin_name(), "late-filter");
    assert_eq!(pipeline.user_filters[1].plugin_id(), late_entry.id);

    let mut keys = runtime.registered_slot_keys();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            ("principal-a".to_owned(), "early-filter".to_owned()),
            ("principal-a".to_owned(), "late-filter".to_owned()),
        ]
    );
}

#[tokio::test]
async fn router_pipeline_instantiation_failure_sets_error_without_committing_slots() {
    let (dir, storage) = storage_fixture(32);
    let stores = stores(storage.clone());
    let principal = create_principal(&storage, "principal-a").await;
    let plugin = register_plugin(&storage, "not-a-filter", observe_only_wat()).await;
    insert_router_entry(&storage, principal.id, &plugin, 100).await;
    let runtime = ExtismRuntime::new();

    let view = build_view(&stores, &runtime, dir.path()).await;

    let spec = view
        .principal_view
        .get("principal-a")
        .expect("principal is cached");
    let pipeline = spec.resolved_pipeline(None);
    let error = pipeline
        .instantiation_error
        .as_deref()
        .expect("pipeline fails closed");
    assert!(
        error.contains("router pipeline instantiation failed"),
        "unexpected error: {error}"
    );
    assert!(pipeline.user_filters.is_empty());
    assert!(runtime.registered_slot_keys().is_empty());
}

#[tokio::test]
async fn router_pipeline_depth_above_sixteen_fails_closed() {
    let (dir, storage) = storage_fixture(33);
    let stores = stores(storage.clone());
    let principal = create_principal(&storage, "principal-a").await;
    for index in 0..17 {
        let plugin = register_plugin(
            &storage,
            &format!("filter-{index:02}"),
            filter_wat(&format!("filter-{index:02}")),
        )
        .await;
        insert_router_entry(&storage, principal.id, &plugin, i64::from(index)).await;
    }
    let runtime = ExtismRuntime::new();

    let view = build_view(&stores, &runtime, dir.path()).await;

    let spec = view
        .principal_view
        .get("principal-a")
        .expect("principal is cached");
    let pipeline = spec.resolved_pipeline(None);
    let error = pipeline
        .instantiation_error
        .as_deref()
        .expect("pipeline fails closed");
    assert!(
        error.contains("exceeds maximum 16"),
        "unexpected error: {error}"
    );
    assert!(pipeline.user_filters.is_empty());
    assert!(runtime.registered_slot_keys().is_empty());
}
