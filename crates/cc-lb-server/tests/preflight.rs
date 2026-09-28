use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::preflight::{self, PreflightReport};
use cc_lb_storage_api::{
    MetaStore, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
    PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamStore, WasmBlob, WasmRegistryEntry,
    WasmRegistryEntryInput,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn empty_db_report() {
    let fixture = Fixture::new().await;

    let report = run_preflight(&fixture).await;

    assert_eq!(report.upstream_count, 0);
    assert_eq!(report.upstream_warnings, 0);
    assert_eq!(report.principal_count, 0);
    assert_eq!(report.principal_disabled_count, 0);
    assert_eq!(report.plugin_chain_entry_count, 0);
    assert_eq!(report.plugin_blob_missing_count, 0);
    assert!(report.warnings.is_empty(), "warnings={:?}", report.warnings);
}

#[tokio::test]
async fn partial_state() {
    let fixture = Fixture::new().await;
    seed_upstream(
        &fixture.storage,
        "upstream-a",
        UpstreamKind::AnthropicApiKey,
        None,
    )
    .await;
    seed_upstream(
        &fixture.storage,
        "upstream-b",
        UpstreamKind::AnthropicApiKey,
        None,
    )
    .await;
    seed_upstream(
        &fixture.storage,
        "upstream-c",
        UpstreamKind::AnthropicOauth,
        None,
    )
    .await;
    let enabled = seed_principal(
        &fixture.storage,
        "principal-enabled",
        vec!["claude-*".to_owned()],
    )
    .await;
    let disabled = seed_principal(
        &fixture.storage,
        "principal-disabled",
        vec!["claude-3-*".to_owned(), "claude-4-*".to_owned()],
    )
    .await;
    PrincipalStore::set_enabled(
        fixture.storage.as_ref(),
        disabled.id,
        disabled.revision,
        false,
        1_800_000_001,
    )
    .await
    .unwrap();
    let registries = [
        seed_registry(&fixture.storage, 1, "plugin-a").await,
        seed_registry(&fixture.storage, 2, "plugin-b").await,
        seed_registry(&fixture.storage, 3, "plugin-c").await,
        seed_registry(&fixture.storage, 4, "plugin-d").await,
    ];
    seed_chain(
        &fixture.storage,
        enabled.id,
        PluginSlotKind::Router,
        registries[0].id,
        1000,
    )
    .await;
    seed_chain(
        &fixture.storage,
        enabled.id,
        PluginSlotKind::Shape,
        registries[1].id,
        1000,
    )
    .await;
    seed_chain(
        &fixture.storage,
        disabled.id,
        PluginSlotKind::Router,
        registries[2].id,
        1000,
    )
    .await;
    seed_chain(
        &fixture.storage,
        disabled.id,
        PluginSlotKind::Shape,
        registries[3].id,
        1000,
    )
    .await;

    let report = run_preflight(&fixture).await;
    print_report(&report);

    assert_eq!(report.upstream_count, 3);
    assert_eq!(report.upstream_warnings, 0);
    assert_eq!(report.principal_count, 2);
    assert_eq!(report.principal_disabled_count, 1);
    assert_eq!(report.plugin_chain_entry_count, 6);
    assert_eq!(report.plugin_blob_missing_count, 0);
    assert!(report.warnings.is_empty(), "warnings={:?}", report.warnings);
}

struct Fixture {
    _db_dir: TempDir,
    data_dir: TempDir,
    storage: Arc<SqliteStorage>,
    stores: Stores,
}

impl Fixture {
    async fn new() -> Self {
        let db_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite://{}",
            db_dir.path().join("preflight.sqlite").display()
        );
        let storage = open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
        storage.initialize().await.unwrap();
        let storage = Arc::new(storage);
        let upstreams: Arc<dyn UpstreamStore> = storage.clone();
        let principals: Arc<dyn PrincipalStore> = storage.clone();
        let plugin_registry: Arc<dyn PluginRegistryStore> = storage.clone();
        let stores = Stores {
            upstreams,
            principals,
            plugin_registry,
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: storage.clone(),
            organization_metadata: storage.clone(),
            plan_tiers: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: None,
        };
        Self {
            _db_dir: db_dir,
            data_dir,
            storage,
            stores,
        }
    }
}

async fn run_preflight(fixture: &Fixture) -> PreflightReport {
    preflight::run_preflight(&fixture.stores, fixture.data_dir.path())
        .await
        .unwrap()
}

fn print_report(report: &PreflightReport) {
    println!("preflight: ok");
    println!("preflight: upstream_count: {}", report.upstream_count);
    println!("preflight: upstream_warnings: {}", report.upstream_warnings);
    println!("preflight: principal_count: {}", report.principal_count);
    println!(
        "preflight: principal_disabled_count: {}",
        report.principal_disabled_count
    );
    println!(
        "preflight: plugin_chain_entry_count: {}",
        report.plugin_chain_entry_count
    );
    println!(
        "preflight: plugin_blob_missing_count: {}",
        report.plugin_blob_missing_count
    );
    for warning in &report.warnings {
        println!("preflight: warning: {warning}");
    }
}

async fn seed_upstream(
    storage: &SqliteStorage,
    name: &str,
    kind: UpstreamKind,
    base_url: Option<&str>,
) {
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind,
            base_url: base_url.map(|value| value.parse().unwrap()),
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .unwrap();
    if kind == UpstreamKind::AnthropicApiKey {
        let ciphertext = AeadService::from_master_key([0; 32])
            .encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())
            .unwrap();
        UpstreamStore::update_api_key_secret(storage, created.id, Some(ciphertext))
            .await
            .unwrap();
    }
}

async fn seed_principal(
    storage: &SqliteStorage,
    name: &str,
    allowed_models: Vec<String>,
) -> cc_lb_storage_api::PrincipalRecord {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models,
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1_800_000_000,
    )
    .await
    .unwrap()
}

async fn seed_registry(storage: &SqliteStorage, seed: u8, name: &str) -> WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; 4],
                size_bytes: 4,
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
                supported_slots: vec![PluginSlotKind::Router, PluginSlotKind::ObservabilityHook],
            },
        )
        .await
        .unwrap();
    entry
}

async fn seed_chain(
    storage: &SqliteStorage,
    principal_id: Uuid,
    slot: PluginSlotKind,
    wasm_registry_id: Uuid,
    order: i64,
) {
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot,
            order,
            wasm_registry_id,
            config: json!({}),
        })
        .await
        .unwrap();
}
