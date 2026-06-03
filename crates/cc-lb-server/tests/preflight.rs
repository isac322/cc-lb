use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use cc_lb_core::LifecycleConfig;
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::preflight::{self, PreflightReport};
use cc_lb_storage_api::{
    PluginChainEntryInput, PluginRegistryStore, PluginSlot, PrincipalCreate, PrincipalKind,
    PrincipalStore, UpstreamCreate, UpstreamStore, WasmBlob, WasmRegistryEntry,
    WasmRegistryEntryInput,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_redb::Storage;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[tokio::test]
async fn empty_db_report() {
    let fixture = Fixture::new();

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
    let fixture = Fixture::new();
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
        PluginSlot::Router,
        registries[0].id,
        1000,
    )
    .await;
    seed_chain(
        &fixture.storage,
        enabled.id,
        PluginSlot::ObservabilityHook,
        registries[1].id,
        1000,
    )
    .await;
    seed_chain(
        &fixture.storage,
        disabled.id,
        PluginSlot::Router,
        registries[2].id,
        1000,
    )
    .await;
    seed_chain(
        &fixture.storage,
        disabled.id,
        PluginSlot::ObservabilityHook,
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
    assert_eq!(report.plugin_chain_entry_count, 4);
    assert_eq!(report.plugin_blob_missing_count, 0);
    assert!(report.warnings.is_empty(), "warnings={:?}", report.warnings);
}

#[tokio::test]
async fn custom_upstream_probe_success() {
    let fixture = Fixture::new();
    let (base_url, server_task) = spawn_head_ok().await;
    seed_upstream(
        &fixture.storage,
        "custom-ok",
        UpstreamKind::Custom,
        Some(&base_url),
    )
    .await;

    let report = run_preflight(&fixture).await;
    server_task.abort();

    assert_eq!(report.upstream_count, 1);
    assert_eq!(report.upstream_warnings, 0);
    assert!(report.warnings.is_empty(), "warnings={:?}", report.warnings);
}

#[tokio::test]
async fn custom_upstream_probe_network_failure_warning() {
    let fixture = Fixture::new();
    seed_upstream(
        &fixture.storage,
        "custom-fail",
        UpstreamKind::Custom,
        Some("http://does-not-exist.invalid:99"),
    )
    .await;

    let report = run_preflight(&fixture).await;
    print_report(&report);

    assert_eq!(report.upstream_count, 1);
    assert_eq!(report.upstream_warnings, 1);
    assert_eq!(report.warnings.len(), 1);
    assert!(
        report.warnings[0].contains("upstream probe failed"),
        "warnings={:?}",
        report.warnings
    );
}

struct Fixture {
    _db_dir: TempDir,
    data_dir: TempDir,
    storage: Arc<Storage>,
    stores: Stores,
}

impl Fixture {
    fn new() -> Self {
        let db_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let storage =
            Arc::new(Storage::open(&db_dir.path().join("preflight.redb"), [36; 32]).unwrap());
        let upstreams: Arc<dyn UpstreamStore> = storage.clone();
        let principals: Arc<dyn PrincipalStore> = storage.clone();
        let plugin_registry: Arc<dyn PluginRegistryStore> = storage.clone();
        let stores = Stores {
            upstreams,
            principals,
            plugin_registry,
            upstream_rate_limits: storage.clone(),
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
    preflight::run_preflight(
        &fixture.stores,
        &LifecycleConfig::default(),
        fixture.data_dir.path(),
    )
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

async fn seed_upstream(storage: &Storage, name: &str, kind: UpstreamKind, base_url: Option<&str>) {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind,
            base_url: base_url.map(|value| value.parse().unwrap()),
            api_key_ciphertext: Some(vec![1, 2, 3]),
            shape_plugin: None,
        },
    )
    .await
    .unwrap();
}

async fn seed_principal(
    storage: &Storage,
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
        },
        1_800_000_000,
    )
    .await
    .unwrap()
}

async fn seed_registry(storage: &Storage, seed: u8, name: &str) -> WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; 4],
                size_bytes: 4,
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
    storage: &Storage,
    principal_id: Uuid,
    slot: PluginSlot,
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
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
        })
        .await
        .unwrap();
}

async fn spawn_head_ok() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route("/", get(|| async { "ok" }));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), task)
}
