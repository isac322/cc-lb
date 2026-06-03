use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::DynamicViewHolder;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::reconcile::Reconciler;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, CompatibilityKvRecord, PluginChainEntry, PluginChainEntryInput,
    PluginChainEntryUpdate, PluginRegistryStore, PluginSlot, PrincipalCreate, PrincipalKind,
    PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageResult,
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
    UpstreamCreate, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    UpstreamRecord, UpstreamStore, UpstreamSubscriptionQuotaStore, UpstreamUpdate, WasmBlob,
    WasmRegistryEntry, WasmRegistryEntryInput,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_redb::Storage;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn install_prometheus() -> PrometheusHandle {
    static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();
    PROMETHEUS
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("prometheus recorder")
        })
        .clone()
}

fn labeled_counter_value(handle: &PrometheusHandle, name: &str, label: &str, value: &str) -> f64 {
    let metric_prefix = format!("{name}{{");
    let label_fragment = format!(r#"{label}="{value}""#);
    handle
        .render()
        .lines()
        .find(|line| line.starts_with(&metric_prefix) && line.contains(&label_fragment))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn storage_fixture() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage =
        Arc::new(Storage::open(&dir.path().join("test.redb"), [25; 32]).expect("storage"));
    (dir, storage)
}

fn stores(storage: Arc<Storage>) -> Arc<Stores> {
    Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        anthropic_compatibility_kv: storage,
        audit: None,
        plugin_registry_repo: None,
    })
}

async fn create_principal(storage: &Storage, name: &str) -> PrincipalRecord {
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

async fn create_upstream(storage: &Storage, name: &str) -> UpstreamRecord {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![1, 2, 3]),
            shape_plugin: None,
        },
    )
    .await
    .expect("upstream created")
}

async fn seed_registry(storage: &Storage, seed: u8, name: &str) -> WasmRegistryEntry {
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
        .expect("registry entry created");
    entry
}

async fn initial_holder(
    stores: &Stores,
    runtime: &ExtismRuntime,
    data_dir: &std::path::Path,
) -> Arc<DynamicViewHolder> {
    let view = build_dynamic_view(
        stores,
        &AnthropicOAuthConfig::default(),
        Arc::new(AeadService::from_master_key([25; 32])),
        None,
        0,
        runtime,
        data_dir,
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
    )
    .await
    .expect("initial dynamic view");
    Arc::new(DynamicViewHolder::new(view))
}

fn reconciler(
    stores: Arc<Stores>,
    holder: Arc<DynamicViewHolder>,
    runtime: Arc<ExtismRuntime>,
    cancel: CancellationToken,
    data_dir: &std::path::Path,
) -> Arc<Reconciler> {
    Arc::new(Reconciler::new(
        stores,
        holder,
        Arc::new(AnthropicOAuthConfig::default()),
        runtime,
        Arc::new(AeadService::from_master_key([25; 32])),
        None,
        cancel,
        data_dir.to_path_buf(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
    ))
}

async fn advance_until(mut done: impl FnMut() -> bool) {
    tokio::task::yield_now().await;
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn unchanged_tick_emits_unchanged_metric_and_does_not_rebuild() {
    let metrics = install_prometheus();
    let (dir, storage) = storage_fixture();
    create_principal(&storage, "principal-a").await;
    create_upstream(&storage, "upstream-a").await;
    let stores = stores(storage);
    let runtime = Arc::new(ExtismRuntime::new());
    let holder = initial_holder(&stores, &runtime, dir.path()).await;
    let generation = holder.load().generation;
    let before = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "unchanged");
    let cancel = CancellationToken::new();
    let reconciler = reconciler(stores, holder.clone(), runtime, cancel, dir.path());
    reconciler.reconcile_once().await.expect("reconcile tick");

    assert_eq!(holder.load().generation, generation);
    let after = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "unchanged");
    assert!(after > before);
}

#[tokio::test(start_paused = true)]
async fn notify_dropped_then_reconcile_catches_up_within_one_tick() {
    let metrics = install_prometheus();
    let (dir, storage) = storage_fixture();
    create_principal(&storage, "principal-a").await;
    create_upstream(&storage, "upstream-a").await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(ExtismRuntime::new());
    let holder = initial_holder(&stores, &runtime, dir.path()).await;
    let generation = holder.load().generation;
    let before = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "changed");
    create_upstream(&storage, "upstream-b").await;
    let cancel = CancellationToken::new();
    let reconciler = reconciler(stores, holder.clone(), runtime, cancel, dir.path());
    reconciler.reconcile_once().await.expect("reconcile tick");

    assert!(holder.load().generation > generation);
    let after = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "changed");
    assert!(after > before);
}

#[tokio::test(start_paused = true)]
async fn reconciler_rebuilds_after_registry_entry_insert() {
    let (dir, storage) = storage_fixture();
    create_principal(&storage, "principal-registry-insert").await;
    create_upstream(&storage, "upstream-registry-insert").await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(ExtismRuntime::new());
    let holder = initial_holder(&stores, &runtime, dir.path()).await;
    let generation = holder.load().generation;

    seed_registry(&storage, 31, "plugin-registry-insert").await;
    let cancel = CancellationToken::new();
    let reconciler = reconciler(stores, holder.clone(), runtime, cancel, dir.path());
    reconciler.reconcile_once().await.expect("reconcile tick");

    assert!(holder.load().generation > generation);
}

#[tokio::test(start_paused = true)]
async fn reconciler_rebuilds_after_registry_entry_delete() {
    let (dir, storage) = storage_fixture();
    create_principal(&storage, "principal-registry-delete").await;
    create_upstream(&storage, "upstream-registry-delete").await;
    let entry = seed_registry(&storage, 32, "plugin-registry-delete").await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(ExtismRuntime::new());
    let holder = initial_holder(&stores, &runtime, dir.path()).await;
    let generation = holder.load().generation;

    storage
        .delete_registry_entry(entry.id, entry.revision)
        .await
        .expect("registry delete")
        .expect("registry entry deleted");
    let cancel = CancellationToken::new();
    let reconciler = reconciler(stores, holder.clone(), runtime, cancel, dir.path());
    reconciler.reconcile_once().await.expect("reconcile tick");

    assert!(holder.load().generation > generation);
}

#[tokio::test(start_paused = true)]
async fn reconciler_rebuilds_after_registry_entry_update() {
    let (dir, storage) = storage_fixture();
    create_principal(&storage, "principal-registry-update").await;
    create_upstream(&storage, "upstream-registry-update").await;
    let entry = seed_registry(&storage, 33, "plugin-registry-update").await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(ExtismRuntime::new());
    let holder = initial_holder(&stores, &runtime, dir.path()).await;
    let generation = holder.load().generation;

    storage
        .update_registry_label(entry.id, entry.revision, Some("Updated".to_owned()))
        .await
        .expect("registry label update");
    let cancel = CancellationToken::new();
    let reconciler = reconciler(stores, holder.clone(), runtime, cancel, dir.path());
    reconciler.reconcile_once().await.expect("reconcile tick");

    assert!(holder.load().generation > generation);
}

#[tokio::test(start_paused = true)]
async fn cancel_during_tick_is_graceful() {
    let blocking_store = Arc::new(BlockingUpstreamStore::default());
    let blocking_stores = Arc::new(Stores {
        upstreams: blocking_store.clone(),
        principals: Arc::new(EmptyPrincipalStore),
        plugin_registry: Arc::new(EmptyPluginRegistryStore),
        upstream_rate_limits: Arc::new(EmptyRateLimitStore),
        upstream_subscription_quotas: Arc::new(EmptySubscriptionQuotaStore),
        anthropic_compatibility_kv: Arc::new(EmptyCompatibilityKvStore),
        audit: None,
        plugin_registry_repo: None,
    });
    let (_dir, storage) = storage_fixture();
    let runtime = Arc::new(ExtismRuntime::new());
    let real_stores = stores(storage);
    let data_dir = tempfile::tempdir().expect("tempdir");
    let holder = initial_holder(&real_stores, &runtime, data_dir.path()).await;
    let cancel = CancellationToken::new();
    let reconciler = reconciler(
        blocking_stores.clone(),
        holder,
        runtime,
        cancel.clone(),
        data_dir.path(),
    );
    let release = blocking_store.release.clone();
    let task = tokio::spawn(reconciler.run());

    advance_until(|| blocking_store.entered()).await;
    assert!(blocking_store.entered());
    cancel.cancel();
    release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("task exits")
        .expect("task join");
}

#[derive(Default)]
struct BlockingUpstreamStore {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    blocked: AtomicBool,
    entered_flag: AtomicBool,
}

impl BlockingUpstreamStore {
    fn entered(&self) -> bool {
        self.entered_flag.load(Ordering::Acquire)
    }
}

#[async_trait]
impl UpstreamStore for BlockingUpstreamStore {
    async fn create(&self, _create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn get_by_name(&self, _name: &str) -> StorageResult<Option<UpstreamRecord>> {
        unimplemented!()
    }

    async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        unimplemented!()
    }

    async fn list(
        &self,
        _after: Option<Uuid>,
        _limit: usize,
    ) -> StorageResult<Vec<UpstreamRecord>> {
        if !self.blocked.swap(true, Ordering::AcqRel) {
            self.entered_flag.store(true, Ordering::Release);
            self.entered.notify_waiters();
            self.release.notified().await;
        }
        Ok(Vec::new())
    }

    async fn update(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn set_enabled(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn store_oauth_tokens(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn claim_refresh_lease(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _ttl_secs: u64,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn complete_refresh(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn release_lease_on_failure(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _reason: String,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn set_last_apply_error(&self, _id: Uuid, _error: Option<String>) -> StorageResult<()> {
        unimplemented!()
    }

    async fn soft_delete(&self, _id: Uuid, _expected_revision: u64) -> StorageResult<()> {
        unimplemented!()
    }

    async fn hard_delete(&self, _id: Uuid) -> StorageResult<()> {
        unimplemented!()
    }
}

struct EmptyRateLimitStore;

#[async_trait]
impl UpstreamRateLimitStateStore for EmptyRateLimitStore {
    async fn put_observation(
        &self,
        _record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        _upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        Ok(Vec::new())
    }
}

struct EmptySubscriptionQuotaStore;

#[async_trait]
impl UpstreamSubscriptionQuotaStore for EmptySubscriptionQuotaStore {
    async fn put_subscription_quota_batch(
        &self,
        _records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        _upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaObservationRecord>> {
        Ok(Vec::new())
    }

    async fn list_subscription_quota_series(
        &self,
        _query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
        Ok(Vec::new())
    }

    async fn delete_subscription_quota_before(
        &self,
        _cutoff_unix_millis: u64,
        _batch_size: u32,
    ) -> StorageResult<u64> {
        Ok(0)
    }
}

struct EmptyCompatibilityKvStore;

#[async_trait]
impl AnthropicCompatibilityKvStore for EmptyCompatibilityKvStore {
    async fn put_compatibility_kv_value(
        &self,
        _key: &str,
        _value: &str,
        _observed_at_unix_secs: u64,
        _source_url: Option<&str>,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn put_compatibility_kv_failure(
        &self,
        _key: &str,
        _attempted_at_unix_secs: u64,
        _error: &str,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn get_compatibility_kv(
        &self,
        _key: &str,
    ) -> StorageResult<Option<CompatibilityKvRecord>> {
        Ok(None)
    }

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
        Ok(Vec::new())
    }
}

struct EmptyPrincipalStore;

#[async_trait]
impl PrincipalStore for EmptyPrincipalStore {
    async fn create(
        &self,
        _input: PrincipalCreate,
        _now_unix_secs: u64,
    ) -> StorageResult<PrincipalRecord> {
        unimplemented!()
    }
    async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
    async fn get_by_name(&self, _name: &str) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
    async fn list(
        &self,
        _offset: usize,
        _limit: usize,
        _include_deleted: bool,
    ) -> StorageResult<Vec<PrincipalRecord>> {
        Ok(Vec::new())
    }
    async fn update(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: PrincipalUpdate,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
    async fn set_enabled(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _enabled: bool,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
    async fn soft_delete(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
    async fn hard_delete(&self, _id: Uuid) -> StorageResult<bool> {
        unimplemented!()
    }
    async fn set_last_apply_error(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _error: Option<String>,
        _applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
}

struct EmptyPluginRegistryStore;

#[async_trait]
impl PluginRegistryStore for EmptyPluginRegistryStore {
    async fn persist_wasm_upload(
        &self,
        _blob: WasmBlob,
        _entry: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        unimplemented!()
    }
    async fn get_blob_bytes(&self, _sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
        unimplemented!()
    }
    async fn get_blob(
        &self,
        _sha256: [u8; 32],
    ) -> StorageResult<Option<cc_lb_storage_api::WasmBlobRecord>> {
        unimplemented!()
    }
    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
        unimplemented!()
    }
    async fn list_registry(
        &self,
        _after: Option<Uuid>,
        _limit: usize,
    ) -> StorageResult<Vec<WasmRegistryEntry>> {
        Ok(Vec::new())
    }
    async fn get_registry_entry_by_sha(
        &self,
        _sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        unimplemented!()
    }
    async fn get_registry_entry_by_id(
        &self,
        _id: Uuid,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        unimplemented!()
    }
    async fn update_registry_label(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        unimplemented!()
    }
    async fn delete_registry_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        unimplemented!()
    }
    async fn decrement_blob_refcount_or_delete(&self, _sha256: [u8; 32]) -> StorageResult<bool> {
        unimplemented!()
    }
    async fn insert_chain_entry(
        &self,
        _entry: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        unimplemented!()
    }
    async fn list_chain_for_principal(
        &self,
        _principal_id: Uuid,
        _slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        Ok(Vec::new())
    }
    async fn update_chain_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>> {
        unimplemented!()
    }
    async fn reorder_chain(
        &self,
        _principal_id: Uuid,
        _slot: PluginSlot,
        _new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        unimplemented!()
    }
    async fn delete_chain_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>> {
        unimplemented!()
    }
    async fn rebalance_chain(
        &self,
        _principal_id: Uuid,
        _slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        unimplemented!()
    }
}
