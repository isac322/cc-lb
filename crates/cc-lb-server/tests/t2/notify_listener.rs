// tier-allow(silent-skip): storage trait optional-return signatures never skip assertions until=2027-03-31
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_engine::DynamicViewHolder;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::notify_listener::{NotifyListener, NotifyListenerParams};
use cc_lb_storage_api::upstream::UpstreamStatusUpdate;
use cc_lb_storage_api::{
    BackfillApplyOutcome, ChangeChannel, ChangeEvent, MetadataTierMappingOverrideRecord,
    OrganizationMetadataRecord, OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore,
    RuntimeChangeNotifier, StorageError, StorageResult, UpstreamCreate, UpstreamPlanTierRecord,
    UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore, UpstreamRecord, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore, UpstreamUpdate,
};
use cc_lb_testkit::InMemoryStorage as Storage;
use metrics_util::debugging::Snapshotter;
use tokio::sync::{Notify, broadcast, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const INITIAL_GENERATION: u64 = 1;

struct MockNotifier {
    tx: Mutex<Option<broadcast::Sender<ChangeEvent>>>,
    subscribed: Notify,
}

impl MockNotifier {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            tx: Mutex::new(Some(tx)),
            subscribed: Notify::new(),
        }
    }

    fn send(&self, channel: ChangeChannel) {
        let _ = self
            .tx
            .lock()
            .expect("notifier sender lock")
            .as_ref()
            .expect("notifier sender remains open")
            .send(ChangeEvent::new(channel, "test", std::time::UNIX_EPOCH));
    }

    fn close(&self) {
        self.tx.lock().expect("notifier sender lock").take();
    }

    async fn wait_for_subscriber(&self) {
        self.subscribed.notified().await;
    }
}

#[async_trait]
impl RuntimeChangeNotifier for MockNotifier {
    async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
        let receiver = self
            .tx
            .lock()
            .expect("notifier sender lock")
            .as_ref()
            .expect("notifier sender remains open")
            .subscribe();
        self.subscribed.notify_one();
        Ok(receiver)
    }

    async fn run(&self, cancel: CancellationToken) -> StorageResult<()> {
        cancel.cancelled().await;
        Ok(())
    }
}

struct ControlledUpstreamStore {
    inner: Arc<Storage>,
    fail_list: bool,
    list_started: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
    list_release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

impl ControlledUpstreamStore {
    fn failing(inner: Arc<Storage>, list_started: oneshot::Sender<()>) -> Self {
        Self {
            inner,
            fail_list: true,
            list_started: tokio::sync::Mutex::new(Some(list_started)),
            list_release: tokio::sync::Mutex::new(None),
        }
    }

    fn blocking(
        inner: Arc<Storage>,
        list_started: oneshot::Sender<()>,
        list_release: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            inner,
            fail_list: false,
            list_started: tokio::sync::Mutex::new(Some(list_started)),
            list_release: tokio::sync::Mutex::new(Some(list_release)),
        }
    }
}

#[async_trait]
impl UpstreamStore for ControlledUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        UpstreamStore::create(&*self.inner, create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_name(&*self.inner, name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(&*self.inner, id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        if let Some(sender) = self.list_started.lock().await.take() {
            let _ = sender.send(());
        }
        if self.fail_list {
            return Err(StorageError::Unavailable {
                message: "injected list failure".to_owned(),
            });
        }
        if let Some(receiver) = self.list_release.lock().await.take() {
            let _ = receiver.await;
        }
        UpstreamStore::list(&*self.inner, after, limit).await
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update(&*self.inner, id, expected_revision, update).await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::set_enabled(&*self.inner, id, expected_revision, enabled).await
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_spec(&*self.inner, id, expected_revision, update).await
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_api_key_secret(&*self.inner, id, api_key_ciphertext).await
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_oauth_token(&*self.inner, id, tokens).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        UpstreamStore::set_status(&*self.inner, id, status).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::store_oauth_tokens(&*self.inner, id, expected_revision, tokens).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(&*self.inner, id, holder, tokens).await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        UpstreamStore::set_last_apply_error(&*self.inner, id, error).await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        UpstreamStore::soft_delete(&*self.inner, id, expected_revision).await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        UpstreamStore::hard_delete(&*self.inner, id).await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::clear_warmup_dialect_plugin(&*self.inner, id, expected_revision).await
    }
}

struct EmptyDynamicViewStore;

#[async_trait]
impl UpstreamRateLimitStateStore for EmptyDynamicViewStore {
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

#[async_trait]
impl UpstreamSubscriptionMetadataStore for EmptyDynamicViewStore {
    async fn put_upstream_subscription_metadata(
        &self,
        _record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn get_upstream_subscription_metadata(
        &self,
        _upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        Ok(None)
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl OrganizationMetadataStore for EmptyDynamicViewStore {
    async fn put_organization_metadata(
        &self,
        _record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn get_organization_metadata(
        &self,
        _organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        Ok(None)
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl PlanTierStore for EmptyDynamicViewStore {
    async fn upsert_plan_tier_ratio(&self, _record: &PlanTierRatioRecord) -> StorageResult<()> {
        Ok(())
    }

    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        Ok(Vec::new())
    }

    async fn list_plan_tier_ratios_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<PlanTierRatioRecord>> {
        Ok(Vec::new())
    }

    async fn upsert_metadata_tier_override(
        &self,
        _record: &MetadataTierMappingOverrideRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        Ok(Vec::new())
    }

    async fn list_metadata_tier_overrides_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        Ok(Vec::new())
    }

    async fn append_upstream_plan_tier(
        &self,
        _record: &UpstreamPlanTierRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn backfill_upstream_plan_tier_intervals(
        &self,
        _upstream_id: Uuid,
        _intervals: &[UpstreamPlanTierRecord],
        _terminal_cap_unix_millis: i64,
        _provenance: &str,
    ) -> StorageResult<BackfillApplyOutcome> {
        Ok(BackfillApplyOutcome::Skipped)
    }

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        Ok(Vec::new())
    }

    async fn list_upstream_plan_tiers_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        Ok(Vec::new())
    }
}

struct Fixture {
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    oauth: Arc<AnthropicOAuthConfig>,
    aead: Arc<AeadService>,
    runtime: Arc<WasmtimeRuntime>,
    holder: Arc<DynamicViewHolder>,
}

async fn fixture() -> Fixture {
    let storage = Arc::new(Storage::with_clock(cc_lb_testkit::fixed_clock(
        1_800_000_000,
    )));
    let empty = Arc::new(EmptyDynamicViewStore);
    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: empty.clone(),
        upstream_subscription_quotas: storage.clone(),
        upstream_subscription_metadata: empty.clone(),
        organization_metadata: empty.clone(),
        plan_tiers: empty,
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: None,
    });
    let oauth = Arc::new(AnthropicOAuthConfig::default());
    let aead = Arc::new(AeadService::from_master_key([24; 32]));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let initial_generation = INITIAL_GENERATION;
    let initial = build_dynamic_view(
        &stores,
        &oauth,
        aead.clone(),
        None,
        initial_generation - 1,
        &runtime,
        Path::new("."),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        None,
        None,
        1800,
        cc_lb_testkit::fixed_clock(1_800_000_000),
    )
    .await
    .expect("initial dynamic view builds");
    assert_eq!(initial.generation, initial_generation);
    let holder = Arc::new(DynamicViewHolder::new(initial));
    Fixture {
        storage,
        stores,
        oauth,
        aead,
        runtime,
        holder,
    }
}

async fn spawn_listener(
    fixture: &Fixture,
    notifier: Arc<MockNotifier>,
    cancel: CancellationToken,
    stores: Arc<Stores>,
) -> tokio::task::JoinHandle<()> {
    let listener = Arc::new(NotifyListener::new(NotifyListenerParams {
        notifier: notifier.clone(),
        cancel,
        holder: fixture.holder.clone(),
        stores,
        oauth_cfg: fixture.oauth.clone(),
        runtime: fixture.runtime.clone(),
        aead: fixture.aead.clone(),
        data_dir: Path::new(".").to_path_buf(),
        lazy_refresher: None,
        subscription_quota_cache: Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        prompt_cache_observation_cache: None,
        prompt_cache_observation_sink: None,
        subscription_quota_routing_max_staleness_secs: 1800,
        clock: cc_lb_testkit::fixed_clock(1_800_000_000),
    }));
    let task = tokio::spawn(async move {
        listener.run().await;
    });
    notifier.wait_for_subscriber().await;
    task
}

fn stores_with_upstream(fixture: &Fixture, upstreams: Arc<dyn UpstreamStore>) -> Arc<Stores> {
    Arc::new(Stores {
        upstreams,
        principals: fixture.stores.principals.clone(),
        plugin_registry: fixture.stores.plugin_registry.clone(),
        upstream_rate_limits: fixture.stores.upstream_rate_limits.clone(),
        upstream_subscription_quotas: fixture.stores.upstream_subscription_quotas.clone(),
        upstream_subscription_metadata: fixture.stores.upstream_subscription_metadata.clone(),
        organization_metadata: fixture.stores.organization_metadata.clone(),
        plan_tiers: fixture.stores.plan_tiers.clone(),
        prompt_cache_observations: fixture.stores.prompt_cache_observations.clone(),
        anthropic_compatibility_kv: fixture.stores.anthropic_compatibility_kv.clone(),
        audit: None,
    })
}

#[tokio::test]
async fn t2__single_notify_triggers_single_rebuild() {
    tokio::time::pause();
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
    assert_eq!(start, INITIAL_GENERATION);
    let cancel = CancellationToken::new();
    let task = spawn_listener(
        &fixture,
        notifier.clone(),
        cancel.clone(),
        fixture.stores.clone(),
    )
    .await;

    notifier.send(ChangeChannel::Upstream);
    notifier.close();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(250)).await;
    task.await
        .expect("listener exits after completing notified rebuild");
    assert_eq!(fixture.holder.generation(), start + 1);
}

#[tokio::test]
async fn t2__burst_of_10_notifies_in_100ms_triggers_1_rebuild() {
    tokio::time::pause();
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
    assert_eq!(start, INITIAL_GENERATION);
    let cancel = CancellationToken::new();
    let task = spawn_listener(
        &fixture,
        notifier.clone(),
        cancel.clone(),
        fixture.stores.clone(),
    )
    .await;

    for _ in 0..10 {
        notifier.send(ChangeChannel::Upstream);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    notifier.close();
    tokio::time::advance(Duration::from_millis(250)).await;
    task.await
        .expect("listener exits after completing debounced rebuild");
    assert_eq!(fixture.holder.generation(), start + 1);
}

#[tokio::test]
async fn t2__cancel_during_rebuild_graceful() {
    tokio::time::pause();
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let stores = stores_with_upstream(
        &fixture,
        Arc::new(ControlledUpstreamStore::blocking(
            fixture.storage.clone(),
            started_tx,
            release_rx,
        )),
    );
    let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

    notifier.send(ChangeChannel::Upstream);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(250)).await;
    started_rx.await.expect("rebuild starts");
    cancel.cancel();
    let _ = release_tx.send(());
    task.await.expect("listener exits without panic");

    assert_eq!(fixture.holder.generation(), start);
}

#[tokio::test]
async fn t2__rebuild_failure_does_not_swap_view() {
    let (recorder, metrics) = cc_lb_testkit::local_recorder();
    let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
    tokio::time::pause();
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let before = labeled_counter_value(&metrics, "cclb_rebind_total", "outcome", "error");
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let (started_tx, started_rx) = oneshot::channel();
    let stores = stores_with_upstream(
        &fixture,
        Arc::new(ControlledUpstreamStore::failing(
            fixture.storage.clone(),
            started_tx,
        )),
    );
    let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

    notifier.send(ChangeChannel::Upstream);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(250)).await;
    started_rx.await.expect("failed rebuild starts");
    tokio::task::yield_now().await;

    assert_eq!(fixture.holder.generation(), start);
    assert_eq!(
        labeled_counter_value(&metrics, "cclb_rebind_total", "outcome", "error"),
        before + 1.0
    );
    cancel.cancel();
    task.await.expect("listener exits");
}

fn labeled_counter_value(snapshotter: &Snapshotter, name: &str, label: &str, value: &str) -> f64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, metric)| {
            (key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|item| item.key() == label && item.value() == value))
            .then(|| format!("{metric:?}"))
            .and_then(|rendered| {
                rendered
                    .strip_prefix("Counter(")?
                    .strip_suffix(')')?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0) as f64
}
