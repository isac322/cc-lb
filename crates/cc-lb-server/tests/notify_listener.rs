use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_engine::DynamicViewHolder;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::notify_listener::{NotifyListener, NotifyListenerParams};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    BackendKind, ChangeChannel, ChangeEvent, RateLimitKind, RuntimeChangeNotifier, StorageError,
    StorageResult, SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate,
    UpstreamRateLimitObservationRecord, UpstreamRecord, UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_sqlite::{SqliteStorage as Storage, open_sqlite};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::sync::{broadcast, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct MockNotifier {
    tx: broadcast::Sender<ChangeEvent>,
}

impl MockNotifier {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self { tx }
    }

    fn send(&self, channel: ChangeChannel) {
        let _ = self
            .tx
            .send(ChangeEvent::new(channel, "test", std::time::UNIX_EPOCH));
    }

    async fn wait_for_subscriber(&self) {
        for _ in 0..100 {
            if self.tx.receiver_count() > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("notify listener did not subscribe");
    }
    fn send_for_upstream(&self, channel: ChangeChannel, upstream_id: Uuid) {
        let _ = self.tx.send(ChangeEvent::new(
            channel,
            upstream_id.to_string(),
            std::time::UNIX_EPOCH,
        ));
    }
}

#[async_trait]
impl RuntimeChangeNotifier for MockNotifier {
    async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
        Ok(self.tx.subscribe())
    }

    async fn run(&self, cancel: CancellationToken) -> StorageResult<()> {
        cancel.cancelled().await;
        Ok(())
    }
}

struct ControlledUpstreamStore {
    inner: Arc<Storage>,
    fail_list: bool,
    list_started: Option<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
    delay_list: Option<Duration>,
}

impl ControlledUpstreamStore {
    fn failing(inner: Arc<Storage>) -> Self {
        Self {
            inner,
            fail_list: true,
            list_started: None,
            delay_list: None,
        }
    }

    fn delayed(inner: Arc<Storage>, delay: Duration, list_started: oneshot::Sender<()>) -> Self {
        Self {
            inner,
            fail_list: false,
            list_started: Some(tokio::sync::Mutex::new(Some(list_started))),
            delay_list: Some(delay),
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
        if self.fail_list {
            return Err(StorageError::Unavailable {
                message: "injected list failure".to_owned(),
            });
        }
        if let Some(started) = &self.list_started
            && let Some(sender) = started.lock().await.take()
        {
            let _ = sender.send(());
        }
        if let Some(delay) = self.delay_list {
            tokio::time::sleep(delay).await;
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

struct Fixture {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    oauth: Arc<AnthropicOAuthConfig>,
    aead: Arc<AeadService>,
    runtime: Arc<WasmtimeRuntime>,
    holder: Arc<DynamicViewHolder>,
    quota_cache: Arc<cc_lb_server::SubscriptionQuotaCache>,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("notify.sqlite");
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
        .await
        .expect("storage opens");
    cc_lb_storage_api::MetaStore::initialize(&storage, BackendKind::Sqlite)
        .await
        .expect("initialize");
    let storage = Arc::new(storage);
    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        upstream_subscription_metadata: storage.clone(),
        organization_metadata: storage.clone(),
        plan_tiers: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: None,
    });
    let oauth = Arc::new(AnthropicOAuthConfig::default());
    let aead = Arc::new(AeadService::from_master_key([24; 32]));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let quota_cache = Arc::new(cc_lb_server::SubscriptionQuotaCache::new());
    let initial = build_dynamic_view(
        &stores,
        &oauth,
        aead.clone(),
        None,
        0,
        &runtime,
        dir.path(),
        quota_cache.clone(),
        30,
        None,
        None,
        1800,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("initial dynamic view builds");
    let holder = Arc::new(DynamicViewHolder::new(initial));
    Fixture {
        _dir: dir,
        storage,
        stores,
        oauth,
        aead,
        runtime,
        quota_cache,
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
        data_dir: fixture._dir.path().to_path_buf(),
        lazy_refresher: None,
        subscription_quota_cache: fixture.quota_cache.clone(),
        prompt_cache_thread_usage: None,
        prompt_cache_grace_margin_secs: 30,
        prompt_cache_observation_sink: None,
        subscription_quota_routing_max_staleness_secs: 1800,
        clock: Arc::new(cc_lb_engine::SystemClock),
    }));
    let task = tokio::spawn(async move {
        listener.run().await;
    });
    notifier.wait_for_subscriber().await;
    task
}

async fn wait_for_generation(holder: &DynamicViewHolder, expected: u64) {
    for _ in 0..100 {
        if holder.generation() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(holder.generation(), expected);
}

#[tokio::test]
async fn single_notify_triggers_single_rebuild() {
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let task = spawn_listener(
        &fixture,
        notifier.clone(),
        cancel.clone(),
        fixture.stores.clone(),
    )
    .await;

    notifier.send(ChangeChannel::Upstream);
    wait_for_generation(&fixture.holder, start + 1).await;

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(fixture.holder.generation(), start + 1);
    cancel.cancel();
    task.await.expect("listener exits");
}

#[tokio::test]
async fn burst_of_10_notifies_in_100ms_triggers_1_rebuild() {
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
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
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_for_generation(&fixture.holder, start + 1).await;

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(fixture.holder.generation(), start + 1);
    cancel.cancel();
    task.await.expect("listener exits");
}

#[tokio::test]
async fn cancel_during_rebuild_graceful() {
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let (started_tx, started_rx) = oneshot::channel();
    let stores = Arc::new(Stores {
        upstreams: Arc::new(ControlledUpstreamStore::delayed(
            fixture.storage.clone(),
            Duration::from_millis(400),
            started_tx,
        )),
        principals: fixture.storage.clone(),
        plugin_registry: fixture.storage.clone(),
        upstream_rate_limits: fixture.storage.clone(),
        upstream_subscription_quotas: fixture.storage.clone(),
        upstream_subscription_metadata: fixture.storage.clone(),
        organization_metadata: fixture.storage.clone(),
        plan_tiers: fixture.storage.clone(),
        prompt_cache_observations: fixture.storage.clone(),
        anthropic_compatibility_kv: fixture.storage.clone(),
        audit: None,
    });
    let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

    notifier.send(ChangeChannel::Upstream);
    started_rx.await.expect("rebuild starts");
    cancel.cancel();
    task.await.expect("listener exits without panic");

    assert_eq!(fixture.holder.generation(), start);
}

#[tokio::test]
async fn rebuild_failure_does_not_swap_view() {
    let handle = crate::common::install_prometheus();
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let before = labeled_counter_value(handle, "cclb_rebind_total", "outcome", "error");
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let stores = Arc::new(Stores {
        upstreams: Arc::new(ControlledUpstreamStore::failing(fixture.storage.clone())),
        principals: fixture.storage.clone(),
        plugin_registry: fixture.storage.clone(),
        upstream_rate_limits: fixture.storage.clone(),
        upstream_subscription_quotas: fixture.storage.clone(),
        upstream_subscription_metadata: fixture.storage.clone(),
        organization_metadata: fixture.storage.clone(),
        plan_tiers: fixture.storage.clone(),
        prompt_cache_observations: fixture.storage.clone(),
        anthropic_compatibility_kv: fixture.storage.clone(),
        audit: None,
    });
    let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

    notifier.send(ChangeChannel::Upstream);
    for _ in 0..100 {
        let after = labeled_counter_value(handle, "cclb_rebind_total", "outcome", "error");
        if after >= before + 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(fixture.holder.generation(), start);
    assert_eq!(
        labeled_counter_value(handle, "cclb_rebind_total", "outcome", "error"),
        before + 1.0
    );
    cancel.cancel();
    task.await.expect("listener exits");
}
#[tokio::test]
async fn hydrate_notifications_refresh_peer_caches_without_view_rebuild() {
    let fixture = fixture().await;
    let start = fixture.holder.generation();

    // Real SQLite rows: one upstream plus its rate-limit observation and a
    // subscription quota sample.
    let upstream = UpstreamStore::create(
        &*fixture.storage,
        UpstreamCreate {
            name: "hydrate-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("upstream created");
    let ciphertext = fixture
        .aead
        .encrypt(b"sk-ant-fixture-secret", upstream.id.as_bytes())
        .expect("api-key ciphertext");
    UpstreamStore::update_api_key_secret(&*fixture.storage, upstream.id, Some(ciphertext))
        .await
        .expect("upstream api-key secret");
    fixture
        .stores
        .upstream_rate_limits
        .put_observation(&UpstreamRateLimitObservationRecord {
            upstream_id: upstream.id,
            window: "1m".to_owned(),
            kind: RateLimitKind::Requests,
            limit: Some(1_000),
            remaining: Some(750),
            reset: Some("30s".to_owned()),
            observed_at_unix_secs: 1_800_000_000,
        })
        .await
        .expect("rate limit observation stored");
    fixture
        .stores
        .upstream_subscription_quotas
        .record_subscription_quota_sample(&SubscriptionQuotaSample {
            upstream_id: upstream.id,
            window: SubscriptionQuotaWindow::FiveHour,
            source: SubscriptionQuotaSource::Header,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis: 1_800_000_000_000,
            sample_id: Uuid::new_v4(),
            utilization: Some(0.42),
            status: Some(SubscriptionQuotaStatus::Allowed),
            resets_at_unix_secs: Some(1_800_018_000),
            surpassed_threshold: None,
            representative_claim: None,
            fallback_percentage: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            ingested_at_unix_millis: 1_800_000_000_000,
        })
        .await
        .expect("quota sample stored");

    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let task = spawn_listener(
        &fixture,
        notifier.clone(),
        cancel.clone(),
        fixture.stores.clone(),
    )
    .await;

    notifier.send_for_upstream(ChangeChannel::UpstreamRateLimit, upstream.id);
    notifier.send_for_upstream(ChangeChannel::SubscriptionQuota, upstream.id);

    // Wait for observable cache state, not an assumed scheduling delay.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let rate_limit_seen = {
                let view = fixture.holder.load();
                let cache = view.upstream_rate_limit_cache.read();
                cache
                    .snapshots
                    .get(&upstream.id)
                    .is_some_and(|snapshots| !snapshots.is_empty())
            };
            let quota_seen = fixture
                .quota_cache
                .snapshot_for_upstream(upstream.id, 1_800_000_001_000, 30)
                .iter()
                .any(|snapshot| snapshot.window == "5h" && snapshot.utilization == Some(0.42));
            if rate_limit_seen && quota_seen {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both notifications hydrate peer caches");

    {
        let view = fixture.holder.load();
        let cache = view.upstream_rate_limit_cache.read();
        assert_eq!(
            cache.snapshots.get(&upstream.id),
            Some(&vec![cc_lb_domain::RateLimitObservation {
                kind: RateLimitKind::Requests,
                window: "1m".to_owned(),
                limit: Some(1_000),
                remaining: Some(750),
                reset: Some("30s".to_owned()),
            }])
        );
    }
    let quota_snapshots =
        fixture
            .quota_cache
            .snapshot_for_upstream(upstream.id, 1_800_000_001_000, 30);
    let five_hour = quota_snapshots
        .iter()
        .find(|snapshot| snapshot.window == "5h")
        .expect("5h quota snapshot");
    assert_eq!(
        five_hour.state,
        cc_lb_domain::SubscriptionQuotaDataState::Fresh
    );
    assert_eq!(five_hour.utilization, Some(0.42));
    assert_eq!(five_hour.status.as_deref(), Some("allowed"));

    // Hydrate channels refresh peer caches only; the view generation is
    // untouched because no rebuild ran.
    assert_eq!(fixture.holder.generation(), start);
    cancel.cancel();
    task.await.expect("listener exits");
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
