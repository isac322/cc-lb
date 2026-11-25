use std::sync::Arc;
use std::time::Duration;

use crate::dynamic_view_rebind::{assert_catalog_urls, catalog_configs, create_catalog_upstreams};
use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_control::DynamicViewHolder;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::notify_listener::{NotifyListener, NotifyListenerParams};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    ChangeChannel, ChangeEvent, RateLimitKind, RuntimeChangeNotifier, StorageError, StorageResult,
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
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

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        UpstreamStore::set_status(&*self.inner, id, status).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
        never_refresh: bool,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::store_oauth_tokens(
            &*self.inner,
            id,
            expected_revision,
            tokens,
            never_refresh,
        )
        .await
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
    aead: Arc<AeadService>,
    runtime: Arc<WasmtimeRuntime>,
    holder: Arc<DynamicViewHolder>,
    quota_cache: Arc<cc_lb_server::SubscriptionQuotaCache>,
    gemini_config: cc_lb_config::GeminiOAuthConfig,
    antigravity_config: cc_lb_config::AntigravityOAuthConfig,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("notify.sqlite");
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .expect("storage opens");
    cc_lb_storage_api::MetaStore::initialize(&storage)
        .await
        .expect("initialize");
    let storage = Arc::new(storage);
    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        vertex_state: Arc::new(cc_lb_server::dynamic_view_builder::VertexState::new(
            storage.clone(),
        )),
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
    let aead = Arc::new(AeadService::from_master_key([24; 32]));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let quota_cache = Arc::new(cc_lb_server::SubscriptionQuotaCache::new());
    let (gemini_config, antigravity_config) = catalog_configs();
    let initial = build_dynamic_view(
        &stores,
        Some(&gemini_config),
        Some(&antigravity_config),
        aead.clone(),
        None,
        0,
        &runtime,
        dir.path(),
        quota_cache.clone(),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
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
        aead,
        runtime,
        quota_cache,
        holder,
        gemini_config,
        antigravity_config,
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
        runtime: fixture.runtime.clone(),
        aead: fixture.aead.clone(),
        data_dir: fixture._dir.path().to_path_buf(),
        lazy_refresher: None,
        subscription_quota_cache: fixture.quota_cache.clone(),
        prompt_cache_grace_margin_secs: 0,
        prompt_cache_observation_sink: Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        subscription_quota_routing_max_staleness_secs: 1800,
        key_store: None,
        thread_sticky_subagent_latch: None,
        clock: Arc::new(cc_lb_clock::SystemClock),
        gemini_config: Some(fixture.gemini_config.clone()),
        antigravity_config: Some(fixture.antigravity_config.clone()),
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
async fn notify_rebuild_preserves_configured_catalog_metadata_endpoints() {
    let fixture = fixture().await;
    let upstreams = create_catalog_upstreams(&fixture.storage, &fixture.aead).await;
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let task = spawn_listener(
        &fixture,
        notifier.clone(),
        cancel.clone(),
        fixture.stores.clone(),
    )
    .await;
    let initial_hash = fixture.holder.load().upstream_status_snapshot.revision_hash;
    let generation = fixture.holder.generation();
    notifier.send(ChangeChannel::Upstream);
    wait_for_generation(&fixture.holder, generation + 1).await;
    let initial = fixture.holder.load();
    assert_ne!(initial.upstream_status_snapshot.revision_hash, initial_hash);
    assert_catalog_urls(&initial, &upstreams).await;

    let mut revised_upstreams = upstreams;
    let record = &revised_upstreams[0].0;
    let revised = UpstreamStore::update(
        &*fixture.storage,
        record.id,
        record.revision,
        UpstreamUpdate {
            name: Some("catalog-cli-config-revised".to_owned()),
            ..UpstreamUpdate::default()
        },
    )
    .await
    .expect("update catalog revision");
    revised_upstreams[0].0 = revised;
    notifier.send(ChangeChannel::Upstream);
    wait_for_generation(&fixture.holder, initial.generation + 1).await;
    let rebuilt = fixture.holder.load();
    assert_ne!(
        rebuilt.upstream_status_snapshot.revision_hash,
        initial.upstream_status_snapshot.revision_hash,
    );
    assert_catalog_urls(&rebuilt, &revised_upstreams).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("listener stops within deadline")
        .expect("listener exits");
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

    tokio::time::pause();
    for _ in 0..10 {
        notifier.send(ChangeChannel::Upstream);
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    tokio::time::resume();
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
        vertex_state: Arc::new(cc_lb_server::dynamic_view_builder::VertexState::new(
            fixture.storage.clone(),
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
        vertex_state: Arc::new(cc_lb_server::dynamic_view_builder::VertexState::new(
            fixture.storage.clone(),
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
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: Uuid::new_v4(),
            name: "hydrate-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("upstream created");
    let ciphertext = fixture
        .aead
        .encrypt(b"sk-ant-fixture-secret", upstream.id.as_bytes())
        .expect("api-key ciphertext");
    UpstreamStore::update(
        &*fixture.storage,
        upstream.id,
        upstream.revision,
        UpstreamUpdate {
            api_key_ciphertext: Some(ciphertext),
            ..UpstreamUpdate::default()
        },
    )
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
        .record_subscription_quota_samples(&[SubscriptionQuotaSample {
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
            credits: None,
            ingested_at_unix_millis: 1_800_000_000_000,
        }])
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_listener_persists_across_repeated_subscribe_failures_and_channel_reset() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FlakyReconnectNotifier {
        attempts: AtomicUsize,
        first_tx: std::sync::Mutex<Option<broadcast::Sender<ChangeEvent>>>,
        second_tx: broadcast::Sender<ChangeEvent>,
        second_subscribed: tokio::sync::Notify,
    }

    #[async_trait]
    impl RuntimeChangeNotifier for FlakyReconnectNotifier {
        async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(StorageError::Unavailable {
                    message: "transient subscribe error".to_owned(),
                })
            } else if n == 2 {
                Ok(self
                    .first_tx
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("first_tx open on initial subscribe")
                    .subscribe())
            } else {
                self.second_subscribed.notify_one();
                Ok(self.second_tx.subscribe())
            }
        }

        async fn run(&self, cancel: CancellationToken) -> StorageResult<()> {
            cancel.cancelled().await;
            Ok(())
        }
    }

    let fixture = fixture().await;
    let first_tx = Some(broadcast::channel::<ChangeEvent>(16).0);
    let (second_tx, _) = broadcast::channel::<ChangeEvent>(16);
    let notifier = Arc::new(FlakyReconnectNotifier {
        attempts: AtomicUsize::new(0),
        first_tx: std::sync::Mutex::new(first_tx),
        second_tx,
        second_subscribed: tokio::sync::Notify::new(),
    });
    let cancel = CancellationToken::new();
    let listener = Arc::new(NotifyListener::new(NotifyListenerParams {
        notifier: notifier.clone(),
        cancel: cancel.clone(),
        holder: fixture.holder.clone(),
        stores: fixture.stores.clone(),
        runtime: fixture.runtime.clone(),
        aead: fixture.aead.clone(),
        data_dir: fixture._dir.path().to_path_buf(),
        lazy_refresher: None,
        subscription_quota_cache: fixture.quota_cache.clone(),
        prompt_cache_grace_margin_secs: 30,
        prompt_cache_observation_sink: Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        subscription_quota_routing_max_staleness_secs: 1800,
        key_store: None,
        thread_sticky_subagent_latch: None,
        gemini_config: Some(fixture.gemini_config.clone()),
        antigravity_config: Some(fixture.antigravity_config.clone()),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }));

    let task = tokio::spawn(listener.run());
    tokio::time::timeout(Duration::from_secs(5), async {
        while notifier.attempts.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("listener retries beyond a single initial subscribe failure");

    let wait_second = notifier.second_subscribed.notified();
    *notifier.first_tx.lock().unwrap() = None;
    tokio::time::timeout(Duration::from_secs(5), wait_second)
        .await
        .expect("listener resubscribes after RecvError::Closed");

    cancel.cancel();
    task.await.expect("listener task exits cleanly");
}

#[tokio::test]
async fn managed_key_change_notification_invalidates_key_store_immediately_without_rebuild() {
    use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
    use cc_lb_storage_api::{
        NotifierConnectionState, PrincipalCreate, PrincipalKind, PrincipalStore,
    };

    let fixture = fixture().await;
    let start_generation = fixture.holder.generation();
    let _principal = PrincipalStore::create(
        &*fixture.storage,
        PrincipalCreate {
            name: "p-key-notify".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
            openai_oauth_compat: None,
        },
        1,
    )
    .await
    .expect("create principal");
    let conn_state = Arc::new(NotifierConnectionState::new_always_connected());
    let key_store =
        Arc::new(KeyStore::new(fixture.storage.clone()).with_positive_cache(conn_state));
    let (record, plaintext) = key_store
        .create(
            "p-key-notify",
            CreateParams {
                label: "test".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("issue key");
    let key_id = record.index_hash;
    assert!(
        key_store
            .lookup_by_index_hash(&key_id)
            .await
            .expect("lookup")
            .is_some()
    );

    // Directly revoke in backing storage so the positive cache still holds the stale entry.
    let (parsed_key_id, _) =
        cc_lb_control::api_keys::secret::parse(plaintext.expose()).expect("parse key");
    cc_lb_storage_api::ManagedKeyStore::revoke_zero_secrets(
        &*fixture.storage,
        "p-key-notify",
        &parsed_key_id,
    )
    .await
    .expect("direct revoke");
    assert!(
        key_store
            .lookup_by_index_hash(&key_id)
            .await
            .expect("cached hit before notify")
            .is_some()
    );

    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let listener = Arc::new(NotifyListener::new(NotifyListenerParams {
        notifier: notifier.clone(),
        cancel: cancel.clone(),
        holder: fixture.holder.clone(),
        stores: fixture.stores.clone(),
        runtime: fixture.runtime.clone(),
        aead: fixture.aead.clone(),
        data_dir: fixture._dir.path().to_path_buf(),
        lazy_refresher: None,
        subscription_quota_cache: fixture.quota_cache.clone(),
        prompt_cache_grace_margin_secs: 0,
        prompt_cache_observation_sink: Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        subscription_quota_routing_max_staleness_secs: 1800,
        key_store: Some(Arc::clone(&key_store)),
        thread_sticky_subagent_latch: None,
        clock: Arc::new(cc_lb_clock::SystemClock),
        gemini_config: Some(fixture.gemini_config.clone()),
        antigravity_config: Some(fixture.antigravity_config.clone()),
    }));
    let task = tokio::spawn(listener.run());
    notifier.wait_for_subscriber().await;

    let _ = notifier.tx.send(ChangeEvent::new(
        ChangeChannel::ManagedKey,
        format!("p-key-notify\t{parsed_key_id}"),
        std::time::UNIX_EPOCH,
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if key_store
                .lookup_by_index_hash(&key_id)
                .await
                .expect("lookup after notify")
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed key notification invalidates positive cache without debounce");
    assert_eq!(
        fixture.holder.generation(),
        start_generation,
        "managed key notification must not rebuild DynamicView"
    );

    cancel.cancel();
    task.await.expect("listener task exits cleanly");
}

#[tokio::test]
async fn notify_rebuild_shares_rate_limit_cache_and_prevents_stale_db_regression() {
    let fixture = fixture().await;
    let upstream = UpstreamStore::create(
        &*fixture.storage,
        UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: Uuid::new_v4(),
            name: "rate-limit-shared-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("upstream created");
    let upstream_id = upstream.id;
    let initial_cache = Arc::clone(&fixture.holder.load().upstream_rate_limit_cache);
    let start = fixture.holder.generation();

    // Seed an older observation in storage before an in-memory live header arrives.
    fixture
        .stores
        .upstream_rate_limits
        .put_observation(&UpstreamRateLimitObservationRecord {
            upstream_id,
            window: "default".to_owned(),
            kind: RateLimitKind::Requests,
            limit: Some(1000),
            remaining: Some(10),
            reset: Some("2026-05-29T00:00:00Z".to_owned()),
            observed_at_unix_secs: 100,
        })
        .await
        .expect("seed stale DB rate-limit observation");

    // Live header subscriber updates the shared cache with a newer timestamp.
    initial_cache
        .write()
        .upsert_record(UpstreamRateLimitObservationRecord {
            upstream_id,
            window: "default".to_owned(),
            kind: RateLimitKind::Requests,
            limit: Some(1000),
            remaining: Some(500),
            reset: Some("2026-05-29T00:01:00Z".to_owned()),
            observed_at_unix_secs: 200,
        });

    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let task = spawn_listener(
        &fixture,
        notifier.clone(),
        cancel.clone(),
        fixture.stores.clone(),
    )
    .await;

    // Trigger a rebuild via upstream change notification.
    notifier.send(ChangeChannel::Upstream);
    wait_for_generation(&fixture.holder, start + 1).await;

    let rebuilt = fixture.holder.load();
    assert!(Arc::ptr_eq(
        &initial_cache,
        &rebuilt.upstream_rate_limit_cache
    ));
    {
        let cache = rebuilt.upstream_rate_limit_cache.read();
        let obs = cache
            .snapshots
            .get(&upstream_id)
            .expect("upstream snapshots present");
        assert!(obs.iter().any(|o| o.remaining == Some(500)));
        assert_eq!(cache.observed_at_for_upstream(upstream_id), 200);
    }

    // Header update AFTER the rebuild (mutating `initial_cache`) is visible immediately on `rebuilt`.
    initial_cache
        .write()
        .upsert_record(UpstreamRateLimitObservationRecord {
            upstream_id,
            window: "default".to_owned(),
            kind: RateLimitKind::Requests,
            limit: Some(1000),
            remaining: Some(450),
            reset: Some("2026-05-29T00:02:00Z".to_owned()),
            observed_at_unix_secs: 300,
        });
    assert!(
        rebuilt
            .upstream_rate_limit_cache
            .read()
            .snapshots
            .get(&upstream_id)
            .expect("upstream snapshots present")
            .iter()
            .any(|o| o.remaining == Some(450))
    );
    assert_eq!(
        rebuilt
            .upstream_rate_limit_cache
            .read()
            .observed_at_for_upstream(upstream_id),
        300
    );

    cancel.cancel();
    task.await.expect("listener task exits cleanly");
}

#[tokio::test]
async fn thread_sticky_subagent_notification_warms_latch_and_overrides_negative() {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::Body;
    use bytes::Bytes;
    use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
    use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::{DynamicViewBuilder, new_in_memory_bus};
    use cc_lb_domain::Upstream;
    use cc_lb_engine::{
        ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig,
        ThreadStickySubagentLatch, UpstreamDispatch,
    };
    use cc_lb_lifecycle::LifecycleEvent;
    use cc_lb_storage_api::{
        PrincipalCreate, PrincipalKind, PrincipalStore, ThreadStickySubagentStore,
    };
    use cc_lb_upstream::{
        RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
        SigningCapability, UpstreamError,
    };
    use http::header::CONTENT_TYPE;
    use http::{HeaderValue, Method, Request, Response, StatusCode};

    struct StubSigner;

    #[async_trait]
    impl Signer for StubSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    struct StubSignerFactory;

    impl ApiKeyAwareSignerFactory for StubSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self)
        }
    }

    #[async_trait]
    impl SignerFactory for StubSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(StubSigner))
        }
    }

    struct StubDispatch;

    #[async_trait]
    impl UpstreamDispatch for StubDispatch {
        async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            let mut resp = Response::new(Body::from(Bytes::from_static(
                br#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
            )));
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            Ok(resp)
        }
    }

    struct CountingStickyStore {
        rows: Mutex<HashMap<(String, String), u64>>,
        lookups: AtomicUsize,
    }

    #[async_trait]
    impl ThreadStickySubagentStore for CountingStickyStore {
        async fn seed_thread_sticky_subagent(
            &self,
            principal_id: &str,
            observed_session_id: &str,
            now_unix_secs: u64,
            ttl_secs: u64,
        ) -> StorageResult<()> {
            let expires_at = now_unix_secs.saturating_add(ttl_secs);
            self.rows
                .lock()
                .expect("rows lock")
                .entry((principal_id.to_owned(), observed_session_id.to_owned()))
                .and_modify(|existing| *existing = (*existing).max(expires_at))
                .or_insert(expires_at);
            Ok(())
        }

        async fn is_thread_sticky_subagent(
            &self,
            principal_id: &str,
            observed_session_id: &str,
            now_unix_secs: u64,
        ) -> StorageResult<Option<u64>> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .rows
                .lock()
                .expect("rows lock")
                .get(&(principal_id.to_owned(), observed_session_id.to_owned()))
                .copied()
                .filter(|expires_at| *expires_at > now_unix_secs))
        }

        async fn purge_expired_thread_sticky_subagents(
            &self,
            _now_unix_secs: u64,
            _batch_size: usize,
        ) -> StorageResult<u64> {
            Ok(0)
        }
    }

    async fn send_main_and_read_kind(
        lifecycle: &Lifecycle,
        api_key: &str,
        session_id: &str,
        rx: &mut broadcast::Receiver<LifecycleEvent>,
    ) -> Option<String> {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("x-session-affinity", session_id)
            .header(CONTENT_TYPE, "application/json")
            .body(Bytes::from_static(
                br#"{"model":"claude-sonnet-4-6","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .expect("request builds");
        let auth = lifecycle.authenticate(req.headers()).await.expect("auth");
        let resp = lifecycle.handle(req, &auth).await.expect("handle");
        assert_eq!(resp.status(), StatusCode::OK);
        loop {
            match rx.try_recv() {
                Ok(LifecycleEvent::ParseCompleted {
                    result: Ok(info), ..
                }) => return info.request_kind,
                Ok(_) => {}
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(err) => panic!("unexpected lifecycle bus state: {err:?}"),
            }
        }
    }

    let fixture = fixture().await;
    PrincipalStore::create(
        &*fixture.storage,
        PrincipalCreate {
            name: "p-sticky-notify".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
            openai_oauth_compat: None,
        },
        1,
    )
    .await
    .expect("create principal");
    let key_store = Arc::new(KeyStore::new(fixture.storage.clone()));
    let (_record, plaintext) = key_store
        .create(
            "p-sticky-notify",
            CreateParams {
                label: "sticky".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("issue key");
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(StubSignerFactory))
        .principal_view(Arc::new(PrincipalView::for_tests(
            "p-sticky-notify",
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        )))
        .upstream_records(vec![UpstreamRecord {
            id: Uuid::new_v4(),
            name: "ant-sticky".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            enabled: true,
            revision: 1,
            ..UpstreamRecord::default()
        }])
        .build();
    fixture.holder.store(view);
    let start_generation = fixture.holder.generation();

    let latch = Arc::new(ThreadStickySubagentLatch::new());
    let sticky_store = Arc::new(CountingStickyStore {
        rows: Mutex::new(HashMap::new()),
        lookups: AtomicUsize::new(0),
    });
    let bus = new_in_memory_bus();
    let mut lifecycle_rx = bus.subscribe_lifecycle();
    let authn = Arc::new(BuiltinAuthn::new(
        Arc::clone(&key_store),
        Arc::new(cc_lb_clock::SystemClock),
    ));
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn,
        fixture.holder.clone(),
        Arc::new(StubDispatch),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(bus)
    .with_thread_sticky_subagent_latch(Arc::clone(&latch))
    .with_thread_sticky_subagent_store(sticky_store.clone());

    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let listener = Arc::new(NotifyListener::new(NotifyListenerParams {
        notifier: notifier.clone(),
        cancel: cancel.clone(),
        holder: fixture.holder.clone(),
        stores: fixture.stores.clone(),
        runtime: fixture.runtime.clone(),
        aead: fixture.aead.clone(),
        data_dir: fixture._dir.path().to_path_buf(),
        lazy_refresher: None,
        subscription_quota_cache: fixture.quota_cache.clone(),
        prompt_cache_grace_margin_secs: 0,
        prompt_cache_observation_sink: Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        subscription_quota_routing_max_staleness_secs: 1800,
        key_store: None,
        thread_sticky_subagent_latch: Some(Arc::clone(&latch)),
        clock: Arc::new(cc_lb_clock::SystemClock),
        gemini_config: Some(fixture.gemini_config.clone()),
        antigravity_config: Some(fixture.antigravity_config.clone()),
    }));
    let task = tokio::spawn(listener.run());
    notifier.wait_for_subscriber().await;

    let api_key = plaintext.expose();
    assert_eq!(
        send_main_and_read_kind(&lifecycle, api_key, "sess-sticky", &mut lifecycle_rx)
            .await
            .as_deref(),
        Some("unknown")
    );
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 1);

    // Negative L1 entry prevents duplicate L2 lookup.
    assert_eq!(
        send_main_and_read_kind(&lifecycle, api_key, "sess-sticky", &mut lifecycle_rx)
            .await
            .as_deref(),
        Some("unknown")
    );
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 1);

    // Positive notification with stored expiry seeds L1 directly with 0 DB lookups.
    let future_expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let _ = notifier.tx.send(ChangeEvent::new(
        ChangeChannel::ThreadStickySubagent,
        format!("p-sticky-notify\tsess-sticky\t{future_expiry}"),
        std::time::UNIX_EPOCH,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if send_main_and_read_kind(&lifecycle, api_key, "sess-sticky", &mut lifecycle_rx)
                .await
                .as_deref()
                == Some("subagent")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sticky subagent notification warms latch immediately");
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.holder.generation(),
        start_generation,
        "thread sticky subagent notification must not rebuild DynamicView"
    );

    // Notification without expiry (legacy 2-field payload) invalidates L1 and reloads expiry from DB.
    assert_eq!(
        send_main_and_read_kind(&lifecycle, api_key, "sess-sticky-legacy", &mut lifecycle_rx)
            .await
            .as_deref(),
        Some("unknown")
    );
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 2);
    sticky_store.rows.lock().expect("rows lock").insert(
        (
            "p-sticky-notify".to_owned(),
            "sess-sticky-legacy".to_owned(),
        ),
        future_expiry,
    );
    let _ = notifier.tx.send(ChangeEvent::new(
        ChangeChannel::ThreadStickySubagent,
        "p-sticky-notify\tsess-sticky-legacy",
        std::time::UNIX_EPOCH,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if send_main_and_read_kind(&lifecycle, api_key, "sess-sticky-legacy", &mut lifecycle_rx)
                .await
                .as_deref()
                == Some("subagent")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("2-field sticky subagent notification invalidates negative L1 and reloads L2");
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 3);

    // Empty-payload fallback clears negative L1 entries so L2 lookup succeeds on next request.
    assert_eq!(
        send_main_and_read_kind(&lifecycle, api_key, "sess-sticky-2", &mut lifecycle_rx)
            .await
            .as_deref(),
        Some("unknown")
    );
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 4);
    sticky_store.rows.lock().expect("rows lock").insert(
        ("p-sticky-notify".to_owned(), "sess-sticky-2".to_owned()),
        future_expiry,
    );
    let _ = notifier.tx.send(ChangeEvent::new(
        ChangeChannel::ThreadStickySubagent,
        "",
        std::time::UNIX_EPOCH,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if send_main_and_read_kind(&lifecycle, api_key, "sess-sticky-2", &mut lifecycle_rx)
                .await
                .as_deref()
                == Some("subagent")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("empty sticky subagent notification clears negative L1 entries");
    assert_eq!(sticky_store.lookups.load(Ordering::SeqCst), 5);
    assert_eq!(fixture.holder.generation(), start_generation);

    cancel.cancel();
    task.await.expect("listener task exits cleanly");
}

#[tokio::test]
async fn warmup_only_notify_listener_ignores_plugin_chain_and_rebuilds_on_upstream() {
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let listener = Arc::new(
        NotifyListener::new(NotifyListenerParams {
            notifier: notifier.clone(),
            cancel: cancel.clone(),
            holder: fixture.holder.clone(),
            stores: fixture.stores.clone(),
            runtime: fixture.runtime.clone(),
            aead: fixture.aead.clone(),
            data_dir: fixture._dir.path().to_path_buf(),
            lazy_refresher: None,
            subscription_quota_cache: fixture.quota_cache.clone(),
            prompt_cache_grace_margin_secs: 0,
            prompt_cache_observation_sink: Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
            subscription_quota_routing_max_staleness_secs: 1800,
            key_store: None,
            thread_sticky_subagent_latch: None,
            clock: Arc::new(cc_lb_clock::SystemClock),
            gemini_config: Some(fixture.gemini_config.clone()),
            antigravity_config: Some(fixture.antigravity_config.clone()),
        })
        .with_plugin_scope(cc_lb_server::dynamic_view_builder::DynamicViewPluginScope::WarmupOnly),
    );
    let task = tokio::spawn(async move {
        listener.run().await;
    });
    notifier.wait_for_subscriber().await;

    notifier.send(ChangeChannel::PluginChain);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        fixture.holder.generation(),
        start,
        "WarmupOnly notify listener must ignore PluginChain notifications"
    );

    notifier.send(ChangeChannel::Upstream);
    wait_for_generation(&fixture.holder, start + 1).await;

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
