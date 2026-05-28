use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::DynamicViewHolder;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::notify_listener::{NotifyListener, NotifyListenerParams};
use cc_lb_storage_api::{
    ChangeChannel, ChangeEvent, RuntimeChangeNotifier, StorageError, StorageResult, UpstreamCreate,
    UpstreamRecord, UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_redb::Storage;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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
        let _ = self.tx.send(ChangeEvent::new(channel, "test"));
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
        if let Some(started) = &self.list_started {
            if let Some(sender) = started.lock().await.take() {
                let _ = sender.send(());
            }
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

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::store_oauth_tokens(&*self.inner, id, expected_revision, tokens).await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_refresh_lease(&*self.inner, id, holder, ttl_secs).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(&*self.inner, id, holder, tokens).await
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        UpstreamStore::release_lease_on_failure(&*self.inner, id, holder, reason).await
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
}

struct Fixture {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    oauth: Arc<AnthropicOAuthConfig>,
    aead: Arc<AeadService>,
    runtime: Arc<ExtismRuntime>,
    holder: Arc<DynamicViewHolder>,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage =
        Arc::new(Storage::open(&dir.path().join("notify.redb"), [24; 32]).expect("storage"));
    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        audit: None,
    });
    let oauth = Arc::new(AnthropicOAuthConfig::default());
    let aead = Arc::new(AeadService::from_master_key([24; 32]));
    let runtime = Arc::new(ExtismRuntime::new());
    let initial = build_dynamic_view(&stores, &oauth, aead.clone(), None, 0, &runtime, dir.path())
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
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("prometheus recorder installs");
    let fixture = fixture().await;
    let start = fixture.holder.generation();
    let before = labeled_counter_value(&handle, "cclb_rebind_total", "outcome", "error");
    let notifier = Arc::new(MockNotifier::new());
    let cancel = CancellationToken::new();
    let stores = Arc::new(Stores {
        upstreams: Arc::new(ControlledUpstreamStore::failing(fixture.storage.clone())),
        principals: fixture.storage.clone(),
        plugin_registry: fixture.storage.clone(),
        audit: None,
    });
    let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

    notifier.send(ChangeChannel::Upstream);
    for _ in 0..100 {
        let after = labeled_counter_value(&handle, "cclb_rebind_total", "outcome", "error");
        if after >= before + 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(fixture.holder.generation(), start);
    assert_eq!(
        labeled_counter_value(&handle, "cclb_rebind_total", "outcome", "error"),
        before + 1.0
    );
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
