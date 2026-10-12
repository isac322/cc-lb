//! Regression tests for periodic subscription-quota hydration (D4).
//!
//! Quota observations are outside the dynamic-view revision hash and their
//! change notification can be lost after the batch commits. Every reconcile
//! tick therefore re-reads committed quota observations for the resident
//! upstreams, even when the revision hash is unchanged, without publishing a
//! new view. The hydration path is backend-agnostic
//! (`UpstreamSubscriptionQuotaStore::list_latest_subscription_quota_for_upstreams`),
//! so real SQLite storage is used; SQLite emits no quota notification, which
//! stands in for a lost NOTIFY.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_control::DynamicViewHolder;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::SubscriptionQuotaCache;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::reconcile::Reconciler;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    MetaStore, StorageError, StorageResult, SubscriptionQuotaCheckpointRecord,
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamSubscriptionQuotaStore,
};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const AEAD_KEY: [u8; 32] = [25; 32];
const STALENESS_SECS: u64 = 1800;

fn now_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("millis fit in u64")
}

fn sample(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    utilization: f64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(observed_at_unix_millis / 1000 + 3600),
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
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

/// Pass-through quota store whose latest-observation read can be switched to
/// fail, to show that a failed periodic hydration does not fail reconcile. It
/// also counts latest-observation reads, and an armed [`ReadGate`] holds the
/// next successful read result until the test releases it.
struct SwitchableQuotaStore {
    inner: Arc<Storage>,
    fail_reads: AtomicBool,
    latest_reads: AtomicUsize,
    read_gate: Mutex<Option<ReadGate>>,
}

/// Signals once the storage read has returned, then waits for release before
/// handing the result to the caller.
struct ReadGate {
    read_returned: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[async_trait]
impl UpstreamSubscriptionQuotaStore for SwitchableQuotaStore {
    async fn record_subscription_quota_samples(
        &self,
        records: &[SubscriptionQuotaSample],
    ) -> StorageResult<()> {
        self.inner.record_subscription_quota_samples(records).await
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaSample>> {
        self.latest_reads.fetch_add(1, Ordering::AcqRel);
        if self.fail_reads.load(Ordering::Acquire) {
            return Err(StorageError::Unavailable {
                message: "injected quota read failure".to_owned(),
            });
        }
        let records = self
            .inner
            .list_latest_subscription_quota_for_upstreams(upstream_ids)
            .await?;
        let gate = self.read_gate.lock().expect("read gate lock").take();
        if let Some(gate) = gate {
            gate.read_returned
                .send(())
                .expect("test awaits the read signal");
            gate.release.await.expect("test releases the read");
        }
        Ok(records)
    }

    async fn put_subscription_quota_checkpoints(
        &self,
        records: &[SubscriptionQuotaCheckpointRecord],
    ) -> StorageResult<usize> {
        self.inner.put_subscription_quota_checkpoints(records).await
    }
}

struct Peer {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    quota_store: Arc<SwitchableQuotaStore>,
    stores: Arc<Stores>,
    quota: Arc<SubscriptionQuotaCache>,
    holder: Arc<DynamicViewHolder>,
    reconciler: Reconciler,
}

impl Peer {
    fn five_hour_utilization(&self, upstream_id: Uuid) -> Option<f64> {
        self.holder
            .load()
            .subscription_quota_cache
            .snapshot_for_upstream(upstream_id, now_millis(), STALENESS_SECS)
            .into_iter()
            .find(|snapshot| snapshot.window == "5h")
            .and_then(|snapshot| snapshot.utilization)
    }
}

async fn create_upstream(storage: &Storage, name: &str) -> UpstreamRecord {
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("upstream created");
    let ciphertext = AeadService::from_master_key(AEAD_KEY)
        .encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())
        .expect("api-key ciphertext");
    UpstreamStore::update_api_key_secret(storage, created.id, Some(ciphertext))
        .await
        .expect("upstream api-key secret")
}

async fn peer(upstream_names: &[&str]) -> (Peer, Vec<UpstreamRecord>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!(
        "sqlite://{}",
        dir.path().join("quota-hydration.sqlite").display()
    );
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("sqlite"),
    );
    storage.initialize().await.expect("init");
    let mut upstreams = Vec::new();
    for name in upstream_names {
        upstreams.push(create_upstream(&storage, name).await);
    }

    let quota_store = Arc::new(SwitchableQuotaStore {
        inner: storage.clone(),
        fail_reads: AtomicBool::new(false),
        latest_reads: AtomicUsize::new(0),
        read_gate: Mutex::new(None),
    });
    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: quota_store.clone(),
        upstream_subscription_metadata: storage.clone(),
        organization_metadata: storage.clone(),
        plan_tiers: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: None,
    });
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("wasmtime"));
    let aead = Arc::new(AeadService::from_master_key(AEAD_KEY));
    let quota = Arc::new(SubscriptionQuotaCache::new());
    let initial = build_dynamic_view(
        &stores,
        aead.clone(),
        None,
        0,
        &runtime,
        dir.path(),
        quota.clone(),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        STALENESS_SECS,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("initial view");
    let holder = Arc::new(DynamicViewHolder::new(initial));
    let reconciler = Reconciler::new(
        stores.clone(),
        holder.clone(),
        runtime,
        aead,
        None,
        CancellationToken::new(),
        dir.path().to_path_buf(),
        quota.clone(),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        STALENESS_SECS,
        Arc::new(cc_lb_clock::SystemClock),
    );
    (
        Peer {
            _dir: dir,
            storage,
            quota_store,
            stores,
            quota,
            holder,
            reconciler,
        },
        upstreams,
    )
}

/// A quota observation committed without any notification reaching this
/// peer is picked up by the next reconcile tick even though the revision
/// hash is unchanged, and the tick publishes no new view.
#[tokio::test]
async fn reconcile_hydrates_committed_quota_without_notification() {
    let (peer, upstreams) = peer(&["quota-upstream"]).await;
    let upstream = &upstreams[0];
    let generation = peer.holder.generation();
    assert_eq!(peer.five_hour_utilization(upstream.id), None);

    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, now_millis(), 0.42)])
        .await
        .expect("quota observation committed");
    assert_eq!(
        peer.five_hour_utilization(upstream.id),
        None,
        "no notification delivered the committed observation"
    );

    peer.reconciler.reconcile_once().await.expect("reconcile");

    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.42));
    assert_eq!(
        peer.holder.generation(),
        generation,
        "quota-only hydration must not publish a new view"
    );

    // A newer committed observation replaces it on the next tick.
    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, now_millis() + 1, 0.9)])
        .await
        .expect("newer quota observation committed");
    peer.reconciler.reconcile_once().await.expect("reconcile");
    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.9));
    assert_eq!(peer.holder.generation(), generation);
}

/// A failed periodic quota read is logged and does not fail an otherwise
/// successful reconcile tick or change the published view.
#[tokio::test]
async fn reconcile_continues_when_quota_hydration_fails() {
    let (peer, upstreams) = peer(&["quota-upstream"]).await;
    let upstream = &upstreams[0];
    let generation = peer.holder.generation();

    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, now_millis(), 0.42)])
        .await
        .expect("quota observation committed");
    peer.quota_store.fail_reads.store(true, Ordering::Release);

    peer.reconciler
        .reconcile_once()
        .await
        .expect("reconcile tick succeeds despite the quota read failure");
    assert_eq!(peer.holder.generation(), generation);
    assert_eq!(peer.five_hour_utilization(upstream.id), None);

    peer.quota_store.fail_reads.store(false, Ordering::Release);
    peer.reconciler.reconcile_once().await.expect("reconcile");
    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.42));
}

/// With no resident upstreams the tick skips the quota read entirely.
#[tokio::test]
async fn reconcile_skips_quota_hydration_without_upstreams() {
    let (peer, _) = peer(&[]).await;
    let generation = peer.holder.generation();
    // The initial build's own hydration may have read already; count only
    // reads issued by the reconcile tick.
    peer.quota_store.latest_reads.store(0, Ordering::Release);

    peer.reconciler.reconcile_once().await.expect("reconcile");
    assert_eq!(
        peer.quota_store.latest_reads.load(Ordering::Acquire),
        0,
        "no quota read may be issued for an empty upstream id list"
    );
    assert_eq!(peer.holder.generation(), generation);
}

/// Periodic hydration applies only strictly newer stored observations: a
/// resident observation with the same timestamp survives an unchanged-hash
/// tick, a strictly newer stored one replaces it, and the tick never
/// publishes a new view.
#[tokio::test]
async fn reconcile_keeps_resident_quota_on_equal_observed_at() {
    let (peer, upstreams) = peer(&["quota-upstream"]).await;
    let upstream = &upstreams[0];
    let generation = peer.holder.generation();
    let observed_at = now_millis();

    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, observed_at, 0.42)])
        .await
        .expect("stored observation A@T committed");
    peer.quota
        .upsert_observation(upstream.id, &sample(upstream.id, observed_at, 0.77));

    peer.reconciler.reconcile_once().await.expect("reconcile");
    assert_eq!(
        peer.five_hour_utilization(upstream.id),
        Some(0.77),
        "periodic hydration must not replace resident B@T with stored A@T"
    );
    assert_eq!(peer.holder.generation(), generation);

    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, observed_at + 1, 0.9)])
        .await
        .expect("stored observation A@T+1 committed");
    peer.reconciler.reconcile_once().await.expect("reconcile");
    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.9));
    assert_eq!(peer.holder.generation(), generation);
}

/// A periodic read that returned A@T before a same-timestamp observation B@T
/// was installed locally applies after B and must not restore A.
#[tokio::test]
async fn reconcile_delayed_read_does_not_restore_equal_observed_at() {
    let (peer, upstreams) = peer(&["quota-upstream"]).await;
    let upstream = &upstreams[0];
    let generation = peer.holder.generation();
    let observed_at = now_millis();

    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, observed_at, 0.42)])
        .await
        .expect("stored observation A@T committed");
    let (read_returned_tx, read_returned_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *peer.quota_store.read_gate.lock().expect("read gate lock") = Some(ReadGate {
        read_returned: read_returned_tx,
        release: release_rx,
    });

    let install_local = async {
        read_returned_rx.await.expect("storage read returned A@T");
        peer.quota
            .upsert_observation(upstream.id, &sample(upstream.id, observed_at, 0.77));
        release_tx.send(()).expect("reconcile awaits release");
    };
    let (reconciled, ()) = tokio::join!(peer.reconciler.reconcile_once(), install_local);
    reconciled.expect("reconcile");

    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.77));
    assert_eq!(peer.holder.generation(), generation);
}

/// Builder/NOTIFY hydration and direct upserts keep their existing policy:
/// an equal-timestamp observation replaces the resident one.
#[tokio::test]
async fn hydrate_from_store_and_upsert_replace_on_equal_observed_at() {
    let (peer, upstreams) = peer(&["quota-upstream"]).await;
    let upstream = &upstreams[0];
    let observed_at = now_millis();

    peer.quota
        .upsert_observation(upstream.id, &sample(upstream.id, observed_at, 0.77));
    peer.quota
        .upsert_observation(upstream.id, &sample(upstream.id, observed_at, 0.55));
    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.55));

    peer.storage
        .record_subscription_quota_samples(&[sample(upstream.id, observed_at, 0.42)])
        .await
        .expect("stored observation A@T committed");
    peer.quota
        .hydrate_from_store(&peer.stores, &[upstream.id])
        .await
        .expect("hydrate from store");
    assert_eq!(peer.five_hour_utilization(upstream.id), Some(0.42));
}
