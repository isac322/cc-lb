use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{AnthropicOAuthConfig, Config, SchedulerConfig, StorageConfig};
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::key_store::KeyStore;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{
    DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher, RequestSnapshot,
};
use cc_lb_engine::clock::{ClockHandle, TestClock};
use cc_lb_engine::{ApiKeyAwareSignerFactory, DynamicViewBuilder, DynamicViewHolder, SystemClock};
use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin, RoutingContext};
use cc_lb_runtime_wasmtime::{HotEngineConfig, WasmtimeRuntime};
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::oauth_refresh::RefreshOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, Filter, SchedulerPushTask, TaskStatus};
use cc_lb_storage_api::upstream::{
    UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStatusUpdate, UpstreamStore,
    UpstreamUpdate,
};
use cc_lb_storage_api::{BackendKind, MetaStore, Storage, StorageError, StorageResult};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::should_poll_oauth_usage;
use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;
use crate::dynamic_view_builder::Stores;
use crate::refresh::{LazyRefreshClaim, LazyRefreshClaimGuard, LazyRefresher, LazyRefresherDeps};
use crate::scheduler_dispatch::{SchedulerDispatch, SchedulerDispatchDeps};
use crate::subscription_quota_cache::SubscriptionQuotaCache;

#[test]
fn usage_poll_includes_disabled_registered_oauth_upstream() {
    let mut upstream = registered_oauth_upstream();
    upstream.enabled = false;
    upstream.warmup_enabled = false;

    assert!(should_poll_oauth_usage(&upstream));
}

#[test]
fn usage_poll_excludes_deleted_or_missing_credentials() {
    let mut deleted = registered_oauth_upstream();
    deleted.deleted_at_unix_secs = Some(1_800_000_000);
    let mut missing_credentials = registered_oauth_upstream();
    missing_credentials.oauth_credentials = None;

    assert!(!should_poll_oauth_usage(&deleted));
    assert!(!should_poll_oauth_usage(&missing_credentials));
}

#[test]
fn usage_poll_excludes_non_oauth_upstream() {
    let mut upstream = registered_oauth_upstream();
    upstream.kind = UpstreamKind::AnthropicApiKey;

    assert!(!should_poll_oauth_usage(&upstream));
}

#[test]
fn usage_poll_includes_never_refresh_upstream() {
    let mut upstream = registered_oauth_upstream();
    upstream.oauth_never_refresh = true;

    assert!(should_poll_oauth_usage(&upstream));
}

/// A long-lived (365-day) credential must never reach the token endpoint:
/// refreshing it makes Anthropic revoke the grant and issue an 8-hour token.
/// The dispatch's `token_url` points at a guaranteed-closed port, so if the
/// `never_refresh` guard were missing this call would fail with a connection
/// error instead of returning `NotRefreshable`.
#[tokio::test]
async fn refresh_upstream_returns_not_refreshable_for_long_lived_credential() {
    let fixture = DispatchFixture::new().await;
    let upstream = fixture.oauth_upstream(true).await;

    let outcome = fixture
        .dispatch
        .refresh_upstream(upstream)
        .await
        .expect("long-lived credential resolves without contacting the token endpoint");

    assert!(matches!(outcome, RefreshOutcome::NotRefreshable));
}

/// Contrast case: a refreshing credential is not short-circuited and the
/// refresh attempt reaches the token endpoint, which fails fast against the
/// fixture's closed port.
#[tokio::test]
async fn refresh_upstream_attempts_token_endpoint_for_refreshing_credential() {
    let fixture = DispatchFixture::new().await;
    let upstream = fixture.oauth_upstream(false).await;

    let outcome = fixture.dispatch.refresh_upstream(upstream).await;

    assert!(
        outcome.is_err(),
        "a refreshing credential must attempt the token endpoint, got {outcome:?}"
    );
}

/// The proactive usage-poll refresh must skip long-lived credentials entirely.
/// `refresh_one` reads the upstream record before anything else, so a zero
/// `get_by_id` count on the refresher's store proves it was never invoked.
#[tokio::test]
async fn ensure_fresh_usage_token_skips_lazy_refresh_for_long_lived_credential() {
    let fixture = DispatchFixture::new().await;
    let mut upstream = fixture.oauth_upstream(true).await;

    fixture
        .dispatch
        .ensure_fresh_usage_token(&mut upstream)
        .await
        .expect("long-lived credential skips the lazy refresher");

    assert_eq!(
        fixture.refresh_entry_calls.load(Ordering::SeqCst),
        0,
        "the lazy refresher must not be invoked for a long-lived credential"
    );
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 0);
}

/// Contrast case: an expired refreshing credential is handed to the lazy
/// refresher, which fetches the upstream and then reaches the claim guard.
/// The guard refuses the claim, and `ensure_fresh_usage_token` swallows that
/// failure by design.
#[tokio::test]
async fn ensure_fresh_usage_token_invokes_lazy_refresh_for_refreshing_credential() {
    let fixture = DispatchFixture::new().await;
    let mut upstream = fixture.oauth_upstream(false).await;

    fixture
        .dispatch
        .ensure_fresh_usage_token(&mut upstream)
        .await
        .expect("a refused claim is swallowed, not propagated");

    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 1);
}

/// The 401-retry force refresh must also skip long-lived credentials and
/// report that no refresh happened. Because a long-lived credential can never
/// be rotated, the terminal 401 is recorded durably as `status_401` so the
/// upstream surfaces as broken.
#[tokio::test]
async fn force_refresh_usage_token_skips_lazy_refresh_for_long_lived_credential() {
    let fixture = DispatchFixture::new().await;
    let mut upstream = fixture.oauth_upstream(true).await;

    let refreshed = fixture
        .dispatch
        .force_refresh_usage_token(&mut upstream)
        .await
        .expect("long-lived credential reports no refresh");

    assert!(!refreshed);
    assert_eq!(
        fixture.refresh_entry_calls.load(Ordering::SeqCst),
        0,
        "the lazy refresher must not be invoked for a long-lived credential"
    );
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 0);
    let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("upstream fetch")
        .expect("upstream exists");
    assert_eq!(stored.last_apply_error.as_deref(), Some("status_401"));
}

/// Contrast case: a refreshing credential reaches the lazy refresher; the
/// claim guard's refusal propagates as an error, proving the call was made.
#[tokio::test]
async fn force_refresh_usage_token_invokes_lazy_refresh_for_refreshing_credential() {
    let fixture = DispatchFixture::new().await;
    let mut upstream = fixture.oauth_upstream(false).await;

    let result = fixture
        .dispatch
        .force_refresh_usage_token(&mut upstream)
        .await;

    assert!(
        result.is_err(),
        "the claim guard's refusal must propagate for a refreshing credential"
    );
    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 1);
}

/// A successful usage poll clears a previously recorded apply error, mirroring
/// `complete_refresh` clearing the error on a successful token refresh. A
/// clean upstream is a no-op.
#[tokio::test]
async fn clear_recorded_apply_error_clears_stored_error() {
    let fixture = DispatchFixture::new().await;
    let upstream = fixture.oauth_upstream(true).await;

    fixture
        .dispatch
        .clear_recorded_apply_error(&upstream)
        .await
        .expect("clean upstream is a no-op");

    UpstreamStore::set_last_apply_error(
        fixture.storage.as_ref(),
        upstream.id,
        Some("status_401".to_owned()),
    )
    .await
    .expect("error recorded");
    let errored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("upstream fetch")
        .expect("upstream exists");
    assert_eq!(errored.last_apply_error.as_deref(), Some("status_401"));

    fixture
        .dispatch
        .clear_recorded_apply_error(&errored)
        .await
        .expect("error cleared");
    let cleared = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("upstream fetch")
        .expect("upstream exists");
    assert_eq!(cleared.last_apply_error, None);
}

/// Long-lived upstreams never reach the token-refresh path that schedules
/// `MetadataRefreshJob`, so the usage poll enqueues it instead. The job's
/// idempotency key dedupes repeated ticks: a second enqueue for the same
/// credential generation is a swallowed `Conflict`, not a second task.
#[tokio::test]
async fn enqueue_metadata_refresh_for_long_lived_dedupes_by_generation() {
    let fixture = DispatchFixture::new().await;
    let upstream = fixture.oauth_upstream(true).await;

    fixture
        .dispatch
        .enqueue_metadata_refresh_for_long_lived(&upstream, None)
        .await
        .expect("first enqueue succeeds");
    fixture
        .dispatch
        .enqueue_metadata_refresh_for_long_lived(&upstream, None)
        .await
        .expect("duplicate enqueue is a swallowed conflict");

    let tasks = fixture
        .dispatch
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(100),
        })
        .await
        .expect("list adaptive tasks");
    let metadata_tasks = tasks
        .iter()
        .filter(|task| matches!(task.args, AdaptiveJob::MetadataRefresh(_)))
        .count();
    assert_eq!(metadata_tasks, 1, "duplicate ticks must not pile up tasks");
}

fn registered_oauth_upstream() -> UpstreamRecord {
    UpstreamRecord {
        id: uuid::Uuid::new_v4(),
        name: "oauth-upstream".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        enabled: true,
        oauth_credentials: Some(EncryptedOAuthTokens::from_ciphertext(vec![1])),
        oauth_never_refresh: false,
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 1,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

/// `SchedulerDispatch::lazy_refresher` is the concrete `LazyRefresher`, so a
/// test cannot substitute a mock handle. The refresher is real and recording
/// happens on its two observable dependencies: `stores.upstreams`, whose
/// `get_by_id` is the first thing `refresh_one` does, and the claim guard,
/// whose `begin_refresh` runs once a refreshing bundle passes the refresher's
/// own `never_refresh` check.
struct DispatchFixture {
    _dir: TempDir,
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    aead: Arc<AeadService>,
    dispatch: SchedulerDispatch,
    refresh_entry_calls: Arc<AtomicUsize>,
    claim_calls: Arc<AtomicUsize>,
}

impl DispatchFixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite_path = dir.path().join("cc-lb.sqlite");
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(SystemClock))
                .await
                .expect("open sqlite"),
        );
        storage
            .initialize(BackendKind::Sqlite)
            .await
            .expect("initialize sqlite");
        let scheduler = crate::scheduler_factory::open_scheduler_storage(
            &StorageConfig::Sqlite {
                path: sqlite_path.clone(),
            },
            &SchedulerConfig::default(),
            Arc::new(SystemClock),
        )
        .await
        .expect("open scheduler storage");
        let storage_dyn: Arc<dyn Storage> = storage.clone();
        let aead = Arc::new(AeadService::from_master_key([9; 32]));
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse("http://127.0.0.1/oauth/authorize").expect("auth url"),
            token_url: refused_token_url(),
            redirect_uri: Url::parse("http://127.0.0.1/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        let refresh_entry_calls = Arc::new(AtomicUsize::new(0));
        let claim_calls = Arc::new(AtomicUsize::new(0));
        let refresher_stores = Arc::new(Stores {
            upstreams: Arc::new(CountingUpstreamStore {
                inner: storage.clone(),
                get_by_id_calls: refresh_entry_calls.clone(),
            }),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: storage.clone(),
            organization_metadata: storage.clone(),
            plan_tiers: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage_dyn.clone()),
        });
        let lazy_refresher = Arc::new(LazyRefresher::new_with_claim_guard(
            LazyRefresherDeps {
                stores: refresher_stores,
                aead: aead.clone(),
                oauth_cfg: oauth_cfg.clone(),
                clock: clock.clone(),
            },
            Uuid::new_v4(),
            None,
            CancellationToken::new(),
            Arc::new(RecordingClaimGuard {
                begin_refresh_calls: claim_calls.clone(),
            }),
            scheduler.backend.clone(),
        ));
        let dispatch_stores = Arc::new(Stores {
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
            audit: Some(storage_dyn.clone()),
        });
        let dynamic_view = Arc::new(DynamicViewHolder::new(
            DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(StubSignerFactory))
                .global_router(Arc::new(NoRouteRouter))
                .global_observability_hooks(Vec::new())
                .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
                .build(),
        ));
        let dispatch = SchedulerDispatch::new(SchedulerDispatchDeps {
            backend: scheduler.backend.clone(),
            cache_keepalive_pusher: Arc::new(NoopKeepalivePusher),
            config: Config::default(),
            storage: storage_dyn,
            stores: dispatch_stores,
            aead: aead.clone(),
            oauth_cfg,
            runtime: Arc::new(
                WasmtimeRuntime::new(HotEngineConfig::default()).expect("wasmtime runtime"),
            ),
            data_dir: dir.path().to_path_buf(),
            lazy_refresher: Some(lazy_refresher),
            subscription_quota_sink: cc_lb_engine::SubscriptionQuotaSink::new().0,
            subscription_quota_cache: Arc::new(SubscriptionQuotaCache::new()),
            cancel: CancellationToken::new(),
            replica_id: None,
            price_catalog: cc_lb_pricing::global_catalog().clone(),
            key_store: Arc::new(KeyStore::new(storage.clone())),
            limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone()),
            dynamic_view,
            keepalive_dispatcher: Arc::new(NoopKeepaliveDispatcher),
            event_bus: Arc::new(cc_lb_control::InMemoryBus::new()) as Arc<dyn RequestEventBus>,
            clock,
        });
        Self {
            _dir: dir,
            storage,
            aead,
            dispatch,
            refresh_entry_calls,
            claim_calls,
        }
    }

    /// Creates a stored OAuth upstream whose credential bundle is already
    /// expired, so a refreshing credential is always past the lookahead window
    /// and reaches the lazy refresher whenever the `never_refresh` guard does
    /// not stop it first.
    async fn oauth_upstream(&self, never_refresh: bool) -> UpstreamRecord {
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                name: format!("oauth-upstream-{never_refresh}"),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created");
        let encrypted = EncryptedOAuthTokens::encrypt(
            self.aead.as_ref(),
            &OAuthTokenBundle {
                access_token: "sk-ant-oat01-old".to_owned(),
                refresh_token: "sk-ant-ort01-old".to_owned(),
                expires_at_unix_secs: 1,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["messages".to_owned()],
                never_refresh,
            },
            record.id.as_bytes(),
        )
        .expect("tokens encrypt");
        UpstreamStore::store_oauth_tokens(
            self.storage.as_ref(),
            record.id,
            record.revision,
            encrypted,
            never_refresh,
        )
        .await
        .expect("tokens stored")
    }
}

/// A loopback URL whose port is guaranteed closed: the listener is bound and
/// immediately dropped, so any request fails fast without real network use.
fn refused_token_url() -> Url {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    Url::parse(&format!("http://{addr}/oauth/token")).expect("token url")
}

/// Counts `get_by_id` calls; `LazyRefresher::refresh_one` fetches the upstream
/// record before anything else, so this counter is a proxy for "the lazy
/// refresher was invoked".
struct CountingUpstreamStore {
    inner: Arc<dyn UpstreamStore>,
    get_by_id_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl UpstreamStore for CountingUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        self.inner.create(create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        self.inner.get_by_name(name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        self.get_by_id_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_by_id(id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        self.inner.list(after, limit).await
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.update(id, expected_revision, update).await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.set_enabled(id, expected_revision, enabled).await
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.update_spec(id, expected_revision, update).await
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        self.inner
            .update_api_key_secret(id, api_key_ciphertext)
            .await
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.update_oauth_token(id, tokens).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        self.inner.set_status(id, status).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
        never_refresh: bool,
    ) -> StorageResult<UpstreamRecord> {
        self.inner
            .store_oauth_tokens(id, expected_revision, tokens, never_refresh)
            .await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.complete_refresh(id, holder, tokens).await
    }

    async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
        self.inner.read_oauth_token_generation(id).await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.inner.set_last_apply_error(id, error).await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        self.inner.soft_delete(id, expected_revision).await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        self.inner.hard_delete(id).await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        self.inner
            .clear_warmup_dialect_plugin(id, expected_revision)
            .await
    }
}

/// Records `begin_refresh` calls and refuses every claim, so a refresh attempt
/// stops deterministically before any HTTP request or storage write.
struct RecordingClaimGuard {
    begin_refresh_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LazyRefreshClaimGuard for RecordingClaimGuard {
    async fn begin_refresh(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _expires_at_unix_secs: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.begin_refresh_calls.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::Unavailable {
            message: "test claim guard refuses claims".to_owned(),
        })
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(false)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(false)
    }
}

struct NoopKeepalivePusher;

#[async_trait]
impl CacheKeepaliveTaskPusher for NoopKeepalivePusher {
    async fn push_cache_keepalive_task(
        &self,
        _task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        Ok(())
    }
}

struct NoopKeepaliveDispatcher;

#[async_trait]
impl KeepaliveDispatcher for NoopKeepaliveDispatcher {
    async fn dispatch(
        &self,
        _snapshot: &RequestSnapshot,
        _context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        DispatchOutcome::Error("unused in oauth dispatch tests".to_owned())
    }
}

struct StubSignerFactory;

impl ApiKeyAwareSignerFactory for StubSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(StubSigner)
    }
}

struct StubSigner;

#[async_trait]
impl SignerFactory for StubSigner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(StubSigner))
    }
}

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

struct NoRouteRouter;

impl RouterPlugin for NoRouteRouter {
    fn route(
        &self,
        _ctx: &RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "unused in oauth dispatch tests".to_owned(),
        })
    }
}
