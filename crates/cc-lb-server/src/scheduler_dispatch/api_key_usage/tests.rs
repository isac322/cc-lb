//! API-key usage poller through the real dispatch arm, against
//! fake-anthropic on loopback (QA POL, RPL-06, RCK-07, CONC-04, OAU-03).
//!
//! The org API origin and every `base_url` point at loopback fixtures; the
//! only non-loopback `base_url` is the custom-host case, which never dials.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use cc_lb_aead::AeadService;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::{AnthropicOAuthConfig, Config, SchedulerConfig, StorageConfig};
use cc_lb_control::anthropic_api_key::AnthropicOrgApi;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_domain::Upstream;
use cc_lb_engine::cache_keepalive::{
    DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher, RequestSnapshot,
};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};
use cc_lb_runtime_wasmtime::{HotEngineConfig, WasmtimeRuntime};
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::api_key_usage_poll::{
    ApiKeyUsagePollCronJob, immediate_poll_idempotency_key,
};
use cc_lb_scheduler::worker::{AdaptiveJob, CronJob, SchedulerCtx, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamUpdate};
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability,
    ApiKeyResolution, ApiKeyType, ApiKeyWorkspace, ApiKeyWorkspaceMode, ApiUsageAttribution,
    MetaStore, Storage, UpstreamApiKeyDetailsRecord, UpstreamApiKeyStore,
    UpstreamApiUsageBucketQuery, UpstreamApiUsageBucketRecord, UpstreamRecord, UpstreamStore,
};
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use fake_anthropic::{
    AppConfig, KeyPolicies, KeyPolicy, OAuthRefreshPause, RequestRecorder, Script,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;
use crate::dynamic_view_builder::Stores;
use crate::scheduler_dispatch::{SchedulerDispatch, SchedulerDispatchDeps};
use crate::subscription_quota_cache::SubscriptionQuotaCache;

use super::{
    CLAIM_EXCHANGE_BUDGET_SECS, CLAIM_LEASE_MARGIN_SECS, CLAIM_LEASE_SECS, ClaimWindow,
    POLL_CONCURRENCY, PollLabel, PollTimes,
};

/// 2026-10-20T13:45:00Z, day 20 of the month.
const NOW: u64 = 1_792_503_900;
/// 2026-10-01T00:00:00Z.
const OCT_1: u64 = 1_790_812_800;
/// 2026-10-20T13:00:00Z.
const HOUR_START: u64 = 1_792_501_200;
const INTERVAL: u64 = 300;
const USAGE_PATH: &str = "/v1/organizations/usage_report/messages";
const MODEL: &str = "claude-poller-test";

const KEY_A: &str = "sk-ant-api03-POLLERkeyAAAAAAAAAAAAAAAAAAAAAAAA0001";
const KEY_B: &str = "sk-ant-api03-POLLERkeyBBBBBBBBBBBBBBBBBBBBBBBB0002";

// ---------------------------------------------------------------- fixture

struct Fake {
    url: Url,
    policies: KeyPolicies,
    recorder: RequestRecorder,
}

impl Fake {
    async fn spawn() -> Self {
        Self::spawn_with(AppConfig::default()).await
    }

    async fn spawn_with(config: AppConfig) -> Self {
        let policies = config.key_policies.clone();
        let recorder = config.recorder.clone();
        let url = serve(fake_anthropic::app(config)).await;
        Self {
            url,
            policies,
            recorder,
        }
    }

    fn usage_requests(&self, key: &str) -> Vec<BTreeMap<String, Vec<String>>> {
        self.recorder
            .snapshot()
            .into_iter()
            .filter(|request| {
                request.path == USAGE_PATH && request.header("x-api-key") == Some(key)
            })
            .map(|request| query_map(request.query.as_deref().unwrap_or_default()))
            .collect()
    }

    fn usage_count(&self, key: &str) -> usize {
        self.usage_requests(key).len()
    }

    fn requests_for(&self, key: &str) -> usize {
        self.recorder
            .snapshot()
            .iter()
            .filter(|request| request.header("x-api-key") == Some(key))
            .count()
    }
}

async fn serve(router: Router) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback fixture");
    let addr: SocketAddr = listener.local_addr().expect("fixture addr");
    tokio::spawn(async move { axum::serve(listener, router).await });
    Url::parse(&format!("http://{addr}/")).expect("loopback url parses")
}

fn query_map(query: &str) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        map.entry(name.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    map
}

struct Fixture {
    _dir: TempDir,
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    storage_dyn: Arc<dyn Storage>,
    scheduler: crate::scheduler_factory::OpenedScheduler,
    aead: Arc<AeadService>,
    clock: Arc<TestClock>,
    catalog: Arc<PriceCatalog>,
    org_api: AnthropicOrgApi,
    config: Config,
    dynamic_view: Arc<DynamicViewHolder>,
    event_bus: Arc<cc_lb_control::InMemoryBus>,
    data_dir: std::path::PathBuf,
}

impl Fixture {
    async fn new(org_origin: Url) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite_path = dir.path().join("cc-lb.sqlite");
        let clock = Arc::new(TestClock::new_at_secs(NOW));
        let clock_handle: ClockHandle = clock.clone();
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(
                &format!("sqlite://{}", sqlite_path.display()),
                clock_handle,
            )
            .await
            .expect("open sqlite"),
        );
        storage.initialize().await.expect("initialize sqlite");
        let scheduler = crate::scheduler_factory::open_scheduler_storage(
            &StorageConfig::Sqlite { path: sqlite_path },
            &SchedulerConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("open scheduler storage");
        let dynamic_view = Arc::new(DynamicViewHolder::new(
            DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(NoopSignerFactory))
                .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
                .build(),
        ));
        let catalog = PriceCatalog::new_empty();
        catalog.install_snapshot(test_catalog());
        let storage_dyn: Arc<dyn Storage> = storage.clone();
        Self {
            data_dir: dir.path().to_path_buf(),
            _dir: dir,
            storage,
            storage_dyn,
            scheduler,
            aead: Arc::new(AeadService::from_master_key([5; 32])),
            clock,
            catalog,
            org_api: AnthropicOrgApi::with_origin(org_origin),
            config: Config::default(),
            dynamic_view,
            event_bus: Arc::new(cc_lb_control::InMemoryBus::new()),
        }
    }

    /// A fresh dispatch over the same storage, as after a server restart.
    fn dispatch(&self) -> SchedulerDispatch {
        let stores = Arc::new(Stores {
            upstreams: self.storage_dyn.clone(),
            principals: self.storage_dyn.clone(),
            plugin_registry: self.storage_dyn.clone(),
            upstream_rate_limits: self.storage_dyn.clone(),
            upstream_subscription_quotas: self.storage_dyn.clone(),
            upstream_subscription_metadata: self.storage_dyn.clone(),
            organization_metadata: self.storage_dyn.clone(),
            plan_tiers: self.storage_dyn.clone(),
            prompt_cache_observations: self.storage_dyn.clone(),
            anthropic_compatibility_kv: self.storage_dyn.clone(),
            audit: Some(self.storage_dyn.clone()),
        });
        SchedulerDispatch::new(SchedulerDispatchDeps {
            backend: self.scheduler.backend.clone(),
            cache_keepalive_pusher: Arc::new(RejectingPusher),
            config: self.config.clone(),
            storage: self.storage_dyn.clone(),
            stores,
            aead: self.aead.clone(),
            oauth_cfg: Arc::new(AnthropicOAuthConfig::default()),
            runtime: Arc::new(
                WasmtimeRuntime::new(HotEngineConfig::default()).expect("wasmtime runtime"),
            ),
            data_dir: self.data_dir.clone(),
            lazy_refresher: None,
            subscription_quota_sink: cc_lb_engine::SubscriptionQuotaSink::new().0,
            subscription_quota_cache: Arc::new(SubscriptionQuotaCache::new()),
            cancel: CancellationToken::new(),
            replica_id: None,
            price_catalog: self.catalog.clone(),
            key_store: Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(
                self.storage.clone(),
            )),
            limit_engine: LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                self.clock.clone(),
            ),
            dynamic_view: self.dynamic_view.clone(),
            keepalive_dispatcher: Arc::new(UnusedKeepaliveDispatcher),
            event_bus: self.event_bus.clone() as Arc<dyn RequestEventBus>,
            clock: self.clock.clone(),
            anthropic_org_api: self.org_api.clone(),
        })
    }

    fn now(&self) -> u64 {
        cc_lb_clock::unix_secs(cc_lb_clock::Clock::now(self.clock.as_ref()))
    }

    /// Runs one regular tick at the current clock time.
    async fn tick(&self) {
        self.tick_with(&self.dispatch()).await;
    }

    async fn tick_with(&self, dispatch: &SchedulerDispatch) {
        let outcome = dispatch
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(
                self.now(),
            )))
            .await
            .expect("api-key usage poll tick");
        assert_eq!(outcome, cc_lb_scheduler::retry::JobOutcome::Done);
    }

    /// Runs an immediate poll of one upstream at the current clock time.
    async fn poll_now(&self, upstream_id: Uuid) {
        self.dispatch()
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(
                ApiKeyUsagePollCronJob::for_upstream(self.now(), upstream_id),
            ))
            .await
            .expect("immediate api-key usage poll");
    }

    fn advance(&self, secs: u64) {
        self.clock.advance_secs(secs);
    }

    /// An api-key upstream whose credential is sealed with AAD = its id.
    async fn api_key_upstream(
        &self,
        name: &str,
        key: &str,
        base_url: Option<Url>,
        workspace_id: Option<&str>,
    ) -> UpstreamRecord {
        let created = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: workspace_id.map(str::to_owned),
                api_key_access: None,
            },
        )
        .await
        .expect("create api-key upstream");
        let ciphertext = self
            .aead
            .encrypt(key.as_bytes(), created.id.as_bytes())
            .expect("seal api key");
        UpstreamStore::update(
            self.storage.as_ref(),
            created.id,
            created.revision,
            UpstreamUpdate {
                api_key_ciphertext: Some(ciphertext),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("store api key")
    }

    /// An org-scoped key on the fixture origin whose usage report is `ok`.
    async fn org_upstream(
        &self,
        name: &str,
        key: &str,
        api_key_id: Option<&str>,
        workspace_id: Option<&str>,
    ) -> UpstreamRecord {
        let upstream = self
            .api_key_upstream(name, key, Some(self.org_api.origin().clone()), workspace_id)
            .await;
        self.write_access(&upstream, api_key_id, ApiKeyOrgCapability::Ok, NOW - 100)
            .await;
        upstream
    }

    async fn write_access(
        &self,
        upstream: &UpstreamRecord,
        api_key_id: Option<&str>,
        usage_report_state: ApiKeyOrgCapability,
        checked_at: u64,
    ) {
        self.storage
            .write_api_key_access(
                upstream.id,
                upstream.revision,
                &org_access(api_key_id, usage_report_state, checked_at),
            )
            .await
            .expect("write api-key access");
    }

    async fn details(&self, upstream_id: Uuid) -> UpstreamApiKeyDetailsRecord {
        self.storage
            .get_api_key_details(upstream_id)
            .await
            .expect("read details")
            .expect("details row exists")
    }

    async fn buckets(
        &self,
        upstream_id: Uuid,
        attribution: ApiUsageAttribution,
        subject_id: &str,
        bucket_width_secs: u32,
    ) -> Vec<UpstreamApiUsageBucketRecord> {
        self.storage
            .query_upstream_api_usage_buckets(&UpstreamApiUsageBucketQuery {
                upstream_id,
                attribution,
                subject_id: subject_id.to_owned(),
                bucket_width_secs,
                start_unix_secs: 0,
                end_unix_secs: i64::MAX as u64,
            })
            .await
            .expect("query buckets")
    }
}

fn org_access(
    api_key_id: Option<&str>,
    usage_report_state: ApiKeyOrgCapability,
    checked_at: u64,
) -> ApiKeyAccessWrite {
    ApiKeyAccessWrite {
        verified: true,
        key_class: ApiKeyClass::OrgScoped,
        key_hint: "sk-ant-api03-...test".to_owned(),
        api_key_id: api_key_id.map(str::to_owned),
        needs_workspace: true,
        capabilities: ApiKeyCapabilities {
            inference: ApiKeyInference::Ok,
            workspace: ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Selected,
                id: Some("wrkspc_P".to_owned()),
                name: None,
            },
            workspace_list: ApiKeyOrgCapability::Ok,
            api_key_resolution: if api_key_id.is_some() {
                ApiKeyResolution::Resolved
            } else {
                ApiKeyResolution::Unresolved
            },
            key_type: Some(ApiKeyType::ServiceAccount),
        },
        remediation: Vec::new(),
        usage_report_state: Some(usage_report_state),
        last_error: None,
        checked_at_unix_secs: Some(checked_at),
        last_attempt_at_unix_secs: checked_at,
    }
}

fn test_catalog() -> CatalogSnapshot {
    let mut snapshot = CatalogSnapshot::empty_cost_disabled();
    snapshot.status = CatalogStatus::Ok;
    snapshot.models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            by_tier: BTreeMap::new(),
        },
    );
    snapshot
        .cache_creation_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(3_750_000));
    snapshot
        .cache_read_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(300_000));
    snapshot
}

fn rfc3339(secs: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(i64::try_from(secs).expect("secs fit"), 0)
        .expect("valid timestamp")
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// One usage-report page with one bucket and the given results.
fn usage_page(start: u64, width: u64, results: Value) -> Value {
    json!({
        "data": [{
            "starting_at": rfc3339(start),
            "ending_at": rfc3339(start + width),
            "results": results,
        }],
        "has_more": false,
        "next_page": null,
    })
}

fn tokens(model: Option<&str>, uncached: u64, output: u64) -> Value {
    json!({
        "model": model,
        "uncached_input_tokens": uncached,
        "cache_creation": {"ephemeral_5m_input_tokens": 1_000_000, "ephemeral_1h_input_tokens": 1_000_000},
        "cache_read_input_tokens": 1_000_000,
        "output_tokens": output,
    })
}

/// A policy for an org-scoped key whose pages alternate hourly then daily.
fn org_policy(usage_pages: Vec<Value>) -> KeyPolicy {
    KeyPolicy {
        requires_workspace: true,
        usage_pages,
        ..KeyPolicy::default()
    }
}

fn default_pages() -> Vec<Value> {
    vec![
        usage_page(
            HOUR_START,
            3_600,
            json!([tokens(Some(MODEL), 1_000_000, 1_000_000)]),
        ),
        usage_page(OCT_1, 86_400, json!([tokens(None, 7, 9)])),
    ]
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(
        &self,
        _upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
        Err(cc_lb_upstream::SignerError::MissingCredentials {
            reason: "api-key poller tests never sign".to_owned(),
        })
    }
}

struct RejectingPusher;

#[async_trait]
impl CacheKeepaliveTaskPusher for RejectingPusher {
    async fn push_cache_keepalive_task(
        &self,
        _task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        Err(SchedulerError::Job(
            "api-key poller tests never keep caches alive".to_owned(),
        ))
    }
}

struct UnusedKeepaliveDispatcher;

#[async_trait]
impl KeepaliveDispatcher for UnusedKeepaliveDispatcher {
    async fn dispatch(
        &self,
        _snapshot: &RequestSnapshot,
        _context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        DispatchOutcome::Error("api-key poller tests never dispatch keepalives".to_owned())
    }
}

// ------------------------------------------------------------------ tests

/// POL-01, POL-03: a regular tick through the cron dispatch arm sends the
/// rolling 7-day `1h` page and the month-to-date `1d` page for the resolved
/// key id, with the credential decrypted under AAD = upstream id, and stores
/// list-priced buckets.
#[tokio::test]
async fn pol01_pol03_tick_reads_hourly_and_month_pages() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("org-a", KEY_A, Some("apikey_01"), Some("wrkspc_P"))
        .await;

    fixture.tick().await;

    let requests = fake.usage_requests(KEY_A);
    assert_eq!(requests.len(), 2, "{requests:?}");
    let expect = |starting_at: u64, width: &str, limit: &str| {
        BTreeMap::from([
            ("api_key_ids[]".to_owned(), vec!["apikey_01".to_owned()]),
            ("bucket_width".to_owned(), vec![width.to_owned()]),
            ("ending_at".to_owned(), vec![rfc3339(NOW)]),
            ("group_by[]".to_owned(), vec!["model".to_owned()]),
            ("limit".to_owned(), vec![limit.to_owned()]),
            ("starting_at".to_owned(), vec![rfc3339(starting_at)]),
        ])
    };
    assert_eq!(requests[0], expect(HOUR_START - 167 * 3_600, "1h", "168"));
    assert_eq!(requests[1], expect(OCT_1, "1d", "31"));
    assert_eq!(
        fake.requests_for(KEY_A),
        2,
        "no inference or other org calls"
    );

    let hourly = fixture
        .buckets(upstream.id, ApiUsageAttribution::ApiKey, "apikey_01", 3_600)
        .await;
    assert_eq!(hourly.len(), 1);
    assert_eq!(hourly[0].bucket_start_unix_secs, HOUR_START);
    assert_eq!(hourly[0].model, MODEL);
    // 1M tokens each: input $3 + output $15 + 5m write $3.75 + 1h write $6
    // + cache read $0.30.
    assert_eq!(hourly[0].list_cost_micros, 28_050_000);
    assert_eq!(hourly[0].observed_at_unix_secs, NOW);
    let daily = fixture
        .buckets(
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_01",
            86_400,
        )
        .await;
    assert_eq!(daily.len(), 1);
    assert_eq!(daily[0].bucket_start_unix_secs, OCT_1);
    assert_eq!(daily[0].model, "", "provider model null is stored as \"\"");
    assert_eq!(daily[0].uncached_input_tokens, 7);
    assert_eq!(daily[0].output_tokens, 9);
    assert_eq!(daily[0].list_cost_micros, 0, "an unknown model costs 0");

    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(details.usage_poll_failures, 0);
    assert_eq!(details.usage_last_success_at_unix_secs, Some(NOW));
    assert_eq!(details.usage_last_attempt_at_unix_secs, Some(NOW));
    assert_eq!(
        details.usage_next_poll_after_unix_secs,
        Some(NOW + INTERVAL)
    );
}

/// POL-01, POL-20: a pushed `api_key_usage_poll` task is consumed by the
/// scheduler's cron consumer and executed through the dispatch arm; the
/// immediate-poll key deduplicates repeated pushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pol01_pol20_pushed_poll_is_consumed_and_executed() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("org-a", KEY_A, Some("apikey_01"), None)
        .await;

    let mut scheduler_config = SchedulerConfig::default();
    for job in scheduler_config.recurring_jobs.values_mut() {
        job.enabled = false;
    }
    let dispatch = fixture.dispatch();
    let (done_tx, mut done_rx) = mpsc::channel::<(String, bool)>(4);
    let cron_dispatch: cc_lb_scheduler::worker::CronDispatchFn = Arc::new(move |job| {
        let dispatch = dispatch.clone();
        let done_tx = done_tx.clone();
        Box::pin(async move {
            let kind = job.kind().to_owned();
            let result = dispatch.dispatch_singleton(job).await;
            let _ = done_tx.send((kind, result.is_ok())).await;
            result
        })
    });
    let ctx = SchedulerCtx::new(
        scheduler_config,
        fixture.dispatch().adaptive_dispatch(),
        cron_dispatch,
        Arc::new(cc_lb_clock::SystemClock),
    );
    let cancel = CancellationToken::new();
    let handles = fixture
        .scheduler
        .backend
        .spawn(ctx, cancel.clone())
        .await
        .expect("spawn scheduler");

    let handle = SchedulerAdminHandle::new(fixture.scheduler.backend.clone());
    let push = || SchedulerPushTask {
        args: CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::for_upstream(NOW, upstream.id)),
        idempotency_key: Some(immediate_poll_idempotency_key(
            upstream.id,
            upstream.revision,
            NOW - 100,
        )),
        run_at_unix_secs: None,
        max_attempts: Some(1),
    };
    handle
        .push_cron_task(push())
        .await
        .expect("push immediate poll");
    assert!(matches!(
        handle.push_cron_task(push()).await,
        Err(SchedulerError::Conflict(_))
    ));

    let done = tokio::time::timeout(Duration::from_secs(30), done_rx.recv())
        .await
        .expect("immediate poll is consumed")
        .expect("dispatch channel open");
    cancel.cancel();
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }

    assert_eq!(done, ("api_key_usage_poll".to_owned(), true));
    assert_eq!(fake.usage_count(KEY_A), 2);
    assert_eq!(
        fixture
            .buckets(upstream.id, ApiUsageAttribution::ApiKey, "apikey_01", 3_600)
            .await
            .len(),
        1
    );
}

/// POL-02, OAU-03: only live, enabled, org-scoped `ok` rows are polled;
/// a legacy row with no state gets one lazy probe; deleted, disabled,
/// OAuth, denied, and not-applicable rows get zero calls.
#[tokio::test]
async fn pol02_oau03_candidate_set() {
    let fake = Fake::spawn().await;
    let keys = [
        "sk-ant-api03-CANDIDATEokLIVE00000000000000000001",
        "sk-ant-api03-CANDIDATEdeleted000000000000000000002",
        "sk-ant-api03-CANDIDATEdisabled00000000000000000003",
        "sk-ant-api03-CANDIDATEdenied00000000000000000000004",
        "sk-ant-api03-CANDIDATEnotapplic000000000000000005",
        "sk-ant-api03-CANDIDATElegacy00000000000000000000006",
    ];
    for key in keys {
        fake.policies.set(key, org_policy(default_pages()));
    }
    let fixture = Fixture::new(fake.url.clone()).await;
    let live = fixture
        .org_upstream("live", keys[0], Some("apikey_live"), None)
        .await;
    let deleted = fixture
        .org_upstream("deleted", keys[1], Some("apikey_del"), None)
        .await;
    UpstreamStore::soft_delete(fixture.storage.as_ref(), deleted.id, deleted.revision)
        .await
        .expect("soft delete");
    let disabled = fixture
        .org_upstream("disabled", keys[2], Some("apikey_dis"), None)
        .await;
    UpstreamStore::set_enabled(
        fixture.storage.as_ref(),
        disabled.id,
        disabled.revision,
        false,
    )
    .await
    .expect("disable");
    let denied = fixture
        .api_key_upstream("denied", keys[3], Some(fake.url.clone()), None)
        .await;
    fixture
        .write_access(
            &denied,
            Some("apikey_den"),
            ApiKeyOrgCapability::Denied,
            NOW - 100,
        )
        .await;
    let not_applicable = fixture
        .api_key_upstream("not-applicable", keys[4], Some(fake.url.clone()), None)
        .await;
    fixture
        .write_access(
            &not_applicable,
            Some("apikey_na"),
            ApiKeyOrgCapability::NotApplicable,
            NOW - 100,
        )
        .await;
    let legacy = fixture
        .api_key_upstream("legacy", keys[5], Some(fake.url.clone()), None)
        .await;
    let oauth = UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "oauth".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(fake.url.clone()),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create oauth upstream");

    fixture.tick().await;

    assert_eq!(fake.usage_count(keys[0]), 2, "live ok row runs one cycle");
    for (key, label) in [
        (keys[1], "deleted"),
        (keys[2], "disabled"),
        (keys[3], "denied"),
        (keys[4], "not applicable"),
    ] {
        assert_eq!(fake.requests_for(key), 0, "{label} row is never polled");
    }
    let legacy_requests = fake.usage_requests(keys[5]);
    assert_eq!(legacy_requests.len(), 1, "legacy row gets one lazy probe");
    assert_eq!(legacy_requests[0]["limit"], vec!["1".to_owned()]);
    assert_eq!(
        fake.recorder.snapshot().len(),
        3,
        "OAuth upstreams never reach the api-key poller"
    );
    assert!(
        fixture
            .storage
            .get_api_key_details(oauth.id)
            .await
            .expect("read oauth details")
            .is_none()
    );
    assert_eq!(
        fixture
            .details(live.id)
            .await
            .usage_last_success_at_unix_secs,
        Some(NOW)
    );
    assert_eq!(
        fixture.details(legacy.id).await.usage_report_state,
        Some(ApiKeyOrgCapability::Ok)
    );
}

/// POL-04: with no resolved key id the report is filtered to the selected
/// workspace and buckets are attributed to it.
#[tokio::test]
async fn pol04_unresolved_key_id_uses_workspace_filter() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("org-ws", KEY_A, None, Some("wrkspc_P"))
        .await;

    fixture.tick().await;

    let requests = fake.usage_requests(KEY_A);
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(request["workspace_ids[]"], vec!["wrkspc_P".to_owned()]);
        assert!(!request.contains_key("api_key_ids[]"));
    }
    assert_eq!(
        fixture
            .buckets(
                upstream.id,
                ApiUsageAttribution::Workspace,
                "wrkspc_P",
                86_400
            )
            .await
            .len(),
        1
    );
}

/// POL-06, RCK-07: a 403 or 401 demotes to `denied` (buckets kept) and
/// polling stops; only a Re-check that sees usage `ok` again brings the
/// upstream back, and the next tick polls it.
#[tokio::test]
async fn pol06_rck07_denial_demotes_until_recheck() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    fake.policies.set(KEY_B, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let forbidden = fixture
        .org_upstream("org-403", KEY_A, Some("apikey_01"), None)
        .await;
    let unauthorized = fixture
        .org_upstream("org-401", KEY_B, Some("apikey_02"), None)
        .await;
    fixture.tick().await;

    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::error(403, "permission_error", "no usage access"),
            ..org_policy(default_pages())
        },
    );
    fake.policies.set(
        KEY_B,
        KeyPolicy {
            usage_report: Script::error(401, "authentication_error", "invalid x-api-key"),
            ..org_policy(default_pages())
        },
    );
    fixture.advance(INTERVAL);
    fixture.tick().await;

    for (upstream, key, subject) in [
        (&forbidden, KEY_A, "apikey_01"),
        (&unauthorized, KEY_B, "apikey_02"),
    ] {
        let details = fixture.details(upstream.id).await;
        assert_eq!(
            details.usage_report_state,
            Some(ApiKeyOrgCapability::Denied)
        );
        assert_eq!(details.checked_at_unix_secs, Some(NOW - 100));
        assert_eq!(details.usage_last_success_at_unix_secs, Some(NOW));
        assert_eq!(fake.usage_count(key), 3, "two good + one denied request");
        assert_eq!(
            fixture
                .buckets(upstream.id, ApiUsageAttribution::ApiKey, subject, 3_600)
                .await
                .len(),
            1,
            "buckets survive demotion"
        );
    }

    fake.policies.set(KEY_A, org_policy(default_pages()));
    fixture.advance(INTERVAL);
    fixture.tick().await;
    assert_eq!(
        fake.usage_count(KEY_A),
        3,
        "a denied row is never re-polled"
    );

    // RCK-07: Re-check sees usage ok again.
    let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), forbidden.id)
        .await
        .expect("read upstream")
        .expect("upstream exists");
    fixture
        .write_access(
            &current,
            Some("apikey_01"),
            ApiKeyOrgCapability::Ok,
            fixture.now(),
        )
        .await;
    fixture.advance(INTERVAL);
    fixture.tick().await;

    assert_eq!(fake.usage_count(KEY_A), 5, "the next tick polls it again");
    let details = fixture.details(forbidden.id).await;
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(details.usage_last_success_at_unix_secs, Some(fixture.now()));
}

/// POL-07: 5xx answers keep the state `ok` and polling continues; the
/// consecutive-failure counter is persisted across a restart and reaches 2;
/// the next success clears it.
#[tokio::test]
async fn pol07_transient_failures_persist_across_restart() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::error(500, "api_error", "internal error"),
            ..org_policy(default_pages())
        },
    );
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("org-5xx", KEY_A, Some("apikey_01"), None)
        .await;

    fixture.tick_with(&fixture.dispatch()).await;
    assert_eq!(fixture.details(upstream.id).await.usage_poll_failures, 1);

    // Restart: a new dispatch over the same storage.
    let restarted = fixture.dispatch();
    fixture.advance(INTERVAL);
    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::error(529, "overloaded_error", "overloaded"),
            ..org_policy(default_pages())
        },
    );
    fixture.tick_with(&restarted).await;
    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_poll_failures, 2);
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(details.usage_last_success_at_unix_secs, None);
    assert_eq!(fake.usage_count(KEY_A), 2, "one failed request per tick");

    fake.policies.set(KEY_A, org_policy(default_pages()));
    fixture.advance(INTERVAL);
    fixture.tick_with(&restarted).await;
    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_poll_failures, 0);
    assert_eq!(details.usage_last_success_at_unix_secs, Some(fixture.now()));
    assert_eq!(fake.usage_count(KEY_A), 4);
}

/// POL-08: a legacy row (no details) gets exactly one lazy usage-report
/// probe over three ticks, never an inference or other org call; the state
/// becomes ok, denied, or error and a key hint is stored.
#[tokio::test]
async fn pol08_legacy_rows_probe_once() {
    let fake = Fake::spawn().await;
    let keys = [
        (
            "sk-ant-api03-LEGACYok000000000000000000000000AAA1",
            Script::Default,
            ApiKeyOrgCapability::Ok,
        ),
        (
            "sk-ant-api03-LEGACYdenied0000000000000000000000AAA2",
            Script::error(403, "permission_error", "no usage access"),
            ApiKeyOrgCapability::Denied,
        ),
        (
            "sk-ant-api03-LEGACYerror00000000000000000000000AAA3",
            Script::error(500, "api_error", "internal error"),
            ApiKeyOrgCapability::Error,
        ),
    ];
    for (key, script, _) in &keys {
        fake.policies.set(
            *key,
            KeyPolicy {
                usage_report: script.clone(),
                ..org_policy(Vec::new())
            },
        );
    }
    let fixture = Fixture::new(fake.url.clone()).await;
    let mut upstreams = Vec::new();
    for (index, (key, _, _)) in keys.iter().enumerate() {
        upstreams.push(
            fixture
                .api_key_upstream(
                    &format!("legacy-{index}"),
                    key,
                    Some(fake.url.clone()),
                    None,
                )
                .await,
        );
    }

    for _ in 0..3 {
        fixture.tick().await;
        fixture.advance(INTERVAL);
    }

    for ((key, _, state), upstream) in keys.iter().zip(&upstreams) {
        assert_eq!(fake.usage_count(key), 1, "{key}: one probe in total");
        assert_eq!(fake.requests_for(key), 1, "{key}: nothing but the probe");
        let details = fixture.details(upstream.id).await;
        assert_eq!(details.usage_report_state, Some(*state));
        assert_eq!(details.key_class, ApiKeyClass::Unknown);
        let hint = details.key_hint;
        assert!(hint.starts_with("sk-ant-api03-") && hint.ends_with(&key[key.len() - 4..]));
        assert!(!hint.contains(&key[13..30]), "hint never stores the key");
    }
}

/// POL-12 and the decrypt backoff: an undecryptable credential makes zero
/// outbound calls and records `error`; no poller log line carries key
/// material, only the upstream id and name.
#[tokio::test]
async fn pol12_poller_logs_never_carry_key_material() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::error(500, "api_error", "internal error"),
            ..org_policy(default_pages())
        },
    );
    fake.policies.set(
        KEY_B,
        KeyPolicy {
            usage_report: Script::error(403, "permission_error", "denied"),
            ..org_policy(default_pages())
        },
    );
    let fixture = Fixture::new(fake.url.clone()).await;
    let failing = fixture
        .org_upstream("failing-5xx", KEY_A, Some("apikey_01"), None)
        .await;
    let denied = fixture
        .org_upstream("denied-403", KEY_B, Some("apikey_02"), None)
        .await;
    let corrupt_key = "sk-ant-api03-CORRUPTciphertext0000000000000000CCCC";
    let corrupt = fixture
        .org_upstream("corrupt", corrupt_key, Some("apikey_03"), None)
        .await;
    let corrupt = UpstreamStore::update(
        fixture.storage.as_ref(),
        corrupt.id,
        corrupt.revision,
        UpstreamUpdate {
            api_key_ciphertext: Some(vec![0xde, 0xad, 0xbe, 0xef]),
            ..UpstreamUpdate::default()
        },
    )
    .await
    .expect("corrupt ciphertext");
    fixture
        .write_access(
            &corrupt,
            Some("apikey_03"),
            ApiKeyOrgCapability::Ok,
            NOW - 100,
        )
        .await;

    // tracing-core caches each callsite's interest globally. While at most
    // one dispatcher is registered, a callsite first reached on another
    // test's thread (no subscriber there) caches `never` for every thread,
    // so this test's lines could vanish. A second live dispatcher makes
    // registration consult every registered dispatcher instead.
    let _second_dispatcher = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let logs = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = {
        use tracing_subscriber::layer::SubscriberExt as _;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::filter::Targets::new()
                    .with_target("cc_lb_server", tracing::Level::DEBUG)
                    .with_target("cc_lb_control", tracing::Level::DEBUG)
                    .with_target("cc_lb_storage_sqlite", tracing::Level::DEBUG),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer({
                        let logs = logs.clone();
                        move || LogWriter(logs.clone())
                    }),
            )
    };
    {
        let _guard = tracing::subscriber::set_default(subscriber);
        fixture.tick().await;
    }

    assert_eq!(
        fake.requests_for(corrupt_key),
        0,
        "decrypt failure: zero outbound"
    );
    let details = fixture.details(corrupt.id).await;
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Error));
    assert_eq!(details.usage_poll_failures, 1);
    assert_eq!(
        details.usage_next_poll_after_unix_secs,
        Some(NOW + INTERVAL)
    );

    let logs = String::from_utf8(logs.lock().expect("log buffer").clone()).expect("utf8 logs");
    for upstream in [&failing, &denied, &corrupt] {
        assert!(
            logs.contains(&upstream.id.to_string()),
            "log names {}:\n{logs}",
            upstream.name
        );
    }
    for key in [KEY_A, KEY_B, corrupt_key] {
        for fragment in [key, &key[13..25], &key[key.len() - 12..]] {
            assert!(!logs.contains(fragment), "key material in logs:\n{logs}");
        }
    }
}

#[derive(Clone)]
struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Custom hosts never get org calls: the state becomes `not_applicable`
/// once, and the row leaves the candidate set.
#[tokio::test]
async fn custom_host_is_marked_not_applicable_without_calls() {
    let fake = Fake::spawn().await;
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .api_key_upstream(
            "custom-host",
            KEY_A,
            Some(Url::parse("https://proxy.example.com/").expect("custom url")),
            None,
        )
        .await;
    fixture
        .write_access(
            &upstream,
            Some("apikey_01"),
            ApiKeyOrgCapability::Ok,
            NOW - 100,
        )
        .await;

    fixture.tick().await;
    fixture.advance(INTERVAL);
    fixture.tick().await;

    assert!(fake.recorder.snapshot().is_empty());
    let details = fixture.details(upstream.id).await;
    assert_eq!(
        details.usage_report_state,
        Some(ApiKeyOrgCapability::NotApplicable)
    );
    assert_eq!(details.usage_last_attempt_at_unix_secs, Some(NOW));
}

/// POL-13: ten upstreams over one hour of 300 s ticks, with duplicate tick
/// executions and immediate polls in between, see one cycle (two requests)
/// per upstream per interval.
#[tokio::test]
async fn pol13_one_cycle_per_upstream_per_interval() {
    let fake = Fake::spawn().await;
    let fixture = Fixture::new(fake.url.clone()).await;
    let mut upstreams = Vec::new();
    let keys: Vec<String> = (0..10)
        .map(|index| format!("sk-ant-api03-RATEkey{index:02}000000000000000000000000RR{index:02}"))
        .collect();
    for (index, key) in keys.iter().enumerate() {
        fake.policies.set(key.clone(), org_policy(default_pages()));
        upstreams.push(
            fixture
                .org_upstream(
                    &format!("rate-{index}"),
                    key,
                    Some(&format!("apikey_{index:02}")),
                    None,
                )
                .await,
        );
    }

    // First tick at NOW + jitter, then every interval for one hour.
    fixture.advance(15);
    for tick in 0..12 {
        fixture.tick().await;
        // A retried execution of the same tick and an immediate poll
        // inside the interval add nothing.
        fixture.tick().await;
        fixture.advance(100);
        fixture.poll_now(upstreams[tick % 10].id).await;
        fixture.advance(INTERVAL - 100);
    }

    for key in &keys {
        assert_eq!(
            fake.usage_count(key),
            24,
            "{key}: 12 cycles of 2 requests in one hour"
        );
    }
}

/// POL-13 overlap: a regular tick and an immediate poll of the same due
/// upstream run concurrently. The tick claims the upstream before its first
/// provider call and is held there; the immediate poll then makes no call.
/// The tick's 429 (`retry-after: 1200`) defers the next poll to 2200 and
/// nothing replaces it with an earlier regular schedule, so another
/// immediate poll inside the window still makes no call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pol13_overlapping_regular_and_immediate_poll_send_one_cycle() {
    let pause = OAuthRefreshPause::new();
    let fake = Fake::spawn_with(AppConfig {
        usage_report_pause: Some(pause.clone()),
        ..AppConfig::default()
    })
    .await;
    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::sequence([
                Script::error(429, "rate_limit_error", "slow down")
                    .with_header("retry-after", "1200"),
                Script::Default,
            ]),
            ..org_policy(default_pages())
        },
    );
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("org-overlap", KEY_A, Some("apikey_01"), None)
        .await;

    let regular = fixture.dispatch();
    let overlap = async {
        let entered = tokio::time::timeout(Duration::from_secs(30), pause.wait_until_entered())
            .await
            .is_ok();
        if entered {
            // The tick holds the claim and waits on its first request.
            fixture.poll_now(upstream.id).await;
        }
        let during = fake.usage_count(KEY_A);
        pause.release();
        (entered, during)
    };
    let ((entered, during), ()) = tokio::join!(overlap, fixture.tick_with(&regular));
    assert!(entered, "the regular tick reached the provider");
    assert_eq!(during, 1, "the immediate poll made no call while claimed");
    assert_eq!(fake.usage_count(KEY_A), 1, "the 429 ended the tick's cycle");

    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_next_poll_after_unix_secs, Some(NOW + 1_200));
    assert_eq!(details.usage_poll_failures, 0);
    assert_eq!(
        details.usage_poll_claim_token, None,
        "completion releases the claim"
    );

    fixture.advance(300);
    fixture.poll_now(upstream.id).await;
    assert_eq!(
        fake.usage_count(KEY_A),
        1,
        "no call before the stored retry-after"
    );
    fixture.advance(900);
    fixture.tick().await;
    assert_eq!(fake.usage_count(KEY_A), 3, "one full cycle once it passes");
}

/// A 429 that arrives 45 s into a tick defers the next poll by the full
/// `retry-after` from the response time, not from the tick start.
async fn assert_late_429_defers_from_response(legacy: bool) {
    let pause = OAuthRefreshPause::new();
    let fake = Fake::spawn_with(AppConfig {
        usage_report_pause: Some(pause.clone()),
        ..AppConfig::default()
    })
    .await;
    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::error(429, "rate_limit_error", "slow down")
                .with_header("retry-after", "300"),
            ..org_policy(default_pages())
        },
    );
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = if legacy {
        fixture
            .api_key_upstream("late-429-legacy", KEY_A, Some(fake.url.clone()), None)
            .await
    } else {
        fixture
            .org_upstream("late-429", KEY_A, Some("apikey_01"), None)
            .await
    };

    let delayed = async {
        let entered = tokio::time::timeout(Duration::from_secs(30), pause.wait_until_entered())
            .await
            .is_ok();
        // The provider answers 45 s after the tick started.
        fixture.advance(45);
        pause.release();
        entered
    };
    let (entered, ()) = tokio::join!(delayed, fixture.tick());
    assert!(entered, "the tick reached the provider");
    assert_eq!(fake.usage_count(KEY_A), 1);

    let details = fixture.details(upstream.id).await;
    assert_eq!(
        details.usage_next_poll_after_unix_secs,
        Some(NOW + 45 + 300),
        "retry-after counts from the 429, not the tick start"
    );
    assert_eq!(details.usage_poll_failures, 0);
    if legacy {
        assert_eq!(
            details.usage_report_state, None,
            "a probe 429 decides nothing"
        );
    }
}

/// POL-14 delayed response, regular collection path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pol14_late_429_defers_from_response_time() {
    assert_late_429_defers_from_response(false).await;
}

/// POL-14 delayed response, lazy capability-probe path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pol14_late_probe_429_defers_from_response_time() {
    assert_late_429_defers_from_response(true).await;
}

/// Claim lease boundary: a candidate queued behind `POLL_CONCURRENCY`
/// in-flight ones claims late in the tick. Its lease counts from its own
/// claim, so while its request is in flight another poller cannot reclaim
/// it at the tick-start lease expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conc_late_claim_lease_counts_from_claim_time() {
    let (arrived_tx, mut arrived_rx) = mpsc::unbounded_channel();
    let origin = serve(
        Router::new()
            .route(USAGE_PATH, axum::routing::get(held_rate_limited_usage))
            .with_state(Arc::new(HeldUsage {
                arrived: arrived_tx,
            })),
    )
    .await;
    let fixture = Fixture::new(origin).await;
    let mut by_key = HashMap::new();
    for index in 0..=POLL_CONCURRENCY {
        let key = format!("sk-ant-api03-LEASEkey{index:040}");
        let upstream = fixture
            .org_upstream(&format!("lease-{index}"), &key, Some("apikey_01"), None)
            .await;
        by_key.insert(key, upstream);
    }

    let dispatch = fixture.dispatch();
    let now = fixture.now();
    let tick = tokio::spawn(async move {
        dispatch
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(now)))
            .await
    });
    let mut held = Vec::new();
    for _ in 0..POLL_CONCURRENCY {
        held.push(
            tokio::time::timeout(Duration::from_secs(30), arrived_rx.recv())
                .await
                .expect("a first-wave request arrives")
                .expect("gate open"),
        );
    }

    // The first wave is still in flight 50 s into the tick; one finishes,
    // which starts the queued candidate.
    fixture.advance(50);
    let (_, first) = held.pop().expect("one held request");
    let _ = first.send(());
    let (late_key, late_release) = tokio::time::timeout(Duration::from_secs(30), arrived_rx.recv())
        .await
        .expect("the queued candidate's request arrives")
        .expect("gate open");
    let late = by_key.get(&late_key).expect("known key");

    let details = fixture.details(late.id).await;
    assert!(
        details.usage_poll_claim_token.is_some(),
        "claimed in flight"
    );
    assert_eq!(
        details.usage_poll_claim_lease_until_unix_secs,
        Some(NOW + 50 + CLAIM_LEASE_SECS),
        "the lease counts from the claim, not the tick start"
    );
    let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), late.id)
        .await
        .expect("read upstream")
        .expect("upstream exists");
    let reclaimed = fixture
        .storage
        .claim_api_key_usage_poll(
            late.id,
            current.revision,
            Uuid::new_v4(),
            NOW + CLAIM_LEASE_SECS + CLAIM_LEASE_SECS,
            "",
            NOW + CLAIM_LEASE_SECS,
        )
        .await
        .expect("second claim attempt");
    assert!(
        !reclaimed,
        "no reclaim at the tick-start expiry while the request is in flight"
    );

    let _ = late_release.send(());
    for (_, release) in held {
        let _ = release.send(());
    }
    tick.await
        .expect("tick task joins")
        .expect("tick completes");
    let details = fixture.details(late.id).await;
    assert_eq!(details.usage_poll_claim_token, None, "completion releases");
    assert_eq!(
        details.usage_next_poll_after_unix_secs,
        Some(NOW + 50 + 600)
    );
}

/// Catch-up cadence: candidates queued behind `POLL_CONCURRENCY` held ones
/// finish half an interval after the tick started. Their next regular
/// deadline counts from when they ran, not the tick snapshot, so the next
/// tick does not poll them again; only the candidate released on time is
/// due.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pol_late_finish_deadline_counts_from_run_time() {
    let (arrived_tx, mut arrived_rx) = mpsc::unbounded_channel();
    let origin = serve(
        Router::new()
            .route(USAGE_PATH, axum::routing::get(held_failed_usage))
            .with_state(Arc::new(HeldUsage {
                arrived: arrived_tx,
            })),
    )
    .await;
    let fixture = Fixture::new(origin).await;
    let mut by_key = HashMap::new();
    for index in 0..=POLL_CONCURRENCY {
        let key = format!("sk-ant-api03-LATEkey{index:040}");
        let upstream = fixture
            .org_upstream(&format!("late-{index}"), &key, Some("apikey_01"), None)
            .await;
        by_key.insert(key, upstream);
    }

    let dispatch = fixture.dispatch();
    let now = fixture.now();
    let tick = tokio::spawn(async move {
        dispatch
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(now)))
            .await
    });
    let mut held = Vec::new();
    for _ in 0..POLL_CONCURRENCY {
        held.push(
            tokio::time::timeout(Duration::from_secs(30), arrived_rx.recv())
                .await
                .expect("a first-wave request arrives")
                .expect("gate open"),
        );
    }

    // Freeing one slot starts the queued candidate; it stays held while
    // half an interval passes, so the whole held wave finishes late.
    let (on_time_key, on_time_release) = held.pop().expect("one held request");
    let _ = on_time_release.send(());
    let (late_key, late_release) = tokio::time::timeout(Duration::from_secs(30), arrived_rx.recv())
        .await
        .expect("the queued candidate's request arrives")
        .expect("gate open");
    fixture.advance(INTERVAL + 30);
    let _ = late_release.send(());
    for (_, release) in held {
        let _ = release.send(());
    }
    tick.await
        .expect("tick task joins")
        .expect("tick completes");

    let late = by_key.get(&late_key).expect("known key");
    assert_eq!(
        fixture
            .details(late.id)
            .await
            .usage_next_poll_after_unix_secs,
        Some(NOW + INTERVAL + 30 + INTERVAL),
        "a late finish counts its deadline from when it ran"
    );
    let on_time = by_key.get(&on_time_key).expect("known key");
    assert_eq!(
        fixture
            .details(on_time.id)
            .await
            .usage_next_poll_after_unix_secs,
        Some(NOW + INTERVAL),
        "the on-time candidate keeps the tick-grid deadline"
    );

    // The next tick re-polls only the on-time candidate, whose deadline
    // legitimately passed; nothing that finished late is due.
    let mut re_polled = Vec::new();
    tokio::select! {
        () = fixture.tick() => {}
        () = async {
            while let Some((key, release)) = arrived_rx.recv().await {
                re_polled.push(key);
                let _ = release.send(());
            }
        } => {}
    }
    assert_eq!(re_polled, vec![on_time_key]);
}

/// Lease boundary between provider calls: the first page is held while
/// 40 s of the 60 s lease pass, leaving less than one 10 s call plus the
/// 16 s skew margin. The second page is never sent, the claim is released,
/// and no failure is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conc_second_call_not_started_without_enough_lease() {
    let pause = OAuthRefreshPause::new();
    let fake = Fake::spawn_with(AppConfig {
        usage_report_pause: Some(pause.clone()),
        ..AppConfig::default()
    })
    .await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("lease-spent", KEY_A, Some("apikey_01"), None)
        .await;

    let held = async {
        let entered = tokio::time::timeout(Duration::from_secs(30), pause.wait_until_entered())
            .await
            .is_ok();
        fixture.advance(40);
        pause.release();
        entered
    };
    let (entered, ()) = tokio::join!(held, fixture.tick());
    assert!(entered, "the first page reached the provider");
    assert_eq!(
        fake.usage_count(KEY_A),
        1,
        "the second page is not sent once the lease cannot cover it"
    );

    let details = fixture.details(upstream.id).await;
    assert_eq!(
        details.usage_poll_claim_token, None,
        "the claim is released"
    );
    assert_eq!(
        details.usage_poll_failures, 0,
        "a spent lease is not a provider failure"
    );
    assert_eq!(details.usage_last_success_at_unix_secs, None);
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(
        details.usage_next_poll_after_unix_secs,
        Some(NOW + INTERVAL)
    );

    // The next tick polls the whole cycle again.
    fixture.advance(INTERVAL);
    fixture.tick().await;
    assert_eq!(fake.usage_count(KEY_A), 3);
}

/// The call window is taken from clock reads after the claim returns: a
/// claim that waited on storage for most of its lease admits no call, and
/// each call must fit both the remaining lease (less the skew margin) and
/// the monotonic exchange budget.
#[test]
fn claim_window_charges_claim_wait_to_the_lease() {
    let per_probe = Duration::from_secs(10);
    let lease_until = NOW + CLAIM_LEASE_SECS;
    let returned = tokio::time::Instant::now();

    let prompt = ClaimWindow::after_claim(lease_until, returned, NOW);
    assert!(prompt.admits(per_probe, returned, NOW));
    let last_fit = CLAIM_LEASE_SECS - CLAIM_LEASE_MARGIN_SECS - 10;
    assert!(prompt.admits(per_probe, returned, NOW + last_fit));
    assert!(
        !prompt.admits(per_probe, returned, NOW + last_fit + 1),
        "a call that would end inside the skew margin is not started"
    );
    let budget_end = returned + Duration::from_secs(CLAIM_EXCHANGE_BUDGET_SECS);
    assert!(prompt.admits(per_probe, budget_end - per_probe, NOW));
    assert!(
        !prompt.admits(
            per_probe,
            budget_end - per_probe + Duration::from_secs(1),
            NOW
        ),
        "a call that would outrun the exchange budget is not started"
    );

    // The claim returned 50 s after the lease was computed.
    let late = ClaimWindow::after_claim(lease_until, returned, NOW + 50);
    assert!(
        !late.admits(per_probe, returned, NOW + 50),
        "a claim that spent its lease waiting on storage sends nothing"
    );
    // Wall time that did not move still leaves no monotonic budget.
    assert!(!late.admits(per_probe, returned, NOW));
}

/// Catch-up ticks: a backlog of elapsed ticks run back to back polls the
/// upstream once, and the next poll counts from when it ran.
#[tokio::test]
async fn pol_backlogged_ticks_poll_once() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("backlog", KEY_A, Some("apikey_01"), None)
        .await;

    let dispatch = fixture.dispatch();
    for behind in [3, 2, 1] {
        dispatch
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(
                NOW - behind * INTERVAL,
            )))
            .await
            .expect("catch-up tick");
    }
    assert_eq!(
        fake.usage_count(KEY_A),
        2,
        "one cycle for the whole backlog"
    );
    let details = fixture.details(upstream.id).await;
    assert_eq!(
        details.usage_next_poll_after_unix_secs,
        Some(NOW + INTERVAL),
        "the next poll counts from when the stale tick ran"
    );

    // The on-time tick at the current time also finds it not due.
    fixture.tick().await;
    assert_eq!(fake.usage_count(KEY_A), 2);
    fixture.advance(INTERVAL);
    fixture.tick().await;
    assert_eq!(fake.usage_count(KEY_A), 4, "regular cadence resumes");
}

/// An on-time tick keeps the scheduled cadence; one half an interval or
/// more late counts from when it ran.
#[test]
fn next_regular_poll_keeps_cadence_unless_behind() {
    let times = |now, tick| PollTimes {
        now,
        tick,
        interval: INTERVAL,
    };
    assert_eq!(times(NOW, NOW).next_regular_poll(), NOW + INTERVAL);
    assert_eq!(times(NOW + 20, NOW).next_regular_poll(), NOW + INTERVAL);
    assert_eq!(
        times(NOW + INTERVAL / 2 - 1, NOW).next_regular_poll(),
        NOW + INTERVAL
    );
    assert_eq!(
        times(NOW + INTERVAL / 2, NOW).next_regular_poll(),
        NOW + INTERVAL / 2 + INTERVAL
    );
    assert_eq!(
        times(NOW + 1_000, NOW).next_regular_poll(),
        NOW + 1_000 + INTERVAL
    );
}

/// POL-14: a 429 with `retry-after: 120` defers the next attempt; the
/// deferral is persisted, so a restarted server inside the window makes no
/// call, and the state never becomes `denied`.
#[tokio::test]
async fn pol14_rate_limit_deferral_survives_restart() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        KEY_A,
        KeyPolicy {
            usage_report: Script::error(429, "rate_limit_error", "slow down")
                .with_header("retry-after", "120"),
            ..org_policy(default_pages())
        },
    );
    let mut fixture = Fixture::new(fake.url.clone()).await;
    fixture
        .config
        .scheduler
        .recurring_jobs
        .get_mut("api_key_usage_poll")
        .expect("default poll job")
        .interval_secs = 60;
    let upstream = fixture
        .org_upstream("org-429", KEY_A, Some("apikey_01"), None)
        .await;

    fixture.tick().await;
    assert_eq!(fake.usage_count(KEY_A), 1);
    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_next_poll_after_unix_secs, Some(NOW + 120));
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(details.usage_poll_failures, 0);

    fake.policies.set(KEY_A, org_policy(default_pages()));
    let restarted = fixture.dispatch();
    fixture.advance(60);
    fixture.tick_with(&restarted).await;
    assert_eq!(
        fake.usage_count(KEY_A),
        1,
        "no call inside the retry window"
    );

    fixture.advance(60);
    fixture.tick_with(&restarted).await;
    assert_eq!(fake.usage_count(KEY_A), 3);
    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(details.usage_last_success_at_unix_secs, Some(NOW + 120));
}

/// POL-22: ten upstreams in one org under a 30-minute 429 storm each honor
/// `retry-after` (one attempt per interval, no immediate retry), none is
/// demoted, and all recover on the first tick after the storm.
#[tokio::test]
async fn pol22_same_org_429_storm() {
    let fake = Fake::spawn().await;
    let fixture = Fixture::new(fake.url.clone()).await;
    let keys: Vec<String> = (0..10)
        .map(|index| format!("sk-ant-api03-STORMkey{index:02}00000000000000000000000S{index:02}"))
        .collect();
    let storm = || KeyPolicy {
        usage_report: Script::error(429, "rate_limit_error", "slow down")
            .with_header("retry-after", "60"),
        ..org_policy(default_pages())
    };
    let mut upstreams = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        fake.policies.set(key.clone(), storm());
        upstreams.push(
            fixture
                .org_upstream(
                    &format!("storm-{index}"),
                    key,
                    Some(&format!("apikey_{index:02}")),
                    None,
                )
                .await,
        );
    }

    for tick in 1..=6 {
        fixture.tick().await;
        fixture.tick().await;
        for key in &keys {
            assert_eq!(fake.usage_count(key), tick, "{key}: one attempt per tick");
        }
        fixture.advance(INTERVAL);
    }
    for key in &keys {
        fake.policies.set(key.clone(), org_policy(default_pages()));
    }
    fixture.tick().await;

    for (key, upstream) in keys.iter().zip(&upstreams) {
        assert_eq!(
            fake.usage_count(key),
            8,
            "{key}: recovered on the first tick"
        );
        let details = fixture.details(upstream.id).await;
        assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
        assert_eq!(details.usage_poll_failures, 0);
        assert_eq!(details.usage_last_success_at_unix_secs, Some(fixture.now()));
    }
}

/// POL-15: with no api-key upstreams an hour of ticks makes zero outbound
/// calls, writes nothing, and enqueues no per-upstream poll job.
#[tokio::test]
async fn pol15_idle_ticks_cost_nothing() {
    let fake = Fake::spawn().await;
    let fixture = Fixture::new(fake.url.clone()).await;
    let oauth = UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "oauth-only".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
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
    .expect("create oauth upstream");

    for _ in 0..12 {
        fixture.tick().await;
        fixture.advance(INTERVAL);
    }

    assert!(fake.recorder.snapshot().is_empty());
    assert!(
        fixture
            .storage
            .get_api_key_details(oauth.id)
            .await
            .expect("read details")
            .is_none()
    );
    let scheduler_pool = match &fixture.scheduler.backend {
        cc_lb_scheduler::worker::SchedulerBackend::Sqlite(sqlite) => sqlite.pool().clone(),
        #[cfg(feature = "postgres")]
        cc_lb_scheduler::worker::SchedulerBackend::Postgres(_) => {
            unreachable!("fixture uses sqlite")
        }
    };
    let poll_jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE 'api_key_usage_poll:%'",
    )
    .fetch_one(&scheduler_pool)
    .await
    .expect("count poll jobs");
    assert_eq!(poll_jobs, 0);
}

/// RPL-06: after a key replace, the next tick reports only the new key id;
/// rows of the old key are kept and nothing of them is attributed to the
/// new key.
#[tokio::test]
async fn rpl06_old_key_usage_is_never_attributed_to_the_new_key() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    fake.policies.set(
        KEY_B,
        org_policy(vec![
            usage_page(HOUR_START, 3_600, json!([tokens(Some(MODEL), 11, 22)])),
            usage_page(OCT_1, 86_400, json!([tokens(Some(MODEL), 33, 44)])),
        ]),
    );
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("replace", KEY_A, Some("apikey_A"), None)
        .await;
    fixture.tick().await;

    let ciphertext = fixture
        .aead
        .encrypt(KEY_B.as_bytes(), upstream.id.as_bytes())
        .expect("seal new key");
    let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("read upstream")
        .expect("upstream exists");
    fixture.advance(30);
    UpstreamStore::update(
        fixture.storage.as_ref(),
        upstream.id,
        current.revision,
        UpstreamUpdate {
            api_key_ciphertext: Some(ciphertext),
            api_key_access: Some(org_access(
                Some("apikey_B"),
                ApiKeyOrgCapability::Ok,
                fixture.now(),
            )),
            reset_api_key_poll_state: true,
            ..UpstreamUpdate::default()
        },
    )
    .await
    .expect("replace key");
    fixture.poll_now(upstream.id).await;

    let new_requests = fake.usage_requests(KEY_B);
    assert_eq!(new_requests.len(), 2);
    for request in &new_requests {
        assert_eq!(request["api_key_ids[]"], vec!["apikey_B".to_owned()]);
    }
    let old = fixture
        .buckets(upstream.id, ApiUsageAttribution::ApiKey, "apikey_A", 86_400)
        .await;
    assert_eq!(old.len(), 1, "old-key rows are kept");
    assert_eq!(old[0].uncached_input_tokens, 7);
    let new = fixture
        .buckets(upstream.id, ApiUsageAttribution::ApiKey, "apikey_B", 86_400)
        .await;
    assert_eq!(new.len(), 1);
    assert_eq!(
        new[0].uncached_input_tokens, 33,
        "only the new key's figures"
    );
}

/// CONC-04: a tick that read key K1 and is blocked before its write loses
/// to a concurrent replace with K2: no K1 bucket is stored, and K2's poll
/// state is not set by K1's response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conc04_replace_during_poll_drops_the_stale_result() {
    let (arrived_tx, mut arrived_rx) = mpsc::channel::<()>(1);
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let gate = Arc::new(BlockingUsage {
        arrived: arrived_tx,
        release: Mutex::new(Some(release_rx)),
    });
    let origin = serve(
        Router::new()
            .route(USAGE_PATH, axum::routing::get(blocking_usage))
            .with_state(gate),
    )
    .await;
    let fixture = Fixture::new(origin).await;
    let upstream = fixture
        .org_upstream("conc", KEY_A, Some("apikey_K1"), None)
        .await;

    let dispatch = fixture.dispatch();
    let now = fixture.now();
    let tick = tokio::spawn(async move {
        dispatch
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(now)))
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), arrived_rx.recv())
        .await
        .expect("poll reaches the usage report")
        .expect("gate open");

    let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("read upstream")
        .expect("upstream exists");
    let ciphertext = fixture
        .aead
        .encrypt(KEY_B.as_bytes(), upstream.id.as_bytes())
        .expect("seal K2");
    UpstreamStore::update(
        fixture.storage.as_ref(),
        upstream.id,
        current.revision,
        UpstreamUpdate {
            api_key_ciphertext: Some(ciphertext),
            api_key_access: Some(org_access(
                Some("apikey_K2"),
                ApiKeyOrgCapability::Ok,
                now + 1,
            )),
            reset_api_key_poll_state: true,
            ..UpstreamUpdate::default()
        },
    )
    .await
    .expect("replace with K2");
    release_tx.send(()).expect("release the blocked poll");
    tick.await
        .expect("tick task joins")
        .expect("tick completes");

    for subject in ["apikey_K1", "apikey_K2"] {
        for width in [3_600, 86_400] {
            assert!(
                fixture
                    .buckets(upstream.id, ApiUsageAttribution::ApiKey, subject, width)
                    .await
                    .is_empty(),
                "{subject}/{width}: stale K1 result must be dropped"
            );
        }
    }
    let details = fixture.details(upstream.id).await;
    assert_eq!(details.api_key_id.as_deref(), Some("apikey_K2"));
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
    assert_eq!(details.usage_last_success_at_unix_secs, None);
    assert_eq!(details.usage_last_attempt_at_unix_secs, None);
    assert_eq!(details.usage_next_poll_after_unix_secs, None);
}

/// D6: disabling and re-enabling the upstream while its first poll is in
/// flight bumps the spec revision but changes nothing the poll read, so the
/// result is stored instead of dropped as a conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conc_enable_toggle_during_poll_keeps_the_result() {
    let (arrived_tx, mut arrived_rx) = mpsc::channel::<()>(1);
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let gate = Arc::new(BlockingUsage {
        arrived: arrived_tx,
        release: Mutex::new(Some(release_rx)),
    });
    let origin = serve(
        Router::new()
            .route(USAGE_PATH, axum::routing::get(blocking_usage))
            .with_state(gate),
    )
    .await;
    let fixture = Fixture::new(origin).await;
    let upstream = fixture
        .org_upstream("toggle", KEY_A, Some("apikey_K1"), None)
        .await;

    let dispatch = fixture.dispatch();
    let now = fixture.now();
    let upstream_id = upstream.id;
    let poll = tokio::spawn(async move {
        dispatch
            .dispatch_singleton(CronJob::ApiKeyUsagePoll(
                ApiKeyUsagePollCronJob::for_upstream(now, upstream_id),
            ))
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), arrived_rx.recv())
        .await
        .expect("poll reaches the usage report")
        .expect("gate open");

    let disabled = UpstreamStore::set_enabled(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        false,
    )
    .await
    .expect("disable");
    let enabled = UpstreamStore::set_enabled(
        fixture.storage.as_ref(),
        upstream.id,
        disabled.revision,
        true,
    )
    .await
    .expect("re-enable");
    assert!(enabled.revision > upstream.revision);
    release_tx.send(()).expect("release the blocked poll");
    poll.await
        .expect("poll task joins")
        .expect("poll completes");

    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_last_success_at_unix_secs, Some(now));
    assert_eq!(details.usage_poll_failures, 0);
    assert!(
        !fixture
            .buckets(upstream.id, ApiUsageAttribution::ApiKey, "apikey_K1", 3_600)
            .await
            .is_empty(),
        "the first poll's buckets are stored"
    );
}

/// A disable that lands after the candidate read but before the claim bumps
/// the revision, so the first claim conflicts. The retry must not re-claim
/// the now-disabled upstream: no claim, no provider call, no result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conc_disable_before_claim_never_polls_the_disabled_upstream() {
    let fake = Fake::spawn().await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("disable-before-claim", KEY_A, Some("apikey_01"), None)
        .await;
    let now = fixture.now();
    let mut candidates = fixture
        .storage
        .list_api_key_usage_poll_candidates(now, Some(upstream.id), 10)
        .await
        .expect("list candidates");
    assert_eq!(candidates.len(), 1, "the enabled upstream is a candidate");
    let candidate = candidates.remove(0);

    UpstreamStore::set_enabled(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        false,
    )
    .await
    .expect("disable");
    let label = fixture
        .dispatch()
        .poll_api_key_candidate(
            candidate,
            PollTimes {
                now,
                tick: now,
                interval: INTERVAL,
            },
        )
        .await;

    assert_eq!(label, PollLabel::Conflict);
    assert_eq!(fake.usage_count(KEY_A), 0, "no provider call after disable");
    let details = fixture.details(upstream.id).await;
    assert_eq!(details.usage_poll_claim_token, None, "never claimed");
    assert_eq!(details.usage_last_attempt_at_unix_secs, None);
    assert_eq!(details.usage_last_success_at_unix_secs, None);
}

/// A disable while the claimed poll waits on the usage report bumps the
/// revision; the completion must drop the result instead of retrying it
/// against the disabled spec.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conc_disable_during_poll_drops_the_result() {
    let pause = OAuthRefreshPause::new();
    let fake = Fake::spawn_with(AppConfig {
        usage_report_pause: Some(pause.clone()),
        ..AppConfig::default()
    })
    .await;
    fake.policies.set(KEY_A, org_policy(default_pages()));
    let fixture = Fixture::new(fake.url.clone()).await;
    let upstream = fixture
        .org_upstream("disable-during-poll", KEY_A, Some("apikey_01"), None)
        .await;

    let disable = async {
        let entered = tokio::time::timeout(Duration::from_secs(30), pause.wait_until_entered())
            .await
            .is_ok();
        UpstreamStore::set_enabled(
            fixture.storage.as_ref(),
            upstream.id,
            upstream.revision,
            false,
        )
        .await
        .expect("disable");
        pause.release();
        entered
    };
    let (entered, ()) = tokio::join!(disable, fixture.tick());
    assert!(entered, "the claimed poll reached the provider");

    let details = fixture.details(upstream.id).await;
    assert_eq!(
        details.usage_last_success_at_unix_secs, None,
        "the result of a poll that outlived a disable is dropped"
    );
    assert_eq!(details.usage_last_attempt_at_unix_secs, None);
    for width in [3_600, 86_400] {
        assert!(
            fixture
                .buckets(upstream.id, ApiUsageAttribution::ApiKey, "apikey_01", width)
                .await
                .is_empty(),
            "{width}: no bucket is stored for the disabled upstream"
        );
    }
}

/// D6: the tick log's `not_ok` leaves out the outcomes that mean the
/// claim/revision guards worked.
#[test]
fn claim_lost_and_conflict_are_not_failures() {
    assert!(!PollLabel::ClaimLost.is_failure());
    assert!(!PollLabel::Conflict.is_failure());
    assert!(!PollLabel::Success.is_failure());
    assert!(PollLabel::Failed.is_failure());
    assert!(PollLabel::StorageError.is_failure());
    assert!(PollLabel::RateLimited.is_failure());
}

#[test]
fn lease_spent_is_a_failure() {
    assert!(PollLabel::LeaseSpent.is_failure());
    assert_eq!(PollLabel::LeaseSpent.as_str(), "lease_spent");
}

struct BlockingUsage {
    arrived: mpsc::Sender<()>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

async fn blocking_usage(State(gate): State<Arc<BlockingUsage>>) -> impl IntoResponse {
    let release = gate.release.lock().expect("gate lock").take();
    if let Some(release) = release {
        let _ = gate.arrived.send(()).await;
        let _ = release.await;
    }
    (
        StatusCode::OK,
        axum::Json(usage_page(
            HOUR_START,
            3_600,
            json!([tokens(Some(MODEL), 5, 5)]),
        )),
    )
}

/// Holds every usage-report request until the test releases it, reporting
/// each request's key to the channel.
struct HeldUsage {
    arrived: mpsc::UnboundedSender<(String, oneshot::Sender<()>)>,
}

/// Answers `429` with `retry-after: 600`.
async fn held_rate_limited_usage(
    State(gate): State<Arc<HeldUsage>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let (release_tx, release_rx) = oneshot::channel();
    if gate.arrived.send((key, release_tx)).is_ok() {
        let _ = release_rx.await;
    }
    (
        StatusCode::TOO_MANY_REQUESTS,
        [("retry-after", "600")],
        axum::Json(json!({
            "type": "error",
            "error": {"type": "rate_limit_error", "message": "slow down"}
        })),
    )
}

/// Same gate as `held_rate_limited_usage`, answering `500` so each held
/// candidate makes exactly one request and keeps the regular deadline.
async fn held_failed_usage(
    State(gate): State<Arc<HeldUsage>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let (release_tx, release_rx) = oneshot::channel();
    if gate.arrived.send((key, release_tx)).is_ok() {
        let _ = release_rx.await;
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(json!({
            "type": "error",
            "error": {"type": "api_error", "message": "boom"}
        })),
    )
}
