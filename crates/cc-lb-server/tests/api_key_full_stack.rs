//! API-key upstreams through the whole server: the real app (proxy router,
//! admin router, scheduler worker, request-event pipeline, synchronous
//! rebind) built over one store, with fake-anthropic on loopback
//! (QA PF-03, UPD-03, RPL-05, ACC-03, BIL-12).
//!
//! The app always builds its org API at the production origin, and no test
//! dials it. PF-03 needs org probes, so its preflights go through a second
//! admin router over the same store whose org origin is the fake: the same
//! construction seam (D-FXGAP1) the admin crate's tests use.
//!
//! Waits are event-driven: proxied responses and admin rebinds are
//! synchronous, request events are read from the admin event stream, and
//! scheduler ticks from the tick-complete log line on this current-thread
//! runtime. Every wait is bounded by `BUDGET`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use axum::Router;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use cc_lb_admin::AdminState;
use cc_lb_aead::AeadService;
use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
use cc_lb_control::DynamicViewHolder;
use cc_lb_control::anthropic_api_key::AnthropicOrgApi;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::jobs::api_key_usage_poll::{
    ApiKeyUsagePollCronJob, immediate_poll_idempotency_key,
};
use cc_lb_scheduler::worker::{CronJob, SchedulerPushTask};
use cc_lb_server::App;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::{
    ApiKeyOrgCapability, ManagedKeyStore, MetaStore, PrincipalCreate, PrincipalKind,
    PrincipalStore, Storage as StorageTrait, UpstreamApiKeyDetailsRecord, UpstreamApiKeyStore,
    UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStore,
};
use fake_anthropic::{
    AppConfig, FakeApiKey, FakeWorkspace, KeyPolicies, KeyPolicy, MessageScript, RecordedRequest,
    RequestRecorder, Script, ScriptedMessageResponse,
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tower::ServiceExt;
use tracing::field::{Field, Visit};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};
use url::Url;
use uuid::Uuid;

const ADMIN_TOKEN: &str = env!("CARGO_PKG_NAME");
const UPSTREAMS: &str = "/admin/v1/upstreams";
const MESSAGES: &str = "/v1/messages";
const COUNT_TOKENS: &str = "/v1/messages/count_tokens";
const ORG_PREFIX: &str = "/v1/organizations/";
const WORKSPACE_HEADER: &str = "anthropic-workspace-id";
const MESSAGE_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#;
/// Upper bound on every wait and on PF-03's total preflight time.
const BUDGET: Duration = Duration::from_secs(30);

const KEY_1: &str = "sk-ant-api03-FULLSTACKone11111111111111111111-0001";
const KEY_2: &str = "sk-ant-api03-FULLSTACKtwo22222222222222222222-0002";
const LEGACY_NAME: &str = "sk-ant-api03-FULLSTACKlegacyNAMEnnnnnnnnnnnnn-0003";
const LEGACY_ID: &str = "sk-ant-api03-FULLSTACKlegacyIDiiiiiiiiiiiiiiii-0004";
const CREDIT_BALANCE: &str = "Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.";

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unix_now() -> u64 {
    cc_lb_clock::unix_secs(SystemTime::now())
}

// ---------------------------------------------------------------- fake

/// In-flight org Admin API requests, per `x-api-key` and in total.
#[derive(Clone)]
struct OrgGauge {
    /// key -> (in flight, peak in flight)
    per_key: Arc<Mutex<HashMap<String, (usize, usize)>>>,
    total: Arc<watch::Sender<usize>>,
}

impl Default for OrgGauge {
    fn default() -> Self {
        Self {
            per_key: Arc::default(),
            total: Arc::new(watch::channel(0).0),
        }
    }
}

impl OrgGauge {
    fn enter(&self, key: String) -> OrgRequest {
        {
            let mut per_key = lock(&self.per_key);
            let entry = per_key.entry(key.clone()).or_default();
            entry.0 += 1;
            entry.1 = entry.1.max(entry.0);
        }
        self.total.send_modify(|total| *total += 1);
        OrgRequest {
            gauge: self.clone(),
            key,
        }
    }

    fn peak(&self, key: &str) -> usize {
        lock(&self.per_key).get(key).map_or(0, |entry| entry.1)
    }

    fn in_flight(&self) -> usize {
        *self.total.borrow()
    }
}

struct OrgRequest {
    gauge: OrgGauge,
    key: String,
}

impl Drop for OrgRequest {
    fn drop(&mut self) {
        if let Some(entry) = lock(&self.gauge.per_key).get_mut(&self.key) {
            entry.0 = entry.0.saturating_sub(1);
        }
        self.gauge
            .total
            .send_modify(|total| *total = total.saturating_sub(1));
    }
}

async fn gauge_org_requests(State(org): State<OrgGauge>, request: Request, next: Next) -> Response {
    if !request.uri().path().starts_with(ORG_PREFIX) {
        return next.run(request).await;
    }
    let key = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let _in_flight = org.enter(key);
    next.run(request).await
}

struct Fake {
    url: Url,
    script: MessageScript,
    recorder: RequestRecorder,
    policies: KeyPolicies,
    org: OrgGauge,
}

impl Fake {
    async fn spawn() -> Self {
        let script = MessageScript::new();
        let config = AppConfig {
            message_script: Some(script.clone()),
            ..AppConfig::default()
        };
        let recorder = config.recorder.clone();
        let policies = config.key_policies.clone();
        let org = OrgGauge::default();
        let router = fake_anthropic::app(config).layer(middleware::from_fn_with_state(
            org.clone(),
            gauge_org_requests,
        ));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake-anthropic");
        let addr = listener.local_addr().expect("fake-anthropic addr");
        tokio::spawn(async move { axum::serve(listener, router).await });
        Self {
            url: Url::parse(&format!("http://{addr}/")).expect("fake url"),
            script,
            recorder,
            policies,
            org,
        }
    }

    fn requests(&self, path: &str) -> Vec<RecordedRequest> {
        self.recorder
            .snapshot()
            .into_iter()
            .filter(|request| request.path == path)
            .collect()
    }

    fn requests_with_key(&self, key: &str) -> usize {
        self.recorder
            .snapshot()
            .iter()
            .filter(|request| {
                request
                    .header("x-api-key")
                    .is_some_and(|value| value.contains(key))
            })
            .count()
    }
}

// ---------------------------------------------------------------- stage

type Seal = fn(&AeadService, &UpstreamRecord, &str) -> Vec<u8>;

/// The current shape: AAD = upstream id over the raw key bytes.
fn canonical(aead: &AeadService, record: &UpstreamRecord, key: &str) -> Vec<u8> {
    aead.encrypt(key.as_bytes(), record.id.as_bytes())
        .expect("seal canonical")
}

/// Pre-#848 create: AAD = upstream name over a JSON-quoted string.
fn legacy_json_under_name(aead: &AeadService, record: &UpstreamRecord, key: &str) -> Vec<u8> {
    let plaintext = serde_json::to_vec(key).expect("json encodes");
    aead.encrypt(&plaintext, record.name.as_bytes())
        .expect("seal legacy name")
}

/// Pre-#848 update: AAD = upstream id over a JSON-quoted string.
fn legacy_json_under_id(aead: &AeadService, record: &UpstreamRecord, key: &str) -> Vec<u8> {
    let plaintext = serde_json::to_vec(key).expect("json encodes");
    aead.encrypt(&plaintext, record.id.as_bytes())
        .expect("seal legacy id")
}

fn app_config(storage: StorageConfig, data_dir: &Path) -> Config {
    let mut config = Config {
        storage,
        ..Config::default()
    };
    config.runtime.data_dir = Some(data_dir.to_path_buf());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
        id: "test".to_owned(),
        token_env: crate::common::TEST_NONEMPTY_ENV.to_owned(),
    }];
    config
}

/// Storage seeded before the app starts.
struct Stage {
    dir: TempDir,
    storage: Arc<dyn StorageTrait>,
    managed: Arc<dyn ManagedKeyStore>,
    config: Config,
    aead: Arc<AeadService>,
    fake: Fake,
    upstreams: Vec<Uuid>,
    pinned: Vec<Vec<Uuid>>,
    _held: Vec<Box<dyn std::any::Any>>,
}

impl Stage {
    async fn sqlite() -> Self {
        let fake = Fake::spawn().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("full_stack.sqlite");
        let sqlite = cc_lb_storage_sqlite::open_sqlite(
            &format!("sqlite://{}", path.display()),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("sqlite opens");
        sqlite.initialize().await.expect("sqlite initializes");
        let sqlite = Arc::new(sqlite);
        let config = app_config(StorageConfig::Sqlite { path }, dir.path());
        Self {
            dir,
            storage: sqlite.clone(),
            managed: sqlite,
            config,
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            fake,
            upstreams: Vec::new(),
            pinned: Vec::new(),
            _held: Vec::new(),
        }
    }

    #[cfg(feature = "postgres")]
    async fn postgres(database_url: &str) -> Self {
        use cc_lb_storage_postgres::adapter::retry::RetryPolicy;
        use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage};

        let held = crate::common::postgres_test_lock(database_url)
            .await
            .expect("postgres test lock");
        let fake = Fake::spawn().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await
            .expect("postgres connects");
        let managed = Arc::new(PostgresManagedKeyStore::new(
            pool.clone(),
            Arc::new(RetryPolicy::default()),
            clock.clone(),
        ));
        let storage = Arc::new(PostgresStorage::new(pool, clock));
        storage.initialize().await.expect("postgres initializes");
        let mut config = app_config(
            StorageConfig::Postgres {
                url: database_url.to_owned(),
                pool: cc_lb_config::PostgresPoolConfig::default(),
            },
            dir.path(),
        );
        // Postgres always runs pg_notify fanout; borrow the always-set CI env
        // as the shared cluster token so the app can build.
        config.cluster.instance_url = Some("http://127.0.0.1:0".to_owned());
        config.cluster.token_env = "CI_POSTGRES_URL".to_owned();
        Self {
            dir,
            storage,
            managed,
            config,
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            fake,
            upstreams: Vec::new(),
            pinned: Vec::new(),
            _held: vec![Box::new(held)],
        }
    }

    /// An API-key upstream whose credential is `seal(key)`. Names are
    /// unique so stages can share a Postgres database.
    async fn upstream(
        &mut self,
        name: &str,
        base_url: Option<Url>,
        workspace_id: Option<&str>,
        key: &str,
        seal: Seal,
    ) -> UpstreamRecord {
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("{name}-{}", Uuid::new_v4().simple()),
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
        .expect("upstream created");
        let ciphertext = seal(&self.aead, &record, key);
        let record = UpstreamStore::update_api_key_secret(
            self.storage.as_ref(),
            record.id,
            Some(ciphertext),
        )
        .await
        .expect("credential stored");
        self.upstreams.push(record.id);
        record
    }

    /// A managed key whose principal may use only `upstreams`; returns its
    /// index in `Stack::keys` (index 0 may use every seeded upstream).
    fn pin(&mut self, upstreams: &[Uuid]) -> usize {
        self.pinned.push(upstreams.to_vec());
        self.pinned.len()
    }

    async fn boot(self) -> Stack {
        let now = unix_now();
        let key_store = KeyStore::new(self.managed.clone());
        let mut keys = Vec::new();
        for allowed in std::iter::once(self.upstreams.clone()).chain(self.pinned.iter().cloned()) {
            let name = format!("full-stack-{}", Uuid::new_v4().simple());
            PrincipalStore::create(
                self.storage.as_ref(),
                PrincipalCreate {
                    name: name.clone(),
                    kind: PrincipalKind::Machine,
                    allowed_models: Vec::new(),
                    allowed_upstreams: allowed,
                    default_limits: Vec::new(),
                    cache_keepalive: None,
                },
                now,
            )
            .await
            .expect("principal created");
            let (_record, secret) = key_store
                .create(
                    &name,
                    CreateParams {
                        label: name.clone(),
                        description: None,
                        expires_at_unix_secs: None,
                        limit_overrides: Vec::new(),
                    },
                )
                .await
                .expect("managed key created");
            keys.push(secret.expose().to_owned());
        }
        let app = build_app_with_storage(
            self.config.clone(),
            None,
            self.managed.clone(),
            self.storage.clone(),
            self.aead.clone(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("app builds");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let proxy_addr = listener.local_addr().expect("proxy addr");
        let router = app.router.clone();
        tokio::spawn(async move { axum::serve(listener, router).await });
        Stack {
            stage: self,
            app,
            proxy_addr,
            keys,
        }
    }
}

/// Runs a Postgres `scenario` in a private schema. The stage seeds
/// credentialed upstreams that point at its own fake, which dies with the
/// test; in the shared `public` schema they would stay routable for every
/// later test that lets its principal use any upstream. `scenario` gets the
/// database URL with `search_path` pinned to the schema, so every pool the
/// app opens from its config lands there too.
///
/// The scenario runs on its own runtime: the app's background tasks keep
/// writing until that runtime is dropped, and a `DROP SCHEMA` racing them
/// deadlocks.
#[cfg(feature = "postgres")]
fn run_in_pg_schema<F, Fut>(database_url: &str, scenario: F)
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    async fn ddl(database_url: &str, sql: String) {
        use sqlx::Connection as _;

        let mut connection = sqlx::PgConnection::connect(database_url)
            .await
            .expect("postgres connects");
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut connection)
            .await
            .expect("schema DDL runs");
        connection.close().await.expect("postgres disconnects");
    }
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    }

    let schema = format!("api_key_full_stack_{}", Uuid::new_v4().simple());
    runtime().block_on(ddl(database_url, format!("CREATE SCHEMA {schema}")));
    let mut url = Url::parse(database_url).expect("postgres URL parses");
    url.query_pairs_mut()
        .append_pair("options[search_path]", &schema);
    runtime().block_on(scenario(url.into()));
    runtime().block_on(ddl(
        database_url,
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
    ));
}

// ---------------------------------------------------------------- stack

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    fn etag(&self) -> String {
        self.headers
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }
}

fn admin_http_request(
    method: Method,
    uri: &str,
    body: Option<&Value>,
    if_match: Option<&str>,
) -> axum::http::Request<Body> {
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"));
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(tag) = if_match {
        builder = builder.header(header::IF_MATCH, tag);
    }
    builder
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .expect("admin request builds")
}

async fn admin_request(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> Reply {
    let response = router
        .clone()
        .oneshot(admin_http_request(method, uri, body.as_ref(), if_match))
        .await
        .expect("admin answers");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("admin body")
        .to_bytes();
    Reply {
        status,
        headers,
        body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    }
}

/// A running app plus the fake it proxies to.
struct Stack {
    stage: Stage,
    app: App,
    proxy_addr: SocketAddr,
    keys: Vec<String>,
}

impl Stack {
    fn fake(&self) -> &Fake {
        &self.stage.fake
    }

    /// One proxied `/v1/messages` with managed key `key`; returns the status.
    async fn send(&self, key: usize, headers: &[(&str, &str)]) -> u16 {
        let managed_key = self.keys.get(key).expect("managed key index");
        crate::common::http_post(
            self.proxy_addr,
            MESSAGES,
            managed_key,
            MESSAGE_BODY,
            headers,
        )
        .await
        .expect("proxy answers")
        .status
    }

    async fn admin(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
        if_match: Option<&str>,
    ) -> Reply {
        admin_request(&self.app.admin_router, method, uri, body, if_match).await
    }

    async fn detail(&self, id: Uuid) -> Reply {
        self.admin(Method::GET, &format!("{UPSTREAMS}/{id}"), None, None)
            .await
    }

    /// `month.billing_stop` from `GET /usage`.
    async fn billing_stop(&self, id: Uuid) -> Value {
        let reply = self
            .admin(
                Method::GET,
                &format!("{UPSTREAMS}/{id}/usage?range=24h&step=hour"),
                None,
                None,
            )
            .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        reply.body["month"]["billing_stop"].clone()
    }

    async fn record(&self, id: Uuid) -> UpstreamRecord {
        UpstreamStore::get_by_id(self.stage.storage.as_ref(), id)
            .await
            .expect("read upstream")
            .expect("upstream exists")
    }

    async fn details(&self, id: Uuid) -> Option<UpstreamApiKeyDetailsRecord> {
        UpstreamApiKeyStore::get_api_key_details(self.stage.storage.as_ref(), id)
            .await
            .expect("read details")
    }

    /// Pushes an immediate usage poll for `id` onto the app's scheduler
    /// queue; the app's own cron worker consumes it.
    async fn push_poll(&self, id: Uuid) {
        let opened = cc_lb_server::open_scheduler_storage(
            &self.stage.config.storage,
            &self.stage.config.scheduler,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("scheduler storage opens");
        let now = unix_now();
        let revision = self.record(id).await.revision;
        SchedulerAdminHandle::new(opened.backend)
            .push_cron_task(SchedulerPushTask {
                args: CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::for_upstream(now, id)),
                idempotency_key: Some(immediate_poll_idempotency_key(id, revision, now)),
                run_at_unix_secs: None,
                max_attempts: Some(1),
            })
            .await
            .expect("poll pushed");
    }

    /// Opens the admin request-event stream; the bus subscription exists
    /// once this returns, so no later event is missed.
    async fn events(&self) -> EventTail {
        let response = self
            .app
            .admin_router
            .clone()
            .oneshot(admin_http_request(
                Method::GET,
                "/admin/v1/events/stream",
                None,
                None,
            ))
            .await
            .expect("event stream answers");
        assert_eq!(response.status(), StatusCode::OK);
        EventTail {
            body: response.into_body().into_data_stream(),
            buffer: String::new(),
        }
    }
}

/// Final request events from the admin SSE stream.
struct EventTail {
    body: BodyDataStream,
    buffer: String,
}

impl EventTail {
    /// The next persisted request event (the bus publishes `final` after the
    /// row is appended).
    async fn next_final(&mut self) -> Value {
        tokio::time::timeout(BUDGET, async {
            loop {
                while let Some(end) = self.buffer.find("\n\n") {
                    let block: String = self.buffer.drain(..end + 2).collect();
                    if let Some(event) = final_event(&block) {
                        return event;
                    }
                }
                let chunk = self
                    .body
                    .next()
                    .await
                    .expect("event stream stays open")
                    .expect("event stream chunk");
                self.buffer.push_str(&String::from_utf8_lossy(&chunk));
            }
        })
        .await
        .expect("final request event within budget")
    }
}

fn final_event(block: &str) -> Option<Value> {
    let data = block
        .lines()
        .find_map(|line| line.strip_prefix("data:"))?
        .trim_start();
    let update: Value = serde_json::from_str(data).ok()?;
    (update["phase"] == "final").then(|| update["payload"]["event"].clone())
}

// ---------------------------------------------------------------- poll ticks

/// Forwards `only_upstream` of every "api-key usage poll tick complete"
/// event. The tick logs after its outcome rows are written.
struct PollTickLayer {
    tx: mpsc::UnboundedSender<String>,
}

#[derive(Default)]
struct TickFields {
    message: Option<String>,
    only_upstream: Option<String>,
}

impl Visit for TickFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = Some(format!("{value:?}")),
            "only_upstream" => self.only_upstream = Some(format!("{value:?}")),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for PollTickLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = TickFields::default();
        event.record(&mut fields);
        if fields.message.as_deref() == Some("api-key usage poll tick complete")
            && let Some(only_upstream) = fields.only_upstream
        {
            let _ = self.tx.send(only_upstream);
        }
    }
}

struct PollTicks {
    rx: mpsc::UnboundedReceiver<String>,
    _guard: tracing::subscriber::DefaultGuard,
}

/// Captures tick-complete events on this thread. Tests using it run on the
/// default current-thread runtime, so the app's scheduler worker logs here.
fn watch_poll_ticks() -> PollTicks {
    let (tx, rx) = mpsc::unbounded_channel();
    let subscriber = Registry::default().with(
        PollTickLayer { tx }.with_filter(
            Targets::new()
                .with_default(LevelFilter::OFF)
                .with_target("cc_lb_server::scheduler_dispatch", LevelFilter::INFO),
        ),
    );
    let guard = tracing::subscriber::set_default(subscriber);
    // A callsite another test thread registered before this subscriber
    // existed may have cached "disabled"; recompute it.
    tracing::callsite::rebuild_interest_cache();
    PollTicks { rx, _guard: guard }
}

impl PollTicks {
    async fn completed(&mut self, id: Uuid) {
        let wanted = format!("Some({id})");
        tokio::time::timeout(BUDGET, async {
            while let Some(only_upstream) = self.rx.recv().await {
                if only_upstream == wanted {
                    return;
                }
            }
            panic!("tick channel closed");
        })
        .await
        .expect("poll tick within budget");
    }
}

// ---------------------------------------------------------------- PF-03

/// Anthropic's `partial_key_hint`: 16 leading characters and the last four.
fn anthropic_hint(key: &str) -> String {
    format!(
        "{}...{}",
        key.get(..16).unwrap_or_default(),
        key.get(key.len().saturating_sub(4)..).unwrap_or_default()
    )
}

/// An org-scoped key whose org probes are slow: the workspace and key lists
/// answer inside the per-probe deadline, the usage report overruns it.
fn slow_org_policy(key: &str) -> KeyPolicy {
    KeyPolicy {
        requires_workspace: true,
        workspaces: vec![FakeWorkspace::new("wrkspc_P", "Production")],
        workspaces_list: Script::delay(8, Script::Default),
        api_keys: Script::delay(8, Script::Default),
        usage_report: Script::delay(12, Script::Default),
        api_key_items: vec![FakeApiKey {
            id: "apikey_pf03".to_owned(),
            partial_key_hint: anthropic_hint(key),
            principal: Some(json!({"type": "service_account_actor"})),
            scope: Some(json!({"type": "organization"})),
        }],
        ..KeyPolicy::default()
    }
}

/// The app's admin routes over the app's store, with the org origin at the
/// fake instead of production.
async fn admin_with_fake_org(stack: &Stack) -> Router {
    let stage = &stack.stage;
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let storage = stage.storage.clone();
    let stores = Stores {
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
        audit: Some(storage.clone()),
    };
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("wasm runtime"));
    let view = build_dynamic_view(
        &stores,
        stage.aead.clone(),
        None,
        0,
        &runtime,
        stage.dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        1800,
        clock.clone(),
    )
    .await
    .expect("dynamic view builds");
    let providers =
        cc_lb_admin::auth::build_providers(&stage.config.admin.auth).expect("admin auth builds");
    cc_lb_admin::router(AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage),
        key_store: Some(Arc::new(KeyStore::new(stage.managed.clone()))),
        aead: stage.aead.clone(),
        limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone()),
        lifecycle: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        dynamic_view: Arc::new(DynamicViewHolder::new(view)),
        config: Arc::new(stage.config.clone()),
        dynamic_view_rebinder: None,
        scheduler: None,
        admin_auth: Arc::new(cc_lb_admin::auth::AdminAuthenticator::new(providers)),
        start_time: Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
        anthropic_org_api: AnthropicOrgApi::with_origin(stage.fake.url.clone()),
    })
}

/// PF-03: 20 concurrent preflights against slow org probes finish within
/// the 30 s budget with 200s (never a hang or a 500); each preflight keeps
/// at most 4 org probes in flight; the overrunning probe reads `error` while
/// the finished ones are kept; proxy traffic flows meanwhile.
#[tokio::test]
async fn pf_03_slow_org_probes_stay_in_budget_and_leave_the_proxy_alone() {
    const PREFLIGHTS: usize = 20;
    const ORG_PROBES: usize = 3;
    let mut stage = Stage::sqlite().await;
    let fake_url = stage.fake.url.clone();
    stage
        .upstream("proxied", Some(fake_url.clone()), None, KEY_1, canonical)
        .await;
    let stack = stage.boot().await;
    let admin = admin_with_fake_org(&stack).await;
    let keys: Vec<String> = (0..PREFLIGHTS)
        .map(|index| format!("sk-ant-api03-PF03concurrent{index:02}xxxxxxxxxxxxxxxx-{index:04}"))
        .collect();
    for key in &keys {
        stack.fake().policies.set(key.clone(), slow_org_policy(key));
    }
    let mut total = stack.fake().org.total.subscribe();

    let started = Instant::now();
    let preflight_uri = format!("{UPSTREAMS}/preflight");
    let preflights = futures_util::future::join_all(keys.iter().map(|key| {
        admin_request(
            &admin,
            Method::POST,
            &preflight_uri,
            Some(json!({
                "api_key_value": key,
                "base_url": fake_url.as_str(),
                "workspace_id": "wrkspc_P",
            })),
            None,
        )
    }));
    let proxy_while_probing = async {
        tokio::time::timeout(
            BUDGET,
            total.wait_for(|in_flight| *in_flight >= PREFLIGHTS * ORG_PROBES),
        )
        .await
        .unwrap_or_else(|_| {
            let paths: Vec<String> = stack
                .fake()
                .recorder
                .snapshot()
                .iter()
                .map(|request| request.path.clone())
                .collect();
            panic!(
                "every org probe in flight; in flight {}, seen {paths:?}",
                stack.fake().org.in_flight()
            )
        })
        .expect("gauge open");
        let mut statuses = Vec::new();
        for _ in 0..5 {
            statuses.push(stack.send(0, &[]).await);
        }
        (statuses, stack.fake().org.in_flight())
    };
    let (replies, (proxy_statuses, in_flight_after_proxy)) =
        tokio::join!(preflights, proxy_while_probing);
    let elapsed = started.elapsed();

    assert!(elapsed <= BUDGET, "preflights took {elapsed:?}");
    assert_eq!(proxy_statuses, vec![200; 5], "proxy traffic during probes");
    assert_eq!(
        in_flight_after_proxy,
        PREFLIGHTS * ORG_PROBES,
        "proxied requests finished while every org probe was still pending"
    );
    for (key, reply) in keys.iter().zip(&replies) {
        assert_eq!(reply.status, StatusCode::OK, "{key}: {}", reply.body);
        assert_eq!(
            reply
                .headers
                .get(header::CACHE_CONTROL)
                .map(|v| v.as_bytes()),
            Some(&b"no-store"[..]),
            "{key}"
        );
        let capabilities = &reply.body["capabilities"];
        assert_eq!(
            capabilities["usage_report"], "error",
            "{key}: {}",
            reply.body
        );
        assert_eq!(
            capabilities["workspace_list"], "ok",
            "{key}: {}",
            reply.body
        );
        assert_eq!(capabilities["inference"], "ok", "{key}: {}", reply.body);
        let peak = stack.fake().org.peak(key);
        assert!((1..=4).contains(&peak), "{key}: peak org probes {peak}");
    }
}

// ---------------------------------------------------------------- UPD-03 / RPL-05

/// UPD-03: a workspace PUT on a running server moves the next proxied
/// request to the new `anthropic-workspace-id` without a restart.
async fn upd_03_scenario(mut stage: Stage) {
    let fake_url = stage.fake.url.clone();
    let upstream = stage
        .upstream("org", Some(fake_url), Some("wrkspc_P"), KEY_1, canonical)
        .await;
    let stack = stage.boot().await;

    assert_eq!(stack.send(0, &[]).await, 200);
    let before = stack.detail(upstream.id).await;
    assert_eq!(before.status, StatusCode::OK, "{}", before.body);
    let put = stack
        .admin(
            Method::PUT,
            &format!("{UPSTREAMS}/{}", upstream.id),
            Some(json!({"workspace_id": "wrkspc_S"})),
            Some(&before.etag()),
        )
        .await;
    assert_eq!(put.status, StatusCode::OK, "{}", put.body);
    assert_eq!(put.body["spec_revision"], upstream.revision + 1);
    assert!(put.headers.contains_key("x-cc-lb-generation"), "rebind ran");
    assert_eq!(
        stack.record(upstream.id).await.workspace_id.as_deref(),
        Some("wrkspc_S"),
        "{}",
        put.body
    );
    assert_eq!(stack.send(0, &[]).await, 200);

    let workspaces: Vec<Option<String>> = stack
        .fake()
        .requests(MESSAGES)
        .iter()
        .map(|request| request.header(WORKSPACE_HEADER).map(str::to_owned))
        .collect();
    assert_eq!(
        workspaces,
        vec![Some("wrkspc_P".to_owned()), Some("wrkspc_S".to_owned())]
    );
}

#[tokio::test]
async fn upd_03_workspace_change_rebinds_live_sqlite() {
    upd_03_scenario(Stage::sqlite().await).await;
}

#[cfg(feature = "postgres")]
#[test]
fn upd_03_workspace_change_rebinds_live_postgres() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };
    run_in_pg_schema(&url, |url| async move {
        upd_03_scenario(Stage::postgres(&url).await).await;
    });
}

/// RPL-05: a key replace on a running server signs the next proxied request
/// with the new key at once.
#[tokio::test]
async fn rpl_05_key_replace_rebinds_live() {
    let mut stage = Stage::sqlite().await;
    let fake_url = stage.fake.url.clone();
    let upstream = stage
        .upstream("replace", Some(fake_url), None, KEY_1, canonical)
        .await;
    let stack = stage.boot().await;

    assert_eq!(stack.send(0, &[]).await, 200);
    let before = stack.detail(upstream.id).await;
    let put = stack
        .admin(
            Method::PUT,
            &format!("{UPSTREAMS}/{}", upstream.id),
            Some(json!({"api_key_value": KEY_2})),
            Some(&before.etag()),
        )
        .await;
    assert_eq!(put.status, StatusCode::OK, "{}", put.body);
    assert_eq!(put.body["spec_revision"], upstream.revision + 1);
    assert_eq!(stack.send(0, &[]).await, 200);

    let keys: Vec<Option<String>> = stack
        .fake()
        .requests(MESSAGES)
        .iter()
        .map(|request| request.header("x-api-key").map(str::to_owned))
        .collect();
    assert_eq!(keys, vec![Some(KEY_1.to_owned()), Some(KEY_2.to_owned())]);
}

// ---------------------------------------------------------------- ACC-03

/// No byte sequence of the key, quoted or not, appears in `text`.
fn assert_no_key(text: &str, key: &str, label: &str) {
    assert!(!text.contains(key), "{label}: {text}");
}

/// ACC-03 per OI-1 (a): the canonical shape routes, polls, and re-checks;
/// both legacy shapes fail closed with a non-secret apply error, read as
/// not checked, answer Re-check and the workspace list with 500
/// `api_key_credential_decrypt_failed`, and the poller records a decrypt
/// failure and backs off without any outbound call.
async fn acc_03_scenario(mut stage: Stage) {
    let fake_url = stage.fake.url.clone();
    let canonical_row = stage
        .upstream("canonical", Some(fake_url), None, KEY_1, canonical)
        .await;
    let legacy_rows = [
        (
            "name-aad",
            stage
                .upstream(
                    "legacy-name",
                    None,
                    None,
                    LEGACY_NAME,
                    legacy_json_under_name,
                )
                .await,
            LEGACY_NAME,
        ),
        (
            "id-aad",
            stage
                .upstream("legacy-id", None, None, LEGACY_ID, legacy_json_under_id)
                .await,
            LEGACY_ID,
        ),
    ];
    let stack = stage.boot().await;
    let mut ticks = watch_poll_ticks();

    // Proxy: only the canonical row is routable, and it sends the bare key.
    assert_eq!(stack.send(0, &[]).await, 200);
    let sent: Vec<Option<String>> = stack
        .fake()
        .requests(MESSAGES)
        .iter()
        .map(|request| request.header("x-api-key").map(str::to_owned))
        .collect();
    assert_eq!(sent, vec![Some(KEY_1.to_owned())]);

    // Detail: canonical is healthy; legacy rows carry the apply error and
    // no access snapshot (NOT_CHECKED).
    let canonical_detail = stack.detail(canonical_row.id).await;
    assert!(canonical_detail.body["status"]["last_apply_error"].is_null());
    for (label, row, key) in &legacy_rows {
        let detail = stack.detail(row.id).await;
        assert_eq!(detail.status, StatusCode::OK, "{label}");
        let error = detail.body["status"]["last_apply_error"]
            .as_str()
            .unwrap_or_default();
        assert!(
            error.starts_with("anthropic api-key upstream credential"),
            "{label}: {}",
            detail.body
        );
        assert!(
            detail.body.get("api_key_access").is_none(),
            "{label}: {}",
            detail.body
        );
        assert!(
            detail.body.get("key_hint").is_none(),
            "{label}: {}",
            detail.body
        );
        assert_no_key(&detail.body.to_string(), key, label);
    }

    // Poller: the canonical row resolves; legacy rows record a decrypt
    // failure. Nothing reaches the fake for any of them.
    let outbound_before = stack.fake().recorder.snapshot().len();
    for id in std::iter::once(canonical_row.id).chain(legacy_rows.iter().map(|row| row.1.id)) {
        stack.push_poll(id).await;
        ticks.completed(id).await;
    }
    assert_eq!(stack.fake().recorder.snapshot().len(), outbound_before);
    let polled = stack
        .details(canonical_row.id)
        .await
        .expect("canonical polled");
    assert_eq!(
        polled.usage_report_state,
        Some(ApiKeyOrgCapability::NotApplicable)
    );
    assert_eq!(polled.usage_poll_failures, 0);
    assert_eq!(polled.key_hint, anthropic_style_hint(KEY_1));
    let now = unix_now();
    for (label, row, _key) in &legacy_rows {
        let polled = stack
            .details(row.id)
            .await
            .expect("legacy poll outcome row");
        assert_eq!(
            polled.usage_report_state,
            Some(ApiKeyOrgCapability::Error),
            "{label}"
        );
        assert_eq!(polled.usage_poll_failures, 1, "{label}");
        assert!(polled.usage_last_attempt_at_unix_secs.is_some(), "{label}");
        assert!(
            polled
                .usage_next_poll_after_unix_secs
                .is_some_and(|next| next > now),
            "{label}: backs off"
        );
        let due = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
            stack.stage.storage.as_ref(),
            now,
            Some(row.id),
            10,
        )
        .await
        .expect("poll candidates");
        assert!(due.is_empty(), "{label}: not polled again this tick");
    }

    // Re-check: canonical succeeds with an unquoted hint; legacy rows 500.
    let recheck = stack
        .admin(
            Method::POST,
            &format!("{UPSTREAMS}/{}/preflight", canonical_row.id),
            None,
            None,
        )
        .await;
    assert_eq!(recheck.status, StatusCode::OK, "{}", recheck.body);
    let hint = recheck.body["key_hint"].as_str().unwrap_or_default();
    assert!(!hint.is_empty() && !hint.contains('"'), "{}", recheck.body);
    assert_eq!(
        stack
            .fake()
            .requests(COUNT_TOKENS)
            .iter()
            .filter_map(|request| request.header("x-api-key"))
            .collect::<Vec<_>>(),
        vec![KEY_1]
    );
    for (label, row, key) in &legacy_rows {
        for (path, method) in [
            (format!("{UPSTREAMS}/{}/preflight", row.id), Method::POST),
            (format!("{UPSTREAMS}/{}/workspaces", row.id), Method::GET),
        ] {
            let reply = stack.admin(method, &path, None, None).await;
            assert_eq!(
                reply.status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{label} {path}"
            );
            assert_eq!(
                reply.body,
                json!({"error": "api_key_credential_decrypt_failed"}),
                "{label} {path}"
            );
            assert_no_key(&reply.body.to_string(), key, label);
        }
        assert_eq!(
            stack.fake().requests_with_key(key),
            0,
            "{label}: never sent"
        );
    }
}

/// The poller's legacy-row hint: the prefix family, `...`, the last four.
fn anthropic_style_hint(key: &str) -> String {
    format!(
        "sk-ant-api03-...{}",
        key.get(key.len().saturating_sub(4)..).unwrap_or_default()
    )
}

#[tokio::test]
async fn acc_03_canonical_serves_and_legacy_shapes_fail_closed_sqlite() {
    acc_03_scenario(Stage::sqlite().await).await;
}

#[cfg(feature = "postgres")]
#[test]
fn acc_03_canonical_serves_and_legacy_shapes_fail_closed_postgres() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };
    run_in_pg_schema(&url, |url| async move {
        acc_03_scenario(Stage::postgres(&url).await).await;
    });
}

// ---------------------------------------------------------------- BIL-12

/// BIL-12: a 400 credit-balance and a 402 answer are relayed after one
/// dispatch with another upstream available (not failover reasons);
/// `/usage` shows the billing stop, and a later 2xx on the same upstream
/// clears it.
#[tokio::test]
async fn bil_12_billing_stops_relay_without_failover_and_clear_on_success() {
    let mut stage = Stage::sqlite().await;
    let fake_url = stage.fake.url.clone();
    let first = stage
        .upstream("billing-a", Some(fake_url.clone()), None, KEY_1, canonical)
        .await;
    let second = stage
        .upstream("billing-b", Some(fake_url), None, KEY_2, canonical)
        .await;
    let only_first = stage.pin(&[first.id]);
    let stack = stage.boot().await;
    let mut events = stack.events().await;
    let upstream_by_key = |key: Option<&str>| match key {
        Some(KEY_1) => first.id,
        Some(KEY_2) => second.id,
        other => panic!("unexpected upstream key {other:?}"),
    };
    let usage = |id: Uuid| stack.billing_stop(id);

    for (status, error_type, message, kind) in [
        (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            CREDIT_BALANCE,
            "credits_exhausted",
        ),
        (
            StatusCode::PAYMENT_REQUIRED,
            "billing_error",
            "Payment required",
            "billing_error",
        ),
    ] {
        let dispatched_before = stack.fake().requests(MESSAGES).len();
        stack
            .fake()
            .script
            .push_response(ScriptedMessageResponse::error(status, error_type, message));

        assert_eq!(stack.send(0, &[]).await, status.as_u16(), "{kind}: relayed");
        let dispatched = stack.fake().requests(MESSAGES);
        assert_eq!(
            dispatched.len(),
            dispatched_before + 1,
            "{kind}: one dispatch"
        );
        let target = upstream_by_key(
            dispatched
                .last()
                .and_then(|request| request.header("x-api-key")),
        );
        let event = events.next_final().await;
        assert_eq!(event["status"], status.as_u16(), "{kind}");
        assert_eq!(event["upstream_id"], target.to_string(), "{kind}");
        assert_eq!(usage(target).await["kind"], kind, "{kind}");
    }

    // The credit stop was on whichever upstream took the first request;
    // make the first upstream's latest outcome a stop, then clear it.
    stack
        .fake()
        .script
        .push_response(ScriptedMessageResponse::error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            CREDIT_BALANCE,
        ));
    assert_eq!(stack.send(only_first, &[]).await, 400);
    events.next_final().await;
    assert_eq!(usage(first.id).await["kind"], "credits_exhausted");

    assert_eq!(stack.send(only_first, &[]).await, 200);
    let event = events.next_final().await;
    assert_eq!(event["status"], 200);
    assert_eq!(event["upstream_id"], first.id.to_string());
    assert!(
        usage(first.id).await.is_null(),
        "a later 2xx clears the stop"
    );
}
