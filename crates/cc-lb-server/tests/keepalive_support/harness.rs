//! Shared end-to-end keepalive harness for SQLite and PostgreSQL scenarios.

use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_clock::{Clock, ClockHandle, SystemClock, TestClock, unix_secs};
#[cfg(feature = "postgres")]
use cc_lb_config::PostgresPoolConfig;
use cc_lb_config::{Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::cache_keepalive::SessionKey;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::upstream::{
    OAuthTokensCreate, UpstreamCreate, UpstreamKind, UpstreamUpdate,
};
use cc_lb_storage_api::{
    CacheKeepaliveConfig, ManagedKeyStore, MetaStore, PrincipalStore, Storage, UpstreamStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use reqwest::{Client, Response};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::net::TcpListener;
use tokio::time::sleep;
use url::Url;
use uuid::Uuid;

#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::adapter::retry::RetryPolicy;
#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage};

use super::mock_upstream::{MockAnthropic, MockEvent};

pub const NOW: u64 = 1_800_000_000;
const DISPATCH_WAIT: Duration = Duration::from_secs(20);
const PRINCIPAL_NAME: &str = "real-session-principal";
const ANTHROPIC_BETA: &str =
    "claude-code-20250219,interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Sqlite,
    Postgres,
}

#[derive(Clone, Debug)]
pub enum ClockMode {
    Test { now_secs: u64 },
    System,
}

#[derive(Clone, Debug)]
pub struct KeepaliveOptions {
    pub refresh_lead_time_5m_secs: u64,
    pub refresh_lead_time_1h_secs: u64,
    pub max_refreshes_per_session: u32,
    pub max_total_duration_secs: u64,
    pub snapshot_max_bytes: u32,
    pub min_cache_tokens: u64,
}

impl Default for KeepaliveOptions {
    fn default() -> Self {
        Self {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 3,
            max_total_duration_secs: 1_800,
            snapshot_max_bytes: 8_388_608,
            min_cache_tokens: 10_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HarnessOptions {
    pub backend: Backend,
    pub clock: ClockMode,
    pub keepalive: KeepaliveOptions,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            backend: Backend::Sqlite,
            clock: ClockMode::Test { now_secs: NOW },
            keepalive: KeepaliveOptions::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[cfg(not(feature = "postgres"))]
    #[error("postgres feature is disabled")]
    PostgresFeatureDisabled,
    #[cfg(feature = "postgres")]
    #[error("CI_POSTGRES_URL is unset")]
    PostgresUrlUnset,
    #[error("harness setup failed: {0}")]
    Setup(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRow {
    pub status: String,
    pub terminal_reason: Option<String>,
    pub enqueue_state: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobTiming {
    pub lock_at: Option<i64>,
    pub run_at: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTiming {
    pub id: String,
    pub started_at: Option<i64>,
    pub last_heartbeat: Option<i64>,
}

/// Exact scheduler generation and upstream attempt expected at a virtual-clock boundary.
pub struct DueJob<'a> {
    pub session_key_hash: &'a str,
    pub generation: u64,
    /// Stable cached system-text label; None for transport or missing-upstream failures.
    pub mock_prefix: Option<&'a str>,
}
#[derive(Clone)]
enum StorageBackend {
    Sqlite(Arc<SqliteStorage>),
    #[cfg(feature = "postgres")]
    Postgres(Arc<PostgresStorage>),
}
impl StorageBackend {
    fn as_storage(&self) -> &dyn Storage {
        match self {
            Self::Sqlite(storage) => storage.as_ref(),
            #[cfg(feature = "postgres")]
            Self::Postgres(storage) => storage.as_ref(),
        }
    }
    fn as_storage_arc(&self) -> Arc<dyn Storage> {
        match self {
            Self::Sqlite(storage) => storage.clone(),
            #[cfg(feature = "postgres")]
            Self::Postgres(storage) => storage.clone(),
        }
    }
}

enum SchedulerPool {
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}

/// Shared test harness. PostgreSQL tests are isolated in a private database
/// created from `CI_POSTGRES_URL`; unset URLs return `HarnessError::PostgresUrlUnset`.
pub struct Harness {
    pub principal_id: Uuid,
    pub upstream_id: Uuid,
    storage: StorageBackend,
    scheduler_pool: SchedulerPool,
    pub clock: Option<Arc<TestClock>>,
    pub mock: MockAnthropic,
    pub client: Client,
    pub key: String,
    pub proxy_addr: std::net::SocketAddr,
    pub admin_addr: std::net::SocketAddr,
    #[cfg(feature = "postgres")]
    postgres_database: Option<(String, String)>,
    _dir: Option<TempDir>,
    app: Option<cc_lb_server::App>,
    _proxy_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    _admin_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Harness {
    pub async fn new(snapshot_max_bytes: u32) -> Self {
        let mut options = HarnessOptions::default();
        options.keepalive.snapshot_max_bytes = snapshot_max_bytes;
        Self::with_options(options)
            .await
            .expect("SQLite harness setup")
    }

    pub async fn with_options(options: HarnessOptions) -> Result<Self, HarnessError> {
        // reqwest 0.13 needs a process-wide rustls provider before any client is built.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let now = match options.clock {
            ClockMode::Test { now_secs } => now_secs,
            ClockMode::System => unix_secs(SystemClock.now()),
        };
        let (clock, test_clock) = match options.clock {
            ClockMode::Test { now_secs } => {
                let test = Arc::new(TestClock::new_at_secs(now_secs));
                (test.clone() as ClockHandle, Some(test))
            }
            ClockMode::System => (Arc::new(SystemClock) as ClockHandle, None),
        };
        let mock = MockAnthropic::start(clock.clone())
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let (storage, managed_store, storage_config, scheduler_pool, _postgres_database, dir) =
            match options.backend {
                Backend::Sqlite => setup_sqlite(&clock, now, &mock, &options).await?,
                Backend::Postgres => setup_postgres(&clock, now, &mock, &options).await?,
            };
        let aead = Arc::new(AeadService::from_master_key([0; 32]));
        let upstream_id = Uuid::new_v4();
        let tokens = OAuthTokenBundle {
            access_token: "sk-ant-oat-test".to_owned(),
            refresh_token: "sk-ant-ort-test".to_owned(),
            expires_at_unix_secs: now + 86_400,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: vec!["messages".to_owned()],
            never_refresh: true,
        };
        let encrypted =
            cc_lb_aead::EncryptedOAuthTokens::encrypt(&aead, &tokens, upstream_id.as_bytes())
                .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let storage_ref = storage.as_storage();
        let principal_name = PRINCIPAL_NAME.to_owned();
        let upstream = UpstreamStore::create(
            storage_ref,
            UpstreamCreate {
                id: upstream_id,
                name: "real-session-upstream".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(mock.base_url()),
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens: encrypted,
                    never_refresh: true,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let principal = PrincipalStore::create(
            storage_ref,
            PrincipalCreate {
                name: principal_name.clone(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: vec![upstream.id],
                default_limits: Vec::new(),
                cache_keepalive: Some(CacheKeepaliveConfig {
                    enabled: true,
                    refresh_lead_time_5m_secs: u32::try_from(
                        options.keepalive.refresh_lead_time_5m_secs,
                    )
                    .map_err(|error| HarnessError::Setup(error.to_string()))?,
                    refresh_lead_time_1h_secs: u32::try_from(
                        options.keepalive.refresh_lead_time_1h_secs,
                    )
                    .map_err(|error| HarnessError::Setup(error.to_string()))?,
                    max_refreshes_per_session: options.keepalive.max_refreshes_per_session,
                    max_total_duration_secs: options.keepalive.max_total_duration_secs,
                    snapshot_max_bytes: options.keepalive.snapshot_max_bytes,
                    min_cache_tokens: options.keepalive.min_cache_tokens,
                    classifier: Default::default(),
                }),
            },
            now,
        )
        .await
        .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let key_store = KeyStore::new(managed_store.clone());
        let (_record, secret) = key_store
            .create(
                &principal.id.to_string(),
                CreateParams {
                    label: "real-session".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let mut config = Config {
            storage: storage_config,
            ..Default::default()
        };
        config.price_catalog.url = "http://127.0.0.1:9/price-catalog.json".to_owned();
        config.body.messages_cap_bytes = 12 * 1024 * 1024;
        if matches!(options.clock, ClockMode::System) {
            // Disable unrelated recurring inserts so D1/D2 cannot borrow their notification wakes.
            for job in config.scheduler.recurring_jobs.values_mut() {
                job.enabled = false;
            }
        }
        config.listener.proxy_addr = "127.0.0.1:0"
            .parse()
            .map_err(|error: std::net::AddrParseError| HarnessError::Setup(error.to_string()))?;
        config.listener.admin_addr = "127.0.0.1:0"
            .parse()
            .map_err(|error: std::net::AddrParseError| HarnessError::Setup(error.to_string()))?;
        if options.backend == Backend::Postgres {
            config.cluster.instance_url = Some("http://127.0.0.1:0".to_owned());
            config.cluster.token_env = "CI_POSTGRES_URL".to_owned();
        }
        config.admin.auth.providers = vec![cc_lb_config::AdminAuthProviderConfig::StaticToken {
            id: "test".to_owned(),
            token_env: "CARGO_PKG_NAME".to_owned(),
        }];
        let app = build_app_with_storage(
            config,
            None,
            managed_store,
            storage.as_storage_arc(),
            aead,
            clock,
        )
        .await
        .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let proxy_addr = proxy_listener
            .local_addr()
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let proxy_task = tokio::spawn({
            let router = app.router.clone();
            async move { axum::serve(proxy_listener, router).await }
        });
        let admin_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let admin_addr = admin_listener
            .local_addr()
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let admin_task = tokio::spawn({
            let router = app.admin_router.clone();
            async move { axum::serve(admin_listener, router).await }
        });
        Ok(Self {
            principal_id: principal.id,
            upstream_id,
            storage,
            scheduler_pool,
            clock: test_clock,
            mock,
            client: Client::new(),
            key: secret.expose().to_owned(),
            proxy_addr,
            admin_addr,
            #[cfg(feature = "postgres")]
            postgres_database: _postgres_database,
            _dir: dir,
            app: Some(app),
            _proxy_task: proxy_task,
            _admin_task: admin_task,
        })
    }
    pub async fn optional(options: HarnessOptions) -> Result<Option<Self>, HarnessError> {
        match Self::with_options(options).await {
            Ok(harness) => Ok(Some(harness)),
            #[cfg(feature = "postgres")]
            Err(HarnessError::PostgresUrlUnset) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn session_key_hash(&self, thread_id: &str, agent_id: Option<&str>) -> String {
        SessionKey::from_thread_id(self.principal_id, thread_id)
            .with_agent_scope(
                agent_id
                    .and_then(|raw| SessionKey::normalize_agent_id(Some(raw)))
                    .map(ToOwned::to_owned),
            )
            .to_string()
    }
    pub async fn post(&self, body: Vec<u8>, session: &str, agent: Option<&str>) -> Value {
        let mut request = self
            .client
            .post(format!("http://{}/v1/messages", self.proxy_addr))
            .header("x-api-key", &self.key)
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", ANTHROPIC_BETA)
            .header("x-claude-code-session-id", session)
            .body(body);
        if let Some(agent) = agent {
            request = request.header("x-claude-code-agent-id", agent);
        }
        let response = request.send().await.expect("proxy request");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response.json().await.expect("proxy JSON")
    }
    pub async fn admin_get(&self, path: &str) -> Response {
        self.client
            .get(format!("http://{}{}", self.admin_addr, path))
            .header(
                "authorization",
                format!("Bearer {}", env!("CARGO_PKG_NAME")),
            )
            .send()
            .await
            .expect("admin request")
    }
    pub async fn admin_cache_keepalive(&self) -> Response {
        self.admin_get(&format!(
            "/admin/v1/principals/{}/cache-keepalive",
            self.principal_id
        ))
        .await
    }
    pub async fn admin_cache_keepalive_detail(&self, id: &str) -> Response {
        let encoded: String = url::form_urlencoded::byte_serialize(id.as_bytes()).collect();
        self.admin_get(&format!(
            "/admin/v1/principals/{}/cache-keepalive/{encoded}",
            self.principal_id
        ))
        .await
    }
    pub async fn set_upstream_base_url(&self, base_url: Url) {
        let record = UpstreamStore::get_by_id(self.storage.as_storage(), self.upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        UpstreamStore::update(
            self.storage.as_storage(),
            self.upstream_id,
            record.revision,
            UpstreamUpdate {
                base_url: Some(Some(base_url)),
                ..Default::default()
            },
        )
        .await
        .expect("upstream URL update");
    }
    pub async fn delete_upstream(&self) {
        let record = UpstreamStore::get_by_id(self.storage.as_storage(), self.upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        UpstreamStore::soft_delete(self.storage.as_storage(), self.upstream_id, record.revision)
            .await
            .expect("upstream delete");
    }
    pub async fn session_count(&self, status: &str, reason: Option<&str>) -> i64 {
        match &self.storage {
            StorageBackend::Sqlite(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_sessions \
                 WHERE status = ? AND (? IS NULL OR terminal_reason = ?)",
            )
            .bind(status)
            .bind(reason)
            .bind(reason)
            .fetch_one(storage.pool())
            .await
            .expect("session count"),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_sessions \
                 WHERE status = $1 AND ($2::text IS NULL OR terminal_reason = $2)",
            )
            .bind(status)
            .bind(reason)
            .fetch_one(storage.pool())
            .await
            .expect("session count"),
        }
    }

    pub async fn decision_count(&self, reason_pattern: &str) -> i64 {
        match &self.storage {
            StorageBackend::Sqlite(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_decisions WHERE reason LIKE ?",
            )
            .bind(reason_pattern)
            .fetch_one(storage.pool())
            .await
            .expect("decision count"),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_decisions WHERE reason LIKE $1",
            )
            .bind(reason_pattern)
            .fetch_one(storage.pool())
            .await
            .expect("decision count"),
        }
    }

    pub async fn renewal_hit_count(&self) -> i64 {
        match &self.storage {
            StorageBackend::Sqlite(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_turns WHERE hit_miss = 'hit'",
            )
            .fetch_one(storage.pool())
            .await
            .expect("renewal hit count"),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_turns WHERE hit_miss = 'hit'",
            )
            .fetch_one(storage.pool())
            .await
            .expect("renewal hit count"),
        }
    }

    pub async fn wait_for_session_count(&self, status: &str, min_count: i64) {
        for _ in 0..200 {
            let count = self.storage_count(status == "cancelled").await;
            if count >= min_count {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {status} session count >= {min_count}");
    }
    pub async fn wait_for_enqueued_generation(&self, hash: &str, generation: i64) {
        for _ in 0..200 {
            let count = self.storage_scalar(hash, generation).await;
            if count > 0 {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for session {hash} generation = {generation}");
    }
    pub async fn wait_for_not_tracked(&self, hash: &str, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                let found = match &self.storage {
                    StorageBackend::Sqlite(storage) => sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM cache_keepalive_decisions \
                         WHERE session_key_hash = ? AND reason LIKE 'cached prefix too small%'",
                    )
                    .bind(hash)
                    .fetch_one(storage.pool())
                    .await
                    .expect("not-tracked decision count"),
                    #[cfg(feature = "postgres")]
                    StorageBackend::Postgres(storage) => sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM cache_keepalive_decisions \
                         WHERE session_key_hash = $1 AND reason LIKE 'cached prefix too small%'",
                    )
                    .bind(hash)
                    .fetch_one(storage.pool())
                    .await
                    .expect("not-tracked decision count"),
                };
                if found > 0 {
                    return true;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    pub async fn keepalive_job_count(&self) -> i64 {
        match &self.scheduler_pool {
            SchedulerPool::Sqlite(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = 'cache_keepalive'")
                    .fetch_one(pool)
                    .await
                    .expect("keepalive job count")
            }
            #[cfg(feature = "postgres")]
            SchedulerPool::Postgres(pool) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = 'cache_keepalive'",
            )
            .fetch_one(pool)
            .await
            .expect("keepalive job count"),
        }
    }
    pub async fn wait_for_latest_enqueued_generation(
        &self,
        hash: &str,
        timeout: Duration,
    ) -> Option<i64> {
        tokio::time::timeout(timeout, async {
            loop {
                let generation = match &self.storage {
                    StorageBackend::Sqlite(storage) => sqlx::query_scalar::<_, i64>(
                        "SELECT generation FROM cache_keepalive_sessions \
                         WHERE session_key_hash = ? AND status = 'active' \
                         AND enqueue_state = 'enqueued'",
                    )
                    .bind(hash)
                    .fetch_optional(storage.pool())
                    .await
                    .expect("read latest enqueued generation"),
                    #[cfg(feature = "postgres")]
                    StorageBackend::Postgres(storage) => sqlx::query_scalar::<_, i64>(
                        "SELECT generation FROM cache_keepalive_sessions \
                         WHERE session_key_hash = $1 AND status = 'active' \
                         AND enqueue_state = 'enqueued'",
                    )
                    .bind(hash)
                    .fetch_optional(storage.pool())
                    .await
                    .expect("read latest enqueued generation"),
                };
                if generation.is_some() {
                    return generation;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .ok()
        .flatten()
    }
    pub async fn read_session(&self, hash: &str) -> Option<SessionRow> {
        type Row = (String, Option<String>, String, Option<String>);
        match &self.storage {
            StorageBackend::Sqlite(storage) => sqlx::query_as::<_, Row>(
                "SELECT status, terminal_reason, enqueue_state, error \
                 FROM cache_keepalive_sessions WHERE session_key_hash = ?",
            )
            .bind(hash)
            .fetch_optional(storage.pool())
            .await
            .expect("read session")
            .map(row),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => sqlx::query_as::<_, Row>(
                "SELECT status, terminal_reason, enqueue_state, error \
                 FROM cache_keepalive_sessions WHERE session_key_hash = $1",
            )
            .bind(hash)
            .fetch_optional(storage.pool())
            .await
            .expect("read session")
            .map(row),
        }
    }
    pub async fn wait_for_session_terminal(
        &self,
        hash: &str,
        timeout: Duration,
    ) -> Option<SessionRow> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(row) = self.read_session(hash).await
                    && row.status == "terminal"
                {
                    return row;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .ok()
    }
    pub async fn count_renewal_rows(&self) -> (i64, i64) {
        match &self.storage {
            StorageBackend::Sqlite(storage) => (
                sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
                    .fetch_one(storage.pool())
                    .await
                    .expect("count turns"),
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM request_events_v1 WHERE source_kind = 'renewal'",
                )
                .fetch_one(storage.pool())
                .await
                .expect("count events"),
            ),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => (
                sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
                    .fetch_one(storage.pool())
                    .await
                    .expect("count turns"),
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM request_events_v1 WHERE source_kind = 'renewal'",
                )
                .fetch_one(storage.pool())
                .await
                .expect("count events"),
            ),
        }
    }
    pub async fn read_job_timing(&self, job_key: &str) -> Option<JobTiming> {
        match &self.scheduler_pool {
            SchedulerPool::Sqlite(pool) => sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
                "SELECT lock_at, run_at FROM Jobs \
                     WHERE idempotency_key = ? ORDER BY id DESC LIMIT 1",
            )
            .bind(job_key)
            .fetch_optional(pool)
            .await
            .expect("read job timing")
            .map(|(lock_at, run_at)| JobTiming { lock_at, run_at }),
            #[cfg(feature = "postgres")]
            SchedulerPool::Postgres(pool) => {
                type Timestamp = chrono::DateTime<chrono::Utc>;
                let row = sqlx::query_as::<_, (Option<Timestamp>, Option<Timestamp>)>(
                    "SELECT lock_at, run_at FROM apalis.jobs \
                     WHERE idempotency_key = $1 ORDER BY id DESC LIMIT 1",
                )
                .bind(job_key)
                .fetch_optional(pool)
                .await
                .expect("read job timing");
                row.map(|(lock_at, run_at)| JobTiming {
                    lock_at: lock_at.map(|value| value.timestamp_micros() / 1_000),
                    run_at: run_at.map(|value| value.timestamp_micros() / 1_000),
                })
            }
        }
    }
    pub async fn wait_for_job_locked(&self, job_key: &str, timeout: Duration) -> Option<JobTiming> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(timing) = self.read_job_timing(job_key).await
                    && timing.lock_at.is_some()
                {
                    return timing;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .ok()
    }
    pub async fn debug_keepalive(&self, hash: &str) -> String {
        type DebugRow = (i64, String, String, Option<String>, Option<String>);
        let session = match &self.storage {
            StorageBackend::Sqlite(storage) => sqlx::query_as::<_, DebugRow>(
                "SELECT generation, status, enqueue_state, current_job_key, terminal_reason \
                 FROM cache_keepalive_sessions WHERE session_key_hash = ?",
            )
            .bind(hash)
            .fetch_optional(storage.pool())
            .await
            .expect("debug session"),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => sqlx::query_as::<_, DebugRow>(
                "SELECT generation, status, enqueue_state, current_job_key, terminal_reason \
                 FROM cache_keepalive_sessions WHERE session_key_hash = $1",
            )
            .bind(hash)
            .fetch_optional(storage.pool())
            .await
            .expect("debug session"),
        };
        let Some((generation, status, enqueue_state, current_job_key, terminal_reason)) = session
        else {
            return "session=None".to_owned();
        };
        let jobs = match (&self.scheduler_pool, current_job_key.as_deref()) {
            (SchedulerPool::Sqlite(pool), Some(key)) => format!(
                "{:?}",
                sqlx::query_as::<_, (String, String, Option<i64>, Option<i64>)>(
                    "SELECT id, status, lock_at, run_at FROM Jobs WHERE idempotency_key = ?"
                )
                .bind(key)
                .fetch_all(pool)
                .await
                .expect("debug jobs")
            ),
            #[cfg(feature = "postgres")]
            (SchedulerPool::Postgres(pool), Some(key)) => format!(
                "{:?}",
                sqlx::query_as::<
                    _,
                    (
                        String,
                        String,
                        Option<chrono::DateTime<chrono::Utc>>,
                        Option<chrono::DateTime<chrono::Utc>>
                    ),
                >(
                    "SELECT id, status, lock_at, run_at FROM apalis.jobs WHERE idempotency_key = $1"
                )
                .bind(key)
                .fetch_all(pool)
                .await
                .expect("debug jobs")
            ),
            (_, None) => "[]".to_owned(),
        };
        format!(
            "session=({generation}, {status}, {enqueue_state}, {current_job_key:?}, {terminal_reason:?}) jobs={jobs}"
        )
    }
    pub async fn read_keepalive_worker(&self) -> Option<WorkerTiming> {
        match &self.scheduler_pool {
            SchedulerPool::Sqlite(pool) => sqlx::query_as::<_, (String, Option<i64>, Option<i64>)>(
                "SELECT id, started_at, last_seen FROM Workers \
                     WHERE worker_type = 'cache_keepalive' ORDER BY last_seen DESC LIMIT 1",
            )
            .fetch_optional(pool)
            .await
            .expect("read keepalive worker")
            .map(|(id, started_at, last_heartbeat)| WorkerTiming {
                id,
                started_at,
                last_heartbeat,
            }),
            #[cfg(feature = "postgres")]
            SchedulerPool::Postgres(pool) => {
                type Timestamp = chrono::DateTime<chrono::Utc>;
                sqlx::query_as::<_, (String, Option<Timestamp>, Timestamp)>(
                    "SELECT id, started_at, last_seen FROM apalis.workers \
                     WHERE worker_type = 'cache_keepalive' ORDER BY last_seen DESC LIMIT 1",
                )
                .fetch_optional(pool)
                .await
                .expect("read keepalive worker")
                .map(|(id, started_at, last_heartbeat)| WorkerTiming {
                    id,
                    started_at: started_at.map(|value| value.timestamp_micros() / 1_000_000),
                    last_heartbeat: Some(last_heartbeat.timestamp_micros() / 1_000_000),
                })
            }
        }
    }
    pub async fn wait_for_keepalive_worker(&self, timeout: Duration) -> Option<WorkerTiming> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(worker) = self.read_keepalive_worker().await {
                    return worker;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .ok()
    }

    pub fn mock_events(&self) -> Vec<MockEvent> {
        self.mock.events()
    }
    pub fn advance_clock(&self, delta: Duration) {
        self.clock
            .as_ref()
            .expect("virtual clock required")
            .advance(delta);
    }

    pub async fn advance_and_dispatch_due(&self, delta: Duration, expected_jobs: &[DueJob<'_>]) {
        assert!(
            !expected_jobs.is_empty(),
            "provide the exact jobs expected to dispatch"
        );
        #[cfg(feature = "postgres")]
        let pool = match &self.scheduler_pool {
            SchedulerPool::Sqlite(pool) => pool,
            SchedulerPool::Postgres(_) => panic!("advance_and_dispatch_due is SQLite-only"),
        };
        #[cfg(not(feature = "postgres"))]
        let SchedulerPool::Sqlite(pool) = &self.scheduler_pool;
        let mut expected = Vec::with_capacity(expected_jobs.len());
        for job in expected_jobs {
            let key =
                cc_lb_storage_api::cache_keepalive_job_key(job.session_key_hash, job.generation);
            let row = sqlx::query_as::<_, (String, String, Option<i64>)>(
                "SELECT id, status, run_at FROM Jobs WHERE idempotency_key = ?",
            )
            .bind(&key)
            .fetch_optional(pool)
            .await
            .expect("read exact expected job");
            let Some((id, status, run_at)) = row else {
                panic!(
                    "expected job {key} does not exist; {}",
                    self.debug_keepalive(job.session_key_hash).await
                );
            };
            assert!(
                matches!(status.as_str(), "Pending" | "Queued" | "Running"),
                "expected unprocessed job {key}, got {status}"
            );
            let attempt_count = job
                .mock_prefix
                .map(|prefix| self.renewal_attempt_count(prefix));
            expected.push((job, key, id, run_at, attempt_count));
        }

        self.advance_clock(delta);
        let now = i64::try_from(unix_secs(self.clock.as_ref().expect("virtual clock").now()))
            .expect("now fits i64");
        for (_, key, id, run_at, _) in &expected {
            assert!(
                run_at.is_some_and(|scheduled| scheduled <= now),
                "expected job {key} is not due: run_at={run_at:?}, now={now}"
            );
            sqlx::query(
                "UPDATE Jobs SET run_at = 0 WHERE id = ? \
                 AND idempotency_key = ? AND status = 'Pending'",
            )
            .bind(id)
            .bind(key)
            .execute(pool)
            .await
            .expect("release exact due job");
        }

        for (job, key, id, _, attempt_count) in expected {
            let terminal = tokio::time::timeout(DISPATCH_WAIT, async {
                loop {
                    let status = sqlx::query_scalar::<_, String>(
                        "SELECT status FROM Jobs WHERE id = ? AND idempotency_key = ?",
                    )
                    .bind(&id)
                    .bind(&key)
                    .fetch_one(pool)
                    .await
                    .expect("read exact job status");
                    if matches!(status.as_str(), "Done" | "Failed" | "Killed") {
                        return status;
                    }
                    // Queued is in-flight, not completion (Apalis fetch_next.sql).
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            assert!(
                terminal.is_ok(),
                "exact job {key} did not finish within {DISPATCH_WAIT:?}; {}",
                self.debug_keepalive(job.session_key_hash).await
            );
            if let (Some(prefix), Some(before)) = (job.mock_prefix, attempt_count) {
                assert!(
                    self.renewal_attempt_count(prefix) > before,
                    "exact job {key} finished without its upstream attempt; {}; events={:?}",
                    self.debug_keepalive(job.session_key_hash).await,
                    self.mock_events()
                );
            }
        }
    }

    fn renewal_attempt_count(&self, prefix: &str) -> usize {
        self.mock_events()
            .iter()
            .filter(|event| {
                event.kind == "renewal"
                    && event
                        .body
                        .get("system")
                        .and_then(Value::as_array)
                        .is_some_and(|blocks| {
                            blocks.iter().any(|block| {
                                block
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .is_some_and(|text| text.contains(prefix))
                            })
                        })
            })
            .count()
    }

    async fn storage_count(&self, cancelled: bool) -> i64 {
        if cancelled {
            self.session_count("terminal", Some("cancelled")).await
        } else {
            match &self.storage {
                StorageBackend::Sqlite(storage) => sqlx::query_scalar(
                    "SELECT COUNT(*) FROM cache_keepalive_sessions \
                     WHERE status = 'active' AND enqueue_state = 'enqueued'",
                )
                .fetch_one(storage.pool())
                .await
                .expect("storage count"),
                #[cfg(feature = "postgres")]
                StorageBackend::Postgres(storage) => sqlx::query_scalar(
                    "SELECT COUNT(*) FROM cache_keepalive_sessions \
                     WHERE status = 'active' AND enqueue_state = 'enqueued'",
                )
                .fetch_one(storage.pool())
                .await
                .expect("storage count"),
            }
        }
    }
    async fn storage_scalar(&self, hash: &str, generation: i64) -> i64 {
        match &self.storage {
            StorageBackend::Sqlite(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_sessions \
                 WHERE session_key_hash = ? AND generation = ? \
                 AND status = 'active' AND enqueue_state = 'enqueued'",
            )
            .bind(hash)
            .bind(generation)
            .fetch_one(storage.pool())
            .await
            .expect("storage scalar"),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(storage) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_sessions \
                 WHERE session_key_hash = $1 AND generation = $2 \
                 AND status = 'active' AND enqueue_state = 'enqueued'",
            )
            .bind(hash)
            .bind(generation)
            .fetch_one(storage.pool())
            .await
            .expect("storage scalar"),
        }
    }
}

#[cfg(feature = "postgres")]
fn drop_postgres_database(admin_url: &str, database: &str) {
    let admin_url = admin_url.to_owned();
    let database = database.to_owned();
    let _ = std::thread::Builder::new()
        .name("cc-lb-keepalive-db-cleanup".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            let Ok(runtime) = runtime else {
                return;
            };
            runtime.block_on(async move {
                if let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&admin_url)
                    .await
                {
                    let sql = format!("DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)");
                    let _ = sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&pool).await;
                    pool.close().await;
                }
            });
        })
        .and_then(|handle| {
            handle
                .join()
                .map_err(|_| std::io::Error::other("database cleanup thread panicked"))
        });
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(app) = self.app.take() {
            app.signal_handle().start_shutdown();
        }
        self._proxy_task.abort();
        self._admin_task.abort();
        #[cfg(feature = "postgres")]
        if let Some((admin_url, database)) = self.postgres_database.take() {
            drop_postgres_database(&admin_url, &database);
        }
    }
}

fn row(
    (status, terminal_reason, enqueue_state, error): (
        String,
        Option<String>,
        String,
        Option<String>,
    ),
) -> SessionRow {
    SessionRow {
        status,
        terminal_reason,
        enqueue_state,
        error,
    }
}

async fn setup_sqlite(
    clock: &ClockHandle,
    _now: u64,
    _mock: &MockAnthropic,
    _options: &HarnessOptions,
) -> Result<
    (
        StorageBackend,
        Arc<dyn ManagedKeyStore>,
        StorageConfig,
        SchedulerPool,
        Option<(String, String)>,
        Option<TempDir>,
    ),
    HarnessError,
> {
    let dir = tempdir().map_err(|error| HarnessError::Setup(error.to_string()))?;
    let path = dir.path().join("storage.sqlite");
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&format!("sqlite://{}", path.display()), clock.clone())
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?,
    );
    storage
        .initialize()
        .await
        .map_err(|error| HarnessError::Setup(error.to_string()))?;
    let scheduler_path = path.with_extension("scheduler.sqlite");
    let scheduler_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}?mode=rwc", scheduler_path.display()))
        .await
        .map_err(|error| HarnessError::Setup(error.to_string()))?;
    Ok((
        StorageBackend::Sqlite(storage.clone()),
        storage.clone(),
        StorageConfig::Sqlite { path },
        SchedulerPool::Sqlite(scheduler_pool),
        None,
        Some(dir),
    ))
}

#[cfg(feature = "postgres")]
async fn setup_postgres(
    clock: &ClockHandle,
    _now: u64,
    _mock: &MockAnthropic,
    _options: &HarnessOptions,
) -> Result<
    (
        StorageBackend,
        Arc<dyn ManagedKeyStore>,
        StorageConfig,
        SchedulerPool,
        Option<(String, String)>,
        Option<TempDir>,
    ),
    HarnessError,
> {
    let admin_url = std::env::var("CI_POSTGRES_URL").map_err(|_| HarnessError::PostgresUrlUnset)?;
    let parsed = Url::parse(&admin_url).map_err(|error| HarnessError::Setup(error.to_string()))?;
    let database = format!("cc_lb_keepalive_{}", Uuid::new_v4().simple());
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .map_err(|error| HarnessError::Setup(error.to_string()))?;
    let create = format!("CREATE DATABASE \"{database}\"");
    if let Err(error) = sqlx::query(sqlx::AssertSqlSafe(create))
        .execute(&admin_pool)
        .await
    {
        admin_pool.close().await;
        return Err(HarnessError::Setup(error.to_string()));
    }
    admin_pool.close().await;
    let setup = async {
        let mut db_url = parsed;
        db_url.set_path(&format!("/{database}"));
        let database_url = db_url.to_string();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect(&database_url)
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let storage = Arc::new(PostgresStorage::new(pool.clone(), clock.clone()));
        storage
            .initialize()
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        let managed: Arc<dyn ManagedKeyStore> = Arc::new(PostgresManagedKeyStore::new(
            pool.clone(),
            Arc::new(RetryPolicy::default()),
            clock.clone(),
        ));
        let scheduler_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .map_err(|error| HarnessError::Setup(error.to_string()))?;
        Ok((
            StorageBackend::Postgres(storage),
            managed,
            StorageConfig::Postgres {
                url: database_url.clone(),
                pool: PostgresPoolConfig::default(),
            },
            SchedulerPool::Postgres(scheduler_pool),
            Some((admin_url.clone(), database.clone())),
            None,
        ))
    }
    .await;
    if setup.is_err() {
        drop_postgres_database(&admin_url, &database);
    }
    setup
}

#[cfg(not(feature = "postgres"))]
async fn setup_postgres(
    _clock: &ClockHandle,
    _now: u64,
    _mock: &MockAnthropic,
    _options: &HarnessOptions,
) -> Result<
    (
        StorageBackend,
        Arc<dyn ManagedKeyStore>,
        StorageConfig,
        SchedulerPool,
        Option<(String, String)>,
        Option<TempDir>,
    ),
    HarnessError,
> {
    Err(HarnessError::PostgresFeatureDisabled)
}

pub fn request_body(
    target_size: usize,
    prefix_label: &str,
    prefix_tokens: u64,
    tool: &str,
    stop_reason: &str,
    oversized_prefix: bool,
) -> Vec<u8> {
    let prefix = if oversized_prefix {
        format!(
            "tokens={prefix_tokens};{}",
            "p".repeat(target_size.saturating_sub(2_000))
        )
    } else {
        format!("tokens={prefix_tokens};stable cached prefix {prefix_label}")
    };
    let tail_directive = format!("tool={tool};stop={stop_reason};");
    let mut value = json!({
        "model": "claude-opus-5-5",
        "max_tokens": 256,
        "stream": false,
        "thinking": {
            "type": "adaptive",
            "display": "summarized",
            "block_binding": {"prefix_mismatch_behavior": "drop_block"},
        },
        "output_config": {"effort": "medium"},
        "tools": [{
            "name": "bash",
            "description": "test tool",
            "input_schema": {"type": "object"},
        }],
        "system": [{
            "type": "text",
            "text": prefix,
            "cache_control": {"type": "ephemeral", "ttl": "5m"},
        }],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "cached breakpoint",
                        "cache_control": {"type": "ephemeral", "ttl": "5m"},
                    },
                    {"type": "text", "text": ""},
                ],
            },
            {
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": "prior-tool",
                    "name": "bash",
                    "input": {},
                }],
            },
            {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "prior-tool",
                    "content": tail_directive,
                }],
            },
        ],
    });
    let base = serde_json::to_vec(&value).expect("request JSON").len();
    if !oversized_prefix {
        value["messages"][0]["content"][1]["text"] =
            Value::String("x".repeat(target_size.saturating_sub(base)));
    }
    serde_json::to_vec(&value).expect("request body")
}
