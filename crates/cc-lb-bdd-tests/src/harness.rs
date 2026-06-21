//! Per-scenario harness that boots cc-lb-server in-process + fake-anthropic
//! on a real TcpListener. Implements plan v3.2 §7.2 L1 isolation: every
//! scenario gets its own SqliteStorage / Postgres schema + its own cc-lb
//! `App` (admin + proxy router) + its own fake-anthropic upstream. The
//! persona clients invoke the routers through `tower::ServiceExt::oneshot`
//! so the full axum middleware + handlers + business logic runs end-to-end;
//! only the outbound upstream call goes over a real TCP socket to the
//! fake-anthropic server.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use axum::Router;
use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_config::{
    AdminConfig, Config, DownstreamAuthConfig, DownstreamAuthMode, NoneModeConfig,
    NoneModeUpstreamKind, StorageConfig,
};
use cc_lb_server::app::{App, build_app_with_storage};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore,
    Storage as StorageTrait, UpstreamCreate, UpstreamStore,
};
use cc_lb_storage_sqlite::open_sqlite;
use fake_anthropic::{AppConfig as FakeAnthropicConfig, MessageScript, app as fake_anthropic_app};
use mock_anthropic_oauth_server::app as oauth_mock_app;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

#[cfg(feature = "postgres")]
use cc_lb_config::PostgresPoolConfig;
#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::PostgresManagedKeyStore;
#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::PostgresStorage;
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
#[cfg(feature = "postgres")]
use std::str::FromStr;

/// Default test principal name. Single seeded principal per scenario.
pub const TEST_PRINCIPAL_NAME: &str = "bdd-principal";
/// Default seeded upstream name (points at the fake-anthropic spawned by
/// this harness).
pub const TEST_UPSTREAM_NAME: &str = "bdd-upstream";
pub const TEST_ADMIN_TOKEN: &str = "bdd-admin-token";

/// Backend tag for routing-only logic inside the persona clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessBackend {
    Sqlite,
    Postgres,
}

/// A live BDD harness instance owning the cc-lb-server App + the seeded
/// upstream + the storage handle. Dropped at the end of each scenario;
/// teardown is RAII via the embedded TempDir / Postgres schema drop.
pub struct BddHarness {
    pub backend: HarnessBackend,
    pub storage: Arc<dyn StorageTrait>,
    pub managed: Arc<dyn ManagedKeyStore>,
    pub aead: Arc<AeadService>,
    pub app: App,
    pub script: MessageScript,
    pub upstream_id: Uuid,
    pub principal_id: Uuid,
    pub oauth_mock: Option<OAuthMockHandle>,
    _upstream_task: JoinHandle<std::io::Result<()>>,
    _state: BackendState,
}

pub struct OAuthMockHandle {
    pub addr: SocketAddr,
    _task: JoinHandle<std::io::Result<()>>,
}

enum BackendState {
    Sqlite {
        _dir: tempfile::TempDir,
    },
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
        schema: String,
        pool: sqlx::PgPool,
        _dir: tempfile::TempDir,
    },
    #[cfg(feature = "postgres")]
    PostgresReplica {
        _dir: tempfile::TempDir,
    },
    #[allow(dead_code)]
    None,
}

impl Drop for BackendState {
    fn drop(&mut self) {
        #[cfg(feature = "postgres")]
        if let BackendState::Postgres {
            url, schema, pool, ..
        } = self
        {
            let url = std::mem::take(url);
            let schema = std::mem::take(schema);
            let pool = pool.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    pool.close().await;
                    if let Ok(opts) = PgConnectOptions::from_str(&url)
                        && let Ok(admin) = PgPoolOptions::new()
                            .max_connections(1)
                            .connect_with(opts)
                            .await
                    {
                        let stmt = format!("DROP SCHEMA IF EXISTS {} CASCADE", schema);
                        let _ = sqlx::query(AssertSqlSafe(stmt.clone()))
                            .execute(&admin)
                            .await;
                        admin.close().await;
                    }
                });
            }
        }
    }
}

impl BddHarness {
    /// Build a sqlite-backed harness: tempdir + open_sqlite + fake-anthropic
    /// TcpListener + cc-lb-server App + seeded principal + seeded upstream.
    pub async fn spawn_sqlite() -> Result<Self> {
        Self::spawn_sqlite_inner(false).await
    }

    pub async fn spawn_sqlite_with_oauth_mock() -> Result<Self> {
        Self::spawn_sqlite_inner(true).await
    }

    async fn spawn_sqlite_inner(with_oauth_mock: bool) -> Result<Self> {
        let dir = tempfile::tempdir().context("create scenario tempdir")?;
        let storage_path = dir.path().join("bdd.sqlite");
        let database_url = format!("sqlite://{}", storage_path.display());
        let storage_arc = Arc::new(open_sqlite(&database_url).await?);
        storage_arc.initialize(BackendKind::Sqlite).await?;
        let storage_dyn: Arc<dyn StorageTrait> = storage_arc.clone();
        let managed_dyn: Arc<dyn ManagedKeyStore> = storage_arc.clone();

        let (script, upstream_addr, upstream_task) = spawn_fake_anthropic().await?;
        let oauth_mock = if with_oauth_mock {
            Some(spawn_oauth_mock().await?)
        } else {
            None
        };

        let config = base_config_sqlite(
            storage_path.clone(),
            dir.path().join("data"),
            oauth_mock.as_ref().map(|mock| mock.addr),
        );
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let app = build_app_with_storage(
            config,
            None,
            managed_dyn.clone(),
            storage_dyn.clone(),
            aead.clone(),
        )
        .await
        .map_err(|e| anyhow!("build_app_with_storage failed: {e}"))?;

        let principal_id = seed_principal(storage_dyn.as_ref(), TEST_PRINCIPAL_NAME).await?;
        let upstream_id =
            seed_upstream(storage_dyn.as_ref(), TEST_UPSTREAM_NAME, upstream_addr).await?;

        Ok(Self {
            backend: HarnessBackend::Sqlite,
            storage: storage_dyn,
            managed: managed_dyn,
            aead,
            app,
            script,
            upstream_id,
            principal_id,
            oauth_mock,
            _upstream_task: upstream_task,
            _state: BackendState::Sqlite { _dir: dir },
        })
    }

    /// Build a postgres-backed harness. Returns `Ok(None)` when
    /// `CI_POSTGRES_URL` is unset so the scenario can declare itself
    /// skipped instead of failing.
    #[cfg(feature = "postgres")]
    pub async fn spawn_postgres() -> Result<Option<Self>> {
        Self::spawn_postgres_inner(false).await
    }

    #[cfg(feature = "postgres")]
    pub async fn spawn_postgres_with_oauth_mock() -> Result<Option<Self>> {
        Self::spawn_postgres_inner(true).await
    }

    #[cfg(feature = "postgres")]
    async fn spawn_postgres_inner(with_oauth_mock: bool) -> Result<Option<Self>> {
        let Some(url) = std::env::var("CI_POSTGRES_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
        else {
            return Ok(None);
        };

        let schema = format!("bdd_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {}", schema)))
            .execute(&admin)
            .await?;
        admin.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        for migration in POSTGRES_MIGRATIONS {
            sqlx::raw_sql(*migration).execute(&pool).await?;
        }
        let postgres = Arc::new(PostgresStorage::new(pool.clone()));
        let storage_dyn: Arc<dyn StorageTrait> = postgres.clone();
        let managed_dyn: Arc<dyn ManagedKeyStore> = Arc::new(PostgresManagedKeyStore::new(
            pool.clone(),
            Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
        ));
        postgres.initialize(BackendKind::Postgres).await?;

        let (script, upstream_addr, upstream_task) = spawn_fake_anthropic().await?;
        let oauth_mock = if with_oauth_mock {
            Some(spawn_oauth_mock().await?)
        } else {
            None
        };

        let dir = tempfile::tempdir().context("create postgres scenario tempdir")?;
        let config = base_config_postgres(
            &url,
            &schema,
            dir.path().join("data"),
            oauth_mock.as_ref().map(|mock| mock.addr),
        );
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let app = build_app_with_storage(
            config,
            None,
            managed_dyn.clone(),
            storage_dyn.clone(),
            aead.clone(),
        )
        .await
        .map_err(|e| anyhow!("build_app_with_storage failed: {e}"))?;

        let principal_id = seed_principal(storage_dyn.as_ref(), TEST_PRINCIPAL_NAME).await?;
        let upstream_id =
            seed_upstream(storage_dyn.as_ref(), TEST_UPSTREAM_NAME, upstream_addr).await?;

        Ok(Some(Self {
            backend: HarnessBackend::Postgres,
            storage: storage_dyn,
            managed: managed_dyn,
            aead,
            app,
            script,
            upstream_id,
            principal_id,
            oauth_mock,
            _upstream_task: upstream_task,
            _state: BackendState::Postgres {
                url,
                schema,
                pool,
                _dir: dir,
            },
        }))
    }

    #[cfg(feature = "postgres")]
    pub async fn spawn_postgres_replica_pair() -> Result<Option<(Self, Self)>> {
        let Some(url) = std::env::var("CI_POSTGRES_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
        else {
            return Ok(None);
        };

        let schema = format!("bdd_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {}", schema)))
            .execute(&admin)
            .await?;
        admin.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        for migration in POSTGRES_MIGRATIONS {
            sqlx::raw_sql(*migration).execute(&pool).await?;
        }
        let postgres = Arc::new(PostgresStorage::new(pool.clone()));
        let storage_dyn: Arc<dyn StorageTrait> = postgres.clone();
        let managed_dyn: Arc<dyn ManagedKeyStore> = Arc::new(PostgresManagedKeyStore::new(
            pool.clone(),
            Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
        ));
        postgres.initialize(BackendKind::Postgres).await?;

        let (script_a, upstream_addr_a, upstream_task_a) = spawn_fake_anthropic().await?;
        let upstream_id =
            seed_upstream(storage_dyn.as_ref(), TEST_UPSTREAM_NAME, upstream_addr_a).await?;
        let principal_id = seed_principal(storage_dyn.as_ref(), TEST_PRINCIPAL_NAME).await?;

        let (script_b, _upstream_addr_b, upstream_task_b) = spawn_fake_anthropic().await?;
        let dir_a = tempfile::tempdir().context("create postgres replica A tempdir")?;
        let dir_b = tempfile::tempdir().context("create postgres replica B tempdir")?;
        let config_a = base_config_postgres(&url, &schema, dir_a.path().join("data"), None);
        let config_b = base_config_postgres(&url, &schema, dir_b.path().join("data"), None);
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let app_a = build_app_with_storage(
            config_a,
            None,
            managed_dyn.clone(),
            storage_dyn.clone(),
            aead.clone(),
        )
        .await
        .map_err(|e| anyhow!("build_app_with_storage replica A failed: {e}"))?;
        let app_b = build_app_with_storage(
            config_b,
            None,
            managed_dyn.clone(),
            storage_dyn.clone(),
            aead.clone(),
        )
        .await
        .map_err(|e| anyhow!("build_app_with_storage replica B failed: {e}"))?;

        let replica_a = Self {
            backend: HarnessBackend::Postgres,
            storage: storage_dyn.clone(),
            managed: managed_dyn.clone(),
            aead: aead.clone(),
            app: app_a,
            script: script_a,
            upstream_id,
            principal_id,
            oauth_mock: None,
            _upstream_task: upstream_task_a,
            _state: BackendState::PostgresReplica { _dir: dir_a },
        };
        let replica_b = Self {
            backend: HarnessBackend::Postgres,
            storage: storage_dyn,
            managed: managed_dyn,
            aead,
            app: app_b,
            script: script_b,
            upstream_id,
            principal_id,
            oauth_mock: None,
            _upstream_task: upstream_task_b,
            _state: BackendState::Postgres {
                url,
                schema,
                pool,
                _dir: dir_b,
            },
        };
        Ok(Some((replica_a, replica_b)))
    }

    /// Admin router clone for tower::ServiceExt::oneshot.
    pub fn admin_router(&self) -> Router {
        self.app.admin_router.clone()
    }

    /// Proxy router clone for tower::ServiceExt::oneshot.
    pub fn proxy_router(&self) -> Router {
        self.app.router.clone()
    }

    pub fn charlie(&self) -> crate::persona::Charlie<'_> {
        crate::persona::Charlie::from_harness(self)
    }
}

fn base_config_sqlite(path: PathBuf, data_dir: PathBuf, oauth_addr: Option<SocketAddr>) -> Config {
    let mut config = Config {
        storage: StorageConfig::Sqlite { path },
        admin: AdminConfig {
            token: Some(TEST_ADMIN_TOKEN.to_owned()),
            ..AdminConfig::default()
        },
        downstream_auth: DownstreamAuthConfig {
            mode: DownstreamAuthMode::None,
            none_mode: Some(NoneModeConfig {
                principal_id: TEST_PRINCIPAL_NAME.to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
        },
        ..Config::default()
    };
    config.runtime.data_dir = Some(data_dir);
    if let Some(addr) = oauth_addr {
        config.oauth.anthropic = Some(oauth_config(addr));
    }
    config
}

#[cfg(feature = "postgres")]
fn base_config_postgres(
    url: &str,
    _schema: &str,
    data_dir: PathBuf,
    oauth_addr: Option<SocketAddr>,
) -> Config {
    let mut config = Config {
        storage: StorageConfig::Postgres {
            url: url.to_owned(),
            pool: PostgresPoolConfig {
                max_connections: 8,
                ..PostgresPoolConfig::default()
            },
        },
        admin: AdminConfig {
            token: Some(TEST_ADMIN_TOKEN.to_owned()),
            ..AdminConfig::default()
        },
        downstream_auth: DownstreamAuthConfig {
            mode: DownstreamAuthMode::None,
            none_mode: Some(NoneModeConfig {
                principal_id: TEST_PRINCIPAL_NAME.to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
        },
        ..Config::default()
    };
    config.runtime.data_dir = Some(data_dir);
    if let Some(addr) = oauth_addr {
        config.oauth.anthropic = Some(oauth_config(addr));
    }
    config
}

fn oauth_config(addr: SocketAddr) -> AnthropicOAuthConfig {
    let base = format!("http://{addr}");
    AnthropicOAuthConfig {
        client_id: "bdd-oauth-client".to_owned(),
        auth_url: Url::parse(&format!("{base}/oauth/authorize")).expect("oauth auth URL parses"),
        token_url: Url::parse(&format!("{base}/v1/oauth/token")).expect("oauth token URL parses"),
        redirect_uri: Url::parse("http://127.0.0.1/admin/oauth/callback")
            .expect("oauth redirect URL parses"),
        scopes: vec!["messages".to_owned(), "files".to_owned()],
    }
}

async fn spawn_fake_anthropic()
-> Result<(MessageScript, SocketAddr, JoinHandle<std::io::Result<()>>)> {
    let script = MessageScript::default();
    let cfg = FakeAnthropicConfig {
        message_script: Some(script.clone()),
        ..FakeAnthropicConfig::default()
    };
    let app = fake_anthropic_app(cfg);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((script, addr, task))
}

async fn spawn_oauth_mock() -> Result<OAuthMockHandle> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, oauth_mock_app()).await });
    Ok(OAuthMockHandle { addr, _task: task })
}

async fn seed_principal(storage: &dyn StorageTrait, name: &str) -> Result<Uuid> {
    let record = PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
        },
        unix_now_secs(),
    )
    .await
    .map_err(|e| anyhow!("seed principal failed: {e:?}"))?;
    Ok(record.id)
}

async fn seed_upstream(storage: &dyn StorageTrait, name: &str, addr: SocketAddr) -> Result<Uuid> {
    let base = Url::parse(&format!("http://{}", addr))?;
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(base),
            api_key_ciphertext: Some(vec![1, 2, 3]),
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
        },
    )
    .await
    .map_err(|e| anyhow!("seed upstream failed: {e:?}"))?;
    Ok(created.id)
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

#[cfg(feature = "postgres")]
const POSTGRES_MIGRATIONS: &[&str] = &[
    include_str!("../../cc-lb-storage-postgres/migrations/0001_meta.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0002_killswitch.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0003_audit_log.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0004_request_events.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0005_quotas.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0006_principal_limit_states.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0007_oauth_credentials.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0008_api_keys.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0009_anthropic_api_keys.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0010_usage_rollups.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0011_config_draft.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0012_config_history.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0016_principals.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0019_principal_allowed_upstreams.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0033_router_pipeline.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0045_audit_log_wider_columns.sql"),
];
