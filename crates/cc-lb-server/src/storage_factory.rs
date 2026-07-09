//! Storage factory — the ONLY place in cc-lb-server that contains
//! #[cfg(feature = "postgres")] blocks.

use std::path::Path;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::StorageConfig;
#[cfg(feature = "sqlite")]
use cc_lb_storage_api::MetaStore;
use cc_lb_storage_api::{BackendKind, ManagedKeyStore, PluginBlobRepo, Storage, StorageResult};

pub struct OpenedStorage {
    pub storage: Arc<dyn Storage>,
    pub managed_key_store: Arc<dyn ManagedKeyStore>,
    pub plugin_blob_repo: Arc<dyn PluginBlobRepo>,
}

impl OpenedStorage {
    pub async fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        self.storage
            .put_anthropic_api_key_ciphertext(storage_key, ciphertext)
            .await
    }

    pub async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.storage
            .get_oauth_ciphertext(principal_id, provider)
            .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageFactoryError {
    #[error("storage backend '{backend}' requires the '{backend}' cargo feature")]
    FeatureDisabled { backend: String },
    #[error("backend kind mismatch: stored={stored:?}, configured={configured:?}")]
    BackendKindMismatch {
        stored: BackendKind,
        configured: BackendKind,
    },
    #[error("storage connection failed: {message}")]
    ConnectionFailed { message: String },
    #[error("storage initialization failed: {message}")]
    InitFailed { message: String },
}

pub async fn open_storage(
    config: &StorageConfig,
    _aead: Arc<AeadService>,
    _master_key: [u8; 32],
    clock: cc_lb_engine::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    match config {
        StorageConfig::Postgres {
            url,
            pool: pool_config,
        } => open_postgres(url, pool_config, clock).await,
        StorageConfig::Sqlite { path } => open_sqlite(path, clock).await,
    }
}

pub async fn probe_postgres_connection(url: &str) -> Result<(), StorageFactoryError> {
    probe_postgres_connection_impl(url).await
}

#[cfg(feature = "postgres")]
pub async fn open_pg_fanout_pool(
    config: &StorageConfig,
) -> Result<Option<sqlx::PgPool>, StorageFactoryError> {
    let StorageConfig::Postgres {
        url,
        pool: pool_config,
    } = config
    else {
        return Ok(None);
    };
    open_postgres_pool(url, pool_config).await.map(Some)
}

#[cfg(not(feature = "postgres"))]
pub async fn open_pg_fanout_pool(
    config: &StorageConfig,
) -> Result<Option<()>, StorageFactoryError> {
    match config {
        StorageConfig::Postgres { .. } => Err(StorageFactoryError::FeatureDisabled {
            backend: "postgres".to_owned(),
        }),
        StorageConfig::Sqlite { .. } => Ok(None),
    }
}

fn map_init_error(
    error: cc_lb_storage_api::StorageError,
    configured: BackendKind,
) -> StorageFactoryError {
    match error {
        cc_lb_storage_api::StorageError::BackendKindMismatch { stored, .. } => {
            StorageFactoryError::BackendKindMismatch { stored, configured }
        }
        other => StorageFactoryError::InitFailed {
            message: other.to_string(),
        },
    }
}

#[cfg(not(feature = "sqlite"))]
async fn open_sqlite(
    _path: &Path,
    _clock: cc_lb_engine::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "sqlite".to_owned(),
    })
}

#[cfg(feature = "sqlite")]
async fn open_sqlite(
    path: &Path,
    clock: cc_lb_engine::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .map_err(|error| StorageFactoryError::ConnectionFailed {
            message: error.to_string(),
        })?;
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .map_err(|error| map_init_error(error, BackendKind::Sqlite))?;
    let storage = Arc::new(storage);
    Ok(OpenedStorage {
        storage: storage.clone() as Arc<dyn Storage>,
        managed_key_store: storage.clone() as Arc<dyn ManagedKeyStore>,
        plugin_blob_repo: storage as Arc<dyn PluginBlobRepo>,
    })
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(
    _url: &str,
    _pool: &cc_lb_config::PostgresPoolConfig,
    _clock: cc_lb_engine::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(not(feature = "postgres"))]
async fn probe_postgres_connection_impl(_url: &str) -> Result<(), StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(feature = "postgres")]
async fn open_postgres(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
    clock: cc_lb_engine::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    let pool = open_postgres_pool(url, pool_config).await?;

    let plugin_blob_repo = Arc::new(cc_lb_storage_postgres::PostgresPluginBlobRepo::new(
        pool.clone(),
    )) as Arc<dyn PluginBlobRepo>;
    let storage = cc_lb_storage_postgres::PostgresStorage::new(pool.clone(), clock.clone());
    let storage: Arc<dyn Storage> = Arc::new(storage);
    storage
        .initialize(BackendKind::Postgres)
        .await
        .map_err(|error| map_init_error(error, BackendKind::Postgres))?;
    let managed_key_store = Arc::new(cc_lb_storage_postgres::PostgresManagedKeyStore::new(
        pool,
        Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
        clock,
    ));
    Ok(OpenedStorage {
        storage,
        managed_key_store,
        plugin_blob_repo,
    })
}

#[cfg(feature = "postgres")]
async fn open_postgres_pool(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
) -> Result<sqlx::PgPool, StorageFactoryError> {
    use std::str::FromStr;
    use std::time::Duration;

    use sqlx::AssertSqlSafe;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let statement_timeout_ms = pool_config.statement_timeout_secs * 1000;
    let ssl_mode = parse_ssl_mode(&pool_config.sslmode)?;
    let connect_options =
        PgConnectOptions::from_str(url).map_err(|error| StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })?;
    let connect_options = connect_options.ssl_mode(ssl_mode);

    PgPoolOptions::new()
        .max_connections(pool_config.max_connections)
        .min_connections(pool_config.min_connections)
        .acquire_timeout(Duration::from_secs(pool_config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(pool_config.idle_timeout_secs))
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                let statement_timeout =
                    format!("SET statement_timeout = '{statement_timeout_ms}ms'");
                sqlx::query(AssertSqlSafe(statement_timeout))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await
        .map_err(|error| StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })
}

#[cfg(feature = "postgres")]
fn parse_ssl_mode(value: &str) -> Result<sqlx::postgres::PgSslMode, StorageFactoryError> {
    use sqlx::postgres::PgSslMode;
    match value.to_ascii_lowercase().as_str() {
        "disable" => Ok(PgSslMode::Disable),
        "allow" => Ok(PgSslMode::Allow),
        "prefer" => Ok(PgSslMode::Prefer),
        "require" => Ok(PgSslMode::Require),
        "verify-ca" | "verify_ca" => Ok(PgSslMode::VerifyCa),
        "verify-full" | "verify_full" => Ok(PgSslMode::VerifyFull),
        other => Err(StorageFactoryError::InitFailed {
            message: format!(
                "unknown postgres sslmode '{other}'; expected one of: disable, allow, prefer, \
                 require, verify-ca, verify-full"
            ),
        }),
    }
}

#[cfg(feature = "postgres")]
async fn probe_postgres_connection_impl(url: &str) -> Result<(), StorageFactoryError> {
    use std::time::Duration;

    use sqlx::Connection;
    use tokio::time::timeout;

    let probe = async {
        let mut connection = sqlx::PgConnection::connect(url).await?;
        sqlx::query("SELECT 1").execute(&mut connection).await?;
        Ok::<(), sqlx::Error>(())
    };

    match timeout(Duration::from_secs(5), probe).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        }),
        Err(_) => Err(StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": connection timed out",
        }),
    }
}
/// Extract only the host from a connection URL to avoid leaking credentials in logs.
#[cfg(feature = "postgres")]
fn host_only(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "<unknown host>".to_owned())
}
