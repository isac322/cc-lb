//! Storage factory — the ONLY place in cc-lb-server that contains
//! #[cfg(feature = "postgres")] blocks.

use std::path::Path;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::StorageConfig;
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, PluginBlobRepo, PluginRegistryRepo, Storage, StorageError,
    StorageResult,
};

pub struct OpenedStorage {
    pub storage: Arc<dyn Storage>,
    pub managed_key_store: Arc<dyn ManagedKeyStore>,
    pub plugin_registry_repo: Arc<dyn PluginRegistryRepo>,
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

pub enum BackendHandle {
    Redb(Arc<cc_lb_storage_redb::Storage>),
    #[cfg(feature = "postgres")]
    Postgres(Arc<dyn Storage>),
}

impl BackendHandle {
    pub async fn put_anthropic_api_key_ciphertext(
        &self,
        _storage_key: &str,
        _ciphertext: &[u8],
    ) -> StorageResult<()> {
        match self {
            Self::Redb(_) => Err(raw_ciphertext_unavailable(
                "put anthropic API key ciphertext",
            )),
            #[cfg(feature = "postgres")]
            Self::Postgres(storage) => {
                storage
                    .put_anthropic_api_key_ciphertext(_storage_key, _ciphertext)
                    .await
            }
        }
    }

    pub async fn get_oauth_ciphertext(
        &self,
        _principal_id: &str,
        _provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        match self {
            Self::Redb(_) => Err(raw_ciphertext_unavailable("get OAuth ciphertext")),
            #[cfg(feature = "postgres")]
            Self::Postgres(storage) => storage.get_oauth_ciphertext(_principal_id, _provider).await,
        }
    }
}

fn raw_ciphertext_unavailable(operation: &str) -> StorageError {
    StorageError::Unavailable {
        message: format!("{operation} is unavailable for redb through storage_factory"),
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
    master_key: [u8; 32],
) -> Result<OpenedStorage, StorageFactoryError> {
    match config {
        StorageConfig::Redb { path } => open_redb(path, master_key).await,
        StorageConfig::Postgres {
            url,
            pool: pool_config,
        } => open_postgres(url, pool_config).await,
        StorageConfig::Sqlite { .. } => unimplemented!("sqlite backend not yet wired"),
    }
}

pub async fn probe_postgres_connection(url: &str) -> Result<(), StorageFactoryError> {
    probe_postgres_connection_impl(url).await
}

#[cfg(feature = "redb")]
async fn open_redb(
    path: &Path,
    master_key: [u8; 32],
) -> Result<OpenedStorage, StorageFactoryError> {
    let storage =
        cc_lb_storage_redb::Storage::open(path, master_key).map_err(map_redb_open_error)?;
    storage
        .initialize(BackendKind::Redb)
        .map_err(map_redb_open_error)?;
    let storage = Arc::new(storage);
    let managed_key_store = Arc::new(cc_lb_storage_redb::RedbManagedKeyStore::new(
        storage.clone(),
    ));
    let plugin_registry_repo = Arc::new(
        cc_lb_storage_redb::RedbPluginRegistryRepo::new(storage.as_ref().clone())
            .map_err(map_redb_open_error)?,
    ) as Arc<dyn PluginRegistryRepo>;
    let plugin_blob_repo = Arc::new(
        cc_lb_storage_redb::RedbPluginBlobRepo::new(storage.as_ref().clone())
            .map_err(map_redb_open_error)?,
    ) as Arc<dyn PluginBlobRepo>;
    Ok(OpenedStorage {
        storage: storage as Arc<dyn Storage>,
        managed_key_store,
        plugin_registry_repo,
        plugin_blob_repo,
    })
}

#[cfg(not(feature = "redb"))]
async fn open_redb(
    _path: &Path,
    _master_key: [u8; 32],
) -> Result<OpenedStorage, StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "redb".to_owned(),
    })
}

#[cfg(feature = "redb")]
fn map_redb_open_error(error: cc_lb_storage_redb::StorageError) -> StorageFactoryError {
    match error {
        cc_lb_storage_redb::StorageError::BackendKindMismatch { stored, configured } => {
            StorageFactoryError::BackendKindMismatch { stored, configured }
        }
        other => StorageFactoryError::InitFailed {
            message: other.to_string(),
        },
    }
}

#[cfg(feature = "postgres")]
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

#[cfg(not(feature = "postgres"))]
async fn open_postgres(
    _url: &str,
    _pool: &cc_lb_config::PostgresPoolConfig,
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
) -> Result<OpenedStorage, StorageFactoryError> {
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

    let pool = PgPoolOptions::new()
        .max_connections(pool_config.max_connections)
        .min_connections(pool_config.min_connections)
        .acquire_timeout(Duration::from_secs(pool_config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(pool_config.idle_timeout_secs))
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                // sqlx 0.9 requires SqlSafeStr; statement_timeout_ms is a u64, so the
                // interpolated string is SQL-injection safe by construction.
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
        })?;

    let plugin_registry_repo = Arc::new(cc_lb_storage_postgres::PostgresPluginRegistryRepo::new(
        pool.clone(),
    )) as Arc<dyn PluginRegistryRepo>;
    let plugin_blob_repo = Arc::new(cc_lb_storage_postgres::PostgresPluginBlobRepo::new(
        pool.clone(),
    )) as Arc<dyn PluginBlobRepo>;
    let storage = cc_lb_storage_postgres::PostgresStorage::new(pool.clone());
    let storage: Arc<dyn Storage> = Arc::new(storage);
    storage
        .initialize(BackendKind::Postgres)
        .await
        .map_err(|error| map_init_error(error, BackendKind::Postgres))?;
    let managed_key_store = Arc::new(cc_lb_storage_postgres::PostgresManagedKeyStore::new(
        pool,
        Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
    ));
    Ok(OpenedStorage {
        storage,
        managed_key_store,
        plugin_registry_repo,
        plugin_blob_repo,
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
