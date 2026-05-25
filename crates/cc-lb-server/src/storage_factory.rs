//! Storage factory — the ONLY place in cc-lb-server that contains
//! #[cfg(feature = "postgres")] blocks.

use std::path::Path;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::StorageConfig;
use cc_lb_storage_api::{BackendKind, Storage};

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

/// Build an `Arc<dyn Storage>` from config.
/// The caller is responsible for constructing `AeadService` from `AeadConfig.key_env`
/// and passing it here.
pub async fn open_storage(
    config: &StorageConfig,
    _aead: Arc<AeadService>,
) -> Result<Arc<dyn Storage>, StorageFactoryError> {
    match config {
        StorageConfig::Redb { path } => open_redb(path).await,
        StorageConfig::Postgres {
            url,
            pool: pool_config,
        } => open_postgres(url, pool_config).await,
    }
}

pub async fn probe_postgres_connection(url: &str) -> Result<(), StorageFactoryError> {
    probe_postgres_connection_impl(url).await
}

async fn open_redb(_path: &Path) -> Result<Arc<dyn Storage>, StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "redb-storage-api".to_owned(),
    })
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(
    _url: &str,
    _pool: &cc_lb_config::PostgresPoolConfig,
) -> Result<Arc<dyn Storage>, StorageFactoryError> {
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
) -> Result<Arc<dyn Storage>, StorageFactoryError> {
    use std::str::FromStr;
    use std::time::Duration;

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
                let statement_timeout =
                    format!("SET statement_timeout = '{statement_timeout_ms}ms'");
                sqlx::query(&statement_timeout).execute(conn).await?;
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await
        .map_err(|error| StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })?;

    let storage = cc_lb_storage_postgres::PostgresStorage::new(pool);
    let storage: Arc<dyn Storage> = Arc::new(storage);
    storage
        .initialize(BackendKind::Postgres)
        .await
        .map_err(|error| map_init_error(error, BackendKind::Postgres))?;
    Ok(storage)
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
