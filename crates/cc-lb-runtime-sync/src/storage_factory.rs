//! Storage factory — shared backend opening & Postgres pool configuration.

#[cfg(all(test, feature = "postgres"))]
mod pool_metrics_tests {
    use super::record_postgres_pool_metrics;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use sqlx::postgres::PgPoolOptions;

    fn assert_pool_samples(rendered: &str, size: u32, idle: u32) {
        for (name, value) in [("size", size), ("idle", idle), ("in_use", size - idle)] {
            assert!(
                rendered.contains(&format!(
                    "cc_lb_sqlx_pool_{name}{{store=\"postgres\"}} {value}"
                )),
                "missing {name}={value} in rendered metrics:\n{rendered}"
            );
        }
    }

    #[tokio::test]
    async fn postgres_pool_metrics_track_live_connections() {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skipped: CI_POSTGRES_URL unset");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .min_connections(1)
            .connect(&url)
            .await
            .expect("connect isolated test pool");
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || record_postgres_pool_metrics(&pool));
        assert_pool_samples(&handle.render(), pool.size(), pool.num_idle() as u32);

        let connection = pool.acquire().await.expect("acquire connection");
        let mut second_connection = pool.acquire().await.expect("acquire second connection");
        sqlx::query("SELECT 1")
            .execute(&mut *second_connection)
            .await
            .expect("execute independent query");
        metrics::with_local_recorder(&recorder, || record_postgres_pool_metrics(&pool));
        assert_eq!((pool.size(), pool.num_idle()), (2, 0));
        assert_pool_samples(&handle.render(), 2, 0);

        drop(second_connection);
        drop(connection);
        pool.close().await;
        metrics::with_local_recorder(&recorder, || record_postgres_pool_metrics(&pool));
        assert_pool_samples(&handle.render(), 0, 0);
    }
}

use std::path::Path;
use std::sync::Arc;

use cc_lb_config::StorageConfig;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use cc_lb_storage_api::MetaStore;
use cc_lb_storage_api::{ManagedKeyStore, Storage};
#[cfg(feature = "postgres")]
const PG_FANOUT_MAX_CONNECTIONS: u32 = 2;

pub struct OpenedStorage {
    pub storage: Arc<dyn Storage>,
    pub managed_key_store: Arc<dyn ManagedKeyStore>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageFactoryError {
    #[error("storage backend '{backend}' requires the '{backend}' cargo feature")]
    FeatureDisabled { backend: String },
    #[error("storage connection failed: {message}")]
    ConnectionFailed { message: String },
    #[error("storage initialization failed: {message}")]
    InitFailed { message: String },
}

pub async fn open_storage(
    config: &StorageConfig,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    match config {
        StorageConfig::Postgres {
            url,
            pool: pool_config,
        } => open_postgres(url, pool_config, clock).await,
        StorageConfig::Sqlite { path } => open_sqlite(path, clock).await,
    }
}

pub async fn probe_postgres_connection(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
) -> Result<(), StorageFactoryError> {
    probe_postgres_connection_impl(url, pool_config).await
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
    let mut fanout_pool_config = pool_config.clone();
    fanout_pool_config.max_connections = PG_FANOUT_MAX_CONNECTIONS;
    fanout_pool_config.min_connections = 0;
    fanout_pool_config.pgbouncer_transaction_pooling = false;
    fanout_pool_config.statement_cache_capacity = None;
    open_postgres_pool(pool_config.effective_listener_url(url), &fanout_pool_config)
        .await
        .map(Some)
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

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn init_error(error: cc_lb_storage_api::StorageError) -> StorageFactoryError {
    StorageFactoryError::InitFailed {
        message: error.to_string(),
    }
}

#[cfg(not(feature = "sqlite"))]
async fn open_sqlite(
    _path: &Path,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "sqlite".to_owned(),
    })
}

#[cfg(feature = "sqlite")]
async fn open_sqlite(
    path: &Path,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .map_err(|error| StorageFactoryError::ConnectionFailed {
            message: error.to_string(),
        })?;
    storage.initialize().await.map_err(init_error)?;
    let storage = Arc::new(storage);
    Ok(OpenedStorage {
        storage: storage.clone() as Arc<dyn Storage>,
        managed_key_store: storage as Arc<dyn ManagedKeyStore>,
    })
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(
    _url: &str,
    _pool: &cc_lb_config::PostgresPoolConfig,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(not(feature = "postgres"))]
async fn probe_postgres_connection_impl(
    _url: &str,
    _pool_config: &cc_lb_config::PostgresPoolConfig,
) -> Result<(), StorageFactoryError> {
    Err(StorageFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(feature = "postgres")]
async fn open_postgres(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedStorage, StorageFactoryError> {
    let pool = open_postgres_pool(url, pool_config).await?;
    let mut listener_pool_config = pool_config.clone();
    listener_pool_config.max_connections = 2;
    listener_pool_config.min_connections = 0;
    listener_pool_config.pgbouncer_transaction_pooling = false;
    listener_pool_config.statement_cache_capacity = None;
    let listener_pool = open_postgres_pool(
        pool_config.effective_listener_url(url),
        &listener_pool_config,
    )
    .await?;

    let storage = Arc::new(
        cc_lb_storage_postgres::PostgresStorage::new_with_listener_pool(
            pool.clone(),
            listener_pool,
            clock.clone(),
        ),
    );
    storage.initialize().await.map_err(init_error)?;
    record_postgres_pool_metrics(storage.pool());
    let weak_storage = Arc::downgrade(&storage);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let Some(storage) = weak_storage.upgrade() else {
                break;
            };
            record_postgres_pool_metrics(storage.pool());
        }
    });
    let managed_key_store = Arc::new(cc_lb_storage_postgres::PostgresManagedKeyStore::new(
        pool,
        Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
        clock,
    ));
    Ok(OpenedStorage {
        storage: storage as Arc<dyn Storage>,
        managed_key_store,
    })
}

#[cfg(feature = "postgres")]
fn record_postgres_pool_metrics(pool: &sqlx::PgPool) {
    let size = pool.size();
    let idle = pool.num_idle() as u32;
    metrics::gauge!("cc_lb_sqlx_pool_size", "store" => "postgres").set(f64::from(size));
    metrics::gauge!("cc_lb_sqlx_pool_idle", "store" => "postgres").set(f64::from(idle));
    metrics::gauge!("cc_lb_sqlx_pool_in_use", "store" => "postgres")
        .set(f64::from(size.saturating_sub(idle)));
}

#[cfg(feature = "postgres")]
async fn open_postgres_pool(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
) -> Result<sqlx::PgPool, StorageFactoryError> {
    use std::time::Duration;

    use sqlx::AssertSqlSafe;
    use sqlx::postgres::PgPoolOptions;

    let statement_timeout_ms = pool_config.statement_timeout_secs * 1000;
    let mut connect_options = postgres_connect_options(url, pool_config.sslmode.as_deref())?;
    if let Some(cache_capacity) = pool_config.effective_statement_cache_capacity() {
        connect_options = connect_options.statement_cache_capacity(cache_capacity);
    }

    let mut builder = PgPoolOptions::new()
        .max_connections(pool_config.max_connections)
        .min_connections(pool_config.min_connections)
        .acquire_timeout(Duration::from_secs(pool_config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(pool_config.idle_timeout_secs))
        .max_lifetime(Duration::from_secs(pool_config.max_lifetime_secs))
        .test_before_acquire(pool_config.test_before_acquire);
    if !pool_config.pgbouncer_transaction_pooling {
        builder = builder.after_connect(move |conn, _meta| {
            Box::pin(async move {
                let statement_timeout =
                    format!("SET statement_timeout = '{statement_timeout_ms}ms'");
                sqlx::query(AssertSqlSafe(statement_timeout))
                    .execute(&mut *conn)
                    .await?;
                let idle_in_tx_timeout =
                    format!("SET idle_in_transaction_session_timeout = '{statement_timeout_ms}ms'");
                sqlx::query(AssertSqlSafe(idle_in_tx_timeout))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        });
    }
    builder
        .connect_with(connect_options)
        .await
        .map_err(|error| StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })
}

#[cfg(feature = "postgres")]
pub fn postgres_connect_options(
    url: &str,
    sslmode: Option<&str>,
) -> Result<sqlx::postgres::PgConnectOptions, StorageFactoryError> {
    use sqlx::ConnectOptions;
    use sqlx::postgres::{PgConnectOptions, PgSslMode};

    let explicit_ssl_mode = sslmode.map(parse_ssl_mode).transpose()?;
    let parsed_url =
        url::Url::parse(url).map_err(|error| StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })?;
    let options = PgConnectOptions::from_url(&parsed_url).map_err(|error| {
        StorageFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        }
    })?;
    if let Some(ssl_mode) = explicit_ssl_mode {
        return Ok(options.ssl_mode(ssl_mode));
    }
    if parsed_url
        .query_pairs()
        .any(|(key, _)| matches!(key.as_ref(), "sslmode" | "ssl-mode"))
    {
        return Ok(options);
    }
    // Preserve the application's default even when PGSSLMODE is set.
    Ok(options.ssl_mode(PgSslMode::Prefer))
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
async fn probe_postgres_connection_impl(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
) -> Result<(), StorageFactoryError> {
    use std::time::Duration;

    use sqlx::Connection;
    use tokio::time::timeout;

    let connect_options = postgres_connect_options(url, pool_config.sslmode.as_deref())?;
    let probe = async {
        let mut connection = sqlx::PgConnection::connect_with(&connect_options).await?;
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
