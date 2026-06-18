use cc_lb_config::{SchedulerConfig, StorageConfig};
#[cfg(feature = "postgres")]
pub use cc_lb_scheduler::worker::PostgresSchedulerStorage;
pub use cc_lb_scheduler::worker::SchedulerBackend;
#[cfg(feature = "sqlite")]
pub use cc_lb_scheduler::worker::SqliteSchedulerStorage;

#[derive(Debug)]
pub struct OpenedScheduler {
    pub backend: SchedulerBackend,
    pub leader_connection: Option<LeaderConnectionHandle>,
}

#[cfg(feature = "postgres")]
#[derive(Debug)]
pub struct LeaderConnectionHandle {
    pub election: cc_lb_scheduler::leader_election::LeaderElection,
}

#[cfg(not(feature = "postgres"))]
#[derive(Debug)]
pub struct LeaderConnectionHandle;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerFactoryError {
    #[error("scheduler backend '{backend}' requires the '{backend}' cargo feature")]
    FeatureDisabled { backend: String },
    #[error("scheduler connection failed: {message}")]
    ConnectionFailed { message: String },
    #[error("scheduler migration failed: {message}")]
    MigrationFailed { message: String },
    #[error("scheduler leader connection failed: {message}")]
    LeaderConnectionFailed { message: String },
}

pub async fn open_scheduler_storage(
    storage: &StorageConfig,
    scheduler: &SchedulerConfig,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    match storage {
        StorageConfig::Sqlite { path } => open_sqlite(path, scheduler).await,
        StorageConfig::Postgres { url, pool: _ } => open_postgres(url, scheduler).await,
    }
}

#[cfg(not(feature = "sqlite"))]
async fn open_sqlite(
    _path: &std::path::Path,
    _scheduler: &SchedulerConfig,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    Err(SchedulerFactoryError::FeatureDisabled {
        backend: "sqlite".to_owned(),
    })
}

#[cfg(feature = "sqlite")]
async fn open_sqlite(
    path: &std::path::Path,
    scheduler: &SchedulerConfig,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    use std::str::FromStr as _;
    use std::time::Duration;

    use scheduler_sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };

    let database_url = format!("sqlite://{}", path.display());
    let options = SqliteConnectOptions::from_str(&database_url)
        .map_err(connection_error)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(scheduler.separate_pool.max_connections)
        .min_connections(scheduler.separate_pool.min_connections)
        .acquire_timeout(Duration::from_secs(
            scheduler.separate_pool.acquire_timeout_secs,
        ))
        .idle_timeout(Duration::from_secs(
            scheduler.separate_pool.idle_timeout_secs,
        ))
        .connect_with(options)
        .await
        .map_err(connection_error)?;
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .map_err(migration_error)?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .map_err(migration_error)?;
    let storage = apalis_sqlite::SqliteStorage::new(&pool);
    Ok(OpenedScheduler {
        backend: SchedulerBackend::Sqlite(SqliteSchedulerStorage { pool, storage }),
        leader_connection: None,
    })
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(
    _url: &str,
    _scheduler: &SchedulerConfig,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    Err(SchedulerFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(feature = "postgres")]
async fn open_postgres(
    url: &str,
    scheduler: &SchedulerConfig,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    use std::str::FromStr as _;
    use std::time::Duration;

    use scheduler_sqlx::Connection as _;
    use scheduler_sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let ssl_mode = parse_ssl_mode(&scheduler.separate_pool.sslmode)?;
    let connect_options = PgConnectOptions::from_str(url)
        .map_err(|error| connection_error_with_host(url, error))?
        .ssl_mode(ssl_mode);
    let statement_timeout_ms = scheduler.separate_pool.statement_timeout_secs * 1000;
    let pool = PgPoolOptions::new()
        .max_connections(scheduler.separate_pool.max_connections)
        .min_connections(scheduler.separate_pool.min_connections)
        .acquire_timeout(Duration::from_secs(
            scheduler.separate_pool.acquire_timeout_secs,
        ))
        .idle_timeout(Duration::from_secs(
            scheduler.separate_pool.idle_timeout_secs,
        ))
        .after_connect(move |connection, _metadata| {
            Box::pin(async move {
                let statement_timeout =
                    format!("SET statement_timeout = '{statement_timeout_ms}ms'");
                scheduler_sqlx::query(&statement_timeout)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(connect_options.clone())
        .await
        .map_err(|error| connection_error_with_host(url, error))?;
    apalis_postgres::PostgresStorage::setup(&pool)
        .await
        .map_err(migration_error)?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .map_err(migration_error)?;
    let leader = scheduler_sqlx::PgConnection::connect_with(&connect_options)
        .await
        .map_err(|error| SchedulerFactoryError::LeaderConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })?;
    let election = cc_lb_scheduler::leader_election::LeaderElection::from_postgres_connection(
        url.to_owned(),
        leader,
        scheduler.leader_lock_key,
    );
    let storage = apalis_postgres::PostgresStorage::new(&pool);
    Ok(OpenedScheduler {
        backend: SchedulerBackend::Postgres(PostgresSchedulerStorage { pool, storage }),
        leader_connection: Some(LeaderConnectionHandle { election }),
    })
}

#[cfg(feature = "sqlite")]
fn connection_error(error: scheduler_sqlx::Error) -> SchedulerFactoryError {
    SchedulerFactoryError::ConnectionFailed {
        message: error.to_string(),
    }
}

fn migration_error(error: impl std::fmt::Display) -> SchedulerFactoryError {
    SchedulerFactoryError::MigrationFailed {
        message: error.to_string(),
    }
}

#[cfg(feature = "postgres")]
fn connection_error_with_host(url: &str, error: scheduler_sqlx::Error) -> SchedulerFactoryError {
    SchedulerFactoryError::ConnectionFailed {
        message: host_only(url) + ": " + &error.to_string(),
    }
}

#[cfg(feature = "postgres")]
fn parse_ssl_mode(
    value: &str,
) -> Result<scheduler_sqlx::postgres::PgSslMode, SchedulerFactoryError> {
    use scheduler_sqlx::postgres::PgSslMode;
    match value.to_ascii_lowercase().as_str() {
        "disable" => Ok(PgSslMode::Disable),
        "allow" => Ok(PgSslMode::Allow),
        "prefer" => Ok(PgSslMode::Prefer),
        "require" => Ok(PgSslMode::Require),
        "verify-ca" | "verify_ca" => Ok(PgSslMode::VerifyCa),
        "verify-full" | "verify_full" => Ok(PgSslMode::VerifyFull),
        other => Err(SchedulerFactoryError::ConnectionFailed {
            message: format!("unknown postgres sslmode '{other}'"),
        }),
    }
}

#[cfg(feature = "postgres")]
fn host_only(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "<unknown host>".to_owned())
}
