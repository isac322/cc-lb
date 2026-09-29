use cc_lb_config::{SchedulerConfig, StorageConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "postgres")]
const SCHEDULER_SETUP_LOCK_KEY: i64 = 0x_CC1B_5CDE_0002_i64;

#[cfg(feature = "postgres")]
pub use cc_lb_scheduler::worker::PostgresSchedulerBackend;
pub use cc_lb_scheduler::worker::SchedulerBackend;
#[cfg(feature = "sqlite")]
pub use cc_lb_scheduler::worker::SqliteSchedulerBackend;

#[derive(Debug)]
pub struct OpenedScheduler {
    pub backend: SchedulerBackend,
}

impl OpenedScheduler {
    pub fn lazy_handle(&self) -> SchedulerBackend {
        self.backend.clone()
    }

    pub async fn spawn(
        &self,
        ctx: cc_lb_scheduler::worker::SchedulerCtx,
        cancel: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, SchedulerFactoryError> {
        self.backend.spawn(ctx, cancel).await.map_err(|error| {
            SchedulerFactoryError::StartupFailed {
                message: error.to_string(),
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerFactoryError {
    #[error("scheduler backend '{backend}' requires the '{backend}' cargo feature")]
    FeatureDisabled { backend: String },
    #[error("scheduler connection failed: {message}")]
    ConnectionFailed { message: String },
    #[error("scheduler migration failed: {message}")]
    MigrationFailed { message: String },
    #[error("scheduler startup failed: {message}")]
    StartupFailed { message: String },
}

pub async fn open_scheduler_storage(
    storage: &StorageConfig,
    scheduler: &SchedulerConfig,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    let opened = match storage {
        StorageConfig::Sqlite { path } => open_sqlite(path, scheduler, clock).await,
        StorageConfig::Postgres { url, pool: _ } => open_postgres(url, scheduler, clock).await,
    };
    cc_lb_scheduler::scheduler_metrics::set_scheduler_init_failure(opened.is_err());
    opened
}

#[cfg(not(feature = "sqlite"))]
async fn open_sqlite(
    _path: &std::path::Path,
    _scheduler: &SchedulerConfig,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    Err(SchedulerFactoryError::FeatureDisabled {
        backend: "sqlite".to_owned(),
    })
}

#[cfg(feature = "sqlite")]
async fn open_sqlite(
    path: &std::path::Path,
    scheduler: &SchedulerConfig,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    use std::str::FromStr as _;
    use std::time::Duration;

    use scheduler_sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };

    let scheduler_path = scheduler_sqlite_path(path);
    let database_url = format!("sqlite://{}", scheduler_path.display());
    let options = SqliteConnectOptions::from_str(&database_url)
        .map_err(connection_error)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    let configured_max = scheduler.separate_pool.max_connections;
    if configured_max != 1 {
        tracing::warn!(
            configured = configured_max,
            "scheduler.separate_pool.max_connections is forced to 1 for SQLite backend; reconfigure to silence this warning",
        );
    }
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
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
    Ok(OpenedScheduler {
        backend: SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(pool, clock)),
    })
}

#[cfg(feature = "sqlite")]
fn scheduler_sqlite_path(path: &std::path::Path) -> std::path::PathBuf {
    path.with_extension("scheduler.sqlite")
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(
    _url: &str,
    _scheduler: &SchedulerConfig,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    Err(SchedulerFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(feature = "postgres")]
async fn open_postgres(
    url: &str,
    scheduler: &SchedulerConfig,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    use std::str::FromStr as _;
    use std::time::Duration;

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
                    .execute(&mut *connection)
                    .await?;
                scheduler_sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(connect_options.clone())
        .await
        .map_err(|error| connection_error_with_host(url, error))?;
    run_postgres_scheduler_setup(&connect_options, &pool).await?;
    Ok(OpenedScheduler {
        backend: SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool)),
    })
}

#[cfg(feature = "postgres")]
async fn run_postgres_scheduler_setup(
    connect_options: &scheduler_sqlx::postgres::PgConnectOptions,
    pool: &scheduler_sqlx::PgPool,
) -> Result<(), SchedulerFactoryError> {
    let connect_options = connect_options.clone();
    let pool = pool.clone();
    tokio::spawn(async move {
        use scheduler_sqlx::Connection as _;

        let mut conn = scheduler_sqlx::PgConnection::connect_with(&connect_options)
            .await
            .map_err(migration_error)?;
        scheduler_sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        scheduler_sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(SCHEDULER_SETUP_LOCK_KEY)
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        scheduler_sqlx::query("CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler")
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        let migrator = apalis_postgres::PostgresStorage::<(), (), ()>::migrations();
        let apalis_result = migrator.run_direct(&mut conn).await;
        let post_setup_result = if apalis_result.is_ok() {
            cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await
        } else {
            Ok(())
        };
        let _ = scheduler_sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(SCHEDULER_SETUP_LOCK_KEY)
            .execute(&mut conn)
            .await;
        apalis_result.map_err(migration_error)?;
        post_setup_result.map_err(migration_error)
    })
    .await
    .map_err(migration_error)?
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
