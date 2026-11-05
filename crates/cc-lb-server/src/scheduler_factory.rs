use cc_lb_config::{SchedulerConfig, StorageConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "postgres")]
const SCHEDULER_SETUP_LOCK_KEY: i64 = 0x_CC1B_5CDE_0002_i64;
#[cfg(feature = "postgres")]
const APALIS_FIRST_MIGRATION_VERSION: i64 = 20220530084123;
#[cfg(feature = "postgres")]
const APALIS_PRE_1_0_FIRST_MIGRATION_CHECKSUM: [u8; 48] = [
    0x0e, 0x2a, 0xe5, 0xfd, 0x11, 0x7c, 0x32, 0x92, 0x5d, 0x7f, 0xf4, 0xba, 0x4f, 0x26, 0x36, 0x82,
    0x69, 0xfb, 0xa7, 0x70, 0x6b, 0xf1, 0xc3, 0xe9, 0x57, 0x8a, 0x76, 0x3a, 0x3e, 0x2a, 0xcc, 0x5f,
    0xd4, 0x4c, 0x8e, 0xdf, 0x2a, 0xa2, 0x5e, 0x7c, 0x0a, 0xc1, 0xc6, 0xeb, 0xc8, 0xe9, 0x5a, 0x9a,
];
#[cfg(feature = "postgres")]
const APALIS_1_0_FIRST_MIGRATION_CHECKSUM: [u8; 48] = [
    0xd0, 0x83, 0x9c, 0x6f, 0x57, 0xa3, 0x79, 0x76, 0x9d, 0xc2, 0x7c, 0xcd, 0x58, 0x1f, 0xeb, 0x3d,
    0x27, 0x09, 0x23, 0x9c, 0x8f, 0x13, 0x8e, 0x05, 0x27, 0x1c, 0x9e, 0x3c, 0x76, 0x0c, 0x45, 0x17,
    0xa7, 0x8a, 0x4d, 0x89, 0x12, 0xab, 0x3d, 0x63, 0xb0, 0x74, 0xb2, 0x8d, 0x15, 0xec, 0x74, 0xe9,
];

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

    use sqlx::sqlite::{
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
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;

    let connect_options = crate::storage_factory::postgres_connect_options(
        url,
        scheduler.separate_pool.sslmode.as_deref(),
    )
    .map_err(|error| {
        use crate::storage_factory::StorageFactoryError;

        let message = match error {
            StorageFactoryError::ConnectionFailed { message }
            | StorageFactoryError::InitFailed { message } => message,
            error => error.to_string(),
        };
        SchedulerFactoryError::ConnectionFailed { message }
    })?;
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
                sqlx::query(sqlx::AssertSqlSafe(statement_timeout))
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
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
    connect_options: &sqlx::postgres::PgConnectOptions,
    pool: &sqlx::PgPool,
) -> Result<(), SchedulerFactoryError> {
    let connect_options = connect_options.clone();
    let pool = pool.clone();
    tokio::spawn(async move {
        use sqlx::Connection as _;

        let mut conn = sqlx::PgConnection::connect_with(&connect_options)
            .await
            .map_err(migration_error)?;
        sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(SCHEDULER_SETUP_LOCK_KEY)
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler")
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        let setup_result: Result<(), SchedulerFactoryError> = async {
            reconcile_pre_1_0_apalis_migration_history(&mut conn).await?;
            let migrator = apalis_postgres::PostgresStorage::<()>::migrations();
            migrator
                .run_direct(None, &mut conn, false)
                .await
                .map_err(migration_error)?;
            cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
                .await
                .map_err(migration_error)?;
            Ok(())
        }
        .await;
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(SCHEDULER_SETUP_LOCK_KEY)
            .execute(&mut conn)
            .await;
        setup_result
    })
    .await
    .map_err(migration_error)?
}

#[cfg(feature = "postgres")]
async fn reconcile_pre_1_0_apalis_migration_history(
    conn: &mut sqlx::PgConnection,
) -> Result<(), SchedulerFactoryError> {
    use sqlx::Acquire as _;

    let legacy_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('cc_lb_scheduler._sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await
            .map_err(migration_error)?;
    if !legacy_exists {
        return Ok(());
    }

    let mut tx = conn.begin().await.map_err(migration_error)?;
    let legacy_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM cc_lb_scheduler._sqlx_migrations")
            .fetch_one(&mut *tx)
            .await
            .map_err(migration_error)?;
    if legacy_rows == 0 {
        sqlx::query("DROP TABLE cc_lb_scheduler._sqlx_migrations")
            .execute(&mut *tx)
            .await
            .map_err(migration_error)?;
        tx.commit().await.map_err(migration_error)?;
        return Ok(());
    }

    sqlx::query("CREATE SCHEMA IF NOT EXISTS apalis")
        .execute(&mut *tx)
        .await
        .map_err(migration_error)?;
    let target_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('apalis._sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *tx)
            .await
            .map_err(migration_error)?;
    if target_exists {
        let target_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations")
            .fetch_one(&mut *tx)
            .await
            .map_err(migration_error)?;
        if target_rows > 0 {
            tx.rollback().await.map_err(migration_error)?;
            return Err(SchedulerFactoryError::MigrationFailed {
                message: format!(
                    "both cc_lb_scheduler._sqlx_migrations ({legacy_rows} rows) and non-empty apalis._sqlx_migrations ({target_rows} rows) exist; refusing to overwrite authoritative scheduler migration ledger"
                ),
            });
        }
        sqlx::query("DROP TABLE apalis._sqlx_migrations")
            .execute(&mut *tx)
            .await
            .map_err(migration_error)?;
    }

    let first_checksum: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT checksum FROM cc_lb_scheduler._sqlx_migrations WHERE version = $1",
    )
    .bind(APALIS_FIRST_MIGRATION_VERSION)
    .fetch_optional(&mut *tx)
    .await
    .map_err(migration_error)?;

    match first_checksum.as_deref() {
        Some(bytes) if bytes == APALIS_PRE_1_0_FIRST_MIGRATION_CHECKSUM => {
            sqlx::query("ALTER TABLE cc_lb_scheduler._sqlx_migrations SET SCHEMA apalis")
                .execute(&mut *tx)
                .await
                .map_err(migration_error)?;
            let updated = sqlx::query(
                "UPDATE apalis._sqlx_migrations SET checksum = $1 WHERE version = $2 AND checksum = $3",
            )
            .bind(&APALIS_1_0_FIRST_MIGRATION_CHECKSUM[..])
            .bind(APALIS_FIRST_MIGRATION_VERSION)
            .bind(&APALIS_PRE_1_0_FIRST_MIGRATION_CHECKSUM[..])
            .execute(&mut *tx)
            .await
            .map_err(migration_error)?;
            if updated.rows_affected() != 1 {
                tx.rollback().await.map_err(migration_error)?;
                return Err(SchedulerFactoryError::MigrationFailed {
                    message: format!(
                        "expected to update 1 checksum row for migration {APALIS_FIRST_MIGRATION_VERSION}, updated {}",
                        updated.rows_affected()
                    ),
                });
            }
            tx.commit().await.map_err(migration_error)?;
            Ok(())
        }
        Some(bytes) if bytes == APALIS_1_0_FIRST_MIGRATION_CHECKSUM => {
            sqlx::query("ALTER TABLE cc_lb_scheduler._sqlx_migrations SET SCHEMA apalis")
                .execute(&mut *tx)
                .await
                .map_err(migration_error)?;
            tx.commit().await.map_err(migration_error)?;
            Ok(())
        }
        _ => {
            tx.rollback().await.map_err(migration_error)?;
            Err(SchedulerFactoryError::MigrationFailed {
                message: format!(
                    "unexpected checksum for migration {APALIS_FIRST_MIGRATION_VERSION} in cc_lb_scheduler._sqlx_migrations"
                ),
            })
        }
    }
}

#[cfg(feature = "sqlite")]
fn connection_error(error: sqlx::Error) -> SchedulerFactoryError {
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
fn connection_error_with_host(url: &str, error: sqlx::Error) -> SchedulerFactoryError {
    SchedulerFactoryError::ConnectionFailed {
        message: host_only(url) + ": " + &error.to_string(),
    }
}

#[cfg(feature = "postgres")]
fn host_only(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "<unknown host>".to_owned())
}
