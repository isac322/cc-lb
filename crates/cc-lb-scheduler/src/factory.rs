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
pub use crate::worker::PostgresSchedulerBackend;
pub use crate::worker::SchedulerBackend;
#[cfg(feature = "sqlite")]
pub use crate::worker::SqliteSchedulerBackend;

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
        ctx: crate::worker::SchedulerCtx,
        cancel: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, SchedulerFactoryError> {
        crate::scheduler_metrics::touch_scheduler_metric_handles();
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerPoolRole {
    Worker,
    EnqueueOnly,
}

pub async fn open_scheduler_storage(
    storage: &StorageConfig,
    scheduler: &SchedulerConfig,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    open_scheduler_storage_with_role(storage, scheduler, SchedulerPoolRole::Worker, clock).await
}

pub async fn open_scheduler_storage_with_role(
    storage: &StorageConfig,
    scheduler: &SchedulerConfig,
    role: SchedulerPoolRole,
    clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    let opened = match storage {
        StorageConfig::Sqlite { path } => open_sqlite(path, scheduler, clock).await,
        StorageConfig::Postgres { url, pool } => {
            open_postgres(url, pool, scheduler, role, clock).await
        }
    };
    crate::scheduler_metrics::set_scheduler_init_failure(opened.is_err());
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
    crate::migrations::apply_post_setup_migrations(&pool)
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
    _storage_pool: &cc_lb_config::PostgresPoolConfig,
    _scheduler: &SchedulerConfig,
    _role: SchedulerPoolRole,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    Err(SchedulerFactoryError::FeatureDisabled {
        backend: "postgres".to_owned(),
    })
}

#[cfg(feature = "postgres")]
async fn open_postgres(
    url: &str,
    storage_pool: &cc_lb_config::PostgresPoolConfig,
    scheduler: &SchedulerConfig,
    role: SchedulerPoolRole,
    _clock: cc_lb_clock::ClockHandle,
) -> Result<OpenedScheduler, SchedulerFactoryError> {
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;

    let mut connect_options =
        postgres_connect_options(url, scheduler.separate_pool.sslmode.as_deref())?;
    if let Some(cache_capacity) = storage_pool.effective_statement_cache_capacity() {
        connect_options = connect_options.statement_cache_capacity(cache_capacity);
    }
    let direct_listener_url = storage_pool.effective_listener_url(url);
    let direct_connect_options = postgres_connect_options(
        direct_listener_url,
        scheduler.separate_pool.sslmode.as_deref(),
    )?;
    if storage_pool.listener_url.is_some() {
        verify_same_postgres_database(
            url,
            &connect_options,
            direct_listener_url,
            &direct_connect_options,
        )
        .await?;
    }

    let pgbouncer_tx_pooling = storage_pool.pgbouncer_transaction_pooling;
    let hstore_schema = run_postgres_scheduler_setup(
        direct_listener_url,
        &direct_connect_options,
        pgbouncer_tx_pooling,
    )
    .await?;

    let (max_connections, min_connections) = match role {
        SchedulerPoolRole::Worker => (
            scheduler.separate_pool.max_connections,
            scheduler.separate_pool.min_connections,
        ),
        SchedulerPoolRole::EnqueueOnly => (scheduler.separate_pool.max_connections.min(2), 0),
    };
    let statement_timeout_ms = scheduler.separate_pool.statement_timeout_secs * 1000;
    let mut builder = PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(Duration::from_secs(
            scheduler.separate_pool.acquire_timeout_secs,
        ))
        .idle_timeout(Duration::from_secs(
            scheduler.separate_pool.idle_timeout_secs,
        ));
    if !pgbouncer_tx_pooling {
        let search_path_sql = scheduler_search_path_sql(&hstore_schema);
        builder = builder.after_connect(move |connection, _metadata| {
            let search_path_sql = search_path_sql.clone();
            Box::pin(async move {
                let statement_timeout =
                    format!("SET statement_timeout = '{statement_timeout_ms}ms'");
                sqlx::query(sqlx::AssertSqlSafe(statement_timeout))
                    .execute(&mut *connection)
                    .await?;
                let idle_in_tx_timeout =
                    format!("SET idle_in_transaction_session_timeout = '{statement_timeout_ms}ms'");
                sqlx::query(sqlx::AssertSqlSafe(idle_in_tx_timeout))
                    .execute(&mut *connection)
                    .await?;
                sqlx::query(sqlx::AssertSqlSafe(search_path_sql))
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        });
    }
    let pool = builder
        .connect_with(connect_options)
        .await
        .map_err(|error| connection_error_with_host(url, error))?;
    if pgbouncer_tx_pooling {
        ensure_pooled_hstore_visible(&pool, &direct_connect_options).await?;
    }
    let listener_pool = PgPoolOptions::new()
        .max_connections(2)
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(
            scheduler.separate_pool.acquire_timeout_secs,
        ))
        .idle_timeout(Duration::from_secs(
            scheduler.separate_pool.idle_timeout_secs,
        ))
        .connect_lazy_with(direct_connect_options);
    Ok(OpenedScheduler {
        backend: SchedulerBackend::Postgres(PostgresSchedulerBackend::new_with_listener_pool(
            pool,
            listener_pool,
        )),
    })
}

#[cfg(feature = "postgres")]
fn postgres_connect_options(
    url: &str,
    sslmode: Option<&str>,
) -> Result<sqlx::postgres::PgConnectOptions, SchedulerFactoryError> {
    use sqlx::ConnectOptions;
    use sqlx::postgres::{PgConnectOptions, PgSslMode};

    let explicit_ssl_mode = sslmode.map(parse_ssl_mode).transpose()?;
    let parsed_url =
        url::Url::parse(url).map_err(|error| SchedulerFactoryError::ConnectionFailed {
            message: host_only(url) + ": " + &error.to_string(),
        })?;
    let options = PgConnectOptions::from_url(&parsed_url).map_err(|error| {
        SchedulerFactoryError::ConnectionFailed {
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
    Ok(options.ssl_mode(PgSslMode::Prefer))
}

#[cfg(feature = "postgres")]
fn parse_ssl_mode(value: &str) -> Result<sqlx::postgres::PgSslMode, SchedulerFactoryError> {
    use sqlx::postgres::PgSslMode;

    match value.to_ascii_lowercase().as_str() {
        "disable" => Ok(PgSslMode::Disable),
        "allow" => Ok(PgSslMode::Allow),
        "prefer" => Ok(PgSslMode::Prefer),
        "require" => Ok(PgSslMode::Require),
        "verify-ca" | "verify_ca" => Ok(PgSslMode::VerifyCa),
        "verify-full" | "verify_full" => Ok(PgSslMode::VerifyFull),
        other => Err(SchedulerFactoryError::ConnectionFailed {
            message: format!(
                "unknown postgres sslmode '{other}'; expected one of: disable, allow, prefer, \
                 require, verify-ca, verify-full"
            ),
        }),
    }
}

#[cfg(feature = "postgres")]
fn quote_pg_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(feature = "postgres")]
fn scheduler_search_path_sql(hstore_schema: &str) -> String {
    if matches!(hstore_schema, "cc_lb_scheduler" | "apalis" | "public") {
        "SET search_path = cc_lb_scheduler, apalis, public".to_owned()
    } else {
        format!(
            "SET search_path = cc_lb_scheduler, apalis, {}, public",
            quote_pg_ident(hstore_schema)
        )
    }
}

#[cfg(feature = "postgres")]
fn random_advisory_lock_key() -> i64 {
    let raw = uuid::Uuid::new_v4().as_u128();
    let folded = ((raw >> 64) ^ raw) as i64;
    if folded == 0 { 1 } else { folded }
}

#[cfg(feature = "postgres")]
async fn verify_same_postgres_database(
    url: &str,
    primary_options: &sqlx::postgres::PgConnectOptions,
    listener_url: &str,
    direct_options: &sqlx::postgres::PgConnectOptions,
) -> Result<(), SchedulerFactoryError> {
    use std::time::Duration;

    use sqlx::Connection as _;
    use tokio::time::timeout;

    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    let mut primary_conn = timeout(
        PROBE_TIMEOUT,
        sqlx::PgConnection::connect_with(primary_options),
    )
    .await
    .map_err(|_| SchedulerFactoryError::ConnectionFailed {
        message: format!("{}: scheduler probe timed out", host_only(url)),
    })?
    .map_err(|error| connection_error_with_host(url, error))?;
    let mut direct_conn = timeout(
        PROBE_TIMEOUT,
        sqlx::PgConnection::connect_with(direct_options),
    )
    .await
    .map_err(|_| SchedulerFactoryError::ConnectionFailed {
        message: format!(
            "{}: direct listener probe timed out",
            host_only(listener_url)
        ),
    })?
    .map_err(|error| connection_error_with_host(listener_url, error))?;

    let primary_db: String = timeout(
        PROBE_TIMEOUT,
        sqlx::query_scalar("SELECT current_database()").fetch_one(&mut primary_conn),
    )
    .await
    .map_err(|_| SchedulerFactoryError::ConnectionFailed {
        message: format!("{}: scheduler database check timed out", host_only(url)),
    })?
    .map_err(|error| connection_error_with_host(url, error))?;
    let direct_db: String = timeout(
        PROBE_TIMEOUT,
        sqlx::query_scalar("SELECT current_database()").fetch_one(&mut direct_conn),
    )
    .await
    .map_err(|_| SchedulerFactoryError::ConnectionFailed {
        message: format!(
            "{}: direct listener database check timed out",
            host_only(listener_url)
        ),
    })?
    .map_err(|error| connection_error_with_host(listener_url, error))?;
    if primary_db != direct_db {
        let _ = primary_conn.close().await;
        let _ = direct_conn.close().await;
        return Err(SchedulerFactoryError::ConnectionFailed {
            message: format!(
                "storage.url ({primary_db}) and storage.pool.listener_url ({direct_db}) resolve to different PostgreSQL databases"
            ),
        });
    }

    let mut probe_key = None;
    for _ in 0..3 {
        let candidate = random_advisory_lock_key();
        let locked: bool = timeout(
            PROBE_TIMEOUT,
            sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(candidate)
                .fetch_one(&mut direct_conn),
        )
        .await
        .map_err(|_| SchedulerFactoryError::ConnectionFailed {
            message: format!(
                "{}: direct listener advisory-lock handshake timed out",
                host_only(listener_url)
            ),
        })?
        .map_err(|error| connection_error_with_host(listener_url, error))?;
        if locked {
            probe_key = Some(candidate);
            break;
        }
    }
    let Some(probe_key) = probe_key else {
        let _ = primary_conn.close().await;
        let _ = direct_conn.close().await;
        return Err(SchedulerFactoryError::ConnectionFailed {
            message: format!(
                "{}: failed to acquire advisory-lock probe on direct listener connection",
                host_only(listener_url)
            ),
        });
    };

    let acquired_while_held = timeout(
        PROBE_TIMEOUT,
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
            .bind(probe_key)
            .fetch_one(&mut primary_conn),
    )
    .await;
    let _ = timeout(
        PROBE_TIMEOUT,
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(probe_key)
            .execute(&mut direct_conn),
    )
    .await;
    let _ = direct_conn.close().await;

    let acquired_while_held = acquired_while_held
        .map_err(|_| SchedulerFactoryError::ConnectionFailed {
            message: format!("{}: advisory-lock handshake timed out", host_only(url)),
        })?
        .map_err(|error| connection_error_with_host(url, error))?;
    if acquired_while_held {
        let _ = primary_conn.close().await;
        return Err(SchedulerFactoryError::ConnectionFailed {
            message: format!(
                "storage.url ({}) and storage.pool.listener_url ({}) do not reach the same PostgreSQL database instance",
                host_only(url),
                host_only(listener_url)
            ),
        });
    }

    let acquired_after_release = timeout(
        PROBE_TIMEOUT,
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
            .bind(probe_key)
            .fetch_one(&mut primary_conn),
    )
    .await
    .map_err(|_| SchedulerFactoryError::ConnectionFailed {
        message: format!(
            "{}: advisory-lock release verification timed out",
            host_only(url)
        ),
    })?
    .map_err(|error| connection_error_with_host(url, error))?;
    let _ = primary_conn.close().await;
    if !acquired_after_release {
        return Err(SchedulerFactoryError::ConnectionFailed {
            message: format!(
                "storage.url ({}) could not verify advisory-lock release from storage.pool.listener_url ({})",
                host_only(url),
                host_only(listener_url)
            ),
        });
    }
    Ok(())
}

#[cfg(feature = "postgres")]
async fn find_hstore_schema(
    conn: &mut sqlx::PgConnection,
) -> Result<Option<String>, SchedulerFactoryError> {
    sqlx::query_scalar(
        "SELECT n.nspname::text \
         FROM pg_extension e \
         JOIN pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname = 'hstore'",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(migration_error)
}

#[cfg(feature = "postgres")]
async fn install_hstore_extension(
    conn: &mut sqlx::PgConnection,
) -> Result<String, SchedulerFactoryError> {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS hstore")
        .execute(&mut *conn)
        .await
        .map_err(migration_error)?;
    find_hstore_schema(conn)
        .await?
        .ok_or_else(|| SchedulerFactoryError::MigrationFailed {
            message: "hstore extension was not found after CREATE EXTENSION IF NOT EXISTS hstore"
                .to_owned(),
        })
}

#[cfg(feature = "postgres")]
async fn ensure_pooled_hstore_visible(
    pool: &sqlx::PgPool,
    direct_connect_options: &sqlx::postgres::PgConnectOptions,
) -> Result<(), SchedulerFactoryError> {
    let hstore_visible: bool = sqlx::query_scalar("SELECT to_regtype('hstore') IS NOT NULL")
        .fetch_one(pool)
        .await
        .map_err(migration_error)?;
    if hstore_visible {
        return Ok(());
    }

    let pooled_schema: Option<String> = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(pool)
        .await
        .map_err(migration_error)?;
    if let Some(target_schema) = pooled_schema
        .as_deref()
        .filter(|schema| *schema != "cc_lb_scheduler")
    {
        let target_schema = target_schema.to_owned();
        let direct_options = direct_connect_options.clone();
        tokio::spawn(async move {
            use sqlx::Connection as _;

            let mut conn = sqlx::PgConnection::connect_with(&direct_options)
                .await
                .map_err(migration_error)?;
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(SCHEDULER_SETUP_LOCK_KEY)
                .execute(&mut conn)
                .await
                .map_err(migration_error)?;
            let outcome: Result<(), SchedulerFactoryError> = async {
                let ext_schema: Option<String> = sqlx::query_scalar(
                    "SELECT n.nspname::text \
                     FROM pg_extension e \
                     JOIN pg_namespace n ON n.oid = e.extnamespace \
                     WHERE e.extname = 'hstore'",
                )
                .fetch_optional(&mut conn)
                .await
                .map_err(migration_error)?;
                if ext_schema.as_deref() == Some("cc_lb_scheduler") {
                    let sql = format!(
                        "ALTER EXTENSION hstore SET SCHEMA {}",
                        quote_pg_ident(&target_schema)
                    );
                    sqlx::query(sqlx::AssertSqlSafe(sql))
                        .execute(&mut conn)
                        .await
                        .map_err(migration_error)?;
                }
                Ok(())
            }
            .await;
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(SCHEDULER_SETUP_LOCK_KEY)
                .execute(&mut conn)
                .await;
            outcome
        })
        .await
        .map_err(migration_error)??;
    }

    let hstore_visible_after: bool = sqlx::query_scalar("SELECT to_regtype('hstore') IS NOT NULL")
        .fetch_one(pool)
        .await
        .map_err(migration_error)?;
    if hstore_visible_after {
        return Ok(());
    }
    Err(SchedulerFactoryError::MigrationFailed {
        message: "PostgreSQL `hstore` type is not visible on transaction-pooled scheduler connections; relocate it with `ALTER EXTENSION hstore SET SCHEMA public;` (or append `cc_lb_scheduler` after the application schema on the role `search_path`)".to_owned(),
    })
}

#[cfg(feature = "postgres")]
async fn run_postgres_scheduler_setup(
    direct_listener_url: &str,
    connect_options: &sqlx::postgres::PgConnectOptions,
    pgbouncer_transaction_pooling: bool,
) -> Result<String, SchedulerFactoryError> {
    let direct_listener_url = direct_listener_url.to_owned();
    let connect_options = connect_options.clone();
    tokio::spawn(async move {
        use sqlx::Connection as _;

        let mut conn = sqlx::PgConnection::connect_with(&connect_options)
            .await
            .map_err(|error| connection_error_with_host(&direct_listener_url, error))?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(SCHEDULER_SETUP_LOCK_KEY)
            .execute(&mut conn)
            .await
            .map_err(migration_error)?;
        let setup_result: Result<String, SchedulerFactoryError> = async {
            let hstore_schema = if pgbouncer_transaction_pooling {
                // Install BEFORE switching search_path so PostgreSQL places hstore in the
                // pooled role/database default schema (typically public), keeping it
                // resolvable on transaction-pooled connections without SET search_path.
                let schema = match find_hstore_schema(&mut conn).await? {
                    Some(schema) => schema,
                    None => install_hstore_extension(&mut conn).await?,
                };
                sqlx::query("CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler")
                    .execute(&mut conn)
                    .await
                    .map_err(migration_error)?;
                schema
            } else {
                // Preserve cc_lb_scheduler placement for fresh unpooled setups, while
                // honoring any already-installed extension schema and exposing it via
                // query-pool after_connect search_path.
                sqlx::query("CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler")
                    .execute(&mut conn)
                    .await
                    .map_err(migration_error)?;
                match find_hstore_schema(&mut conn).await? {
                    Some(schema) => schema,
                    None => {
                        sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
                            .execute(&mut conn)
                            .await
                            .map_err(migration_error)?;
                        install_hstore_extension(&mut conn).await?
                    }
                }
            };
            let search_path = scheduler_search_path_sql(&hstore_schema);
            sqlx::query(sqlx::AssertSqlSafe(search_path))
                .execute(&mut conn)
                .await
                .map_err(migration_error)?;
            reconcile_pre_1_0_apalis_migration_history(&mut conn).await?;
            let migrator = apalis_postgres::PostgresStorage::<()>::migrations();
            migrator
                .run_direct(None, &mut conn, false)
                .await
                .map_err(migration_error)?;
            crate::migrations::apply_postgres_post_setup_migrations_on_conn(&mut conn)
                .await
                .map_err(migration_error)?;
            Ok(hstore_schema)
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
