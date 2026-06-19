use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::{
    Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, StorageConfig,
};
use cc_lb_server::app::{BuildError, build_app_with_storage};
use cc_lb_server::scheduler_factory::SchedulerFactoryError;
use cc_lb_storage_api::{BackendKind, ManagedKeyStore, MetaStore, Storage as StorageTrait};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_unreachable_scheduler_connection_failure_aborts_app_build() -> TestResult {
    let directory = tempfile::tempdir()?;
    let main_path = directory.path().join("main-storage.sqlite");
    let main_storage = open_main_sqlite(&main_path).await?;
    let mut config = app_config(StorageConfig::Postgres {
        url: "postgres://invalid:invalid@127.0.0.1:1/nope".to_owned(),
        pool: cc_lb_config::PostgresPoolConfig::default(),
    });
    config.runtime.data_dir = Some(directory.path().join("data"));

    let error = build_failing_app(config, main_storage.clone()).await;

    assert_scheduler_error(&error, "connection");
    assert_no_main_apalis_tables(main_storage.pool()).await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_idempotency_tables_migration_failure_aborts_app_build() -> TestResult {
    let directory = tempfile::tempdir()?;
    let main_path = directory.path().join("main-storage.sqlite");
    let scheduler_path = scheduler_sqlite_path(&main_path);
    let main_storage = open_main_sqlite(&main_path).await?;
    precreate_scheduler_failures_without_columns(&scheduler_path).await?;
    let mut config = app_config(StorageConfig::Sqlite { path: main_path });
    config.runtime.data_dir = Some(directory.path().join("data"));

    let error = build_failing_app(config, main_storage.clone()).await;

    assert_scheduler_error(&error, "migration");
    assert_migration_error_mentions_idempotency_table(&error);
    assert_no_main_apalis_tables(main_storage.pool()).await?;
    assert_scheduler_workers_not_spawned(&scheduler_path).await?;
    Ok(())
}

fn app_config(storage: StorageConfig) -> Config {
    let mut config = Config {
        storage,
        ..Config::default()
    };
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: "task40-hard-fail".to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
    });
    config.scheduler.separate_pool.min_connections = 0;
    config.scheduler.separate_pool.acquire_timeout_secs = 1;
    config
}

async fn open_main_sqlite(path: &Path) -> TestResult<Arc<cc_lb_storage_sqlite::SqliteStorage>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = Arc::new(cc_lb_storage_sqlite::open_sqlite(&database_url).await?);
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

async fn build_failing_app(
    config: Config,
    main_storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
) -> BuildError {
    let managed_store: Arc<dyn ManagedKeyStore> = main_storage.clone();
    let storage: Arc<dyn StorageTrait> = main_storage;
    match build_app_with_storage(
        config,
        None,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key([0; 32])),
    )
    .await
    {
        Ok(_) => panic!("scheduler initialization failure must abort app startup"),
        Err(error) => error,
    }
}

fn assert_scheduler_error(error: &BuildError, expected: &str) {
    match (error, expected) {
        (
            BuildError::SchedulerFactory(SchedulerFactoryError::ConnectionFailed { .. }),
            "connection",
        )
        | (
            BuildError::SchedulerFactory(SchedulerFactoryError::MigrationFailed { .. }),
            "migration",
        ) => {}
        other => panic!("expected scheduler {expected} error, got {other:?}"),
    }

    let rendered = error.to_string().to_ascii_lowercase();
    let debug = format!("{error:?}").to_ascii_lowercase();
    assert!(
        ["scheduler", "apalis", "migration", expected]
            .iter()
            .any(|token| rendered.contains(token) || debug.contains(token)),
        "scheduler init token missing from error: {error:?}"
    );
}

fn assert_migration_error_mentions_idempotency_table(error: &BuildError) {
    let rendered = error.to_string().to_ascii_lowercase();
    assert!(
        rendered.contains("scheduler_failures")
            || rendered.contains("last_failed_at_unix_secs")
            || rendered.contains("job_type"),
        "sqlite migration failure should come from the idempotency-tables migration: {error:?}"
    );
}

async fn assert_no_main_apalis_tables(pool: &sqlx::SqlitePool) -> TestResult {
    for table_name in ["Jobs", "Workers"] {
        let table_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table_name)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            table_count, 0,
            "main storage must not receive apalis table {table_name} after scheduler init failure"
        );
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
fn scheduler_sqlite_path(path: &Path) -> std::path::PathBuf {
    path.with_extension("scheduler.sqlite")
}

#[cfg(feature = "sqlite")]
async fn precreate_scheduler_failures_without_columns(path: &Path) -> TestResult {
    use std::str::FromStr as _;

    use scheduler_sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let database_url = format!("sqlite://{}", path.display());
    let options = SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    scheduler_sqlx::query("CREATE TABLE scheduler_failures (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn assert_scheduler_workers_not_spawned(path: &Path) -> TestResult {
    use std::str::FromStr as _;

    use scheduler_sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let database_url = format!("sqlite://{}", path.display());
    let options = SqliteConnectOptions::from_str(&database_url)?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let workers_table_count: i64 = scheduler_sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'Workers'",
    )
    .fetch_one(&pool)
    .await?;
    if workers_table_count == 0 {
        pool.close().await;
        return Ok(());
    }

    let worker_rows: i64 = scheduler_sqlx::query_scalar("SELECT COUNT(*) FROM Workers")
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    assert_eq!(
        worker_rows, 0,
        "scheduler workers must not register rows after startup hard-fail"
    );
    Ok(())
}
