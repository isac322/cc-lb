use std::sync::Arc;

use cc_lb_aead::AeadService;

use cc_lb_config::{
    Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, StorageConfig,
};
use cc_lb_server::app::{BuildError, build_app_with_storage};
use cc_lb_server::scheduler_factory::SchedulerFactoryError;
use cc_lb_storage_api::{BackendKind, ManagedKeyStore, MetaStore, Storage as StorageTrait};

#[tokio::test]
async fn t3__sqlite_scheduler_init_failure_aborts_app_build() {
    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
    let directory = tempfile::tempdir().expect("tempdir is created");
    let storage_path = directory.path().join("cc-lb.sqlite");
    let scheduler_path = storage_path.with_extension("scheduler.sqlite");
    std::fs::create_dir(&scheduler_path).expect("directory blocks scheduler sqlite file creation");
    let main_url = format!(
        "sqlite://{}",
        directory.path().join("main-storage.sqlite").display()
    );
    let main_storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&main_url, clock.clone())
            .await
            .expect("main sqlite storage opens"),
    );
    main_storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("main sqlite storage initializes");
    let managed_store: Arc<dyn ManagedKeyStore> = main_storage.clone();
    let storage: Arc<dyn StorageTrait> = main_storage;
    let mut config = app_config(StorageConfig::Sqlite { path: storage_path });
    config.runtime.data_dir = Some(directory.path().join("data"));

    let error = match build_app_with_storage(
        config,
        None,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key([0; 32])),
        clock,
    )
    .await
    {
        Ok(_) => panic!("scheduler sqlite initialization must abort app startup"),
        Err(error) => error,
    };
    assert_scheduler_factory_error(error);
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn t3_postgres__postgres_scheduler_init_failure_aborts_app_build() {
    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
    let directory = tempfile::tempdir().expect("tempdir is created");
    let sqlite_url = format!(
        "sqlite://{}",
        directory.path().join("main-storage.sqlite").display()
    );
    let sqlite = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&sqlite_url, clock.clone())
            .await
            .expect("main sqlite storage opens"),
    );
    sqlite
        .initialize(BackendKind::Sqlite)
        .await
        .expect("main sqlite storage initializes");
    let managed_store: Arc<dyn ManagedKeyStore> = sqlite.clone();
    let storage: Arc<dyn StorageTrait> = sqlite;
    let mut config = app_config(StorageConfig::Postgres {
        url: "postgres://invalid:invalid@127.0.0.1:1/none".to_owned(),
        pool: cc_lb_config::PostgresPoolConfig::default(),
    });
    config.scheduler.separate_pool.min_connections = 0;
    config.scheduler.separate_pool.acquire_timeout_secs = 1;
    config.runtime.data_dir = Some(directory.path().join("data"));

    let error = match build_app_with_storage(
        config,
        None,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key([0; 32])),
        clock.clone(),
    )
    .await
    {
        Ok(_) => panic!("scheduler postgres initialization must abort app startup"),
        Err(error) => error,
    };
    assert_scheduler_factory_error(error);
}

fn app_config(storage: StorageConfig) -> Config {
    let mut config = Config {
        storage,
        ..Config::default()
    };
    config.aead.key_env = crate::common::TEST_NONEMPTY_ENV.to_owned();
    // Postgres validates the cluster token before opening the scheduler pool.
    // Borrow an immutable harness variable instead of mutating process-wide env.
    config.cluster.token_env = crate::common::TEST_NONEMPTY_ENV.to_owned();
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: "task25-hard-fail".to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
    });
    config.scheduler.separate_pool.min_connections = 0;
    config.scheduler.separate_pool.acquire_timeout_secs = 1;
    config
}

fn assert_scheduler_factory_error(error: BuildError) {
    match error {
        BuildError::SchedulerFactory(
            SchedulerFactoryError::ConnectionFailed { .. }
            | SchedulerFactoryError::MigrationFailed { .. }
            | SchedulerFactoryError::StartupFailed { .. },
        ) => {}
        other => panic!("expected scheduler init failure, got {other:?}"),
    }
}
