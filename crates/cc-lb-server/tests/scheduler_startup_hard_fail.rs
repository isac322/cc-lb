use std::env;
use std::ffi::OsString;

use cc_lb_config::{
    Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, StorageConfig,
};
use cc_lb_server::app::{BuildError, build_app_with_path};
use cc_lb_server::scheduler_factory::SchedulerFactoryError;

const TEST_KEY_ENV: &str = "CC_LB_TASK25_AEAD_KEY";
const TEST_KEY_HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn sqlite_scheduler_init_failure_aborts_app_build() {
    let _env = EnvGuard::set(TEST_KEY_ENV, TEST_KEY_HEX);
    let directory = tempfile::tempdir().expect("tempdir is created");
    let storage_path = directory.path().join("cc-lb.sqlite");
    let scheduler_path = storage_path.with_extension("scheduler.sqlite");
    std::fs::create_dir(&scheduler_path).expect("directory blocks scheduler sqlite file creation");

    let mut config = app_config(StorageConfig::Sqlite { path: storage_path });
    config.runtime.data_dir = Some(directory.path().join("data"));

    let error = match build_app_with_path(config, None).await {
        Ok(_) => panic!("scheduler sqlite initialization must abort app startup"),
        Err(error) => error,
    };
    assert_scheduler_factory_error(error);
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_scheduler_init_failure_aborts_app_build() {
    use std::sync::Arc;

    use cc_lb_aead::AeadService;
    use cc_lb_server::app::build_app_with_storage;
    use cc_lb_storage_api::{BackendKind, ManagedKeyStore, MetaStore, Storage as StorageTrait};

    let directory = tempfile::tempdir().expect("tempdir is created");
    let sqlite_url = format!(
        "sqlite://{}",
        directory.path().join("main-storage.sqlite").display()
    );
    let sqlite = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&sqlite_url)
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
    config.aead.key_env = TEST_KEY_ENV.to_owned();
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
            | SchedulerFactoryError::LeaderConnectionFailed { .. }
            | SchedulerFactoryError::StartupFailed { .. },
        ) => {}
        other => panic!("expected scheduler init failure, got {other:?}"),
    }
}

struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = env::var_os(key);
        unsafe { env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            unsafe { env::set_var(self.key, previous) };
        } else {
            unsafe { env::remove_var(self.key) };
        }
    }
}
