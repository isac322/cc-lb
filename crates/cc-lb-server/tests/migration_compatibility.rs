#![cfg(feature = "postgres")]

use std::{env, fs, future::Future, path::PathBuf, process::Command, sync::Arc};

use cc_lb_config::{Config, PostgresPoolConfig, StorageConfig};
use cc_lb_server::{app::open_storage, scheduler_factory::SchedulerBackend};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use url::Url;

const FIXTURE_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/migration_compatibility"
);
const MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/migration_compatibility/manifest.json"
));
const TEST_AEAD_KEY_ENV: &str = "CC_LB_MIGRATION_COMPAT_TEST_KEY";
const TEST_AEAD_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Deserialize)]
struct FixtureManifest {
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    name: String,
    dump: String,
    sha256: String,
}

#[tokio::test]
async fn historical_postgres_release_fixture_starts_through_shared_path() {
    let manifest: FixtureManifest =
        serde_json::from_str(MANIFEST).expect("migration compatibility manifest is valid JSON");
    assert!(
        !manifest.cases.is_empty(),
        "migration compatibility manifest must contain at least one case"
    );

    let admin_url = postgres_url();
    for case in manifest.cases {
        let case_name = case.name;
        let fixture_path = PathBuf::from(FIXTURE_DIR).join(&case.dump);
        let fixture = fs::read(&fixture_path).unwrap_or_else(|error| {
            panic!(
                "{}: read fixture {}: {error}",
                case_name,
                fixture_path.display()
            )
        });
        let digest = hex::encode(Sha256::digest(&fixture));
        assert_eq!(
            digest, case.sha256,
            "{}: fixture SHA-256 mismatch",
            case_name
        );
        let database_case_name = case_name.clone();

        with_temporary_database(&admin_url, &database_case_name, move |database_url| {
            let case_name = case_name.clone();
            let fixture_path = fixture_path.clone();
            async move {
                restore_fixture(&database_url, &fixture_path, &case_name);
                let _key = ScopedEnv::set(TEST_AEAD_KEY_ENV, TEST_AEAD_KEY);
                let config = postgres_config(database_url.clone());

                let opened = open_storage(&config, Arc::new(cc_lb_clock::SystemClock))
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "{}: shared startup path must upgrade the historical database: {error}",
                            case_name
                        )
                    });
                let (_managed, _main, _aead, _refresh_guard, scheduler) = opened;
                let SchedulerBackend::Postgres(postgres) = &scheduler.backend else {
                    panic!("{}: expected PostgreSQL scheduler backend", case_name);
                };
                assert_upgraded_database(postgres.pool(), &case_name).await;
                drop(scheduler);

                // A second open is the production restart/idempotency path, not a fresh seed.
                let reopened = open_storage(&config, Arc::new(cc_lb_clock::SystemClock))
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "{}: restart after historical upgrade failed: {error}",
                            case_name
                        )
                    });
                let (_managed, _main, _aead, _refresh_guard, scheduler) = reopened;
                let SchedulerBackend::Postgres(postgres) = &scheduler.backend else {
                    panic!(
                        "{}: expected PostgreSQL scheduler backend after restart",
                        case_name
                    );
                };
                assert_upgraded_database(postgres.pool(), &case_name).await;
            }
        })
        .await;
    }
}

fn postgres_url() -> String {
    env::var("CI_POSTGRES_URL")
        .or_else(|_| env::var("DATABASE_URL_TEST"))
        .or_else(|_| env::var("DATABASE_URL"))
        .expect("CI_POSTGRES_URL, DATABASE_URL_TEST, or DATABASE_URL must be set")
}

fn postgres_config(database_url: String) -> Config {
    let mut config = Config {
        storage: StorageConfig::Postgres {
            url: database_url,
            pool: PostgresPoolConfig::default(),
        },
        ..Config::default()
    };
    config.aead.key_env = TEST_AEAD_KEY_ENV.to_owned();
    config.scheduler.separate_pool.min_connections = 0;
    config
}

async fn with_temporary_database<F, Fut>(admin_url: &str, case_name: &str, test_case: F)
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
        .unwrap_or_else(|error| {
            panic!("{case_name}: connect to PostgreSQL admin database: {error}")
        });
    let database_name = format!("cc_lb_migration_compat_{}", uuid::Uuid::new_v4().simple());
    let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
    sqlx::query(sqlx::AssertSqlSafe(create_database))
        .execute(&admin_pool)
        .await
        .unwrap_or_else(|error| panic!("{case_name}: create isolated database: {error}"));

    let database_url = database_url_for(admin_url, &database_name)
        .unwrap_or_else(|| panic!("{case_name}: PostgreSQL URL has no usable database path"));
    let result = std::panic::AssertUnwindSafe(test_case(database_url));
    let result = futures_util::FutureExt::catch_unwind(result).await;

    let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)"#);
    let cleanup = sqlx::query(sqlx::AssertSqlSafe(drop_database))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;
    cleanup.unwrap_or_else(|error| panic!("{case_name}: drop isolated database: {error}"));
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

fn database_url_for(admin_url: &str, database_name: &str) -> Option<String> {
    let mut url = Url::parse(admin_url).ok()?;
    url.set_path(&format!("/{database_name}"));
    Some(url.to_string())
}

fn restore_fixture(database_url: &str, fixture_path: &PathBuf, case_name: &str) {
    let status = Command::new("psql")
        .arg("--no-psqlrc")
        .arg("--set=ON_ERROR_STOP=1")
        .arg(database_url)
        .arg("--file")
        .arg(fixture_path)
        .status()
        .unwrap_or_else(|error| panic!("{case_name}: launch psql to restore fixture: {error}"));
    assert!(
        status.success(),
        "{case_name}: psql fixture restore failed with {status}"
    );
}

async fn assert_upgraded_database(pool: &PgPool, case_name: &str) {
    let expected_main_migrations = fs::read_dir(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../cc-lb-storage-postgres/migrations"
    ))
    .expect("read PostgreSQL migration directory")
    .filter_map(Result::ok)
    .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sql"))
    .count() as i64;
    let main_migrations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM public._sqlx_migrations WHERE success")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| panic!("{case_name}: query main migration state: {error}"));
    assert_eq!(
        main_migrations, expected_main_migrations,
        "{case_name}: all main storage migrations must be applied"
    );

    let backend_kind: String =
        sqlx::query_scalar("SELECT value FROM public.meta WHERE key = 'backend_kind'")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| {
                panic!("{case_name}: query main storage health marker: {error}")
            });
    assert_eq!(
        backend_kind, "postgres",
        "{case_name}: main storage health marker"
    );

    let legacy_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('cc_lb_scheduler._sqlx_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| panic!("{case_name}: query legacy scheduler ledger: {error}"));
    assert!(
        !legacy_exists,
        "{case_name}: legacy scheduler ledger must be moved"
    );

    let expected_scheduler_migrations = apalis_postgres::PostgresStorage::<()>::migrations()
        .iter()
        .count() as i64;
    let scheduler_migrations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations WHERE success")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| {
                panic!("{case_name}: query scheduler migration state: {error}")
            });
    assert_eq!(
        scheduler_migrations, expected_scheduler_migrations,
        "{case_name}: all current Apalis migrations must be applied"
    );

    let metadata: Option<String> = sqlx::query_scalar(
        "SELECT metadata -> 'traceparent' FROM apalis.jobs WHERE id = 'fixture-job'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("{case_name}: query upgraded Apalis job metadata: {error}"));
    assert_eq!(
        metadata.as_deref(),
        Some("00-fixture"),
        "{case_name}: historical job metadata must survive the ledger upgrade"
    );
    let idempotency_key: Option<String> =
        sqlx::query_scalar("SELECT idempotency_key FROM apalis.jobs WHERE id = 'fixture-job'")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| panic!("{case_name}: query upgraded Apalis job: {error}"));
    assert_eq!(
        idempotency_key.as_deref(),
        Some("adaptive:fixture:1"),
        "{case_name}: historical job data must survive the ledger upgrade"
    );
}

struct ScopedEnv {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnv {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = env::var_os(key);
        // The test uses a unique variable name and restores it before yielding.
        unsafe { env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            unsafe { env::set_var(self.key, previous) };
        } else {
            unsafe { env::remove_var(self.key) };
        }
    }
}
