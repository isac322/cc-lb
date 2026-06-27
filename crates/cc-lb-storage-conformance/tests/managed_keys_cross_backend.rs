#![cfg(all(feature = "sqlite", feature = "postgres"))]

use std::{str::FromStr, sync::Arc};

use cc_lb_core::{ClockHandle, SystemClock};
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_conformance::scenarios::managed_keys::managed_keys_cross_backend_equivalence;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage, adapter::retry};
use cc_lb_storage_sqlite::open_sqlite;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::runtime::Runtime;

struct PostgresFixture {
    url: String,
    schema: String,
    pool: PgPool,
}

#[test]
fn managed_keys_cross_backend_equivalence_sqlite_postgres() {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(run_cross_backend_equivalence(url))
        .expect("managed_keys_cross_backend_equivalence sqlite/postgres");
}

async fn run_cross_backend_equivalence(url: String) -> anyhow::Result<()> {
    let sqlite_dir = tempfile::tempdir()?;
    let sqlite_path = sqlite_dir.path().join("managed_keys_cross_backend.sqlite");
    let sqlite_url = format!("sqlite://{}", sqlite_path.display());
    let clock = system_clock();
    let sqlite_store = open_sqlite(&sqlite_url, Arc::clone(&clock)).await?;
    sqlite_store.initialize(BackendKind::Sqlite).await?;

    let fixture = create_postgres_fixture(url).await?;
    let postgres_store = PostgresManagedKeyStore::new(
        fixture.pool.clone(),
        Arc::new(retry::RetryPolicy::default()),
        clock,
    );

    let result = managed_keys_cross_backend_equivalence(&sqlite_store, &postgres_store).await;
    let teardown = teardown_postgres_fixture(fixture).await;
    result?;
    teardown
}

async fn create_postgres_fixture(url: String) -> anyhow::Result<PostgresFixture> {
    let schema = schema_name();
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&url)?)
        .await?;
    sqlx::query(AssertSqlSafe(format!(
        "CREATE SCHEMA {}",
        quote_ident(&schema)
    )))
    .execute(&admin_pool)
    .await?;
    admin_pool.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]))
        .await?;
    PostgresStorage::new(pool.clone(), system_clock())
        .initialize(BackendKind::Postgres)
        .await?;

    Ok(PostgresFixture { url, schema, pool })
}

async fn teardown_postgres_fixture(fixture: PostgresFixture) -> anyhow::Result<()> {
    fixture.pool.close().await;
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&fixture.url)?)
        .await?;
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_ident(&fixture.schema)
    )))
    .execute(&admin_pool)
    .await?;
    admin_pool.close().await;
    Ok(())
}

fn schema_name() -> String {
    format!(
        "managed_keys_cross_backend_{}",
        uuid::Uuid::new_v4().simple()
    )
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}

fn system_clock() -> ClockHandle {
    Arc::new(SystemClock)
}
