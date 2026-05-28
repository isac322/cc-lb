#![cfg(all(feature = "redb", feature = "postgres"))]

use std::{
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_conformance::scenarios::managed_keys::managed_keys_cross_backend_equivalence;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage, adapter::retry};
use cc_lb_storage_redb::{RedbManagedKeyStore, RedbStorage};
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
fn managed_keys_cross_backend_equivalence_redb_postgres() {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(run_cross_backend_equivalence(url))
        .expect("managed_keys_cross_backend_equivalence redb/postgres");
}

async fn run_cross_backend_equivalence(url: String) -> anyhow::Result<()> {
    let redb_dir = tempfile::tempdir()?;
    let redb_path = redb_dir.path().join("managed_keys_cross_backend.redb");
    let redb_storage = RedbStorage::open(&redb_path, [41; 32])?;
    let redb_store = RedbManagedKeyStore::new(Arc::new(redb_storage));

    let fixture = create_postgres_fixture(url).await?;
    let postgres_store = PostgresManagedKeyStore::new(
        fixture.pool.clone(),
        Arc::new(retry::RetryPolicy::default()),
    );

    let result = managed_keys_cross_backend_equivalence(&redb_store, &postgres_store).await;
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
    PostgresStorage::new(pool.clone())
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
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("managed_keys_cross_backend_{nanos}_{}", std::process::id())
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
