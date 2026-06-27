#![cfg(feature = "postgres")]

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use cc_lb_core::{ClockHandle, SystemClock};
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_conformance::scenarios::managed_keys::{
    ManagedKeyBackend, managed_keys_concurrent_issue_no_index_collision,
    managed_keys_equivalent_records, managed_keys_happy_path, managed_keys_nul_byte_rejected,
};
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage, adapter::retry};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::runtime::Runtime;

struct PostgresManagedKeyBackend {
    url: String,
}

struct PostgresFixture {
    url: String,
    schema: String,
    pool: PgPool,
}

#[async_trait]
impl ManagedKeyBackend for PostgresManagedKeyBackend {
    type Store = PostgresManagedKeyStore;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        let schema = schema_name();
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&self.url)?)
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
            .connect_with(
                PgConnectOptions::from_str(&self.url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        PostgresStorage::new(pool.clone(), system_clock())
            .initialize(BackendKind::Postgres)
            .await?;

        Ok(PostgresFixture {
            url: self.url.clone(),
            schema,
            pool,
        })
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Store> {
        Ok(PostgresManagedKeyStore::new(
            fixture.pool.clone(),
            Arc::new(retry::RetryPolicy::default()),
            system_clock(),
        ))
    }

    async fn teardown(&self, fixture: Self::Fixture) -> anyhow::Result<()> {
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
}

#[test]
fn managed_keys_happy_path_postgres() {
    run_postgres_scenario("managed_keys_happy_path", managed_keys_happy_path);
}

#[test]
fn managed_keys_nul_byte_rejected_postgres() {
    run_postgres_scenario(
        "managed_keys_nul_byte_rejected",
        managed_keys_nul_byte_rejected,
    );
}

#[test]
fn managed_keys_concurrent_issue_no_index_collision_postgres() {
    run_postgres_scenario(
        "managed_keys_concurrent_issue_no_index_collision",
        managed_keys_concurrent_issue_no_index_collision,
    );
}

#[test]
fn managed_keys_equivalent_records_postgres() {
    run_postgres_scenario(
        "managed_keys_equivalent_records",
        managed_keys_equivalent_records,
    );
}

fn run_postgres_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<PostgresManagedKeyBackend>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(PostgresManagedKeyBackend { url })))
        .unwrap_or_else(|error| panic!("{name} postgres: {error}"));
}

fn schema_name() -> String {
    format!("managed_keys_{}", uuid::Uuid::new_v4().simple())
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
