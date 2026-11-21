#![cfg(feature = "postgres")]

use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_storage_api::MetaStore;
use cc_lb_storage_conformance::scenarios::managed_keys::{
    ManagedKeyBackend, managed_keys_concurrent_issue_no_index_collision,
    managed_keys_empty_label_roundtrip, managed_keys_equivalent_records, managed_keys_happy_path,
    managed_keys_nul_byte_rejected,
};
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage, adapter::retry};
use sqlx::PgPool;
use tokio::runtime::Runtime;

struct PostgresManagedKeyBackend {
    url: String,
}

struct PostgresFixture {
    isolated: crate::pg_template_helper::IsolatedPg,
    pool: PgPool,
}

#[async_trait]
impl ManagedKeyBackend for PostgresManagedKeyBackend {
    type Store = PostgresManagedKeyStore;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        let isolated = crate::pg_template_helper::IsolatedPg::create_templated(&self.url).await?;
        let pool = match isolated.pool(8).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error);
            }
        };
        PostgresStorage::new(pool.clone(), system_clock())
            .initialize()
            .await?;

        Ok(PostgresFixture { isolated, pool })
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
        fixture.isolated.drop().await
    }
}

#[test]
fn managed_keys_happy_path_postgres() {
    run_postgres_scenario("managed_keys_happy_path", managed_keys_happy_path);
}

#[test]
fn managed_keys_empty_label_roundtrip_postgres() {
    run_postgres_scenario(
        "managed_keys_empty_label_roundtrip",
        managed_keys_empty_label_roundtrip,
    );
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

fn system_clock() -> ClockHandle {
    Arc::new(SystemClock)
}
