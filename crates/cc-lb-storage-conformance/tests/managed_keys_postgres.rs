#![cfg(feature = "postgres")]

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use cc_lb_storage_conformance::{
    PostgresFixture,
    scenarios::managed_keys::{
        ManagedKeyBackend, managed_keys_concurrent_issue_no_index_collision,
        managed_keys_equivalent_records, managed_keys_happy_path, managed_keys_nul_byte_rejected,
    },
};
use cc_lb_storage_postgres::{PostgresManagedKeyStore, adapter::retry};
use tokio::runtime::Runtime;

struct PostgresManagedKeyBackend;

#[async_trait]
impl ManagedKeyBackend for PostgresManagedKeyBackend {
    type Store = PostgresManagedKeyStore;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        cc_lb_storage_conformance::postgres_fixture().await
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Store> {
        Ok(PostgresManagedKeyStore::new(
            fixture.pool().clone(),
            Arc::new(retry::RetryPolicy::default()),
            cc_lb_testkit::fixed_clock(1_700_000_000),
        ))
    }

    async fn teardown(&self, fixture: Self::Fixture) -> anyhow::Result<()> {
        fixture.teardown().await
    }
}

#[test]
fn t3_postgres__managed_keys_happy_path_postgres() {
    run_postgres_scenario("managed_keys_happy_path", managed_keys_happy_path);
}

#[test]
fn t3_postgres__managed_keys_nul_byte_rejected_postgres() {
    run_postgres_scenario(
        "managed_keys_nul_byte_rejected",
        managed_keys_nul_byte_rejected,
    );
}

#[test]
fn t3_postgres__managed_keys_concurrent_issue_no_index_collision_postgres() {
    run_postgres_scenario(
        "managed_keys_concurrent_issue_no_index_collision",
        managed_keys_concurrent_issue_no_index_collision,
    );
}

#[test]
fn t3_postgres__managed_keys_equivalent_records_postgres() {
    run_postgres_scenario(
        "managed_keys_equivalent_records",
        managed_keys_equivalent_records,
    );
}

fn run_postgres_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<PostgresManagedKeyBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(PostgresManagedKeyBackend)))
        .unwrap_or_else(|error| panic!("{name} postgres: {error}"));
}
