#![cfg(all(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use cc_lb_storage_conformance::scenarios::managed_keys::managed_keys_cross_backend_equivalence;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, adapter::retry};
use cc_lb_storage_sqlite::open_sqlite;
use tokio::runtime::Runtime;

#[test]
fn t3_postgres__managed_keys_cross_backend_equivalence_sqlite_postgres() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(run_cross_backend_equivalence())
        .expect("managed_keys_cross_backend_equivalence sqlite/postgres");
}

async fn run_cross_backend_equivalence() -> anyhow::Result<()> {
    let sqlite_dir = tempfile::tempdir()?;
    let sqlite_path = sqlite_dir.path().join("managed_keys_cross_backend.sqlite");
    let sqlite_url = format!("sqlite://{}", sqlite_path.display());
    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
    let sqlite_store = open_sqlite(&sqlite_url, Arc::clone(&clock)).await?;
    cc_lb_storage_api::MetaStore::initialize(&sqlite_store, cc_lb_storage_api::BackendKind::Sqlite)
        .await?;

    let fixture = cc_lb_storage_conformance::postgres_fixture().await?;
    let postgres_store = PostgresManagedKeyStore::new(
        fixture.pool().clone(),
        Arc::new(retry::RetryPolicy::default()),
        clock,
    );

    let result = managed_keys_cross_backend_equivalence(&sqlite_store, &postgres_store).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}
