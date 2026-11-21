#![cfg(all(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_storage_api::MetaStore;
use cc_lb_storage_conformance::scenarios::managed_keys::managed_keys_cross_backend_equivalence;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage, adapter::retry};
use cc_lb_storage_sqlite::open_sqlite;
use sqlx::PgPool;
use tokio::runtime::Runtime;

struct PostgresFixture {
    isolated: crate::pg_template_helper::IsolatedPg,
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
    sqlite_store.initialize().await?;

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
    let isolated = crate::pg_template_helper::IsolatedPg::create_templated(&url).await?;
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

async fn teardown_postgres_fixture(fixture: PostgresFixture) -> anyhow::Result<()> {
    fixture.pool.close().await;
    fixture.isolated.drop().await
}

fn system_clock() -> ClockHandle {
    Arc::new(SystemClock)
}
