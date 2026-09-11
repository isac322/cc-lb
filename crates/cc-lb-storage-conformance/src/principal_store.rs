#![allow(non_snake_case)]

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_engine::ClockHandle;
use cc_lb_storage_api::{BackendKind, MetaStore};
#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};

use crate::harness::ConformanceBackend;
use crate::scenarios::principal_store::{run_all, stale_revision_conflict};

#[tokio::test]
async fn t3__principal_store_sqlite() -> Result<()> {
    let clock: ClockHandle = cc_lb_testkit::fixed_clock(1_700_000_000);
    run_all(Arc::new(SqlitePrincipalBackend { clock })).await
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn t3_postgres__principal_store_postgres() -> Result<()> {
    run_all(Arc::new(PostgresPrincipalBackend)).await
}

#[tokio::test]
async fn t3__principal_store_stale_revision_conflict_sqlite() -> Result<()> {
    let clock: ClockHandle = cc_lb_testkit::fixed_clock(1_700_000_000);
    stale_revision_conflict(Arc::new(SqlitePrincipalBackend { clock })).await
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn t3_postgres__principal_store_stale_revision_conflict_postgres() -> Result<()> {
    stale_revision_conflict(Arc::new(PostgresPrincipalBackend)).await
}

struct SqlitePrincipalBackend {
    clock: ClockHandle,
}

struct SqliteFixture {
    _dir: tempfile::TempDir,
    database_url: String,
}

#[async_trait]
impl ConformanceBackend for SqlitePrincipalBackend {
    type Storage = SqliteStorage;
    type Fixture = SqliteFixture;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("principal_store.sqlite");
        let database_url = format!("sqlite://{}", path.display());
        Ok(SqliteFixture {
            _dir: dir,
            database_url,
        })
    }

    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage> {
        let storage = open_sqlite(&fixture.database_url, self.clock.clone()).await?;
        storage.initialize(BackendKind::Sqlite).await?;
        Ok(storage)
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }
}

#[cfg(feature = "postgres")]
struct PostgresPrincipalBackend;

#[async_trait]
#[cfg(feature = "postgres")]
impl ConformanceBackend for PostgresPrincipalBackend {
    type Storage = PostgresStorage;
    type Fixture = crate::PostgresFixture;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
        crate::postgres_fixture().await
    }

    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage> {
        Ok(fixture.storage().clone())
    }

    async fn teardown(&self, fixture: Self::Fixture) -> Result<()> {
        fixture.teardown().await
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }
}
