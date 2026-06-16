#![cfg(feature = "sqlite")]

use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;
use cc_lb_storage_conformance::harness::{ConformanceBackend, with_conformance_fixture};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use tokio::runtime::Runtime;

struct SqliteConformanceBackend;

struct SqliteFixture {
    _dir: tempfile::TempDir,
    database_url: String,
}

#[async_trait]
impl ConformanceBackend for SqliteConformanceBackend {
    type Storage = SqliteStorage;
    type Fixture = SqliteFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("storage_roundtrips_conformance.sqlite");
        let database_url = format!("sqlite://{}", path.display());
        Ok(SqliteFixture {
            _dir: dir,
            database_url,
        })
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Storage> {
        Ok(open_sqlite(&fixture.database_url).await?)
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }
}

#[test]
fn storage_roundtrips_sqlite_smoke() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(with_conformance_fixture(
            Arc::new(SqliteConformanceBackend),
            |_storage| async move { Ok(()) },
        ))
        .expect("storage_roundtrips sqlite smoke");
}
