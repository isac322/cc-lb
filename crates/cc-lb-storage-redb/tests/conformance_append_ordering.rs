use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;
use cc_lb_storage_conformance::{ConformanceBackend, scenarios::append_ordering};
use cc_lb_storage_redb::RedbStorage;
use tempfile::TempDir;

struct LocalRedbBackend;

struct LocalRedbFixture {
    _dir: TempDir,
    path: PathBuf,
}

#[async_trait]
impl ConformanceBackend for LocalRedbBackend {
    type Storage = RedbStorage;
    type Fixture = LocalRedbFixture;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("storage.redb");

        Ok(LocalRedbFixture { _dir: dir, path })
    }

    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage> {
        Ok(RedbStorage::open(&fixture.path)?)
    }

    async fn teardown(&self, fixture: Self::Fixture) -> Result<()> {
        drop(fixture);
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Redb
    }
}

#[tokio::test]
async fn append_ordering_conformance() -> Result<()> {
    append_ordering::run_all(Arc::new(LocalRedbBackend)).await
}
