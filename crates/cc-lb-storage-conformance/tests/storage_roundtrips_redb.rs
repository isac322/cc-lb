#![cfg(feature = "redb")]

use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;
use cc_lb_storage_conformance::{harness::ConformanceBackend, scenarios::storage_roundtrips};
use cc_lb_storage_redb::RedbStorage;
use tokio::runtime::Runtime;

struct RedbConformanceBackend;

struct RedbFixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

#[async_trait]
impl ConformanceBackend for RedbConformanceBackend {
    type Storage = RedbStorage;
    type Fixture = RedbFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("storage_roundtrips_conformance.redb");
        Ok(RedbFixture { _dir: dir, path })
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Storage> {
        Ok(RedbStorage::open(&fixture.path, [41; 32])?)
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Redb
    }
}

#[test]
fn storage_roundtrips_redb() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(storage_roundtrips::run_all(Arc::new(
            RedbConformanceBackend,
        )))
        .expect("storage_roundtrips redb");
}
