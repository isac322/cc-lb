#![cfg(feature = "redb")]

use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_storage_conformance::scenarios::managed_keys::{
    ManagedKeyBackend, managed_keys_concurrent_issue_no_index_collision,
    managed_keys_equivalent_records, managed_keys_happy_path, managed_keys_nul_byte_rejected,
};
use cc_lb_storage_redb::{RedbManagedKeyStore, RedbStorage};
use tokio::runtime::Runtime;

struct RedbManagedKeyBackend;

struct RedbFixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

#[async_trait]
impl ManagedKeyBackend for RedbManagedKeyBackend {
    type Store = RedbManagedKeyStore;
    type Fixture = RedbFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("managed_keys_conformance.redb");
        Ok(RedbFixture { _dir: dir, path })
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Store> {
        let storage = RedbStorage::open(&fixture.path, [41; 32])?;
        Ok(RedbManagedKeyStore::new(Arc::new(storage)))
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }
}

#[test]
fn managed_keys_happy_path_redb() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(managed_keys_happy_path(Arc::new(RedbManagedKeyBackend)))
        .expect("managed_keys_happy_path redb");
}

#[test]
fn managed_keys_nul_byte_rejected_redb() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(managed_keys_nul_byte_rejected(Arc::new(
            RedbManagedKeyBackend,
        )))
        .expect("managed_keys_nul_byte_rejected redb");
}

#[test]
fn managed_keys_concurrent_issue_no_index_collision_redb() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(managed_keys_concurrent_issue_no_index_collision(Arc::new(
            RedbManagedKeyBackend,
        )))
        .expect("managed_keys_concurrent_issue_no_index_collision redb");
}

#[test]
fn managed_keys_equivalent_records_redb() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(managed_keys_equivalent_records(Arc::new(
            RedbManagedKeyBackend,
        )))
        .expect("managed_keys_equivalent_records redb");
}
