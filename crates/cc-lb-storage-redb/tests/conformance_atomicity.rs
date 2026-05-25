use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;
use cc_lb_storage_conformance::{ConformanceBackend, ConformanceFixture, scenarios::atomicity};
use cc_lb_storage_redb::RedbStorage;
use tempfile::TempDir;

struct RedbBackend;

struct RedbFixture {
    _dir: TempDir,
    path: PathBuf,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_atomicity_run_all() -> Result<()> {
    atomicity::run_all(Arc::new(RedbBackend)).await
}

#[async_trait]
impl ConformanceBackend for RedbBackend {
    type Fixture = RedbFixture;
    type Storage = RedbStorage;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("storage.redb");

        Ok(RedbFixture { _dir: dir, path })
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_atomic_rmw() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::quota_atomic_rmw(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_try_incr_capacity() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::quota_try_incr_capacity(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_adjust_signed() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::quota_adjust_signed(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_sweep_range() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::quota_sweep_range(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_state_upsert_idempotent() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::limit_state_upsert_idempotent(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_state_list_by_principal() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::limit_state_list_by_principal(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usage_rollup_idempotent() -> Result<()> {
    let mut fixture = new_fixture().await?;

    let scenario_result = atomicity::usage_rollup_idempotent(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

async fn new_fixture() -> Result<ConformanceFixture<RedbBackend>> {
    ConformanceFixture::new(Arc::new(RedbBackend)).await
}
