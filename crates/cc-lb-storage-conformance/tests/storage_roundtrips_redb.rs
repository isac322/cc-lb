#![cfg(feature = "redb")]

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;
use cc_lb_storage_conformance::{
    harness::ConformanceBackend,
    scenarios::{principal_store, storage_roundtrips, upstream_rate_limit_store},
};
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
    run_redb_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn principal_allowed_upstreams_roundtrip_redb() {
    run_redb_scenario(
        "principal_allowed_upstreams_roundtrip",
        principal_store::principal_allowed_upstreams_roundtrip,
    );
}

#[test]
fn upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip_redb() {
    run_redb_scenario(
        "upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip",
        upstream_rate_limit_store::put_then_list_for_upstream_ids_roundtrip,
    );
}

#[test]
fn upstream_rate_limit_latest_write_wins_within_same_key_redb() {
    run_redb_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key",
        upstream_rate_limit_store::latest_write_wins_within_same_key,
    );
}

#[test]
fn upstream_rate_limit_latest_write_wins_within_same_key_forward_redb() {
    run_redb_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key_forward",
        upstream_rate_limit_store::latest_write_wins_within_same_key_forward,
    );
}

#[test]
fn upstream_rate_limit_empty_list_for_unknown_id_redb() {
    run_redb_scenario(
        "upstream_rate_limit_empty_list_for_unknown_id",
        upstream_rate_limit_store::empty_list_for_unknown_id,
    );
}

fn run_redb_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<RedbConformanceBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(RedbConformanceBackend)))
        .unwrap_or_else(|error| panic!("{name} redb: {error}"));
}
