#![cfg(feature = "redb")]

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;
use cc_lb_storage_conformance::{
    harness::ConformanceBackend,
    scenarios::{
        plugin_registry_store, principal_store, storage_roundtrips, upstream_rate_limit_store,
    },
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

macro_rules! plugin_registry_redb_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_redb_scenario(stringify!($scenario), plugin_registry_store::$scenario);
        }
    };
}

plugin_registry_redb_test!(
    plugin_registry_list_orphan_blobs_returns_blobs_without_registry_redb,
    list_orphan_blobs_returns_blobs_without_registry
);
plugin_registry_redb_test!(
    plugin_registry_chain_delete_keeps_blob_for_reinsert_redb,
    chain_delete_keeps_blob_for_reinsert
);
plugin_registry_redb_test!(
    plugin_registry_delete_registry_rejects_while_chain_refed_redb,
    delete_registry_rejects_while_chain_refed
);
plugin_registry_redb_test!(
    plugin_registry_delete_registry_removes_blob_atomically_redb,
    delete_registry_removes_blob_atomically
);
plugin_registry_redb_test!(
    plugin_registry_persist_wasm_upload_heals_missing_blob_redb,
    persist_wasm_upload_heals_missing_blob
);
plugin_registry_redb_test!(
    plugin_registry_decrement_blob_refcount_or_delete_skips_registry_backed_redb,
    decrement_blob_refcount_or_delete_skips_registry_backed
);
plugin_registry_redb_test!(
    plugin_registry_insert_chain_entry_rejects_unknown_principal_redb,
    insert_chain_entry_rejects_unknown_principal
);
plugin_registry_redb_test!(
    plugin_registry_reorder_chain_rejects_final_chain_gap_redb,
    reorder_chain_rejects_final_chain_gap
);
plugin_registry_redb_test!(
    plugin_registry_update_chain_entry_rejects_no_op_redb,
    update_chain_entry_rejects_no_op
);
plugin_registry_redb_test!(
    plugin_registry_upload_returns_existed_flag_redb,
    upload_returns_existed_flag
);
plugin_registry_redb_test!(
    plugin_registry_same_sha_metadata_mismatch_conflicts_redb,
    same_sha_metadata_mismatch_conflicts
);

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
