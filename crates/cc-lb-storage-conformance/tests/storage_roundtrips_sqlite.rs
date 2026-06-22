#![cfg(feature = "sqlite")]

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use cc_lb_core::{ClockHandle, TestClock};
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_conformance::{
    harness::ConformanceBackend,
    scenarios::{
        anthropic_compatibility_kv_store, atomicity, organization_metadata_store,
        plugin_registry_store, principal_store, prompt_cache_observation_store, storage_roundtrips,
        storage_roundtrips_cache_split, storage_roundtrips_latency_stages,
        upstream_rate_limit_store, upstream_subscription_metadata_store,
        upstream_subscription_quota_store,
    },
};
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
        let storage = open_sqlite(&fixture.database_url).await?;
        storage.initialize(BackendKind::Sqlite).await?;
        Ok(storage)
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }
}

#[test]
fn storage_roundtrips_sqlite() {
    run_sqlite_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn request_event_cache_split_round_trip_sqlite() {
    run_sqlite_scenario(
        "request_event_cache_split_round_trip",
        storage_roundtrips_cache_split::request_event_cache_split_round_trip,
    );
}

#[test]
fn request_event_latency_stage_round_trip_sqlite() {
    run_sqlite_scenario(
        "request_event_latency_stage_round_trip",
        storage_roundtrips_latency_stages::request_event_latency_stage_round_trip,
    );
}

#[test]
fn usage_rollup_v2_preserves_upstream_id_across_renames_sqlite() {
    run_sqlite_scenario(
        "usage_rollup_v2_preserves_upstream_id_across_renames",
        |backend| async move {
            let mut fixture = cc_lb_storage_conformance::ConformanceFixture::new(backend).await?;
            let result =
                atomicity::usage_rollup_v2_preserves_upstream_id_across_renames(&fixture).await;
            let teardown = fixture.teardown().await;
            result?;
            teardown
        },
    );
}

#[test]
fn principal_allowed_upstreams_roundtrip_sqlite() {
    run_sqlite_scenario(
        "principal_allowed_upstreams_roundtrip",
        principal_store::principal_allowed_upstreams_roundtrip,
    );
}

#[test]
fn upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip",
        upstream_rate_limit_store::put_then_list_for_upstream_ids_roundtrip,
    );
}

#[test]
fn upstream_rate_limit_latest_write_wins_within_same_key_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key",
        upstream_rate_limit_store::latest_write_wins_within_same_key,
    );
}

#[test]
fn upstream_rate_limit_latest_write_wins_within_same_key_forward_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key_forward",
        upstream_rate_limit_store::latest_write_wins_within_same_key_forward,
    );
}

#[test]
fn upstream_rate_limit_empty_list_for_unknown_id_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_empty_list_for_unknown_id",
        upstream_rate_limit_store::empty_list_for_unknown_id,
    );
}

macro_rules! anthropic_compatibility_kv_sqlite_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_sqlite_scenario(
                stringify!($scenario),
                anthropic_compatibility_kv_store::$scenario,
            );
        }
    };
}

anthropic_compatibility_kv_sqlite_test!(
    anthropic_compatibility_kv_put_then_get_roundtrip_sqlite,
    put_then_get_roundtrip
);
anthropic_compatibility_kv_sqlite_test!(
    anthropic_compatibility_kv_update_replaces_value_and_clears_error_sqlite,
    update_replaces_value_and_clears_error
);
anthropic_compatibility_kv_sqlite_test!(
    anthropic_compatibility_kv_failure_preserves_last_good_value_sqlite,
    failure_preserves_last_good_value
);
anthropic_compatibility_kv_sqlite_test!(
    anthropic_compatibility_kv_failure_on_never_seen_key_is_noop_sqlite,
    failure_on_never_seen_key_is_noop
);
anthropic_compatibility_kv_sqlite_test!(
    anthropic_compatibility_kv_list_returns_keys_in_some_order_sqlite,
    list_returns_keys_in_some_order
);
anthropic_compatibility_kv_sqlite_test!(
    anthropic_compatibility_kv_older_observation_does_not_replace_newer_sqlite,
    older_observation_does_not_replace_newer
);

macro_rules! upstream_subscription_quota_sqlite_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_sqlite_scenario(
                stringify!($scenario),
                upstream_subscription_quota_store::$scenario,
            );
        }
    };
}

upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_append_then_list_latest_roundtrip_sqlite,
    append_then_list_latest_roundtrip
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_same_millis_appends_with_different_sample_ids_dont_collide_sqlite,
    same_millis_appends_with_different_sample_ids_dont_collide
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_header_and_api_sources_coexist_in_latest_sqlite,
    header_and_api_sources_coexist_in_latest
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_latest_is_monotonic_in_millis_sqlite,
    latest_is_monotonic_in_millis
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_series_returns_buckets_with_correct_bounds_sqlite,
    series_returns_buckets_with_correct_bounds
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_series_source_merge_merged_collapses_both_sources_sqlite,
    series_source_merge_merged_collapses_both_sources
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_series_source_merge_header_filters_api_sqlite,
    series_source_merge_header_filters_api
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_series_max_points_per_series_downsamples_sqlite,
    series_max_points_per_series_downsamples
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_delete_before_removes_old_observations_sqlite,
    delete_before_removes_old_observations
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_delete_before_does_not_touch_latest_table_sqlite,
    delete_before_does_not_touch_latest_table
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_process_start_marker_persists_with_sample_kind_sqlite,
    process_start_marker_persists_with_sample_kind
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_empty_upstream_ids_returns_empty_sqlite,
    empty_upstream_ids_returns_empty
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_series_filters_observed_at_window_sqlite,
    series_filters_observed_at_window
);

macro_rules! prompt_cache_observation_sqlite_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_sqlite_scenario(stringify!($scenario), |backend| async move {
                prompt_cache_observation_store::$scenario(backend, prompt_cache_clock()).await
            });
        }
    };
}

prompt_cache_observation_sqlite_test!(
    prompt_cache_observation_upsert_then_list_returns_active_only_sqlite,
    upsert_then_list_returns_active_only
);
prompt_cache_observation_sqlite_test!(
    prompt_cache_observation_asymmetric_ttl_snapshot_visibility_sqlite,
    asymmetric_ttl_snapshot_visibility
);
prompt_cache_observation_sqlite_test!(
    prompt_cache_observation_purge_expired_before_removes_only_expired_sqlite,
    purge_expired_before_removes_only_expired
);
prompt_cache_observation_sqlite_test!(
    prompt_cache_observation_hydrate_after_restart_filters_expired_sqlite,
    hydrate_after_restart_filters_expired
);
prompt_cache_observation_sqlite_test!(
    prompt_cache_observation_observation_list_is_sorted_sqlite,
    observation_list_is_sorted
);

#[test]
fn upstream_subscription_metadata_store_sqlite() {
    run_sqlite_scenario(
        "upstream_subscription_metadata_store",
        upstream_subscription_metadata_store::run_all,
    );
}

#[test]
fn organization_metadata_store_sqlite() {
    run_sqlite_scenario(
        "organization_metadata_store",
        organization_metadata_store::run_all,
    );
}

macro_rules! plugin_registry_sqlite_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_sqlite_scenario(stringify!($scenario), plugin_registry_store::$scenario);
        }
    };
}

plugin_registry_sqlite_test!(
    plugin_registry_registry_by_id_returns_seeded_builtin_cache_affinity_sqlite,
    registry_by_id_returns_seeded_builtin_cache_affinity
);
plugin_registry_sqlite_test!(
    plugin_registry_insert_chain_entry_with_builtin_cache_affinity_succeeds_sqlite,
    insert_chain_entry_with_builtin_cache_affinity_succeeds
);
plugin_registry_sqlite_test!(
    plugin_registry_list_orphan_blobs_returns_blobs_without_registry_sqlite,
    list_orphan_blobs_returns_blobs_without_registry
);
plugin_registry_sqlite_test!(
    plugin_registry_chain_delete_keeps_blob_for_reinsert_sqlite,
    chain_delete_keeps_blob_for_reinsert
);
plugin_registry_sqlite_test!(
    plugin_registry_delete_registry_rejects_while_chain_refed_sqlite,
    delete_registry_rejects_while_chain_refed
);
plugin_registry_sqlite_test!(
    plugin_registry_delete_registry_removes_blob_atomically_sqlite,
    delete_registry_removes_blob_atomically
);
plugin_registry_sqlite_test!(
    plugin_registry_decrement_blob_refcount_or_delete_skips_registry_backed_sqlite,
    decrement_blob_refcount_or_delete_skips_registry_backed
);
plugin_registry_sqlite_test!(
    plugin_registry_insert_chain_entry_rejects_unknown_principal_sqlite,
    insert_chain_entry_rejects_unknown_principal
);
plugin_registry_sqlite_test!(
    plugin_registry_router_multi_entry_ordered_sqlite,
    router_multi_entry_ordered
);
plugin_registry_sqlite_test!(
    plugin_registry_insert_chain_entry_rejects_duplicate_for_shape_slot_sqlite,
    insert_chain_entry_rejects_duplicate_for_shape_slot
);
plugin_registry_sqlite_test!(
    plugin_registry_router_reorder_preserves_invariants_sqlite,
    router_reorder_preserves_invariants
);
plugin_registry_sqlite_test!(
    plugin_registry_shape_singleton_preserved_sqlite,
    shape_singleton_preserved
);
plugin_registry_sqlite_test!(
    plugin_registry_insert_chain_entry_allows_multi_for_observability_hook_sqlite,
    insert_chain_entry_allows_multi_for_observability_hook
);
plugin_registry_sqlite_test!(
    plugin_registry_reorder_chain_rejects_final_chain_gap_sqlite,
    reorder_chain_rejects_final_chain_gap
);
plugin_registry_sqlite_test!(
    plugin_registry_update_chain_entry_rejects_no_op_sqlite,
    update_chain_entry_rejects_no_op
);
plugin_registry_sqlite_test!(
    plugin_registry_upload_returns_existed_flag_sqlite,
    upload_returns_existed_flag
);
plugin_registry_sqlite_test!(
    plugin_registry_same_sha_metadata_mismatch_conflicts_sqlite,
    same_sha_metadata_mismatch_conflicts
);

fn run_sqlite_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<SqliteConformanceBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(SqliteConformanceBackend)))
        .unwrap_or_else(|error| panic!("{name} sqlite: {error}"));
}

fn prompt_cache_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(1_700_000_000))
}
