#![cfg(feature = "sqlite")]

use crate::request_event_quota_support;

#[path = "support/request_event_quota_sqlite.rs"]
mod request_event_quota_sqlite;

request_event_quota_sqlite::define_request_event_quota_sqlite_tests!();

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_conformance::{
    harness::ConformanceBackend,
    scenarios::{
        anthropic_compatibility_kv_store, api_key_usage_bucket_store, atomicity, audit_sink,
        cache_keepalive_projection_store, cache_keepalive_session_reads,
        cache_keepalive_session_store, organization_metadata_store, plan_tier_store,
        plan_tier_store_backfill, plugin_blob_repo, plugin_registry_store,
        pool_quota_history_store, price_catalog, principal_store, prompt_cache_observation_store,
        request_event_key_usage, request_event_list, request_event_principal_costs,
        request_events_prune, runtime_change_notifier, storage_roundtrips,
        storage_roundtrips_cache_split, storage_roundtrips_latency_stages,
        upstream_rate_limit_store, upstream_store, upstream_subscription_metadata_store,
        upstream_subscription_quota_store, usage_rollups, warmup_attempts_store,
    },
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use cc_lb_testkit::TestClock;
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
        let storage = open_sqlite(
            &fixture.database_url,
            cc_lb_testkit::fixed_clock(1_700_000_000),
        )
        .await?;
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

#[async_trait]
impl upstream_store::UpstreamStoreBackend for SqliteConformanceBackend {
    type Store = SqliteStorage;
    type Fixture = SqliteFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        <Self as ConformanceBackend>::create_fixture(self).await
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Store> {
        <Self as ConformanceBackend>::open(self, fixture).await
    }

    async fn teardown(&self, fixture: Self::Fixture) -> anyhow::Result<()> {
        <Self as ConformanceBackend>::teardown(self, fixture).await
    }
}

#[async_trait]
impl price_catalog::PriceCatalogCorruptionBackend for SqliteConformanceBackend {
    async fn corrupt_latest_price_catalog_hash(
        &self,
        fixture: &Self::Fixture,
    ) -> anyhow::Result<()> {
        let pool = sqlx::SqlitePool::connect(&fixture.database_url).await?;
        let result = sqlx::query(
            "UPDATE price_catalog_snapshots_v1 SET payload_hash = ? \
             WHERE id = (SELECT id FROM price_catalog_snapshots_v1 \
             ORDER BY fetched_at_ms DESC, id DESC LIMIT 1)",
        )
        .bind("corrupted-payload-hash")
        .execute(&pool)
        .await?;
        pool.close().await;
        anyhow::ensure!(result.rows_affected() == 1, "expected one corrupted row");
        Ok(())
    }
}

#[async_trait]
impl cache_keepalive_projection_store::ProjectionAtomicityBackend for SqliteConformanceBackend {
    async fn install_turn_insert_failure(&self, fixture: &Self::Fixture) -> anyhow::Result<()> {
        let pool = sqlx::SqlitePool::connect(&fixture.database_url).await?;
        sqlx::query(
            "CREATE TRIGGER conformance_fail_projection_turn_insert \
             BEFORE INSERT ON cache_keepalive_turns \
             BEGIN \
                 SELECT RAISE(ABORT, 'forced cache keepalive turn insert failure'); \
             END",
        )
        .execute(&pool)
        .await?;
        pool.close().await;
        Ok(())
    }

    async fn projection_row_counts(
        &self,
        fixture: &Self::Fixture,
    ) -> anyhow::Result<(i64, i64, i64)> {
        let pool = sqlx::SqlitePool::connect(&fixture.database_url).await?;
        let counts = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT \
                 (SELECT COUNT(*) FROM request_events_v1), \
                 (SELECT COUNT(*) FROM cache_keepalive_turns), \
                 (SELECT COUNT(*) FROM cache_keepalive_decisions)",
        )
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        Ok(counts)
    }
}

#[test]
fn t3__storage_roundtrips_sqlite() {
    run_sqlite_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn t3__runtime_change_notifier_delivery_sqlite() {
    run_sqlite_scenario(
        "runtime_change_notifier_delivery",
        runtime_change_notifier::subscriber_receives_change,
    );
}

#[test]
fn tx__notify_latency_budget_sqlite() {
    run_sqlite_scenario(
        "notify_latency_budget",
        runtime_change_notifier::subscriber_receives_within_latency_budget,
    );
}

#[test]
fn t3__principal_store_full_conformance_sqlite() {
    run_sqlite_scenario("principal_store", principal_store::run_all);
}

#[test]
fn t3__upstream_store_sqlite_conformance() {
    run_sqlite_scenario("upstream_store", upstream_store::run_all);
}

#[test]
fn t3__cache_keepalive_session_store_conformance_sqlite() {
    run_sqlite_scenario(
        "cache_keepalive_session_store",
        cache_keepalive_session_store::run_all,
    );
}

#[test]
fn t3__audit_sink_batch_roundtrip_and_prune_boundary_sqlite() {
    run_sqlite_scenario(
        "audit_sink_batch_roundtrip_and_prune_boundary",
        audit_sink::batch_roundtrip_and_prune_boundary,
    );
}

#[test]
fn t3__cache_keepalive_projection_decision_append_roundtrip_sqlite() {
    run_sqlite_scenario(
        "cache_keepalive_projection_decision_append_roundtrip",
        cache_keepalive_projection_store::decision_append_roundtrip,
    );
}

#[test]
fn t3__cache_keepalive_projection_append_is_atomic_sqlite() {
    run_sqlite_scenario(
        "cache_keepalive_projection_append_is_atomic",
        cache_keepalive_projection_store::append_with_projections_is_atomic,
    );
}

#[test]
fn t3__plugin_blob_repo_roundtrip_delete_and_missing_sqlite() {
    run_sqlite_scenario(
        "plugin_blob_repo_roundtrip_delete_and_missing",
        plugin_blob_repo::roundtrip_delete_and_missing,
    );
}

#[test]
fn t3__request_events_prune_strict_millisecond_boundary_and_batch_limit_sqlite() {
    run_sqlite_scenario(
        "request_events_prune_strict_millisecond_boundary_and_batch_limit",
        request_events_prune::strict_millisecond_boundary_and_batch_limit,
    );
}

#[test]
fn t3__pool_quota_history_fable_roundtrip_sqlite() {
    run_sqlite_scenario(
        "pool_quota_history_fable_roundtrip",
        pool_quota_history_store::fable_roundtrip,
    );
}

#[test]
fn t3__pool_quota_summary_latest_and_range_sqlite() {
    run_sqlite_scenario(
        "pool_quota_summary_latest_and_range",
        pool_quota_history_store::summary_latest_and_range,
    );
}

#[test]
fn t3__request_event_cache_split_round_trip_sqlite() {
    run_sqlite_scenario(
        "request_event_cache_split_round_trip",
        storage_roundtrips_cache_split::request_event_cache_split_round_trip,
    );
}

#[test]
fn t3__request_event_latency_stage_round_trip_sqlite() {
    run_sqlite_scenario(
        "request_event_latency_stage_round_trip",
        storage_roundtrips_latency_stages::request_event_latency_stage_round_trip,
    );
}

#[test]
fn t3__request_event_list_projects_rows_and_preserves_detail_sqlite() {
    run_sqlite_scenario(
        "request_event_list_projects_rows_and_preserves_detail",
        request_event_list::request_event_list_projects_rows_and_preserves_detail,
    );
}

#[test]
fn t3__request_event_list_model_filter_matches_case_insensitive_prefix_sqlite() {
    run_sqlite_scenario(
        "request_event_list_model_filter_matches_case_insensitive_prefix",
        request_event_list::request_event_list_model_filter_matches_case_insensitive_prefix,
    );
}

#[test]
fn t3__request_event_principal_cost_components_sqlite() {
    run_sqlite_scenario(
        "request_event_principal_cost_components",
        request_event_principal_costs::principal_cost_components_aggregate_without_fabrication,
    );
}

#[test]
fn t3__request_event_key_usage_materialized_columns_sqlite() {
    run_sqlite_scenario(
        "request_event_key_usage_materialized_columns",
        request_event_key_usage::materialized_key_usage_preserves_bucket_contract,
    );
}

#[test]
fn t3__request_event_key_last_used_preserves_inclusive_range_and_filters_sqlite() {
    run_sqlite_scenario(
        "request_event_key_last_used_preserves_inclusive_range_and_filters",
        request_event_key_usage::last_used_preserves_inclusive_range_and_filters,
    );
}

#[test]
fn t3__cache_keepalive_batch_turn_reads_match_per_session_sqlite() {
    run_sqlite_scenario(
        "cache_keepalive_batch_turn_reads_match_per_session",
        cache_keepalive_session_reads::batch_turn_reads_match_canonical_per_session_reads,
    );
}

#[test]
fn t3__cache_keepalive_session_read_list_detail_filters_sqlite() {
    run_sqlite_scenario(
        "cache_keepalive_session_read_list_detail_filters",
        cache_keepalive_session_reads::list_detail_filters_preserve_frozen_projection_contract,
    );
}

#[test]
fn t3__cache_keepalive_session_read_pagination_cursor_sqlite() {
    run_sqlite_scenario(
        "cache_keepalive_session_read_pagination_cursor",
        cache_keepalive_session_reads::pagination_horizon_cursor_and_frozen_order_contract,
    );
}

#[test]
fn t3__usage_rollup_v2_preserves_upstream_id_across_renames_sqlite() {
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
fn t3__usage_rollup_filtered_analysis_query_sqlite() {
    run_sqlite_scenario(
        "usage_rollup_filtered_analysis_query",
        usage_rollups::run_all,
    );
}

#[test]
fn t3__usage_rollup_checkpoint_absent_before_first_rollup_sqlite() {
    run_sqlite_scenario(
        "usage_rollup_checkpoint_absent_before_first_rollup",
        usage_rollups::checkpoint_is_absent_before_first_rollup,
    );
}

#[test]
fn t3__usage_rollup_empty_queries_preserve_range_boundaries_sqlite() {
    run_sqlite_scenario(
        "usage_rollup_empty_queries_preserve_range_boundaries",
        usage_rollups::empty_queries_preserve_range_boundaries,
    );
}

#[test]
fn t3__overview_excluded_error_buckets_preserve_boundaries_sqlite() {
    run_sqlite_scenario(
        "overview_excluded_error_buckets_preserve_boundaries",
        usage_rollups::overview_excluded_error_buckets_preserve_boundaries,
    );
}

#[test]
fn t3__principal_allowed_upstreams_roundtrip_sqlite() {
    run_sqlite_scenario(
        "principal_allowed_upstreams_roundtrip",
        principal_store::principal_allowed_upstreams_roundtrip,
    );
}

#[test]
fn t3__upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip",
        upstream_rate_limit_store::put_then_list_for_upstream_ids_roundtrip,
    );
}

#[test]
fn t3__upstream_rate_limit_roundtrip_and_list_boundary_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_roundtrip_and_list_boundary",
        upstream_rate_limit_store::roundtrip_and_list_boundary,
    );
}

#[test]
fn t3__upstream_rate_limit_latest_write_wins_within_same_key_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key",
        upstream_rate_limit_store::latest_write_wins_within_same_key,
    );
}

#[test]
fn t3__upstream_rate_limit_latest_write_wins_within_same_key_forward_sqlite() {
    run_sqlite_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key_forward",
        upstream_rate_limit_store::latest_write_wins_within_same_key_forward,
    );
}

#[test]
fn t3__upstream_rate_limit_empty_list_for_unknown_id_sqlite() {
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
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_checkpoint_writer_latest_freshness_sqlite,
    checkpoint_writer_latest_freshness
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_checkpoint_writer_decrease_sqlite,
    checkpoint_writer_decrease
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_checkpoint_series_anchor_merge_sqlite,
    checkpoint_series_anchor_merge
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_checkpoint_history_sqlite,
    checkpoint_history
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_aggregate_store_sqlite,
    aggregate_store
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_absent_replaces_latest_without_erasing_history_sqlite,
    absent_replaces_latest_without_erasing_history
);
upstream_subscription_quota_sqlite_test!(
    upstream_subscription_quota_absent_is_idempotent_sqlite,
    absent_is_idempotent
);

#[test]
fn t3__warmup_attempts_store_sqlite() {
    run_sqlite_scenario("warmup_attempts_store", |backend| async move {
        warmup_attempts_store::run_all(backend, warmup_attempts_clock()).await
    });
}

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
prompt_cache_observation_sqlite_test!(
    prompt_cache_observation_cross_model_same_prefix_keeps_both_rows_sqlite,
    cross_model_same_prefix_keeps_both_rows
);

#[test]
fn t3__upstream_subscription_metadata_store_sqlite() {
    run_sqlite_scenario(
        "upstream_subscription_metadata_store",
        upstream_subscription_metadata_store::run_all,
    );
}

#[test]
fn t3__organization_metadata_store_sqlite() {
    run_sqlite_scenario(
        "organization_metadata_store",
        organization_metadata_store::run_all,
    );
}

#[test]
fn t3__plan_tier_store_sqlite() {
    run_sqlite_scenario("plan_tier_store", plan_tier_store::run_all);
}

#[test]
fn t3__plan_tier_store_backfill_sqlite() {
    run_sqlite_scenario(
        "plan_tier_store_backfill",
        plan_tier_store_backfill::upstream_tier_backfill_intervals,
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
    plugin_registry_registry_by_id_returns_seeded_builtin_subscription_preference_sqlite,
    registry_by_id_returns_seeded_builtin_subscription_preference
);
plugin_registry_sqlite_test!(
    plugin_registry_created_principal_has_builtin_subscription_preference_chain_entry_sqlite,
    created_principal_has_builtin_subscription_preference_chain_entry
);
plugin_registry_sqlite_test!(
    t3__plugin_registry_builtin_subscription_preference_is_visible_and_insertable_sqlite,
    builtin_subscription_preference_is_visible_and_insertable
);
plugin_registry_sqlite_test!(
    t3__plugin_registry_refcount_transitions_follow_live_chain_and_warmup_references_sqlite,
    refcount_transitions_follow_live_chain_and_warmup_references
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
plugin_registry_sqlite_test!(
    plugin_registry_refcount_counts_chain_and_warmup_references_sqlite,
    refcount_counts_chain_and_warmup_references
);
plugin_registry_sqlite_test!(
    plugin_registry_replace_wasm_entry_preserves_id_and_references_sqlite,
    replace_wasm_entry_preserves_id_and_references
);
plugin_registry_sqlite_test!(
    plugin_registry_replace_wasm_entry_with_stale_revision_conflicts_sqlite,
    replace_wasm_entry_with_stale_revision_conflicts
);
plugin_registry_sqlite_test!(
    plugin_registry_list_registry_references_returns_chain_and_warmup_sqlite,
    list_registry_references_returns_chain_and_warmup
);
plugin_registry_sqlite_test!(
    plugin_registry_cascade_delete_registry_entry_removes_chain_warmup_and_blob_sqlite,
    cascade_delete_registry_entry_removes_chain_warmup_and_blob
);
plugin_registry_sqlite_test!(
    plugin_registry_cascade_delete_registry_entry_rejects_changed_fingerprint_sqlite,
    cascade_delete_registry_entry_rejects_changed_fingerprint
);

#[test]
fn t3__price_catalog_roundtrip_smoke_sqlite() {
    run_sqlite_scenario(
        "price_catalog_roundtrip_smoke",
        price_catalog::roundtrip_smoke,
    );
}

#[test]
fn t3__price_catalog_put_same_payload_twice_updates_fetched_at_sqlite() {
    run_sqlite_scenario(
        "price_catalog_put_same_payload_twice_updates_fetched_at",
        price_catalog::put_same_payload_twice_updates_fetched_at,
    );
}

#[test]
fn t3__price_catalog_corrupted_payload_hash_is_rejected_sqlite() {
    run_sqlite_scenario(
        "price_catalog_corrupted_payload_hash_is_rejected",
        price_catalog::corrupted_payload_hash_is_rejected,
    );
}

#[test]
fn t3__api_key_usage_flush_idempotency_aggregation_and_range_boundary_sqlite() {
    run_sqlite_scenario(
        "api_key_usage_flush_idempotency_aggregation_and_range_boundary",
        api_key_usage_bucket_store::flush_idempotency_aggregation_and_range_boundary,
    );
}

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

fn prompt_cache_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new_at_secs(1_700_000_000))
}

fn warmup_attempts_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new_at_secs(1_800_604_800))
}
