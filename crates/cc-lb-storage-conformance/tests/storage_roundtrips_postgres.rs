#![cfg(feature = "postgres")]

use crate::request_event_quota_support;

#[path = "support/request_event_quota_postgres.rs"]
mod request_event_quota_postgres;

request_event_quota_postgres::define_request_event_quota_postgres_tests!();

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use cc_lb_storage_api::{BackendKind, PluginRegistryStore, WasmBlob, WasmRegistryEntryInput};
use cc_lb_storage_conformance::{
    PostgresFixture,
    harness::ConformanceBackend,
    scenarios::{
        anthropic_compatibility_kv_store, api_key_usage_bucket_store, audit_sink,
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
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_testkit::TestClock;
use tokio::{runtime::Runtime, sync::Barrier};

struct PostgresConformanceBackend;

static PRICE_CATALOG_SCENARIO_LOCK: Mutex<()> = Mutex::new(());

#[async_trait]
impl ConformanceBackend for PostgresConformanceBackend {
    type Storage = PostgresStorage;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        cc_lb_storage_conformance::postgres_fixture().await
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Storage> {
        Ok(fixture.storage().clone())
    }

    async fn teardown(&self, fixture: Self::Fixture) -> anyhow::Result<()> {
        fixture.teardown().await
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }

    async fn wait_for_events_visible_for_rollup(
        &self,
        storage: &Self::Storage,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE request_events_v1 SET tx_id = '0'::xid8 WHERE tx_id IS NOT NULL")
            .execute(storage.pool())
            .await?;
        let eligible = sqlx::query_scalar::<_, bool>(
            "SELECT NOT EXISTS ( \
                 SELECT 1 FROM request_events_v1 \
                 WHERE COALESCE(tx_id, '0'::xid8) >= pg_snapshot_xmin(pg_current_snapshot()) \
             )",
        )
        .fetch_one(storage.pool())
        .await?;
        anyhow::ensure!(
            eligible,
            "seeded request events must be below the Postgres snapshot xmin horizon"
        );
        Ok(())
    }
}
#[async_trait]
impl upstream_store::UpstreamStoreBackend for PostgresConformanceBackend {
    type Store = PostgresStorage;
    type Fixture = PostgresFixture;

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
impl price_catalog::PriceCatalogCorruptionBackend for PostgresConformanceBackend {
    async fn corrupt_latest_price_catalog_hash(
        &self,
        fixture: &Self::Fixture,
    ) -> anyhow::Result<()> {
        let result = sqlx::query(
            "UPDATE price_catalog_snapshots_v1 SET payload_hash = $1 \
             WHERE id = (SELECT id FROM price_catalog_snapshots_v1 \
             ORDER BY fetched_at_ms DESC, id DESC LIMIT 1)",
        )
        .bind("corrupted-payload-hash")
        .execute(fixture.pool())
        .await?;
        anyhow::ensure!(result.rows_affected() == 1, "expected one corrupted row");
        Ok(())
    }
}

#[async_trait]
impl cache_keepalive_projection_store::ProjectionAtomicityBackend for PostgresConformanceBackend {
    async fn install_turn_insert_failure(&self, fixture: &Self::Fixture) -> anyhow::Result<()> {
        sqlx::query(
            "CREATE FUNCTION conformance_fail_projection_turn_insert() \
             RETURNS trigger \
             LANGUAGE plpgsql \
             AS $$ \
             BEGIN \
                 RAISE EXCEPTION 'forced cache keepalive turn insert failure'; \
             END; \
             $$",
        )
        .execute(fixture.pool())
        .await?;
        sqlx::query(
            "CREATE TRIGGER conformance_fail_projection_turn_insert \
             BEFORE INSERT ON cache_keepalive_turns \
             FOR EACH ROW \
             EXECUTE FUNCTION conformance_fail_projection_turn_insert()",
        )
        .execute(fixture.pool())
        .await?;
        Ok(())
    }

    async fn projection_row_counts(
        &self,
        fixture: &Self::Fixture,
    ) -> anyhow::Result<(i64, i64, i64)> {
        Ok(sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT \
                 (SELECT COUNT(*) FROM request_events_v1), \
                 (SELECT COUNT(*) FROM cache_keepalive_turns), \
                 (SELECT COUNT(*) FROM cache_keepalive_decisions)",
        )
        .fetch_one(fixture.pool())
        .await?)
    }
}

#[test]
fn t3_postgres__storage_roundtrips_postgres() {
    run_postgres_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn t3_postgres__runtime_change_notifier_delivery_postgres() {
    run_postgres_scenario(
        "runtime_change_notifier_delivery",
        runtime_change_notifier::subscriber_receives_change,
    );
}

#[test]
fn tx__notify_latency_budget_postgres() {
    run_postgres_scenario(
        "notify_latency_budget",
        runtime_change_notifier::subscriber_receives_within_latency_budget,
    );
}

#[test]
fn t3_postgres__principal_store_full_conformance_postgres() {
    run_postgres_scenario("principal_store", principal_store::run_all);
}

#[test]
fn t3_postgres__upstream_store_full_conformance_postgres() {
    run_postgres_scenario("upstream_store", upstream_store::run_all);
}

#[test]
fn t3_postgres__cache_keepalive_session_store_conformance_postgres() {
    run_postgres_scenario(
        "cache_keepalive_session_store",
        cache_keepalive_session_store::run_all,
    );
}

#[test]
fn t3_postgres__audit_sink_batch_roundtrip_and_prune_boundary_postgres() {
    run_postgres_scenario(
        "audit_sink_batch_roundtrip_and_prune_boundary",
        audit_sink::batch_roundtrip_and_prune_boundary,
    );
}

#[test]
fn t3_postgres__cache_keepalive_projection_decision_append_roundtrip_postgres() {
    run_postgres_scenario(
        "cache_keepalive_projection_decision_append_roundtrip",
        cache_keepalive_projection_store::decision_append_roundtrip,
    );
}

#[test]
fn t3_postgres__cache_keepalive_projection_append_is_atomic_postgres() {
    run_postgres_scenario(
        "cache_keepalive_projection_append_is_atomic",
        cache_keepalive_projection_store::append_with_projections_is_atomic,
    );
}

#[test]
fn t3_postgres__plugin_blob_repo_roundtrip_delete_and_missing_postgres() {
    run_postgres_scenario(
        "plugin_blob_repo_roundtrip_delete_and_missing",
        plugin_blob_repo::roundtrip_delete_and_missing,
    );
}

#[test]
fn t3_postgres__request_events_prune_strict_millisecond_boundary_and_batch_limit_postgres() {
    run_postgres_scenario(
        "request_events_prune_strict_millisecond_boundary_and_batch_limit",
        request_events_prune::strict_millisecond_boundary_and_batch_limit,
    );
}

#[test]
fn t3_postgres__pool_quota_history_fable_roundtrip_postgres() {
    run_postgres_scenario(
        "pool_quota_history_fable_roundtrip",
        pool_quota_history_store::fable_roundtrip,
    );
}

#[test]
fn t3_postgres__pool_quota_summary_latest_and_range_postgres() {
    run_postgres_scenario(
        "pool_quota_summary_latest_and_range",
        pool_quota_history_store::summary_latest_and_range,
    );
}

#[test]
fn t3_postgres__request_event_cache_split_round_trip_postgres() {
    run_postgres_scenario(
        "request_event_cache_split_round_trip",
        storage_roundtrips_cache_split::request_event_cache_split_round_trip,
    );
}

#[test]
fn t3_postgres__request_event_latency_stage_round_trip_postgres() {
    run_postgres_scenario(
        "request_event_latency_stage_round_trip",
        storage_roundtrips_latency_stages::request_event_latency_stage_round_trip,
    );
}

#[test]
fn t3_postgres__request_event_list_projects_rows_and_preserves_detail_postgres() {
    run_postgres_scenario(
        "request_event_list_projects_rows_and_preserves_detail",
        request_event_list::request_event_list_projects_rows_and_preserves_detail,
    );
}

#[test]
fn t3_postgres__request_event_list_model_filter_matches_case_insensitive_prefix_postgres() {
    run_postgres_scenario(
        "request_event_list_model_filter_matches_case_insensitive_prefix",
        request_event_list::request_event_list_model_filter_matches_case_insensitive_prefix,
    );
}

#[test]
fn t3_postgres__request_event_principal_cost_components_postgres() {
    run_postgres_scenario(
        "request_event_principal_cost_components",
        request_event_principal_costs::principal_cost_components_aggregate_without_fabrication,
    );
}

#[test]
fn t3_postgres__request_event_key_usage_materialized_columns_postgres() {
    run_postgres_scenario(
        "request_event_key_usage_materialized_columns",
        request_event_key_usage::materialized_key_usage_preserves_bucket_contract,
    );
}

#[test]
fn t3_postgres__request_event_key_last_used_preserves_inclusive_range_and_filters_postgres() {
    run_postgres_scenario(
        "request_event_key_last_used_preserves_inclusive_range_and_filters",
        request_event_key_usage::last_used_preserves_inclusive_range_and_filters,
    );
}

#[test]
fn t3_postgres__cache_keepalive_batch_turn_reads_match_per_session_postgres() {
    run_postgres_scenario(
        "cache_keepalive_batch_turn_reads_match_per_session",
        cache_keepalive_session_reads::batch_turn_reads_match_canonical_per_session_reads,
    );
}

#[test]
fn t3_postgres__cache_keepalive_session_read_list_detail_filters_postgres() {
    run_postgres_scenario(
        "cache_keepalive_session_read_list_detail_filters",
        cache_keepalive_session_reads::list_detail_filters_preserve_frozen_projection_contract,
    );
}

#[test]
fn t3_postgres__cache_keepalive_session_read_pagination_cursor_postgres() {
    run_postgres_scenario(
        "cache_keepalive_session_read_pagination_cursor",
        cache_keepalive_session_reads::pagination_horizon_cursor_and_frozen_order_contract,
    );
}

#[test]
fn t3_postgres__usage_rollup_filtered_analysis_query_postgres() {
    run_postgres_scenario(
        "usage_rollup_filtered_analysis_query",
        usage_rollups::run_all,
    );
}

#[test]
fn t3_postgres__usage_rollup_checkpoint_absent_before_first_rollup_postgres() {
    run_postgres_scenario(
        "usage_rollup_checkpoint_absent_before_first_rollup",
        usage_rollups::checkpoint_is_absent_before_first_rollup,
    );
}

#[test]
fn t3_postgres__usage_rollup_empty_queries_preserve_range_boundaries_postgres() {
    run_postgres_scenario(
        "usage_rollup_empty_queries_preserve_range_boundaries",
        usage_rollups::empty_queries_preserve_range_boundaries,
    );
}

#[test]
fn t3_postgres__overview_excluded_error_buckets_preserve_boundaries_postgres() {
    run_postgres_scenario(
        "overview_excluded_error_buckets_preserve_boundaries",
        usage_rollups::overview_excluded_error_buckets_preserve_boundaries,
    );
}

#[test]
fn t3_postgres__plugin_registry_store_postgres() {
    run_postgres_scenario("plugin_registry_store", plugin_registry_store::run_all);
}

#[test]
fn t3_postgres__plugin_registry_refcount_counts_chain_and_warmup_references_postgres() {
    run_postgres_scenario(
        "refcount_counts_chain_and_warmup_references",
        plugin_registry_store::refcount_counts_chain_and_warmup_references,
    );
}

#[test]
fn t3_postgres__plugin_registry_replace_wasm_entry_preserves_id_and_references_postgres() {
    run_postgres_scenario(
        "replace_wasm_entry_preserves_id_and_references",
        plugin_registry_store::replace_wasm_entry_preserves_id_and_references,
    );
}

#[test]
fn t3_postgres__plugin_registry_replace_wasm_entry_with_stale_revision_conflicts_postgres() {
    run_postgres_scenario(
        "replace_wasm_entry_with_stale_revision_conflicts",
        plugin_registry_store::replace_wasm_entry_with_stale_revision_conflicts,
    );
}

#[test]
fn t3_postgres__plugin_registry_list_registry_references_returns_chain_and_warmup_postgres() {
    run_postgres_scenario(
        "list_registry_references_returns_chain_and_warmup",
        plugin_registry_store::list_registry_references_returns_chain_and_warmup,
    );
}

#[test]
fn t3_postgres__plugin_registry_cascade_delete_registry_entry_removes_chain_warmup_and_blob_postgres()
 {
    run_postgres_scenario(
        "cascade_delete_registry_entry_removes_chain_warmup_and_blob",
        plugin_registry_store::cascade_delete_registry_entry_removes_chain_warmup_and_blob,
    );
}

#[test]
fn t3_postgres__plugin_registry_cascade_delete_registry_entry_rejects_changed_fingerprint_postgres()
{
    run_postgres_scenario(
        "cascade_delete_registry_entry_rejects_changed_fingerprint",
        plugin_registry_store::cascade_delete_registry_entry_rejects_changed_fingerprint,
    );
}

#[test]
fn t3_postgres__plugin_registry_registry_by_id_returns_seeded_builtin_subscription_preference_postgres()
 {
    run_postgres_scenario(
        "registry_by_id_returns_seeded_builtin_subscription_preference",
        plugin_registry_store::registry_by_id_returns_seeded_builtin_subscription_preference,
    );
}

#[test]
fn t3_postgres__plugin_registry_created_principal_has_builtin_subscription_preference_chain_entry_postgres()
 {
    run_postgres_scenario(
        "created_principal_has_builtin_subscription_preference_chain_entry",
        plugin_registry_store::created_principal_has_builtin_subscription_preference_chain_entry,
    );
}

#[test]
fn t3_postgres__plugin_registry_builtin_subscription_preference_is_visible_and_insertable() {
    run_postgres_scenario(
        "builtin_subscription_preference_is_visible_and_insertable",
        plugin_registry_store::builtin_subscription_preference_is_visible_and_insertable,
    );
}

#[test]
fn t3_postgres__plugin_registry_refcount_transitions_follow_live_chain_and_warmup_references() {
    run_postgres_scenario(
        "refcount_transitions_follow_live_chain_and_warmup_references",
        plugin_registry_store::refcount_transitions_follow_live_chain_and_warmup_references,
    );
}

#[test]
fn t3_postgres__plugin_registry_list_orphan_blobs_returns_blobs_without_registry_postgres() {
    run_postgres_scenario(
        "list_orphan_blobs_returns_blobs_without_registry",
        plugin_registry_store::list_orphan_blobs_returns_blobs_without_registry,
    );
}

#[test]
fn t3_postgres__plugin_registry_same_sha_metadata_mismatch_conflicts_postgres() {
    run_postgres_scenario(
        "same_sha_metadata_mismatch_conflicts",
        plugin_registry_store::same_sha_metadata_mismatch_conflicts,
    );
}

#[test]
fn t3_postgres__plugin_registry_router_multi_entry_ordered_postgres() {
    run_postgres_scenario(
        "router_multi_entry_ordered",
        plugin_registry_store::router_multi_entry_ordered,
    );
}

#[test]
fn t3_postgres__plugin_registry_insert_chain_entry_rejects_duplicate_for_shape_slot_postgres() {
    run_postgres_scenario(
        "insert_chain_entry_rejects_duplicate_for_shape_slot",
        plugin_registry_store::insert_chain_entry_rejects_duplicate_for_shape_slot,
    );
}

#[test]
fn t3_postgres__plugin_registry_router_reorder_preserves_invariants_postgres() {
    run_postgres_scenario(
        "router_reorder_preserves_invariants",
        plugin_registry_store::router_reorder_preserves_invariants,
    );
}

#[test]
fn t3_postgres__plugin_registry_shape_singleton_preserved_postgres() {
    run_postgres_scenario(
        "shape_singleton_preserved",
        plugin_registry_store::shape_singleton_preserved,
    );
}

#[test]
fn t3_postgres__plugin_registry_insert_chain_entry_allows_multi_for_observability_hook_postgres() {
    run_postgres_scenario(
        "insert_chain_entry_allows_multi_for_observability_hook",
        plugin_registry_store::insert_chain_entry_allows_multi_for_observability_hook,
    );
}

#[test]
fn t3_postgres__plugin_registry_concurrent_upload_returns_existed_once_postgres() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(concurrent_upload_returns_existed_once())
        .unwrap_or_else(|error| panic!("concurrent_upload_returns_existed_once postgres: {error}"));
}

#[test]
fn t3_postgres__principal_allowed_upstreams_roundtrip_postgres() {
    run_postgres_scenario(
        "principal_allowed_upstreams_roundtrip",
        principal_store::principal_allowed_upstreams_roundtrip,
    );
}

#[test]
fn t3_postgres__upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip",
        upstream_rate_limit_store::put_then_list_for_upstream_ids_roundtrip,
    );
}

#[test]
fn t3_postgres__upstream_rate_limit_roundtrip_and_list_boundary_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_roundtrip_and_list_boundary",
        upstream_rate_limit_store::roundtrip_and_list_boundary,
    );
}

#[test]
fn t3_postgres__upstream_rate_limit_latest_write_wins_within_same_key_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key",
        upstream_rate_limit_store::latest_write_wins_within_same_key,
    );
}

#[test]
fn t3_postgres__upstream_rate_limit_latest_write_wins_within_same_key_forward_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key_forward",
        upstream_rate_limit_store::latest_write_wins_within_same_key_forward,
    );
}

#[test]
fn t3_postgres__upstream_rate_limit_empty_list_for_unknown_id_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_empty_list_for_unknown_id",
        upstream_rate_limit_store::empty_list_for_unknown_id,
    );
}

#[test]
fn t3_postgres__anthropic_compatibility_kv_store_postgres() {
    run_postgres_scenario(
        "anthropic_compatibility_kv_store",
        anthropic_compatibility_kv_store::run_all,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_store_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_store",
        upstream_subscription_quota_store::run_all,
    );
}
#[test]
fn t3_postgres__upstream_subscription_quota_absent_replaces_latest_without_erasing_history_postgres()
 {
    run_postgres_scenario(
        "upstream_subscription_quota_absent_replaces_latest_without_erasing_history",
        upstream_subscription_quota_store::absent_replaces_latest_without_erasing_history,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_absent_is_idempotent_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_absent_is_idempotent",
        upstream_subscription_quota_store::absent_is_idempotent,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_checkpoint_writer_latest_freshness_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_writer_latest_freshness",
        upstream_subscription_quota_store::checkpoint_writer_latest_freshness,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_checkpoint_writer_decrease_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_writer_decrease",
        upstream_subscription_quota_store::checkpoint_writer_decrease,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_checkpoint_series_anchor_merge_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_series_anchor_merge",
        upstream_subscription_quota_store::checkpoint_series_anchor_merge,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_checkpoint_history_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_history",
        upstream_subscription_quota_store::checkpoint_history,
    );
}

#[test]
fn t3_postgres__upstream_subscription_quota_aggregate_store_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_aggregate_store",
        upstream_subscription_quota_store::aggregate_store,
    );
}

#[test]
fn t3_postgres__warmup_attempts_store_postgres() {
    run_postgres_scenario("warmup_attempts_store", |backend| async move {
        warmup_attempts_store::run_all(backend, warmup_attempts_clock()).await
    });
}

#[test]
fn t3_postgres__upstream_subscription_metadata_store_postgres() {
    run_postgres_scenario(
        "upstream_subscription_metadata_store",
        upstream_subscription_metadata_store::run_all,
    );
}

#[test]
fn t3_postgres__organization_metadata_store_postgres() {
    run_postgres_scenario(
        "organization_metadata_store",
        organization_metadata_store::run_all,
    );
}

#[test]
fn t3_postgres__plan_tier_store_postgres() {
    run_postgres_scenario("plan_tier_store", plan_tier_store::run_all);
}

#[test]
fn t3_postgres__plan_tier_store_backfill_postgres() {
    run_postgres_scenario(
        "plan_tier_store_backfill",
        plan_tier_store_backfill::upstream_tier_backfill_intervals,
    );
}

macro_rules! prompt_cache_observation_postgres_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_postgres_scenario(stringify!($scenario), |backend| async move {
                prompt_cache_observation_store::$scenario(backend, prompt_cache_clock()).await
            });
        }
    };
}

prompt_cache_observation_postgres_test!(
    t3_postgres__prompt_cache_observation_upsert_then_list_returns_active_only_postgres,
    upsert_then_list_returns_active_only
);
prompt_cache_observation_postgres_test!(
    t3_postgres__prompt_cache_observation_asymmetric_ttl_snapshot_visibility_postgres,
    asymmetric_ttl_snapshot_visibility
);
prompt_cache_observation_postgres_test!(
    t3_postgres__prompt_cache_observation_purge_expired_before_removes_only_expired_postgres,
    purge_expired_before_removes_only_expired
);
prompt_cache_observation_postgres_test!(
    t3_postgres__prompt_cache_observation_hydrate_after_restart_filters_expired_postgres,
    hydrate_after_restart_filters_expired
);
prompt_cache_observation_postgres_test!(
    t3_postgres__prompt_cache_observation_observation_list_is_sorted_postgres,
    observation_list_is_sorted
);
prompt_cache_observation_postgres_test!(
    t3_postgres__prompt_cache_observation_cross_model_same_prefix_keeps_both_rows_postgres,
    cross_model_same_prefix_keeps_both_rows
);

#[test]
fn t3_postgres__price_catalog_roundtrip_smoke_postgres() {
    run_postgres_price_scenario(
        "price_catalog_roundtrip_smoke",
        price_catalog::roundtrip_smoke,
    );
}

#[test]
fn t3_postgres__price_catalog_put_same_payload_twice_updates_fetched_at_postgres() {
    run_postgres_price_scenario(
        "price_catalog_put_same_payload_twice_updates_fetched_at",
        price_catalog::put_same_payload_twice_updates_fetched_at,
    );
}

#[test]
fn t3_postgres__price_catalog_corrupted_payload_hash_is_rejected_postgres() {
    run_postgres_price_scenario(
        "price_catalog_corrupted_payload_hash_is_rejected",
        price_catalog::corrupted_payload_hash_is_rejected,
    );
}

#[test]
fn t3_postgres__api_key_usage_flush_idempotency_aggregation_and_range_boundary() {
    run_postgres_scenario(
        "api_key_usage_flush_idempotency_aggregation_and_range_boundary",
        api_key_usage_bucket_store::flush_idempotency_aggregation_and_range_boundary,
    );
}

fn run_postgres_price_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<PostgresConformanceBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let _guard = PRICE_CATALOG_SCENARIO_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_postgres_scenario(name, scenario);
}

fn run_postgres_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<PostgresConformanceBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(PostgresConformanceBackend)))
        .unwrap_or_else(|error| panic!("{name} postgres: {error}"));
}

fn prompt_cache_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new_at_secs(1_700_000_000))
}

fn warmup_attempts_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new_at_secs(1_800_604_800))
}

async fn concurrent_upload_returns_existed_once() -> anyhow::Result<()> {
    let backend = PostgresConformanceBackend;
    let fixture = backend.create_fixture().await?;
    let result = concurrent_upload_returns_existed_once_on_fixture(&backend, &fixture).await;
    let teardown = backend.teardown(fixture).await;
    result?;
    teardown?;
    Ok(())
}

async fn concurrent_upload_returns_existed_once_on_fixture(
    backend: &PostgresConformanceBackend,
    fixture: &PostgresFixture,
) -> anyhow::Result<()> {
    let first_storage = backend.open(fixture).await?;
    let second_storage = backend.open(fixture).await?;
    let blob = WasmBlob {
        sha256: [42; 32],
        bytes: b"concurrent-upload".to_vec(),
        size_bytes: b"concurrent-upload".len() as u64,
        parse_validated_at_unix_secs: 1_800_000_000,
    };
    let input = WasmRegistryEntryInput {
        schema_hash: None,
        name: "plugin-concurrent-upload".to_owned(),
        version: None,
        original_filename: "plugin-concurrent-upload.wasm".to_owned(),
        label: None,
        uploaded_at_unix_secs: 1_800_000_100,
        uploaded_by_admin_id: uuid::Uuid::from_u128(0x7003),
        description: "concurrent upload".to_owned(),
        usage: "test fixture".to_owned(),
        hook_metadata: Default::default(),
        supported_slots: Vec::new(),
    };

    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = barrier.clone();
    let second_barrier = barrier.clone();
    let first_blob = blob.clone();
    let first_input = input.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_storage
            .persist_wasm_upload(first_blob, first_input)
            .await
    });
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_storage.persist_wasm_upload(blob, input).await
    });

    let results = [first.await??, second.await??];
    anyhow::ensure!(
        results[0].0.id == results[1].0.id,
        "concurrent uploads return the same registry id"
    );
    anyhow::ensure!(
        results[0].0.sha256 == results[1].0.sha256,
        "concurrent uploads return the same sha"
    );
    let mut existed = results
        .iter()
        .map(|(_entry, did_exist)| *did_exist)
        .collect::<Vec<_>>();
    existed.sort();
    anyhow::ensure!(
        existed == [false, true],
        "exactly one upload reports existed"
    );
    Ok(())
}
