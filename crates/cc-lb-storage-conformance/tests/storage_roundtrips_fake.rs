#![allow(non_snake_case)]

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, MetaStore, UsageRollup, UsageRollupResolution, UsageTokenInterval,
    UsageTokenIntervalStore, UsageTokenIntervalSum,
};
use cc_lb_storage_conformance::{
    harness::ConformanceBackend,
    plugin_registry_store,
    scenarios::{
        api_key_usage_bucket_store, audit_sink, cache_keepalive_projection_store,
        cache_keepalive_session_reads, cache_keepalive_session_store, managed_keys,
        organization_metadata_store, plan_tier_store, plugin_blob_repo, pool_quota_history_store,
        price_catalog, principal_store, request_event_key_usage, request_event_list,
        request_events_prune, storage_roundtrips, upstream_rate_limit_store, upstream_store,
        upstream_subscription_metadata_store, upstream_subscription_quota_store, usage_rollups,
    },
};
use cc_lb_testkit::InMemoryStorage;
use tokio::runtime::Runtime;
use uuid::Uuid;

struct FakeConformanceBackend;

#[async_trait]
impl ConformanceBackend for FakeConformanceBackend {
    type Storage = InMemoryStorage;
    type Fixture = ();

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        Ok(())
    }

    async fn open(&self, _fixture: &Self::Fixture) -> anyhow::Result<Self::Storage> {
        let storage = InMemoryStorage::default();
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
impl managed_keys::ManagedKeyBackend for FakeConformanceBackend {
    type Store = InMemoryStorage;
    type Fixture = ();

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        Ok(())
    }

    async fn open(&self, _fixture: &Self::Fixture) -> anyhow::Result<Self::Store> {
        Ok(InMemoryStorage::default())
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl upstream_store::UpstreamStoreBackend for FakeConformanceBackend {
    type Store = InMemoryStorage;
    type Fixture = ();

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        Ok(())
    }

    async fn open(&self, _fixture: &Self::Fixture) -> anyhow::Result<Self::Store> {
        Ok(InMemoryStorage::default())
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }
}

#[test]
fn t3__fake_storage_roundtrips() {
    run_fake_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn t3__fake_plugin_registry_list_ordering_pagination_and_filter() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async {
            let storage = InMemoryStorage::default();
            plugin_registry_store::registry_list_paginates(&storage).await
        })
        .unwrap_or_else(|error| panic!("plugin registry list fake: {error}"));
}

#[test]
fn t3__fake_plugin_registry_upload_blob_and_chain_delete_contracts() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async {
            let storage = InMemoryStorage::default();
            plugin_registry_store::persist_wasm_upload_creates_blob_and_registry(&storage).await?;
            let storage = InMemoryStorage::default();
            plugin_registry_store::delete_chain_entry_decrements_refcount(&storage).await?;
            let storage = InMemoryStorage::default();
            plugin_registry_store::delete_chain_entry_missing_is_false(&storage).await
        })
        .unwrap_or_else(|error| panic!("plugin registry upload/delete fake: {error}"));
}

#[test]
fn t3__fake_builtin_subscription_preference_is_visible_and_insertable() {
    run_fake_scenario(
        "builtin_subscription_preference_is_visible_and_insertable",
        plugin_registry_store::builtin_subscription_preference_is_visible_and_insertable,
    );
}

#[test]
fn t3__fake_refcount_transitions_follow_live_chain_and_warmup_references() {
    run_fake_scenario(
        "refcount_transitions_follow_live_chain_and_warmup_references",
        plugin_registry_store::refcount_transitions_follow_live_chain_and_warmup_references,
    );
}

#[test]
fn t3__fake_managed_keys_conformance() {
    run_fake_scenario("managed_keys", managed_keys::run_all);
}

#[test]
fn t3__fake_upstream_store_full_conformance() {
    run_fake_scenario("upstream_store", upstream_store::run_all);
}

#[test]
fn t3__fake_upstream_rate_limit_roundtrip_and_list_boundary() {
    run_fake_scenario(
        "upstream_rate_limit_roundtrip_and_list_boundary",
        upstream_rate_limit_store::roundtrip_and_list_boundary,
    );
}

#[test]
fn t3__fake_principal_store_full_conformance() {
    run_fake_scenario("principal_store", principal_store::run_all);
}

#[test]
fn t3__fake_plan_tier_store_conformance() {
    run_fake_scenario("plan_tier_store", plan_tier_store::run_all);
}

#[test]
fn t3__fake_upstream_subscription_metadata_store_conformance() {
    run_fake_scenario(
        "upstream_subscription_metadata_store",
        upstream_subscription_metadata_store::run_all,
    );
}

#[test]
fn t3__fake_organization_metadata_store_conformance() {
    run_fake_scenario(
        "organization_metadata_store",
        organization_metadata_store::run_all,
    );
}

#[test]
fn t3__fake_cache_keepalive_session_store_conformance() {
    run_fake_scenario(
        "cache_keepalive_session_store",
        cache_keepalive_session_store::run_all,
    );
}

#[test]
fn t3__fake_cache_keepalive_session_read_list_detail_filters() {
    run_fake_scenario(
        "cache_keepalive_session_read_list_detail_filters",
        cache_keepalive_session_reads::list_detail_filters_preserve_frozen_projection_contract,
    );
}

#[test]
fn t3__fake_cache_keepalive_session_read_pagination_cursor() {
    run_fake_scenario(
        "cache_keepalive_session_read_pagination_cursor",
        cache_keepalive_session_reads::pagination_horizon_cursor_and_frozen_order_contract,
    );
}

#[test]
fn t3__fake_cache_keepalive_batch_turn_reads_match_per_session() {
    run_fake_scenario(
        "cache_keepalive_batch_turn_reads_match_per_session",
        cache_keepalive_session_reads::batch_turn_reads_match_canonical_per_session_reads,
    );
}

#[test]
fn t3__fake_usage_rollup_checkpoint_absent_before_first_rollup() {
    run_fake_scenario(
        "usage_rollup_checkpoint_absent_before_first_rollup",
        usage_rollups::checkpoint_is_absent_before_first_rollup,
    );
}

#[test]
fn t3__fake_usage_rollup_empty_queries_preserve_range_boundaries() {
    run_fake_scenario(
        "usage_rollup_empty_queries_preserve_range_boundaries",
        usage_rollups::empty_queries_preserve_range_boundaries,
    );
}

#[test]
fn t3__fake_usage_token_intervals_preserve_input_order_and_inclusive_boundaries() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async {
            let storage = InMemoryStorage::default();
            let upstream = Uuid::from_u128(0x500);
            let other_upstream = Uuid::from_u128(0x501);
            let rollup =
                |resolution, bucket_start, upstream_id, tokens: [u64; 4]| UsageRollup {
                    resolution,
                    bucket_start,
                    principal: "principal".to_owned(),
                    upstream_id,
                    upstream_name: "upstream".to_owned(),
                    model: "model".to_owned(),
                    request_count: 1,
                    input_tokens: tokens[0],
                    output_tokens: tokens[1],
                    cache_creation_input_tokens: tokens[2],
                    cache_read_input_tokens: tokens[3],
                    error_count: 0,
                    latency_count: 0,
                    latency_ms_sum: 0,
                    latency_ms_min: None,
                    latency_ms_max: None,
                    proxy_setup_ms_count: 0,
                    proxy_setup_ms_sum: 0,
                    shape_ms_count: 0,
                    shape_ms_sum: 0,
                    sign_ms_count: 0,
                    sign_ms_sum: 0,
                    upstream_ttfb_ms_count: 0,
                    upstream_ttfb_ms_sum: 0,
                    upstream_body_ms_count: 0,
                    upstream_body_ms_sum: 0,
                    virtual_cost_micros: 0,
                };
            storage.script_usage_rollups(vec![
                rollup(UsageRollupResolution::Minute, 99, upstream, [100, 0, 0, 0]),
                rollup(UsageRollupResolution::Minute, 100, upstream, [1, 2, 3, 4]),
                rollup(UsageRollupResolution::Minute, 150, upstream, [2, 3, 4, 5]),
                rollup(UsageRollupResolution::Minute, 200, upstream, [5, 6, 7, 8]),
                rollup(UsageRollupResolution::Minute, 201, upstream, [100, 0, 0, 0]),
                rollup(
                    UsageRollupResolution::Hour,
                    150,
                    upstream,
                    [1_000, 0, 0, 0],
                ),
                rollup(
                    UsageRollupResolution::Minute,
                    150,
                    other_upstream,
                    [200, 0, 0, 0],
                ),
            ])?;

            let actual = storage
                .sum_usage_tokens_for_intervals(&[
                    UsageTokenInterval {
                        interval_id: 30,
                        upstream_id: upstream,
                        start_unix_secs: 200,
                        end_unix_secs: 200,
                    },
                    UsageTokenInterval {
                        interval_id: 10,
                        upstream_id: upstream,
                        start_unix_secs: 100,
                        end_unix_secs: 200,
                    },
                    UsageTokenInterval {
                        interval_id: 30,
                        upstream_id: upstream,
                        start_unix_secs: 99,
                        end_unix_secs: 100,
                    },
                    UsageTokenInterval {
                        interval_id: 20,
                        upstream_id: other_upstream,
                        start_unix_secs: 100,
                        end_unix_secs: 200,
                    },
                    UsageTokenInterval {
                        interval_id: 40,
                        upstream_id: Uuid::from_u128(0x502),
                        start_unix_secs: 0,
                        end_unix_secs: 1_000,
                    },
                    UsageTokenInterval {
                        interval_id: 50,
                        upstream_id: upstream,
                        start_unix_secs: 201,
                        end_unix_secs: 200,
                    },
                ])
                .await?;

            assert_eq!(
                actual,
                vec![
                    UsageTokenIntervalSum {
                        interval_id: 30,
                        tokens: 26,
                    },
                    UsageTokenIntervalSum {
                        interval_id: 10,
                        tokens: 50,
                    },
                    UsageTokenIntervalSum {
                        interval_id: 30,
                        tokens: 110,
                    },
                    UsageTokenIntervalSum {
                        interval_id: 20,
                        tokens: 200,
                    },
                    UsageTokenIntervalSum {
                        interval_id: 40,
                        tokens: 0,
                    },
                    UsageTokenIntervalSum {
                        interval_id: 50,
                        tokens: 0,
                    },
                ],
                "results must follow input ordinal, keep duplicate ids, include both boundaries, and filter by upstream and minute resolution",
            );
            Ok::<(), cc_lb_storage_api::StorageError>(())
        })
        .expect("usage token interval fake conformance");
}

#[test]
fn t3__fake_usage_token_intervals_empty_input_returns_empty() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async {
            let storage = InMemoryStorage::default();
            let actual = storage.sum_usage_tokens_for_intervals(&[]).await?;
            assert_eq!(actual, Vec::<UsageTokenIntervalSum>::new());
            Ok::<(), cc_lb_storage_api::StorageError>(())
        })
        .expect("empty usage token interval fake conformance");
}

#[test]
fn t3__fake_audit_sink_batch_roundtrip_and_prune_boundary() {
    run_fake_scenario(
        "audit_sink_batch_roundtrip_and_prune_boundary",
        audit_sink::batch_roundtrip_and_prune_boundary,
    );
}

#[test]
fn t3__fake_audit_sink_records_in_order() {
    run_fake_scenario(
        "audit_sink_records_in_order",
        audit_sink::sink_records_in_order,
    );
}

#[test]
fn t3__fake_cache_keepalive_projection_decision_append_roundtrip() {
    run_fake_scenario(
        "cache_keepalive_projection_decision_append_roundtrip",
        cache_keepalive_projection_store::decision_append_roundtrip,
    );
}

#[test]
fn t3__fake_plugin_blob_repo_roundtrip_delete_and_missing() {
    run_fake_scenario(
        "plugin_blob_repo_roundtrip_delete_and_missing",
        plugin_blob_repo::roundtrip_delete_and_missing,
    );
}

#[test]
fn t3__fake_plugin_registry_blob_gc_contracts() {
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async {
            plugin_registry_store::list_orphan_blobs_returns_blobs_without_registry(Arc::new(
                FakeConformanceBackend,
            ))
            .await?;
            let storage = InMemoryStorage::default();
            plugin_registry_store::decrement_blob_refcount_or_delete_missing_is_false(&storage)
                .await?;
            plugin_registry_store::decrement_blob_refcount_or_delete_skips_registry_backed(
                Arc::new(FakeConformanceBackend),
            )
            .await
        })
        .unwrap_or_else(|error| panic!("plugin registry blob GC fake: {error}"));
}

#[test]
fn t3__fake_price_catalog_roundtrip_smoke() {
    run_fake_scenario(
        "price_catalog_roundtrip_smoke",
        price_catalog::roundtrip_smoke,
    );
}

#[test]
fn t3__fake_price_catalog_put_same_payload_twice_updates_fetched_at() {
    run_fake_scenario(
        "price_catalog_put_same_payload_twice_updates_fetched_at",
        price_catalog::put_same_payload_twice_updates_fetched_at,
    );
}

#[test]
fn t3__fake_pool_quota_summary_latest_and_range() {
    run_fake_scenario(
        "pool_quota_summary_latest_and_range",
        pool_quota_history_store::summary_latest_and_range,
    );
}

#[test]
fn t3__fake_request_event_key_last_used_preserves_inclusive_range_and_filters() {
    run_fake_scenario(
        "request_event_key_last_used_preserves_inclusive_range_and_filters",
        request_event_key_usage::last_used_preserves_inclusive_range_and_filters,
    );
}

#[test]
fn t3__fake_request_events_prune_strict_millisecond_boundary_and_batch_limit() {
    run_fake_scenario(
        "request_events_prune_strict_millisecond_boundary_and_batch_limit",
        request_events_prune::strict_millisecond_boundary_and_batch_limit,
    );
}

#[test]
fn t3__fake_request_event_list_projects_rows_and_preserves_detail() {
    run_fake_scenario(
        "request_event_list_projects_rows_and_preserves_detail",
        request_event_list::request_event_list_projects_rows_and_preserves_detail,
    );
}

#[test]
fn t3__fake_request_event_list_model_filter_matches_case_insensitive_prefix() {
    run_fake_scenario(
        "request_event_list_model_filter_matches_case_insensitive_prefix",
        request_event_list::request_event_list_model_filter_matches_case_insensitive_prefix,
    );
}

#[test]
fn t3__fake_subscription_quota_slim_checkpoint_projection_contract() {
    run_fake_scenario(
        "subscription_quota_slim_checkpoint_projection",
        upstream_subscription_quota_store::slim_checkpoints_preserve_left_anchor_sources_and_same_millis_ties,
    );
}

#[test]
fn t3__fake_upstream_subscription_quota_store_conformance() {
    run_fake_scenario(
        "upstream_subscription_quota_store",
        fake_upstream_subscription_quota_store_conformance,
    );
}

async fn fake_upstream_subscription_quota_store_conformance(
    backend: Arc<FakeConformanceBackend>,
) -> anyhow::Result<()> {
    upstream_subscription_quota_store::append_then_list_latest_roundtrip(Arc::clone(&backend))
        .await?;
    upstream_subscription_quota_store::same_millis_appends_with_different_sample_ids_dont_collide(
        Arc::clone(&backend),
    )
    .await?;
    upstream_subscription_quota_store::header_and_api_sources_coexist_in_latest(Arc::clone(
        &backend,
    ))
    .await?;
    upstream_subscription_quota_store::latest_is_monotonic_in_millis(Arc::clone(&backend)).await?;
    upstream_subscription_quota_store::series_returns_buckets_with_correct_bounds(Arc::clone(
        &backend,
    ))
    .await?;
    upstream_subscription_quota_store::series_source_merge_merged_collapses_both_sources(
        Arc::clone(&backend),
    )
    .await?;
    upstream_subscription_quota_store::series_source_merge_header_filters_api(Arc::clone(&backend))
        .await?;
    upstream_subscription_quota_store::series_max_points_per_series_downsamples(Arc::clone(
        &backend,
    ))
    .await?;
    upstream_subscription_quota_store::process_start_marker_persists_with_sample_kind(Arc::clone(
        &backend,
    ))
    .await?;
    upstream_subscription_quota_store::absent_replaces_latest_without_erasing_history(Arc::clone(
        &backend,
    ))
    .await?;
    upstream_subscription_quota_store::absent_is_idempotent(Arc::clone(&backend)).await?;
    upstream_subscription_quota_store::empty_upstream_ids_returns_empty(Arc::clone(&backend))
        .await?;
    upstream_subscription_quota_store::series_filters_observed_at_window(Arc::clone(&backend))
        .await?;
    upstream_subscription_quota_store::checkpoint_writer_latest_freshness(Arc::clone(&backend))
        .await?;
    upstream_subscription_quota_store::checkpoint_writer_decrease(Arc::clone(&backend)).await?;
    upstream_subscription_quota_store::checkpoint_series_anchor_merge(Arc::clone(&backend)).await?;
    upstream_subscription_quota_store::checkpoint_history(backend).await
}

#[test]
fn t3__fake_api_key_usage_flush_idempotency_aggregation_and_range_boundary() {
    run_fake_scenario(
        "api_key_usage_flush_idempotency_aggregation_and_range_boundary",
        api_key_usage_bucket_store::flush_idempotency_aggregation_and_range_boundary,
    );
}

fn run_fake_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<FakeConformanceBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(FakeConformanceBackend)))
        .unwrap_or_else(|error| panic!("{name} fake: {error}"));
}
