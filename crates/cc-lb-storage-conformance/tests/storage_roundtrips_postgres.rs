#![cfg(feature = "postgres")]

#[path = "support/request_event_quota_postgres.rs"]
mod request_event_quota_postgres;
#[path = "support/request_event_quota.rs"]
mod request_event_quota_support;

request_event_quota_postgres::define_request_event_quota_postgres_tests!();

use std::{future::Future, str::FromStr, sync::Arc};

use async_trait::async_trait;
use cc_lb_engine::{ClockHandle, SystemClock, TestClock};
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginRegistryStore, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_conformance::{
    harness::ConformanceBackend,
    scenarios::{
        anthropic_compatibility_kv_store, organization_metadata_store, plan_tier_store,
        plan_tier_store_backfill, plugin_registry_store, price_catalog, principal_store,
        prompt_cache_observation_store, storage_roundtrips, storage_roundtrips_cache_split,
        storage_roundtrips_latency_stages, upstream_rate_limit_store,
        upstream_subscription_metadata_store, upstream_subscription_quota_store,
        warmup_attempts_store,
    },
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{runtime::Runtime, sync::Barrier};

struct PostgresConformanceBackend {
    url: String,
}

struct PostgresFixture {
    url: String,
    schema: String,
    pool: PgPool,
}

#[async_trait]
impl ConformanceBackend for PostgresConformanceBackend {
    type Storage = PostgresStorage;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        let schema = schema_name();
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&self.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                PgConnectOptions::from_str(&self.url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        PostgresStorage::new(pool.clone(), system_clock())
            .initialize(BackendKind::Postgres)
            .await?;

        Ok(PostgresFixture {
            url: self.url.clone(),
            schema,
            pool,
        })
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Storage> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                PgConnectOptions::from_str(&fixture.url)?
                    .options([("search_path", fixture.schema.as_str())]),
            )
            .await?;
        Ok(PostgresStorage::new(pool, system_clock()))
    }

    async fn teardown(&self, fixture: Self::Fixture) -> anyhow::Result<()> {
        fixture.pool.close().await;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&fixture.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&fixture.schema)
        )))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }
}

#[test]
fn storage_roundtrips_postgres() {
    run_postgres_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn request_event_cache_split_round_trip_postgres() {
    run_postgres_scenario(
        "request_event_cache_split_round_trip",
        storage_roundtrips_cache_split::request_event_cache_split_round_trip,
    );
}

#[test]
fn request_event_latency_stage_round_trip_postgres() {
    run_postgres_scenario(
        "request_event_latency_stage_round_trip",
        storage_roundtrips_latency_stages::request_event_latency_stage_round_trip,
    );
}

#[test]
fn plugin_registry_store_postgres() {
    run_postgres_scenario("plugin_registry_store", plugin_registry_store::run_all);
}

#[test]
fn plugin_registry_registry_by_id_returns_seeded_builtin_cache_affinity_postgres() {
    run_postgres_scenario(
        "registry_by_id_returns_seeded_builtin_cache_affinity",
        plugin_registry_store::registry_by_id_returns_seeded_builtin_cache_affinity,
    );
}

#[test]
fn plugin_registry_insert_chain_entry_with_builtin_cache_affinity_succeeds_postgres() {
    run_postgres_scenario(
        "insert_chain_entry_with_builtin_cache_affinity_succeeds",
        plugin_registry_store::insert_chain_entry_with_builtin_cache_affinity_succeeds,
    );
}

#[test]
fn plugin_registry_list_orphan_blobs_returns_blobs_without_registry_postgres() {
    run_postgres_scenario(
        "list_orphan_blobs_returns_blobs_without_registry",
        plugin_registry_store::list_orphan_blobs_returns_blobs_without_registry,
    );
}

#[test]
fn plugin_registry_same_sha_metadata_mismatch_conflicts_postgres() {
    run_postgres_scenario(
        "same_sha_metadata_mismatch_conflicts",
        plugin_registry_store::same_sha_metadata_mismatch_conflicts,
    );
}

#[test]
fn plugin_registry_router_multi_entry_ordered_postgres() {
    run_postgres_scenario(
        "router_multi_entry_ordered",
        plugin_registry_store::router_multi_entry_ordered,
    );
}

#[test]
fn plugin_registry_insert_chain_entry_rejects_duplicate_for_shape_slot_postgres() {
    run_postgres_scenario(
        "insert_chain_entry_rejects_duplicate_for_shape_slot",
        plugin_registry_store::insert_chain_entry_rejects_duplicate_for_shape_slot,
    );
}

#[test]
fn plugin_registry_router_reorder_preserves_invariants_postgres() {
    run_postgres_scenario(
        "router_reorder_preserves_invariants",
        plugin_registry_store::router_reorder_preserves_invariants,
    );
}

#[test]
fn plugin_registry_shape_singleton_preserved_postgres() {
    run_postgres_scenario(
        "shape_singleton_preserved",
        plugin_registry_store::shape_singleton_preserved,
    );
}

#[test]
fn plugin_registry_insert_chain_entry_allows_multi_for_observability_hook_postgres() {
    run_postgres_scenario(
        "insert_chain_entry_allows_multi_for_observability_hook",
        plugin_registry_store::insert_chain_entry_allows_multi_for_observability_hook,
    );
}

#[test]
fn plugin_registry_concurrent_upload_returns_existed_once_postgres() {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or PG_URL not set");
        return;
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(concurrent_upload_returns_existed_once(url))
        .unwrap_or_else(|error| panic!("concurrent_upload_returns_existed_once postgres: {error}"));
}

#[test]
fn principal_allowed_upstreams_roundtrip_postgres() {
    run_postgres_scenario(
        "principal_allowed_upstreams_roundtrip",
        principal_store::principal_allowed_upstreams_roundtrip,
    );
}

#[test]
fn upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_put_then_list_for_upstream_ids_roundtrip",
        upstream_rate_limit_store::put_then_list_for_upstream_ids_roundtrip,
    );
}

#[test]
fn upstream_rate_limit_latest_write_wins_within_same_key_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key",
        upstream_rate_limit_store::latest_write_wins_within_same_key,
    );
}

#[test]
fn upstream_rate_limit_latest_write_wins_within_same_key_forward_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_latest_write_wins_within_same_key_forward",
        upstream_rate_limit_store::latest_write_wins_within_same_key_forward,
    );
}

#[test]
fn upstream_rate_limit_empty_list_for_unknown_id_postgres() {
    run_postgres_scenario(
        "upstream_rate_limit_empty_list_for_unknown_id",
        upstream_rate_limit_store::empty_list_for_unknown_id,
    );
}

#[test]
fn anthropic_compatibility_kv_store_postgres() {
    run_postgres_scenario(
        "anthropic_compatibility_kv_store",
        anthropic_compatibility_kv_store::run_all,
    );
}

#[test]
fn upstream_subscription_quota_store_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_store",
        upstream_subscription_quota_store::run_all,
    );
}

#[test]
fn upstream_subscription_quota_checkpoint_writer_latest_freshness_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_writer_latest_freshness",
        upstream_subscription_quota_store::checkpoint_writer_latest_freshness,
    );
}

#[test]
fn upstream_subscription_quota_checkpoint_writer_decrease_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_writer_decrease",
        upstream_subscription_quota_store::checkpoint_writer_decrease,
    );
}

#[test]
fn upstream_subscription_quota_checkpoint_series_anchor_merge_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_series_anchor_merge",
        upstream_subscription_quota_store::checkpoint_series_anchor_merge,
    );
}

#[test]
fn upstream_subscription_quota_checkpoint_history_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_checkpoint_history",
        upstream_subscription_quota_store::checkpoint_history,
    );
}

#[test]
fn warmup_attempts_store_postgres() {
    run_postgres_scenario("warmup_attempts_store", |backend| async move {
        warmup_attempts_store::run_all(backend, warmup_attempts_clock()).await
    });
}

#[test]
fn upstream_subscription_metadata_store_postgres() {
    run_postgres_scenario(
        "upstream_subscription_metadata_store",
        upstream_subscription_metadata_store::run_all,
    );
}

#[test]
fn organization_metadata_store_postgres() {
    run_postgres_scenario(
        "organization_metadata_store",
        organization_metadata_store::run_all,
    );
}

#[test]
fn plan_tier_store_postgres() {
    run_postgres_scenario("plan_tier_store", plan_tier_store::run_all);
}

#[test]
fn plan_tier_store_backfill_postgres() {
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
    prompt_cache_observation_upsert_then_list_returns_active_only_postgres,
    upsert_then_list_returns_active_only
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_asymmetric_ttl_snapshot_visibility_postgres,
    asymmetric_ttl_snapshot_visibility
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_purge_expired_before_removes_only_expired_postgres,
    purge_expired_before_removes_only_expired
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_hydrate_after_restart_filters_expired_postgres,
    hydrate_after_restart_filters_expired
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_observation_list_is_sorted_postgres,
    observation_list_is_sorted
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_cross_model_same_prefix_keeps_both_rows_postgres,
    cross_model_same_prefix_keeps_both_rows
);

#[test]
fn price_catalog_roundtrip_smoke_postgres() {
    run_postgres_scenario(
        "price_catalog_roundtrip_smoke",
        price_catalog::roundtrip_smoke,
    );
}

#[test]
fn price_catalog_put_same_payload_twice_updates_fetched_at_postgres() {
    run_postgres_scenario(
        "price_catalog_put_same_payload_twice_updates_fetched_at",
        price_catalog::put_same_payload_twice_updates_fetched_at,
    );
}

fn run_postgres_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(Arc<PostgresConformanceBackend>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or PG_URL not set");
        return;
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(scenario(Arc::new(PostgresConformanceBackend { url })))
        .unwrap_or_else(|error| panic!("{name} postgres: {error}"));
}

fn prompt_cache_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(1_700_000_000))
}

fn warmup_attempts_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(1_800_604_800))
}

fn system_clock() -> ClockHandle {
    Arc::new(SystemClock)
}

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
}

async fn concurrent_upload_returns_existed_once(url: String) -> anyhow::Result<()> {
    let backend = PostgresConformanceBackend { url };
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
        original_filename: "plugin-concurrent-upload.wasm".to_owned(),
        label: None,
        uploaded_at_unix_secs: 1_800_000_100,
        uploaded_by_admin_id: uuid::Uuid::new_v4(),
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

fn schema_name() -> String {
    format!("storage_roundtrips_{}", uuid::Uuid::new_v4().simple())
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
