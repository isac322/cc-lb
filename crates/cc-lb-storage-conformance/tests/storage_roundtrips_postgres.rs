#![cfg(feature = "postgres")]

use crate::request_event_quota_support;

#[path = "support/request_event_quota_postgres.rs"]
mod request_event_quota_postgres;

request_event_quota_postgres::define_request_event_quota_postgres_tests!();

use std::{
    future::Future,
    str::FromStr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use cc_lb_clock::{ClockHandle, SystemClock, TestClock};
// `RequestEventStore` methods are in scope through the quota macro's
// `use cc_lb_storage_api::RequestEventStore as _;` above.
use cc_lb_storage_api::{
    BackendKind, ChangeChannel, MetaStore, PluginRegistryStore, RequestEvent,
    RequestEventExportPage, RequestEventPayloadScan, UpstreamCreate, UpstreamKind, UpstreamStore,
    UpstreamUpdate, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_conformance::{
    harness::{ConformanceBackend, ConformanceFixture},
    scenarios::{
        anthropic_compatibility_kv_store, cache_keepalive_session_reads,
        organization_metadata_store, plan_tier_store, plugin_registry_store,
        pool_quota_history_store, price_catalog, principal_store, prompt_cache_observation_store,
        request_event_list, request_event_operator, request_event_principal_costs,
        storage_roundtrips, storage_roundtrips_cache_split, storage_roundtrips_latency_stages,
        storage_roundtrips_upstream_diagnostics, upstream_api_key_store, upstream_rate_limit_store,
        upstream_subscription_metadata_store, upstream_subscription_quota_store,
        warmup_attempts_store,
    },
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgListener, PgPoolOptions},
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

static PRICE_CATALOG_SCENARIO_LOCK: Mutex<()> = Mutex::new(());

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
            .initialize()
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

    async fn wait_for_events_visible_for_rollup(
        &self,
        storage: &Self::Storage,
    ) -> anyhow::Result<()> {
        for _ in 0..600 {
            let eligible = sqlx::query_scalar::<_, bool>(
                "SELECT NOT EXISTS ( \
                     SELECT 1 FROM request_events_v1 \
                     WHERE COALESCE(tx_id, '0'::xid8) >= pg_snapshot_xmin(pg_current_snapshot()) \
                 )",
            )
            .fetch_one(storage.pool())
            .await?;

            if eligible {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        anyhow::bail!(
            "timed out waiting for seeded request events to fall below the Postgres snapshot xmin horizon"
        );
    }

    async fn execute_raw_sql(
        &self,
        storage: &Self::Storage,
        sql: &'static str,
    ) -> anyhow::Result<u64> {
        Ok(sqlx::query(sql)
            .execute(storage.pool())
            .await?
            .rows_affected())
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
        .execute(&fixture.pool)
        .await?;
        anyhow::ensure!(result.rows_affected() == 1, "expected one corrupted row");
        Ok(())
    }
}

#[test]
fn storage_roundtrips_postgres() {
    run_postgres_scenario("storage_roundtrips", storage_roundtrips::run_all);
}

#[test]
fn config_draft_optimistic_revision_postgres() {
    run_postgres_scenario(
        "config_draft_optimistic_revision",
        cc_lb_storage_conformance::scenarios::revisioning_meta::config_draft_optimistic_revision,
    );
}

#[test]
fn config_history_cap_50_postgres() {
    run_postgres_scenario(
        "config_history_cap_50",
        cc_lb_storage_conformance::scenarios::revisioning_meta::config_history_cap_50,
    );
}

#[test]
fn config_last_validated_revision_postgres() {
    run_postgres_scenario(
        "config_last_validated_revision",
        cc_lb_storage_conformance::scenarios::revisioning_meta::config_last_validated_revision,
    );
}

#[test]
fn meta_compare_and_put_postgres() {
    run_postgres_scenario(
        "meta_compare_and_put",
        cc_lb_storage_conformance::scenarios::revisioning_meta::meta_compare_and_put,
    );
}

#[test]
fn meta_backend_kind_stamp_postgres() {
    run_postgres_scenario(
        "meta_backend_kind_stamp",
        cc_lb_storage_conformance::scenarios::revisioning_meta::meta_backend_kind_stamp,
    );
}

#[test]
fn pool_quota_history_fable_roundtrip_postgres() {
    run_postgres_scenario(
        "pool_quota_history_fable_roundtrip",
        pool_quota_history_store::fable_roundtrip,
    );
}

#[test]
fn request_event_cache_split_round_trip_postgres() {
    run_postgres_scenario(
        "request_event_cache_split_round_trip",
        storage_roundtrips_cache_split::request_event_cache_split_round_trip,
    );
}

#[test]
fn request_event_upstream_diagnostics_round_trip_postgres() {
    run_postgres_scenario(
        "request_event_upstream_diagnostics_round_trip",
        storage_roundtrips_upstream_diagnostics::request_event_upstream_diagnostics_round_trip,
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
fn request_event_list_projects_rows_and_preserves_detail_postgres() {
    run_postgres_scenario(
        "request_event_list_projects_rows_and_preserves_detail",
        request_event_list::request_event_list_projects_rows_and_preserves_detail,
    );
}

#[test]
fn request_event_list_model_filter_matches_case_insensitive_prefix_postgres() {
    run_postgres_scenario(
        "request_event_list_model_filter_matches_case_insensitive_prefix",
        request_event_list::request_event_list_model_filter_matches_case_insensitive_prefix,
    );
}

#[test]
fn request_event_operator_methods_postgres() {
    run_postgres_scenario(
        "request_event_operator_methods",
        request_event_operator::request_event_operator_methods,
    );
}

#[test]
fn verify_schema_current_detects_drift_postgres() {
    run_postgres_scenario(
        "verify_schema_current_detects_drift",
        request_event_operator::verify_schema_current_detects_drift,
    );
}

#[test]
fn request_event_export_resume_never_skips_late_lower_seq_postgres() {
    run_postgres_scenario(
        "request_event_export_resume_never_skips_late_lower_seq",
        request_event_export_resume_never_skips_late_lower_seq,
    );
}

async fn insert_raw_request_event(
    conn: &mut sqlx::PgConnection,
    event_id: &str,
    ts_ms: u64,
) -> anyhow::Result<u64> {
    let payload = serde_json::to_string(&RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: format!("{event_id}-request"),
        event_id: Some(event_id.to_owned()),
        status: 200,
        ..Default::default()
    })?;
    let seq = sqlx::query_scalar::<_, i64>(
        "INSERT INTO request_events_v1 (ts, event_id, list_ts_ms, list_event_key, payload) \
         VALUES (to_timestamp($1::double precision / 1000.0), $2, $1, $2, convert_to($3, 'UTF8')) \
         RETURNING seq",
    )
    .bind(i64::try_from(ts_ms)?)
    .bind(event_id)
    .bind(payload)
    .fetch_one(&mut *conn)
    .await?;
    Ok(u64::try_from(seq)?)
}

/// Regression for the export seq gap: transaction B takes its xid first,
/// then transaction A takes a later xid and the LOWER seq, then B commits
/// while A is still open. The xmin guard alone exposes B (its xid is below
/// every open xid) but not A, so a pager that resumes from `seq > B` skips A
/// forever. Clamping to `request_event_stable_seq_ceiling` waits for A.
async fn request_event_export_resume_never_skips_late_lower_seq(
    backend: Arc<PostgresConformanceBackend>,
) -> anyhow::Result<()> {
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result = async {
        let storage = fixture.storage();
        let ts_ms = 1_728_000_000_000_u64;

        let mut tx_b = storage.pool().begin().await?;
        sqlx::query_scalar::<_, String>("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *tx_b)
            .await?;
        let mut tx_a = storage.pool().begin().await?;
        let seq_a = insert_raw_request_event(&mut tx_a, "gap-a", ts_ms).await?;
        let seq_b = insert_raw_request_event(&mut tx_b, "gap-b", ts_ms + 1).await?;
        anyhow::ensure!(
            seq_a < seq_b,
            "A must hold the lower seq: {seq_a} vs {seq_b}"
        );
        tx_b.commit().await?;

        // The gap: under the xmin guard alone, B is exported while A is not.
        let mut exposed = Vec::new();
        for _ in 0..200 {
            exposed = storage
                .export_request_events(&RequestEventExportPage {
                    after_seq: 0,
                    until_seq: seq_b,
                    from_ts_ms: None,
                    to_ts_ms: None,
                    limit: 10,
                })
                .await?
                .into_iter()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>();
            if !exposed.is_empty() {
                break;
            }
            // A sibling test's open transaction can hold xmin below B.
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        anyhow::ensure!(
            exposed == [seq_b],
            "precondition: the xmin guard exposes only B while A is open, got {exposed:?}"
        );

        // The ceiling must wait for A instead of returning while it is open.
        let ceiling_storage = fixture.storage();
        let ceiling = tokio::spawn(async move {
            ceiling_storage
                .request_event_stable_seq_ceiling(std::time::Duration::from_secs(30))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        anyhow::ensure!(
            !ceiling.is_finished(),
            "stable ceiling returned while a lower seq was still uncommitted"
        );
        tx_a.commit().await?;
        let ceiling = ceiling.await??;
        anyhow::ensure!(
            ceiling == seq_b,
            "ceiling {ceiling} must be B's seq {seq_b}"
        );

        // Resumable paging (limit 1) up to the ceiling sees both rows.
        let mut after_seq = 0;
        let mut exported = Vec::new();
        loop {
            let page = storage
                .export_request_events(&RequestEventExportPage {
                    after_seq,
                    until_seq: ceiling,
                    from_ts_ms: None,
                    to_ts_ms: None,
                    limit: 1,
                })
                .await?;
            let Some((seq, _)) = page.last() else {
                break;
            };
            after_seq = *seq;
            exported.push(*seq);
        }
        anyhow::ensure!(
            exported == [seq_a, seq_b],
            "export clamped to the stable ceiling must include the late lower seq: {exported:?}"
        );

        // Timeout: an open transaction older than the snapshot fails the call.
        let mut blocker = storage.pool().begin().await?;
        sqlx::query_scalar::<_, String>("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *blocker)
            .await?;
        let timed_out = storage
            .request_event_stable_seq_ceiling(std::time::Duration::from_millis(100))
            .await;
        blocker.rollback().await?;
        anyhow::ensure!(
            matches!(
                timed_out,
                Err(cc_lb_storage_api::StorageError::Unavailable { .. })
            ),
            "an unsettled ceiling must time out as Unavailable, got {timed_out:?}"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

#[test]
fn request_event_export_respects_snapshot_xmin_postgres() {
    run_postgres_scenario(
        "request_event_export_respects_snapshot_xmin",
        request_event_export_respects_snapshot_xmin,
    );
}

/// QA 11 (export xmin guard): a row committed after a still-running writer
/// took its xid stays out of the export until that writer finishes, exactly
/// like `query_request_events_between_cursors`. The raw payload scan has no
/// such guard.
async fn request_event_export_respects_snapshot_xmin(
    backend: Arc<PostgresConformanceBackend>,
) -> anyhow::Result<()> {
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result = async {
        let storage = fixture.storage();
        let mut in_flight_writer = storage.pool().begin().await?;
        sqlx::query_scalar::<_, String>("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *in_flight_writer)
            .await?;

        let ts_ms = 1_728_000_000_000_u64;
        let seq = storage
            .append_request_event(&RequestEvent {
                ts: ts_ms / 1_000,
                ts_ms: Some(ts_ms),
                request_id: "xmin-guard-request".to_owned(),
                event_id: Some("xmin-guard-event".to_owned()),
                status: 200,
                ..Default::default()
            })
            .await?;
        let page = RequestEventExportPage {
            after_seq: 0,
            until_seq: seq,
            from_ts_ms: None,
            to_ts_ms: None,
            limit: 10,
        };
        let hidden = storage.export_request_events(&page).await?;
        anyhow::ensure!(
            hidden.is_empty(),
            "export must hide rows above the snapshot xmin horizon, got {} rows",
            hidden.len()
        );
        let scanned = storage
            .scan_request_event_payloads(&RequestEventPayloadScan {
                after_seq: 0,
                until_seq: seq,
                limit: 10,
            })
            .await?;
        anyhow::ensure!(
            scanned.iter().map(|(row_seq, _)| *row_seq).eq([seq]),
            "payload scan reads committed rows without the xmin guard"
        );

        in_flight_writer.rollback().await?;
        backend.wait_for_events_visible_for_rollup(&storage).await?;
        let visible = storage.export_request_events(&page).await?;
        anyhow::ensure!(
            visible.iter().map(|(row_seq, _)| *row_seq).eq([seq]),
            "export must return the row once the horizon passes it"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

#[test]
fn request_event_principal_cost_components_postgres() {
    run_postgres_scenario(
        "request_event_principal_cost_components",
        request_event_principal_costs::principal_cost_components_aggregate_without_fabrication,
    );
}

#[test]
fn cache_keepalive_batch_turn_reads_match_per_session_postgres() {
    run_postgres_scenario(
        "cache_keepalive_batch_turn_reads_match_per_session",
        cache_keepalive_session_reads::batch_turn_reads_match_canonical_per_session_reads,
    );
}

#[test]
fn plugin_registry_store_postgres() {
    run_postgres_scenario("plugin_registry_store", plugin_registry_store::run_all);
}

#[test]
fn plugin_registry_refcount_counts_chain_and_warmup_references_postgres() {
    run_postgres_scenario(
        "refcount_counts_chain_and_warmup_references",
        plugin_registry_store::refcount_counts_chain_and_warmup_references,
    );
}

#[test]
fn plugin_registry_replace_wasm_entry_preserves_id_and_references_postgres() {
    run_postgres_scenario(
        "replace_wasm_entry_preserves_id_and_references",
        plugin_registry_store::replace_wasm_entry_preserves_id_and_references,
    );
}

#[test]
fn plugin_registry_replace_wasm_entry_with_stale_revision_conflicts_postgres() {
    run_postgres_scenario(
        "replace_wasm_entry_with_stale_revision_conflicts",
        plugin_registry_store::replace_wasm_entry_with_stale_revision_conflicts,
    );
}

#[test]
fn plugin_registry_list_registry_references_returns_chain_and_warmup_postgres() {
    run_postgres_scenario(
        "list_registry_references_returns_chain_and_warmup",
        plugin_registry_store::list_registry_references_returns_chain_and_warmup,
    );
}

#[test]
fn plugin_registry_cascade_delete_registry_entry_removes_chain_warmup_and_blob_postgres() {
    run_postgres_scenario(
        "cascade_delete_registry_entry_removes_chain_warmup_and_blob",
        plugin_registry_store::cascade_delete_registry_entry_removes_chain_warmup_and_blob,
    );
}

#[test]
fn plugin_registry_cascade_delete_registry_entry_rejects_changed_fingerprint_postgres() {
    run_postgres_scenario(
        "cascade_delete_registry_entry_rejects_changed_fingerprint",
        plugin_registry_store::cascade_delete_registry_entry_rejects_changed_fingerprint,
    );
}

#[test]
fn plugin_registry_registry_by_id_returns_seeded_builtin_subscription_preference_postgres() {
    run_postgres_scenario(
        "registry_by_id_returns_seeded_builtin_subscription_preference",
        plugin_registry_store::registry_by_id_returns_seeded_builtin_subscription_preference,
    );
}

#[test]
fn plugin_registry_created_principal_has_builtin_subscription_preference_chain_entry_postgres() {
    run_postgres_scenario(
        "created_principal_has_builtin_subscription_preference_chain_entry",
        plugin_registry_store::created_principal_has_builtin_subscription_preference_chain_entry,
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
fn upstream_subscription_quota_absent_replaces_latest_without_erasing_history_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_absent_replaces_latest_without_erasing_history",
        upstream_subscription_quota_store::absent_replaces_latest_without_erasing_history,
    );
}

#[test]
fn upstream_subscription_quota_absent_is_idempotent_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_absent_is_idempotent",
        upstream_subscription_quota_store::absent_is_idempotent,
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
fn upstream_subscription_quota_slim_checkpoints_select_per_source_left_anchors_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_slim_checkpoints_select_per_source_left_anchors",
        upstream_subscription_quota_store::slim_checkpoints_select_per_source_left_anchors,
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
fn upstream_subscription_quota_aggregate_store_postgres() {
    run_postgres_scenario(
        "upstream_subscription_quota_aggregate_store",
        upstream_subscription_quota_store::aggregate_store,
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
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_upsert_stale_write_does_not_regress_postgres,
    upsert_stale_write_does_not_regress
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_upsert_newer_write_overwrites_postgres,
    upsert_newer_write_overwrites
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_upsert_tiebreak_keeps_coherent_winner_postgres,
    upsert_tiebreak_keeps_coherent_winner
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_list_active_for_candidates_isolates_candidates_postgres,
    list_active_for_candidates_isolates_candidates
);
prompt_cache_observation_postgres_test!(
    prompt_cache_observation_concurrent_upserts_keep_freshest_postgres,
    concurrent_upserts_keep_freshest
);

#[test]
fn price_catalog_roundtrip_smoke_postgres() {
    run_postgres_price_scenario(
        "price_catalog_roundtrip_smoke",
        price_catalog::roundtrip_smoke,
    );
}

#[test]
fn price_catalog_put_same_payload_twice_updates_fetched_at_postgres() {
    run_postgres_price_scenario(
        "price_catalog_put_same_payload_twice_updates_fetched_at",
        price_catalog::put_same_payload_twice_updates_fetched_at,
    );
}

#[test]
fn price_catalog_corrupted_payload_hash_is_rejected_postgres() {
    run_postgres_price_scenario(
        "price_catalog_corrupted_payload_hash_is_rejected",
        price_catalog::corrupted_payload_hash_is_rejected,
    );
}

macro_rules! upstream_api_key_store_postgres_test {
    ($test_name:ident, $scenario:ident) => {
        #[test]
        fn $test_name() {
            run_postgres_scenario(stringify!($scenario), upstream_api_key_store::$scenario);
        }
    };
}

upstream_api_key_store_postgres_test!(
    upstream_api_key_sto05_upd08_workspace_id_tri_state_postgres,
    sto05_upd08_workspace_id_tri_state_bumps_revision
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_sto04_details_crud_postgres,
    sto04_details_create_get_list_update_conflict
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_acc08_capabilities_json_roundtrip_postgres,
    acc08_capabilities_json_roundtrip_all_values
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc07_column_scoped_writes_postgres,
    conc07_access_and_poll_writes_are_column_scoped
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc07_concurrent_writes_survive_postgres,
    conc07_concurrent_access_and_poll_writes_both_survive
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc07_same_second_recheck_postgres,
    conc07_same_second_recheck_rejects_stale_demotion
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_pol07_failure_counter_postgres,
    pol07_failure_counter_and_schedule_persist
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_pol05_bucket_upsert_postgres,
    pol05_bucket_upsert_is_idempotent_and_latest_wins
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_sto04_poll_candidates_postgres,
    sto04_poll_candidate_query_filters
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_sto04_poll_candidates_rotate_postgres,
    sto04_poll_candidates_rotate_past_the_limit
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_acc06_delete_semantics_postgres,
    acc06_soft_and_hard_delete_semantics
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_sto06_time_roundtrip_postgres,
    sto06_unix_seconds_roundtrip_exactly
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_prune_bounded_postgres,
    prune_upstream_api_usage_is_bounded
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_reset_poll_state_postgres,
    reset_poll_state_keeps_usage_rows
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc_poll_claim_two_claimers_postgres,
    conc_poll_claim_two_claimers_one_wins
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc_poll_claim_stale_completion_postgres,
    conc_poll_claim_stale_completion_rejected
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc_poll_claim_disabled_postgres,
    conc_poll_claim_and_completion_reject_disabled_upstream
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc_poll_claim_lease_expiry_postgres,
    conc_poll_claim_lease_expiry_is_reclaimable
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_pol_completion_never_shortens_retry_after_postgres,
    pol_completion_never_shortens_retry_after
);
upstream_api_key_store_postgres_test!(
    upstream_api_key_conc_access_write_releases_poll_claim_postgres,
    conc_access_write_releases_poll_claim
);

/// STO-05: a workspace-only update bumps `spec_revision` and publishes the
/// upstream NOTIFY that drives Postgres rebinds.
#[test]
fn upstream_api_key_sto05_workspace_update_notifies_postgres() {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or PG_URL not set");
        return;
    };
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let backend = PostgresConformanceBackend { url };
            let fixture = backend.create_fixture().await?;
            let result = workspace_update_notifies(&backend, &fixture).await;
            let teardown = backend.teardown(fixture).await;
            result?;
            teardown
        })
        .unwrap_or_else(|error| panic!("sto05 workspace notify postgres: {error}"));
}

async fn workspace_update_notifies(
    backend: &PostgresConformanceBackend,
    fixture: &PostgresFixture,
) -> anyhow::Result<()> {
    let storage = backend.open(fixture).await?;
    let created = UpstreamStore::create(
        &storage,
        UpstreamCreate {
            id: uuid::Uuid::new_v4(),
            name: "sto05-notify".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![1, 2, 3]),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await?;
    let mut listener = PgListener::connect_with(&fixture.pool).await?;
    listener
        .listen(ChangeChannel::Upstream.postgres_channel())
        .await?;
    let updated = UpstreamStore::update(
        &storage,
        created.id,
        created.revision,
        UpstreamUpdate {
            workspace_id: Some(Some("wrkspc_notify".to_owned())),
            ..UpstreamUpdate::default()
        },
    )
    .await?;
    anyhow::ensure!(
        updated.revision == created.revision + 1,
        "workspace update bumps spec_revision"
    );
    // The channel is database-wide; sibling tests may notify other ids.
    let expected = created.id.to_string();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let notification = listener.recv().await?;
            if notification.payload() == expected {
                return anyhow::Ok(());
            }
        }
    })
    .await??;
    Ok(())
}

/// One column, normalized across backends: SQLite `INTEGER` covers Postgres
/// `bigint`/`integer`/`boolean`, and `TEXT` covers `text`/`uuid`.
#[derive(Debug, PartialEq, Eq)]
struct ColumnShape {
    name: String,
    family: String,
    not_null: bool,
    default: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct TableShape {
    columns: Vec<ColumnShape>,
    primary_key: Vec<String>,
    /// `(column, referenced table, referenced column, on delete)`.
    foreign_keys: Vec<(String, String, String, String)>,
    indexes: Vec<String>,
}

const API_KEY_TABLES: [&str; 2] = ["upstream_api_key_details_v1", "upstream_api_usage_v1"];

/// STO-03: a fresh install gives both backends the same columns,
/// nullability, defaults, primary keys, and cascading foreign keys.
#[test]
fn upstream_api_key_sto03_fresh_schema_parity_postgres() {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or PG_URL not set");
        return;
    };
    Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let backend = PostgresConformanceBackend { url };
            let fixture = backend.create_fixture().await?;
            let result = fresh_schema_parity(&fixture.pool).await;
            let teardown = backend.teardown(fixture).await;
            result?;
            teardown
        })
        .unwrap_or_else(|error| panic!("sto03 schema parity: {error}"));
}

async fn fresh_schema_parity(postgres: &PgPool) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_url = format!("sqlite://{}", dir.path().join("fresh.sqlite").display());
    let sqlite = cc_lb_storage_sqlite::open_sqlite(&sqlite_url, system_clock()).await?;
    sqlite.initialize().await?;
    let sqlite_pool = sqlite.pool();

    for table in API_KEY_TABLES {
        let sqlite_shape = sqlite_table_shape(sqlite_pool, table).await?;
        let postgres_shape = postgres_table_shape(postgres, table).await?;
        anyhow::ensure!(
            sqlite_shape == postgres_shape,
            "{table} differs:\nsqlite   {sqlite_shape:?}\npostgres {postgres_shape:?}"
        );
        anyhow::ensure!(
            sqlite_shape.foreign_keys
                == [(
                    "upstream_id".to_owned(),
                    "upstream_spec_v1".to_owned(),
                    "id".to_owned(),
                    "CASCADE".to_owned()
                )],
            "{table} cascades from upstream_spec_v1"
        );
    }
    let sqlite_workspace = sqlite_table_shape(sqlite_pool, "upstream_spec_v1")
        .await?
        .columns
        .into_iter()
        .find(|column| column.name == "workspace_id");
    let postgres_workspace = postgres_table_shape(postgres, "upstream_spec_v1")
        .await?
        .columns
        .into_iter()
        .find(|column| column.name == "workspace_id");
    let expected = ColumnShape {
        name: "workspace_id".to_owned(),
        family: "text".to_owned(),
        not_null: false,
        default: None,
    };
    anyhow::ensure!(
        sqlite_workspace.as_ref() == Some(&expected) && postgres_workspace == sqlite_workspace,
        "workspace_id: sqlite {sqlite_workspace:?} postgres {postgres_workspace:?}"
    );
    Ok(())
}

async fn sqlite_table_shape(pool: &sqlx::SqlitePool, table: &str) -> anyhow::Result<TableShape> {
    use sqlx::Row as _;

    let rows = sqlx::query(
        "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?) ORDER BY cid",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let mut columns = Vec::with_capacity(rows.len());
    let mut primary_key = Vec::new();
    for row in rows {
        let name: String = row.try_get("name")?;
        let declared: String = row.try_get("type")?;
        let pk: i64 = row.try_get("pk")?;
        if pk > 0 {
            primary_key.push((pk, name.clone()));
        }
        columns.push(ColumnShape {
            family: type_family(&declared),
            // A single-column TEXT PRIMARY KEY is NOT NULL in meaning.
            not_null: row.try_get::<i64, _>("notnull")? != 0 || pk > 0,
            default: row
                .try_get::<Option<String>, _>("dflt_value")?
                .map(|value| normalize_default(&value)),
            name,
        });
    }
    primary_key.sort();
    let foreign_keys = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT \"from\", \"table\", \"to\", on_delete FROM pragma_foreign_key_list(?) ORDER BY \"from\"",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    Ok(TableShape {
        columns,
        primary_key: primary_key.into_iter().map(|(_, name)| name).collect(),
        foreign_keys,
        indexes,
    })
}

async fn postgres_table_shape(pool: &PgPool, table: &str) -> anyhow::Result<TableShape> {
    let rows = sqlx::query_as::<_, (String, String, String, Option<String>)>(
        "SELECT column_name::text, data_type::text, is_nullable::text, column_default::text \
         FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = $1 \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let columns = rows
        .into_iter()
        .map(|(name, data_type, nullable, default)| {
            Ok(ColumnShape {
                family: type_family(&data_type),
                not_null: nullable == "NO",
                default: default.map(|value| normalize_default(&value)),
                name,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let primary_key = sqlx::query_scalar::<_, String>(
        "SELECT a.attname::text \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = (current_schema() || '.' || $1)::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey::int2[], a.attnum)",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let foreign_keys = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT kcu.column_name::text, ccu.table_name::text, ccu.column_name::text, rc.delete_rule::text \
         FROM information_schema.referential_constraints rc \
         JOIN information_schema.key_column_usage kcu \
           ON kcu.constraint_name = rc.constraint_name AND kcu.constraint_schema = rc.constraint_schema \
         JOIN information_schema.constraint_column_usage ccu \
           ON ccu.constraint_name = rc.unique_constraint_name AND ccu.constraint_schema = rc.unique_constraint_schema \
         WHERE kcu.table_schema = current_schema() AND kcu.table_name = $1 \
         ORDER BY kcu.column_name",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname::text FROM pg_indexes \
         WHERE schemaname = current_schema() AND tablename = $1 \
           AND indexname NOT IN ( \
               SELECT conname FROM pg_constraint \
               WHERE conrelid = (current_schema() || '.' || $1)::regclass AND contype = 'p') \
         ORDER BY indexname",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    Ok(TableShape {
        columns,
        primary_key,
        foreign_keys,
        indexes,
    })
}

/// Unmapped types keep their declared spelling, so they only match the same
/// spelling on the other backend.
fn type_family(declared: &str) -> String {
    match declared.to_ascii_lowercase().as_str() {
        "integer" | "bigint" | "boolean" => "int".to_owned(),
        "text" | "uuid" => "text".to_owned(),
        other => other.to_owned(),
    }
}

fn normalize_default(value: &str) -> String {
    value.trim_end_matches("::text").to_owned()
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
    };
    let input = WasmRegistryEntryInput {
        name: "plugin-concurrent-upload".to_owned(),
        version: None,
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
