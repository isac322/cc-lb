use std::sync::Arc;

use cc_lb_storage_api::{
    BackendKind, MetaStore, MetadataTierMappingOverrideRecord, PlanTierRatioRecord, PlanTierStore,
    TierResolutionSource, UpstreamPlanTierRecord,
};
use sqlx::AssertSqlSafe;
use uuid::Uuid;

use crate::SqliteStorage;

pub(super) async fn migrated_storage() -> (tempfile::TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("plan-tiers.sqlite").display()
    );
    let storage = crate::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    (temp_dir, storage)
}

pub(super) async fn exercise_plan_tier_store(storage: &SqliteStorage) {
    let seeded = storage
        .list_current_plan_tier_ratios()
        .await
        .expect("list seeded ratios");
    assert_eq!(seeded.len(), 5);
    assert!(
        seeded
            .iter()
            .any(|record| record.tier_key == "max_5x" && record.pro_relative_ratio == 5.0)
    );

    let changed_ratio = PlanTierRatioRecord {
        tier_key: "max_5x".to_owned(),
        pro_relative_ratio: 4.5,
        effective_from_unix_millis: 1_000,
        effective_to_unix_millis: None,
        provenance: "test".to_owned(),
        created_at_unix_millis: 1_000,
    };
    storage
        .upsert_plan_tier_ratio(&changed_ratio)
        .await
        .expect("change ratio");
    let current = storage
        .list_current_plan_tier_ratios()
        .await
        .expect("current ratios");
    let before_change = storage
        .list_plan_tier_ratios_as_of(999)
        .await
        .expect("as-of ratios");
    assert_eq!(
        current
            .iter()
            .find(|record| record.tier_key == "max_5x")
            .expect("current max_5x")
            .pro_relative_ratio,
        4.5
    );
    assert_eq!(
        before_change
            .iter()
            .find(|record| record.tier_key == "max_5x")
            .expect("old max_5x")
            .pro_relative_ratio,
        5.0
    );
    let ratio_rows = table_count(storage.pool(), "plan_tier_ratio_history_v1").await;
    storage
        .upsert_plan_tier_ratio(&changed_ratio)
        .await
        .expect("idempotent ratio");
    assert_eq!(
        table_count(storage.pool(), "plan_tier_ratio_history_v1").await,
        ratio_rows
    );

    let override_record = MetadataTierMappingOverrideRecord {
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
        tier_key: "pro".to_owned(),
        effective_from_unix_millis: 2_000,
        effective_to_unix_millis: None,
        provenance: "test".to_owned(),
        created_at_unix_millis: 2_000,
    };
    storage
        .upsert_metadata_tier_override(&override_record)
        .await
        .expect("insert override");
    assert_eq!(
        storage
            .list_current_metadata_tier_overrides()
            .await
            .expect("current overrides"),
        vec![override_record]
    );
    assert_eq!(
        raw_override_key(storage.pool()).await,
        (String::new(), String::new(), String::new())
    );

    let upstream_id = Uuid::from_u128(42);
    let upstream = upstream_record(
        upstream_id,
        Some("max_5x"),
        TierResolutionSource::Builtin,
        3_000,
    );
    storage
        .append_upstream_plan_tier(&upstream)
        .await
        .expect("append upstream");
    let upstream_rows = table_count(storage.pool(), "upstream_plan_tier_history_v1").await;
    storage
        .append_upstream_plan_tier(&upstream)
        .await
        .expect("idempotent upstream");
    assert_eq!(
        table_count(storage.pool(), "upstream_plan_tier_history_v1").await,
        upstream_rows
    );

    let unknown = upstream_record(upstream_id, None, TierResolutionSource::Unknown, 4_000);
    storage
        .append_upstream_plan_tier(&unknown)
        .await
        .expect("append unknown upstream");
    assert_eq!(
        storage
            .list_current_upstream_plan_tiers()
            .await
            .expect("current upstreams"),
        vec![unknown]
    );
}

pub(super) fn upstream_record(
    upstream_id: Uuid,
    tier_key: Option<&str>,
    resolution_source: TierResolutionSource,
    effective_from_unix_millis: i64,
) -> UpstreamPlanTierRecord {
    UpstreamPlanTierRecord {
        upstream_id,
        organization_uuid: Some("org-uuid".to_owned()),
        organization_type: Some("team".to_owned()),
        rate_limit_tier: Some("tier".to_owned()),
        seat_tier: None,
        tier_key: tier_key.map(str::to_owned),
        resolution_source,
        resolved_ratio_snapshot: tier_key.map(|_| 4.5),
        observed_at_unix_millis: effective_from_unix_millis,
        effective_from_unix_millis,
        effective_to_unix_millis: None,
        provenance: "test".to_owned(),
        created_at_unix_millis: effective_from_unix_millis,
    }
}

pub(super) async fn table_count(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    sqlx::query_scalar::<_, i64>(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .expect("count table")
}

pub(super) async fn raw_override_key(pool: &sqlx::SqlitePool) -> (String, String, String) {
    sqlx::query_as("SELECT organization_type, rate_limit_tier, seat_tier FROM metadata_tier_mapping_override_v1 WHERE tier_key = 'pro'")
        .fetch_one(pool)
        .await
        .expect("raw override key")
}
