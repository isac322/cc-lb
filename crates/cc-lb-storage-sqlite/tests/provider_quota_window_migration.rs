use std::{error::Error, path::Path, str::FromStr, sync::Arc};

use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    MetaStore, SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaSource,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaAggregateStore,
    UpstreamSubscriptionQuotaStore,
};
use cc_lb_storage_sqlite::open_sqlite;
use sqlx::{Connection, SqliteConnection, migrate::Migrator, sqlite::SqliteConnectOptions};
use uuid::Uuid;
#[tokio::test]
async fn quota_window_migration_preserves_rows_and_accepts_provider_windows()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let url = format!("sqlite://{}", dir.path().join("quota.sqlite").display());
    let options = SqliteConnectOptions::from_str(&url)?
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let migrator = Migrator::new(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/migrations"
    )))
    .await?;
    migrator
        .run_direct(Some(101), &mut connection, false)
        .await?;
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_checkpoints_v1 (
            upstream_id, window, source, changed_at_unix_millis, sample_id,
            semantic_fingerprint, utilization, status, resets_at_unix_secs,
            ingested_at_unix_millis
        ) VALUES ('00000000-0000-0000-0000-000000000001', '5h', 'api', 1,
                  '00000000-0000-0000-0000-000000000002', zeroblob(32), 0.1, 'allowed', 2, 1)",
    )
    .execute(&mut connection)
    .await?;
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_latest_v1 (
            upstream_id, window, source, sample_kind, observed_at_unix_millis,
            sample_id, utilization, status, resets_at_unix_secs, ingested_at_unix_millis
        ) VALUES ('00000000-0000-0000-0000-000000000001', '5h', 'api', 'sample', 1,
                  '00000000-0000-0000-0000-000000000002', 0.1, 'allowed', 2, 1)",
    )
    .execute(&mut connection)
    .await?;
    sqlx::query(
        "INSERT INTO pool_subscription_quota_history_v1 (
            snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
            capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
            stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
            header_contributing_upstreams, api_contributing_upstreams,
            max_observed_at_unix_millis, computed_at_unix_millis, policy_version
        ) VALUES (1, '5h', 0.1, 0.1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1)",
    )
    .execute(&mut connection)
    .await?;
    connection.close().await?;

    let storage = open_sqlite(&url, Arc::new(SystemClock)).await?;
    storage.initialize().await?;
    let checkpoints: i64 =
        sqlx::query_scalar("SELECT count(*) FROM upstream_subscription_quota_checkpoints_v1")
            .fetch_one(storage.pool())
            .await?;
    let latest: i64 =
        sqlx::query_scalar("SELECT count(*) FROM upstream_subscription_quota_latest_v1")
            .fetch_one(storage.pool())
            .await?;
    let history: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pool_subscription_quota_history_v1")
            .fetch_one(storage.pool())
            .await?;
    assert_eq!((checkpoints, latest, history), (1, 1, 1));

    let upstream_id = Uuid::from_u128(1);
    let legacy_latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream_id])
        .await?;
    assert_eq!(legacy_latest.len(), 1);
    assert_eq!(legacy_latest[0].credits, None);
    assert_eq!(legacy_latest[0].utilization, Some(0.1));
    assert_eq!(legacy_latest[0].source, SubscriptionQuotaSource::Api);
    let legacy_checkpoints = storage
        .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: vec![upstream_id],
            windows: vec![SubscriptionQuotaWindow::FiveHour],
            sources: vec![SubscriptionQuotaSource::Api],
            since_unix_millis: 0,
            until_unix_millis: 2,
        })
        .await?;
    assert_eq!(legacy_checkpoints.len(), 1);
    assert_eq!(legacy_checkpoints[0].utilization, Some(0.1));
    let legacy_credits: Option<String> = sqlx::query_scalar(
        "SELECT credits_json FROM upstream_subscription_quota_checkpoints_v1 WHERE upstream_id = ?",
    )
    .bind(upstream_id.to_string())
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(legacy_credits, None);

    sqlx::query(
        "INSERT INTO upstream_subscription_quota_checkpoints_v1 (
            upstream_id, window, source, changed_at_unix_millis, sample_id,
            semantic_fingerprint, utilization, status, resets_at_unix_secs,
            ingested_at_unix_millis
        ) VALUES ('u', 'gemini_5h', 'api', 2, 's2', zeroblob(32), 0.2, 'allowed', 3, 2)",
    )
    .execute(storage.pool())
    .await?;
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_latest_v1 (
            upstream_id, window, source, sample_kind, observed_at_unix_millis,
            sample_id, utilization, status, resets_at_unix_secs, ingested_at_unix_millis
        ) VALUES ('u', 'gemini_5h', 'api', 'sample', 2, 's2', 0.2, 'allowed', 3, 2)",
    )
    .execute(storage.pool())
    .await?;
    sqlx::query(
        "INSERT INTO pool_subscription_quota_history_v1 (
            snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
            capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
            stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
            header_contributing_upstreams, api_contributing_upstreams,
            max_observed_at_unix_millis, computed_at_unix_millis, policy_version
        ) VALUES (2, 'gemini_5h', 0.2, 0.2, 1, 1, 1, 0, 0, 0, 0, 1, 2, 2, 1)",
    )
    .execute(storage.pool())
    .await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(integrity, "ok");
    Ok(())
}
