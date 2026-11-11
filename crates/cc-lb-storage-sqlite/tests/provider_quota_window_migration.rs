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
            provider, snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
            capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
            stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
            header_contributing_upstreams, api_contributing_upstreams,
            max_observed_at_unix_millis, computed_at_unix_millis, policy_version
        ) VALUES ('gemini', 2, 'gemini_5h', 0.2, 0.2, 1, 1, 1, 0, 0, 0, 0, 1, 2, 2, 1)",
    )
    .execute(storage.pool())
    .await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(integrity, "ok");
    Ok(())
}

#[tokio::test]
async fn pool_history_provider_migration_preserves_legacy_anthropic() -> Result<(), Box<dyn Error>>
{
    use cc_lb_storage_api::{PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, Provider};

    let dir = tempfile::tempdir()?;
    let url = format!(
        "sqlite://{}",
        dir.path().join("pool-provider.sqlite").display()
    );
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
        .run_direct(Some(119), &mut connection, false)
        .await?;
    sqlx::query(
        "INSERT INTO pool_subscription_quota_history_v1 (
            snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
            capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
            stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
            header_contributing_upstreams, api_contributing_upstreams,
            max_observed_at_unix_millis, computed_at_unix_millis, policy_version
        ) VALUES (10, '5h', 0.3, 1.2, 4, 7, 3, 2, 1, 1, 1, 2, 9000, 11000, 7),
                 (20, '7d', NULL, 1.2, 4, 7, 3, 2, 1, 1, 1, 2, 9000, 11000, 7)",
    )
    .execute(&mut connection)
    .await?;
    connection.close().await?;
    let storage = open_sqlite(&url, Arc::new(SystemClock)).await?;
    storage.initialize().await?;
    storage.initialize().await?;
    let expected = PoolQuotaSnapshotRecord {
        provider: Provider::Anthropic,
        snapshot_at_unix_secs: 10,
        window: SubscriptionQuotaWindow::FiveHour,
        utilization: Some(0.3),
        weighted_utilization_sum: 1.2,
        capacity_ratio_sum: 4.0,
        eligible_upstreams: 7,
        contributing_upstreams: 3,
        stale_upstreams: 2,
        missing_observation_upstreams: 1,
        missing_metadata_upstreams: 1,
        header_contributing_upstreams: 1,
        api_contributing_upstreams: 2,
        max_observed_at_unix_millis: Some(9000),
        computed_at_unix_millis: 11000,
        policy_version: 7,
    };
    let windows = [
        SubscriptionQuotaWindow::FiveHour,
        SubscriptionQuotaWindow::SevenDay,
    ];
    let legacy = storage
        .list_latest_pool_quota_snapshots(Provider::Anthropic, &windows)
        .await?;
    assert_eq!(
        legacy,
        vec![
            expected.clone(),
            PoolQuotaSnapshotRecord {
                snapshot_at_unix_secs: 20,
                window: SubscriptionQuotaWindow::SevenDay,
                utilization: None,
                ..expected.clone()
            },
        ]
    );
    for provider in [Provider::Openai, Provider::Gemini] {
        assert!(
            storage
                .list_latest_pool_quota_snapshots(provider, &windows)
                .await?
                .is_empty()
        );
    }
    storage
        .record_pool_quota_snapshots(&[PoolQuotaSnapshotRecord {
            provider: Provider::Openai,
            utilization: Some(0.8),
            ..expected
        }])
        .await?;
    assert_eq!(
        storage
            .list_latest_pool_quota_snapshots(Provider::Anthropic, &windows)
            .await?,
        legacy
    );
    let index_columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_index_info('pool_subscription_quota_history_v1_window_time_idx') ORDER BY seqno",
    ).fetch_all(storage.pool()).await?;
    assert_eq!(
        index_columns,
        vec!["provider", "quota_window", "snapshot_at_unix_secs"]
    );
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(integrity, "ok");
    Ok(())
}
