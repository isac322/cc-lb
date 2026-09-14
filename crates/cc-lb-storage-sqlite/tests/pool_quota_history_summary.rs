use cc_lb_storage_api::{PoolQuotaHistoryStore, SubscriptionQuotaWindow};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::runtime::Builder;

#[test]
fn t3__summary_queries_do_not_require_contributors_json_column() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime builds")
        .block_on(async {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .expect("sqlite memory database opens");
            create_minimal_pool_history_table(&pool).await;
            insert_pool_history_summary_row(&pool).await;

            let storage = SqliteStorage::new(pool, cc_lb_testkit::fixed_clock(1_700_000_000));

            let latest = PoolQuotaHistoryStore::list_latest_pool_quota_snapshot_summaries(
                &storage,
                &[SubscriptionQuotaWindow::FiveHour],
            )
            .await
            .expect("latest summary query succeeds without contributors_json");
            let range = PoolQuotaHistoryStore::list_pool_quota_snapshot_summaries_in_range(
                &storage,
                &[SubscriptionQuotaWindow::FiveHour],
                0,
                20,
            )
            .await
            .expect("range summary query succeeds without contributors_json");

            assert_eq!(latest.len(), 1);
            assert_eq!(range.len(), 1);
            assert_eq!(latest[0].contributing_upstreams, 2);
            assert_eq!(range[0].max_observed_at_unix_millis, Some(9_000));
        });
}

async fn create_minimal_pool_history_table(pool: &sqlx::SqlitePool) {
    sqlx::query(
        r#"CREATE TABLE pool_subscription_quota_history_v1 (
            snapshot_at_unix_secs INTEGER NOT NULL,
            quota_window TEXT NOT NULL,
            utilization REAL,
            weighted_utilization_sum REAL NOT NULL,
            capacity_ratio_sum REAL NOT NULL,
            eligible_upstreams INTEGER NOT NULL,
            contributing_upstreams INTEGER NOT NULL,
            stale_upstreams INTEGER NOT NULL,
            missing_observation_upstreams INTEGER NOT NULL,
            missing_metadata_upstreams INTEGER NOT NULL,
            header_contributing_upstreams INTEGER NOT NULL,
            api_contributing_upstreams INTEGER NOT NULL,
            max_observed_at_unix_millis INTEGER,
            computed_at_unix_millis INTEGER NOT NULL,
            policy_version INTEGER NOT NULL,
            PRIMARY KEY(snapshot_at_unix_secs, quota_window)
        )"#,
    )
    .execute(pool)
    .await
    .expect("minimal pool history table exists");
}

async fn insert_pool_history_summary_row(pool: &sqlx::SqlitePool) {
    sqlx::query(
        r#"INSERT INTO pool_subscription_quota_history_v1 (
            snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
            capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
            stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
            header_contributing_upstreams, api_contributing_upstreams,
            max_observed_at_unix_millis, computed_at_unix_millis, policy_version
        ) VALUES (10, '5h', 0.25, 0.25, 1.0, 3, 2, 1, 0, 0, 1, 1, 9_000, 10_000, 1)"#,
    )
    .execute(pool)
    .await
    .expect("pool history summary row inserts");
}
