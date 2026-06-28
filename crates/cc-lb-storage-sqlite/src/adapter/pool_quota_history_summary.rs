use cc_lb_storage_api::{
    PoolQuotaSnapshotSummaryRecord, StorageError, StorageResult, SubscriptionQuotaWindow,
};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

pub(super) async fn list_latest(
    storage: &SqliteStorage,
    windows: &[SubscriptionQuotaWindow],
) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
    if windows.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(windows.len());
    for window in windows {
        let row = sqlx::query(
            r#"SELECT snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
                      capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                      stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                      header_contributing_upstreams, api_contributing_upstreams,
                      max_observed_at_unix_millis, computed_at_unix_millis, policy_version
               FROM pool_subscription_quota_history_v1
              WHERE quota_window = ?
           ORDER BY snapshot_at_unix_secs DESC
              LIMIT 1"#,
        )
        .bind(window.as_str())
        .fetch_optional(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
        if let Some(row) = row {
            out.push(row_to_summary(row)?);
        }
    }
    Ok(out)
}

pub(super) async fn list_range(
    storage: &SqliteStorage,
    windows: &[SubscriptionQuotaWindow],
    since_unix_secs: i64,
    until_unix_secs: i64,
) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
    if windows.is_empty() || since_unix_secs > until_unix_secs {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for window in windows {
        let rows = sqlx::query(
            r#"SELECT snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
                      capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                      stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                      header_contributing_upstreams, api_contributing_upstreams,
                      max_observed_at_unix_millis, computed_at_unix_millis, policy_version
               FROM pool_subscription_quota_history_v1
              WHERE quota_window = ?
                AND snapshot_at_unix_secs BETWEEN ? AND ?
           ORDER BY snapshot_at_unix_secs ASC"#,
        )
        .bind(window.as_str())
        .bind(since_unix_secs)
        .bind(until_unix_secs)
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
        for row in rows {
            out.push(row_to_summary(row)?);
        }
    }
    Ok(out)
}

fn row_to_summary(row: sqlx::sqlite::SqliteRow) -> StorageResult<PoolQuotaSnapshotSummaryRecord> {
    let window_text = row
        .try_get::<String, _>("quota_window")
        .map_err(map_sqlx_error)?;
    let window =
        SubscriptionQuotaWindow::from_str(&window_text).ok_or_else(|| StorageError::Corrupted {
            message: format!("unknown quota_window in pool quota history: {window_text}"),
        })?;
    Ok(PoolQuotaSnapshotSummaryRecord {
        snapshot_at_unix_secs: row
            .try_get("snapshot_at_unix_secs")
            .map_err(map_sqlx_error)?,
        window,
        utilization: row.try_get("utilization").map_err(map_sqlx_error)?,
        weighted_utilization_sum: row
            .try_get("weighted_utilization_sum")
            .map_err(map_sqlx_error)?,
        capacity_ratio_sum: row.try_get("capacity_ratio_sum").map_err(map_sqlx_error)?,
        eligible_upstreams: row.try_get("eligible_upstreams").map_err(map_sqlx_error)?,
        contributing_upstreams: row
            .try_get("contributing_upstreams")
            .map_err(map_sqlx_error)?,
        stale_upstreams: row.try_get("stale_upstreams").map_err(map_sqlx_error)?,
        missing_observation_upstreams: row
            .try_get("missing_observation_upstreams")
            .map_err(map_sqlx_error)?,
        missing_metadata_upstreams: row
            .try_get("missing_metadata_upstreams")
            .map_err(map_sqlx_error)?,
        header_contributing_upstreams: row
            .try_get("header_contributing_upstreams")
            .map_err(map_sqlx_error)?,
        api_contributing_upstreams: row
            .try_get("api_contributing_upstreams")
            .map_err(map_sqlx_error)?,
        max_observed_at_unix_millis: row
            .try_get("max_observed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        computed_at_unix_millis: row
            .try_get("computed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        policy_version: row.try_get("policy_version").map_err(map_sqlx_error)?,
    })
}

#[cfg(test)]
mod tests {
    use cc_lb_storage_api::{PoolQuotaHistoryStore, SubscriptionQuotaWindow};
    use sqlx::sqlite::SqlitePoolOptions;
    use tokio::runtime::Builder;

    use crate::SqliteStorage;

    #[test]
    fn summary_queries_do_not_require_contributors_json_column() {
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

                let storage =
                    SqliteStorage::new(pool, std::sync::Arc::new(cc_lb_core::SystemClock));

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
}
