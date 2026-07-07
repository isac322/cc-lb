use async_trait::async_trait;
use cc_lb_storage_api::{
    PoolQuotaContributorBlob, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord,
    PoolQuotaSnapshotSummaryRecord, StorageError, StorageResult, SubscriptionQuotaWindow,
};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl PoolQuotaHistoryStore for SqliteStorage {
    async fn record_pool_quota_snapshots(
        &self,
        records: &[PoolQuotaSnapshotRecord],
    ) -> StorageResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut tx = self.begin_immediate().await?;
        for record in records {
            sqlx::query(
                r#"INSERT INTO pool_subscription_quota_history_v1 (
                    snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
                    capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                    stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                    header_contributing_upstreams, api_contributing_upstreams,
                    max_observed_at_unix_millis, contributors_json, computed_at_unix_millis,
                    policy_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_at_unix_secs, quota_window) DO UPDATE SET
                    utilization = excluded.utilization,
                    weighted_utilization_sum = excluded.weighted_utilization_sum,
                    capacity_ratio_sum = excluded.capacity_ratio_sum,
                    eligible_upstreams = excluded.eligible_upstreams,
                    contributing_upstreams = excluded.contributing_upstreams,
                    stale_upstreams = excluded.stale_upstreams,
                    missing_observation_upstreams = excluded.missing_observation_upstreams,
                    missing_metadata_upstreams = excluded.missing_metadata_upstreams,
                    header_contributing_upstreams = excluded.header_contributing_upstreams,
                    api_contributing_upstreams = excluded.api_contributing_upstreams,
                    max_observed_at_unix_millis = excluded.max_observed_at_unix_millis,
                    contributors_json = excluded.contributors_json,
                    computed_at_unix_millis = excluded.computed_at_unix_millis,
                    policy_version = excluded.policy_version"#,
            )
            .bind(record.snapshot_at_unix_secs)
            .bind(record.window.as_str())
            .bind(record.utilization)
            .bind(record.weighted_utilization_sum)
            .bind(record.capacity_ratio_sum)
            .bind(record.eligible_upstreams)
            .bind(record.contributing_upstreams)
            .bind(record.stale_upstreams)
            .bind(record.missing_observation_upstreams)
            .bind(record.missing_metadata_upstreams)
            .bind(record.header_contributing_upstreams)
            .bind(record.api_contributing_upstreams)
            .bind(record.max_observed_at_unix_millis)
            .bind(record.contributors_json.as_deref())
            .bind(record.computed_at_unix_millis)
            .bind(record.policy_version)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_latest_pool_quota_snapshots(
        &self,
        windows: &[SubscriptionQuotaWindow],
    ) -> StorageResult<Vec<PoolQuotaSnapshotRecord>> {
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
                          max_observed_at_unix_millis, contributors_json, computed_at_unix_millis,
                          policy_version
                   FROM pool_subscription_quota_history_v1
                  WHERE quota_window = ?
               ORDER BY snapshot_at_unix_secs DESC
                  LIMIT 1"#,
            )
            .bind(window.as_str())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
            if let Some(row) = row {
                out.push(row_to_record(row)?);
            }
        }
        Ok(out)
    }

    async fn list_pool_quota_snapshots_in_range(
        &self,
        windows: &[SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
    ) -> StorageResult<Vec<PoolQuotaSnapshotRecord>> {
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
                          max_observed_at_unix_millis, contributors_json, computed_at_unix_millis,
                          policy_version
                   FROM pool_subscription_quota_history_v1
                  WHERE quota_window = ?
                    AND snapshot_at_unix_secs BETWEEN ? AND ?
               ORDER BY snapshot_at_unix_secs ASC"#,
            )
            .bind(window.as_str())
            .bind(since_unix_secs)
            .bind(until_unix_secs)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;
            for row in rows {
                out.push(row_to_record(row)?);
            }
        }
        Ok(out)
    }

    async fn list_pool_quota_contributor_blobs_page(
        &self,
        after: Option<(i64, SubscriptionQuotaWindow)>,
        limit: u32,
    ) -> StorageResult<Vec<PoolQuotaContributorBlob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut query = sqlx::query(
            r#"SELECT snapshot_at_unix_secs, quota_window, contributors_json
                 FROM pool_subscription_quota_history_v1
                WHERE quota_window IN ('5h', '7d')
                  AND contributors_json IS NOT NULL
                  AND (? IS NULL OR snapshot_at_unix_secs > ? OR (snapshot_at_unix_secs = ? AND quota_window > ?))
             ORDER BY snapshot_at_unix_secs ASC, quota_window ASC
                LIMIT ?"#,
        );
        match after {
            Some((snapshot_at_unix_secs, window)) => {
                query = query
                    .bind(snapshot_at_unix_secs)
                    .bind(snapshot_at_unix_secs)
                    .bind(snapshot_at_unix_secs)
                    .bind(window.as_str());
            }
            None => {
                query = query
                    .bind(Option::<i64>::None)
                    .bind(Option::<i64>::None)
                    .bind(Option::<i64>::None)
                    .bind(Option::<&str>::None);
            }
        }
        query
            .bind(limit)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .map(row_to_blob)
            .collect()
    }

    async fn list_latest_pool_quota_snapshot_summaries(
        &self,
        windows: &[SubscriptionQuotaWindow],
    ) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
        super::pool_quota_history_summary::list_latest(self, windows).await
    }

    async fn list_pool_quota_snapshot_summaries_in_range(
        &self,
        windows: &[SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
    ) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
        super::pool_quota_history_summary::list_range(
            self,
            windows,
            since_unix_secs,
            until_unix_secs,
        )
        .await
    }

    async fn delete_pool_quota_snapshots_before(
        &self,
        cutoff_unix_secs: i64,
        batch_size: u32,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let result = sqlx::query(
            r#"DELETE FROM pool_subscription_quota_history_v1 WHERE rowid IN (
                SELECT rowid FROM pool_subscription_quota_history_v1
                 WHERE snapshot_at_unix_secs < ?
                 ORDER BY snapshot_at_unix_secs ASC
                 LIMIT ?
            )"#,
        )
        .bind(cutoff_unix_secs)
        .bind(batch_size)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn row_to_blob(row: sqlx::sqlite::SqliteRow) -> StorageResult<PoolQuotaContributorBlob> {
    let window_text = row
        .try_get::<String, _>("quota_window")
        .map_err(map_sqlx_error)?;
    let window =
        SubscriptionQuotaWindow::from_str(&window_text).ok_or_else(|| StorageError::Corrupted {
            message: format!("unknown quota_window in pool quota history: {window_text}"),
        })?;
    Ok(PoolQuotaContributorBlob {
        snapshot_at_unix_secs: row
            .try_get::<i64, _>("snapshot_at_unix_secs")
            .map_err(map_sqlx_error)?,
        window,
        contributors_json: row
            .try_get::<String, _>("contributors_json")
            .map_err(map_sqlx_error)?,
    })
}

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> StorageResult<PoolQuotaSnapshotRecord> {
    let window_text = row
        .try_get::<String, _>("quota_window")
        .map_err(map_sqlx_error)?;
    let window =
        SubscriptionQuotaWindow::from_str(&window_text).ok_or_else(|| StorageError::Corrupted {
            message: format!("unknown quota_window in pool quota history: {window_text}"),
        })?;
    Ok(PoolQuotaSnapshotRecord {
        snapshot_at_unix_secs: row
            .try_get::<i64, _>("snapshot_at_unix_secs")
            .map_err(map_sqlx_error)?,
        window,
        utilization: row
            .try_get::<Option<f64>, _>("utilization")
            .map_err(map_sqlx_error)?,
        weighted_utilization_sum: row
            .try_get::<f64, _>("weighted_utilization_sum")
            .map_err(map_sqlx_error)?,
        capacity_ratio_sum: row
            .try_get::<f64, _>("capacity_ratio_sum")
            .map_err(map_sqlx_error)?,
        eligible_upstreams: row
            .try_get::<i64, _>("eligible_upstreams")
            .map_err(map_sqlx_error)?,
        contributing_upstreams: row
            .try_get::<i64, _>("contributing_upstreams")
            .map_err(map_sqlx_error)?,
        stale_upstreams: row
            .try_get::<i64, _>("stale_upstreams")
            .map_err(map_sqlx_error)?,
        missing_observation_upstreams: row
            .try_get::<i64, _>("missing_observation_upstreams")
            .map_err(map_sqlx_error)?,
        missing_metadata_upstreams: row
            .try_get::<i64, _>("missing_metadata_upstreams")
            .map_err(map_sqlx_error)?,
        header_contributing_upstreams: row
            .try_get::<i64, _>("header_contributing_upstreams")
            .map_err(map_sqlx_error)?,
        api_contributing_upstreams: row
            .try_get::<i64, _>("api_contributing_upstreams")
            .map_err(map_sqlx_error)?,
        max_observed_at_unix_millis: row
            .try_get::<Option<i64>, _>("max_observed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        contributors_json: row
            .try_get::<Option<String>, _>("contributors_json")
            .map_err(map_sqlx_error)?,
        computed_at_unix_millis: row
            .try_get::<i64, _>("computed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        policy_version: row
            .try_get::<i32, _>("policy_version")
            .map_err(map_sqlx_error)?,
    })
}
