use cc_lb_storage_api::{
    PoolQuotaChartPointRecord, PoolQuotaSnapshotSummaryRecord, StorageError, StorageResult,
    SubscriptionQuotaWindow,
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

pub(super) async fn list_chart_range(
    storage: &SqliteStorage,
    windows: &[SubscriptionQuotaWindow],
    since_unix_secs: i64,
    until_unix_secs: i64,
    bucket_secs: Option<i64>,
) -> StorageResult<Vec<PoolQuotaChartPointRecord>> {
    if windows.is_empty() || since_unix_secs > until_unix_secs {
        return Ok(Vec::new());
    }
    if bucket_secs.is_some_and(|value| value <= 0) {
        return Err(StorageError::Fatal {
            message: "pool quota chart bucket_secs must be positive".to_owned(),
        });
    }
    let mut out = Vec::new();
    for window in windows {
        let rows = match bucket_secs {
            Some(bucket_secs) => sqlx::query(
                r#"SELECT bucket_start AS snapshot_at_unix_secs,
                              quota_window,
                              MAX(utilization) AS utilization
                       FROM (
                           SELECT snapshot_at_unix_secs
                                      - (
                                          (
                                              snapshot_at_unix_secs % ? + ?
                                          ) % ?
                                      ) AS bucket_start,
                                  quota_window,
                                  utilization
                           FROM pool_subscription_quota_history_v1
                           WHERE quota_window = ?
                             AND snapshot_at_unix_secs BETWEEN ? AND ?
                       ) chart_points
                       GROUP BY quota_window, bucket_start
                       ORDER BY bucket_start ASC"#,
            )
            .bind(bucket_secs)
            .bind(bucket_secs)
            .bind(bucket_secs)
            .bind(window.as_str())
            .bind(since_unix_secs)
            .bind(until_unix_secs)
            .fetch_all(storage.pool())
            .await
            .map_err(map_sqlx_error)?,
            None => sqlx::query(
                r#"SELECT snapshot_at_unix_secs, quota_window, utilization
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
            .map_err(map_sqlx_error)?,
        };
        for row in rows {
            out.push(row_to_chart_point(row)?);
        }
    }
    Ok(out)
}

fn row_to_chart_point(row: sqlx::sqlite::SqliteRow) -> StorageResult<PoolQuotaChartPointRecord> {
    let window_text = row
        .try_get::<String, _>("quota_window")
        .map_err(map_sqlx_error)?;
    let window =
        SubscriptionQuotaWindow::from_str(&window_text).ok_or_else(|| StorageError::Corrupted {
            message: format!("unknown quota_window in pool quota history: {window_text}"),
        })?;
    Ok(PoolQuotaChartPointRecord {
        snapshot_at_unix_secs: row
            .try_get("snapshot_at_unix_secs")
            .map_err(map_sqlx_error)?,
        window,
        utilization: row.try_get("utilization").map_err(map_sqlx_error)?,
    })
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
