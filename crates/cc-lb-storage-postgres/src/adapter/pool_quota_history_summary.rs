use cc_lb_storage_api::{
    PoolQuotaSnapshotSummaryRecord, StorageError, StorageResult, SubscriptionQuotaWindow,
};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

pub(super) async fn list_latest(
    storage: &PostgresStorage,
    windows: &[SubscriptionQuotaWindow],
) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
    if windows.is_empty() {
        return Ok(Vec::new());
    }
    let windows = windows
        .iter()
        .map(|window| (*window).as_str())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"SELECT DISTINCT ON ("quota_window")
                  snapshot_at_unix_secs, "quota_window", utilization, weighted_utilization_sum,
                  capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                  stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                  header_contributing_upstreams, api_contributing_upstreams,
                  max_observed_at_unix_millis, computed_at_unix_millis, policy_version
           FROM pool_subscription_quota_history_v1
          WHERE "quota_window" = ANY($1)
       ORDER BY "quota_window", snapshot_at_unix_secs DESC"#,
    )
    .bind(&windows)
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_summary).collect()
}

pub(super) async fn list_range(
    storage: &PostgresStorage,
    windows: &[SubscriptionQuotaWindow],
    since_unix_secs: i64,
    until_unix_secs: i64,
) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
    if windows.is_empty() || since_unix_secs > until_unix_secs {
        return Ok(Vec::new());
    }
    let windows = windows
        .iter()
        .map(|window| (*window).as_str())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"SELECT snapshot_at_unix_secs, "quota_window", utilization, weighted_utilization_sum,
                  capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                  stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                  header_contributing_upstreams, api_contributing_upstreams,
                  max_observed_at_unix_millis, computed_at_unix_millis, policy_version
           FROM pool_subscription_quota_history_v1
          WHERE "quota_window" = ANY($1)
            AND snapshot_at_unix_secs BETWEEN $2 AND $3
       ORDER BY "quota_window" DESC, snapshot_at_unix_secs ASC"#,
    )
    .bind(&windows)
    .bind(since_unix_secs)
    .bind(until_unix_secs)
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_summary).collect()
}

fn row_to_summary(row: sqlx::postgres::PgRow) -> StorageResult<PoolQuotaSnapshotSummaryRecord> {
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
