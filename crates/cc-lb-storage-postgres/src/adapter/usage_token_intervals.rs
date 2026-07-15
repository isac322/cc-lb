use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UsageTokenInterval, UsageTokenIntervalStore, UsageTokenIntervalSum,
};
use sqlx::Row;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl UsageTokenIntervalStore for PostgresStorage {
    async fn sum_usage_tokens_for_intervals(
        &self,
        intervals: &[UsageTokenInterval],
    ) -> StorageResult<Vec<UsageTokenIntervalSum>> {
        if intervals.is_empty() {
            return Ok(Vec::new());
        }
        let interval_ids = intervals
            .iter()
            .map(|interval| u64_to_i64(interval.interval_id, "usage token interval_id"))
            .collect::<StorageResult<Vec<_>>>()?;
        let upstream_ids = intervals
            .iter()
            .map(|interval| interval.upstream_id)
            .collect::<Vec<_>>();
        let starts = intervals
            .iter()
            .map(|interval| {
                u64_to_i64(
                    interval.start_unix_secs,
                    "usage token interval start_unix_secs",
                )
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let ends = intervals
            .iter()
            .map(|interval| {
                u64_to_i64(interval.end_unix_secs, "usage token interval end_unix_secs")
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let rows = sqlx::query(
            "WITH intervals AS ( \
                 SELECT interval_id, upstream_id, start_unix_secs, end_unix_secs, ordinal \
                 FROM UNNEST($1::bigint[], $2::uuid[], $3::bigint[], $4::bigint[]) \
                      WITH ORDINALITY AS input( \
                          interval_id, upstream_id, start_unix_secs, end_unix_secs, ordinal \
                      ) \
             ) \
             SELECT intervals.interval_id, \
                    COALESCE(SUM( \
                        rollups.input_tokens + rollups.output_tokens + \
                        rollups.cache_creation_input_tokens + rollups.cache_read_input_tokens \
                    ), 0)::bigint AS tokens \
             FROM intervals \
             LEFT JOIN usage_rollups_v2 AS rollups \
               ON rollups.resolution = 'minute' \
              AND rollups.upstream_id = intervals.upstream_id \
              AND rollups.bucket_start_unix_secs >= intervals.start_unix_secs \
              AND rollups.bucket_start_unix_secs <= intervals.end_unix_secs \
             GROUP BY intervals.ordinal, intervals.interval_id \
             ORDER BY intervals.ordinal ASC",
        )
        .bind(&interval_ids)
        .bind(&upstream_ids)
        .bind(&starts)
        .bind(&ends)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(UsageTokenIntervalSum {
                    interval_id: i64_to_u64(
                        row.try_get("interval_id").map_err(map_sqlx_error)?,
                        "usage token interval_id",
                    )?,
                    tokens: i64_to_u64(
                        row.try_get("tokens").map_err(map_sqlx_error)?,
                        "usage token interval tokens",
                    )?,
                })
            })
            .collect()
    }
}
