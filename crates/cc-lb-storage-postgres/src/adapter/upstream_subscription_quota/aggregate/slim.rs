use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaSlimCheckpoint,
};
use sqlx::{Row, postgres::PgRow};

use super::super::{parse_source, parse_status, parse_window};
use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

pub(in super::super) async fn list_slim_checkpoints(
    storage: &PostgresStorage,
    query: &SubscriptionQuotaCheckpointRangeQuery,
) -> StorageResult<Vec<SubscriptionQuotaSlimCheckpoint>> {
    if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
        return Ok(Vec::new());
    }
    let windows = query
        .windows
        .iter()
        .map(|window| window.as_str())
        .collect::<Vec<_>>();
    let sources = query
        .sources
        .iter()
        .map(|source| source.as_str())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "WITH anchors AS ( \
             SELECT DISTINCT ON (upstream_id, \"window\", source) \
                    upstream_id, \"window\", source, changed_at_unix_millis, sample_id, \
                    utilization, status, resets_at_unix_secs \
             FROM upstream_subscription_quota_checkpoints_v1 \
             WHERE upstream_id = ANY($1::uuid[]) \
               AND \"window\" = ANY($2::text[]) \
               AND source = ANY($3::text[]) \
               AND changed_at_unix_millis < $4 \
             ORDER BY upstream_id ASC, \"window\" ASC, source ASC, \
                      changed_at_unix_millis DESC, sample_id DESC \
         ), ranged AS ( \
             SELECT upstream_id, \"window\", source, changed_at_unix_millis, sample_id, \
                    utilization, status, resets_at_unix_secs \
             FROM upstream_subscription_quota_checkpoints_v1 \
             WHERE upstream_id = ANY($1::uuid[]) \
               AND \"window\" = ANY($2::text[]) \
               AND source = ANY($3::text[]) \
               AND changed_at_unix_millis >= $4 \
               AND changed_at_unix_millis <= $5 \
         ) \
         SELECT * FROM anchors UNION ALL SELECT * FROM ranged",
    )
    .bind(&query.upstream_ids)
    .bind(&windows)
    .bind(&sources)
    .bind(u64_to_i64(
        query.since_unix_millis,
        "subscription quota slim since_unix_millis",
    )?)
    .bind(u64_to_i64(
        query.until_unix_millis,
        "subscription quota slim until_unix_millis",
    )?)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    let mut checkpoints = rows
        .into_iter()
        .map(row_to_slim_checkpoint)
        .collect::<StorageResult<Vec<_>>>()?;
    checkpoints.sort_by_key(|checkpoint| {
        (
            checkpoint.upstream_id,
            checkpoint.window.as_str(),
            checkpoint.source.as_str(),
            checkpoint.changed_at_unix_millis,
            checkpoint.sample_id,
        )
    });
    Ok(checkpoints)
}

fn row_to_slim_checkpoint(row: PgRow) -> StorageResult<SubscriptionQuotaSlimCheckpoint> {
    let status = row
        .try_get::<Option<String>, _>("status")
        .map_err(map_sqlx_error)?
        .as_deref()
        .map(parse_status)
        .transpose()?;
    Ok(SubscriptionQuotaSlimCheckpoint {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        window: parse_window(&row.try_get::<String, _>("window").map_err(map_sqlx_error)?)?,
        source: parse_source(&row.try_get::<String, _>("source").map_err(map_sqlx_error)?)?,
        changed_at_unix_millis: i64_to_u64(
            row.try_get("changed_at_unix_millis")
                .map_err(map_sqlx_error)?,
            "subscription quota slim changed_at_unix_millis",
        )?,
        sample_id: row.try_get("sample_id").map_err(map_sqlx_error)?,
        utilization: row.try_get("utilization").map_err(map_sqlx_error)?,
        status,
        resets_at_unix_secs: row
            .try_get::<Option<i64>, _>("resets_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "subscription quota slim resets_at_unix_secs"))
            .transpose()?,
    })
}
