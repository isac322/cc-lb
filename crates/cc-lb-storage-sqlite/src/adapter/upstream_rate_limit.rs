use async_trait::async_trait;
use cc_lb_storage_api::{
    RateLimitKind, StorageError, StorageResult, UpstreamRateLimitObservationRecord,
    UpstreamRateLimitStateStore,
};
use sqlx::{AssertSqlSafe, Row};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl UpstreamRateLimitStateStore for SqliteStorage {
    async fn put_observation(
        &self,
        record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        let payload = serde_json::to_string(record)?;
        sqlx::query(
            "INSERT INTO upstream_rate_limit_state_v1 (upstream_id, key, value, updated_at) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(upstream_id, key) DO UPDATE SET \
             value = excluded.value, updated_at = excluded.updated_at \
             WHERE excluded.updated_at >= upstream_rate_limit_state_v1.updated_at",
        )
        .bind(record.upstream_id.to_string())
        .bind(observation_key(&record.window, record.kind))
        .bind(payload)
        .bind(u64_to_i64(
            record.observed_at_unix_secs,
            "upstream rate limit observed_at_unix_secs",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = std::iter::repeat_n("?", upstream_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT value FROM upstream_rate_limit_state_v1 \
             WHERE upstream_id IN ({placeholders}) \
             ORDER BY upstream_id ASC, key ASC"
        );
        let mut query = sqlx::query(AssertSqlSafe(sql));
        for upstream_id in upstream_ids {
            query = query.bind(upstream_id.to_string());
        }
        let rows = query.fetch_all(self.pool()).await.map_err(map_sqlx_error)?;

        let mut records = rows
            .into_iter()
            .map(|row| {
                let payload: String = row.try_get("value").map_err(map_sqlx_error)?;
                serde_json::from_str::<UpstreamRateLimitObservationRecord>(&payload)
                    .map_err(StorageError::from)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        records.sort_by(|left, right| {
            left.upstream_id
                .cmp(&right.upstream_id)
                .then_with(|| left.window.cmp(&right.window))
                .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
        });
        Ok(records)
    }
}

fn observation_key(window: &str, kind: RateLimitKind) -> String {
    format!("{}:{window}:{}", window.len(), kind.as_str())
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}
