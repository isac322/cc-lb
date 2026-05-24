use async_trait::async_trait;
use cc_lb_storage_api::{BucketKind, QuotaStore, StorageResult};
use sqlx::Row;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl QuotaStore for PostgresStorage {
    async fn incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        amount: u64,
    ) -> StorageResult<u64> {
        let value = upsert_quota_delta(
            &self.pool,
            principal_id,
            window_start,
            kind,
            u64_to_i64(amount, "quota increment amount")?,
        )
        .await?;
        i64_to_u64(value, "quota value")
    }

    async fn try_incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        amount: u64,
        capacity: u64,
    ) -> StorageResult<Option<u64>> {
        let window_start = u64_to_i64(window_start, "quota window_start")?;
        let amount = u64_to_i64(amount, "quota increment amount")?;
        let capacity = u64_to_i64(capacity, "quota capacity")?;
        let kind = kind.as_str();
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;

        let current = sqlx::query(
            "SELECT value FROM quotas_by_principal_v1              WHERE principal_id = $1 AND window_start = $2 AND kind = $3 FOR UPDATE",
        )
        .bind(principal_id)
        .bind(window_start)
        .bind(kind)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .map(|row| row.try_get::<i64, _>("value"))
        .transpose()
        .map_err(map_sqlx_error)?
        .unwrap_or(0);

        if current
            .checked_add(amount)
            .is_none_or(|next| next > capacity)
        {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        }

        let new_value = sqlx::query(
            "INSERT INTO quotas_by_principal_v1 (principal_id, window_start, kind, value)              VALUES ($1, $2, $3, $4)              ON CONFLICT (principal_id, window_start, kind) DO UPDATE              SET value = quotas_by_principal_v1.value + EXCLUDED.value              WHERE quotas_by_principal_v1.value + EXCLUDED.value <= $5              RETURNING value",
        )
        .bind(principal_id)
        .bind(window_start)
        .bind(kind)
        .bind(amount)
        .bind(capacity)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .map(|row| row.try_get::<i64, _>("value"))
        .transpose()
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        new_value
            .map(|value| i64_to_u64(value, "quota value"))
            .transpose()
    }

    async fn get_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
    ) -> StorageResult<u64> {
        let value = sqlx::query_scalar::<_, i64>(
            "SELECT value FROM quotas_by_principal_v1              WHERE principal_id = $1 AND window_start = $2 AND kind = $3",
        )
        .bind(principal_id)
        .bind(u64_to_i64(window_start, "quota window_start")?)
        .bind(kind.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .unwrap_or(0);

        i64_to_u64(value, "quota value")
    }

    async fn adjust_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        delta: i64,
    ) -> StorageResult<u64> {
        let value = upsert_quota_delta(&self.pool, principal_id, window_start, kind, delta).await?;
        i64_to_u64(value, "quota value")
    }

    async fn sweep_old_quotas(&self, older_than_window_start: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM quotas_by_principal_v1 WHERE window_start < $1")
            .bind(u64_to_i64(
                older_than_window_start,
                "quota sweep older_than_window_start",
            )?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

async fn upsert_quota_delta(
    pool: &sqlx::PgPool,
    principal_id: &str,
    window_start: u64,
    kind: BucketKind,
    delta: i64,
) -> StorageResult<i64> {
    let row = sqlx::query(
        "INSERT INTO quotas_by_principal_v1 (principal_id, window_start, kind, value)          VALUES ($1, $2, $3, $4)          ON CONFLICT (principal_id, window_start, kind) DO UPDATE          SET value = quotas_by_principal_v1.value + EXCLUDED.value          RETURNING value",
    )
    .bind(principal_id)
    .bind(u64_to_i64(window_start, "quota window_start")?)
    .bind(kind.as_str())
    .bind(delta)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)?;

    row.try_get::<i64, _>("value").map_err(map_sqlx_error)
}
