use uuid::Uuid;

use super::{ACTIVE_PREFIX, NOW_UNIX_SECS, TestResult, assert_done, map_sqlx_error, u64_to_i64};
use crate::jobs::prompt_cache_purge::{
    PromptCacheObservationPurgeJob, PromptCacheObservationPurgeJobHandler,
    PromptCacheObservationPurgeStore,
};
type MaybePrefix = Option<String>;

#[tokio::test]
async fn t3__sqlite_purge_job_removes_expired_observations() -> TestResult {
    let pool = sqlite_pool().await?;
    let upstream_id = Uuid::from_u128(1);
    seed_sqlite(&pool, upstream_id, ACTIVE_PREFIX, "0", NOW_UNIX_SECS + 300).await?;
    seed_sqlite(
        &pool,
        upstream_id,
        "sha256:prompt-cache-expired-5m",
        "0",
        NOW_UNIX_SECS - 1,
    )
    .await?;
    seed_sqlite(
        &pool,
        upstream_id,
        "sha256:prompt-cache-expired-1h",
        "1",
        NOW_UNIX_SECS - 3_600,
    )
    .await?;

    let result =
        PromptCacheObservationPurgeJobHandler::new(SqlitePurgeStore { pool: pool.clone() })
            .handle(PromptCacheObservationPurgeJob::default(), NOW_UNIX_SECS)
            .await;

    assert_done(result);
    assert_eq!(sqlite_count(&pool).await?, 1);
    assert_eq!(
        sqlite_active_prefix(&pool, upstream_id).await?,
        Some(ACTIVE_PREFIX.to_owned())
    );
    Ok(())
}

struct SqlitePurgeStore {
    pool: sqlx::SqlitePool,
}

impl PromptCacheObservationPurgeStore for SqlitePurgeStore {
    async fn purge_expired_before(
        &self,
        ts_unix_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<u64> {
        let result = sqlx::query("DELETE FROM prompt_cache_observations WHERE expires_at < ?")
            .bind(u64_to_i64(ts_unix_secs)?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

async fn sqlite_pool() -> Result<sqlx::SqlitePool, sqlx::Error> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../../../cc-lb-storage-sqlite/migrations/0006_rate_subscription_prompt_org_compat.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(pool)
}

async fn seed_sqlite(
    pool: &sqlx::SqlitePool,
    upstream_id: Uuid,
    prefix_hash: &str,
    ttl_class: &str,
    expires_at: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO prompt_cache_observations \
         (upstream_id, canonical_model_id, prefix_hash, ttl_class, expires_at, last_observed_at, hash_schema_version) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(upstream_id.to_string())
    .bind("claude-sonnet-4-5-20250929")
    .bind(prefix_hash)
    .bind(ttl_class)
    .bind(u64_to_i64(expires_at).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .bind(u64_to_i64(NOW_UNIX_SECS).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .bind(1_i64)
    .execute(pool)
    .await?;
    Ok(())
}

async fn sqlite_count(pool: &sqlx::SqlitePool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM prompt_cache_observations")
        .fetch_one(pool)
        .await
}

async fn sqlite_active_prefix(
    pool: &sqlx::SqlitePool,
    upstream_id: Uuid,
) -> Result<MaybePrefix, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT prefix_hash FROM prompt_cache_observations \
         WHERE upstream_id = ? AND expires_at > ?",
    )
    .bind(upstream_id.to_string())
    .bind(u64_to_i64(NOW_UNIX_SECS).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .fetch_optional(pool)
    .await
}
