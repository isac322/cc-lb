use std::str::FromStr as _;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::{ACTIVE_PREFIX, NOW_UNIX_SECS, TestResult, assert_done, map_sqlx_error, u64_to_i64};
use crate::jobs::prompt_cache_purge::{
    PromptCacheObservationPurgeJob, PromptCacheObservationPurgeJobHandler,
    PromptCacheObservationPurgeStore,
};
type MaybePrefix = Option<String>;

#[tokio::test]
async fn t3_postgres__purge_job_removes_expired_observations() -> TestResult {
    let fixture = cc_lb_storage_conformance::postgres_fixture().await?;
    let search_path = format!("{},public", fixture.schema_name());
    let options = PgConnectOptions::from_str(fixture.database_url())?
        .options([("search_path", search_path.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?;
    let upstream_id = uuid::Uuid::from_u128(1);
    seed_postgres(&pool, upstream_id, ACTIVE_PREFIX, 0, NOW_UNIX_SECS + 300).await?;
    seed_postgres(
        &pool,
        upstream_id,
        "sha256:prompt-cache-expired-5m",
        0,
        NOW_UNIX_SECS - 1,
    )
    .await?;
    seed_postgres(
        &pool,
        upstream_id,
        "sha256:prompt-cache-expired-1h",
        1,
        NOW_UNIX_SECS - 3_600,
    )
    .await?;

    let result =
        PromptCacheObservationPurgeJobHandler::new(PostgresPurgeStore { pool: pool.clone() })
            .handle(PromptCacheObservationPurgeJob::default(), NOW_UNIX_SECS)
            .await;

    assert_done(result);
    assert_eq!(postgres_count(&pool).await?, 1);
    assert_eq!(
        postgres_active_prefix(&pool, upstream_id).await?,
        Some(ACTIVE_PREFIX.to_owned())
    );
    pool.close().await;
    fixture.teardown().await?;
    Ok(())
}

struct PostgresPurgeStore {
    pool: sqlx::PgPool,
}

impl PromptCacheObservationPurgeStore for PostgresPurgeStore {
    async fn purge_expired_before(
        &self,
        ts_unix_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<u64> {
        let result = sqlx::query("DELETE FROM prompt_cache_observations WHERE expires_at < $1")
            .bind(u64_to_i64(ts_unix_secs)?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

async fn seed_postgres(
    pool: &sqlx::PgPool,
    upstream_id: uuid::Uuid,
    prefix_key: &str,
    ttl_class: i16,
    expires_at: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO prompt_cache_observations \
         (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, \
          hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(upstream_id)
    .bind("claude-sonnet-4-5-20250929")
    .bind(prefix_key)
    .bind(ttl_class)
    .bind(u64_to_i64(expires_at).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .bind(u64_to_i64(NOW_UNIX_SECS).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .bind(3_i16)
    .bind(0_i64)
    .bind(1_i64)
    .bind("heuristic")
    .execute(pool)
    .await?;
    Ok(())
}

async fn postgres_count(pool: &sqlx::PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM prompt_cache_observations")
        .fetch_one(pool)
        .await
}

async fn postgres_active_prefix(
    pool: &sqlx::PgPool,
    upstream_id: uuid::Uuid,
) -> Result<MaybePrefix, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT v3_prefix_key FROM prompt_cache_observations \
         WHERE upstream_id = $1 AND expires_at > $2",
    )
    .bind(upstream_id)
    .bind(u64_to_i64(NOW_UNIX_SECS).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .fetch_optional(pool)
    .await
}
