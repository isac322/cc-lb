use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use uuid::Uuid;

use super::{ACTIVE_PREFIX, NOW_UNIX_SECS, TestResult, assert_done, map_sqlx_error, u64_to_i64};
use crate::jobs::prompt_cache_purge::{
    PromptCacheObservationPurgeJob, PromptCacheObservationPurgeJobHandler,
    PromptCacheObservationPurgeStore,
};

#[tokio::test]
async fn postgres_purge_job_removes_expired_observations() -> TestResult {
    let Some(fixture) = PostgresFixture::create().await? else {
        return Ok(());
    };
    let upstream_id = Uuid::new_v4();
    seed_postgres(
        &fixture.pool,
        upstream_id,
        ACTIVE_PREFIX,
        0,
        NOW_UNIX_SECS + 300,
    )
    .await?;
    seed_postgres(
        &fixture.pool,
        upstream_id,
        "sha256:prompt-cache-expired-5m",
        0,
        NOW_UNIX_SECS - 1,
    )
    .await?;
    seed_postgres(
        &fixture.pool,
        upstream_id,
        "sha256:prompt-cache-expired-1h",
        1,
        NOW_UNIX_SECS - 3_600,
    )
    .await?;

    let result = PromptCacheObservationPurgeJobHandler::new(PostgresPurgeStore {
        pool: fixture.pool.clone(),
    })
    .handle(PromptCacheObservationPurgeJob::default(), NOW_UNIX_SECS)
    .await;

    assert_done(result);
    assert_eq!(postgres_count(&fixture.pool).await?, 1);
    assert_eq!(
        postgres_active_prefix(&fixture.pool, upstream_id).await?,
        Some(ACTIVE_PREFIX.to_owned())
    );
    fixture.drop_schema().await
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

struct PostgresFixture {
    schema: String,
    admin_pool: sqlx::PgPool,
    pool: sqlx::PgPool,
}

impl PostgresFixture {
    async fn create() -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP: DATABASE_URL not set - skipping postgres prompt cache purge job test");
            return Ok(None);
        };
        let schema = format!("prompt_cache_purge_job_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;
        let create_schema_sql = format!("CREATE SCHEMA {}", quote_ident(&schema));
        sqlx::query(&create_schema_sql).execute(&admin_pool).await?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        sqlx::raw_sql(include_str!(
            "../../../../../cc-lb-storage-postgres/migrations/0030_prompt_cache_observation.sql"
        ))
        .execute(&pool)
        .await?;
        Ok(Some(Self {
            schema,
            admin_pool,
            pool,
        }))
    }

    async fn drop_schema(self) -> TestResult {
        self.pool.close().await;
        let drop_schema_sql = format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        );
        sqlx::query(&drop_schema_sql)
            .execute(&self.admin_pool)
            .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn seed_postgres(
    pool: &sqlx::PgPool,
    upstream_id: Uuid,
    prefix_hash: &str,
    ttl_class: i16,
    expires_at: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO prompt_cache_observations \
         (upstream_id, canonical_model_id, prefix_hash, ttl_class, expires_at, last_observed_at, hash_schema_version) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(upstream_id)
    .bind("claude-sonnet-4-5-20250929")
    .bind(prefix_hash)
    .bind(ttl_class)
    .bind(u64_to_i64(expires_at).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .bind(u64_to_i64(NOW_UNIX_SECS).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .bind(1_i16)
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
    upstream_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT prefix_hash FROM prompt_cache_observations \
         WHERE upstream_id = $1 AND expires_at > $2",
    )
    .bind(upstream_id)
    .bind(u64_to_i64(NOW_UNIX_SECS).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
    .fetch_optional(pool)
    .await
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
