use sqlx::Postgres;
use uuid::Uuid;

use crate::{
    error::Result,
    idempotency::{OAuthRefreshClaimsStore, i64_to_u64, u64_to_i64},
};

impl OAuthRefreshClaimsStore<Postgres> {
    pub async fn try_acquire(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> Result<bool> {
        let expires_at = now_unix_secs.saturating_add(ttl_secs);
        let acquired: Option<i64> = sqlx::query_scalar(
            "INSERT INTO oauth_refresh_claims (upstream_id, holder, expires_at_unix_secs, generation) \
             VALUES ($1, $2, $3, 0) \
             ON CONFLICT(upstream_id) DO UPDATE SET \
             holder = EXCLUDED.holder, expires_at_unix_secs = EXCLUDED.expires_at_unix_secs \
             WHERE oauth_refresh_claims.holder = EXCLUDED.holder \
                OR oauth_refresh_claims.expires_at_unix_secs <= $4 \
             RETURNING 1::bigint",
        )
        .bind(upstream_id)
        .bind(holder)
        .bind(u64_to_i64(expires_at, "expires_at_unix_secs")?)
        .bind(u64_to_i64(now_unix_secs, "now_unix_secs")?)
        .fetch_optional(&self.pool)
        .await?;
        Ok(acquired.is_some())
    }

    pub async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_generation: u64,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE oauth_refresh_claims SET generation = $3, expires_at_unix_secs = 0 \
             WHERE upstream_id = $1 AND holder = $2",
        )
        .bind(upstream_id)
        .bind(holder)
        .bind(u64_to_i64(new_generation, "generation")?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE oauth_refresh_claims SET expires_at_unix_secs = 0 \
             WHERE upstream_id = $1 AND holder = $2 AND expires_at_unix_secs > 0",
        )
        .bind(upstream_id)
        .bind(holder)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn read_current_generation(&self, upstream_id: Uuid) -> Result<u64> {
        let generation: Option<i64> = sqlx::query_scalar(
            "SELECT generation FROM oauth_refresh_claims WHERE upstream_id = $1",
        )
        .bind(upstream_id)
        .fetch_optional(&self.pool)
        .await?;
        generation.map_or(Ok(0), |value| i64_to_u64(value, "generation"))
    }

    pub async fn bump_generation(&self, upstream_id: Uuid) -> Result<u64> {
        let generation: i64 = sqlx::query_scalar(
            "INSERT INTO oauth_refresh_claims (upstream_id, holder, expires_at_unix_secs, generation) \
             VALUES ($1, 'generation-bump', 0, 1) \
             ON CONFLICT(upstream_id) DO UPDATE SET generation = oauth_refresh_claims.generation + 1 \
             RETURNING generation",
        )
        .bind(upstream_id)
        .fetch_one(&self.pool)
        .await?;
        i64_to_u64(generation, "generation")
    }
}
