use sqlx::Postgres;
use uuid::Uuid;

use crate::{
    error::Result,
    idempotency::{WarmupEffectsStore, u64_to_i64},
};

impl WarmupEffectsStore<Postgres> {
    pub async fn try_acquire_cycle(
        &self,
        upstream_id: Uuid,
        cycle_key: u64,
        completed_at_unix_secs: u64,
    ) -> Result<bool> {
        let result = sqlx::query(
            "INSERT INTO warmup_effects (upstream_id, cycle_key, completed_at_unix_secs) \
             VALUES ($1, $2, $3) ON CONFLICT(upstream_id, cycle_key) DO NOTHING",
        )
        .bind(upstream_id)
        .bind(u64_to_i64(cycle_key, "cycle_key")?)
        .bind(u64_to_i64(
            completed_at_unix_secs,
            "completed_at_unix_secs",
        )?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn is_already_done(&self, upstream_id: Uuid, cycle_key: u64) -> Result<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM warmup_effects WHERE upstream_id = $1 AND cycle_key = $2",
        )
        .bind(upstream_id)
        .bind(u64_to_i64(cycle_key, "cycle_key")?)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    pub async fn prune_older_than(&self, cutoff_unix_secs: u64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM warmup_effects WHERE completed_at_unix_secs < $1")
            .bind(u64_to_i64(cutoff_unix_secs, "cutoff_unix_secs")?)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}
