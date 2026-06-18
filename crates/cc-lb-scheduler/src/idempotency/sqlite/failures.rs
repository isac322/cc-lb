use sqlx::{Row, Sqlite, sqlite::SqliteRow};

use crate::{
    error::Result,
    idempotency::{
        SchedulerFailure, SchedulerFailuresStore, i64_to_u32, i64_to_u64, u32_to_i32, u64_to_i64,
    },
};

impl SchedulerFailuresStore<Sqlite> {
    pub async fn record(
        &self,
        job_type: &str,
        payload_summary: &str,
        error: &str,
        attempts: u32,
        now_unix_secs: u64,
    ) -> Result<u64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO scheduler_failures \
             (job_type, payload_summary, last_error, attempts, first_failed_at_unix_secs, last_failed_at_unix_secs) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?5) RETURNING id",
        )
        .bind(job_type)
        .bind(payload_summary)
        .bind(error)
        .bind(u32_to_i32(attempts, "attempts")?)
        .bind(u64_to_i64(now_unix_secs, "now_unix_secs")?)
        .fetch_one(&self.pool)
        .await?;
        i64_to_u64(id, "id")
    }

    pub async fn list_paged(
        &self,
        job_type_filter: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<SchedulerFailure>> {
        let rows = if let Some(job_type) = job_type_filter {
            sqlx::query(
                "SELECT * FROM scheduler_failures WHERE job_type = ?1 \
                 ORDER BY last_failed_at_unix_secs DESC, id DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(job_type)
            .bind(i64::from(limit))
            .bind(i64::from(offset))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT * FROM scheduler_failures \
                 ORDER BY last_failed_at_unix_secs DESC, id DESC LIMIT ?1 OFFSET ?2",
            )
            .bind(i64::from(limit))
            .bind(i64::from(offset))
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_failure).collect()
    }

    pub async fn prune_older_than(&self, cutoff_unix_secs: u64) -> Result<u64> {
        let result =
            sqlx::query("DELETE FROM scheduler_failures WHERE last_failed_at_unix_secs < ?1")
                .bind(u64_to_i64(cutoff_unix_secs, "cutoff_unix_secs")?)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected())
    }
}

fn row_to_failure(row: SqliteRow) -> Result<SchedulerFailure> {
    Ok(SchedulerFailure {
        id: i64_to_u64(row.try_get("id")?, "id")?,
        job_type: row.try_get("job_type")?,
        payload_summary: row.try_get("payload_summary")?,
        last_error: row.try_get("last_error")?,
        attempts: i64_to_u32(row.try_get("attempts")?, "attempts")?,
        first_failed_at_unix_secs: i64_to_u64(
            row.try_get("first_failed_at_unix_secs")?,
            "first_failed_at_unix_secs",
        )?,
        last_failed_at_unix_secs: i64_to_u64(
            row.try_get("last_failed_at_unix_secs")?,
            "last_failed_at_unix_secs",
        )?,
    })
}
