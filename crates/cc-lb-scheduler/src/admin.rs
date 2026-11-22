use sqlx::Row as _;
use uuid::Uuid;

use crate::error::Result;
use crate::worker::{ADAPTIVE_QUEUE, AdaptiveJob, CronJob, SchedulerBackend, SchedulerPushTask};

#[derive(Clone, Debug)]
pub struct SchedulerAdminHandle {
    backend: SchedulerBackend,
}

impl SchedulerAdminHandle {
    pub const fn new(backend: SchedulerBackend) -> Self {
        Self { backend }
    }

    pub async fn push_adaptive_task(&self, task: SchedulerPushTask<AdaptiveJob>) -> Result<()> {
        self.backend.push_adaptive_task(task).await
    }

    pub async fn push_cron_task(&self, task: SchedulerPushTask<CronJob>) -> Result<()> {
        self.backend.push_cron_task(task).await
    }

    pub async fn next_run_for_upstream(
        &self,
        upstream_id: Uuid,
        job_kind: &str,
    ) -> Result<Option<i64>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                sqlite_next_run_for_upstream(sqlite.pool(), upstream_id, job_kind).await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                postgres_next_run_for_upstream(postgres.pool(), upstream_id, job_kind).await
            }
        }
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_next_run_for_upstream(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    upstream_id: Uuid,
    job_kind: &str,
) -> Result<Option<i64>> {
    let prefix = format!("{ADAPTIVE_QUEUE}:{job_kind}:{upstream_id}:%");
    let row = sqlx::query(
        "SELECT MIN(run_at) AS next_run_at \
         FROM Jobs \
         WHERE job_type = ?1 \
           AND idempotency_key LIKE ?2 \
           AND (status IN ('Pending', 'Queued', 'Running') \
                OR (status = 'Failed' AND attempts < max_attempts))",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(prefix)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("next_run_at")?)
}

#[cfg(feature = "postgres")]
async fn postgres_next_run_for_upstream(
    pool: &sqlx::Pool<sqlx::Postgres>,
    upstream_id: Uuid,
    job_kind: &str,
) -> Result<Option<i64>> {
    let prefix = format!("{ADAPTIVE_QUEUE}:{job_kind}:{upstream_id}:%");
    let row = sqlx::query(
        "SELECT EXTRACT(EPOCH FROM MIN(run_at))::BIGINT AS next_run_at \
         FROM apalis.jobs \
         WHERE job_type = $1 \
           AND idempotency_key LIKE $2 \
           AND (status IN ('Pending', 'Queued', 'Running') \
                OR (status = 'Failed' AND attempts < max_attempts))",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(prefix)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("next_run_at")?)
}
