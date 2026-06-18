use std::time::{SystemTime, UNIX_EPOCH};

use apalis_core::backend::codec::Codec as _;
use sqlx::SqlitePool;

use crate::error::{Result, SchedulerError};
use crate::worker::EntityJob;

const DEFAULT_MAX_ATTEMPTS: i32 = 5;

pub(crate) async fn push_entity_job(pool: &SqlitePool, queue: &str, job: EntityJob) -> Result<()> {
    let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::encode(&job)
        .map_err(|error| SchedulerError::Job(format!("encode entity job: {error}")))?;
    let idempotency_key = entity_idempotency_key(&job);
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, 0, ?6, ?7) \
         ON CONFLICT(job_type, idempotency_key) WHERE status IN ('Pending','Running','Queued') DO NOTHING",
    )
    .bind(payload)
    .bind(ulid::Ulid::new().to_string())
    .bind(queue)
    .bind(DEFAULT_MAX_ATTEMPTS)
    .bind(now_unix_secs()?)
    .bind("{}")
    .bind(idempotency_key)
    .execute(pool)
    .await?;
    Ok(())
}

fn entity_idempotency_key(job: &EntityJob) -> String {
    match job {
        EntityJob::Warmup(job) => format!("entity:warmup:{}", job.upstream_id),
        EntityJob::OAuthRefresh(job) => job.idempotency_key(),
        EntityJob::OAuthUsagePoll(job) => job.idempotency_key(),
        EntityJob::AnthropicCompatRefresh(job) => {
            format!("entity:anthropic_compat_refresh:{}", job.key)
        }
        EntityJob::MetadataRefresh(job) => job.idempotency_key(),
    }
}

fn now_unix_secs() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SchedulerError::Job(format!("system clock before unix epoch: {error}")))?
        .as_secs();
    i64::try_from(seconds)
        .map_err(|_| SchedulerError::Job("current time exceeds i64::MAX".to_owned()))
}
