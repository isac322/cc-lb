pub use apalis_core::backend::Filter;
use apalis_core::backend::ListTasks;
use apalis_core::backend::codec::Codec as _;
use apalis_core::task::builder::TaskBuilder;
pub use apalis_core::task::status::Status as TaskStatus;

use serde::de::DeserializeOwned;

use crate::error::SchedulerError;

use super::{
    ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob, SchedulerBackend,
};

const DEFAULT_MAX_ATTEMPTS: i32 = 5;

#[cfg(feature = "sqlite")]
fn from_sqlite_apalis_error(error: apalis_sqlite::Error) -> SchedulerError {
    match error {
        apalis_sqlite::Error::Database(db_err) => SchedulerError::Database(db_err),
        other => SchedulerError::Job(other.to_string()),
    }
}

#[cfg(feature = "postgres")]
fn from_postgres_apalis_error(error: apalis_postgres::Error) -> SchedulerError {
    match error {
        apalis_postgres::Error::Database(db_err) => SchedulerError::Database(db_err),
        other => SchedulerError::Job(other.to_string()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerTaskRow<Args> {
    pub args: Args,
    pub idempotency_key: Option<String>,
    pub run_at_unix_secs: u64,
    pub status: TaskStatus,
    pub attempts: i32,
    pub max_attempts: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdaptiveTaskKeyState {
    Missing,
    Active,
    Done,
    TerminalFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerTaskStatusRow {
    pub status: TaskStatus,
    pub attempts: i32,
    pub max_attempts: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerPushTask<Args> {
    pub args: Args,
    pub idempotency_key: Option<String>,
    pub run_at_unix_secs: Option<u64>,
    pub max_attempts: Option<i32>,
}

impl SchedulerBackend {
    pub async fn adaptive_task_key_state(
        &self,
        idempotency_key: &str,
    ) -> Result<AdaptiveTaskKeyState, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                sqlite_adaptive_task_key_state(sqlite.pool(), idempotency_key).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                postgres_adaptive_task_key_state(postgres.pool(), idempotency_key).await
            }
        }
    }

    pub async fn adaptive_task_key_family_state(
        &self,
        idempotency_key: &str,
    ) -> Result<AdaptiveTaskKeyState, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                sqlite_adaptive_task_key_family_state(sqlite.pool(), idempotency_key).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                postgres_adaptive_task_key_family_state(postgres.pool(), idempotency_key).await
            }
        }
    }

    pub async fn find_active_adaptive_key_in_family(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<String>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                sqlite_find_active_adaptive_key_in_family(sqlite.pool(), idempotency_key).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                postgres_find_active_adaptive_key_in_family(postgres.pool(), idempotency_key).await
            }
        }
    }

    pub async fn push_adaptive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(ADAPTIVE_QUEUE, task, false).await
    }

    /// Pushes an adaptive task or expedites a matching unstarted `Pending`
    /// task (`attempts = 0`) to run at `min(existing.run_at, new.run_at)`.
    pub async fn push_or_expedite_adaptive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(ADAPTIVE_QUEUE, task, true).await
    }

    async fn push_task_with_mode<Args>(
        &self,
        queue: &str,
        task: SchedulerPushTask<Args>,
        expedite_pending: bool,
    ) -> Result<(), SchedulerError>
    where
        Args: DeserializeOwned + serde::Serialize,
    {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                insert_sqlite_task_with_mode(sqlite.pool(), queue, task, expedite_pending).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                insert_postgres_task_with_mode(postgres.pool(), queue, task, expedite_pending).await
            }
        }
    }

    pub async fn push_keepalive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(CACHE_KEEPALIVE_QUEUE, task, false)
            .await
    }

    pub async fn push_cron_task(
        &self,
        task: SchedulerPushTask<CronJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(CRON_QUEUE, task, false).await
    }

    pub async fn list_adaptive_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<SchedulerTaskRow<AdaptiveJob>>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let storage = sqlite.adaptive_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_sqlite_apalis_error)?
                    .into_iter()
                    .map(sqlite_task_to_row)
                    .collect()
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let storage = postgres.adaptive_operation_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_postgres_apalis_error)?
                    .into_iter()
                    .map(postgres_task_to_row)
                    .collect()
            }
        }
    }

    pub async fn list_keepalive_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<SchedulerTaskRow<AdaptiveJob>>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let storage = sqlite.keepalive_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_sqlite_apalis_error)?
                    .into_iter()
                    .map(sqlite_task_to_row)
                    .collect()
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let storage = postgres.keepalive_operation_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_postgres_apalis_error)?
                    .into_iter()
                    .map(postgres_task_to_row)
                    .collect()
            }
        }
    }

    /// Atomically cancels an unstarted `Pending` adaptive job (`attempts == 0`) by
    /// transitioning it to terminal `Killed`. Returns `true` only if this call
    /// performed the transition, proving no worker had claimed the job yet.
    pub async fn cancel_pending_adaptive_task_by_key(
        &self,
        idempotency_key: &str,
    ) -> Result<bool, SchedulerError> {
        let rows_affected = match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => sqlx::query(
                "UPDATE Jobs \
                     SET status = 'Killed', \
                         done_at = CAST(strftime('%s', 'now') AS INTEGER), \
                         last_result = '{\"Err\":\"cancelled_before_start\"}' \
                     WHERE job_type = ?1 \
                       AND idempotency_key = ?2 \
                       AND status = 'Pending' \
                       AND attempts = 0",
            )
            .bind(ADAPTIVE_QUEUE)
            .bind(idempotency_key)
            .execute(sqlite.pool())
            .await?
            .rows_affected(),
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => sqlx::query(
                "UPDATE apalis.jobs \
                     SET status = 'Killed', \
                         done_at = NOW(), \
                         last_result = '{\"Err\":\"cancelled_before_start\"}'::jsonb \
                     WHERE job_type = $1 \
                       AND idempotency_key = $2 \
                       AND status = 'Pending' \
                       AND attempts = 0",
            )
            .bind(ADAPTIVE_QUEUE)
            .bind(idempotency_key)
            .execute(postgres.pool())
            .await?
            .rows_affected(),
        };
        Ok(rows_affected > 0)
    }

    /// Reads status/attempt metadata for matching adaptive tasks by exact idempotency key
    /// without decoding the serialized `job` payload.
    pub async fn get_adaptive_task_statuses_by_key(
        &self,
        idempotency_key: &str,
    ) -> Result<Vec<SchedulerTaskStatusRow>, SchedulerError> {
        let rows: Vec<(String, i32, i32)> = match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                sqlx::query_as(
                    "SELECT status, attempts, max_attempts FROM Jobs \
                     WHERE job_type = ?1 AND idempotency_key = ?2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(idempotency_key)
                .fetch_all(sqlite.pool())
                .await?
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                sqlx::query_as(
                    "SELECT status, attempts, max_attempts FROM apalis.jobs \
                     WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(idempotency_key)
                .fetch_all(postgres.pool())
                .await?
            }
        };
        parse_status_rows(rows)
    }

    /// Reads status/attempt metadata for matching adaptive tasks by exact idempotency key
    /// or `"{idempotency_key}:*"` subkeys (such as `:watchdog:*` or `:bridge:*` recovery rows).
    pub async fn get_adaptive_task_statuses_by_key_or_subkey(
        &self,
        idempotency_key: &str,
    ) -> Result<Vec<SchedulerTaskStatusRow>, SchedulerError> {
        let subkey_prefix = format!("{idempotency_key}:");
        let rows: Vec<(String, i32, i32)> = match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                // Keep a binary-collated range predicate so SQLite uses `(job_type, idempotency_key)`
                // for both the exact lookup and the `:` subkey seek instead of scanning via `substr`.
                if let Some(subkey_upper) = prefix_upper_bound(&subkey_prefix) {
                    sqlx::query_as(
                        "SELECT status, attempts, max_attempts FROM Jobs \
                         WHERE job_type = ?1 \
                           AND (idempotency_key = ?2 OR (idempotency_key >= ?3 AND idempotency_key < ?4))",
                    )
                    .bind(ADAPTIVE_QUEUE)
                    .bind(idempotency_key)
                    .bind(&subkey_prefix)
                    .bind(subkey_upper)
                    .fetch_all(sqlite.pool())
                    .await?
                } else {
                    sqlx::query_as(
                        "SELECT status, attempts, max_attempts FROM Jobs \
                         WHERE job_type = ?1 \
                           AND (idempotency_key = ?2 OR idempotency_key >= ?3)",
                    )
                    .bind(ADAPTIVE_QUEUE)
                    .bind(idempotency_key)
                    .bind(&subkey_prefix)
                    .fetch_all(sqlite.pool())
                    .await?
                }
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                // Postgres `starts_with` is collation-independent (unlike `>= ':' AND < ';'`,
                // which misorders punctuation under ICU/libc locale collations such as `und`).
                sqlx::query_as(
                    "SELECT status, attempts, max_attempts FROM apalis.jobs \
                     WHERE job_type = $1 \
                       AND (idempotency_key = $2 OR starts_with(idempotency_key, $3))",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(idempotency_key)
                .bind(&subkey_prefix)
                .fetch_all(postgres.pool())
                .await?
            }
        };
        parse_status_rows(rows)
    }

    /// Lists idempotency keys of active adaptive tasks matching `prefix` using the partial
    /// `(job_type, idempotency_key)` index without decoding `job` payloads.
    pub async fn list_active_adaptive_idempotency_keys_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                if let Some(upper) = prefix_upper_bound(prefix) {
                    sqlx::query_scalar(
                        "SELECT idempotency_key FROM Jobs \
                         WHERE job_type = ?1 \
                           AND status IN ('Pending', 'Queued', 'Running', 'Failed') \
                           AND idempotency_key IS NOT NULL \
                           AND idempotency_key >= ?2 \
                           AND idempotency_key < ?3 \
                           AND (status != 'Failed' OR attempts < max_attempts)",
                    )
                    .bind(ADAPTIVE_QUEUE)
                    .bind(prefix)
                    .bind(upper)
                    .fetch_all(sqlite.pool())
                    .await
                    .map_err(Into::into)
                } else {
                    sqlx::query_scalar(
                        "SELECT idempotency_key FROM Jobs \
                         WHERE job_type = ?1 \
                           AND status IN ('Pending', 'Queued', 'Running', 'Failed') \
                           AND idempotency_key IS NOT NULL \
                           AND idempotency_key >= ?2 \
                           AND (status != 'Failed' OR attempts < max_attempts)",
                    )
                    .bind(ADAPTIVE_QUEUE)
                    .bind(prefix)
                    .fetch_all(sqlite.pool())
                    .await
                    .map_err(Into::into)
                }
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let pattern = escape_like_prefix(prefix);
                sqlx::query_scalar(
                    "SELECT idempotency_key FROM apalis.jobs \
                     WHERE job_type = $1 \
                       AND status IN ('Pending', 'Queued', 'Running', 'Failed') \
                       AND idempotency_key IS NOT NULL \
                       AND idempotency_key LIKE $2 ESCAPE '\\' \
                       AND (status <> 'Failed' OR attempts < max_attempts)",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(pattern)
                .fetch_all(postgres.pool())
                .await
                .map_err(Into::into)
            }
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn classify_task_key_state(row: Option<(String, i32, i32)>) -> AdaptiveTaskKeyState {
    let Some((status, attempts, max_attempts)) = row else {
        return AdaptiveTaskKeyState::Missing;
    };
    match status.as_str() {
        "Pending" | "Queued" | "Running" => AdaptiveTaskKeyState::Active,
        "Failed" if attempts < max_attempts => AdaptiveTaskKeyState::Active,
        "Done" => AdaptiveTaskKeyState::Done,
        _ => AdaptiveTaskKeyState::TerminalFailure,
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_adaptive_task_key_state(
    pool: &sqlx::SqlitePool,
    idempotency_key: &str,
) -> Result<AdaptiveTaskKeyState, SchedulerError> {
    let row: Option<(String, i32, i32)> = sqlx::query_as(
        "SELECT status, attempts, max_attempts \
         FROM Jobs \
         WHERE job_type = ?1 AND idempotency_key = ?2 \
         LIMIT 1",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await?;
    Ok(classify_task_key_state(row))
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn task_key_family_parts(idempotency_key: &str) -> (&str, String) {
    let base = idempotency_key
        .split_once(":watchdog:")
        .map_or(idempotency_key, |(prefix, _)| prefix);
    let watchdog_prefix = format!("{base}:watchdog:");
    (base, watchdog_prefix)
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn fold_task_key_family_state(rows: &[(String, i32, i32)]) -> AdaptiveTaskKeyState {
    let mut saw_done = false;
    let mut saw_terminal_failure = false;
    for (status, attempts, max_attempts) in rows {
        match classify_task_key_state(Some((status.clone(), *attempts, *max_attempts))) {
            AdaptiveTaskKeyState::Active => return AdaptiveTaskKeyState::Active,
            AdaptiveTaskKeyState::Done => saw_done = true,
            AdaptiveTaskKeyState::TerminalFailure => saw_terminal_failure = true,
            AdaptiveTaskKeyState::Missing => {}
        }
    }
    if saw_done {
        AdaptiveTaskKeyState::Done
    } else if saw_terminal_failure {
        AdaptiveTaskKeyState::TerminalFailure
    } else {
        AdaptiveTaskKeyState::Missing
    }
}

#[cfg(feature = "postgres")]
async fn postgres_adaptive_task_key_state(
    pool: &sqlx::PgPool,
    idempotency_key: &str,
) -> Result<AdaptiveTaskKeyState, SchedulerError> {
    let row: Option<(String, i32, i32)> = sqlx::query_as(
        "SELECT status, attempts, max_attempts \
         FROM apalis.jobs \
         WHERE job_type = $1 AND idempotency_key = $2 \
         LIMIT 1",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await?;
    Ok(classify_task_key_state(row))
}

#[cfg(feature = "sqlite")]
async fn sqlite_adaptive_task_key_family_state(
    pool: &sqlx::SqlitePool,
    idempotency_key: &str,
) -> Result<AdaptiveTaskKeyState, SchedulerError> {
    let (base, watchdog_prefix) = task_key_family_parts(idempotency_key);
    let rows: Vec<(String, i32, i32)> = sqlx::query_as(
        "SELECT status, attempts, max_attempts \
         FROM Jobs \
         WHERE job_type = ?1 \
           AND (idempotency_key = ?2 OR substr(idempotency_key, 1, length(?3)) = ?3)",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(base)
    .bind(watchdog_prefix)
    .fetch_all(pool)
    .await?;
    Ok(fold_task_key_family_state(&rows))
}

#[cfg(feature = "postgres")]
async fn postgres_adaptive_task_key_family_state(
    pool: &sqlx::PgPool,
    idempotency_key: &str,
) -> Result<AdaptiveTaskKeyState, SchedulerError> {
    let (base, watchdog_prefix) = task_key_family_parts(idempotency_key);
    let rows: Vec<(String, i32, i32)> = sqlx::query_as(
        "SELECT status, attempts, max_attempts \
         FROM apalis.jobs \
         WHERE job_type = $1 \
           AND (idempotency_key = $2 OR starts_with(idempotency_key, $3))",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(base)
    .bind(watchdog_prefix)
    .fetch_all(pool)
    .await?;
    Ok(fold_task_key_family_state(&rows))
}

#[cfg(feature = "sqlite")]
async fn sqlite_find_active_adaptive_key_in_family(
    pool: &sqlx::SqlitePool,
    idempotency_key: &str,
) -> Result<Option<String>, SchedulerError> {
    let (base, watchdog_prefix) = task_key_family_parts(idempotency_key);
    let key: Option<String> = sqlx::query_scalar(
        "SELECT idempotency_key \
         FROM Jobs \
         WHERE job_type = ?1 \
           AND idempotency_key IS NOT NULL \
           AND (idempotency_key = ?2 OR substr(idempotency_key, 1, length(?3)) = ?3) \
           AND (status IN ('Pending', 'Queued', 'Running') OR (status = 'Failed' AND attempts < max_attempts)) \
         ORDER BY CASE status WHEN 'Running' THEN 1 WHEN 'Queued' THEN 2 WHEN 'Pending' THEN 3 ELSE 4 END, run_at ASC \
         LIMIT 1",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(base)
    .bind(watchdog_prefix)
    .fetch_optional(pool)
    .await?;
    Ok(key)
}

#[cfg(feature = "postgres")]
async fn postgres_find_active_adaptive_key_in_family(
    pool: &sqlx::PgPool,
    idempotency_key: &str,
) -> Result<Option<String>, SchedulerError> {
    let (base, watchdog_prefix) = task_key_family_parts(idempotency_key);
    let key: Option<String> = sqlx::query_scalar(
        "SELECT idempotency_key \
         FROM apalis.jobs \
         WHERE job_type = $1 \
           AND idempotency_key IS NOT NULL \
           AND (idempotency_key = $2 OR starts_with(idempotency_key, $3)) \
           AND (status IN ('Pending', 'Queued', 'Running') OR (status = 'Failed' AND attempts < max_attempts)) \
         ORDER BY CASE status WHEN 'Running' THEN 1 WHEN 'Queued' THEN 2 WHEN 'Pending' THEN 3 ELSE 4 END, run_at ASC \
         LIMIT 1",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(base)
    .bind(watchdog_prefix)
    .fetch_optional(pool)
    .await?;
    Ok(key)
}

#[cfg(feature = "sqlite")]
fn build_sqlite_task<Args>(task: SchedulerPushTask<Args>) -> apalis_sqlite::SqliteTask<Args> {
    build_apalis_task(task)
}

#[cfg(feature = "postgres")]
fn build_postgres_task<Args>(task: SchedulerPushTask<Args>) -> apalis_postgres::PgTask<Args> {
    build_apalis_task(task)
}

fn build_apalis_task<Args>(task: SchedulerPushTask<Args>) -> apalis_core::task::Task<Args> {
    let builder = TaskBuilder::new(task.args);
    let builder = match task.idempotency_key {
        Some(idempotency_key) => builder.idempotency_key(idempotency_key),
        None => builder,
    };
    let builder = match task.run_at_unix_secs {
        Some(run_at_unix_secs) => builder.run_at_timestamp(run_at_unix_secs),
        None => builder,
    };
    builder.build()
}

#[cfg(all(test, feature = "sqlite"))]
async fn insert_sqlite_task<Args>(
    pool: &sqlx::SqlitePool,
    queue: &str,
    task: SchedulerPushTask<Args>,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    insert_sqlite_task_with_mode(pool, queue, task, false).await
}

#[cfg(feature = "sqlite")]
async fn insert_sqlite_task_with_mode<Args>(
    pool: &sqlx::SqlitePool,
    queue: &str,
    task: SchedulerPushTask<Args>,
    expedite_pending: bool,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let max_attempts = task.max_attempts;
    let task = build_sqlite_task(task);
    let payload = encode_task_args(&task.args)?;
    let default_run_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let run_at = u64_to_i64(task.run_at().unwrap_or(default_run_at), "run_at_unix_secs")?;
    let default_max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    let max_attempts = max_attempts.unwrap_or(default_max_attempts);
    let priority = task
        .priority()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0);
    let metadata = serde_json::to_string(task.metadata())
        .map_err(|error| SchedulerError::Job(format!("encode task metadata: {error}")))?;
    let query = if expedite_pending {
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8) \
         ON CONFLICT(job_type, idempotency_key) DO UPDATE \
         SET run_at = MIN(Jobs.run_at, excluded.run_at) \
         WHERE Jobs.status = 'Pending' AND Jobs.attempts = 0"
    } else {
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8)"
    };
    sqlx::query(query)
        .bind(payload)
        .bind(ulid::Ulid::generate().to_string())
        .bind(queue)
        .bind(max_attempts)
        .bind(run_at)
        .bind(priority)
        .bind(metadata)
        .bind(task.idempotency_key().map(ToOwned::to_owned))
        .execute(pool)
        .await
        .map_err(SchedulerError::from_task_push_sqlx_error)?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn insert_postgres_task_with_mode<Args>(
    pool: &sqlx::PgPool,
    queue: &str,
    task: SchedulerPushTask<Args>,
    expedite_pending: bool,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let max_attempts = task.max_attempts;
    let task = build_postgres_task(task);
    let payload = encode_task_args(&task.args)?;
    let default_run_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let run_at = u64_to_i64(task.run_at().unwrap_or(default_run_at), "run_at_unix_secs")?;
    let run_at = chrono::DateTime::<chrono::Utc>::from_timestamp(run_at, 0)
        .ok_or_else(|| SchedulerError::Job("run_at outside chrono timestamp range".to_owned()))?;
    let default_max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    let max_attempts = max_attempts.unwrap_or(default_max_attempts);
    let priority = task
        .priority()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0);
    let query = if expedite_pending {
        "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
         VALUES ($1, $2, $3, 'Pending', 0, $4, $5, $6, ''::hstore, $7) \
         ON CONFLICT (job_type, idempotency_key) DO UPDATE \
         SET run_at = LEAST(apalis.jobs.run_at, EXCLUDED.run_at) \
         WHERE apalis.jobs.status = 'Pending' AND apalis.jobs.attempts = 0"
    } else {
        "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
         VALUES ($1, $2, $3, 'Pending', 0, $4, $5, $6, ''::hstore, $7)"
    };
    sqlx::query(query)
        .bind(ulid::Ulid::generate().to_string())
        .bind(queue)
        .bind(payload)
        .bind(max_attempts)
        .bind(run_at)
        .bind(priority)
        .bind(task.idempotency_key().map(ToOwned::to_owned))
        .execute(pool)
        .await
        .map_err(SchedulerError::from_task_push_sqlx_error)?;
    Ok(())
}

fn encode_task_args<Args>(args: &Args) -> Result<Vec<u8>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    apalis_codec::json::JsonCodec::<Vec<u8>>::default()
        .encode(args)
        .map_err(|error| SchedulerError::Job(format!("encode scheduler task: {error}")))
}

fn decode_task_args<Args>(bytes: &[u8]) -> Result<Args, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    apalis_codec::json::JsonCodec::<Vec<u8>>::default()
        .decode(&bytes.to_vec())
        .map_err(|error| SchedulerError::Job(format!("decode scheduler task: {error}")))
}

#[cfg(feature = "sqlite")]
fn sqlite_task_to_row<Args>(
    task: apalis_sqlite::SqliteTask,
) -> Result<SchedulerTaskRow<Args>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let args = decode_task_args(&task.args)?;
    let idempotency_key = task.idempotency_key().map(ToOwned::to_owned);
    let run_at = task.run_at().unwrap_or(0);
    let status = task.status();
    let attempts = task.attempt();
    let max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    scheduler_task_row(
        args,
        idempotency_key,
        run_at,
        status,
        attempts,
        max_attempts,
    )
}

#[cfg(feature = "postgres")]
fn postgres_task_to_row<Args>(
    task: apalis_postgres::PgTask,
) -> Result<SchedulerTaskRow<Args>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let args = decode_task_args(&task.args)?;
    let idempotency_key = task.idempotency_key().map(ToOwned::to_owned);
    let run_at = task.run_at().unwrap_or(0);
    let status = task.status();
    let attempts = task.attempt();
    let max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    scheduler_task_row(
        args,
        idempotency_key,
        run_at,
        status,
        attempts,
        max_attempts,
    )
}

fn scheduler_task_row<Args>(
    args: Args,
    idempotency_key: Option<String>,
    run_at_unix_secs: u64,
    status: TaskStatus,
    attempts: usize,
    max_attempts: i32,
) -> Result<SchedulerTaskRow<Args>, SchedulerError> {
    let attempts = i32::try_from(attempts)
        .map_err(|_| SchedulerError::Job("task attempts exceeds i32::MAX".to_owned()))?;
    Ok(SchedulerTaskRow {
        args,
        idempotency_key,
        run_at_unix_secs,
        status,
        attempts,
        max_attempts,
    })
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, SchedulerError> {
    i64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i64::MAX")))
}

fn parse_task_status(raw: &str) -> Result<TaskStatus, SchedulerError> {
    match raw {
        "Pending" => Ok(TaskStatus::Pending),
        "Queued" => Ok(TaskStatus::Queued),
        "Running" => Ok(TaskStatus::Running),
        "Failed" => Ok(TaskStatus::Failed),
        "Killed" => Ok(TaskStatus::Killed),
        "Done" => Ok(TaskStatus::Done),
        _ => raw
            .parse::<TaskStatus>()
            .map_err(|_| SchedulerError::Job(format!("unknown scheduler task status `{raw}`"))),
    }
}

fn parse_status_rows(
    rows: Vec<(String, i32, i32)>,
) -> Result<Vec<SchedulerTaskStatusRow>, SchedulerError> {
    rows.into_iter()
        .map(|(status, attempts, max_attempts)| {
            Ok(SchedulerTaskStatusRow {
                status: parse_task_status(&status)?,
                attempts,
                max_attempts,
            })
        })
        .collect()
}

#[cfg(feature = "sqlite")]
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let code = last as u32;
        let next_code = match code {
            0xD7FF => Some(0xE000),
            0x10_FFFF => None,
            _ => Some(code + 1),
        };
        if let Some(ch) = next_code.and_then(char::from_u32) {
            chars.push(ch);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

#[cfg(feature = "postgres")]
fn escape_like_prefix(prefix: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + 2);
    for ch in prefix.chars() {
        match ch {
            '\\' | '%' | '_' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('%');
    out
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use cc_lb_storage_api::CacheTtl;
    use sqlx::SqlitePool;
    use uuid::Uuid;

    use crate::jobs::cache_keepalive::CacheKeepaliveJob;
    use crate::worker::{AdaptiveJob, SchedulerPushTask};

    use super::{ADAPTIVE_QUEUE, insert_sqlite_task};

    const CREATE_JOBS: &str = "
        CREATE TABLE Jobs (
            job BLOB NOT NULL,
            id TEXT NOT NULL UNIQUE,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 25,
            run_at INTEGER NOT NULL,
            last_result TEXT,
            lock_at INTEGER,
            lock_by TEXT,
            done_at INTEGER,
            priority INTEGER NOT NULL DEFAULT 0,
            metadata TEXT,
            idempotency_key TEXT
        )
    ";

    #[tokio::test]
    async fn sqlite_insert_uses_task_max_attempts_override() {
        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        sqlx::query(CREATE_JOBS)
            .execute(&pool)
            .await
            .expect("create Jobs table");
        let job = CacheKeepaliveJob {
            session_key_hash: "session-hash".to_owned(),
            generation: 1,
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::from_u128(1),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        };

        insert_sqlite_task(
            &pool,
            ADAPTIVE_QUEUE,
            SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(job),
                idempotency_key: Some("cache_keepalive:session-hash:1".to_owned()),
                run_at_unix_secs: Some(1_800_000_270),
                max_attempts: Some(1),
            },
        )
        .await
        .expect("insert scheduler task");

        let max_attempts: i64 = sqlx::query_scalar("SELECT max_attempts FROM Jobs")
            .fetch_one(&pool)
            .await
            .expect("read max_attempts");
        assert_eq!(max_attempts, 1);
    }

    #[tokio::test]
    async fn sqlite_insert_defaults_max_attempts_to_five_when_none() {
        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        sqlx::query(CREATE_JOBS)
            .execute(&pool)
            .await
            .expect("create Jobs table");
        let job = CacheKeepaliveJob {
            session_key_hash: "session-hash".to_owned(),
            generation: 1,
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::from_u128(1),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        };

        insert_sqlite_task(
            &pool,
            ADAPTIVE_QUEUE,
            SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(job),
                idempotency_key: Some("cache_keepalive:session-hash:default".to_owned()),
                run_at_unix_secs: Some(1_800_000_270),
                max_attempts: None,
            },
        )
        .await
        .expect("insert scheduler task");

        let max_attempts: i64 = sqlx::query_scalar("SELECT max_attempts FROM Jobs")
            .fetch_one(&pool)
            .await
            .expect("read max_attempts");
        assert_eq!(max_attempts, 5);
    }

    #[tokio::test]
    async fn sqlite_indexed_queries_skip_corrupt_job_payloads() {
        use std::sync::Arc;

        use cc_lb_clock::SystemClock;

        use crate::worker::{SchedulerBackend, SqliteSchedulerBackend, TaskStatus};

        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        apalis_sqlite::SqliteStorage::setup(&pool)
            .await
            .expect("apalis setup");
        crate::migrations::apply_post_setup_migrations(&pool)
            .await
            .expect("post setup migrations");

        let upstream_id = Uuid::new_v4();
        let active_key = format!("adaptive:warmup:{upstream_id}:100");
        let exhausted_key = format!("adaptive:warmup:{upstream_id}:exhausted");
        let done_key = format!("adaptive:warmup:{upstream_id}:done");

        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)",
        )
        .bind(b"{corrupt-not-json".to_vec())
        .bind(ulid::Ulid::generate().to_string())
        .bind(ADAPTIVE_QUEUE)
        .bind("Pending")
        .bind(0_i32)
        .bind(5_i32)
        .bind(&active_key)
        .execute(&pool)
        .await
        .expect("insert corrupt active row");

        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)",
        )
        .bind(b"null".to_vec())
        .bind(ulid::Ulid::generate().to_string())
        .bind(ADAPTIVE_QUEUE)
        .bind("Failed")
        .bind(5_i32)
        .bind(5_i32)
        .bind(&exhausted_key)
        .execute(&pool)
        .await
        .expect("insert exhausted failed row");

        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)",
        )
        .bind(b"null".to_vec())
        .bind(ulid::Ulid::generate().to_string())
        .bind(ADAPTIVE_QUEUE)
        .bind("Done")
        .bind(1_i32)
        .bind(5_i32)
        .bind(&done_key)
        .execute(&pool)
        .await
        .expect("insert done row");

        let backend =
            SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(pool, Arc::new(SystemClock)));

        let statuses = backend
            .get_adaptive_task_statuses_by_key(&active_key)
            .await
            .expect("indexed status query ignores corrupt job payload");
        assert_eq!(
            statuses,
            vec![super::SchedulerTaskStatusRow {
                status: TaskStatus::Pending,
                attempts: 0,
                max_attempts: 5,
            }]
        );

        let active_keys = backend
            .list_active_adaptive_idempotency_keys_by_prefix("adaptive:warmup:")
            .await
            .expect("indexed prefix query ignores corrupt job payload");
        assert_eq!(active_keys, vec![active_key]);
    }

    #[tokio::test]
    async fn get_adaptive_task_statuses_by_key_or_subkey_matches_exact_and_colons_only() {
        use std::sync::Arc;

        use cc_lb_clock::SystemClock;

        use crate::worker::{SchedulerBackend, SqliteSchedulerBackend, TaskStatus};

        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        apalis_sqlite::SqliteStorage::setup(&pool)
            .await
            .expect("apalis setup");
        crate::migrations::apply_post_setup_migrations(&pool)
            .await
            .expect("post setup migrations");

        let base_key = format!("adaptive:oauth_refresh:{}:gen:0:exp:100", Uuid::new_v4());
        let rows = [
            (base_key.clone(), "Pending", 0_i32, 5_i32),
            (format!("{base_key}:watchdog:1"), "Running", 1_i32, 3_i32),
            (format!("{base_key}:bridge:abc"), "Done", 1_i32, 2_i32),
            (format!("{base_key}_other"), "Pending", 0_i32, 5_i32),
            (format!("{base_key};other"), "Pending", 0_i32, 5_i32),
            (format!("other:{base_key}"), "Pending", 0_i32, 5_i32),
        ];
        for (key, status, attempts, max_attempts) in rows {
            sqlx::query(
                "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)",
            )
            .bind(b"null".to_vec())
            .bind(ulid::Ulid::generate().to_string())
            .bind(ADAPTIVE_QUEUE)
            .bind(status)
            .bind(attempts)
            .bind(max_attempts)
            .bind(key)
            .execute(&pool)
            .await
            .expect("insert job row");
        }

        let subkey_prefix = format!("{base_key}:");
        let subkey_upper =
            super::prefix_upper_bound(&subkey_prefix).expect("colon subkey prefix has upper bound");
        let plan_rows: Vec<(i32, i32, i32, String)> = sqlx::query_as(
            "EXPLAIN QUERY PLAN \
             SELECT status, attempts, max_attempts FROM Jobs \
             WHERE job_type = ?1 \
               AND (idempotency_key = ?2 OR (idempotency_key >= ?3 AND idempotency_key < ?4))",
        )
        .bind(ADAPTIVE_QUEUE)
        .bind(&base_key)
        .bind(&subkey_prefix)
        .bind(&subkey_upper)
        .fetch_all(&pool)
        .await
        .expect("explain sqlite subkey range query plan");
        let plan_details = plan_rows
            .into_iter()
            .map(|(_, _, _, detail)| detail)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan_details.contains("idempotency_key=?"),
            "expected exact idempotency_key index lookup in SQLite query plan: {plan_details}"
        );
        assert!(
            plan_details.contains("idempotency_key>?"),
            "expected bounded idempotency_key range seek in SQLite query plan: {plan_details}"
        );

        let backend =
            SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(pool, Arc::new(SystemClock)));
        let mut statuses = backend
            .get_adaptive_task_statuses_by_key_or_subkey(&base_key)
            .await
            .expect("query key or subkeys");
        statuses.sort_by_key(|row| format!("{:?}:{}", row.status, row.attempts));
        assert_eq!(
            statuses,
            vec![
                super::SchedulerTaskStatusRow {
                    status: TaskStatus::Done,
                    attempts: 1,
                    max_attempts: 2,
                },
                super::SchedulerTaskStatusRow {
                    status: TaskStatus::Pending,
                    attempts: 0,
                    max_attempts: 5,
                },
                super::SchedulerTaskStatusRow {
                    status: TaskStatus::Running,
                    attempts: 1,
                    max_attempts: 3,
                },
            ]
        );
    }

    #[tokio::test]
    async fn cancel_pending_adaptive_task_by_key_transitions_only_unstarted_pending() {
        use std::sync::Arc;

        use cc_lb_clock::SystemClock;

        use crate::worker::{SchedulerBackend, SqliteSchedulerBackend, TaskStatus};

        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        apalis_sqlite::SqliteStorage::setup(&pool)
            .await
            .expect("apalis setup");
        crate::migrations::apply_post_setup_migrations(&pool)
            .await
            .expect("post setup migrations");

        let pending_key = format!(
            "split_manual_warmup:{}:primary:{}",
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let queued_key = format!(
            "split_manual_warmup:{}:primary:{}",
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let running_key = format!(
            "split_manual_warmup:{}:primary:{}",
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let retried_pending_key = format!(
            "split_manual_warmup:{}:primary:{}",
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        for (key, status, attempts) in [
            (&pending_key, "Pending", 0_i32),
            (&queued_key, "Queued", 0_i32),
            (&running_key, "Running", 1_i32),
            (&retried_pending_key, "Pending", 1_i32),
        ] {
            sqlx::query(
                "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 1, 0, ?6)",
            )
            .bind(b"null".to_vec())
            .bind(ulid::Ulid::generate().to_string())
            .bind(ADAPTIVE_QUEUE)
            .bind(status)
            .bind(attempts)
            .bind(key)
            .execute(&pool)
            .await
            .expect("insert job row");
        }

        let backend =
            SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(pool, Arc::new(SystemClock)));
        assert!(
            backend
                .cancel_pending_adaptive_task_by_key(&pending_key)
                .await
                .expect("cancel pending")
        );
        assert!(
            !backend
                .cancel_pending_adaptive_task_by_key(&pending_key)
                .await
                .expect("second cancel is idempotent false")
        );
        assert!(
            !backend
                .cancel_pending_adaptive_task_by_key(&queued_key)
                .await
                .expect("cancel queued returns false")
        );
        assert!(
            !backend
                .cancel_pending_adaptive_task_by_key(&running_key)
                .await
                .expect("cancel running returns false")
        );
        assert!(
            !backend
                .cancel_pending_adaptive_task_by_key(&retried_pending_key)
                .await
                .expect("cancel retried returns false")
        );

        let statuses = backend
            .get_adaptive_task_statuses_by_key(&pending_key)
            .await
            .expect("status after cancel");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].status, TaskStatus::Killed);
        assert_eq!(
            backend
                .get_adaptive_task_statuses_by_key(&queued_key)
                .await
                .expect("queued untouched")[0]
                .status,
            TaskStatus::Queued
        );
        assert_eq!(
            backend
                .get_adaptive_task_statuses_by_key(&running_key)
                .await
                .expect("running untouched")[0]
                .status,
            TaskStatus::Running
        );
        assert_eq!(
            backend
                .get_adaptive_task_statuses_by_key(&retried_pending_key)
                .await
                .expect("retried untouched")[0]
                .status,
            TaskStatus::Pending
        );
    }

    #[test]
    fn prefix_upper_bound_computes_exclusive_range() {
        assert_eq!(
            super::prefix_upper_bound("adaptive:warmup:").as_deref(),
            Some("adaptive:warmup;")
        );
    }
}
