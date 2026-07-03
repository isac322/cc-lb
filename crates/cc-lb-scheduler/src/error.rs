//! Error types for the scheduler.

use std::time::Duration;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("job error: {0}")]
    Job(String),

    #[error("scheduler task conflict: {0}")]
    Conflict(String),

    /// Handler exceeded the per-job timeout enforced inside the dispatcher.
    ///
    /// This error MUST reach the apalis ack path so the row is marked `Failed`
    /// (or retried by the retry class layer) rather than being dropped mid-flight
    /// by a tower `TimeoutLayer`, which historically left rows stuck in
    /// `status='Running'` forever.
    #[error("job {job_kind} exceeded timeout of {}ms", .timeout.as_millis())]
    JobTimeout {
        job_kind: &'static str,
        timeout: Duration,
    },
}

impl SchedulerError {
    pub(crate) fn from_task_push_sqlx_error(error: sqlx::Error) -> Self {
        if is_unique_constraint_error(&error) {
            Self::Conflict(error.to_string())
        } else {
            Self::Database(error)
        }
    }
}

#[allow(clippy::collapsible_if)]
fn is_unique_constraint_error(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    if let Some(code) = database_error.code() {
        if matches!(code.as_ref(), "23505" | "2067" | "1555") {
            return true;
        }
    }
    let message = database_error.message();
    message.contains("UNIQUE constraint failed")
        || message.contains("duplicate key value violates unique constraint")
}

pub type Result<T> = std::result::Result<T, SchedulerError>;
