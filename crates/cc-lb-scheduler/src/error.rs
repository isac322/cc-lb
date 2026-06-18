//! Error types for the scheduler.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("job error: {0}")]
    Job(String),

    #[error("leader election error: {0}")]
    LeaderElection(String),
}

pub type Result<T> = std::result::Result<T, SchedulerError>;
