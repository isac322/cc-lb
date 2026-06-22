//! Database connection pooling for the scheduler.

use sqlx::Pool;

#[cfg(feature = "sqlite")]
pub type SchedulerPool = Pool<sqlx::Sqlite>;

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub type SchedulerPool = Pool<sqlx::Postgres>;
