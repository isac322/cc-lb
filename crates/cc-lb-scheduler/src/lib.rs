//! cc-lb-scheduler: apalis-backed distributed job scheduler with cron support.
//!
//! Cluster-safe cron scheduling is achieved via storage-level idempotency-key
//! deduplication, following the maintainer-recommended pattern from
//! <https://github.com/apalis-dev/apalis/issues/281>. Every replica runs its
//! own `CronStream`; duplicate pushes collide on the unique
//! `(job_type, idempotency_key)` index so exactly one job per tick is queued.

pub mod admin;
pub mod cron;
pub mod error;
pub mod jobs;
pub mod middleware;
pub mod migrations;
pub mod pool;
pub mod retry;
pub mod scheduler_metrics;
#[cfg(feature = "sqlite")]
mod sqlite_enqueue;
pub mod state_stores;
pub mod worker;
