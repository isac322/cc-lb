#![cfg(feature = "sqlite")]

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_clock::SystemClock;
use cc_lb_config::{RecurringJobConfig, SchedulerConfig};
use cc_lb_scheduler::migrations::apply_post_setup_migrations;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{CRON_QUEUE, SchedulerBackend, SchedulerCtx, SqliteSchedulerBackend};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn sqlite_cron_producer_runs_after_partial_idempotency_index()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    apply_post_setup_migrations(&pool).await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
    let cancel = CancellationToken::new();
    let handles = backend
        .spawn(
            SchedulerCtx::new(
                fast_singleton_config(),
                Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
                Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
                Arc::new(SystemClock),
            ),
            cancel.clone(),
        )
        .await?;

    let done_count = wait_for_done_singleton(&pool).await;
    cancel.cancel();
    stop_handles(handles).await;

    assert_eq!(done_count?, 1);
    Ok(())
}

struct SqliteTestDb {
    pool: sqlx::SqlitePool,
    _dir: TempDir,
}

async fn sqlite_test_db() -> Result<SqliteTestDb, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let database_url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
    let options = SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options)
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    Ok(SqliteTestDb { pool, _dir: dir })
}

fn fast_singleton_config() -> SchedulerConfig {
    let mut config = SchedulerConfig::default();
    for job in config.recurring_jobs.values_mut() {
        job.enabled = false;
    }
    config.recurring_jobs.insert(
        "usage_prune".to_owned(),
        RecurringJobConfig {
            enabled: true,
            interval_secs: 2,
            jitter_secs: 0,
        },
    );
    config
}

async fn wait_for_done_singleton(pool: &sqlx::SqlitePool) -> Result<i64, sqlx::Error> {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let done_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?1 AND status = 'Done'")
                .bind(CRON_QUEUE)
                .fetch_one(pool)
                .await?;
        if done_count > 0 || Instant::now() >= deadline {
            return Ok(done_count);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn stop_handles(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
}
