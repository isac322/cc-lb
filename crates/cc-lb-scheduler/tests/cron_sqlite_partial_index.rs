#![cfg(feature = "sqlite")]

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::{Config, RecurringJobConfig};
use cc_lb_scheduler::migrations::apply_post_setup_migrations;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{
    CRON_QUEUE, CronDispatchFn, SchedulerBackend, SchedulerCtx, SqliteSchedulerBackend,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WAIT_TIMEOUT: Duration = Duration::from_secs(4);

#[tokio::test]
async fn t3__sqlite_cron_producer_runs_after_partial_idempotency_index()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    let observer_pool = db.observer_pool.clone();
    apply_post_setup_migrations(&pool).await?;
    let clock: ClockHandle = Arc::new(TestClock::new_at(std::time::SystemTime::now()));
    let backend =
        SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(pool.clone(), clock.clone()));
    let started = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Barrier::new(2));
    let cron_dispatch: CronDispatchFn = Arc::new({
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        move |_job| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            Box::pin(async move {
                started.wait().await;
                release.wait().await;
                Ok(JobOutcome::Done)
            })
        }
    });
    let cancel = CancellationToken::new();
    let handles = backend
        .spawn(
            fast_singleton_config(),
            SchedulerCtx::new(
                Default::default(),
                Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
                cron_dispatch,
                clock.clone(),
            ),
            cancel.clone(),
        )
        .await?;

    tokio::time::timeout(WAIT_TIMEOUT, started.wait())
        .await
        .expect("first cron job should start before the barrier timeout");

    cancel.cancel();
    assert_eq!(count_singleton_jobs(&observer_pool).await?, 1);
    tokio::time::timeout(WAIT_TIMEOUT, release.wait())
        .await
        .expect("running cron job should reach the release barrier");
    stop_handles(handles).await?;
    assert_eq!(
        count_singleton_jobs_with_status(&observer_pool, "Done").await?,
        1,
        "exactly one matching cron job must be Done after worker shutdown",
    );

    Ok(())
}

struct SqliteTestDb {
    pool: sqlx::SqlitePool,
    observer_pool: sqlx::SqlitePool,
    _dir: TempDir,
}

async fn sqlite_test_db() -> Result<SqliteTestDb, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("scheduler.sqlite");
    let database_url = format!("sqlite://{}", path.display());
    let options = SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(4)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options.clone())
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    let observer_pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options)
        .await?;
    Ok(SqliteTestDb {
        pool,
        observer_pool,
        _dir: dir,
    })
}

fn fast_singleton_config() -> Config {
    let mut config = Config::default();
    for job in config.scheduler.recurring_jobs.values_mut() {
        job.enabled = false;
    }
    config.scheduler.recurring_jobs.insert(
        "usage_prune".to_owned(),
        RecurringJobConfig {
            enabled: true,
            interval_secs: 1,
            jitter_secs: 0,
        },
    );
    config
}

async fn count_singleton_jobs(pool: &sqlx::SqlitePool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?1")
        .bind(CRON_QUEUE)
        .fetch_one(pool)
        .await
}

async fn count_singleton_jobs_with_status(
    pool: &sqlx::SqlitePool,
    status: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?1 AND status = ?2")
        .bind(CRON_QUEUE)
        .bind(status)
        .fetch_one(pool)
        .await
}

async fn stop_handles(mut handles: Vec<JoinHandle<()>>) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(mut handle) = handles.pop() {
        match tokio::time::timeout(WAIT_TIMEOUT, &mut handle).await {
            Ok(result) => result?,
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                for handle in handles {
                    handle.abort();
                    let _ = handle.await;
                }
                return Err("timed out waiting for scheduler worker shutdown".into());
            }
        }
    }
    Ok(())
}
