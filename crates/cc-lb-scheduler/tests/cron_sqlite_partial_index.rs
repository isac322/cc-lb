#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_config::{Config, RecurringJobConfig};
use cc_lb_scheduler::leader_election::LeaderElection;
use cc_lb_scheduler::migrations::apply_post_setup_migrations;
use cc_lb_scheduler::worker::{
    ENTITY_QUEUE, EntityJob, SINGLETON_QUEUE, SchedulerBackend, SchedulerCtx,
    SqliteSchedulerStorage,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn sqlite_cron_producer_runs_after_partial_idempotency_index()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlite_memory().await?;
    apply_post_setup_migrations(&pool).await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_in_queue(
            &pool,
            ENTITY_QUEUE,
        ),
    });
    let cancel = CancellationToken::new();
    let handles = backend.spawn(
        fast_singleton_config(),
        SchedulerCtx::default(),
        Arc::new(LeaderElection::sqlite()),
        cancel.clone(),
    )?;

    let done_count = wait_for_done_singleton(&pool).await;
    cancel.cancel();
    stop_handles(handles).await;

    assert_eq!(done_count?, 1);
    Ok(())
}

async fn sqlite_memory() -> Result<sqlx::SqlitePool, Box<dyn std::error::Error>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    Ok(pool)
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

async fn wait_for_done_singleton(pool: &sqlx::SqlitePool) -> Result<i64, sqlx::Error> {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let done_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?1 AND status = 'Done'")
                .bind(SINGLETON_QUEUE)
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
