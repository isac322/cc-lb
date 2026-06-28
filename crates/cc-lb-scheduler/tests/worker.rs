#![cfg(all(feature = "sqlite", not(feature = "postgres")))]

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::{IntervalStrategy, Status, StrategyBuilder, TaskSink};
use cc_lb_config::SchedulerConfig;
use cc_lb_core::clock::SystemClock;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJob;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{
    AdaptiveJob, CronJob, SchedulerBackend, SchedulerCtx, SqliteSchedulerStorage,
    build_adaptive_worker,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use uuid::Uuid;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn worker_sqlite_runs_one_of_each_entity_job_to_done()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    let queue = "entity_worker_sqlite";
    let config = fast_queue_config(queue);
    let mut storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(&pool, &config);
    let upstream_id = Uuid::new_v4();

    for job in entity_jobs(upstream_id) {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(
            &pool, &config,
        ),
        clock: Arc::new(SystemClock),
    });
    let worker = build_adaptive_worker(&backend, done_scheduler_ctx())?;
    worker.run_for(Duration::from_secs(5)).await?;

    let done_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = ?")
            .bind(queue)
            .bind(Status::Done.to_string())
            .fetch_one(&pool)
            .await?;
    let statuses: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status, COUNT(*) FROM Jobs WHERE job_type = ? GROUP BY status ORDER BY status",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await?;
    assert_eq!(done_count, 3, "statuses: {statuses:?}");
    Ok(())
}

#[cfg(feature = "sqlite")]
fn fast_queue_config(queue: &str) -> apalis_sqlite::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(10)))
        .build();
    apalis_sqlite::Config::new(queue)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(5)
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn scheduler_backend_sqlite_push_job_uses_full_idempotency_index()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let config = fast_queue_config("adaptive");
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(
            &pool, &config,
        ),
        clock: Arc::new(SystemClock),
    });
    let upstream_id = Uuid::new_v4();

    backend
        .push_job(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
            upstream_id,
            1,
        )))
        .await?;
    backend
        .push_job(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
            upstream_id,
            1,
        )))
        .await?;
    let active_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ? AND status IN ('Pending','Running','Queued')",
    )
    .bind(format!("adaptive:metadata_refresh:{upstream_id}:1"))
    .fetch_one(&pool)
    .await?;
    assert_eq!(active_count, 1);

    sqlx::query("UPDATE Jobs SET status = 'Done', done_at = strftime('%s', 'now')")
        .execute(&pool)
        .await?;
    backend
        .push_job(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
            upstream_id,
            1,
        )))
        .await?;
    let total_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?")
            .bind(format!("adaptive:metadata_refresh:{upstream_id}:1"))
            .fetch_one(&pool)
            .await?;
    assert_eq!(total_count, 1);
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn worker_sqlite_uses_default_entity_concurrency() {
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(SystemClock),
    );

    assert_eq!(
        ctx.config.entity_concurrency,
        SchedulerConfig::default().entity_concurrency
    );
    assert_eq!(ctx.config.entity_concurrency, 8);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn scheduler_ctx_default_dispatches_succeed() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(SystemClock),
    );
    let upstream_id = Uuid::new_v4();

    let entity =
        (ctx.adaptive_dispatch)(AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)))
            .await?;
    let singleton = (ctx.cron_dispatch)(CronJob::UsagePrune(UsagePruneJob::default())).await?;

    assert_eq!(entity, JobOutcome::Done);
    assert_eq!(singleton, JobOutcome::Done);
    Ok(())
}

#[cfg(feature = "sqlite")]
struct SqliteTestDb {
    pool: sqlx::SqlitePool,
    _dir: TempDir,
}

#[cfg(feature = "sqlite")]
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
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    Ok(SqliteTestDb { pool, _dir: dir })
}

fn entity_jobs(upstream_id: Uuid) -> [AdaptiveJob; 3] {
    [
        AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)),
        AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
    ]
}

fn done_scheduler_ctx() -> SchedulerCtx {
    SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(SystemClock),
    )
}
