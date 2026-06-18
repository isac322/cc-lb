use std::time::Duration;

use apalis::prelude::{IntervalStrategy, Status, StrategyBuilder, TaskSink};
use cc_lb_config::SchedulerConfig;
use cc_lb_scheduler::jobs::compat::AnthropicCompatRefreshJob;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollJob;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::worker::{
    EntityJob, SchedulerBackend, SchedulerCtx, SqliteSchedulerStorage, build_entity_worker,
};
use uuid::Uuid;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn worker_sqlite_runs_one_of_each_entity_job_to_done()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlite_memory().await?;
    let queue = "entity_worker_sqlite";
    let config = fast_queue_config(queue);
    let mut storage =
        apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(&pool, &config);
    let upstream_id = Uuid::new_v4();

    for job in entity_jobs(upstream_id) {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(&pool, &config),
    });
    let worker = build_entity_worker(&backend, SchedulerCtx::default())?;
    worker.run_for(Duration::from_secs(2)).await?;

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
    assert_eq!(done_count, 5, "statuses: {statuses:?}");
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
async fn worker_sqlite_rejects_duplicate_entity_handler_registration() {
    use cc_lb_scheduler::worker::{EntityHandlerRegistry, EntityJobKind};

    let mut registry = EntityHandlerRegistry::default();
    registry
        .register(EntityJobKind::Warmup)
        .expect("first registration succeeds");
    let error = registry
        .register(EntityJobKind::Warmup)
        .expect_err("duplicate registration is rejected");

    assert!(error.to_string().contains("duplicate entity handler"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn worker_sqlite_uses_default_entity_concurrency() {
    let ctx = SchedulerCtx::default();

    assert_eq!(
        ctx.config.entity_concurrency,
        SchedulerConfig::default().entity_concurrency
    );
    assert_eq!(ctx.config.entity_concurrency, 8);
}

#[cfg(feature = "sqlite")]
async fn sqlite_memory() -> Result<sqlx::SqlitePool, Box<dyn std::error::Error>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    Ok(pool)
}

fn entity_jobs(upstream_id: Uuid) -> [EntityJob; 5] {
    [
        EntityJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)),
        EntityJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        EntityJob::OAuthUsagePoll(OAuthUsagePollJob {
            upstream_id,
            traceparent: None,
        }),
        EntityJob::AnthropicCompatRefresh(AnthropicCompatRefreshJob::new(
            "claude_code_stable_version",
        )),
        EntityJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
    ]
}
