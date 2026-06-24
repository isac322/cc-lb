use std::error::Error;
use std::str::FromStr as _;
use std::time::Duration;

use apalis::prelude::{IntervalStrategy, StrategyBuilder, TaskSink};
use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJob;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use cc_lb_scheduler::jobs::quota_gc::SubscriptionQuotaGcJob;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJob;
use cc_lb_scheduler::jobs::usage_rollup::UsageRollupTask;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::scheduler_metrics;
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, CRON_QUEUE, CronJob, SchedulerBackend, SchedulerCtx,
};
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::worker::{SqliteSchedulerStorage, build_adaptive_worker, build_cron_worker};
use metrics_exporter_prometheus::PrometheusBuilder;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(feature = "sqlite")]
#[test]
fn scheduler_metrics_cover_worker_lifecycle() -> Result<(), Box<dyn Error>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            scheduler_metrics::touch_scheduler_metric_handles();
            let db = sqlite_test_db().await?;
            let pool = db.pool.clone();
            run_entity_jobs(&pool).await?;
            run_singleton_jobs(&pool).await?;

            let rendered = handle.render();
            assert_required_metric_names(&rendered);
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[("job_type", "upstream_warmup"), ("status", "done")],
                1.0,
            );
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[("job_type", "adaptive:oauth_refresh"), ("status", "done")],
                1.0,
            );
            Ok::<(), Box<dyn Error>>(())
        })
    })?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn run_entity_jobs(pool: &sqlx::SqlitePool) -> Result<(), Box<dyn Error>> {
    let config = fast_queue_config(ADAPTIVE_QUEUE);
    let mut storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(pool, &config);
    for job in entity_jobs(Uuid::new_v4()) {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(
            pool, &config,
        ),
    });
    build_adaptive_worker(&backend, SchedulerCtx::default())?
        .run_for(Duration::from_secs(5))
        .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn run_singleton_jobs(pool: &sqlx::SqlitePool) -> Result<(), Box<dyn Error>> {
    let config = fast_queue_config(CRON_QUEUE);
    let mut storage =
        apalis_sqlite::SqliteStorage::<CronJob, (), ()>::new_with_config(pool, &config);
    for job in singleton_jobs() {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_in_queue(
            pool,
            ADAPTIVE_QUEUE,
        ),
    });
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        stop.cancel();
    });
    build_cron_worker(&backend, SchedulerCtx::default())?
        .run_until_cancelled(cancel)
        .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
struct SqliteTestDb {
    pool: sqlx::SqlitePool,
    _dir: TempDir,
}

#[cfg(feature = "sqlite")]
async fn sqlite_test_db() -> Result<SqliteTestDb, Box<dyn Error>> {
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

#[cfg(feature = "sqlite")]
fn fast_queue_config(queue: &str) -> apalis_sqlite::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(10)))
        .build();
    apalis_sqlite::Config::new(queue)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(16)
}

fn entity_jobs(upstream_id: Uuid) -> [AdaptiveJob; 3] {
    [
        AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)),
        AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
    ]
}

fn singleton_jobs() -> [CronJob; 7] {
    [
        CronJob::UsageRollup(UsageRollupTask::default()),
        CronJob::UsagePrune(UsagePruneJob::default()),
        CronJob::QuotaGc(SubscriptionQuotaGcJob::default()),
        CronJob::PromptCachePurge(PromptCacheObservationPurgeJob::default()),
        CronJob::PriceCatalogRefresh(PriceCatalogRefreshJob::default()),
        CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()),
        CronJob::OAuthUsagePoll(OAuthUsagePollCronJob::new(1_800_000_000)),
    ]
}

fn assert_required_metric_names(rendered: &str) {
    for name in [
        scheduler_metrics::JOBS_TOTAL,
        scheduler_metrics::JOB_DURATION_SECONDS,
        scheduler_metrics::FAILURES_TOTAL,
        scheduler_metrics::LEADER_ACQUIRED_TOTAL,
        scheduler_metrics::LEADER_LOST_TOTAL,
        scheduler_metrics::INIT_FAILURE,
        scheduler_metrics::LAZY_REFRESH_TIMEOUT_TOTAL,
        scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL,
        scheduler_metrics::QUOTA_GC_ROWS_REMOVED_TOTAL,
        scheduler_metrics::PRICE_CATALOG_STATUS_TOTAL,
    ] {
        assert!(
            rendered.contains(name),
            "missing {name} in prometheus output:\n{rendered}"
        );
    }
}

fn assert_counter_eq(rendered: &str, name: &str, labels: &[(&str, &str)], expected: f64) {
    let value = counter_value(rendered, name, labels);
    assert_eq!(
        value, expected,
        "counter {name} labels {labels:?}:\n{rendered}"
    );
}

fn counter_value(rendered: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let metric_prefix = format!("{name}{{");
    rendered
        .lines()
        .find(|line| {
            line.starts_with(&metric_prefix)
                && labels
                    .iter()
                    .all(|(label, value)| line.contains(&format!(r#"{label}="{value}""#)))
        })
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}
