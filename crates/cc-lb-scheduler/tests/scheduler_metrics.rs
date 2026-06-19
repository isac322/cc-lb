use std::convert::Infallible;
use std::error::Error;
use std::time::Duration;

use apalis::prelude::{IntervalStrategy, StrategyBuilder, TaskSink};
use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use cc_lb_scheduler::jobs::compat::AnthropicCompatRefreshJob;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollJob;
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJob;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use cc_lb_scheduler::jobs::quota_gc::SubscriptionQuotaGcJob;
use cc_lb_scheduler::jobs::reconcile::SchedulerReconcileJob;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJob;
use cc_lb_scheduler::jobs::usage_rollup::UsageRollupTask;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::middleware::SchedulerMetricsLayer;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::scheduler_metrics;
use cc_lb_scheduler::worker::{
    ENTITY_QUEUE, EntityJob, SINGLETON_QUEUE, SchedulerBackend, SchedulerCtx, SingletonJob,
    SqliteSchedulerStorage, build_entity_worker, build_singleton_worker,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio_util::sync::CancellationToken;
use tower::{Layer, ServiceExt, service_fn};
use uuid::Uuid;

#[cfg(feature = "sqlite")]
#[test]
fn scheduler_metrics_cover_worker_lifecycle_and_reconcile_done() -> Result<(), Box<dyn Error>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            scheduler_metrics::touch_scheduler_metric_handles();
            let pool = sqlite_memory().await?;
            run_entity_jobs(&pool).await?;
            run_singleton_jobs(&pool).await?;
            run_reconcile_metric_layer().await?;

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
                &[("job_type", "entity:oauth_refresh"), ("status", "done")],
                1.0,
            );
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[("job_type", "scheduler_reconcile"), ("status", "done")],
                1.0,
            );
            Ok::<(), Box<dyn Error>>(())
        })
    })?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn run_entity_jobs(pool: &sqlx::SqlitePool) -> Result<(), Box<dyn Error>> {
    let config = fast_queue_config(ENTITY_QUEUE);
    let mut storage =
        apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(pool, &config);
    for job in entity_jobs(Uuid::new_v4()) {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(pool, &config),
    });
    build_entity_worker(&backend, SchedulerCtx::default())?
        .run_for(Duration::from_secs(2))
        .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn run_singleton_jobs(pool: &sqlx::SqlitePool) -> Result<(), Box<dyn Error>> {
    let config = fast_queue_config(SINGLETON_QUEUE);
    let mut storage =
        apalis_sqlite::SqliteStorage::<SingletonJob, (), ()>::new_with_config(pool, &config);
    for job in singleton_jobs() {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_in_queue(
            pool,
            ENTITY_QUEUE,
        ),
    });
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        stop.cancel();
    });
    build_singleton_worker(&backend, SchedulerCtx::default())?
        .run_until_cancelled(cancel)
        .await?;
    Ok(())
}

async fn run_reconcile_metric_layer() -> Result<(), Box<dyn Error>> {
    let service =
        SchedulerMetricsLayer::new().layer(service_fn(|_job: SchedulerReconcileJob| async {
            Ok::<_, Infallible>(JobOutcome::Done)
        }));
    service.oneshot(SchedulerReconcileJob::default()).await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn sqlite_memory() -> Result<sqlx::SqlitePool, Box<dyn Error>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    Ok(pool)
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

fn singleton_jobs() -> [SingletonJob; 6] {
    [
        SingletonJob::UsageRollup(UsageRollupTask::default()),
        SingletonJob::UsagePrune(UsagePruneJob::default()),
        SingletonJob::QuotaGc(SubscriptionQuotaGcJob::default()),
        SingletonJob::PromptCachePurge(PromptCacheObservationPurgeJob::default()),
        SingletonJob::PriceCatalogRefresh(PriceCatalogRefreshJob::default()),
        SingletonJob::ApalisHousekeeping(ApalisHousekeepingJob::default()),
    ]
}

fn assert_required_metric_names(rendered: &str) {
    for name in [
        scheduler_metrics::JOBS_TOTAL,
        scheduler_metrics::JOB_DURATION_SECONDS,
        scheduler_metrics::FAILURES_TOTAL,
        scheduler_metrics::LEADER_ACQUIRED_TOTAL,
        scheduler_metrics::LEADER_LOST_TOTAL,
        scheduler_metrics::RECONCILE_ORPHAN_PRUNED_TOTAL,
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
