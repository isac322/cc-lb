#[cfg(feature = "sqlite")]
use apalis::prelude::{IntervalStrategy, StrategyBuilder, TaskSink, WorkerError};
#[cfg(feature = "sqlite")]
use std::collections::BTreeSet;
#[cfg(feature = "sqlite")]
use std::str::FromStr as _;
use std::sync::Arc;
#[cfg(feature = "sqlite")]
use std::time::Duration;

use cc_lb_clock::TestClock;
use cc_lb_config::SchedulerConfig;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJob;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::scheduler_metrics;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, SchedulerBackend, SchedulerPushTask, SqliteSchedulerBackend,
    build_adaptive_worker,
};
use cc_lb_scheduler::worker::{AdaptiveJob, CronJob, SchedulerCtx};
#[cfg(feature = "sqlite")]
use cc_lb_storage_api::CacheTtl;
#[cfg(feature = "sqlite")]
use metrics_exporter_prometheus::PrometheusBuilder;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
#[cfg(feature = "sqlite")]
use tempfile::TempDir;
#[cfg(feature = "sqlite")]
use tokio::sync::mpsc;
#[cfg(feature = "sqlite")]
use tokio::task::JoinHandle;
#[cfg(feature = "sqlite")]
use tokio::time::timeout;
#[cfg(feature = "sqlite")]
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_NOW_UNIX_SECS: u64 = 1_800_000_000;
#[cfg(feature = "sqlite")]
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "sqlite")]
#[test]
fn t3__worker_sqlite_runs_one_of_each_entity_job_to_done() -> Result<(), Box<dyn std::error::Error>>
{
    let recorder = PrometheusBuilder::new().build_recorder();
    let metrics_handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let db = sqlite_test_db().await?;
            let pool = db.pool.clone();
            let config = fast_queue_config(ADAPTIVE_QUEUE);
            let mut storage =
                apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(&pool, &config);
            let upstream_id = Uuid::from_u128(1);

            for job in entity_jobs(upstream_id) {
                storage.push(job).await?;
            }

            let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
                pool,
                Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
            ));
            let observer_pool = db.pool.clone();
            let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<&'static str>(8);
            let worker = build_adaptive_worker(&backend, dispatching_scheduler_ctx(dispatch_tx))?;
            let cancel = CancellationToken::new();
            let handle: JoinHandle<Result<(), WorkerError>> =
                tokio::spawn(worker.run_until_cancelled(cancel.clone()));

            let mut dispatched = BTreeSet::new();
            while dispatched.len() < 4 {
                let kind = timeout(COMPLETION_TIMEOUT, dispatch_rx.recv())
                    .await
                    .map_err(|_| {
                        format!("timed out waiting for entity-job dispatch; saw {dispatched:?}")
                    })?
                    .ok_or("dispatch channel closed before all four entity jobs ran")?;
                dispatched.insert(kind);
            }
            wait_for_done_entity_jobs(&observer_pool, 4).await?;
            cancel.cancel();
            handle.abort();
            let _ = handle.await;

            assert_eq!(
                dispatched,
                BTreeSet::from([
                    "cache_keepalive",
                    "metadata_refresh",
                    "oauth_refresh",
                    "warmup"
                ]),
                "worker must run one of each entity job to Ok(JobOutcome::Done)",
            );
            let rendered = metrics_handle.render();
            assert_eq!(
                counter_value(
                    &rendered,
                    scheduler_metrics::JOBS_TOTAL,
                    &[("job_type", "upstream_warmup"), ("status", "done")],
                ),
                1.0,
                "builder-wired scheduler metrics must count the completed warmup exactly once:\n{rendered}",
            );
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    })
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
async fn t3__scheduler_backend_sqlite_push_job_uses_full_idempotency_index()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    ));
    let upstream_id = Uuid::from_u128(2);
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

    sqlx::query("UPDATE Jobs SET status = 'Done', done_at = 1800000000")
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
async fn t3__scheduler_backend_routes_keepalive_to_dedicated_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    ));
    let upstream_id = Uuid::from_u128(3);
    backend
        .push_keepalive_task(SchedulerPushTask {
            args: AdaptiveJob::CacheKeepalive(keepalive_job(upstream_id)),
            idempotency_key: Some("cache_keepalive:worker-session:1".to_owned()),
            run_at_unix_secs: None,
            max_attempts: None,
        })
        .await?;
    backend
        .push_adaptive_task(SchedulerPushTask {
            args: AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
            idempotency_key: Some(format!("adaptive:metadata_refresh:{upstream_id}:1")),
            run_at_unix_secs: None,
            max_attempts: None,
        })
        .await?;

    let by_queue: Vec<(String, i64)> =
        sqlx::query_as("SELECT job_type, COUNT(*) FROM Jobs GROUP BY job_type ORDER BY job_type")
            .fetch_all(&pool)
            .await?;
    assert_eq!(
        by_queue,
        vec![
            ("adaptive".to_owned(), 1),
            ("cache_keepalive".to_owned(), 1)
        ],
        "keepalive must route to its own queue, isolated from the adaptive entity queue",
    );
    Ok(())
}

#[test]
fn worker_uses_default_entity_concurrency() {
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    );

    assert_eq!(
        ctx.config.entity_concurrency,
        SchedulerConfig::default().entity_concurrency
    );
    assert_eq!(ctx.config.entity_concurrency, 8);
}

#[tokio::test]
async fn t2__scheduler_ctx_default_dispatches_succeed() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    );
    let upstream_id = Uuid::from_u128(4);

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

#[cfg(feature = "sqlite")]
fn entity_jobs(upstream_id: Uuid) -> [AdaptiveJob; 4] {
    [
        AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)),
        AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
        AdaptiveJob::CacheKeepalive(keepalive_job(upstream_id)),
    ]
}

#[cfg(feature = "sqlite")]
fn keepalive_job(upstream_id: Uuid) -> CacheKeepaliveJob {
    CacheKeepaliveJob {
        session_key_hash: "worker-session".to_owned(),
        generation: 1,
        principal_id: "principal".to_owned(),
        upstream_id,
        ttl: CacheTtl::Ttl5m,
        cache_anchor_at_unix_secs: 100,
        expires_at_unix_secs: 400,
        refresh_delay_secs: 270,
        max_refreshes: 3,
        max_total_duration_secs: 600,
        traceparent: None,
    }
}

#[cfg(feature = "sqlite")]
fn dispatching_scheduler_ctx(dispatch_tx: mpsc::Sender<&'static str>) -> SchedulerCtx {
    SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(move |job| {
            let dispatch_tx = dispatch_tx.clone();
            Box::pin(async move {
                let kind = match job {
                    AdaptiveJob::Warmup(_) => "warmup",
                    AdaptiveJob::OAuthRefresh(_) => "oauth_refresh",
                    AdaptiveJob::MetadataRefresh(_) => "metadata_refresh",
                    AdaptiveJob::CacheKeepalive(_) => "cache_keepalive",
                };
                dispatch_tx
                    .send(kind)
                    .await
                    .expect("dispatch receiver remains open while worker runs");
                Ok(JobOutcome::Done)
            })
        }),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
}

#[cfg(feature = "sqlite")]
async fn wait_for_done_entity_jobs(
    pool: &sqlx::SqlitePool,
    expected: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    timeout(COMPLETION_TIMEOUT, async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            let done: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Done'",
            )
            .bind(ADAPTIVE_QUEUE)
            .fetch_one(pool)
            .await?;
            if done == expected {
                return Ok::<(), sqlx::Error>(());
            }
            if done > expected {
                return Err(sqlx::Error::Protocol(format!(
                    "observed {done} completed entity jobs, expected {expected}"
                )));
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for entity jobs to reach Done")??;
    Ok(())
}
#[cfg(feature = "sqlite")]
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
