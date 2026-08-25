#![cfg(all(feature = "sqlite", not(feature = "postgres")))]

use std::collections::BTreeSet;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::{IntervalStrategy, StrategyBuilder, TaskSink, WorkerError};
use cc_lb_clock::SystemClock;
use cc_lb_config::SchedulerConfig;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJob;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, CRON_QUEUE, CronJob, SchedulerBackend, SchedulerCtx,
    SchedulerPushTask, SqliteSchedulerBackend, build_adaptive_worker, build_cron_worker,
};
use cc_lb_storage_api::CacheTtl;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_sqlite_runs_one_of_each_entity_job_to_done()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    let queue = ADAPTIVE_QUEUE;
    let config = fast_queue_config(queue);
    let mut storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(&pool, &config);
    let upstream_id = Uuid::new_v4();

    for job in entity_jobs(upstream_id) {
        storage.push(job).await?;
    }

    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<&'static str>(8);
    let worker = build_adaptive_worker(&backend, dispatching_scheduler_ctx(dispatch_tx))?;
    let cancel = CancellationToken::new();
    let handle: JoinHandle<Result<(), WorkerError>> =
        tokio::spawn(worker.run_until_cancelled(cancel.clone()));

    // Completion-driven teardown (issue #267): wait until the worker has driven each
    // distinct entity job to Ok(JobOutcome::Done), then stop it. The former run_for(5s)
    // wall-clock budget raced job completion and flaked under load.
    let mut dispatched = BTreeSet::new();
    while dispatched.len() < 4 {
        let kind = timeout(COMPLETION_TIMEOUT, dispatch_rx.recv())
            .await
            .map_err(|_| format!("timed out waiting for entity-job dispatch; saw {dispatched:?}"))?
            .ok_or("dispatch channel closed before all four entity jobs ran")?;
        dispatched.insert(kind);
    }
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
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_sqlite_applies_retry_policy_before_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    let config = fast_queue_config(ADAPTIVE_QUEUE);
    let mut storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(&pool, &config);
    storage
        .push(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
            Uuid::new_v4(),
            1,
        )))
        .await?;

    let before_retry: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
        .fetch_one(&pool)
        .await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(move |_job| {
            let dispatch_tx = dispatch_tx.clone();
            Box::pin(async move {
                dispatch_tx
                    .send(())
                    .await
                    .expect("dispatch receiver remains open until the handler returns");
                Ok(JobOutcome::Retry {
                    delay: Duration::ZERO,
                })
            })
        }),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(SystemClock),
    );
    let worker = build_adaptive_worker(&backend, ctx)?;
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(worker.run_until_cancelled(cancel.clone()));

    timeout(COMPLETION_TIMEOUT, dispatch_rx.recv())
        .await
        .map_err(|_| "timed out waiting for retrying entity-job dispatch")?
        .ok_or("dispatch channel closed before retrying entity job ran")?;
    let row = timeout(COMPLETION_TIMEOUT, async {
        loop {
            let row: (String, i64, i64) = sqlx::query_as(
                "SELECT status, attempts, run_at FROM Jobs WHERE job_type = 'adaptive'",
            )
            .fetch_one(&pool)
            .await?;
            if row.0 == "Pending" && row.1 == 1 {
                return Ok::<_, sqlx::Error>(row);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "timed out waiting for retry acknowledgement")??;
    cancel.cancel();
    handle.abort();
    let _ = handle.await;

    let after_retry: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.0, "Pending");
    assert_eq!(row.1, 1);
    assert!(
        row.2 >= before_retry + 27,
        "retry policy delay must be persisted before acknowledgement: {row:?}",
    );
    assert!(
        row.2 <= after_retry + 33,
        "retry policy delay exceeded the adaptive jitter bound: {row:?}",
    );
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_worker_sqlite_applies_retry_policy_before_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    let config = fast_queue_config(CRON_QUEUE);
    let mut storage =
        apalis_sqlite::SqliteStorage::<CronJob, (), ()>::new_with_config(&pool, &config);
    storage
        .push(CronJob::UsagePrune(UsagePruneJob::default()))
        .await?;

    let before_retry: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
        .fetch_one(&pool)
        .await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(move |_job| {
            let dispatch_tx = dispatch_tx.clone();
            Box::pin(async move {
                dispatch_tx
                    .send(())
                    .await
                    .expect("dispatch receiver remains open until the handler returns");
                Ok(JobOutcome::Retry {
                    delay: Duration::ZERO,
                })
            })
        }),
        Arc::new(SystemClock),
    );
    let worker = build_cron_worker(&backend, ctx)?;
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(worker.run_until_cancelled(cancel.clone()));

    timeout(COMPLETION_TIMEOUT, dispatch_rx.recv())
        .await
        .map_err(|_| "timed out waiting for retrying cron-job dispatch")?
        .ok_or("dispatch channel closed before retrying cron job ran")?;
    let row = timeout(COMPLETION_TIMEOUT, async {
        loop {
            let row: (String, i64, i64) =
                sqlx::query_as("SELECT status, attempts, run_at FROM Jobs WHERE job_type = 'cron'")
                    .fetch_one(&pool)
                    .await?;
            if row.0 == "Pending" && row.1 == 1 {
                return Ok::<_, sqlx::Error>(row);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "timed out waiting for cron retry acknowledgement")??;
    cancel.cancel();
    handle.abort();
    let _ = handle.await;

    let after_retry: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.0, "Pending");
    assert_eq!(row.1, 1);
    assert!(
        row.2 >= before_retry + 54,
        "maintenance retry delay must be persisted before acknowledgement: {row:?}",
    );
    assert!(
        row.2 <= after_retry + 66,
        "retry policy delay exceeded the maintenance jitter bound: {row:?}",
    );
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
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
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
async fn scheduler_backend_routes_keepalive_to_dedicated_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let db = sqlite_test_db().await?;
    let pool = db.pool.clone();
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
    let upstream_id = Uuid::new_v4();

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

fn entity_jobs(upstream_id: Uuid) -> [AdaptiveJob; 4] {
    [
        AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)),
        AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
        AdaptiveJob::CacheKeepalive(keepalive_job(upstream_id)),
    ]
}

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
        Arc::new(SystemClock),
    )
}
