#![cfg(all(feature = "sqlite", not(feature = "postgres")))]

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::WorkerError;
use cc_lb_clock::SystemClock;
use cc_lb_config::SchedulerConfig;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, SchedulerBackend, SchedulerCtx, SqliteSchedulerBackend,
    build_adaptive_worker,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);
const JOB_B_DELAY_SECS: i64 = 5;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_one_dispatches_due_job_and_joins_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = sqlite_backend().await?;
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);
    let job_a = Uuid::new_v4();
    fixture
        .backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_a)))
        .await?;

    let (cancel, worker) = start_worker(&fixture.backend, dispatch_tx)?;
    let dispatched_a = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel, worker).await?;

    let dispatched_a = dispatched_a.expect("generation 1 timed out waiting for job A");
    assert_eq!(dispatched_a, Some(job_a));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_worker_dispatches_distinct_job_from_same_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = sqlite_backend().await?;
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);
    let job_a = Uuid::new_v4();
    fixture
        .backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_a)))
        .await?;

    let (cancel_a, worker_a) = start_worker(&fixture.backend, dispatch_tx.clone())?;
    let dispatched_a = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel_a, worker_a).await?;
    let dispatched_a = dispatched_a.expect("generation 1 timed out waiting for job A");
    assert_eq!(dispatched_a, Some(job_a));

    let job_b = Uuid::new_v4();
    fixture
        .backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_b)))
        .await?;
    // The eager initial fetch must see no due work before B exercises continued polling.
    sqlx::query(
        "UPDATE Jobs SET run_at = strftime('%s', 'now') + ?1 WHERE job_type = ?2 AND idempotency_key LIKE ?3",
    )
    .bind(JOB_B_DELAY_SECS)
    .bind(ADAPTIVE_QUEUE)
    .bind(format!("adaptive:oauth_refresh:{job_b}:%"))
    .execute(&fixture.pool)
    .await?;
    let (cancel_b, worker_b) = start_worker(&fixture.backend, dispatch_tx.clone())?;
    let dispatched_b = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel_b, worker_b).await?;

    let dispatched_b = dispatched_b.expect(
        "generation 2 timed out waiting for distinct job B after generation 1 drained the retained poll strategy",
    );
    assert_eq!(dispatched_b, Some(job_b));
    Ok(())
}

struct SqliteFixture {
    backend: SchedulerBackend,
    pool: sqlx::SqlitePool,
    _dir: TempDir,
}

async fn sqlite_backend() -> Result<SqliteFixture, Box<dyn std::error::Error>> {
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
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        pool.clone(),
        Arc::new(SystemClock),
    ));
    Ok(SqliteFixture {
        backend,
        pool,
        _dir: dir,
    })
}

fn start_worker(
    backend: &SchedulerBackend,
    dispatch_tx: mpsc::Sender<Uuid>,
) -> Result<
    (CancellationToken, JoinHandle<Result<(), WorkerError>>),
    cc_lb_scheduler::error::SchedulerError,
> {
    let ctx = SchedulerCtx::new(
        SchedulerConfig::default(),
        Arc::new(move |job| {
            let dispatch_tx = dispatch_tx.clone();
            Box::pin(async move {
                let upstream_id = match job {
                    AdaptiveJob::OAuthRefresh(job) => job.upstream_id,
                    AdaptiveJob::Warmup(_)
                    | AdaptiveJob::MetadataRefresh(_)
                    | AdaptiveJob::CacheKeepalive(_) => {
                        panic!("unexpected adaptive job in restart lifetime test")
                    }
                };
                dispatch_tx
                    .send(upstream_id)
                    .await
                    .expect("dispatch receiver remains open while worker runs");
                Ok(JobOutcome::Done)
            })
        }),
        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        Arc::new(SystemClock),
    );
    let worker = build_adaptive_worker(backend, ctx)?;
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(worker.run_until_cancelled(worker_cancel));
    Ok((cancel, handle))
}

async fn cancel_and_join(
    cancel: CancellationToken,
    worker: JoinHandle<Result<(), WorkerError>>,
) -> Result<(), Box<dyn std::error::Error>> {
    cancel.cancel();
    worker.await??;
    Ok(())
}
