#![cfg(feature = "postgres")]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use apalis::prelude::WorkerError;
use cc_lb_clock::{Clock as _, SystemClock, unix_secs};
use cc_lb_config::SchedulerConfig;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob,
    PostgresSchedulerBackend, SchedulerBackend, SchedulerCtx, SchedulerPushTask,
    build_adaptive_worker,
};
use cc_lb_storage_api::CacheTtl;
use futures_util::FutureExt as _;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);
const QA_TIMEOUT: Duration = Duration::from_secs(6);
const JOB_B_DELAY_SECS: i64 = 2;
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_worker_dispatches_distinct_job_from_same_postgres_backend() -> Result<()> {
    with_isolated_database(|pool| async move { run_two_generation_contract(&pool).await }).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_idle_worker_claims_future_keepalive_job_within_poll_interval() -> Result<()> {
    with_isolated_database(|pool| async move {
        let worker_pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_millis(120))
            .connect_with(pool.connect_options().as_ref().clone())
            .await?;
        let backend = postgres_backend(&worker_pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(2);
        let ctx = scheduler_ctx(adaptive_tx, mpsc::channel(1).0);
        let cancel = CancellationToken::new();
        let handles = guard_tasks(
            &cancel,
            backend.spawn(ctx, cancel.clone()).await?,
        );

        wait_for("cache_keepalive worker registration", QA_TIMEOUT, || {
            let pool = pool.clone();
            async move {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM apalis.workers WHERE worker_type = $1",
                )
                .bind(CACHE_KEEPALIVE_QUEUE)
                .fetch_one(&pool)
                .await?;
                Ok((count > 0).then_some(()))
            }
        })
        .await?;
        wait_for_listen(&pool, 3).await?;

        let mut held = vec![worker_pool.acquire().await?];
        let grab_pool = worker_pool.clone();
        let grabber = tokio::spawn(async move {
            while held.len() < 4 {
                if let Ok(conn) = grab_pool.acquire().await {
                    held.push(conn);
                }
            }
            sleep(Duration::from_millis(300)).await;
            drop(held);
        });
        let terminated: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND query ILIKE '%listen%'",
        )
        .fetch_all(&pool)
        .await?;
        ensure!(
            terminated.len() >= 3 && terminated.iter().all(|terminated| *terminated),
            "expected to terminate all three LISTEN connections"
        );
        grabber.await?;
        sleep(Duration::from_millis(300)).await;
        let upstream_id = Uuid::new_v4();
        let now_secs = unix_secs(SystemClock.now());
        let expires_at_unix_secs = now_secs + 6;
        let job = keepalive_job(upstream_id, expires_at_unix_secs);
        let idempotency_key = format!("qa01:keepalive:{upstream_id}");
        backend
            .push_keepalive_task(SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(job),
                idempotency_key: Some(idempotency_key.clone()),
                run_at_unix_secs: Some(now_secs + 2),
                max_attempts: Some(1),
            })
            .await?;

        let dispatched = wait_for_dispatch(&mut adaptive_rx, QA_TIMEOUT, |job| {
            matches!(job, AdaptiveJob::CacheKeepalive(item) if item.upstream_id == upstream_id)
        })
        .await?;
        ensure!(
            matches!(dispatched, AdaptiveJob::CacheKeepalive(item) if item.upstream_id == upstream_id),
            "keepalive dispatch contained the wrong upstream"
        );
        let (delta_ms, lock_before_expiry) = job_delta_and_expiry(&pool, &idempotency_key, expires_at_unix_secs).await?;
        ensure!(
            (-500.0..=2_500.0).contains(&delta_ms),
            "keepalive lock_at - run_at was {delta_ms}ms"
        );
        ensure!(lock_before_expiry, "keepalive lock_at must be before expires_at");

        stop_spawned_consumers(cancel, handles).await?;
        worker_pool.close().await;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_notify_wakes_due_now_job_before_slow_poll_interval() -> Result<()> {
    with_isolated_database(|pool| async move {
        let backend = postgres_backend(&pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(2);
        let ctx = scheduler_ctx(adaptive_tx, mpsc::channel(1).0);
        let (cancel, worker) = start_worker(&backend, ctx)?;
        wait_for_adaptive_worker(&pool).await?;
        wait_for_listen(&pool, 1).await?;

        let upstream_id = Uuid::new_v4();
        // Integer-second run_at must not make the <800ms assertion depend on
        // which fraction of the current second this test happened to start in.
        let now_secs = wait_for_early_database_second(&pool).await?;
        let idempotency_key = format!("qa02:adaptive:{upstream_id}");
        backend
            .push_adaptive_task(SchedulerPushTask {
                args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
                idempotency_key: Some(idempotency_key.clone()),
                run_at_unix_secs: Some(now_secs),
                max_attempts: Some(1),
            })
            .await?;
        let dispatched = wait_for_dispatch(&mut adaptive_rx, QA_TIMEOUT, |job| {
            matches!(job, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == upstream_id)
        })
        .await?;
        ensure!(
            matches!(dispatched, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == upstream_id),
            "adaptive dispatch contained the wrong upstream"
        );
        let delta_ms = job_delta(&pool, &idempotency_key).await?;
        ensure!(delta_ms < 800.0, "notify wake took {delta_ms}ms");
        cancel_and_join(cancel, worker).await
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_all_worker_queues_claim_future_run_at_after_idle() -> Result<()> {
    with_isolated_database(|pool| async move {
        let backend = postgres_backend(&pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(4);
        let (cron_tx, mut cron_rx) = mpsc::channel(2);
        let ctx = scheduler_ctx(adaptive_tx, cron_tx);
        let cancel = CancellationToken::new();
        let handles = guard_tasks(
            &cancel,
            backend.spawn(ctx, cancel.clone()).await?,
        );
        wait_for_worker_count(&pool, ADAPTIVE_QUEUE, 1).await?;
        wait_for_worker_count(&pool, CACHE_KEEPALIVE_QUEUE, 1).await?;
        wait_for_worker_count(&pool, CRON_QUEUE, 1).await?;
        sleep(Duration::from_millis(2_200)).await;

        let now_secs = unix_secs(SystemClock.now());
        let adaptive_id = Uuid::new_v4();
        let keepalive_id = Uuid::new_v4();
        let adaptive_key = format!("qa03:adaptive:{adaptive_id}");
        let keepalive_key = format!("qa03:keepalive:{keepalive_id}");
        let cron_key = format!("qa03:cron:{keepalive_id}");
        let run_at = now_secs + 2;
        backend
            .push_adaptive_task(SchedulerPushTask {
                args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(adaptive_id)),
                idempotency_key: Some(adaptive_key.clone()),
                run_at_unix_secs: Some(run_at),
                max_attempts: Some(1),
            })
            .await?;
        backend
            .push_keepalive_task(SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(keepalive_job(keepalive_id, now_secs + 8)),
                idempotency_key: Some(keepalive_key.clone()),
                run_at_unix_secs: Some(run_at),
                max_attempts: Some(1),
            })
            .await?;
        backend
            .push_cron_task(SchedulerPushTask {
                args: CronJob::UsagePrune(Default::default()),
                idempotency_key: Some(cron_key.clone()),
                run_at_unix_secs: Some(run_at),
                max_attempts: Some(1),
            })
            .await?;

        timeout(QA_TIMEOUT, async {
            let mut adaptive_seen = false;
            let mut keepalive_seen = false;
            let mut cron_seen = false;
            while !(adaptive_seen && keepalive_seen && cron_seen) {
                tokio::select! {
                    job = adaptive_rx.recv() => {
                        match job.context("adaptive dispatch channel closed")? {
                            AdaptiveJob::OAuthRefresh(item) if item.upstream_id == adaptive_id => {
                                ensure!(!adaptive_seen, "adaptive job dispatched twice");
                                adaptive_seen = true;
                            }
                            AdaptiveJob::CacheKeepalive(item) if item.upstream_id == keepalive_id => {
                                ensure!(!keepalive_seen, "keepalive job dispatched twice");
                                keepalive_seen = true;
                            }
                            other => bail!("unexpected adaptive dispatch: {other:?}"),
                        }
                    }
                    job = cron_rx.recv() => {
                        ensure!(
                            matches!(job.context("cron dispatch channel closed")?, CronJob::UsagePrune(_)),
                            "unexpected cron dispatch"
                        );
                        ensure!(!cron_seen, "cron job dispatched twice");
                        cron_seen = true;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("all three queues did not dispatch within six seconds")??;
        for key in [&adaptive_key, &keepalive_key, &cron_key] {
            let delta_ms = job_delta(&pool, key).await?;
            ensure!(delta_ms <= 2_500.0, "queue job {key} lock delta was {delta_ms}ms");
        }
        stop_spawned_consumers(cancel, handles).await
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_concurrent_workers_claim_distinct_due_jobs() -> Result<()> {
    with_isolated_database(|pool| async move {
        let backend = postgres_backend(&pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(4);
        let ctx = scheduler_ctx(adaptive_tx, mpsc::channel(1).0);
        let (cancel_a, worker_a) = start_worker(&backend, ctx.clone())?;
        let (cancel_b, worker_b) = start_worker(&backend, ctx)?;
        wait_for_adaptive_worker_count(&pool, 2).await?;

        let now_secs = unix_secs(SystemClock.now());
        let job_a = Uuid::new_v4();
        let job_b = Uuid::new_v4();
        for id in [job_a, job_b] {
            backend
                .push_adaptive_task(SchedulerPushTask {
                    args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(id)),
                    idempotency_key: Some(format!("qa06:adaptive:{id}")),
                    run_at_unix_secs: Some(now_secs),
                    max_attempts: Some(1),
                })
                .await?;
        }
        let seen = timeout(QA_TIMEOUT, async {
            let mut seen = Vec::new();
            while seen.len() < 2 {
                let job = adaptive_rx
                    .recv()
                    .await
                    .context("adaptive dispatch channel closed")?;
                match job {
                    AdaptiveJob::OAuthRefresh(item) => seen.push(item.upstream_id),
                    other => bail!("unexpected concurrent dispatch: {other:?}"),
                }
            }
            Ok::<_, anyhow::Error>(seen)
        })
        .await
        .context("concurrent workers did not dispatch both jobs within six seconds")??;
        let mut seen = seen;
        seen.sort_unstable();
        ensure!(
            seen == vec![job_a.min(job_b), job_a.max(job_b)],
            "jobs were not dispatched distinctly"
        );
        for id in [job_a, job_b] {
            let (status, attempts, locked): (String, i32, bool) =
                wait_for_job_done(&pool, &format!("qa06:adaptive:{id}")).await?;
            ensure!(
                status == "Done" && locked,
                "job {id} was not locked and completed"
            );
            ensure!(attempts == 1, "job {id} was handled {attempts} times");
        }
        cancel_and_join(cancel_a, worker_a).await?;
        cancel_and_join(cancel_b, worker_b).await?;
        ensure!(
            adaptive_rx.try_recv().is_err(),
            "duplicate concurrent job execution"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_cancel_leaves_unstarted_future_job_pending_for_next_worker() -> Result<()> {
    with_isolated_database(|pool| async move {
        let backend = postgres_backend(&pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(2);
        let ctx = scheduler_ctx(adaptive_tx, mpsc::channel(1).0);
        let (cancel_a, worker_a) = start_worker(&backend, ctx.clone())?;
        wait_for_adaptive_worker(&pool).await?;
        let upstream_id = Uuid::new_v4();
        let idempotency_key = format!("qa07a:adaptive:{upstream_id}");
        let now_secs = unix_secs(SystemClock.now());
        backend
            .push_adaptive_task(SchedulerPushTask {
                args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
                idempotency_key: Some(idempotency_key.clone()),
                run_at_unix_secs: Some(now_secs + 3),
                max_attempts: Some(1),
            })
            .await?;
        wait_for_pending_unlocked(&pool, &idempotency_key).await?;
        cancel_and_join(cancel_a, worker_a).await?;

        let (status, lock_is_null, still_future): (String, bool, bool) = sqlx::query_as(
            "SELECT status, lock_at IS NULL, run_at > clock_timestamp()
             FROM apalis.jobs WHERE idempotency_key = $1",
        )
        .bind(&idempotency_key)
        .fetch_one(&pool)
        .await?;
        ensure!(still_future, "worker 1 did not join before run_at");
        ensure!(status == "Pending" && lock_is_null, "cancelled worker changed future job state");

        let (cancel_b, worker_b) = start_worker(&backend, ctx)?;
        let dispatched = wait_for_dispatch(&mut adaptive_rx, QA_TIMEOUT, |job| {
            matches!(job, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == upstream_id)
        })
        .await?;
        ensure!(
            matches!(dispatched, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == upstream_id),
            "replacement worker dispatched the wrong job"
        );
        ensure!(job_delta(&pool, &idempotency_key).await? <= 2_500.0, "replacement worker was late");
        let (_, attempts, _) = wait_for_job_done(&pool, &idempotency_key).await?;
        ensure!(attempts == 1, "replacement worker handled the job more than once");
        cancel_and_join(cancel_b, worker_b).await?;
        ensure!(adaptive_rx.try_recv().is_err(), "future job was dispatched more than once");
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_worker_registers_heartbeat_and_claims_retryable_failed_row() -> Result<()> {
    with_isolated_database(|pool| async move {
        let backend = postgres_backend(&pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(2);
        let ctx = scheduler_ctx(adaptive_tx, mpsc::channel(1).0);
        let (cancel, worker) = start_worker(&backend, ctx)?;
        wait_for_adaptive_worker(&pool).await?;

        let upstream_id = Uuid::new_v4();
        let idempotency_key = format!("qa07b:adaptive:{upstream_id}");
        let payload = serde_json::to_vec(&AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)))?;
        let job_id = ulid::Ulid::generate().to_string();
        sqlx::query(
            "INSERT INTO apalis.jobs
                (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key)
             VALUES ($1, $2, $3, 'Failed', 1, 3, now() - interval '1 second', $4)",
        )
        .bind(payload)
        .bind(&job_id)
        .bind(ADAPTIVE_QUEUE)
        .bind(&idempotency_key)
        .execute(&pool)
        .await?;
        let dispatched = wait_for_dispatch(&mut adaptive_rx, QA_TIMEOUT, |job| {
            matches!(job, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == upstream_id)
        })
        .await?;
        ensure!(
            matches!(dispatched, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == upstream_id),
            "retryable failed row dispatched the wrong job"
        );
        ensure!(job_delta(&pool, &idempotency_key).await? <= 2_500.0, "retry dispatch was late");
        let (status, attempts, _) = wait_for_job_done(&pool, &idempotency_key).await?;
        ensure!(status == "Done" && attempts == 2, "failed row ended as {status} after {attempts} attempts");
        cancel_and_join(cancel, worker).await
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_notify_ignores_future_and_other_queue_rows() -> Result<()> {
    with_isolated_database(|pool| async move {
        let backend = postgres_backend(&pool);
        let (adaptive_tx, mut adaptive_rx) = mpsc::channel(2);
        let ctx = scheduler_ctx(adaptive_tx, mpsc::channel(1).0);
        let (cancel, worker) = start_worker(&backend, ctx)?;
        wait_for_adaptive_worker(&pool).await?;
        wait_for_listen(&pool, 1).await?;

        let now_secs = unix_secs(SystemClock.now());
        let adaptive_id = Uuid::new_v4();
        let keepalive_id = Uuid::new_v4();
        let adaptive_key = format!("qa07c:adaptive:{adaptive_id}");
        let keepalive_key = format!("qa07c:keepalive:{keepalive_id}");
        backend
            .push_adaptive_task(SchedulerPushTask {
                args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(adaptive_id)),
                idempotency_key: Some(adaptive_key.clone()),
                run_at_unix_secs: Some(now_secs + 3),
                max_attempts: Some(1),
            })
            .await?;
        backend
            .push_keepalive_task(SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(keepalive_job(keepalive_id, now_secs + 8)),
                idempotency_key: Some(keepalive_key.clone()),
                run_at_unix_secs: Some(now_secs),
                max_attempts: Some(1),
            })
            .await?;
        for (key, queue) in [
            (&adaptive_key, ADAPTIVE_QUEUE),
            (&keepalive_key, CACHE_KEEPALIVE_QUEUE),
        ] {
            let job_id: String = sqlx::query_scalar(
                "SELECT id FROM apalis.jobs WHERE idempotency_key = $1",
            )
            .bind(key)
            .fetch_one(&pool)
            .await?;
            let payload = serde_json::json!({ "job_type": queue, "id": job_id });
            sqlx::query("SELECT pg_notify('apalis::job::insert', $1)")
                .bind(payload.to_string())
                .execute(&pool)
                .await?;
        }
        // Observe every pre-due sample rather than sleeping a fixed duration:
        // integer-second scheduling can be less than three wall-clock seconds away.
        timeout(QA_TIMEOUT, async {
            loop {
                let (not_due, adaptive_unlocked, keepalive_unlocked): (bool, bool, bool) =
                    sqlx::query_as(
                        "SELECT a.run_at > clock_timestamp(),
                                a.lock_at IS NULL, k.lock_at IS NULL
                         FROM apalis.jobs a CROSS JOIN apalis.jobs k
                         WHERE a.idempotency_key = $1 AND k.idempotency_key = $2",
                    )
                    .bind(&adaptive_key)
                    .bind(&keepalive_key)
                    .fetch_one(&pool)
                    .await?;
                ensure!(keepalive_unlocked, "adaptive worker locked a keepalive row");
                if !not_due {
                    break;
                }
                ensure!(adaptive_unlocked, "adaptive job was claimed before run_at");
                ensure!(adaptive_rx.try_recv().is_err(), "job dispatched before run_at");
                sleep(POLL_INTERVAL).await;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("future row did not become due")??;

        let dispatched = wait_for_dispatch(&mut adaptive_rx, QA_TIMEOUT, |job| {
            matches!(job, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == adaptive_id)
        })
        .await?;
        ensure!(
            matches!(dispatched, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == adaptive_id),
            "adaptive worker dispatched the wrong queue row"
        );
        ensure!(job_delta(&pool, &adaptive_key).await? <= 2_500.0, "future adaptive dispatch was late");
        let (keepalive_status, keepalive_lock): (String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, lock_at FROM apalis.jobs WHERE idempotency_key = $1",
            )
            .bind(&keepalive_key)
            .fetch_one(&pool)
            .await?;
        ensure!(
            keepalive_status == "Pending" && keepalive_lock.is_none(),
            "adaptive worker claimed the keepalive queue row"
        );
        cancel_and_join(cancel, worker).await
    })
    .await
}

async fn with_isolated_database<T, F, Fut>(test: F) -> Result<T>
where
    F: FnOnce(sqlx::PgPool) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let admin_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL must be set to a PostgreSQL administrative database")?;
    ensure!(
        !admin_url.trim().is_empty(),
        "DATABASE_URL must not be empty"
    );
    let database_name = format!("cclb_parity_{}", Uuid::new_v4().simple());
    let mut database_url = Url::parse(&admin_url).context("parse DATABASE_URL")?;
    database_url.set_path(&database_name);
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .context("connect to PostgreSQL administrative database")?;
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&admin_pool)
        .await
        .context("read PostgreSQL server version")?;
    ensure!(
        server_version.starts_with("18."),
        "restart parity tests require PostgreSQL 18, reached {server_version}"
    );
    let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
    sqlx::query(sqlx::AssertSqlSafe(create_database))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("create isolated database {database_name}"))?;

    // Catch panics and bound the whole contract so failure paths still reach
    // DROP DATABASE. Worker guards abort their tasks when the test future drops.
    let test_result = timeout(
        Duration::from_secs(45),
        AssertUnwindSafe(async {
            let pool = PgPoolOptions::new()
                .max_connections(12)
                .connect(database_url.as_str())
                .await
                .with_context(|| format!("connect to isolated database {database_name}"))?;
            apalis_postgres::PostgresStorage::setup(&pool)
                .await
                .context("run apalis postgres setup")?;
            cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
                .await
                .context("run scheduler post-setup migrations")?;
            let result = test(pool.clone()).await;
            pool.close().await;
            result
        })
        .catch_unwind(),
    )
    .await;
    let test_result = match test_result {
        Ok(Ok(result)) => result,
        Ok(Err(_panic)) => Err(anyhow::anyhow!("isolated database test panicked")),
        Err(elapsed) => {
            Err(anyhow::Error::new(elapsed).context("isolated database test timed out"))
        }
    };
    let drop_database = format!(r#"DROP DATABASE "{database_name}" WITH (FORCE)"#);
    let drop_result = sqlx::query(sqlx::AssertSqlSafe(drop_database))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;
    match (test_result, drop_result) {
        (Ok(value), Ok(_)) => Ok(value),
        (Err(test_error), Ok(_)) => Err(test_error.context(format!(
            "isolated database {database_name} was dropped after test failure"
        ))),
        (Ok(_), Err(drop_error)) => {
            Err(drop_error).with_context(|| format!("drop isolated database {database_name}"))
        }
        (Err(test_error), Err(drop_error)) => Err(anyhow::anyhow!(
            "isolated database {database_name} test failed: {test_error:#}; drop also failed: {drop_error}"
        )),
    }
}

async fn run_two_generation_contract(pool: &sqlx::PgPool) -> Result<()> {
    let backend = postgres_backend(pool);
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);
    let job_a = Uuid::new_v4();
    backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_a)))
        .await
        .context("enqueue job A")?;
    let (cancel_a, worker_a) = start_worker(
        &backend,
        scheduler_ctx(dispatch_tx.clone(), mpsc::channel(1).0),
    )?;
    let dispatched_a = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel_a, worker_a)
        .await
        .context("cancel and join generation 1")?;
    let dispatched_a = dispatched_a
        .context("generation 1 timed out waiting for job A")?
        .context("generation 1 dispatch channel closed before job A")?;
    ensure!(matches!(dispatched_a, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == job_a));

    let job_b = Uuid::new_v4();
    ensure!(job_b != job_a, "jobs A and B must be distinct");
    backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_b)))
        .await
        .context("enqueue job B")?;
    let deferred = sqlx::query(
        "UPDATE apalis.jobs
         SET run_at = now() + ($1 * INTERVAL '1 second')
         WHERE job_type = $2
           AND convert_from(job, 'UTF8')::jsonb ->> 'type' = 'oauth_refresh'
           AND convert_from(job, 'UTF8')::jsonb #>> '{payload,upstream_id}' = $3",
    )
    .bind(JOB_B_DELAY_SECS)
    .bind(ADAPTIVE_QUEUE)
    .bind(job_b.to_string())
    .execute(pool)
    .await
    .context("defer job B")?;
    ensure!(
        deferred.rows_affected() == 1,
        "expected to defer exactly one job B row"
    );

    let (cancel_b, worker_b) =
        start_worker(&backend, scheduler_ctx(dispatch_tx, mpsc::channel(1).0))?;
    let dispatched_b = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel_b, worker_b)
        .await
        .context("cancel and join generation 2")?;
    let dispatched_b = dispatched_b
        .context("generation 2 timed out waiting for deferred job B")?
        .context("generation 2 dispatch channel closed before job B")?;
    ensure!(matches!(dispatched_b, AdaptiveJob::OAuthRefresh(item) if item.upstream_id == job_b));
    let delta_ms: f64 = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM (lock_at - run_at)) * 1000.0)::double precision
         FROM apalis.jobs
         WHERE job_type = $1
           AND convert_from(job, 'UTF8')::jsonb #>> '{payload,upstream_id}' = $2",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(job_b.to_string())
    .fetch_one(pool)
    .await
    .context("read generation 2 lock/run delta")?;
    ensure!(
        (-500.0..=2_500.0).contains(&delta_ms),
        "generation 2 lock_at - run_at was {delta_ms}ms"
    );
    Ok(())
}

fn postgres_backend(pool: &sqlx::PgPool) -> SchedulerBackend {
    SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool.clone()))
}

fn scheduler_ctx(
    adaptive_tx: mpsc::Sender<AdaptiveJob>,
    cron_tx: mpsc::Sender<CronJob>,
) -> SchedulerCtx {
    let mut config = SchedulerConfig::default();
    for job in config.recurring_jobs.values_mut() {
        job.enabled = false;
    }
    SchedulerCtx::new(
        config,
        Arc::new(move |job| {
            let tx = adaptive_tx.clone();
            Box::pin(async move {
                let _ = tx.send(job).await;
                Ok(JobOutcome::Done)
            })
        }),
        Arc::new(move |job| {
            let tx = cron_tx.clone();
            Box::pin(async move {
                let _ = tx.send(job).await;
                Ok(JobOutcome::Done)
            })
        }),
        Arc::new(SystemClock),
    )
}

fn start_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<(CancellationToken, WorkerTask<Result<(), WorkerError>>)> {
    let worker = build_adaptive_worker(backend, ctx)?;
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(worker.run_until_cancelled(cancel.clone()));
    Ok((cancel.clone(), WorkerTask::new(cancel, handle)))
}

async fn cancel_and_join(
    cancel: CancellationToken,
    worker: WorkerTask<Result<(), WorkerError>>,
) -> Result<()> {
    cancel.cancel();
    worker.join().await??;
    Ok(())
}

async fn stop_spawned_consumers(
    cancel: CancellationToken,
    handles: Vec<WorkerTask<()>>,
) -> Result<()> {
    cancel.cancel();
    for handle in handles {
        handle.join().await.context("join scheduler consumer")?;
    }
    Ok(())
}

fn keepalive_job(upstream_id: Uuid, expires_at_unix_secs: u64) -> CacheKeepaliveJob {
    CacheKeepaliveJob {
        session_key_hash: format!("qa-session-{upstream_id}"),
        generation: 1,
        principal_id: "qa-principal".to_owned(),
        upstream_id,
        ttl: CacheTtl::Ttl5m,
        cache_anchor_at_unix_secs: expires_at_unix_secs.saturating_sub(300),
        expires_at_unix_secs,
        refresh_delay_secs: 270,
        max_refreshes: 12,
        max_total_duration_secs: 14_400,
        traceparent: None,
    }
}

async fn wait_for_adaptive_worker(pool: &sqlx::PgPool) -> Result<()> {
    wait_for_adaptive_worker_count(pool, 1).await
}

async fn wait_for_adaptive_worker_count(pool: &sqlx::PgPool, expected: i64) -> Result<()> {
    wait_for_worker_count(pool, ADAPTIVE_QUEUE, expected).await
}

async fn wait_for_worker_count(pool: &sqlx::PgPool, queue: &str, expected: i64) -> Result<()> {
    wait_for("worker registration", QA_TIMEOUT, || async {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT id) FROM apalis.workers WHERE worker_type = $1",
        )
        .bind(queue)
        .fetch_one(pool)
        .await?;
        Ok((count >= expected).then_some(()))
    })
    .await
}

async fn wait_for_listen(pool: &sqlx::PgPool, expected: i64) -> Result<()> {
    wait_for("PostgreSQL LISTEN connection", QA_TIMEOUT, || async {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND state = 'idle'
               AND query ILIKE 'LISTEN %apalis::job::insert%'",
        )
        .fetch_one(pool)
        .await?;
        Ok((count >= expected).then_some(()))
    })
    .await
}

async fn wait_for_pending_unlocked(pool: &sqlx::PgPool, idempotency_key: &str) -> Result<()> {
    wait_for("pending unlocked job", QA_TIMEOUT, || {
        let pool = pool.clone();
        let idempotency_key = idempotency_key.to_owned();
        async move {
            let row: Option<(String, bool)> = sqlx::query_as(
                "SELECT status, lock_at IS NULL FROM apalis.jobs WHERE idempotency_key = $1",
            )
            .bind(idempotency_key)
            .fetch_optional(&pool)
            .await?;
            Ok(row.and_then(|(status, unlocked)| (status == "Pending" && unlocked).then_some(())))
        }
    })
    .await
}

async fn wait_for_dispatch<T>(
    receiver: &mut mpsc::Receiver<T>,
    duration: Duration,
    matches: impl Fn(&T) -> bool,
) -> Result<T> {
    let deadline = Instant::now() + duration;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("timed out waiting for adaptive dispatch")
        }
        let job = timeout(remaining, receiver.recv())
            .await
            .context("timed out waiting for adaptive dispatch")?
            .context("adaptive dispatch channel closed")?;
        if matches(&job) {
            return Ok(job);
        }
    }
}

async fn wait_for<T, F, Fut>(label: &str, duration: Duration, mut check: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    timeout(duration, async {
        loop {
            if let Some(value) = check().await? {
                return Ok(value);
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {label}"))?
}

async fn job_delta(pool: &sqlx::PgPool, idempotency_key: &str) -> Result<f64> {
    wait_for("job lock timestamp", QA_TIMEOUT, || {
        let pool = pool.clone();
        let idempotency_key = idempotency_key.to_owned();
        async move {
            let delta: Option<f64> = sqlx::query_scalar(
                "SELECT (EXTRACT(EPOCH FROM (lock_at - run_at)) * 1000.0)::double precision
                 FROM apalis.jobs
                 WHERE idempotency_key = $1 AND lock_at IS NOT NULL",
            )
            .bind(idempotency_key)
            .fetch_optional(&pool)
            .await?;
            Ok(delta)
        }
    })
    .await
}

async fn job_delta_and_expiry(
    pool: &sqlx::PgPool,
    idempotency_key: &str,
    expires_at_unix_secs: u64,
) -> Result<(f64, bool)> {
    wait_for("job lock timestamp and expiry", QA_TIMEOUT, || {
        let pool = pool.clone();
        let idempotency_key = idempotency_key.to_owned();
        async move {
            let row: Option<(f64, bool)> = sqlx::query_as(
                "SELECT (EXTRACT(EPOCH FROM (lock_at - run_at)) * 1000.0)::double precision,
                        lock_at < to_timestamp($2::bigint)
                 FROM apalis.jobs
                 WHERE idempotency_key = $1 AND lock_at IS NOT NULL",
            )
            .bind(idempotency_key)
            .bind(i64::try_from(expires_at_unix_secs).context("expiry exceeds i64")?)
            .fetch_optional(&pool)
            .await?;
            Ok(row)
        }
    })
    .await
}

struct WorkerTask<T> {
    cancel: CancellationToken,
    handle: JoinHandle<T>,
}

impl<T> WorkerTask<T> {
    fn new(cancel: CancellationToken, handle: JoinHandle<T>) -> Self {
        Self { cancel, handle }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl<T> Drop for WorkerTask<T> {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.handle.abort();
    }
}

fn guard_tasks(cancel: &CancellationToken, handles: Vec<JoinHandle<()>>) -> Vec<WorkerTask<()>> {
    handles
        .into_iter()
        .map(|handle| WorkerTask::new(cancel.clone(), handle))
        .collect()
}

async fn wait_for_job_done(
    pool: &sqlx::PgPool,
    idempotency_key: &str,
) -> Result<(String, i32, bool)> {
    wait_for("completed job acknowledgement", QA_TIMEOUT, || async {
        let row: (String, i32, bool) = sqlx::query_as(
            "SELECT status, attempts, lock_at IS NOT NULL AND lock_by IS NOT NULL
             FROM apalis.jobs WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_one(pool)
        .await?;
        Ok((row.0 == "Done").then_some(row))
    })
    .await
}

async fn wait_for_early_database_second(pool: &sqlx::PgPool) -> Result<u64> {
    wait_for(
        "early fractional database second",
        Duration::from_secs(2),
        || async {
            let (now_secs, fraction): (i64, f64) = sqlx::query_as(
                "WITH t AS (SELECT EXTRACT(EPOCH FROM clock_timestamp()) AS epoch)
             SELECT FLOOR(epoch)::bigint, (epoch - FLOOR(epoch))::double precision FROM t",
            )
            .fetch_one(pool)
            .await?;
            if fraction < 0.1 {
                Ok(Some(
                    u64::try_from(now_secs).context("negative PostgreSQL timestamp")?,
                ))
            } else {
                Ok(None)
            }
        },
    )
    .await
}
