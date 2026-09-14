#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use apalis::prelude::WorkerError;
use cc_lb_clock::TestClock;
use cc_lb_config::SchedulerConfig;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, PostgresSchedulerBackend, SchedulerBackend, SchedulerCtx,
    build_adaptive_worker,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);
const JOB_B_DELAY_SECS: i64 = 2;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t3_postgres__replacement_worker_dispatches_distinct_job_from_same_postgres_backend()
-> Result<()> {
    let fixture = crate::postgres_fixture().await?;
    let admin_pool = crate::scheduler_postgres_pool(&fixture).await?;
    let test_result = async {
        let admin_options = admin_pool.connect_options().as_ref().clone();
        let suffix = fixture.schema_name().trim_start_matches("cc_lb_test_");
        let database_name = format!("cclb_parity_{suffix}");
        let server_version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&admin_pool)
            .await
            .context("read PostgreSQL server version")?;
        ensure!(
            server_version.starts_with("18."),
            "restart parity test requires PostgreSQL 18, reached {server_version}"
        );

        let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
        sqlx::query(&create_database)
            .execute(&admin_pool)
            .await
            .with_context(|| format!("create isolated database {database_name}"))?;

        let test_result = run_in_isolated_database(admin_options, &database_name).await;
        let drop_database = format!(r#"DROP DATABASE "{database_name}" WITH (FORCE)"#);
        let drop_result = sqlx::query(&drop_database).execute(&admin_pool).await;

        match (test_result, drop_result) {
            (Ok((job_a, job_b)), Ok(_)) => {
                println!(
                    "postgres_restart_parity_receipt server_version={server_version} database={database_name} dispatched_a={job_a} generation_1_joined=true dispatched_b={job_b} generation_2_joined=true database_dropped=true"
                );
                Ok(())
            }
            (Err(test_error), Ok(_)) => Err(test_error.context(format!(
                "restart parity contract failed; isolated database {database_name} was dropped"
            ))),
            (Ok(_), Err(drop_error)) => Err(drop_error)
                .with_context(|| format!("drop isolated database {database_name} after contract")),
            (Err(test_error), Err(drop_error)) => Err(anyhow::anyhow!(
                "restart parity contract failed: {test_error:#}; dropping isolated database {database_name} also failed: {drop_error}"
            )),
        }
    }
    .await;
    admin_pool.close().await;
    let teardown_result = fixture.teardown().await;
    test_result?;
    teardown_result?;
    Ok(())
}

async fn run_in_isolated_database(
    admin_options: PgConnectOptions,
    database_name: &str,
) -> Result<(Uuid, Uuid)> {
    let pool = PgPoolOptions::new()
        .connect_with(admin_options.database(database_name))
        .await
        .with_context(|| format!("connect to isolated database {database_name}"))?;
    let result = run_two_generation_contract(&pool).await;
    pool.close().await;
    result
}

async fn run_two_generation_contract(pool: &sqlx::PgPool) -> Result<(Uuid, Uuid)> {
    apalis_postgres::PostgresStorage::setup(pool)
        .await
        .context("run apalis postgres setup")?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(pool)
        .await
        .context("run scheduler post-setup migrations")?;
    let backend = SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool.clone()));
    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);

    let job_a = Uuid::from_u128(1);
    backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_a)))
        .await
        .context("enqueue job A")?;
    let (cancel_a, worker_a) = start_worker(&backend, dispatch_tx.clone())?;
    let dispatched_a = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel_a, worker_a)
        .await
        .context("cancel and join generation 1")?;
    let dispatched_a = dispatched_a
        .context("generation 1 timed out waiting for job A")?
        .context("generation 1 dispatch channel closed before job A")?;
    ensure!(
        dispatched_a == job_a,
        "generation 1 dispatched the wrong job"
    );

    let job_b = Uuid::from_u128(2);
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
        "expected to defer exactly one job B row, updated {}",
        deferred.rows_affected()
    );

    let (cancel_b, worker_b) = start_worker(&backend, dispatch_tx)?;
    let dispatched_b = timeout(DISPATCH_TIMEOUT, dispatch_rx.recv()).await;
    cancel_and_join(cancel_b, worker_b)
        .await
        .context("cancel and join generation 2")?;
    let dispatched_b = dispatched_b
        .context("generation 2 timed out waiting for deferred job B")?
        .context("generation 2 dispatch channel closed before job B")?;
    ensure!(
        dispatched_b == job_b,
        "generation 2 dispatched the wrong job"
    );

    Ok((job_a, job_b))
}

fn start_worker(
    backend: &SchedulerBackend,
    dispatch_tx: mpsc::Sender<Uuid>,
) -> Result<(CancellationToken, JoinHandle<Result<(), WorkerError>>)> {
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
                        panic!("unexpected adaptive job in postgres restart lifetime test")
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
        Arc::new(TestClock::new_at_secs(1_800_000_000)),
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
) -> Result<()> {
    cancel.cancel();
    worker.await??;
    Ok(())
}
