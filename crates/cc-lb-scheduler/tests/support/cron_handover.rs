use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

use cc_lb_core::clock::{Clock, SystemClock};
use cc_lb_scheduler::{
    cron::WorkerBuilder as CronWorkerBuilder, leader_election::LeaderElection,
    worker::ADAPTIVE_QUEUE,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub const LOCK_KEY: i64 = 0x0000_CC1B_5CDE_0001_u64 as i64;
pub const TICK_INTERVAL: Duration = Duration::from_millis(500);

const CRON_QUEUE: &str = "task_41_cron_handover";
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CronTickJob {
    replica: String,
}

impl cc_lb_scheduler::cron::SingletonCronJob for CronTickJob {
    fn singleton_kind(&self) -> &'static str {
        "tick"
    }
}

#[derive(Clone, Debug)]
struct FastSchedule {
    next: Option<DateTime<Utc>>,
}

impl FastSchedule {
    const fn new() -> Self {
        Self { next: None }
    }
}

impl apalis_cron::Schedule<Utc> for FastSchedule {
    fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
        let interval = ChronoDuration::from_std(TICK_INTERVAL).ok()?;
        let now = DateTime::<Utc>::from(SystemClock.now());
        let next = self.next.unwrap_or(now + interval);
        self.next = Some(next + interval);
        Some(next)
    }
}

pub struct TickRow {
    pub replica: String,
    pub ticked_at: DateTime<Utc>,
}

pub fn spawn_cron_loop(
    replica: &'static str,
    pool: PgPool,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let storage = apalis_postgres::PostgresStorage::<CronTickJob>::new_with_notify(
                &pool,
                &apalis_postgres::Config::new(CRON_QUEUE),
            );
            let worker = CronWorkerBuilder::singleton_queue(
                CRON_QUEUE,
                FastSchedule::new(),
                storage,
                CronTickJob {
                    replica: replica.to_owned(),
                },
            );
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = worker.run(leader.as_ref()) => if result.is_err() { tokio::time::sleep(POLL_INTERVAL).await; },
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
}

pub async fn open_pool(url: &str) -> TestResult<PgPool> {
    Ok(PgPoolOptions::new().max_connections(8).connect(url).await?)
}

pub async fn setup_schema(pool: &PgPool) -> TestResult {
    apalis_postgres::PostgresStorage::setup(pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(pool).await?;
    Ok(())
}

pub async fn with_postgres_container<F, Fut>(run: F) -> TestResult
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = TestResult>,
{
    let docker_host = match std::env::var("DOCKER_HOST") {
        Ok(value) => value,
        Err(error) => {
            eprintln!(
                "SKIP: DOCKER_HOST not set for scheduler cron leader handover test; expected DOCKER_HOST=tcp://localhost:2375 ({error})"
            );
            return Ok(());
        }
    };
    let container = match Postgres::default().start().await {
        Ok(container) => container,
        Err(error) => {
            eprintln!(
                "SKIP: could not start postgres testcontainer using DOCKER_HOST={docker_host}: {error}"
            );
            return Ok(());
        }
    };
    let port = match container.get_host_port_ipv4(5432).await {
        Ok(port) => port,
        Err(error) => {
            eprintln!("SKIP: could not read postgres testcontainer port: {error}");
            return Ok(());
        }
    };
    run(format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
}

pub async fn cron_ticks(pool: &PgPool) -> TestResult<Vec<TickRow>> {
    let rows = sqlx::query_as::<_, (String, DateTime<Utc>)>(
        "SELECT convert_from(job, 'UTF8')::jsonb ->> 'replica', run_at FROM apalis.jobs WHERE job_type = $1 ORDER BY run_at",
    )
    .bind(CRON_QUEUE)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(replica, ticked_at)| TickRow { replica, ticked_at })
        .collect())
}

pub async fn count_replica_ticks(pool: &PgPool, replica: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND convert_from(job, 'UTF8')::jsonb ->> 'replica' = $2",
    )
    .bind(CRON_QUEUE)
    .bind(replica)
    .fetch_one(pool)
    .await?)
}

pub async fn count_replica_ticks_after(
    pool: &PgPool,
    replica: &str,
    after: DateTime<Utc>,
) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND convert_from(job, 'UTF8')::jsonb ->> 'replica' = $2 AND run_at > $3",
    )
    .bind(CRON_QUEUE)
    .bind(replica)
    .bind(after)
    .fetch_one(pool)
    .await?)
}

pub async fn wait_for_replica_ticks(pool: &PgPool, replica: &str, expected: i64) -> TestResult {
    wait_until(
        || async {
            count_replica_ticks(pool, replica)
                .await
                .map(|count| count >= expected)
        },
        "replica cron ticks",
    )
    .await
}

pub async fn wait_for_any_cron_tick(pool: &PgPool) -> TestResult {
    wait_until(
        || async { Ok(!cron_tick_replicas(pool).await?.is_empty()) },
        "any cron tick",
    )
    .await
}

pub async fn wait_for_done_entity_jobs(pool: &PgPool, expected: i64) -> TestResult {
    wait_until(
        || async {
            Ok(sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND status = 'Done'",
            )
            .bind(ADAPTIVE_QUEUE)
            .fetch_one(pool)
            .await?
                >= expected)
        },
        "done entity jobs",
    )
    .await
}

pub async fn cron_tick_replicas(pool: &PgPool) -> TestResult<HashSet<String>> {
    Ok(cron_ticks(pool)
        .await?
        .into_iter()
        .map(|tick| tick.replica)
        .collect())
}

pub async fn wait_for_entity_replicas(
    seen: &Arc<Mutex<Vec<String>>>,
    notify: &Arc<Notify>,
) -> TestResult {
    let wait = async {
        while seen.lock().await.iter().collect::<HashSet<_>>().len() < 2 {
            notify.notified().await;
        }
    };
    tokio::time::timeout(WAIT_TIMEOUT, wait)
        .await
        .map_err(|_| "timed out waiting for both entity replicas to process jobs")?;
    Ok(())
}

async fn wait_until<F, Fut>(mut check: F, label: &str) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = TestResult<bool>>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if check().await? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {label}").into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub fn assert_tick_handover_gap(ticks: &[TickRow]) -> TestResult {
    let max_allowed = chrono::Duration::from_std(2 * TICK_INTERVAL + Duration::from_millis(500))?;
    for pair in ticks.windows(2) {
        let gap = pair[1].ticked_at.signed_duration_since(pair[0].ticked_at);
        assert!(
            gap <= max_allowed,
            "tick gap {gap:?} exceeded handover allowance {max_allowed:?}"
        );
    }
    assert!(ticks.iter().any(|tick| tick.replica == "A"));
    assert!(ticks.iter().any(|tick| tick.replica == "B"));
    Ok(())
}
