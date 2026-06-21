use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use apalis_core::backend::codec::Codec as _;
use cc_lb_config::Config;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::admin::SCHEDULER_RECONCILE_QUEUE;
use crate::cron::WorkerBuilder as CronWorkerBuilder;
use crate::error::SchedulerError;
use crate::jobs::reconcile::SchedulerReconcileJob;
use crate::leader_election::LeaderElection;

const LEADER_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const RECONCILE_RECURRING_IDEMPOTENCY_KEY: &str = "scheduler_reconcile:recurring";
const RECONCILE_MAX_ATTEMPTS: i32 = 1;

#[cfg(feature = "postgres")]
use super::PostgresSchedulerStorage;
#[cfg(feature = "sqlite")]
use super::SqliteSchedulerStorage;
use super::{SINGLETON_QUEUE, SchedulerBackend, SingletonJob};

pub(super) fn spawn_cron_producer(
    backend: SchedulerBackend,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                run_sqlite_cron_producer(sqlite, config, leader, cancel).await;
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                run_postgres_cron_producer(postgres, config, leader, cancel).await;
            }
        }
    })
}

#[cfg(feature = "sqlite")]
async fn run_sqlite_cron_producer(
    sqlite: SqliteSchedulerStorage,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config) {
        let pool = sqlite.pool.clone();
        let leader = leader.clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            run_sqlite_singleton_cron_loop(pool, spec, leader, cancel).await;
        }));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

#[cfg(feature = "postgres")]
async fn run_postgres_cron_producer(
    postgres: PostgresSchedulerStorage,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config) {
        let pool = postgres.pool.clone();
        let leader = leader.clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            run_postgres_singleton_cron_loop(pool, spec, leader, cancel).await;
        }));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

#[cfg(feature = "postgres")]
async fn run_postgres_singleton_cron_loop(
    pool: sqlx::PgPool,
    spec: SingletonCronSpec,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let storage = apalis_postgres::PostgresStorage::<SingletonJob>::new_with_config(
            &pool,
            &apalis_postgres::Config::new(SINGLETON_QUEUE),
        );
        let worker = CronWorkerBuilder::singleton_queue(
            SINGLETON_QUEUE,
            spec.schedule.clone(),
            storage,
            spec.job.clone(),
        );
        run_one_cron_attempt(spec.name, worker, &leader, &cancel).await;
        if !sleep_or_cancel(LEADER_RETRY_INTERVAL, &cancel).await {
            break;
        }
    }
}

#[cfg(feature = "sqlite")]
async fn run_sqlite_singleton_cron_loop(
    pool: sqlx::SqlitePool,
    spec: SingletonCronSpec,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let storage =
            crate::sqlite_enqueue::SqliteSingletonCronStorage::new(pool.clone(), SINGLETON_QUEUE);
        let worker = CronWorkerBuilder::singleton_queue(
            SINGLETON_QUEUE,
            spec.schedule.clone(),
            storage,
            spec.job.clone(),
        );
        run_one_cron_attempt(spec.name, worker, &leader, &cancel).await;
        if !sleep_or_cancel(LEADER_RETRY_INTERVAL, &cancel).await {
            break;
        }
    }
}

async fn run_one_cron_attempt<Storage>(
    name: &'static str,
    worker: CronWorkerBuilder<SingletonJob, IntervalSchedule, Storage>,
    leader: &Arc<LeaderElection>,
    cancel: &CancellationToken,
) where
    Storage: apalis::prelude::TaskSink<SingletonJob> + Send + 'static,
    Storage::Error: std::error::Error + Send + Sync + 'static,
{
    tokio::select! {
        result = worker.run(leader.as_ref()) => {
            if let Err(error) = result {
                tracing::warn!(
                    job = name,
                    error = %error,
                    "scheduler cron producer worker exited with error; retrying after backoff",
                );
            }
        }
        _ = cancel.cancelled() => {}
    }
}

async fn sleep_or_cancel(duration: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => true,
        _ = cancel.cancelled() => false,
    }
}

async fn abort_join_handles(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
}

struct SingletonCronSpec {
    name: &'static str,
    schedule: IntervalSchedule,
    job: SingletonJob,
}

fn singleton_cron_specs(config: &Config) -> Vec<SingletonCronSpec> {
    let mut specs = Vec::new();
    push_singleton_spec(
        &mut specs,
        config,
        "usage_rollup",
        SingletonJob::UsageRollup(Default::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "usage_prune",
        SingletonJob::UsagePrune(Default::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "quota_gc",
        SingletonJob::QuotaGc(Default::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "prompt_cache_purge",
        SingletonJob::PromptCachePurge(Default::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "price_catalog_refresh",
        SingletonJob::PriceCatalogRefresh(Default::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "apalis_housekeeping",
        SingletonJob::ApalisHousekeeping(Default::default()),
    );
    specs
}

fn push_singleton_spec(
    specs: &mut Vec<SingletonCronSpec>,
    config: &Config,
    name: &'static str,
    job: SingletonJob,
) {
    let Some(job_config) = config.scheduler.recurring_jobs.get(name) else {
        return;
    };
    if !job_config.enabled {
        return;
    }
    specs.push(SingletonCronSpec {
        name,
        schedule: IntervalSchedule::new(name, job_config.interval_secs, job_config.jitter_secs),
        job,
    });
}

#[derive(Clone, Debug)]
struct IntervalSchedule {
    interval: chrono::Duration,
    first_delay: chrono::Duration,
    next: Option<DateTime<Utc>>,
}

impl IntervalSchedule {
    fn new(name: &str, interval_secs: u64, jitter_secs: u64) -> Self {
        let bounded_jitter = if jitter_secs == 0 {
            0
        } else {
            stable_hash(name) % jitter_secs.saturating_add(1)
        };
        Self {
            interval: chrono_seconds(interval_secs.max(1)),
            first_delay: chrono_seconds(interval_secs.max(1).saturating_add(bounded_jitter)),
            next: None,
        }
    }
}

impl apalis_cron::Schedule<Utc> for IntervalSchedule {
    fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
        let next = self.next.unwrap_or_else(|| Utc::now() + self.first_delay);
        self.next = Some(next + self.interval);
        Some(next)
    }
}

fn chrono_seconds(seconds: u64) -> chrono::Duration {
    let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
    chrono::Duration::try_seconds(seconds).unwrap_or(chrono::Duration::MAX)
}

fn stable_hash(value: &str) -> u64 {
    value.as_bytes().iter().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u64::from(*byte))
    })
}

pub(super) fn spawn_reconcile_producer(
    backend: SchedulerBackend,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let interval = Duration::from_secs(config.scheduler.reconcile_interval_secs.max(1));
    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let backend_for_work = backend.clone();
            let cancel_for_work = cancel.clone();
            let leader_result = leader
                .run(move || async move {
                    loop {
                        if cancel_for_work.is_cancelled() {
                            break;
                        }
                        match enqueue_recurring_reconcile(&backend_for_work).await {
                            Ok(true) => {
                                tracing::debug!(
                                    "scheduler_reconcile recurring tick enqueued"
                                );
                            }
                            Ok(false) => {
                                tracing::debug!(
                                    "scheduler_reconcile recurring tick skipped (active job present)"
                                );
                            }
                            Err(error) => {
                                tracing::warn!(
                                    error = %error,
                                    "scheduler_reconcile recurring enqueue failed",
                                );
                            }
                        }
                        if !sleep_or_cancel(interval, &cancel_for_work).await {
                            break;
                        }
                    }
                })
                .await;
            if let Err(error) = leader_result {
                tracing::warn!(
                    error = %error,
                    "scheduler_reconcile recurring leader run exited; retrying after backoff",
                );
            }
            if !sleep_or_cancel(LEADER_RETRY_INTERVAL, &cancel).await {
                break;
            }
        }
    })
}

async fn enqueue_recurring_reconcile(
    backend: &SchedulerBackend,
) -> Result<bool, SchedulerError> {
    let job = SchedulerReconcileJob { traceparent: None };
    let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::encode(&job)
        .map_err(|error| SchedulerError::Job(format!("encode recurring reconcile job: {error}")))?;
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SchedulerError::Job(format!("system clock before unix epoch: {error}")))?
        .as_secs();
    let now_i64 = i64::try_from(now_secs)
        .map_err(|_| SchedulerError::Job("now_unix_secs exceeds i64::MAX".to_owned()))?;
    let id = ulid::Ulid::new().to_string();
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => {
            let result = sqlx::query(
                "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
                 VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, 0, ?6, ?7) \
                 ON CONFLICT(job_type, idempotency_key) WHERE status IN ('Pending','Running','Queued') DO NOTHING",
            )
            .bind(payload)
            .bind(&id)
            .bind(SCHEDULER_RECONCILE_QUEUE)
            .bind(RECONCILE_MAX_ATTEMPTS)
            .bind(now_i64)
            .bind("{}")
            .bind(RECONCILE_RECURRING_IDEMPOTENCY_KEY)
            .execute(&sqlite.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        }
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => {
            let run_at = chrono::DateTime::<chrono::Utc>::from_timestamp(now_i64, 0).ok_or_else(
                || SchedulerError::Job("run_at outside chrono timestamp range".to_owned()),
            )?;
            let result = sqlx::query(
                "INSERT INTO apalis.jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
                 VALUES ($1, $2, $3, 'Pending', 0, $4, $5, 0, $6, $7) \
                 ON CONFLICT (job_type, idempotency_key) WHERE status IN ('Pending','Running','Queued') DO NOTHING",
            )
            .bind(payload)
            .bind(&id)
            .bind(SCHEDULER_RECONCILE_QUEUE)
            .bind(RECONCILE_MAX_ATTEMPTS)
            .bind(run_at)
            .bind(serde_json::json!({}))
            .bind(RECONCILE_RECURRING_IDEMPOTENCY_KEY)
            .execute(&postgres.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        }
    }
}
