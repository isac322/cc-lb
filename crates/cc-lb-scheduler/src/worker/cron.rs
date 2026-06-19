use std::error::Error;
use std::sync::Arc;

use apalis::prelude::TaskSink;
use cc_lb_config::Config;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cron::WorkerBuilder as CronWorkerBuilder;
use crate::leader_election::LeaderElection;

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
        let storage = apalis_sqlite::SqliteStorage::<SingletonJob, (), ()>::new_in_queue(
            &sqlite.pool,
            SINGLETON_QUEUE,
        );
        let worker =
            CronWorkerBuilder::singleton_queue(SINGLETON_QUEUE, spec.schedule, storage, spec.job);
        handles.push(spawn_cron_worker(
            spec.name,
            worker,
            leader.clone(),
            cancel.clone(),
        ));
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
        let storage = apalis_postgres::PostgresStorage::<SingletonJob>::new_with_config(
            &postgres.pool,
            &apalis_postgres::Config::new(SINGLETON_QUEUE),
        );
        let worker =
            CronWorkerBuilder::singleton_queue(SINGLETON_QUEUE, spec.schedule, storage, spec.job);
        handles.push(spawn_cron_worker(
            spec.name,
            worker,
            leader.clone(),
            cancel.clone(),
        ));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

fn spawn_cron_worker<Storage>(
    name: &'static str,
    worker: CronWorkerBuilder<SingletonJob, IntervalSchedule, Storage>,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Storage: TaskSink<SingletonJob> + Send + 'static,
    Storage::Error: Error + Send + Sync + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            result = worker.run(leader.as_ref()) => {
                if let Err(error) = result {
                    tracing::error!(job = name, error = %error, "scheduler cron producer exited with error");
                }
            }
            _ = cancel.cancelled() => {}
        }
    })
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
