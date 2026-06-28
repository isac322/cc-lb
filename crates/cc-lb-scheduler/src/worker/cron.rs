use std::sync::Arc;
use std::time::Duration;

use cc_lb_config::Config;
use cc_lb_core::anthropic_compat::CLAUDE_CODE_STABLE_VERSION_KEY;
use cc_lb_core::clock::ClockHandle;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cron::WorkerBuilder as CronWorkerBuilder;
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use crate::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob;
use crate::jobs::watchdog::{OAuthRefreshWatchdogJob, WarmupWatchdogJob};
use crate::leader_election::LeaderElection;

const LEADER_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[cfg(feature = "postgres")]
use super::PostgresSchedulerStorage;
#[cfg(feature = "sqlite")]
use super::SqliteSchedulerStorage;
use super::{CRON_QUEUE, CronJob, SchedulerBackend};

type CronJobFactory = fn(u64) -> CronJob;

impl crate::cron::SingletonCronJob for CronJob {
    fn singleton_kind(&self) -> &'static str {
        self.kind()
    }
}

pub(super) fn spawn_cron_producer(
    backend: SchedulerBackend,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
    clock: ClockHandle,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                run_sqlite_cron_producer(sqlite, config, leader, cancel, clock).await;
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                run_postgres_cron_producer(postgres, config, leader, cancel, clock).await;
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
    clock: ClockHandle,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config, clock.clone()) {
        let pool = sqlite.pool.clone();
        let leader = leader.clone();
        let cancel = cancel.clone();
        let clock = clock.clone();
        handles.push(tokio::spawn(async move {
            run_sqlite_singleton_cron_loop(pool, spec, leader, cancel, clock).await;
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
    clock: ClockHandle,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config, clock) {
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
        let storage = apalis_postgres::PostgresStorage::<CronJob>::new_with_notify(
            &pool,
            &apalis_postgres::Config::new(CRON_QUEUE),
        );
        let worker = CronWorkerBuilder::singleton_queue_factory(
            CRON_QUEUE,
            spec.schedule.clone(),
            storage,
            spec.factory,
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
    clock: ClockHandle,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let storage = crate::sqlite_enqueue::SqliteSingletonCronStorage::new(
            pool.clone(),
            CRON_QUEUE,
            clock.clone(),
        );
        let worker = CronWorkerBuilder::singleton_queue_factory(
            CRON_QUEUE,
            spec.schedule.clone(),
            storage,
            spec.factory,
        );
        run_one_cron_attempt(spec.name, worker, &leader, &cancel).await;
        if !sleep_or_cancel(LEADER_RETRY_INTERVAL, &cancel).await {
            break;
        }
    }
}

async fn run_one_cron_attempt<Storage>(
    name: &'static str,
    worker: CronWorkerBuilder<CronJob, IntervalSchedule, Storage, CronJobFactory>,
    leader: &Arc<LeaderElection>,
    cancel: &CancellationToken,
) where
    Storage: apalis::prelude::TaskSink<CronJob, Error = sqlx::Error> + Send + 'static,
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
    factory: CronJobFactory,
}

fn singleton_cron_specs(config: &Config, clock: ClockHandle) -> Vec<SingletonCronSpec> {
    let mut specs = Vec::new();
    push_singleton_spec(&mut specs, config, &clock, "usage_rollup", |_| {
        CronJob::UsageRollup(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "usage_prune", |_| {
        CronJob::UsagePrune(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "quota_gc", |_| {
        CronJob::QuotaGc(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "prompt_cache_purge", |_| {
        CronJob::PromptCachePurge(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "price_catalog_refresh", |_| {
        CronJob::PriceCatalogRefresh(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "apalis_housekeeping", |_| {
        CronJob::ApalisHousekeeping(Default::default())
    });
    push_singleton_spec(
        &mut specs,
        config,
        &clock,
        "anthropic_compat_refresh",
        |_| {
            CronJob::AnthropicCompatRefresh(AnthropicCompatRefreshJob::new(
                CLAUDE_CODE_STABLE_VERSION_KEY,
            ))
        },
    );
    push_singleton_spec(&mut specs, config, &clock, "warmup_watchdog", |tick_secs| {
        CronJob::WarmupWatchdog(WarmupWatchdogJob::new(tick_secs))
    });
    push_singleton_spec(
        &mut specs,
        config,
        &clock,
        "oauth_refresh_watchdog",
        |tick_secs| CronJob::OAuthRefreshWatchdog(OAuthRefreshWatchdogJob::new(tick_secs)),
    );
    push_singleton_spec(
        &mut specs,
        config,
        &clock,
        "oauth_usage_poll",
        |tick_secs| CronJob::OAuthUsagePoll(OAuthUsagePollCronJob::new(tick_secs)),
    );
    push_singleton_spec(
        &mut specs,
        config,
        &clock,
        "pool_quota_snapshot",
        |tick_secs| CronJob::PoolQuotaSnapshot(PoolQuotaSnapshotCronJob::new(tick_secs)),
    );
    specs
}

fn push_singleton_spec(
    specs: &mut Vec<SingletonCronSpec>,
    config: &Config,
    clock: &ClockHandle,
    name: &'static str,
    factory: CronJobFactory,
) {
    let Some(job_config) = config.scheduler.recurring_jobs.get(name) else {
        return;
    };
    if !job_config.enabled {
        return;
    }
    specs.push(SingletonCronSpec {
        name,
        schedule: IntervalSchedule::new(
            name,
            job_config.interval_secs,
            job_config.jitter_secs,
            clock.clone(),
        ),
        factory,
    });
}

#[derive(Clone)]
struct IntervalSchedule {
    interval: chrono::Duration,
    first_delay: chrono::Duration,
    next: Option<DateTime<Utc>>,
    clock: ClockHandle,
}

impl IntervalSchedule {
    fn new(name: &str, interval_secs: u64, jitter_secs: u64, clock: ClockHandle) -> Self {
        let bounded_jitter = if jitter_secs == 0 {
            0
        } else {
            stable_hash(name) % jitter_secs.saturating_add(1)
        };
        Self {
            interval: chrono_seconds(interval_secs.max(1)),
            first_delay: chrono_seconds(interval_secs.max(1).saturating_add(bounded_jitter)),
            next: None,
            clock,
        }
    }
}

impl apalis_cron::Schedule<Utc> for IntervalSchedule {
    fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
        let now = DateTime::<Utc>::from(self.clock.now());
        let next = self.next.unwrap_or(now + self.first_delay);
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
