use std::time::Duration;

use cc_lb_clock::ClockHandle;
use cc_lb_config::SchedulerConfig;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cron::WorkerBuilder as CronWorkerBuilder;
use crate::jobs::api_key_usage_poll::{API_KEY_USAGE_POLL_JOB, ApiKeyUsagePollCronJob};
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use crate::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob;
use crate::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJob;
use crate::jobs::watchdog::{OAuthRefreshWatchdogJob, WarmupWatchdogJob};

/// Delay between cron producer restarts after a transient failure.
const CRON_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[cfg(feature = "postgres")]
use super::PostgresSchedulerBackend;
#[cfg(feature = "sqlite")]
use super::SqliteSchedulerBackend;
use super::{CRON_QUEUE, CronJob, SchedulerBackend};

type CronJobFactory = fn(u64) -> CronJob;

impl crate::cron::SingletonCronJob for CronJob {
    fn singleton_kind(&self) -> &'static str {
        self.kind()
    }
}

pub(super) fn spawn_cron_producer(
    backend: SchedulerBackend,
    config: SchedulerConfig,
    cancel: CancellationToken,
    clock: ClockHandle,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                run_sqlite_cron_producer(sqlite, config, cancel, clock).await;
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                run_postgres_cron_producer(postgres, config, cancel, clock).await;
            }
        }
    })
}

#[cfg(feature = "sqlite")]
async fn run_sqlite_cron_producer(
    sqlite: SqliteSchedulerBackend,
    config: SchedulerConfig,
    cancel: CancellationToken,
    clock: ClockHandle,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config, clock.clone()) {
        let pool = sqlite.pool().clone();
        let cancel = cancel.clone();
        let clock = clock.clone();
        handles.push(tokio::spawn(async move {
            run_sqlite_singleton_cron_loop(pool, spec, cancel, clock).await;
        }));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

#[cfg(feature = "postgres")]
async fn run_postgres_cron_producer(
    postgres: PostgresSchedulerBackend,
    config: SchedulerConfig,
    cancel: CancellationToken,
    clock: ClockHandle,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config, clock) {
        let pool = postgres.pool().clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            run_postgres_singleton_cron_loop(pool, spec, cancel).await;
        }));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

#[cfg(feature = "postgres")]
async fn run_postgres_singleton_cron_loop(
    pool: sqlx::PgPool,
    spec: SingletonCronSpec,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let storage = apalis_postgres::PostgresStorage::<CronJob>::new(&pool)
            .with_config(apalis_postgres::Config::default().queue(CRON_QUEUE));
        let worker = CronWorkerBuilder::singleton_queue_factory(
            spec.schedule.clone(),
            storage,
            spec.factory,
        );
        run_one_cron_attempt(spec.name, worker, &cancel).await;
        if !sleep_or_cancel(CRON_RETRY_INTERVAL, &cancel).await {
            break;
        }
    }
}

#[cfg(feature = "sqlite")]
async fn run_sqlite_singleton_cron_loop(
    pool: sqlx::SqlitePool,
    spec: SingletonCronSpec,
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
            spec.schedule.clone(),
            storage,
            spec.factory,
        );
        run_one_cron_attempt(spec.name, worker, &cancel).await;
        if !sleep_or_cancel(CRON_RETRY_INTERVAL, &cancel).await {
            break;
        }
    }
}

async fn run_one_cron_attempt<Storage, Kind>(
    name: &'static str,
    worker: CronWorkerBuilder<CronJob, IntervalSchedule, Storage, CronJobFactory, Kind>,
    cancel: &CancellationToken,
) where
    Storage: apalis::prelude::TaskSink<CronJob, Kind> + Send + 'static,
    Storage::Error: crate::cron::CronStorageError,
{
    tokio::select! {
        result = worker.run() => {
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

fn singleton_cron_specs(config: &SchedulerConfig, clock: ClockHandle) -> Vec<SingletonCronSpec> {
    let mut specs = Vec::new();
    push_singleton_spec(&mut specs, config, &clock, "usage_rollup", |_| {
        CronJob::UsageRollup(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "usage_prune", |_| {
        CronJob::UsagePrune(Default::default())
    });
    push_singleton_spec(&mut specs, config, &clock, "prompt_cache_purge", |_| {
        CronJob::PromptCachePurge(Default::default())
    });
    push_singleton_spec(
        &mut specs,
        config,
        &clock,
        "upstream_affinity_purge",
        |_| CronJob::UpstreamAffinityPurge(UpstreamAffinityPurgeJob::default()),
    );
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
        |_| CronJob::AnthropicCompatRefresh(AnthropicCompatRefreshJob::default()),
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
    push_singleton_spec(
        &mut specs,
        config,
        &clock,
        API_KEY_USAGE_POLL_JOB,
        |tick_secs| CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(tick_secs)),
    );
    specs
}

fn push_singleton_spec(
    specs: &mut Vec<SingletonCronSpec>,
    config: &SchedulerConfig,
    clock: &ClockHandle,
    name: &'static str,
    factory: CronJobFactory,
) {
    let Some(job_config) = config.recurring_jobs.get(name) else {
        return;
    };
    if !job_config.enabled {
        return;
    }
    let Some(schedule) = IntervalSchedule::new(
        name,
        job_config.interval_secs,
        job_config.jitter_secs,
        clock.clone(),
    ) else {
        tracing::warn!(
            job = name,
            interval_secs = job_config.interval_secs,
            jitter_secs = job_config.jitter_secs,
            "recurring job interval exceeds the scheduler timestamp range; job not registered",
        );
        return;
    };
    specs.push(SingletonCronSpec {
        name,
        schedule,
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
    /// Returns `None` when the first tick or the one after it cannot be
    /// represented as a `DateTime<Utc>`.
    fn new(name: &str, interval_secs: u64, jitter_secs: u64, clock: ClockHandle) -> Option<Self> {
        let bounded_jitter = if jitter_secs == 0 {
            0
        } else {
            stable_hash(name) % jitter_secs.saturating_add(1)
        };
        let interval = chrono_seconds(interval_secs.max(1))?;
        let first_delay = chrono_seconds(interval_secs.max(1).checked_add(bounded_jitter)?)?;
        DateTime::<Utc>::from(clock.now())
            .checked_add_signed(first_delay)?
            .checked_add_signed(interval)?;
        Some(Self {
            interval,
            first_delay,
            next: None,
            clock,
        })
    }
}

impl apalis_cron::Schedule<Utc> for IntervalSchedule {
    fn next_tick(&mut self, _: &Utc) -> Option<apalis_cron::Tick<Utc>> {
        let next = match self.next {
            Some(next) => next,
            None => DateTime::<Utc>::from(self.clock.now()).checked_add_signed(self.first_delay)?,
        };
        self.next = Some(next.checked_add_signed(self.interval)?);
        let tick_secs = u64::try_from(next.timestamp()).ok()?;
        Some(apalis_cron::Tick::new(tick_secs))
    }
}

fn chrono_seconds(seconds: u64) -> Option<chrono::Duration> {
    chrono::Duration::try_seconds(i64::try_from(seconds).ok()?)
}

fn stable_hash(value: &str) -> u64 {
    value.as_bytes().iter().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u64::from(*byte))
    })
}

#[cfg(test)]
mod tests {
    use cc_lb_clock::TestClock;

    use super::*;

    #[test]
    fn default_registry_contains_the_actual_upstream_affinity_purge_job() {
        let config = SchedulerConfig::default();
        let clock: ClockHandle = std::sync::Arc::new(TestClock::new_at_secs(1_800_000_000));
        let specs = singleton_cron_specs(&config, clock);
        let spec = specs
            .iter()
            .find(|spec| spec.name == "upstream_affinity_purge")
            .expect("default upstream affinity purge cron registration");

        let job = (spec.factory)(1_800_000_600);
        assert!(matches!(job, CronJob::UpstreamAffinityPurge(_)));
        assert_eq!(job.kind(), "upstream_affinity_purge");
    }

    #[test]
    fn disabled_upstream_affinity_purge_is_not_registered() {
        let mut config = SchedulerConfig::default();
        config
            .recurring_jobs
            .get_mut("upstream_affinity_purge")
            .expect("default upstream affinity purge config")
            .enabled = false;
        let clock: ClockHandle = std::sync::Arc::new(TestClock::new_at_secs(1_800_000_000));

        assert!(
            singleton_cron_specs(&config, clock)
                .iter()
                .all(|spec| spec.name != "upstream_affinity_purge")
        );
    }

    #[test]
    fn unrepresentable_interval_is_not_registered() {
        let clock: ClockHandle = std::sync::Arc::new(TestClock::new_at_secs(1_800_000_000));
        for interval_secs in [u64::MAX, i64::MAX.unsigned_abs(), 1_000_000_000_000_000] {
            let mut config = SchedulerConfig::default();
            let job = config
                .recurring_jobs
                .get_mut(API_KEY_USAGE_POLL_JOB)
                .expect("default api-key usage poll config");
            job.interval_secs = interval_secs;
            job.jitter_secs = 0;

            let specs = singleton_cron_specs(&config, clock.clone());

            assert!(
                specs.iter().all(|spec| spec.name != API_KEY_USAGE_POLL_JOB),
                "{interval_secs}"
            );
            assert!(specs.iter().any(|spec| spec.name == "usage_rollup"));
        }
    }

    #[test]
    fn next_tick_ends_the_schedule_instead_of_overflowing() {
        let clock: ClockHandle = std::sync::Arc::new(TestClock::new_at_secs(1_800_000_000));
        let mut schedule = IntervalSchedule {
            interval: chrono::Duration::MAX,
            first_delay: chrono::Duration::MAX,
            next: None,
            clock,
        };

        assert_eq!(apalis_cron::Schedule::next_tick(&mut schedule, &Utc), None);
    }

    #[test]
    fn default_registry_ticks_api_key_usage_poll_every_300_secs() {
        let config = SchedulerConfig::default();
        let clock: ClockHandle = std::sync::Arc::new(TestClock::new_at_secs(1_800_000_000));
        let specs = singleton_cron_specs(&config, clock);
        let spec = specs
            .iter()
            .find(|spec| spec.name == "api_key_usage_poll")
            .expect("default api-key usage poll cron registration");

        let job = (spec.factory)(1_800_000_300);
        assert!(matches!(
            &job,
            CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob {
                tick_unix_secs: 1_800_000_300,
                upstream_id: None,
                traceparent: None,
            })
        ));
        assert_eq!(job.kind(), "api_key_usage_poll");
        assert_eq!(spec.schedule.interval, chrono::Duration::seconds(300));
        assert!(spec.schedule.first_delay >= chrono::Duration::seconds(300));
        assert!(spec.schedule.first_delay <= chrono::Duration::seconds(315));
    }
}
