#[cfg(feature = "sqlite")]
use cc_lb_clock::{Clock, ClockHandle};

#[cfg(feature = "sqlite")]
use std::time::Duration;

#[cfg(feature = "sqlite")]
use apalis::prelude::{BackoffConfig, IntervalStrategy, StrategyBuilder};
#[cfg(feature = "sqlite")]
use apalis_sqlite::Config;

use super::{ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob};

/// Cap for the cache-keepalive queue's poll backoff.
///
/// apalis polls a SQLite queue every 100 ms and doubles that interval whenever a
/// fetch finds nothing due — `failed` there is `prev_count == 0`, not an error —
/// up to `BackoffConfig::default().max_delay` of 60 s. `apalis-sqlite` rc.8
/// initialises `prev_count` to 0 and never writes to it again, so the interval
/// only ever ratchets up. This queue is idle by design, its jobs being scheduled
/// minutes ahead, so a due renewal could sit behind a 60 s sleep and miss the
/// cache TTL it exists to extend. Cap the backoff instead.
#[cfg(feature = "sqlite")]
const KEEPALIVE_POLL_CAP: Duration = Duration::from_secs(2);

#[cfg(feature = "sqlite")]
fn keepalive_poll_config(cap: Duration) -> Config {
    apalis_sqlite::Config::new(CACHE_KEEPALIVE_QUEUE).with_poll_interval(
        StrategyBuilder::new()
            .apply(
                IntervalStrategy::new(Duration::from_millis(100))
                    .with_backoff(BackoffConfig::new(cap)),
            )
            .build(),
    )
}

#[derive(Clone, Debug)]
pub enum SchedulerBackend {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteSchedulerBackend),
    #[cfg(feature = "postgres")]
    Postgres(PostgresSchedulerBackend),
}

#[cfg(feature = "sqlite")]
pub type SqliteApalisStorage = apalis_sqlite::SqliteStorage<
    AdaptiveJob,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

#[cfg(feature = "sqlite")]
pub(crate) type SqliteCronApalisStorage = apalis_sqlite::SqliteStorage<
    CronJob,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteSchedulerBackend {
    pool: sqlx::SqlitePool,
    clock: ClockHandle,
}

#[cfg(feature = "sqlite")]
impl SqliteSchedulerBackend {
    pub fn new(pool: sqlx::SqlitePool, clock: ClockHandle) -> Self {
        Self { pool, clock }
    }

    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub(crate) fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    pub(crate) fn adaptive_storage(&self) -> SqliteApalisStorage {
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_in_queue(
            &self.pool,
            ADAPTIVE_QUEUE,
        )
    }

    pub(crate) fn keepalive_storage(&self) -> SqliteApalisStorage {
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(
            &self.pool,
            &keepalive_poll_config(KEEPALIVE_POLL_CAP),
        )
    }

    pub(crate) fn cron_storage(&self) -> SqliteCronApalisStorage {
        apalis_sqlite::SqliteStorage::<CronJob, (), ()>::new_in_queue(&self.pool, CRON_QUEUE)
    }
}

#[cfg(feature = "sqlite")]
impl std::fmt::Debug for SqliteSchedulerBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteSchedulerBackend")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "postgres")]
pub type PostgresApalisStorage = apalis_postgres::PostgresStorage<
    AdaptiveJob,
    apalis_postgres::CompactType,
    apalis_postgres::JsonCodec<apalis_postgres::CompactType>,
    apalis_postgres::PgNotify,
>;

#[cfg(feature = "postgres")]
pub(crate) type PostgresCronApalisStorage = apalis_postgres::PostgresStorage<
    CronJob,
    apalis_postgres::CompactType,
    apalis_postgres::JsonCodec<apalis_postgres::CompactType>,
    apalis_postgres::PgNotify,
>;

#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresSchedulerBackend {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresSchedulerBackend {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    pub(crate) fn adaptive_worker_storage(&self) -> PostgresApalisStorage {
        apalis_postgres::PostgresStorage::new_with_notify(
            &self.pool,
            &apalis_postgres::Config::new(ADAPTIVE_QUEUE),
        )
    }

    pub(crate) fn keepalive_worker_storage(&self) -> PostgresApalisStorage {
        apalis_postgres::PostgresStorage::new_with_notify(
            &self.pool,
            &apalis_postgres::Config::new(CACHE_KEEPALIVE_QUEUE),
        )
    }

    pub(crate) fn cron_worker_storage(&self) -> PostgresCronApalisStorage {
        apalis_postgres::PostgresStorage::new_with_notify(
            &self.pool,
            &apalis_postgres::Config::new(CRON_QUEUE),
        )
    }

    pub(crate) fn adaptive_operation_storage(
        &self,
    ) -> apalis_postgres::PostgresStorage<AdaptiveJob> {
        apalis_postgres::PostgresStorage::new_with_config(
            &self.pool,
            &apalis_postgres::Config::new(ADAPTIVE_QUEUE),
        )
    }

    pub(crate) fn keepalive_operation_storage(
        &self,
    ) -> apalis_postgres::PostgresStorage<AdaptiveJob> {
        apalis_postgres::PostgresStorage::new_with_config(
            &self.pool,
            &apalis_postgres::Config::new(CACHE_KEEPALIVE_QUEUE),
        )
    }
}

#[cfg(feature = "postgres")]
impl std::fmt::Debug for PostgresSchedulerBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresSchedulerBackend")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    use apalis_core::backend::poll_strategy::{PollContext, PollStrategyExt};
    use apalis_core::worker::context::WorkerContext;
    use futures_util::StreamExt as _;

    use super::{KEEPALIVE_POLL_CAP, keepalive_poll_config};

    /// A keep-alive queue is empty by design — its jobs are due minutes ahead — so
    /// `prev_count` stays 0, apalis counts every poll as a "failure", and the poll
    /// interval doubles each time. Driven that way, the capped strategy must still
    /// tick six times in well under a second (~260 ms: 100 ms base interval, then
    /// pinned at the cap), while the same six polls without a cap take ~3.1 s and
    /// an idle queue ends up on apalis' 60 s default. The 40 ms cap keeps this
    /// instant; the shipped value is `KEEPALIVE_POLL_CAP`.
    #[tokio::test]
    async fn keepalive_poll_strategy_stops_backing_off_at_the_cap() {
        let cap = Duration::from_millis(40);
        let config = keepalive_poll_config(cap);
        let ctx = PollContext::new(
            WorkerContext::new::<()>("keepalive-poll-test"),
            // prev_count 0 == the previous fetch found no due job: the idle case.
            Arc::new(AtomicUsize::new(0)),
        );
        let mut ticks = config.poll_strategy().clone().build_stream(&ctx);

        let started = Instant::now();
        for _ in 0..6 {
            assert!(
                ticks.next().await.is_some(),
                "the poll strategy must keep ticking"
            );
        }
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_millis(900),
            "keepalive poll backoff escaped its cap: six idle polls took {elapsed:?}"
        );
        assert!(
            KEEPALIVE_POLL_CAP <= Duration::from_secs(5),
            "the shipped cap must stay far below apalis' 60 s BackoffConfig default"
        );
    }
}
