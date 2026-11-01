#[cfg(feature = "sqlite")]
use cc_lb_clock::{Clock, ClockHandle};

use super::{ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob};

const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Clone, Debug)]
pub enum SchedulerBackend {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteSchedulerBackend),
    #[cfg(feature = "postgres")]
    Postgres(PostgresSchedulerBackend),
}

#[cfg(feature = "sqlite")]
pub type SqliteApalisStorage = apalis_core::backend::ext::poll_strategy::PollWith<
    apalis_sqlite::SqliteStorage<AdaptiveJob>,
    apalis_core::backend::ext::poll_strategy::IntervalStrategy,
>;

#[cfg(feature = "sqlite")]
pub(crate) type SqliteCronApalisStorage = apalis_core::backend::ext::poll_strategy::PollWith<
    apalis_sqlite::SqliteStorage<CronJob>,
    apalis_core::backend::ext::poll_strategy::IntervalStrategy,
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
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(apalis_sqlite::Config::default().queue(ADAPTIVE_QUEUE))
            .poll_with_interval(DEFAULT_POLL_INTERVAL)
    }

    pub(crate) fn keepalive_storage(&self) -> SqliteApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(apalis_sqlite::Config::default().queue(CACHE_KEEPALIVE_QUEUE))
            .poll_with_interval(DEFAULT_POLL_INTERVAL)
    }

    pub(crate) fn cron_storage(&self) -> SqliteCronApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(apalis_sqlite::Config::default().queue(CRON_QUEUE))
            .poll_with_interval(DEFAULT_POLL_INTERVAL)
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
pub type PostgresApalisStorage = apalis_core::backend::ext::poll_strategy::PollWith<
    apalis_postgres::PostgresStorage<AdaptiveJob>,
    apalis_core::backend::ext::poll_strategy::Strategy,
>;

#[cfg(feature = "postgres")]
pub(crate) type PostgresCronApalisStorage = apalis_core::backend::ext::poll_strategy::PollWith<
    apalis_postgres::PostgresStorage<CronJob>,
    apalis_core::backend::ext::poll_strategy::Strategy,
>;

#[cfg(feature = "postgres")]
fn postgres_worker_strategy(
    pool: &sqlx::PgPool,
    queue: &str,
) -> apalis_core::backend::ext::poll_strategy::Strategy {
    apalis_core::backend::ext::poll_strategy::Strategy::new()
        .stream(apalis_postgres::Pubsub::new(pool.clone(), queue.to_owned()))
        .interval(DEFAULT_POLL_INTERVAL)
}

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
        use apalis_core::backend::ext::BackendExt as _;
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(apalis_postgres::Config::default().queue(ADAPTIVE_QUEUE))
            .poll_with_strategy(postgres_worker_strategy(&self.pool, ADAPTIVE_QUEUE))
    }

    pub(crate) fn keepalive_worker_storage(&self) -> PostgresApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(apalis_postgres::Config::default().queue(CACHE_KEEPALIVE_QUEUE))
            .poll_with_strategy(postgres_worker_strategy(&self.pool, CACHE_KEEPALIVE_QUEUE))
    }

    pub(crate) fn cron_worker_storage(&self) -> PostgresCronApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(apalis_postgres::Config::default().queue(CRON_QUEUE))
            .poll_with_strategy(postgres_worker_strategy(&self.pool, CRON_QUEUE))
    }

    pub(crate) fn adaptive_operation_storage(
        &self,
    ) -> apalis_postgres::PostgresStorage<AdaptiveJob> {
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(apalis_postgres::Config::default().queue(ADAPTIVE_QUEUE))
    }

    pub(crate) fn keepalive_operation_storage(
        &self,
    ) -> apalis_postgres::PostgresStorage<AdaptiveJob> {
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(apalis_postgres::Config::default().queue(CACHE_KEEPALIVE_QUEUE))
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
