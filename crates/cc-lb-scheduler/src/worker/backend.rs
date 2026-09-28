#[cfg(feature = "sqlite")]
use cc_lb_clock::{Clock, ClockHandle};

use super::{ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob};

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
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_in_queue(
            &self.pool,
            CACHE_KEEPALIVE_QUEUE,
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
