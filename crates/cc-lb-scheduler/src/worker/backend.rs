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
    apalis_core::backend::ext::poll_strategy::Strategy,
>;

#[cfg(feature = "sqlite")]
pub(crate) type SqliteCronApalisStorage = apalis_core::backend::ext::poll_strategy::PollWith<
    apalis_sqlite::SqliteStorage<CronJob>,
    apalis_core::backend::ext::poll_strategy::Strategy,
>;

#[cfg(feature = "sqlite")]
struct SqliteWorkerPollStrategy {
    poll_interval: std::time::Duration,
    interval_delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

#[cfg(feature = "sqlite")]
impl SqliteWorkerPollStrategy {
    fn new(poll_interval: std::time::Duration) -> Self {
        Self {
            poll_interval,
            interval_delay: None,
        }
    }
}

#[cfg(feature = "sqlite")]
impl apalis_core::backend::ext::poll_strategy::PollStrategy for SqliteWorkerPollStrategy {
    fn poll_drive(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _snapshot: &apalis_core::backend::ext::poll_strategy::PollSnapshot,
    ) -> std::task::Poll<Option<()>> {
        use std::task::Poll;

        let mut ready = false;
        loop {
            let delay = self
                .interval_delay
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(self.poll_interval)));
            match delay.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    ready = true;
                    self.interval_delay = Some(Box::pin(tokio::time::sleep(self.poll_interval)));
                }
                Poll::Pending => break,
            }
        }
        if ready {
            Poll::Ready(Some(()))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_worker_strategy(
    poll_interval: std::time::Duration,
) -> apalis_core::backend::ext::poll_strategy::Strategy {
    apalis_core::backend::ext::poll_strategy::Strategy::new()
        .apply(SqliteWorkerPollStrategy::new(poll_interval))
}

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
            .poll_with_strategy(sqlite_worker_strategy(DEFAULT_POLL_INTERVAL))
    }

    pub(crate) fn keepalive_storage(&self) -> SqliteApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(apalis_sqlite::Config::default().queue(CACHE_KEEPALIVE_QUEUE))
            .poll_with_strategy(sqlite_worker_strategy(DEFAULT_POLL_INTERVAL))
    }

    pub(crate) fn cron_storage(&self) -> SqliteCronApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(apalis_sqlite::Config::default().queue(CRON_QUEUE))
            .poll_with_strategy(sqlite_worker_strategy(DEFAULT_POLL_INTERVAL))
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

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    use apalis_core::backend::ext::poll_strategy::{PollSnapshot, PollStrategy as _};

    use super::SqliteWorkerPollStrategy;

    struct CountingWake {
        wakes: AtomicUsize,
    }

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn sqlite_worker_poll_strategy_keeps_replacement_timer_armed_after_ready() {
        let mut strategy = SqliteWorkerPollStrategy::new(Duration::from_millis(10));
        let snapshot = PollSnapshot::default();

        poll_fn(|cx| match strategy.poll_drive(cx, &snapshot) {
            Poll::Ready(Some(())) => Poll::Ready(()),
            Poll::Ready(None) => panic!("sqlite poll strategy unexpectedly terminated"),
            Poll::Pending => Poll::Pending,
        })
        .await;

        let counter = Arc::new(CountingWake {
            wakes: AtomicUsize::new(0),
        });
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            matches!(
                strategy.poll_drive(&mut cx, &snapshot),
                Poll::Ready(Some(()))
            ),
            "expired timer must report ready"
        );

        // Without calling poll_drive again, the replacement timer must already have
        // registered `waker` so a single-poll ready transition cannot stall the worker.
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            counter.wakes.load(Ordering::SeqCst) >= 1,
            "replacement timer must wake the task without requiring an extra poll_drive call"
        );
    }
}
