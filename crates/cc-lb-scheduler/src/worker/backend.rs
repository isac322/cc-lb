#[cfg(feature = "sqlite")]
use cc_lb_clock::{Clock, ClockHandle};

use super::{ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob};

#[cfg(feature = "sqlite")]
const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(feature = "postgres")]
const DEFAULT_POSTGRES_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(feature = "postgres")]
const PUBSUB_RECONNECT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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
struct PostgresWorkerPollStrategy {
    pool: sqlx::PgPool,
    queue: String,
    poll_interval: std::time::Duration,
    pubsub: apalis_postgres::Pubsub,
    pubsub_reconnect_delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    interval_delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

#[cfg(feature = "postgres")]
impl PostgresWorkerPollStrategy {
    fn new(pool: sqlx::PgPool, queue: String, poll_interval: std::time::Duration) -> Self {
        let pubsub = apalis_postgres::Pubsub::new(pool.clone(), queue.clone());
        Self {
            pool,
            queue,
            poll_interval,
            pubsub,
            pubsub_reconnect_delay: None,
            interval_delay: None,
        }
    }
}

#[cfg(feature = "postgres")]
impl apalis_core::backend::ext::poll_strategy::PollStrategy for PostgresWorkerPollStrategy {
    fn poll_drive(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _snapshot: &apalis_core::backend::ext::poll_strategy::PollSnapshot,
    ) -> std::task::Poll<Option<()>> {
        use futures_util::Stream as _;
        use std::pin::Pin;
        use std::task::Poll;

        let mut ready = false;

        loop {
            if let Some(delay) = self.pubsub_reconnect_delay.as_mut() {
                match delay.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        self.pubsub_reconnect_delay = None;
                        self.pubsub =
                            apalis_postgres::Pubsub::new(self.pool.clone(), self.queue.clone());
                    }
                    Poll::Pending => break,
                }
            }

            match Pin::new(&mut self.pubsub).poll_next(cx) {
                Poll::Ready(Some(Ok(_))) => {
                    ready = true;
                }
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                    self.pubsub_reconnect_delay =
                        Some(Box::pin(tokio::time::sleep(PUBSUB_RECONNECT_INTERVAL)));
                }
                Poll::Pending => break,
            }
        }

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

#[cfg(feature = "postgres")]
fn postgres_worker_strategy(
    pool: &sqlx::PgPool,
    queue: &str,
) -> apalis_core::backend::ext::poll_strategy::Strategy {
    postgres_worker_strategy_with_interval(pool, queue, DEFAULT_POSTGRES_POLL_INTERVAL)
}

#[cfg(feature = "postgres")]
fn postgres_worker_strategy_with_interval(
    pool: &sqlx::PgPool,
    queue: &str,
    poll_interval: std::time::Duration,
) -> apalis_core::backend::ext::poll_strategy::Strategy {
    apalis_core::backend::ext::poll_strategy::Strategy::new().apply(
        PostgresWorkerPollStrategy::new(pool.clone(), queue.to_owned(), poll_interval),
    )
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

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    use apalis_core::backend::ext::poll_strategy::{PollSnapshot, PollStrategy as _};
    use sqlx::postgres::PgPoolOptions;

    use super::{
        CACHE_KEEPALIVE_QUEUE, postgres_worker_strategy, postgres_worker_strategy_with_interval,
    };

    struct CountingWaker {
        wakes: AtomicUsize,
        notify: tokio::sync::Notify,
    }

    impl CountingWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                wakes: AtomicUsize::new(0),
                notify: tokio::sync::Notify::new(),
            })
        }

        fn count(&self) -> usize {
            self.wakes.load(Ordering::SeqCst)
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    #[tokio::test]
    async fn postgres_worker_strategy_keeps_timer_waker_armed_when_polled_after_interval_elapses() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind silent listener");
        let addr = listener.local_addr().expect("listener addr");
        let _accept_guard = tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .connect_lazy(&format!("postgres://postgres:postgres@{addr}/postgres"))
            .expect("lazy pool");
        let mut strategy = postgres_worker_strategy(&pool, CACHE_KEEPALIVE_QUEUE);
        let snapshot = PollSnapshot::new();
        let initial_waker = CountingWaker::new();
        let waker = Waker::from(Arc::clone(&initial_waker));
        let mut cx = Context::from_waker(&waker);
        let _ = strategy.poll_drive(&mut cx, &snapshot);

        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let next_waker = CountingWaker::new();
        let waker = Waker::from(Arc::clone(&next_waker));
        let notified = next_waker.notify.notified();
        {
            let mut cx = Context::from_waker(&waker);
            let poll = strategy.poll_drive(&mut cx, &snapshot);
            assert!(
                poll.is_ready(),
                "elapsed interval must report ready on completion poll"
            );
        }

        tokio::time::timeout(Duration::from_millis(1_500), notified)
            .await
            .expect("replacement interval timer must register waker and fire on next tick");
        assert!(next_waker.count() >= 1);
    }

    #[tokio::test]
    async fn postgres_worker_strategy_continues_interval_wakes_when_pubsub_connect_fails() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(50))
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
            .expect("lazy pool");
        let mut strategy = postgres_worker_strategy(&pool, CACHE_KEEPALIVE_QUEUE);
        let snapshot = PollSnapshot::new();

        // Drive strategy until the initial Pubsub connection attempt fails and enters Closed.
        poll_fn(|cx| {
            if strategy.poll_drive(cx, &snapshot).is_ready() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;

        let waker_state = CountingWaker::new();
        let waker = Waker::from(Arc::clone(&waker_state));
        let notified = waker_state.notify.notified();
        {
            let mut cx = Context::from_waker(&waker);
            let _ = strategy.poll_drive(&mut cx, &snapshot);
        }

        tokio::time::timeout(Duration::from_millis(1_500), notified)
            .await
            .expect("interval timer must still register waker and fire even after Pubsub closes");
        assert!(waker_state.count() >= 1);
    }

    #[tokio::test]
    async fn postgres_worker_strategy_with_interval_honors_custom_poll_interval() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind silent listener");
        let addr = listener.local_addr().expect("listener addr");
        let _accept_guard = tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .connect_lazy(&format!("postgres://postgres:postgres@{addr}/postgres"))
            .expect("lazy pool");
        let mut strategy = postgres_worker_strategy_with_interval(
            &pool,
            CACHE_KEEPALIVE_QUEUE,
            Duration::from_millis(80),
        );
        let snapshot = PollSnapshot::new();
        let started = std::time::Instant::now();

        tokio::time::timeout(
            Duration::from_millis(400),
            poll_fn(|cx| strategy.poll_drive(cx, &snapshot)),
        )
        .await
        .expect("custom 80ms interval must fire well before 1s default");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(60) && elapsed < Duration::from_millis(400),
            "custom 80ms interval fired in {elapsed:?}"
        );
    }
}
