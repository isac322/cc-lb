#[cfg(feature = "sqlite")]
use cc_lb_clock::{Clock, ClockHandle};

use super::{ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob};

#[cfg(feature = "sqlite")]
const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(feature = "postgres")]
const DEFAULT_POSTGRES_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(feature = "postgres")]
const PUBSUB_RECONNECT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(feature = "postgres")]
const MAX_PUBSUB_NOTIFICATIONS_PER_DRIVE: usize = 32;

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
type PgNotificationStream =
    futures_util::stream::BoxStream<'static, Result<sqlx::postgres::PgNotification, sqlx::Error>>;

#[cfg(feature = "postgres")]
enum PostgresListenerState {
    Starting,
    Connecting(
        apalis_core::backend::future::BoxSyncFuture<
            Result<apalis_postgres::PgListener, sqlx::Error>,
        >,
    ),
    Listening(std::sync::Mutex<PgNotificationStream>),
    ReconnectDelay(std::pin::Pin<Box<tokio::time::Sleep>>),
}

#[cfg(feature = "postgres")]
fn get_mut_unpoisoned<T>(mutex: &mut std::sync::Mutex<T>) -> &mut T {
    match mutex.get_mut() {
        Ok(inner) => inner,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(feature = "postgres")]
struct PostgresWorkerPollStrategy {
    pool: sqlx::PgPool,
    queue: String,
    poll_interval: std::time::Duration,
    listener: PostgresListenerState,
    interval_delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

#[cfg(feature = "postgres")]
impl PostgresWorkerPollStrategy {
    fn new(pool: sqlx::PgPool, queue: String, poll_interval: std::time::Duration) -> Self {
        Self {
            pool,
            queue,
            poll_interval,
            listener: PostgresListenerState::Starting,
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
        use futures_util::StreamExt as _;
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

        let mut raw_notifications = 0usize;
        loop {
            match &mut self.listener {
                PostgresListenerState::Starting => {
                    let pool = self.pool.clone();
                    let fut = Box::pin(async move {
                        let mut listener = apalis_postgres::PgListener::connect_with(&pool).await?;
                        listener.listen("apalis::job::insert").await?;
                        Ok(listener)
                    });
                    self.listener = PostgresListenerState::Connecting(
                        apalis_core::backend::future::BoxSyncFuture::new(fut),
                    );
                }
                PostgresListenerState::Connecting(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => break,
                    Poll::Ready(Ok(listener)) => {
                        self.listener = PostgresListenerState::Listening(std::sync::Mutex::new(
                            listener.into_stream().boxed(),
                        ));
                    }
                    Poll::Ready(Err(_)) => {
                        self.listener = PostgresListenerState::ReconnectDelay(Box::pin(
                            tokio::time::sleep(PUBSUB_RECONNECT_INTERVAL),
                        ));
                    }
                },
                PostgresListenerState::ReconnectDelay(delay) => match delay.as_mut().poll(cx) {
                    Poll::Pending => break,
                    Poll::Ready(()) => {
                        self.listener = PostgresListenerState::Starting;
                    }
                },
                PostgresListenerState::Listening(stream) => {
                    match get_mut_unpoisoned(stream).poll_next_unpin(cx) {
                        Poll::Pending => break,
                        Poll::Ready(Some(Ok(notification))) => {
                            if let Ok(event) = serde_json::from_str::<apalis_postgres::InsertEvent>(
                                notification.payload(),
                            ) && event.job_type == self.queue
                            {
                                ready = true;
                            }
                            raw_notifications = raw_notifications.saturating_add(1);
                            if raw_notifications >= MAX_PUBSUB_NOTIFICATIONS_PER_DRIVE {
                                cx.waker().wake_by_ref();
                                break;
                            }
                        }
                        Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                            self.listener = PostgresListenerState::ReconnectDelay(Box::pin(
                                tokio::time::sleep(PUBSUB_RECONNECT_INTERVAL),
                            ));
                        }
                    }
                }
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

    #[tokio::test]
    async fn postgres_worker_strategy_bounds_notification_drain_and_arms_interval_timer() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&database_url)
            .await
            .expect("connect postgres");
        let queue = format!("notify_flood_{}", ulid::Ulid::generate());
        let mut strategy = super::PostgresWorkerPollStrategy::new(
            pool.clone(),
            queue.clone(),
            Duration::from_secs(5),
        );
        let snapshot = PollSnapshot::new();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waker_state = CountingWaker::new();
                let waker = Waker::from(Arc::clone(&waker_state));
                let mut cx = Context::from_waker(&waker);
                let _ = strategy.poll_drive(&mut cx, &snapshot);
                let listening: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND pid <> pg_backend_pid()
                       AND query ILIKE 'LISTEN %apalis::job::insert%'",
                )
                .fetch_one(&pool)
                .await
                .expect("query listen count");
                if listening > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("pubsub listener connects");

        let other_queue = format!("other_flood_{}", ulid::Ulid::generate());
        for _ in 0..80 {
            let payload = serde_json::json!({
                "job_type": other_queue,
                "id": ulid::Ulid::generate().to_string(),
            });
            sqlx::query("SELECT pg_notify('apalis::job::insert', $1)")
                .bind(payload.to_string())
                .execute(&pool)
                .await
                .expect("emit cross-queue pg_notify");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let cross_waker_state = CountingWaker::new();
        let cross_waker = Waker::from(Arc::clone(&cross_waker_state));
        let mut cross_cx = Context::from_waker(&cross_waker);
        let cross_poll = strategy.poll_drive(&mut cross_cx, &snapshot);
        assert!(
            cross_poll.is_pending(),
            "cross-queue flood must return Pending after bounding raw notification drain"
        );
        assert!(
            strategy.interval_delay.is_some(),
            "interval delay must remain armed during a cross-queue notification flood"
        );
        assert!(
            cross_waker_state.count() >= 1,
            "hitting raw notification drain cap on cross-queue flood must self-wake"
        );
        // Drain remaining cross-queue notifications before testing same-queue flood.
        for _ in 0..4 {
            let _ = strategy.poll_drive(&mut cross_cx, &snapshot);
        }

        for _ in 0..80 {
            let payload = serde_json::json!({
                "job_type": queue,
                "id": ulid::Ulid::generate().to_string(),
            });
            sqlx::query("SELECT pg_notify('apalis::job::insert', $1)")
                .bind(payload.to_string())
                .execute(&pool)
                .await
                .expect("emit same-queue pg_notify");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let waker_state = CountingWaker::new();
        let waker = Waker::from(Arc::clone(&waker_state));
        let mut cx = Context::from_waker(&waker);
        let first_poll = strategy.poll_drive(&mut cx, &snapshot);
        assert!(
            first_poll.is_ready(),
            "first bounded drain must report ready"
        );
        assert!(
            strategy.interval_delay.is_some(),
            "interval delay must be armed even during a notification flood"
        );
        assert!(
            waker_state.count() >= 1,
            "hitting the notification drain cap must self-wake for remaining notifications"
        );

        let second_poll = strategy.poll_drive(&mut cx, &snapshot);
        assert!(
            second_poll.is_ready(),
            "remaining buffered notifications must still be available on the next poll_drive"
        );
        drop(strategy);
        pool.close().await;
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};
    use std::time::Duration;

    use apalis_core::backend::ext::poll_strategy::{PollSnapshot, PollStrategy as _};

    use super::{DEFAULT_POLL_INTERVAL, sqlite_worker_strategy};

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
    async fn sqlite_worker_strategy_keeps_timer_waker_armed_when_polled_after_interval_elapses() {
        let mut strategy = sqlite_worker_strategy(DEFAULT_POLL_INTERVAL);
        let snapshot = PollSnapshot::new();
        let initial_waker = CountingWaker::new();
        let waker = Waker::from(Arc::clone(&initial_waker));
        let mut cx = Context::from_waker(&waker);
        let _ = strategy.poll_drive(&mut cx, &snapshot);

        tokio::time::sleep(Duration::from_millis(150)).await;

        let next_waker = CountingWaker::new();
        let waker = Waker::from(Arc::clone(&next_waker));
        let notified = next_waker.notify.notified();
        {
            let mut cx = Context::from_waker(&waker);
            let poll = strategy.poll_drive(&mut cx, &snapshot);
            assert!(
                poll.is_ready(),
                "elapsed sqlite interval must report ready on completion poll"
            );
        }

        tokio::time::timeout(Duration::from_millis(500), notified)
            .await
            .expect("sqlite replacement interval timer must register waker and fire on next tick");
        assert!(next_waker.count() >= 1);
    }
}
