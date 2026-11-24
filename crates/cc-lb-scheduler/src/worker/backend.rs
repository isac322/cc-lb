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
            .with_config(
                apalis_sqlite::Config::default()
                    .queue(ADAPTIVE_QUEUE)
                    .lock_tasks(false)
                    .persist_results(false),
            )
            .poll_with_strategy(sqlite_worker_strategy(DEFAULT_POLL_INTERVAL))
    }

    pub(crate) fn keepalive_storage(&self) -> SqliteApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(
                apalis_sqlite::Config::default()
                    .queue(CACHE_KEEPALIVE_QUEUE)
                    .lock_tasks(false)
                    .persist_results(false),
            )
            .poll_with_strategy(sqlite_worker_strategy(DEFAULT_POLL_INTERVAL))
    }

    pub(crate) fn cron_storage(&self) -> SqliteCronApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_sqlite::SqliteStorage::new(&self.pool)
            .with_config(
                apalis_sqlite::Config::default()
                    .queue(CRON_QUEUE)
                    .lock_tasks(false)
                    .persist_results(false),
            )
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
#[derive(Debug)]
pub(crate) struct PostgresInsertFanout {
    listener_pool: sqlx::PgPool,
    subscribers: std::sync::Arc<std::sync::Mutex<Vec<tokio::sync::mpsc::Sender<String>>>>,
    started: std::sync::atomic::AtomicBool,
    cancel: tokio_util::sync::CancellationToken,
}

#[cfg(feature = "postgres")]
impl Drop for PostgresInsertFanout {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(feature = "postgres")]
impl PostgresInsertFanout {
    pub(crate) fn new(listener_pool: sqlx::PgPool) -> Self {
        Self {
            listener_pool,
            subscribers: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            started: std::sync::atomic::AtomicBool::new(false),
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn subscribe(&self) -> tokio::sync::mpsc::Receiver<String> {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        {
            let mut subscribers = match self.subscribers.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            subscribers.push(tx);
        }
        if !self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let listener_pool = self.listener_pool.clone();
            let subscribers = std::sync::Arc::clone(&self.subscribers);
            let cancel = self.cancel.clone();
            tokio::spawn(Self::run(listener_pool, subscribers, cancel));
        }
        rx
    }

    async fn run(
        listener_pool: sqlx::PgPool,
        subscribers: std::sync::Arc<std::sync::Mutex<Vec<tokio::sync::mpsc::Sender<String>>>>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        use backon::BackoffBuilder as _;
        use futures_util::StreamExt as _;

        let make_backoff = || {
            backon::ExponentialBuilder::default()
                .with_min_delay(std::time::Duration::from_millis(50))
                .with_max_delay(PUBSUB_RECONNECT_INTERVAL)
                .with_jitter()
                .without_max_times()
                .build()
        };
        let mut backoff = make_backoff();
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let connect = async {
                let mut listener =
                    apalis_postgres::PgListener::connect_with(&listener_pool).await?;
                listener.listen("apalis::job::insert").await?;
                Ok::<_, sqlx::Error>(listener)
            };
            let listener = tokio::select! {
                _ = cancel.cancelled() => return,
                res = connect => match res {
                    Ok(listener) => listener,
                    Err(_) => {
                        let delay = backoff.next().unwrap_or(PUBSUB_RECONNECT_INTERVAL);
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(delay) => continue,
                        }
                    }
                }
            };
            backoff = make_backoff();
            let mut stream = listener.into_stream().boxed();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    item = stream.next() => match item {
                        Some(Ok(notification)) => {
                            if let Ok(event) = serde_json::from_str::<apalis_postgres::InsertEvent>(
                                notification.payload(),
                            ) {
                                let mut guard = match subscribers.lock() {
                                    Ok(guard) => guard,
                                    Err(poisoned) => poisoned.into_inner(),
                                };
                                guard.retain(|tx| match tx.try_send(event.job_type.clone()) {
                                    Ok(())
                                    | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => true,
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
                                });
                            }
                        }
                        Some(Err(_)) | None => break,
                    }
                }
            }
            let delay = backoff.next().unwrap_or(PUBSUB_RECONNECT_INTERVAL);
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
}

#[cfg(feature = "postgres")]
struct PostgresWorkerPollStrategy {
    fanout: std::sync::Arc<PostgresInsertFanout>,
    queue: String,
    poll_interval: std::time::Duration,
    receiver: Option<tokio::sync::mpsc::Receiver<String>>,
    interval_delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

#[cfg(feature = "postgres")]
impl PostgresWorkerPollStrategy {
    #[cfg_attr(not(test), allow(dead_code))]
    fn new(pool: sqlx::PgPool, queue: String, poll_interval: std::time::Duration) -> Self {
        Self::new_with_fanout(
            std::sync::Arc::new(PostgresInsertFanout::new(pool)),
            queue,
            poll_interval,
        )
    }

    fn new_with_fanout(
        fanout: std::sync::Arc<PostgresInsertFanout>,
        queue: String,
        poll_interval: std::time::Duration,
    ) -> Self {
        Self {
            fanout,
            queue,
            poll_interval,
            receiver: None,
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

        let rx = self.receiver.get_or_insert_with(|| self.fanout.subscribe());
        let mut raw_notifications = 0usize;
        while let Poll::Ready(Some(job_type)) = rx.poll_recv(cx) {
            if job_type == self.queue {
                ready = true;
            }
            raw_notifications = raw_notifications.saturating_add(1);
            if raw_notifications >= MAX_PUBSUB_NOTIFICATIONS_PER_DRIVE {
                cx.waker().wake_by_ref();
                break;
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
    fanout: &std::sync::Arc<PostgresInsertFanout>,
    queue: &str,
) -> apalis_core::backend::ext::poll_strategy::Strategy {
    apalis_core::backend::ext::poll_strategy::Strategy::new().apply(
        PostgresWorkerPollStrategy::new_with_fanout(
            std::sync::Arc::clone(fanout),
            queue.to_owned(),
            DEFAULT_POSTGRES_POLL_INTERVAL,
        ),
    )
}

#[cfg(feature = "postgres")]
#[cfg_attr(not(test), allow(dead_code))]
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
    listener_pool: sqlx::PgPool,
    fanout: std::sync::Arc<PostgresInsertFanout>,
}

#[cfg(feature = "postgres")]
impl PostgresSchedulerBackend {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self::new_with_listener_pool(pool.clone(), pool)
    }

    pub fn new_with_listener_pool(pool: sqlx::PgPool, listener_pool: sqlx::PgPool) -> Self {
        let fanout = std::sync::Arc::new(PostgresInsertFanout::new(listener_pool.clone()));
        Self {
            pool,
            listener_pool,
            fanout,
        }
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    pub fn listener_pool(&self) -> &sqlx::PgPool {
        &self.listener_pool
    }

    pub(crate) fn adaptive_worker_storage(&self) -> PostgresApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(
                apalis_postgres::Config::default()
                    .queue(ADAPTIVE_QUEUE)
                    .lock_tasks(false)
                    .persist_results(false),
            )
            .poll_with_strategy(postgres_worker_strategy(&self.fanout, ADAPTIVE_QUEUE))
    }

    pub(crate) fn keepalive_worker_storage(&self) -> PostgresApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(
                apalis_postgres::Config::default()
                    .queue(CACHE_KEEPALIVE_QUEUE)
                    .lock_tasks(false)
                    .persist_results(false),
            )
            .poll_with_strategy(postgres_worker_strategy(
                &self.fanout,
                CACHE_KEEPALIVE_QUEUE,
            ))
    }

    pub(crate) fn cron_worker_storage(&self) -> PostgresCronApalisStorage {
        use apalis_core::backend::ext::BackendExt as _;
        apalis_postgres::PostgresStorage::new(&self.pool)
            .with_config(
                apalis_postgres::Config::default()
                    .queue(CRON_QUEUE)
                    .lock_tasks(false)
                    .persist_results(false),
            )
            .poll_with_strategy(postgres_worker_strategy(&self.fanout, CRON_QUEUE))
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

    use super::{CACHE_KEEPALIVE_QUEUE, postgres_worker_strategy_with_interval};

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
        let mut strategy = postgres_worker_strategy_with_interval(
            &pool,
            CACHE_KEEPALIVE_QUEUE,
            Duration::from_secs(1),
        );
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
        let mut strategy = postgres_worker_strategy_with_interval(
            &pool,
            CACHE_KEEPALIVE_QUEUE,
            Duration::from_secs(1),
        );
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

    #[tokio::test]
    async fn postgres_scheduler_backend_shares_one_insert_listener_across_three_queues_outside_main_pool()
     {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            return;
        };
        let app_name = format!("cclb_shared_listen_{}", ulid::Ulid::generate());
        let connect_opts = database_url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .expect("parse DATABASE_URL")
            .application_name(&app_name);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy_with(connect_opts.clone());
        let listener_pool = PgPoolOptions::new()
            .max_connections(2)
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy_with(connect_opts);
        let observer_pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&database_url)
            .await
            .expect("observer pool connects");

        let backend = super::PostgresSchedulerBackend::new_with_listener_pool(
            pool.clone(),
            listener_pool.clone(),
        );
        let mut s_adaptive =
            super::postgres_worker_strategy(&backend.fanout, super::ADAPTIVE_QUEUE);
        let mut s_keepalive =
            super::postgres_worker_strategy(&backend.fanout, super::CACHE_KEEPALIVE_QUEUE);
        let mut s_cron = super::postgres_worker_strategy(&backend.fanout, super::CRON_QUEUE);
        let snapshot = PollSnapshot::new();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for strategy in [&mut s_adaptive, &mut s_keepalive, &mut s_cron] {
                    let waker_state = CountingWaker::new();
                    let waker = Waker::from(Arc::clone(&waker_state));
                    let mut cx = Context::from_waker(&waker);
                    let _ = strategy.poll_drive(&mut cx, &snapshot);
                }
                let listening: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND application_name = $1
                       AND query ILIKE 'LISTEN %apalis::job::insert%'",
                )
                .bind(&app_name)
                .fetch_one(&observer_pool)
                .await
                .expect("query listen count");
                if listening > 0 {
                    assert_eq!(listening, 1, "expected exactly 1 shared LISTEN session");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("shared insert fanout connects");

        assert_eq!(
            pool.size(),
            0,
            "shared insert listener must not occupy slots in the main scheduler pool"
        );

        let payload = serde_json::json!({
            "job_type": super::CRON_QUEUE,
            "id": ulid::Ulid::generate().to_string(),
        });
        sqlx::query("SELECT pg_notify('apalis::job::insert', $1)")
            .bind(payload.to_string())
            .execute(&observer_pool)
            .await
            .expect("notify cron queue");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waker = Waker::from(CountingWaker::new());
                let mut cx = Context::from_waker(&waker);
                if s_cron.poll_drive(&mut cx, &snapshot).is_ready() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cron strategy wakes on routed notification");

        let waker = Waker::from(CountingWaker::new());
        let mut cx = Context::from_waker(&waker);
        assert!(s_adaptive.poll_drive(&mut cx, &snapshot).is_pending());
        assert!(s_keepalive.poll_drive(&mut cx, &snapshot).is_pending());

        drop((s_adaptive, s_keepalive, s_cron, backend));
        listener_pool.close().await;
        pool.close().await;
        observer_pool.close().await;
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
