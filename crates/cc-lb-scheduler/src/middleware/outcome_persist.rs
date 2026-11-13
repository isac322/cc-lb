use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use apalis_core::backend::{Backend, BackendConfig, WireFormatBackend};
use apalis_core::error::{AbortError, BoxDynError};
use apalis_core::task::Task;
use apalis_core::worker::context::WorkerContext;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};

use crate::error::SchedulerError;
use crate::retry::JobOutcome;

#[derive(Clone, Debug)]
enum OutcomePool {
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}

#[derive(Clone, Debug)]
pub(crate) struct OutcomePersistLayer {
    pool: OutcomePool,
    cancel: CancellationToken,
    in_flight: Arc<AtomicUsize>,
    idle_notify: Arc<Notify>,
    fatal_abort: CancellationToken,
    fatal_error: Arc<Mutex<Option<String>>>,
}

impl OutcomePersistLayer {
    #[cfg(feature = "sqlite")]
    pub(crate) fn sqlite(pool: sqlx::SqlitePool, cancel: CancellationToken) -> Self {
        Self::new(OutcomePool::Sqlite(pool), cancel)
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres(pool: sqlx::PgPool, cancel: CancellationToken) -> Self {
        Self::new(OutcomePool::Postgres(pool), cancel)
    }

    fn new(pool: OutcomePool, cancel: CancellationToken) -> Self {
        Self {
            pool,
            cancel,
            in_flight: Arc::new(AtomicUsize::new(0)),
            idle_notify: Arc::new(Notify::new()),
            fatal_abort: CancellationToken::new(),
            fatal_error: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn wrap_storage<B>(&self, inner: B) -> DrainableStorage<B> {
        DrainableStorage {
            inner,
            cancel: self.cancel.clone(),
            fatal_abort: self.fatal_abort.clone(),
            in_flight_fetch: false,
        }
    }

    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.idle_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_fatal_abort(&self) -> String {
        self.fatal_abort.cancelled().await;
        self.fatal_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| "scheduler start lease recovery failed".to_owned())
    }

    pub(crate) fn fatal_abort_reason(&self) -> Option<String> {
        if !self.fatal_abort.is_cancelled() {
            return None;
        }
        Some(
            self.fatal_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .unwrap_or_else(|| "scheduler start lease recovery failed".to_owned()),
        )
    }
}

impl<S> Layer<S> for OutcomePersistLayer {
    type Service = OutcomePersistService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OutcomePersistService {
            inner,
            pool: self.pool.clone(),
            cancel: self.cancel.clone(),
            in_flight: self.in_flight.clone(),
            idle_notify: self.idle_notify.clone(),
            fatal_abort: self.fatal_abort.clone(),
            fatal_error: self.fatal_error.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DrainableStorage<B> {
    inner: B,
    cancel: CancellationToken,
    fatal_abort: CancellationToken,
    in_flight_fetch: bool,
}

impl<B: Backend> Backend for DrainableStorage<B> {
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let draining = self.cancel.is_cancelled() || self.fatal_abort.is_cancelled();
        if draining && !self.in_flight_fetch {
            return Poll::Pending;
        }
        match self.inner.poll_next(cx, worker) {
            Poll::Pending => {
                self.in_flight_fetch = true;
                Poll::Pending
            }
            Poll::Ready(item) => {
                self.in_flight_fetch = false;
                Poll::Ready(item)
            }
        }
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_close(cx, worker)
    }
}

impl<B: BackendConfig> BackendConfig for DrainableStorage<B> {
    type Args = B::Args;
    type Id = B::Id;
    type Kind = B::Kind;
    type Config = B::Config;
    type Layer = B::Layer;

    fn config(&self) -> &Self::Config {
        self.inner.config()
    }

    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer {
        self.inner.middleware(worker)
    }
}

impl<B: WireFormatBackend> WireFormatBackend for DrainableStorage<B> {
    type Codec = B::Codec;
    type Compact = B::Compact;

    fn codec(&self) -> &Self::Codec {
        self.inner.codec()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OutcomePersistService<S> {
    inner: S,
    pool: OutcomePool,
    cancel: CancellationToken,
    in_flight: Arc<AtomicUsize>,
    idle_notify: Arc<Notify>,
    fatal_abort: CancellationToken,
    fatal_error: Arc<Mutex<Option<String>>>,
}

struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
    idle_notify: Arc<Notify>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.idle_notify.notify_waiters();
        }
    }
}

impl<S, Args> Service<Task<Args>> for OutcomePersistService<S>
where
    Args: Send + 'static,
    S: Service<Task<Args>, Response = JobOutcome> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError> + Send + 'static,
{
    type Response = JobOutcome;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<JobOutcome, BoxDynError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let task_id = task.task_id().map(ToString::to_string);
        let lock_by = task.lock_by().map(ToOwned::to_owned);
        let pre_attempt = i64::try_from(task.attempt()).unwrap_or(i64::MAX);
        let ctx = task.ctx().clone();
        let pool = self.pool.clone();
        let cancel = self.cancel.clone();
        let fatal_abort = self.fatal_abort.clone();
        let fatal_error = self.fatal_error.clone();
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let guard = InFlightGuard {
            in_flight: self.in_flight.clone(),
            idle_notify: self.idle_notify.clone(),
        };

        Box::pin(async move {
            let _guard = guard;
            let (Some(task_id), Some(lock_by)) = (task_id, lock_by) else {
                return Err(Box::new(SchedulerError::Job(
                    "missing task id or lock owner".to_owned(),
                )) as BoxDynError);
            };
            if cancel.is_cancelled() || fatal_abort.is_cancelled() {
                let _ = pool.release_queued(&task_id, &lock_by, pre_attempt).await;
                return Ok(JobOutcome::Noop);
            }
            match pool.start(&task_id, &lock_by, pre_attempt).await {
                Err(db_err) => {
                    match pool
                        .recover_queued_lease(&task_id, &lock_by, pre_attempt)
                        .await
                    {
                        Ok(_) => {
                            tracing::warn!(task_id = %task_id, error = %db_err, "scheduler start mark failed; released queued lease");
                        }
                        Err(recover_err) => {
                            fatal_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get_or_insert_with(|| format!(
                                    "scheduler start lease recovery failed for task {task_id}: start error: {db_err}; recovery error: {recover_err}"
                                ));
                            fatal_abort.cancel();
                            tracing::error!(task_id = %task_id, start_error = %db_err, recovery_error = %recover_err, "scheduler start mark and lease recovery failed; escalating to worker supervision");
                        }
                    }
                    return Err(Box::new(SchedulerError::Database(db_err)) as BoxDynError);
                }
                Ok(0) => {
                    tracing::debug!(task_id = %task_id, "scheduler task lease not held at start; skipping execution");
                    return Ok(JobOutcome::Noop);
                }
                Ok(_) => {}
            }

            let res: Result<JobOutcome, BoxDynError> = inner.call(task).await.map_err(Into::into);
            let post_attempt =
                i64::try_from(ctx.attempt()).unwrap_or(pre_attempt.saturating_add(1));
            let (target_status, delay, last_result) = match &res {
                Ok(outcome) => {
                    let (status, delay) = match outcome {
                        JobOutcome::Done | JobOutcome::Skip | JobOutcome::Noop => ("Done", None),
                        JobOutcome::Retry { delay: Some(delay) } if !delay.is_zero() => {
                            ("Pending", Some(*delay))
                        }
                        JobOutcome::DeadLetter | JobOutcome::Retry { .. } => ("Killed", None),
                    };
                    (status, delay, serde_json::json!({ "Ok": outcome }))
                }
                Err(err) => {
                    let killed = err.downcast_ref::<AbortError>().is_some()
                        || err.downcast_ref::<SchedulerError>().is_some_and(|error| {
                            matches!(error, SchedulerError::InvalidRetryDelay)
                        });
                    (
                        if killed { "Killed" } else { "Failed" },
                        None,
                        serde_json::json!({ "Err": err.to_string() }),
                    )
                }
            };
            let last_result = serde_json::to_string(&last_result)
                .map_err(|error| Box::new(SchedulerError::Job(error.to_string())) as BoxDynError)?;
            match pool
                .finish(
                    &task_id,
                    &lock_by,
                    pre_attempt,
                    post_attempt,
                    &last_result,
                    target_status,
                    delay,
                )
                .await
            {
                Err(db_err) => {
                    tracing::warn!(task_id = %task_id, status = target_status, error = %db_err, "scheduler finish persist failed");
                    return Err(Box::new(SchedulerError::Database(db_err)) as BoxDynError);
                }
                Ok(0) => {
                    tracing::debug!(task_id = %task_id, status = target_status, "scheduler finish persist skipped after lease change");
                }
                Ok(_) => {}
            }
            res
        })
    }
}

#[cfg(feature = "sqlite")]
pub(crate) fn sqlite_retry_run_at_secs(now_since_epoch: Duration, delay: Duration) -> i64 {
    let deadline = now_since_epoch.saturating_add(delay);
    let secs = deadline
        .as_secs()
        .saturating_add(u64::from(deadline.subsec_nanos() > 0));
    i64::try_from(secs).unwrap_or(i64::MAX)
}

impl OutcomePool {
    async fn recover_queued_lease(
        &self,
        task_id: &str,
        lock_by: &str,
        pre_attempt: i64,
    ) -> Result<u64, sqlx::Error> {
        let mut result = self.release_queued(task_id, lock_by, pre_attempt).await;
        for delay_ms in [15, 45] {
            if result.is_ok() {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            result = self.release_queued(task_id, lock_by, pre_attempt).await;
        }
        result
    }

    async fn release_queued(
        &self,
        task_id: &str,
        lock_by: &str,
        pre_attempt: i64,
    ) -> Result<u64, sqlx::Error> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => sqlx::query("UPDATE Jobs SET status = 'Pending', lock_by = NULL, lock_at = NULL WHERE id = ?1 AND lock_by = ?2 AND attempts = ?3 AND status = 'Queued'")
                .bind(task_id).bind(lock_by).bind(pre_attempt).execute(pool).await.map(|result| result.rows_affected()),
            #[cfg(feature = "postgres")]
            Self::Postgres(pool) => sqlx::query("UPDATE apalis.jobs SET status = 'Pending', lock_by = NULL, lock_at = NULL WHERE id = $1 AND lock_by = $2 AND attempts = $3 AND status = 'Queued'")
                .bind(task_id).bind(lock_by).bind(i32::try_from(pre_attempt).unwrap_or(i32::MAX)).execute(pool).await.map(|result| result.rows_affected()),
        }
    }

    async fn start(
        &self,
        task_id: &str,
        lock_by: &str,
        pre_attempt: i64,
    ) -> Result<u64, sqlx::Error> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => sqlx::query("UPDATE Jobs SET status = 'Running', lock_at = strftime('%s', 'now') WHERE id = ?1 AND lock_by = ?2 AND attempts = ?3 AND status = 'Queued'")
                .bind(task_id).bind(lock_by).bind(pre_attempt).execute(pool).await.map(|result| result.rows_affected()),
            #[cfg(feature = "postgres")]
            Self::Postgres(pool) => sqlx::query("UPDATE apalis.jobs SET status = 'Running', lock_at = now() WHERE id = $1 AND lock_by = $2 AND attempts = $3 AND status = 'Queued'")
                .bind(task_id).bind(lock_by).bind(i32::try_from(pre_attempt).unwrap_or(i32::MAX)).execute(pool).await.map(|result| result.rows_affected()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish(
        &self,
        task_id: &str,
        lock_by: &str,
        pre_attempt: i64,
        post_attempt: i64,
        last_result: &str,
        status: &str,
        delay: Option<Duration>,
    ) -> Result<u64, sqlx::Error> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => {
                let result = if let Some(delay) = delay {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    let run_at = sqlite_retry_run_at_secs(now, delay);
                    sqlx::query("UPDATE Jobs SET status = 'Pending', attempts = ?4, last_result = ?5, run_at = ?6, lock_by = NULL, lock_at = NULL, done_at = NULL WHERE id = ?1 AND lock_by = ?2 AND attempts = ?3 AND status IN ('Queued', 'Running')")
                        .bind(task_id).bind(lock_by).bind(pre_attempt).bind(post_attempt).bind(last_result).bind(run_at).execute(pool).await?
                } else {
                    sqlx::query("UPDATE Jobs SET status = ?6, attempts = ?4, last_result = ?5, done_at = strftime('%s', 'now') WHERE id = ?1 AND lock_by = ?2 AND attempts = ?3 AND status IN ('Queued', 'Running')")
                        .bind(task_id).bind(lock_by).bind(pre_attempt).bind(post_attempt).bind(last_result).bind(status).execute(pool).await?
                };
                Ok(result.rows_affected())
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(pool) => {
                let pre_attempt = i32::try_from(pre_attempt).unwrap_or(i32::MAX);
                let post_attempt = i32::try_from(post_attempt).unwrap_or(i32::MAX);
                let result = if let Some(delay) = delay {
                    sqlx::query("UPDATE apalis.jobs SET status = 'Pending', attempts = $4, last_result = $5::jsonb, run_at = now() + make_interval(secs => $6::double precision), lock_by = NULL, lock_at = NULL, done_at = NULL WHERE id = $1 AND lock_by = $2 AND attempts = $3 AND status IN ('Queued', 'Running')")
                        .bind(task_id).bind(lock_by).bind(pre_attempt).bind(post_attempt).bind(last_result).bind(delay.as_secs_f64()).execute(pool).await?
                } else {
                    sqlx::query("UPDATE apalis.jobs SET status = $6, attempts = $4, last_result = $5::jsonb, done_at = now() WHERE id = $1 AND lock_by = $2 AND attempts = $3 AND status IN ('Queued', 'Running')")
                        .bind(task_id).bind(lock_by).bind(pre_attempt).bind(post_attempt).bind(last_result).bind(status).execute(pool).await?
                };
                Ok(result.rows_affected())
            }
        }
    }
}

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
mod test_support {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use apalis_core::task::builder::TaskBuilder;
    use tokio::sync::Notify;
    use tower::{ServiceExt, service_fn};

    use super::*;
    use crate::middleware::{SchedulerMetricPayload, SchedulerMetricsLayer};

    #[derive(Clone)]
    pub(super) struct Args;

    impl SchedulerMetricPayload for Args {
        fn scheduler_job_type(&self) -> &'static str {
            "adaptive:oauth_refresh"
        }
    }

    pub(super) fn task() -> Task<Args> {
        TaskBuilder::new(Args)
            .task_id("lease-test".to_owned().into())
            .lock_by(Some("worker".to_owned()))
            .attempt(0)
            .build()
    }

    impl OutcomePool {
        pub(super) async fn reset_row(&self, owner: &str) {
            match self {
                #[cfg(feature = "sqlite")]
                Self::Sqlite(pool) => {
                    sqlx::query("DELETE FROM Jobs")
                        .execute(pool)
                        .await
                        .expect("clear rows");
                    sqlx::query("INSERT INTO Jobs (id, status, attempts, lock_by, lock_at, run_at, last_result) VALUES ('lease-test', 'Queued', 0, ?1, 100, 200, 'original')")
                        .bind(owner).execute(pool).await.expect("insert lease");
                }
                #[cfg(feature = "postgres")]
                Self::Postgres(pool) => {
                    sqlx::query("DELETE FROM apalis.jobs")
                        .execute(pool)
                        .await
                        .expect("clear rows");
                    sqlx::query("INSERT INTO apalis.jobs (id, status, attempts, lock_by, lock_at, run_at, last_result) VALUES ('lease-test', 'Queued', 0, $1, to_timestamp(100), to_timestamp(200), '\"original\"'::jsonb)")
                        .bind(owner).execute(pool).await.expect("insert lease");
                }
            }
        }

        async fn snapshot(&self) -> String {
            match self {
                #[cfg(feature = "sqlite")]
                Self::Sqlite(pool) => sqlx::query_scalar("SELECT json_object('id', id, 'status', status, 'attempts', attempts, 'lock_by', lock_by, 'lock_at', lock_at, 'run_at', run_at, 'done_at', done_at, 'last_result', last_result) FROM Jobs WHERE id = 'lease-test'")
                    .fetch_one(pool).await.expect("snapshot sqlite row"),
                #[cfg(feature = "postgres")]
                Self::Postgres(pool) => sqlx::query_scalar("SELECT row_to_json(jobs)::text FROM apalis.jobs WHERE id = 'lease-test'")
                    .fetch_one(pool).await.expect("snapshot postgres row"),
            }
        }

        async fn reclaim_same_worker(&self) {
            match self {
                #[cfg(feature = "sqlite")]
                Self::Sqlite(pool) => {
                    let status: String =
                        sqlx::query_scalar("SELECT status FROM Jobs WHERE id = 'lease-test'")
                            .fetch_one(pool)
                            .await
                            .expect("read running status");
                    assert_eq!(status, "Running");
                    sqlx::query("UPDATE Jobs SET attempts = attempts + 1, status = 'Queued', lock_by = 'worker', lock_at = 300, last_result = 'new lease' WHERE id = 'lease-test'")
                        .execute(pool).await.expect("reclaim sqlite lease");
                }
                #[cfg(feature = "postgres")]
                Self::Postgres(pool) => {
                    let status: String = sqlx::query_scalar(
                        "SELECT status FROM apalis.jobs WHERE id = 'lease-test'",
                    )
                    .fetch_one(pool)
                    .await
                    .expect("read running status");
                    assert_eq!(status, "Running");
                    sqlx::query("UPDATE apalis.jobs SET attempts = attempts + 1, status = 'Queued', lock_by = 'worker', lock_at = to_timestamp(300), last_result = '\"new lease\"'::jsonb WHERE id = 'lease-test'")
                        .execute(pool).await.expect("reclaim postgres lease");
                }
            }
        }
    }

    fn started_count(handle: &metrics_exporter_prometheus::PrometheusHandle) -> f64 {
        handle
            .render()
            .lines()
            .filter(|line| {
                line.starts_with(crate::scheduler_metrics::JOBS_TOTAL)
                    && line.contains("job_type=\"adaptive:oauth_refresh\"")
                    && line.contains("status=\"started\"")
            })
            .map(|line| {
                line.rsplit_once(' ')
                    .expect("metric value")
                    .1
                    .parse::<f64>()
                    .expect("numeric counter")
            })
            .sum()
    }

    pub(super) async fn fencing(
        pool: OutcomePool,
        handle: &metrics_exporter_prometheus::PrometheusHandle,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let inner = SchedulerMetricsLayer::new().layer(service_fn(move |_task: Task<Args>| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, BoxDynError>(JobOutcome::Done))
        }));
        pool.reset_row("other-worker").await;
        let before = pool.snapshot().await;
        let layer = OutcomePersistLayer::new(pool.clone(), CancellationToken::new());
        assert_eq!(
            layer
                .layer(inner)
                .oneshot(task())
                .await
                .expect("lost lease is noop"),
            JobOutcome::Noop
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(started_count(handle), 0.0);
        assert_eq!(pool.snapshot().await, before);

        pool.reset_row("worker").await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let observed = calls.clone();
        let inner_entered = entered.clone();
        let inner_release = release.clone();
        let inner = SchedulerMetricsLayer::new().layer(service_fn(move |_task: Task<Args>| {
            observed.fetch_add(1, Ordering::SeqCst);
            let entered = inner_entered.clone();
            let release = inner_release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                Ok::<_, BoxDynError>(JobOutcome::Retry {
                    delay: Some(Duration::from_secs(30)),
                })
            }
        }));
        let finish = layer.layer(inner).oneshot(task());
        let replace_lease = async {
            entered.notified().await;
            pool.reclaim_same_worker().await;
            let newer_row = pool.snapshot().await;
            release.notify_one();
            newer_row
        };
        let (result, newer_row) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(finish, replace_lease)
        })
        .await
        .expect("bounded lease fencing");
        assert_eq!(
            result.expect("stale finish returns original outcome"),
            JobOutcome::Retry {
                delay: Some(Duration::from_secs(30))
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(started_count(handle), 1.0);
        assert_eq!(pool.snapshot().await, newer_row);
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use metrics_exporter_prometheus::PrometheusBuilder;
    use tower::{ServiceExt, service_fn};

    use super::test_support::{Args, task};
    use super::*;

    #[test]
    fn sqlite_retry_run_at_rounds_fractional_deadline_upward() {
        for (now, delay, expected) in [
            (
                Duration::from_millis(100_850),
                Duration::from_millis(250),
                102,
            ),
            (Duration::from_millis(100_850), Duration::from_secs(1), 102),
            (Duration::from_millis(100_850), Duration::from_secs(30), 131),
            (Duration::from_secs(100), Duration::from_secs(30), 130),
            (
                Duration::new(100, 999_999_999),
                Duration::from_nanos(1),
                101,
            ),
            (
                Duration::new(100, 999_999_999),
                Duration::from_nanos(2),
                102,
            ),
            (Duration::MAX, Duration::from_secs(30), i64::MAX),
            (Duration::from_secs(100), Duration::MAX, i64::MAX),
        ] {
            assert_eq!(
                sqlite_retry_run_at_secs(now, delay),
                expected,
                "now={now:?}, delay={delay:?}"
            );
        }
    }

    async fn pool() -> sqlx::SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool");
        sqlx::query("CREATE TABLE Jobs (id TEXT PRIMARY KEY, status TEXT NOT NULL, attempts INTEGER NOT NULL, lock_by TEXT, lock_at INTEGER, run_at INTEGER, done_at INTEGER, last_result TEXT)")
            .execute(&pool).await.expect("sqlite fixture schema");
        pool
    }

    #[test]
    fn sqlite_outcome_persist_fences_lost_lease_and_same_worker_stale_finish() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let pool = pool().await;
                super::test_support::fencing(OutcomePool::Sqlite(pool.clone()), &handle).await;
                pool.close().await;
            })
        });
    }

    #[tokio::test]
    async fn sqlite_outcome_persist_closed_pool_skips_inner_and_returns_err() {
        let pool = pool().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let layer = OutcomePersistLayer::sqlite(pool.clone(), CancellationToken::new());
        let service = layer.layer(service_fn(move |_task: Task<Args>| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, BoxDynError>(JobOutcome::Done))
        }));
        pool.close().await;
        let error = service
            .oneshot(task())
            .await
            .expect_err("closed start pool");
        assert!(matches!(
            error.downcast_ref::<SchedulerError>(),
            Some(SchedulerError::Database(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let reason = tokio::time::timeout(Duration::from_secs(10), layer.wait_fatal_abort())
            .await
            .expect("failed lease recovery must reach supervision");
        assert!(reason.contains("scheduler start lease recovery failed"));
        layer.wait_idle().await;

        let pool = self::pool().await;
        OutcomePool::Sqlite(pool.clone()).reset_row("worker").await;
        let inner_pool = pool.clone();
        let service = OutcomePersistLayer::sqlite(pool, CancellationToken::new()).layer(
            service_fn(move |_task: Task<Args>| {
                let pool = inner_pool.clone();
                async move {
                    pool.close().await;
                    Ok::<_, BoxDynError>(JobOutcome::Done)
                }
            }),
        );
        let error = service
            .oneshot(task())
            .await
            .expect_err("closed finish pool");
        assert!(matches!(
            error.downcast_ref::<SchedulerError>(),
            Some(SchedulerError::Database(_))
        ));
    }
}

#[cfg(all(test, feature = "postgres"))]
mod postgres_tests {
    use std::panic::AssertUnwindSafe;

    use futures_util::FutureExt;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use sqlx::migrate::MigrateDatabase;

    use super::*;

    #[test]
    fn postgres_outcome_persist_fences_lost_lease_and_same_worker_stale_finish()
    -> Result<(), BoxDynError> {
        let Ok(admin_url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP: DATABASE_URL not set - skipping postgres outcome persistence test");
            return Ok(());
        };
        let mut database_url = url::Url::parse(&admin_url)?;
        let database_name = ["cclb_outcome_", &uuid::Uuid::new_v4().simple().to_string()].concat();
        database_url.set_path(&database_name);
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new().max_connections(2).connect_lazy(database_url.as_str())?;
            sqlx::Postgres::create_database(database_url.as_str()).await?;
            let result = tokio::time::timeout(Duration::from_secs(30), AssertUnwindSafe(async {
                sqlx::query("CREATE SCHEMA apalis").execute(&pool).await.expect("postgres fixture schema");
                sqlx::query("CREATE TABLE apalis.jobs (id TEXT PRIMARY KEY, status TEXT NOT NULL, attempts INTEGER NOT NULL, lock_by TEXT, lock_at TIMESTAMPTZ, run_at TIMESTAMPTZ, done_at TIMESTAMPTZ, last_result JSONB)")
                    .execute(&pool).await.expect("postgres fixture table");
                super::test_support::fencing(OutcomePool::Postgres(pool.clone()), &handle).await;
            }).catch_unwind()).await;
            pool.close().await;
            sqlx::Postgres::drop_database(database_url.as_str()).await?;
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(panic)) => std::panic::resume_unwind(panic),
                Err(error) => Err(Box::new(error) as BoxDynError),
            }
        })
        })
    }
}

#[cfg(test)]
mod drain_tests {
    use super::*;

    struct FetchBackend {
        polls: usize,
        pending_once: bool,
    }

    impl Backend for FetchBackend {
        type Task = ();
        type Error = std::io::Error;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
            _worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_next(
            &mut self,
            cx: &mut Context<'_>,
            _worker: &WorkerContext,
        ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
            self.polls += 1;
            if std::mem::take(&mut self.pending_once) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Some(Ok(())))
            }
        }

        fn poll_close(
            &mut self,
            _cx: &mut Context<'_>,
            _worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn drainable_storage_stops_admission_and_completes_only_an_existing_fetch() {
        for fatal in [false, true] {
            for pending_once in [false, true] {
                let cancel = CancellationToken::new();
                let fatal_abort = CancellationToken::new();
                let mut storage = DrainableStorage {
                    inner: FetchBackend {
                        polls: 0,
                        pending_once,
                    },
                    cancel: cancel.clone(),
                    fatal_abort: fatal_abort.clone(),
                    in_flight_fetch: false,
                };
                let worker = WorkerContext::from("drain-test");
                let mut cx = Context::from_waker(std::task::Waker::noop());
                if pending_once {
                    assert!(storage.poll_next(&mut cx, &worker).is_pending());
                    assert_eq!(storage.inner.polls, 1);
                }
                if fatal {
                    fatal_abort.cancel();
                    assert!(
                        !cancel.is_cancelled(),
                        "fatal abort must not cancel the supervisor"
                    );
                } else {
                    cancel.cancel();
                }
                if pending_once {
                    assert!(matches!(
                        storage.poll_next(&mut cx, &worker),
                        Poll::Ready(Some(Ok(())))
                    ));
                    assert_eq!(storage.inner.polls, 2);
                }
                let polls_before = storage.inner.polls;
                assert!(storage.poll_next(&mut cx, &worker).is_pending());
                assert!(storage.poll_next(&mut cx, &worker).is_pending());
                assert_eq!(
                    storage.inner.polls, polls_before,
                    "draining must never start another fetch"
                );
                assert!(matches!(
                    storage.poll_close(&mut cx, &worker),
                    Poll::Ready(Ok(()))
                ));
            }
        }
    }
}
