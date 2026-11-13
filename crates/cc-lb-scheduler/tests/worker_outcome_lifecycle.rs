#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod support {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{Context as _, Result};
    use cc_lb_clock::SystemClock;
    use cc_lb_config::SchedulerConfig;
    use cc_lb_scheduler::error::SchedulerError;
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
    use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
    use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
    use cc_lb_scheduler::retry::JobOutcome;
    use cc_lb_scheduler::worker::{
        ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob, SchedulerBackend,
        SchedulerCtx, SchedulerPushTask, build_adaptive_worker, build_cron_worker,
    };
    use cc_lb_storage_api::CacheTtl;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    pub(super) const DEADLINE: Duration = Duration::from_secs(30);
    pub(super) type Handler = Arc<dyn Fn() -> Result<JobOutcome, SchedulerError> + Send + Sync>;

    pub(super) fn context(handler: Handler) -> SchedulerCtx {
        let adaptive = handler.clone();
        let mut config = SchedulerConfig::default();
        config.recurring_jobs.clear();
        SchedulerCtx::new(
            config,
            Arc::new(move |_| {
                let handler = adaptive.clone();
                Box::pin(async move { handler() })
            }),
            Arc::new(move |_| {
                let handler = handler.clone();
                Box::pin(async move { handler() })
            }),
            Arc::new(SystemClock),
        )
    }

    pub(super) fn fixed(outcome: JobOutcome) -> SchedulerCtx {
        context(Arc::new(move || Ok(outcome.clone())))
    }

    // The guard also aborts workers if an assertion or SQL operation fails.
    pub(super) struct Workers {
        pub(super) cancel: CancellationToken,
        handles: Vec<JoinHandle<()>>,
    }

    impl Workers {
        pub(super) async fn start(
            backend: &SchedulerBackend,
            ctx: SchedulerCtx,
            queue: &str,
            count: usize,
        ) -> Result<Self> {
            let mut workers = Self {
                cancel: CancellationToken::new(),
                handles: Vec::new(),
            };
            for _ in 0..count {
                let cancel = workers.cancel.clone();
                match queue {
                    ADAPTIVE_QUEUE => {
                        let worker = build_adaptive_worker(backend, ctx.clone())?;
                        workers.handles.push(tokio::spawn(async move {
                            worker
                                .run_until_cancelled(cancel)
                                .await
                                .expect("adaptive worker exits cleanly");
                        }));
                    }
                    CRON_QUEUE => {
                        let worker = build_cron_worker(backend, ctx.clone())?;
                        workers.handles.push(tokio::spawn(async move {
                            worker
                                .run_until_cancelled(cancel)
                                .await
                                .expect("cron worker exits cleanly");
                        }));
                    }
                    CACHE_KEEPALIVE_QUEUE => {
                        workers
                            .handles
                            .extend(backend.spawn(ctx.clone(), cancel).await?);
                    }
                    _ => unreachable!("known scheduler queue"),
                }
            }
            Ok(workers)
        }

        pub(super) async fn stop(mut self) -> Result<()> {
            self.cancel.cancel();
            timeout(DEADLINE, async {
                for handle in &mut self.handles {
                    handle.await.context("join scheduler worker")?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await
            .context("worker shutdown deadline")??;
            self.handles.clear();
            Ok(())
        }
    }

    impl Drop for Workers {
        fn drop(&mut self) {
            self.cancel.cancel();
            for handle in &self.handles {
                handle.abort();
            }
        }
    }

    pub(super) async fn enqueue(
        backend: &SchedulerBackend,
        queue: &str,
        key: &str,
        upstream_id: Uuid,
        max_attempts: Option<i32>,
    ) -> Result<()> {
        if queue == CRON_QUEUE {
            backend
                .push_cron_task(SchedulerPushTask {
                    args: CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()),
                    idempotency_key: Some(key.to_owned()),
                    run_at_unix_secs: Some(0),
                    max_attempts,
                })
                .await?;
        } else {
            let args = if queue == CACHE_KEEPALIVE_QUEUE {
                AdaptiveJob::CacheKeepalive(CacheKeepaliveJob {
                    session_key_hash: key.to_owned(),
                    generation: 1,
                    principal_id: "lifecycle-principal".to_owned(),
                    upstream_id,
                    ttl: CacheTtl::Ttl5m,
                    cache_anchor_at_unix_secs: 100,
                    expires_at_unix_secs: 400,
                    refresh_delay_secs: 270,
                    max_refreshes: 3,
                    max_total_duration_secs: 600,
                    traceparent: None,
                })
            } else {
                AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1))
            };
            let task = SchedulerPushTask {
                args,
                idempotency_key: Some(key.to_owned()),
                run_at_unix_secs: Some(0),
                max_attempts,
            };
            if queue == CACHE_KEEPALIVE_QUEUE {
                backend.push_keepalive_task(task).await?;
            } else {
                backend.push_adaptive_task(task).await?;
            }
        }
        Ok(())
    }

    #[derive(Debug)]
    pub(super) struct State {
        pub(super) status: String,
        pub(super) attempts: i64,
        pub(super) run_at: i64,
        pub(super) lock_by: Option<String>,
        pub(super) lock_at: Option<i64>,
        pub(super) done_at: Option<i64>,
        pub(super) last_result: Option<String>,
    }

    impl State {
        pub(super) fn assert_pending(&self) {
            assert_eq!(self.status, "Pending", "{self:?}");
            assert!(self.lock_by.is_none(), "{self:?}");
            assert!(self.lock_at.is_none(), "{self:?}");
            assert!(self.done_at.is_none(), "{self:?}");
        }

        pub(super) fn assert_killed(&self) {
            assert_eq!(self.status, "Killed", "{self:?}");
            assert!(self.done_at.is_some(), "{self:?}");
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use std::collections::HashMap;
    use std::str::FromStr as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use anyhow::{Context as _, Result};
    use cc_lb_clock::SystemClock;
    use cc_lb_config::SchedulerConfig;
    use cc_lb_scheduler::admin::SchedulerAdminHandle;
    use cc_lb_scheduler::error::SchedulerError;
    use cc_lb_scheduler::jobs::apalis_housekeeping::{
        ApalisHousekeepingConfig, ApalisHousekeepingJob, ApalisHousekeepingJobHandler,
        ApalisHousekeepingJobResult,
    };
    use cc_lb_scheduler::jobs::watchdog::{WatchdogEntityKind, run_entity_watchdog};
    use cc_lb_scheduler::retry::JobOutcome;
    use cc_lb_scheduler::scheduler_metrics;
    use cc_lb_scheduler::worker::{
        ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, SchedulerBackend,
        SchedulerCtx, SqliteSchedulerBackend, build_adaptive_worker,
    };
    use metrics_exporter_prometheus::PrometheusBuilder;
    use sqlx::Row as _;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::support::*;

    static SQLITE_TEST_DIR: std::sync::LazyLock<tempfile::TempDir> =
        std::sync::LazyLock::new(|| tempfile::tempdir().expect("sqlite lifecycle tempdir"));

    fn sqlite_test_dir() -> &'static std::path::Path {
        SQLITE_TEST_DIR.path()
    }

    async fn database() -> Result<(sqlx::SqlitePool, SchedulerBackend)> {
        use std::str::FromStr as _;
        // Use an isolated file-backed SQLite DB with max_connections(1) so WAL mode
        // applies as in production and a cancelled in-flight query never wipes schema.
        let db_path = sqlite_test_dir().join(format!("{}.sqlite", Uuid::new_v4()));
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            db_path.display()
        ))?
        .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await?;
        apalis_sqlite::SqliteStorage::setup(&pool).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
        let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
            pool.clone(),
            Arc::new(SystemClock),
        ));
        Ok((pool, backend))
    }
    async fn open_pair(
        dir: &tempfile::TempDir,
    ) -> Result<(sqlx::SqlitePool, sqlx::SqlitePool, SchedulerBackend)> {
        let url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
        let worker_options = sqlx::sqlite::SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_millis(5));
        let worker_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(worker_options)
            .await?;
        apalis_sqlite::SqliteStorage::setup(&worker_pool).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&worker_pool).await?;
        let fault_options = sqlx::sqlite::SqliteConnectOptions::from_str(&url)?
            .busy_timeout(Duration::from_secs(1));
        let fault_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(fault_options)
            .await?;
        let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
            worker_pool.clone(),
            Arc::new(SystemClock),
        ));
        Ok((worker_pool, fault_pool, backend))
    }

    async fn state(pool: &sqlx::SqlitePool, key: &str) -> Result<State> {
        let row = sqlx::query(
            "SELECT status, attempts, run_at, lock_by, lock_at, done_at, last_result FROM Jobs WHERE idempotency_key = ?1",
        ).bind(key).fetch_one(pool).await?;
        Ok(State {
            status: row.try_get("status")?,
            attempts: row.try_get("attempts")?,
            run_at: row.try_get("run_at")?,
            lock_by: row.try_get("lock_by")?,
            lock_at: row.try_get("lock_at")?,
            done_at: row.try_get("done_at")?,
            last_result: row.try_get("last_result")?,
        })
    }

    async fn settled(
        pool: &sqlx::SqlitePool,
        key: &str,
        status: &str,
        attempts: i64,
    ) -> Result<State> {
        timeout(DEADLINE, async {
            loop {
                let row = state(pool, key).await?;
                if row.status == status && row.attempts == attempts {
                    return Ok(row);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("waiting for {key}: {status}, attempts={attempts}"))?
    }

    async fn db_now(pool: &sqlx::SqlitePool) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
                .fetch_one(pool)
                .await?,
        )
    }

    async fn due(pool: &sqlx::SqlitePool, key: &str) -> Result<()> {
        sqlx::query("UPDATE Jobs SET run_at = 0 WHERE idempotency_key = ?1")
            .bind(key)
            .execute(pool)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_worker_outcome_state_matrix_all_queues() -> Result<()> {
        for queue in [ADAPTIVE_QUEUE, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE] {
            for case in 0..9 {
                let (pool, backend) = database().await?;
                let calls = Arc::new(AtomicUsize::new(0));
                let observed = calls.clone();
                let ctx = context(Arc::new(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    match case {
                        0 => Ok(JobOutcome::Done),
                        1 => Ok(JobOutcome::Skip),
                        2 => Ok(JobOutcome::Noop),
                        3 => Ok(JobOutcome::Retry {
                            delay: Some(Duration::from_secs(30)),
                        }),
                        4 => Ok(JobOutcome::Retry { delay: None }),
                        5 => Ok(JobOutcome::DeadLetter),
                        6 => Ok(JobOutcome::Retry {
                            delay: Some(Duration::ZERO),
                        }),
                        7 => panic!("boom"),
                        _ => Err(SchedulerError::Job("ordinary handler error".to_owned())),
                    }
                }));
                enqueue(&backend, queue, "matrix", Uuid::new_v4(), Some(3)).await?;
                let workers = Workers::start(&backend, ctx, queue, 1).await?;
                let (expected, attempts) = match case {
                    0..=2 => ("Done", 1),
                    3..=4 => ("Pending", 1),
                    5..=7 => ("Killed", 1),
                    _ => ("Failed", 3),
                };
                let row = settled(&pool, "matrix", expected, attempts).await?;
                if expected == "Pending" {
                    row.assert_pending();
                }
                if expected == "Killed" {
                    row.assert_killed();
                }
                assert_eq!(calls.load(Ordering::SeqCst), usize::try_from(attempts)?);
                if case == 5 {
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(
                            row.last_result.as_deref().context("persisted result")?
                        )?,
                        serde_json::json!({"Ok": "DeadLetter"})
                    );
                }
                workers.stop().await?;
            }
        }
        for delay in [Some(Duration::from_secs(30)), None, Some(Duration::ZERO)] {
            let (pool, backend) = database().await?;
            enqueue(
                &backend,
                CACHE_KEEPALIVE_QUEUE,
                "single-attempt",
                Uuid::new_v4(),
                Some(1),
            )
            .await?;
            let workers = Workers::start(
                &backend,
                fixed(JobOutcome::Retry { delay }),
                CACHE_KEEPALIVE_QUEUE,
                1,
            )
            .await?;
            let row = settled(&pool, "single-attempt", "Killed", 1).await?;
            row.assert_killed();
            if delay == Some(Duration::ZERO) {
                assert!(
                    row.last_result
                        .as_deref()
                        .context("zero delay result")?
                        .contains("retry delay must be non-zero")
                );
            } else {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(
                        row.last_result
                            .as_deref()
                            .context("single attempt result")?
                    )?,
                    serde_json::json!({"Ok":"DeadLetter"})
                );
            }
            workers.stop().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_adaptive_worker_exhausts_all_five_backoff_retries_to_killed() -> Result<()> {
        let (pool, backend) = database().await?;
        enqueue(&backend, ADAPTIVE_QUEUE, "backoff", Uuid::new_v4(), None).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut before = db_now(&pool).await?;
        let workers = Workers::start(
            &backend,
            context(Arc::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(JobOutcome::Retry { delay: None })
            })),
            ADAPTIVE_QUEUE,
            1,
        )
        .await?;
        for (index, base) in [30_i64, 60, 120, 240, 480].into_iter().enumerate() {
            let row = settled(&pool, "backoff", "Pending", i64::try_from(index + 1)?).await?;
            row.assert_pending();
            let after = db_now(&pool).await?;
            assert!(
                row.run_at >= before + base * 9 / 10 - 2,
                "{row:?}, base={base}"
            );
            assert!(
                row.run_at <= after + base * 11 / 10 + 2,
                "{row:?}, base={base}"
            );
            before = db_now(&pool).await?;
            due(&pool, "backoff").await?;
        }
        settled(&pool, "backoff", "Killed", 6)
            .await?
            .assert_killed();
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        workers.stop().await
    }

    #[tokio::test]
    async fn sqlite_subsecond_retry_delay_rounds_up_and_settles_on_second_run() -> Result<()> {
        let (pool, backend) = database().await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let timestamps = Arc::new(Mutex::new((None::<Instant>, None::<Instant>)));
        enqueue(&backend, ADAPTIVE_QUEUE, "subsecond", Uuid::new_v4(), None).await?;
        let before = db_now(&pool).await?;
        let observed_calls = calls.clone();
        let observed_timestamps = timestamps.clone();
        let mut config = SchedulerConfig::default();
        config.recurring_jobs.clear();
        let ctx = SchedulerCtx::new(
            config,
            Arc::new(move |_| {
                let calls = observed_calls.clone();
                let timestamps = observed_timestamps.clone();
                Box::pin(async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        timeout(Duration::from_secs(2), async {
                            loop {
                                let millis = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map_err(|error| SchedulerError::Job(error.to_string()))?
                                    .subsec_millis();
                                if (750..=930).contains(&millis) {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                            Ok::<(), SchedulerError>(())
                        })
                        .await
                        .map_err(|error| SchedulerError::Job(error.to_string()))??;
                        timestamps.lock().expect("retry timestamp mutex").0 = Some(Instant::now());
                        Ok(JobOutcome::Retry {
                            delay: Some(Duration::from_millis(250)),
                        })
                    } else {
                        timestamps.lock().expect("retry timestamp mutex").1 = Some(Instant::now());
                        Ok(JobOutcome::Done)
                    }
                })
            }),
            Arc::new(|_| Box::pin(async { Ok(JobOutcome::Done) })),
            Arc::new(SystemClock),
        );
        let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 1).await?;
        let row = settled(&pool, "subsecond", "Done", 2).await?;
        assert!(
            row.run_at > before,
            "SQLite must round the delay up: {row:?}"
        );
        let (attempt1_returned, attempt2_started) =
            *timestamps.lock().expect("retry timestamp mutex");
        assert!(
            attempt2_started
                .context("attempt 2 timestamp")?
                .duration_since(attempt1_returned.context("attempt 1 timestamp")?)
                >= Duration::from_millis(250),
            "retry ran before its full delay: attempt1={attempt1_returned:?}, attempt2={attempt2_started:?}",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        workers.stop().await
    }

    #[tokio::test]
    async fn sqlite_transient_start_write_failure_recovers_lease_and_unrecoverable_escalates()
    -> Result<()> {
        let first_dir = tempfile::tempdir()?;
        let (worker_pool, fault_pool, backend) = open_pair(&first_dir).await?;
        sqlx::query("CREATE TABLE start_gate (recovered INTEGER)")
            .execute(&fault_pool)
            .await?;
        sqlx::query(
            "CREATE TRIGGER fail_first_start
             BEFORE UPDATE OF status ON Jobs
             WHEN NEW.status = 'Running'
               AND (SELECT COUNT(*) FROM start_gate) = 0
             BEGIN
                 SELECT RAISE(ABORT, 'transient start write error');
             END",
        )
        .execute(&fault_pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER mark_recovered
             AFTER UPDATE OF status ON Jobs
             WHEN OLD.status = 'Queued' AND NEW.status = 'Pending'
             BEGIN
                 INSERT INTO start_gate VALUES (1);
             END",
        )
        .execute(&fault_pool)
        .await?;
        let transient_calls = Arc::new(AtomicUsize::new(0));
        let observed_transient_calls = transient_calls.clone();
        enqueue(
            &backend,
            ADAPTIVE_QUEUE,
            "transient-start",
            Uuid::new_v4(),
            None,
        )
        .await?;
        let transient_worker = build_adaptive_worker(
            &backend,
            context(Arc::new(move || {
                observed_transient_calls.fetch_add(1, Ordering::SeqCst);
                Ok(JobOutcome::Done)
            })),
        )?;
        let transient_cancel = tokio_util::sync::CancellationToken::new();
        let transient_handle =
            tokio::spawn(transient_worker.run_until_cancelled(transient_cancel.clone()));
        let transient_row = settled(&worker_pool, "transient-start", "Done", 1).await?;
        assert!(transient_row.done_at.is_some());
        assert_eq!(transient_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM start_gate")
                .fetch_one(&fault_pool)
                .await?,
            1
        );
        transient_cancel.cancel();
        let transient_result = timeout(DEADLINE, transient_handle)
            .await
            .context("transient worker shutdown")??;
        assert!(transient_result.is_ok());

        let second_dir = tempfile::tempdir()?;
        let (_worker_pool, fault_pool, backend) = open_pair(&second_dir).await?;
        sqlx::query("CREATE TABLE start_gate (recovered INTEGER)")
            .execute(&fault_pool)
            .await?;
        sqlx::query(
            "CREATE TRIGGER fail_every_start
             BEFORE UPDATE OF status ON Jobs
             WHEN NEW.status = 'Running'
             BEGIN
                 SELECT RAISE(ABORT, 'unrecoverable start write error');
             END",
        )
        .execute(&fault_pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER fail_every_release
             BEFORE UPDATE OF status ON Jobs
             WHEN OLD.status = 'Queued' AND NEW.status = 'Pending'
             BEGIN
                 SELECT RAISE(ABORT, 'unrecoverable release write error');
             END",
        )
        .execute(&fault_pool)
        .await?;
        let unrecoverable_calls = Arc::new(AtomicUsize::new(0));
        let observed_unrecoverable_calls = unrecoverable_calls.clone();
        enqueue(
            &backend,
            ADAPTIVE_QUEUE,
            "unrecoverable-start",
            Uuid::new_v4(),
            None,
        )
        .await?;
        let unrecoverable_worker = build_adaptive_worker(
            &backend,
            context(Arc::new(move || {
                observed_unrecoverable_calls.fetch_add(1, Ordering::SeqCst);
                Ok(JobOutcome::Done)
            })),
        )?;
        let unrecoverable_cancel = tokio_util::sync::CancellationToken::new();
        let unrecoverable_handle =
            tokio::spawn(unrecoverable_worker.run_until_cancelled(unrecoverable_cancel));
        let unrecoverable_result = timeout(DEADLINE, unrecoverable_handle).await??;
        assert!(
            unrecoverable_result.is_err(),
            "unrecoverable start persistence must escalate to worker supervision"
        );
        assert_eq!(unrecoverable_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_fatal_recovery_quiesces_admission_while_active_handler_drains() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (pool, fault_pool, backend) = open_pair(&dir).await?;
        sqlx::query("CREATE TABLE started_once (n INTEGER)")
            .execute(&fault_pool)
            .await?;
        sqlx::query(
            "CREATE TRIGGER reject_later_starts BEFORE UPDATE OF status ON Jobs
             WHEN NEW.status = 'Running' AND (SELECT COUNT(*) FROM started_once) >= 1
             BEGIN SELECT RAISE(ABORT, 'fatal start failure'); END",
        )
        .execute(&fault_pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER record_first_start AFTER UPDATE OF status ON Jobs
             WHEN NEW.status = 'Running'
             BEGIN INSERT INTO started_once VALUES (1); END",
        )
        .execute(&fault_pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER reject_recovery BEFORE UPDATE OF status ON Jobs
             WHEN OLD.status = 'Queued' AND NEW.status = 'Pending'
               AND OLD.idempotency_key = 'fatal-drain-1'
             BEGIN SELECT RAISE(ABORT, 'fatal recovery failure'); END",
        )
        .execute(&fault_pool)
        .await?;
        let mut keys = HashMap::new();
        for index in 0..8 {
            let upstream = Uuid::new_v4();
            let key = format!("fatal-drain-{index}");
            enqueue(&backend, ADAPTIVE_QUEUE, &key, upstream, None).await?;
            sqlx::query("UPDATE Jobs SET run_at = ?1 WHERE idempotency_key = ?2")
                .bind(i64::from(index))
                .bind(&key)
                .execute(&fault_pool)
                .await?;
            keys.insert(upstream, key);
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let mut ctx = fixed(JobOutcome::Done);
        ctx.config.entity_concurrency = 2;
        ctx.adaptive_dispatch = Arc::new(move |job| {
            let AdaptiveJob::Warmup(job) = job else {
                unreachable!("warmup batch")
            };
            let key = keys[&job.upstream_id].clone();
            let entered = entered.clone();
            let calls = observed_calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = entered.lock().expect("entered barrier mutex").take() {
                    let _ = tx.send(key);
                }
                // Keep the first handler in flight past all bounded recovery
                // attempts (15ms + 45ms), exercising the fatal drain path.
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(JobOutcome::Done)
            })
        });
        let worker = build_adaptive_worker(&backend, ctx)?;
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut handle = tokio::spawn(worker.run_until_cancelled(cancel.clone()));
        let first_key = timeout(DEADLINE, entered_rx).await??;
        let joined = timeout(Duration::from_secs(5), &mut handle).await;
        if joined.is_err() {
            handle.abort();
        }
        let result = joined.context("fatal drain must not hang behind buffered jobs")??;
        assert!(result.is_err(), "fatal recovery must reach supervision");
        assert!(
            !cancel.is_cancelled(),
            "fatal drain must not cancel its caller"
        );
        let first = state(&pool, &first_key).await?;
        assert_eq!(first.status, "Done");
        assert_eq!(first.attempts, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Only task 2's recovery is faulted, so close-time cleanup must release
        // the untouched buffered leases without dispatching their handlers.
        for index in 2..8 {
            let key = format!("fatal-drain-{index}");
            let row = state(&pool, &key).await?;
            assert_eq!(row.status, "Pending", "{key}: {row:?}");
            assert!(row.lock_by.is_none(), "{key}: {row:?}");
            // Apalis poll_close reenqueue_abandoned bumps attempts to 1 on buffered rows,
            // while unfetched or pre-start released rows remain at 0; none ran handlers.
            assert!((0..=1).contains(&row.attempts), "{key}: {row:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_housekeeping_preserves_queued_batch_owned_by_live_worker() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (pool, fault_pool, backend) = open_pair(&dir).await?;
        sqlx::query("CREATE TABLE cache_keepalive_sessions (session_key_hash TEXT PRIMARY KEY, principal_id TEXT, upstream_id TEXT, status TEXT NOT NULL, enqueue_state TEXT NOT NULL, expires_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, terminal_reason TEXT, running_since_unix_secs INTEGER, error TEXT, encrypted_payload BLOB NOT NULL DEFAULT X'')")
            .execute(&fault_pool).await?;
        sqlx::query("CREATE TABLE cache_keepalive_evidence (evidence_id TEXT PRIMARY KEY, cycle_id TEXT, evidence_kind TEXT NOT NULL, request_start_ms INTEGER NOT NULL, prefix_hash TEXT, session_key_hash TEXT, prefix_policy_version TEXT, prefix_schema_version TEXT, principal_id TEXT NOT NULL, upstream_id TEXT NOT NULL, model TEXT NOT NULL, ttl TEXT NOT NULL, cache_read_input_tokens INTEGER)")
            .execute(&fault_pool).await?;
        sqlx::query("CREATE TABLE meta_v1 (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&fault_pool)
            .await?;
        // Distinct due timestamps make the first job deterministic without
        // relying on the random task IDs as the tie-breaker.
        for index in 0..3 {
            let key = format!("job-{index}");
            enqueue(&backend, ADAPTIVE_QUEUE, &key, Uuid::new_v4(), None).await?;
            sqlx::query("UPDATE Jobs SET run_at = ?1 WHERE idempotency_key = ?2")
                .bind(i64::from(index))
                .bind(&key)
                .execute(&fault_pool)
                .await?;
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let release = Arc::new(tokio::sync::Notify::new());
        let handler_release = release.clone();
        let mut ctx = fixed(JobOutcome::Done);
        ctx.config.entity_concurrency = 1;
        ctx.adaptive_dispatch = Arc::new(move |_| {
            let entered = entered.clone();
            let release = handler_release.clone();
            let first = observed_calls.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move {
                if first {
                    if let Some(tx) = entered.lock().expect("entered barrier mutex").take() {
                        let _ = tx.send(());
                    }
                    release.notified().await;
                }
                Ok(JobOutcome::Done)
            })
        });
        let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 1).await?;
        timeout(DEADLINE, entered_rx).await??;
        let (owner, last_seen) = timeout(DEADLINE, async {
            loop {
                let first = state(&fault_pool, "job-0").await?;
                let second = state(&fault_pool, "job-1").await?;
                let third = state(&fault_pool, "job-2").await?;
                if first.status == "Running"
                    && second.status == "Queued"
                    && third.status == "Queued"
                    && let Some(owner) = first.lock_by
                    && second.lock_by.as_deref() == Some(owner.as_str())
                    && third.lock_by.as_deref() == Some(owner.as_str())
                    && let Some(last_seen) =
                        sqlx::query_scalar::<_, i64>("SELECT last_seen FROM Workers WHERE id = ?1")
                            .bind(&owner)
                            .fetch_optional(&fault_pool)
                            .await?
                {
                    return Ok::<_, anyhow::Error>((owner, last_seen));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("waiting for live worker's prefetched batch")??;
        sqlx::query("UPDATE Jobs SET lock_at = ?1 WHERE idempotency_key IN ('job-1', 'job-2')")
            .bind(last_seen - 200)
            .execute(&fault_pool)
            .await?;
        let store = cc_lb_storage_sqlite::SqliteStorage::new(pool.clone(), Arc::new(SystemClock));
        let result =
            ApalisHousekeepingJobHandler::new(pool.clone(), ApalisHousekeepingConfig::new(30))
                .handle(
                    ApalisHousekeepingJob::default(),
                    u64::try_from(last_seen + 10)?,
                    &store,
                )
                .await;
        assert!(
            matches!(
                result,
                ApalisHousekeepingJobResult::Done {
                    stale_locks_reaped: 0,
                    ..
                }
            ),
            "{result:?}"
        );
        for key in ["job-1", "job-2"] {
            let row = state(&fault_pool, key).await?;
            assert_eq!(row.status, "Queued", "{key}: {row:?}");
            assert_eq!(row.attempts, 0, "{key}: {row:?}");
            assert_eq!(row.lock_by.as_deref(), Some(owner.as_str()));
        }
        release.notify_one();
        for key in ["job-0", "job-1", "job-2"] {
            settled(&pool, key, "Done", 1).await?;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        workers.stop().await
    }

    #[tokio::test]
    async fn sqlite_admin_surfaces_track_pending_retry_and_killed_dead_letter() -> Result<()> {
        let (pool, backend) = database().await?;
        let upstream_id = Uuid::new_v4();
        let warmup_key = format!("adaptive:warmup:{upstream_id}:1");
        enqueue(&backend, ADAPTIVE_QUEUE, &warmup_key, upstream_id, None).await?;
        enqueue(&backend, CRON_QUEUE, "admin-cron", upstream_id, None).await?;
        let admin = SchedulerAdminHandle::new(backend.clone());
        let retry = JobOutcome::Retry {
            delay: Some(Duration::from_secs(30)),
        };
        let adaptive = Workers::start(&backend, fixed(retry.clone()), ADAPTIVE_QUEUE, 1).await?;
        let cron = Workers::start(&backend, fixed(retry), CRON_QUEUE, 1).await?;
        let warmup = settled(&pool, &warmup_key, "Pending", 1).await?;
        let recurring = settled(&pool, "admin-cron", "Pending", 1).await?;
        assert!(admin.list_failures(None, 100, 0).await?.is_empty());
        assert_eq!(
            admin.next_run_for_upstream(upstream_id, "warmup").await?,
            Some(warmup.run_at)
        );
        let config = SchedulerConfig::default();
        let snapshot = admin.status(&config).await?;
        assert_eq!(
            snapshot
                .recurring_jobs
                .iter()
                .find(|job| job.name == "apalis_housekeeping")
                .context("housekeeping configured")?
                .next_run_at,
            Some(u64::try_from(recurring.run_at)?)
        );
        adaptive.stop().await?;
        cron.stop().await?;
        due(&pool, &warmup_key).await?;
        due(&pool, "admin-cron").await?;
        let adaptive =
            Workers::start(&backend, fixed(JobOutcome::DeadLetter), ADAPTIVE_QUEUE, 1).await?;
        let cron = Workers::start(&backend, fixed(JobOutcome::DeadLetter), CRON_QUEUE, 1).await?;
        settled(&pool, &warmup_key, "Killed", 2).await?;
        settled(&pool, "admin-cron", "Killed", 2).await?;
        adaptive.stop().await?;
        cron.stop().await?;
        enqueue(&backend, ADAPTIVE_QUEUE, "admin-zero", Uuid::new_v4(), None).await?;
        let workers = Workers::start(
            &backend,
            fixed(JobOutcome::Retry {
                delay: Some(Duration::ZERO),
            }),
            ADAPTIVE_QUEUE,
            1,
        )
        .await?;
        settled(&pool, "admin-zero", "Killed", 1).await?;
        workers.stop().await?;
        // Verify the admin's existing secret redaction on a terminal result.
        sqlx::query("UPDATE Jobs SET last_result = ?1 WHERE idempotency_key = 'admin-zero'")
            .bind(r#"{"Err":"Bearer secret-lifecycle-token"}"#)
            .execute(&pool)
            .await?;
        let failures = admin.list_failures(None, 100, 0).await?;
        assert_eq!(failures.len(), 3);
        assert!(
            failures
                .iter()
                .all(|failure| !failure.last_error.contains("secret-lifecycle-token"))
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure.last_error == "<redacted>")
        );
        assert_eq!(
            admin.next_run_for_upstream(upstream_id, "warmup").await?,
            None
        );
        assert_eq!(
            admin
                .status(&config)
                .await?
                .recurring_jobs
                .iter()
                .find(|job| job.name == "apalis_housekeeping")
                .context("housekeeping status")?
                .next_run_at,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_watchdog_skips_pending_retry_and_reseeds_killed() -> Result<()> {
        let (pool, backend) = database().await?;
        let upstream = Uuid::new_v4();
        let watchdog_key = format!("adaptive:warmup:{upstream}:1");
        enqueue(&backend, ADAPTIVE_QUEUE, &watchdog_key, upstream, None).await?;
        let workers = Workers::start(
            &backend,
            fixed(JobOutcome::Retry {
                delay: Some(Duration::from_secs(30)),
            }),
            ADAPTIVE_QUEUE,
            1,
        )
        .await?;
        settled(&pool, &watchdog_key, "Pending", 1).await?;
        workers.stop().await?;
        let tick = u64::try_from(db_now(&pool).await?)?;
        let active = run_entity_watchdog(
            &backend,
            WatchdogEntityKind::Warmup,
            &[upstream],
            tick,
            tick + 1,
        )
        .await?;
        assert_eq!(active.seeded, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
                .fetch_one(&pool)
                .await?,
            1
        );
        due(&pool, &watchdog_key).await?;
        let workers =
            Workers::start(&backend, fixed(JobOutcome::DeadLetter), ADAPTIVE_QUEUE, 1).await?;
        settled(&pool, &watchdog_key, "Killed", 2).await?;
        workers.stop().await?;
        let inactive = run_entity_watchdog(
            &backend,
            WatchdogEntityKind::Warmup,
            &[upstream],
            tick + 1,
            tick + 2,
        )
        .await?;
        assert_eq!(inactive.seeded, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
                .fetch_one(&pool)
                .await?,
            2
        );
        Ok(())
    }

    fn counter(rendered: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
        rendered
            .lines()
            .find(|line| {
                line.starts_with(&format!("{name}{{"))
                    && labels
                        .iter()
                        .all(|(key, value)| line.contains(&format!(r#"{key}="{value}""#)))
            })
            .and_then(|line| line.split_whitespace().last())
            .and_then(|value| value.parse().ok())
            .unwrap_or(0.0)
    }

    #[test]
    fn sqlite_metrics_distinguish_transient_retry_from_dead_letter_failure() -> Result<()> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                scheduler_metrics::touch_scheduler_metric_handles();
                let (pool, backend) = database().await?;
                enqueue(&backend, ADAPTIVE_QUEUE, "metrics", Uuid::new_v4(), None).await?;
                let workers = Workers::start(
                    &backend,
                    fixed(JobOutcome::Retry {
                        delay: Some(Duration::from_secs(30)),
                    }),
                    ADAPTIVE_QUEUE,
                    1,
                )
                .await?;
                settled(&pool, "metrics", "Pending", 1).await?;
                workers.stop().await?;
                let rendered = handle.render();
                let job = "upstream_warmup";
                for (status, expected) in [("started", 1.0), ("retry", 1.0), ("done", 0.0)] {
                    assert_eq!(
                        counter(
                            &rendered,
                            scheduler_metrics::JOBS_TOTAL,
                            &[("job_type", job), ("status", status)]
                        ),
                        expected,
                        "{rendered}"
                    );
                }
                assert_eq!(
                    counter(
                        &rendered,
                        scheduler_metrics::FAILURES_TOTAL,
                        &[("job_type", job)]
                    ),
                    0.0
                );
                due(&pool, "metrics").await?;
                let workers =
                    Workers::start(&backend, fixed(JobOutcome::DeadLetter), ADAPTIVE_QUEUE, 1)
                        .await?;
                settled(&pool, "metrics", "Killed", 2).await?;
                workers.stop().await?;
                assert_eq!(
                    counter(
                        &handle.render(),
                        scheduler_metrics::FAILURES_TOTAL,
                        &[("job_type", job)]
                    ),
                    1.0
                );
                enqueue(
                    &backend,
                    ADAPTIVE_QUEUE,
                    "metrics-zero",
                    Uuid::new_v4(),
                    None,
                )
                .await?;
                let workers = Workers::start(
                    &backend,
                    fixed(JobOutcome::Retry {
                        delay: Some(Duration::ZERO),
                    }),
                    ADAPTIVE_QUEUE,
                    1,
                )
                .await?;
                settled(&pool, "metrics-zero", "Killed", 1).await?;
                workers.stop().await?;
                let rendered = handle.render();
                assert_eq!(
                    counter(
                        &rendered,
                        scheduler_metrics::JOBS_TOTAL,
                        &[("job_type", job), ("status", "panicked")]
                    ),
                    1.0
                );
                for line in rendered
                    .lines()
                    .filter(|line| line.starts_with(scheduler_metrics::JOBS_TOTAL))
                {
                    if let Some((_, label)) = line.split_once("status=\"") {
                        let status = label.split('"').next().context("status label")?;
                        assert!(
                            ["started", "done", "retry", "skip", "panicked", "noop"]
                                .contains(&status),
                            "{line}"
                        );
                    }
                }
                Ok(())
            })
        })
    }

    #[tokio::test]
    async fn sqlite_housekeeping_retains_killed_for_dlq_window_and_preserves_pending_retry()
    -> Result<()> {
        let (pool, backend) = database().await?;
        sqlx::query("CREATE TABLE cache_keepalive_sessions (session_key_hash TEXT PRIMARY KEY, principal_id TEXT, upstream_id TEXT, status TEXT NOT NULL, enqueue_state TEXT NOT NULL, expires_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, terminal_reason TEXT, running_since_unix_secs INTEGER, error TEXT, encrypted_payload BLOB NOT NULL DEFAULT X'')").execute(&pool).await?;
        sqlx::query("CREATE TABLE cache_keepalive_evidence (evidence_id TEXT PRIMARY KEY, cycle_id TEXT, evidence_kind TEXT NOT NULL, request_start_ms INTEGER NOT NULL, prefix_hash TEXT, session_key_hash TEXT, prefix_policy_version TEXT, prefix_schema_version TEXT, principal_id TEXT NOT NULL, upstream_id TEXT NOT NULL, model TEXT NOT NULL, ttl TEXT NOT NULL, cache_read_input_tokens INTEGER)").execute(&pool).await?;
        sqlx::query("CREATE TABLE meta_v1 (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&pool)
            .await?;
        enqueue(
            &backend,
            ADAPTIVE_QUEUE,
            "retention-killed",
            Uuid::new_v4(),
            None,
        )
        .await?;
        let workers =
            Workers::start(&backend, fixed(JobOutcome::DeadLetter), ADAPTIVE_QUEUE, 1).await?;
        let killed = settled(&pool, "retention-killed", "Killed", 1).await?;
        workers.stop().await?;
        enqueue(
            &backend,
            CACHE_KEEPALIVE_QUEUE,
            "cache_keepalive:retention-pending",
            Uuid::new_v4(),
            None,
        )
        .await?;
        let workers = Workers::start(
            &backend,
            fixed(JobOutcome::Retry {
                delay: Some(Duration::from_secs(30)),
            }),
            CACHE_KEEPALIVE_QUEUE,
            1,
        )
        .await?;
        settled(&pool, "cache_keepalive:retention-pending", "Pending", 1).await?;
        workers.stop().await?;
        enqueue(
            &backend,
            ADAPTIVE_QUEUE,
            "retention-running",
            Uuid::new_v4(),
            None,
        )
        .await?;
        let (entered_tx, entered_rx) = oneshot::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let mut ctx = fixed(JobOutcome::Done);
        ctx.adaptive_dispatch = Arc::new(move |_| {
            let entered = entered_tx.clone();
            let release = release_rx.clone();
            Box::pin(async move {
                if let Some(tx) = entered.lock().expect("barrier mutex").take() {
                    let _ = tx.send(());
                }
                if let Some(rx) = release.lock().await.take() {
                    let _ = rx.await;
                }
                Ok(JobOutcome::Done)
            })
        });
        let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 1).await?;
        timeout(DEADLINE, entered_rx).await??;
        let running = state(&pool, "retention-running").await?;
        assert_eq!(running.status, "Running");
        assert!(running.lock_at.is_some());
        let done_at = u64::try_from(killed.done_at.context("dead letter completion time")?)?;
        workers.cancel.cancel();
        // Keep the reaped row out of the live consumer's claim window while
        // checking housekeeping at simulated future clocks.
        sqlx::query("UPDATE Jobs SET run_at = ?1 WHERE idempotency_key = 'retention-running'")
            .bind(i64::try_from(done_at + 9 * 86_400)?)
            .execute(&pool)
            .await?;
        let store = cc_lb_storage_sqlite::SqliteStorage::new(pool.clone(), Arc::new(SystemClock));
        let handler =
            ApalisHousekeepingJobHandler::new(pool.clone(), ApalisHousekeepingConfig::new(7));
        for days in [2_u64, 8] {
            let now = done_at + days * 86_400;
            // Keep this retry fresh relative to each simulated housekeeping clock.
            sqlx::query("UPDATE Jobs SET run_at = ?1 WHERE idempotency_key = 'cache_keepalive:retention-pending'")
                .bind(i64::try_from(now + 30)?).execute(&pool).await?;
            let result = handler
                .handle(ApalisHousekeepingJob::default(), now, &store)
                .await;
            assert!(
                matches!(result, ApalisHousekeepingJobResult::Done { .. }),
                "{result:?}"
            );
            if days == 2 {
                assert!(matches!(
                    result,
                    ApalisHousekeepingJobResult::Done {
                        stale_locks_reaped: 1,
                        ..
                    }
                ));
                state(&pool, "retention-killed").await?.assert_killed();
                let reaped = state(&pool, "retention-running").await?;
                reaped.assert_pending();
                assert_eq!(reaped.attempts, running.attempts + 1);
            } else {
                assert_eq!(
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = 'retention-killed'"
                    )
                    .fetch_one(&pool)
                    .await?,
                    0
                );
            }
            state(&pool, "cache_keepalive:retention-pending")
                .await?
                .assert_pending();
        }
        release_tx.send(()).expect("release the reaped handler");
        workers.stop().await?;
        state(&pool, "retention-running").await?.assert_pending();
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_graceful_shutdown_completes_inflight_persist_write() -> Result<()> {
        for outcome in [
            JobOutcome::Done,
            JobOutcome::Retry {
                delay: Some(Duration::from_secs(30)),
            },
        ] {
            let (pool, backend) = database().await?;
            enqueue(&backend, ADAPTIVE_QUEUE, "shutdown", Uuid::new_v4(), None).await?;
            let (entered_tx, entered_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            let entered = Arc::new(Mutex::new(Some(entered_tx)));
            let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
            let expected = if matches!(outcome, JobOutcome::Done) {
                "Done"
            } else {
                "Pending"
            };
            let mut ctx = fixed(JobOutcome::Done);
            ctx.adaptive_dispatch = Arc::new(move |_| {
                let entered = entered.clone();
                let release = release.clone();
                let outcome = outcome.clone();
                Box::pin(async move {
                    if let Some(tx) = entered.lock().expect("barrier mutex").take() {
                        let _ = tx.send(());
                    }
                    if let Some(rx) = release.lock().await.take() {
                        rx.await.expect("release in-flight handler");
                    }
                    Ok(outcome)
                })
            });
            let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 1).await?;
            timeout(DEADLINE, entered_rx).await??;
            workers.cancel.cancel();
            release_tx.send(()).expect("handler still waiting");
            workers.stop().await?;
            let row = state(&pool, "shutdown").await?;
            assert_eq!(row.status, expected);
            assert_eq!(row.attempts, 1);
            if expected == "Pending" {
                row.assert_pending();
            } else {
                assert!(row.done_at.is_some());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_single_conn_and_dual_worker_concurrent_retries() -> Result<()> {
        // First exercise a mixed batch with the configured concurrency of eight.
        let (pool, backend) = database().await?;
        let outcomes: Arc<Mutex<HashMap<Uuid, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        for index in 0..16 {
            let upstream = Uuid::new_v4();
            outcomes
                .lock()
                .expect("outcomes mutex")
                .insert(upstream, index);
            enqueue(
                &backend,
                ADAPTIVE_QUEUE,
                &format!("mixed-{index}"),
                upstream,
                None,
            )
            .await?;
        }
        let mut ctx = fixed(JobOutcome::Done);
        ctx.config.entity_concurrency = 8;
        ctx.adaptive_dispatch = Arc::new(move |job| {
            let AdaptiveJob::Warmup(job) = job else {
                unreachable!("warmup batch")
            };
            let index = outcomes.lock().expect("outcomes mutex")[&job.upstream_id];
            Box::pin(async move {
                Ok(match index % 4 {
                    0 => JobOutcome::Done,
                    1 => JobOutcome::Skip,
                    2 => JobOutcome::Noop,
                    _ => JobOutcome::DeadLetter,
                })
            })
        });
        let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 1).await?;
        for index in 0..16 {
            settled(
                &pool,
                &format!("mixed-{index}"),
                if index % 4 == 3 { "Killed" } else { "Done" },
                1,
            )
            .await?;
        }
        workers.stop().await?;

        let (pool, backend) = database().await?;
        let mut counts = HashMap::<Uuid, AtomicUsize>::new();
        for index in 0..16 {
            let upstream = Uuid::new_v4();
            counts.insert(upstream, AtomicUsize::new(0));
            enqueue(
                &backend,
                ADAPTIVE_QUEUE,
                &format!("concurrent-{index}"),
                upstream,
                None,
            )
            .await?;
        }
        let counts = Arc::new(counts);
        let observed = counts.clone();
        let mut ctx = fixed(JobOutcome::Done);
        ctx.adaptive_dispatch = Arc::new(move |job| {
            let AdaptiveJob::Warmup(job) = job else {
                unreachable!("warmup batch")
            };
            let first = observed[&job.upstream_id].fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move {
                Ok(if first {
                    JobOutcome::Retry {
                        delay: Some(Duration::from_secs(1)),
                    }
                } else {
                    JobOutcome::Done
                })
            })
        });
        let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 2).await?;
        timeout(DEADLINE, async {
            for index in 0..16 {
                settled(&pool, &format!("concurrent-{index}"), "Done", 2).await?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        assert!(
            counts
                .values()
                .all(|count| count.load(Ordering::SeqCst) == 2)
        );
        workers.stop().await
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use std::collections::HashMap;
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use anyhow::{Context as _, Result, ensure};
    use cc_lb_scheduler::error::SchedulerError;
    use cc_lb_scheduler::retry::JobOutcome;
    use cc_lb_scheduler::worker::{
        ADAPTIVE_QUEUE, AdaptiveJob, PostgresSchedulerBackend, SchedulerBackend,
    };
    use futures_util::FutureExt as _;
    use sqlx::Row as _;
    use sqlx::postgres::PgPoolOptions;
    use tokio::time::timeout;
    use url::Url;
    use uuid::Uuid;

    use super::support::*;

    // Match the restart-lifetime fixture: require a PG18 administrative URL,
    // isolate every test, and DROP even when the body panics or times out.
    async fn with_isolated_database<T, F, Fut>(test: F) -> Result<T>
    where
        F: FnOnce(sqlx::PgPool) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let admin_url = std::env::var("DATABASE_URL")
            .context("DATABASE_URL must be set to a PostgreSQL administrative database")?;
        ensure!(
            !admin_url.trim().is_empty(),
            "DATABASE_URL must not be empty"
        );
        let database_name = format!("cclb_outcome_{}", Uuid::new_v4().simple());
        let mut database_url = Url::parse(&admin_url).context("parse DATABASE_URL")?;
        database_url.set_path(&database_name);
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .context("connect to PostgreSQL administrative database")?;
        let server_version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&admin_pool)
            .await?;
        ensure!(
            server_version.starts_with("18."),
            "outcome lifecycle tests require PostgreSQL 18, reached {server_version}"
        );
        let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
        sqlx::query(sqlx::AssertSqlSafe(create_database))
            .execute(&admin_pool)
            .await?;
        let test_result = timeout(
            Duration::from_secs(45),
            AssertUnwindSafe(async {
                let pool = PgPoolOptions::new()
                    .max_connections(12)
                    .connect(database_url.as_str())
                    .await?;
                apalis_postgres::PostgresStorage::setup(&pool).await?;
                cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
                let result = test(pool.clone()).await;
                pool.close().await;
                result
            })
            .catch_unwind(),
        )
        .await;
        let test_result = match test_result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow::anyhow!("isolated database test panicked")),
            Err(elapsed) => {
                Err(anyhow::Error::new(elapsed).context("isolated database test timed out"))
            }
        };
        let drop_database = format!(r#"DROP DATABASE "{database_name}" WITH (FORCE)"#);
        let drop_result = sqlx::query(sqlx::AssertSqlSafe(drop_database))
            .execute(&admin_pool)
            .await;
        admin_pool.close().await;
        match (test_result, drop_result) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(error), Ok(_)) => Err(error.context(format!(
                "isolated database {database_name} was dropped after test failure"
            ))),
            (Ok(_), Err(error)) => {
                Err(error).with_context(|| format!("drop isolated database {database_name}"))
            }
            (Err(error), Err(drop_error)) => Err(anyhow::anyhow!(
                "isolated database {database_name} test failed: {error:#}; drop also failed: {drop_error}"
            )),
        }
    }

    fn backend(pool: &sqlx::PgPool) -> SchedulerBackend {
        SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool.clone()))
    }

    async fn state(pool: &sqlx::PgPool, key: &str) -> Result<State> {
        let row = sqlx::query("SELECT status, attempts::BIGINT AS attempts, EXTRACT(EPOCH FROM run_at)::BIGINT AS run_at, lock_by, EXTRACT(EPOCH FROM lock_at)::BIGINT AS lock_at, EXTRACT(EPOCH FROM done_at)::BIGINT AS done_at, last_result::TEXT AS last_result FROM apalis.jobs WHERE idempotency_key = $1")
            .bind(key).fetch_one(pool).await?;
        Ok(State {
            status: row.try_get("status")?,
            attempts: row.try_get("attempts")?,
            run_at: row.try_get("run_at")?,
            lock_by: row.try_get("lock_by")?,
            lock_at: row.try_get("lock_at")?,
            done_at: row.try_get("done_at")?,
            last_result: row.try_get("last_result")?,
        })
    }

    async fn settled(pool: &sqlx::PgPool, key: &str, status: &str, attempts: i64) -> Result<State> {
        timeout(DEADLINE, async {
            loop {
                let row = state(pool, key).await?;
                if row.status == status && row.attempts == attempts {
                    return Ok(row);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("waiting for {key}: {status}, attempts={attempts}"))?
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_worker_outcome_state_matrix_core() -> Result<()> {
        with_isolated_database(|pool| async move {
            let backend = backend(&pool);
            let mut cases = HashMap::<Uuid, usize>::new();
            for case in 0..5 {
                let upstream = Uuid::new_v4();
                cases.insert(upstream, case);
                enqueue(
                    &backend,
                    ADAPTIVE_QUEUE,
                    &format!("postgres-matrix-{case}"),
                    upstream,
                    Some(3),
                )
                .await?;
            }
            let mut ctx = fixed(JobOutcome::Done);
            ctx.adaptive_dispatch = Arc::new(move |job| {
                let AdaptiveJob::Warmup(job) = job else {
                    unreachable!("warmup matrix")
                };
                let case = cases[&job.upstream_id];
                Box::pin(async move {
                    match case {
                        0 => Ok(JobOutcome::Done),
                        1 => Ok(JobOutcome::Retry {
                            delay: Some(Duration::from_secs(30)),
                        }),
                        2 => Ok(JobOutcome::Retry {
                            delay: Some(Duration::ZERO),
                        }),
                        3 => panic!("boom"),
                        _ => Err(SchedulerError::Job("ordinary handler error".to_owned())),
                    }
                })
            });
            let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 1).await?;
            let states = futures_util::future::try_join_all((0..5).map(|case| {
                let pool = &pool;
                async move {
                    let (expected, attempts) = match case {
                        0 => ("Done", 1),
                        1 => ("Pending", 1),
                        2..=3 => ("Killed", 1),
                        _ => ("Failed", 3),
                    };
                    settled(pool, &format!("postgres-matrix-{case}"), expected, attempts).await
                }
            }))
            .await?;
            states[1].assert_pending();
            states[2].assert_killed();
            states[3].assert_killed();
            workers.stop().await?;

            sqlx::query(
                "UPDATE apalis.jobs SET run_at = now() - interval '1 second' WHERE idempotency_key = $1",
            )
            .bind("postgres-matrix-1")
            .execute(&pool)
            .await?;
            let workers =
                Workers::start(&backend, fixed(JobOutcome::DeadLetter), ADAPTIVE_QUEUE, 1).await?;
            let row = settled(&pool, "postgres-matrix-1", "Killed", 2).await?;
            row.assert_killed();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(
                    row.last_result.as_deref().context("persisted dead letter")?,
                )?,
                serde_json::json!({"Ok": "DeadLetter"}),
            );
            workers.stop().await
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_rescheduled_pending_retry_is_claimed_when_due() -> Result<()> {
        with_isolated_database(|pool| async move {
            let backend = backend(&pool);
            enqueue(
                &backend,
                ADAPTIVE_QUEUE,
                "postgres-subsecond",
                Uuid::new_v4(),
                None,
            )
            .await?;
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let workers = Workers::start(
                &backend,
                context(Arc::new(move || {
                    Ok(if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                        JobOutcome::Retry {
                            delay: Some(Duration::from_millis(200)),
                        }
                    } else {
                        JobOutcome::Done
                    })
                })),
                ADAPTIVE_QUEUE,
                1,
            )
            .await?;
            let row = settled(&pool, "postgres-subsecond", "Done", 2).await?;
            assert_eq!(row.attempts, 2);
            assert!(row.run_at > 0);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            workers.stop().await
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_dual_worker_concurrent_retries() -> Result<()> {
        with_isolated_database(|pool| async move {
            let backend = backend(&pool);
            let mut counts = HashMap::<Uuid, AtomicUsize>::new();
            for index in 0..16 {
                let upstream = Uuid::new_v4();
                counts.insert(upstream, AtomicUsize::new(0));
                enqueue(
                    &backend,
                    ADAPTIVE_QUEUE,
                    &format!("postgres-concurrent-{index}"),
                    upstream,
                    None,
                )
                .await?;
            }
            let counts = Arc::new(counts);
            let observed = counts.clone();
            let mut ctx = fixed(JobOutcome::Done);
            ctx.adaptive_dispatch = Arc::new(move |job| {
                let AdaptiveJob::Warmup(job) = job else {
                    unreachable!("warmup batch")
                };
                let first = observed[&job.upstream_id].fetch_add(1, Ordering::SeqCst) == 0;
                Box::pin(async move {
                    Ok(if first {
                        JobOutcome::Retry {
                            delay: Some(Duration::from_millis(500)),
                        }
                    } else {
                        JobOutcome::Done
                    })
                })
            });
            let workers = Workers::start(&backend, ctx, ADAPTIVE_QUEUE, 2).await?;
            timeout(DEADLINE, async {
                for index in 0..16 {
                    settled(&pool, &format!("postgres-concurrent-{index}"), "Done", 2).await?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            assert!(
                counts
                    .values()
                    .all(|count| count.load(Ordering::SeqCst) == 2)
            );
            workers.stop().await
        })
        .await
    }
}
