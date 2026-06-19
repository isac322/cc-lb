#[cfg(not(feature = "postgres"))]
#[tokio::test]
async fn scheduler_cron_leader_handover_requires_postgres_feature() {
    eprintln!("SKIP: scheduler cron leader handover scenarios require --features postgres");
}

#[cfg(feature = "postgres")]
mod postgres {
    use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

    use apalis::{
        layers::catch_panic::CatchPanicLayer,
        prelude::{
            Data, IntervalStrategy, StrategyBuilder, TaskSink,
            WorkerBuilder as ApalisWorkerBuilder, WorkerError,
        },
    };
    use cc_lb_scheduler::{
        cron::WorkerBuilder as CronWorkerBuilder,
        leader_election::{LeaderElection, LeaderState},
        retry::JobOutcome,
        worker::{ENTITY_QUEUE, EntityJob, PostgresApalisStorage},
    };
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use serde::{Deserialize, Serialize};
    use sqlx::{PgPool, postgres::PgPoolOptions};
    use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
    use tokio::{
        sync::{Mutex, Notify},
        task::JoinHandle,
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    const LOCK_KEY: i64 = 0x0000_CC1B_5CDE_0001_u64 as i64;
    const CRON_QUEUE: &str = "task_41_cron_handover";
    const TICK_INTERVAL: Duration = Duration::from_millis(500);
    const POLL_INTERVAL: Duration = Duration::from_millis(200);
    const WAIT_TIMEOUT: Duration = Duration::from_secs(8);

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct CronTickJob {
        replica: String,
    }

    #[derive(Clone)]
    struct EntityState {
        replica: &'static str,
        seen: Arc<Mutex<Vec<String>>>,
        notify: Arc<Notify>,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_to_follower_handover_after_dedicated_connection_drop() -> TestResult {
        with_postgres_container(|url| async move {
            let pool = open_pool(&url).await?;
            setup_schema(&pool).await?;
            let leader_a = Arc::new(LeaderElection::postgres(url.clone(), LOCK_KEY).await?);
            let leader_b = Arc::new(LeaderElection::postgres(url, LOCK_KEY).await?);

            assert!(leader_a.try_acquire().await?);
            assert_eq!(leader_a.current_state(), LeaderState::Leader);
            assert!(!leader_b.try_acquire().await?);
            assert_eq!(leader_b.current_state(), LeaderState::Follower);

            let cancel_a = CancellationToken::new();
            let cancel_b = CancellationToken::new();
            let cron_a = spawn_cron_loop("A", pool.clone(), leader_a.clone(), cancel_a.clone());
            let cron_b = spawn_cron_loop("B", pool.clone(), leader_b.clone(), cancel_b.clone());

            wait_for_replica_ticks(&pool, "A", 2).await?;
            tokio::time::sleep(TICK_INTERVAL + Duration::from_millis(250)).await;
            assert_eq!(count_replica_ticks(&pool, "B").await?, 0);

            let drop_started = Utc::now();
            leader_a.close().await?;
            cancel_a.cancel();
            cron_a.await?;

            wait_for_replica_ticks(&pool, "B", 2).await?;
            tokio::time::sleep(TICK_INTERVAL + Duration::from_millis(250)).await;
            let a_after_drop = count_replica_ticks_after(&pool, "A", drop_started).await?;
            assert_eq!(
                a_after_drop, 0,
                "replica A ticked after its leader connection dropped"
            );

            let ticks = cron_ticks(&pool).await?;
            assert_tick_handover_gap(&ticks)?;
            eprintln!(
                "handover ticks: {:?}",
                ticks
                    .iter()
                    .map(|tick| (&tick.replica, tick.ticked_at))
                    .collect::<Vec<_>>()
            );

            cancel_b.cancel();
            cron_b.abort();
            let _ = cron_b.await;
            pool.close().await;
            Ok(())
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn follower_replicas_boot_and_process_entity_jobs_without_leader_lock() -> TestResult {
        with_postgres_container(|url| async move {
            let pool = open_pool(&url).await?;
            setup_schema(&pool).await?;
            let leader_b = Arc::new(LeaderElection::postgres(url.clone(), LOCK_KEY).await?);
            let leader_c = Arc::new(LeaderElection::postgres(url, LOCK_KEY).await?);

            let acquired_b = leader_b.try_acquire().await?;
            let acquired_c = leader_c.try_acquire().await?;
            assert_ne!(acquired_b, acquired_c, "exactly one replica must acquire the advisory lock");
            assert!(matches!(leader_b.current_state(), LeaderState::Leader | LeaderState::Follower));
            assert!(matches!(leader_c.current_state(), LeaderState::Leader | LeaderState::Follower));

            let cancel_cron = CancellationToken::new();
            let cron_b = spawn_cron_loop("B", pool.clone(), leader_b, cancel_cron.clone());
            let cron_c = spawn_cron_loop("C", pool.clone(), leader_c, cancel_cron.clone());
            let entity_cancel = CancellationToken::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let notify = Arc::new(Notify::new());
            let worker_b = spawn_entity_worker(&pool, "B", seen.clone(), notify.clone(), entity_cancel.clone());
            let worker_c = spawn_entity_worker(&pool, "C", seen.clone(), notify.clone(), entity_cancel.clone());

            let mut storage = PostgresApalisStorage::new_with_config(&pool, &entity_queue_config());
            for cycle_key in 0..12_u64 {
                storage.push(EntityJob::Warmup(cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob::new(Uuid::new_v4(), cycle_key))).await?;
            }

            wait_for_entity_replicas(&seen, &notify).await?;
            wait_for_done_entity_jobs(&pool, 2).await?;
            wait_for_any_cron_tick(&pool).await?;
            let cron_replicas = cron_tick_replicas(&pool).await?;
            assert_eq!(cron_replicas.len(), 1, "cron ticks must be produced by exactly one leader replica: {cron_replicas:?}");
            eprintln!("follower boot: acquired_b={acquired_b}, acquired_c={acquired_c}, entity_seen={:?}, cron_replicas={cron_replicas:?}", seen.lock().await);

            entity_cancel.cancel();
            worker_b.await??;
            worker_c.await??;
            cancel_cron.cancel();
            cron_b.abort();
            cron_c.abort();
            let _ = cron_b.await;
            let _ = cron_c.await;
            pool.close().await;
            Ok(())
        }).await
    }

    fn spawn_cron_loop(
        replica: &'static str,
        pool: PgPool,
        leader: Arc<LeaderElection>,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                let schedule = FastSchedule::new();
                let storage = apalis_postgres::PostgresStorage::<CronTickJob>::new_with_config(
                    &pool,
                    &apalis_postgres::Config::new(CRON_QUEUE),
                );
                let worker = CronWorkerBuilder::singleton_queue(
                    CRON_QUEUE,
                    schedule,
                    storage,
                    CronTickJob {
                        replica: replica.to_owned(),
                    },
                );
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = worker.run(leader.as_ref()) => if result.is_err() { tokio::time::sleep(POLL_INTERVAL).await; },
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
    }

    #[derive(Clone, Debug)]
    struct FastSchedule {
        next: Option<DateTime<Utc>>,
    }

    impl FastSchedule {
        const fn new() -> Self {
            Self { next: None }
        }
    }

    impl apalis_cron::Schedule<Utc> for FastSchedule {
        fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
            let interval = ChronoDuration::from_std(TICK_INTERVAL).ok()?;
            let next = self.next.unwrap_or_else(|| Utc::now() + interval);
            self.next = Some(next + interval);
            Some(next)
        }
    }

    fn spawn_entity_worker(
        pool: &PgPool,
        replica: &'static str,
        seen: Arc<Mutex<Vec<String>>>,
        notify: Arc<Notify>,
        cancel: CancellationToken,
    ) -> JoinHandle<Result<(), WorkerError>> {
        let storage = PostgresApalisStorage::new_with_config(pool, &entity_queue_config());
        tokio::spawn(async move {
            ApalisWorkerBuilder::new(ENTITY_QUEUE)
                .backend(storage)
                .data(EntityState {
                    replica,
                    seen,
                    notify,
                })
                .layer(CatchPanicLayer::new())
                .build(entity_handler)
                .run_until(async move {
                    cancel.cancelled().await;
                    Ok::<(), WorkerError>(())
                })
                .await
        })
    }

    fn entity_handler(
        _job: EntityJob,
        ctx: Data<EntityState>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<JobOutcome, cc_lb_scheduler::error::SchedulerError>> + Send>,
    > {
        Box::pin(async move {
            ctx.seen.lock().await.push(ctx.replica.to_owned());
            ctx.notify.notify_waiters();
            Ok(JobOutcome::Done)
        })
    }

    fn entity_queue_config() -> apalis_postgres::Config {
        let poll_strategy = StrategyBuilder::new()
            .apply(IntervalStrategy::new(Duration::from_millis(10)))
            .build();
        apalis_postgres::Config::new(ENTITY_QUEUE)
            .with_poll_interval(poll_strategy)
            .set_buffer_size(8)
    }

    async fn open_pool(url: &str) -> TestResult<PgPool> {
        Ok(PgPoolOptions::new().max_connections(8).connect(url).await?)
    }

    async fn setup_schema(pool: &PgPool) -> TestResult {
        apalis_postgres::PostgresStorage::setup(pool).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(pool).await?;
        Ok(())
    }

    async fn with_postgres_container<F, Fut>(run: F) -> TestResult
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = TestResult>,
    {
        let docker_host = match std::env::var("DOCKER_HOST") {
            Ok(value) => value,
            Err(error) => {
                eprintln!(
                    "SKIP: DOCKER_HOST not set for scheduler cron leader handover test; expected DOCKER_HOST=tcp://localhost:2375 ({error})"
                );
                return Ok(());
            }
        };
        let container = match Postgres::default().start().await {
            Ok(container) => container,
            Err(error) => {
                eprintln!(
                    "SKIP: could not start postgres testcontainer using DOCKER_HOST={docker_host}: {error}"
                );
                return Ok(());
            }
        };
        let port = match container.get_host_port_ipv4(5432).await {
            Ok(port) => port,
            Err(error) => {
                eprintln!("SKIP: could not read postgres testcontainer port: {error}");
                return Ok(());
            }
        };
        run(format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
    }

    struct TickRow {
        replica: String,
        ticked_at: DateTime<Utc>,
    }

    async fn cron_ticks(pool: &PgPool) -> TestResult<Vec<TickRow>> {
        Ok(sqlx::query_as::<_, (String, DateTime<Utc>)>("SELECT convert_from(job, 'UTF8')::jsonb ->> 'replica', run_at FROM apalis.jobs WHERE job_type = $1 ORDER BY run_at").bind(CRON_QUEUE).fetch_all(pool).await?.into_iter().map(|(replica, ticked_at)| TickRow { replica, ticked_at }).collect())
    }

    async fn count_replica_ticks(pool: &PgPool, replica: &str) -> TestResult<i64> {
        Ok(sqlx::query_scalar("SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND convert_from(job, 'UTF8')::jsonb ->> 'replica' = $2").bind(CRON_QUEUE).bind(replica).fetch_one(pool).await?)
    }
    async fn count_replica_ticks_after(
        pool: &PgPool,
        replica: &str,
        after: DateTime<Utc>,
    ) -> TestResult<i64> {
        Ok(sqlx::query_scalar("SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND convert_from(job, 'UTF8')::jsonb ->> 'replica' = $2 AND run_at > $3").bind(CRON_QUEUE).bind(replica).bind(after).fetch_one(pool).await?)
    }

    async fn wait_for_replica_ticks(pool: &PgPool, replica: &str, expected: i64) -> TestResult {
        wait_until(
            || async {
                count_replica_ticks(pool, replica)
                    .await
                    .map(|count| count >= expected)
            },
            "replica cron ticks",
        )
        .await
    }
    async fn wait_for_any_cron_tick(pool: &PgPool) -> TestResult {
        wait_until(
            || async { Ok(!cron_tick_replicas(pool).await?.is_empty()) },
            "any cron tick",
        )
        .await
    }
    async fn wait_for_done_entity_jobs(pool: &PgPool, expected: i64) -> TestResult {
        wait_until(
            || async {
                Ok(sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND status = 'Done'",
                )
                .bind(ENTITY_QUEUE)
                .fetch_one(pool)
                .await?
                    >= expected)
            },
            "done entity jobs",
        )
        .await
    }

    async fn cron_tick_replicas(pool: &PgPool) -> TestResult<HashSet<String>> {
        Ok(cron_ticks(pool)
            .await?
            .into_iter()
            .map(|tick| tick.replica)
            .collect())
    }

    async fn wait_for_entity_replicas(
        seen: &Arc<Mutex<Vec<String>>>,
        notify: &Arc<Notify>,
    ) -> TestResult {
        let wait = async {
            while seen.lock().await.iter().collect::<HashSet<_>>().len() < 2 {
                notify.notified().await;
            }
        };
        tokio::time::timeout(WAIT_TIMEOUT, wait)
            .await
            .map_err(|_| "timed out waiting for both entity replicas to process jobs")?;
        Ok(())
    }

    async fn wait_until<F, Fut>(mut check: F, label: &str) -> TestResult
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = TestResult<bool>>,
    {
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            if check().await? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting for {label}").into());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    fn assert_tick_handover_gap(ticks: &[TickRow]) -> TestResult {
        let max_allowed =
            chrono::Duration::from_std(2 * TICK_INTERVAL + Duration::from_millis(500))?;
        for pair in ticks.windows(2) {
            let gap = pair[1].ticked_at.signed_duration_since(pair[0].ticked_at);
            assert!(
                gap <= max_allowed,
                "tick gap {gap:?} exceeded handover allowance {max_allowed:?}"
            );
        }
        assert!(ticks.iter().any(|tick| tick.replica == "A"));
        assert!(ticks.iter().any(|tick| tick.replica == "B"));
        Ok(())
    }
}
