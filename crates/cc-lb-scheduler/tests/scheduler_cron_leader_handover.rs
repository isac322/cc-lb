#[cfg(not(feature = "postgres"))]
#[tokio::test]
async fn scheduler_cron_leader_handover_requires_postgres_feature() {
    eprintln!("SKIP: scheduler cron leader handover scenarios require --features postgres");
}

#[cfg(feature = "postgres")]
#[path = "support/cron_handover.rs"]
mod cron_handover;

#[cfg(feature = "postgres")]
mod postgres {
    use std::{future::Future, sync::Arc, time::Duration};

    use apalis::{
        layers::catch_panic::CatchPanicLayer,
        prelude::{
            Data, IntervalStrategy, StrategyBuilder, TaskSink,
            WorkerBuilder as ApalisWorkerBuilder, WorkerError,
        },
    };
    use cc_lb_core::clock::{Clock, SystemClock};
    use cc_lb_scheduler::{
        leader_election::{LeaderElection, LeaderState},
        retry::JobOutcome,
        worker::{ADAPTIVE_QUEUE, AdaptiveJob},
    };
    use tokio::{
        sync::{Mutex, Notify},
        task::JoinHandle,
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::cron_handover::*;

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

            let drop_started = chrono::DateTime::<chrono::Utc>::from(SystemClock.now());
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
            assert_ne!(
                acquired_b, acquired_c,
                "exactly one replica must acquire the advisory lock"
            );
            assert!(matches!(
                leader_b.current_state(),
                LeaderState::Leader | LeaderState::Follower
            ));
            assert!(matches!(
                leader_c.current_state(),
                LeaderState::Leader | LeaderState::Follower
            ));

            let cancel_cron = CancellationToken::new();
            let cron_b = spawn_cron_loop("B", pool.clone(), leader_b, cancel_cron.clone());
            let cron_c = spawn_cron_loop("C", pool.clone(), leader_c, cancel_cron.clone());
            let entity_cancel = CancellationToken::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let notify = Arc::new(Notify::new());
            let worker_b = spawn_entity_worker(
                &pool,
                "B",
                seen.clone(),
                notify.clone(),
                entity_cancel.clone(),
            );
            let worker_c = spawn_entity_worker(
                &pool,
                "C",
                seen.clone(),
                notify.clone(),
                entity_cancel.clone(),
            );

            // Production replicas set up LISTEN once at boot, then jobs trickle
            // in over time. To mirror that ordering (and avoid losing NOTIFYs
            // emitted before both PgListener channels are connected), settle
            // worker setup before pushing, and space pushes so each NOTIFY
            // gives both workers a chance to win the row-level lock race.
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut storage = apalis_postgres::PostgresStorage::<AdaptiveJob>::new_with_notify(
                &pool,
                &entity_queue_config(),
            );
            for cycle_key in 0..12_u64 {
                storage
                    .push(AdaptiveJob::Warmup(
                        cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob::new(
                            Uuid::new_v4(),
                            cycle_key,
                        ),
                    ))
                    .await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            wait_for_entity_replicas(&seen, &notify).await?;
            wait_for_done_entity_jobs(&pool, 2).await?;
            wait_for_any_cron_tick(&pool).await?;
            let cron_replicas = cron_tick_replicas(&pool).await?;
            assert_eq!(
                cron_replicas.len(),
                1,
                "cron ticks must be produced by exactly one leader replica: {cron_replicas:?}"
            );
            eprintln!(
                "follower boot: acquired_b={acquired_b}, acquired_c={acquired_c}, entity_seen={:?}, cron_replicas={cron_replicas:?}",
                seen.lock().await
            );

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
        })
        .await
    }

    fn spawn_entity_worker(
        pool: &sqlx::PgPool,
        replica: &'static str,
        seen: Arc<Mutex<Vec<String>>>,
        notify: Arc<Notify>,
        cancel: CancellationToken,
    ) -> JoinHandle<Result<(), WorkerError>> {
        let storage = apalis_postgres::PostgresStorage::<AdaptiveJob>::new_with_notify(
            pool,
            &entity_queue_config(),
        );
        tokio::spawn(async move {
            ApalisWorkerBuilder::new(ADAPTIVE_QUEUE)
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
        _job: AdaptiveJob,
        ctx: Data<EntityState>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<JobOutcome, cc_lb_scheduler::error::SchedulerError>> + Send>,
    > {
        Box::pin(async move {
            ctx.seen.lock().await.push(ctx.replica.to_owned());
            ctx.notify.notify_waiters();
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(JobOutcome::Done)
        })
    }

    fn entity_queue_config() -> apalis_postgres::Config {
        let poll_strategy = StrategyBuilder::new()
            .apply(IntervalStrategy::new(Duration::from_millis(10)))
            .build();
        apalis_postgres::Config::new(ADAPTIVE_QUEUE)
            .with_poll_interval(poll_strategy)
            .set_buffer_size(8)
    }
}
