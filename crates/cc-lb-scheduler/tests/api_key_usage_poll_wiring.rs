//! `api_key_usage_poll` scheduler wiring (QA POL-01, POL-10, POL-20): the
//! cron variant, its lifecycle label and metrics, consumption of pushed
//! tasks, and per-tick / per-check deduplication across replicas.

use cc_lb_scheduler::jobs::api_key_usage_poll::{
    API_KEY_USAGE_POLL_JOB, ApiKeyUsagePollCronJob, immediate_poll_idempotency_key,
};
use cc_lb_scheduler::middleware::SchedulerMetricPayload as _;
use cc_lb_scheduler::worker::CronJob;
use uuid::Uuid;

const TICK: u64 = 1_800_000_000;

/// POL-01: the job has its own cron kind, a bounded lifecycle label, and is
/// a default recurring job.
#[test]
fn pol01_job_kind_label_and_default_registration() {
    let job = CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(TICK));

    assert_eq!(job.kind(), API_KEY_USAGE_POLL_JOB);
    assert_eq!(job.scheduler_job_type(), "cron:api_key_usage_poll");
    let defaults = cc_lb_config::SchedulerConfig::default();
    let config = &defaults.recurring_jobs[API_KEY_USAGE_POLL_JOB];
    assert!(config.enabled);
    assert_eq!((config.interval_secs, config.jitter_secs), (300, 15));
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use std::error::Error;
    use std::str::FromStr as _;
    use std::sync::Arc;
    use std::time::Duration;

    use cc_lb_clock::SystemClock;
    use cc_lb_config::SchedulerConfig;
    use cc_lb_scheduler::admin::SchedulerAdminHandle;
    use cc_lb_scheduler::error::SchedulerError;
    use cc_lb_scheduler::retry::JobOutcome;
    use cc_lb_scheduler::scheduler_metrics;
    use cc_lb_scheduler::worker::{
        CRON_QUEUE, SchedulerBackend, SchedulerCtx, SchedulerPushTask, SqliteSchedulerBackend,
        build_cron_worker,
    };
    use metrics_exporter_prometheus::PrometheusBuilder;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct Db {
        pool: sqlx::SqlitePool,
        _dir: TempDir,
    }

    async fn db() -> Result<Db, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str(&url)?.create_if_missing(true))
            .await?;
        apalis_sqlite::SqliteStorage::setup(&pool).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
        Ok(Db { pool, _dir: dir })
    }

    fn immediate(upstream_id: Uuid, checked_at: u64) -> SchedulerPushTask<CronJob> {
        SchedulerPushTask {
            args: CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::for_upstream(TICK, upstream_id)),
            idempotency_key: Some(immediate_poll_idempotency_key(upstream_id, 4, checked_at)),
            run_at_unix_secs: None,
            max_attempts: Some(1),
        }
    }

    /// POL-01, POL-20: a pushed immediate poll is consumed by the cron
    /// worker, dispatched with its upstream id, and counted under the
    /// `cron:api_key_usage_poll` job type.
    #[test]
    fn pol01_pol20_pushed_poll_is_consumed_and_counted() -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                scheduler_metrics::touch_scheduler_metric_handles();
                let db = db().await?;
                let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
                    db.pool.clone(),
                    Arc::new(SystemClock),
                ));
                let upstream_id = Uuid::from_u128(11);
                SchedulerAdminHandle::new(backend.clone())
                    .push_cron_task(immediate(upstream_id, TICK - 5))
                    .await?;

                let (tx, mut rx) = mpsc::channel::<CronJob>(1);
                let worker = build_cron_worker(
                    &backend,
                    SchedulerCtx::new(
                        SchedulerConfig::default(),
                        Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
                        Arc::new(move |job| {
                            let tx = tx.clone();
                            Box::pin(async move {
                                let _ = tx.send(job).await;
                                Ok(JobOutcome::Done)
                            })
                        }),
                        Arc::new(SystemClock),
                    ),
                )?;
                let cancel = CancellationToken::new();
                let running = tokio::spawn(worker.run_until_cancelled(cancel.clone()));
                let job = tokio::time::timeout(Duration::from_secs(30), rx.recv())
                    .await
                    .map_err(|_| "pushed api-key usage poll was not consumed")?
                    .ok_or("dispatch channel closed")?;
                cancel.cancel();
                running.abort();
                let _ = running.await;

                assert!(matches!(
                    job,
                    CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob {
                        tick_unix_secs: TICK,
                        upstream_id: Some(id),
                        ..
                    }) if id == upstream_id
                ));
                let rendered = handle.render();
                assert!(
                    rendered.contains(scheduler_metrics::API_KEY_USAGE_POLL_TOTAL),
                    "{rendered}"
                );
                let started = rendered.lines().any(|line| {
                    line.starts_with(scheduler_metrics::JOBS_TOTAL)
                        && line.contains(r#"job_type="cron:api_key_usage_poll""#)
                        && line.contains(r#"status="started""#)
                        && line.ends_with(" 1")
                });
                assert!(started, "{rendered}");
                Ok::<(), Box<dyn Error>>(())
            })
        })
    }

    /// POL-20: repeated pushes for one check collapse to one job; a new
    /// `checked_at` at the same revision is a new job.
    #[tokio::test]
    async fn pol20_immediate_polls_deduplicate_per_check() -> Result<(), Box<dyn Error>> {
        let db = db().await?;
        let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
            db.pool.clone(),
            Arc::new(SystemClock),
        ));
        let replica_a = SchedulerAdminHandle::new(backend.clone());
        let replica_b = SchedulerAdminHandle::new(backend);
        let upstream_id = Uuid::from_u128(12);

        replica_a
            .push_cron_task(immediate(upstream_id, TICK - 5))
            .await?;
        assert!(matches!(
            replica_b
                .push_cron_task(immediate(upstream_id, TICK - 5))
                .await,
            Err(SchedulerError::Conflict(_))
        ));
        replica_b
            .push_cron_task(immediate(upstream_id, TICK))
            .await?;

        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT idempotency_key FROM Jobs WHERE job_type = ?1 ORDER BY idempotency_key",
        )
        .bind(CRON_QUEUE)
        .fetch_all(&db.pool)
        .await?;
        assert_eq!(
            keys,
            vec![
                format!("api_key_usage_poll:{upstream_id}:4:{}", TICK - 5),
                format!("api_key_usage_poll:{upstream_id}:4:{TICK}"),
            ]
        );
        Ok(())
    }
}

/// POL-10: two replicas on one Postgres database run the real cron
/// producer for the same three ticks; exactly one job per tick is queued,
/// and both replicas' consumers together execute each tick once.
#[cfg(feature = "postgres")]
mod postgres {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{Context as _, Result};
    use cc_lb_clock::SystemClock;
    use cc_lb_config::SchedulerConfig;
    use cc_lb_scheduler::cron::WorkerBuilder as CronWorkerBuilder;
    use cc_lb_scheduler::retry::JobOutcome;
    use cc_lb_scheduler::worker::{
        CRON_QUEUE, PostgresSchedulerBackend, SchedulerBackend, SchedulerCtx, build_cron_worker,
    };
    use chrono::{DateTime, Utc};
    use sqlx::postgres::PgPoolOptions;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use super::*;

    /// Fixed ticks, one second apart, starting one second from now (the
    /// cron stream rejects ticks in the past).
    #[derive(Clone)]
    struct FixedTicks(std::vec::IntoIter<DateTime<Utc>>);

    impl apalis_cron::Schedule<Utc> for FixedTicks {
        fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
            self.0.next()
        }
    }

    fn ticks(start: i64) -> FixedTicks {
        FixedTicks(
            (1..=3)
                .filter_map(|offset| DateTime::<Utc>::from_timestamp(start + offset, 0))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn factory(tick_secs: u64) -> CronJob {
        CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::new(tick_secs))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pol10_two_replicas_queue_and_run_one_job_per_tick() -> Result<()> {
        let Some(admin_url) = std::env::var("DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            eprintln!("SKIP: DATABASE_URL unset");
            return Ok(());
        };
        let database = format!("cclb_pol10_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new().connect(&admin_url).await?;
        sqlx::query(&format!(r#"CREATE DATABASE "{database}""#))
            .execute(&admin)
            .await?;
        let result = run(&admin_url, &database).await;
        sqlx::query(&format!(r#"DROP DATABASE "{database}" WITH (FORCE)"#))
            .execute(&admin)
            .await?;
        admin.close().await;
        result
    }

    async fn run(admin_url: &str, database: &str) -> Result<()> {
        let mut url = Url::parse(admin_url)?;
        url.set_path(database);
        let pool_a = PgPoolOptions::new().connect(url.as_str()).await?;
        let pool_b = PgPoolOptions::new().connect(url.as_str()).await?;
        apalis_postgres::PostgresStorage::setup(&pool_a).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool_a).await?;

        let start = Utc::now().timestamp();
        let mut producers = Vec::new();
        for pool in [&pool_a, &pool_b] {
            let storage = apalis_postgres::PostgresStorage::<CronJob>::new_with_notify(
                pool,
                &apalis_postgres::Config::new(CRON_QUEUE),
            );
            let factory: fn(u64) -> CronJob = factory;
            producers.push(tokio::spawn(
                CronWorkerBuilder::singleton_queue_factory(ticks(start), storage, factory)
                    .max_ticks(3)
                    .run(),
            ));
        }
        for producer in producers {
            producer.await?.context("cron producer")?;
        }

        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT idempotency_key FROM apalis.jobs WHERE job_type = $1 ORDER BY idempotency_key",
        )
        .bind(CRON_QUEUE)
        .fetch_all(&pool_a)
        .await?;
        assert_eq!(rows.len(), 3, "one queued job per tick: {rows:?}");
        assert!(
            rows.iter()
                .all(|key| key.starts_with("cron:api_key_usage_poll:"))
        );

        let (tx, mut rx) = mpsc::channel::<u64>(8);
        let cancel = CancellationToken::new();
        let mut workers = Vec::new();
        for pool in [&pool_a, &pool_b] {
            let backend = SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool.clone()));
            let tx = tx.clone();
            let worker = build_cron_worker(
                &backend,
                SchedulerCtx::new(
                    SchedulerConfig::default(),
                    Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
                    Arc::new(move |job| {
                        let tx = tx.clone();
                        Box::pin(async move {
                            if let CronJob::ApiKeyUsagePoll(job) = job {
                                let _ = tx.send(job.tick_unix_secs).await;
                            }
                            Ok(JobOutcome::Done)
                        })
                    }),
                    Arc::new(SystemClock),
                ),
            )?;
            workers.push(tokio::spawn(worker.run_until_cancelled(cancel.clone())));
        }
        let mut executed = BTreeMap::<u64, usize>::new();
        while executed.values().sum::<usize>() < 3 {
            let tick = tokio::time::timeout(Duration::from_secs(30), rx.recv())
                .await
                .context("waiting for tick execution")?
                .context("dispatch channel closed")?;
            *executed.entry(tick).or_default() += 1;
        }
        cancel.cancel();
        for worker in workers {
            let _ = worker.await;
        }
        pool_a.close().await;
        pool_b.close().await;

        assert_eq!(executed.len(), 3, "each tick executed: {executed:?}");
        assert!(executed.values().all(|count| *count == 1), "{executed:?}");
        Ok(())
    }
}
