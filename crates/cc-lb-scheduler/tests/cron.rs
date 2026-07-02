use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use apalis_cron::Schedule;
use cc_lb_core::clock::{Clock, SystemClock};
use cc_lb_scheduler::cron::WorkerBuilder;
use chrono::{DateTime, Duration, Utc};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct CronConformanceJob {
    name: String,
}

impl cc_lb_scheduler::cron::SingletonCronJob for CronConformanceJob {
    fn singleton_kind(&self) -> &'static str {
        "conformance"
    }
}

#[derive(Clone, Debug)]
struct FixedTicks {
    ticks: Arc<Mutex<VecDeque<DateTime<Utc>>>>,
    polls: Arc<AtomicUsize>,
}

impl FixedTicks {
    fn once(tick: DateTime<Utc>) -> Self {
        Self {
            ticks: Arc::new(Mutex::new(VecDeque::from([tick]))),
            polls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn poll_count(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }
}

impl Schedule<Utc> for FixedTicks {
    fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.ticks.lock().expect("fixed ticks lock").pop_front()
    }
}

fn conformance_job() -> CronConformanceJob {
    CronConformanceJob {
        name: "cron-conformance".to_owned(),
    }
}

fn shared_tick() -> DateTime<Utc> {
    DateTime::<Utc>::from(SystemClock.now()) + Duration::milliseconds(50)
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cron_sqlite_records_one_tick_across_replicas() -> Result<(), Box<dyn std::error::Error>> {
    use apalis_sqlite::{PoolOptions, Sqlite, SqliteStorage};

    let queue = "cron_conformance_sqlite";
    let pool = PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await?;
    SqliteStorage::setup(&pool).await?;

    // Both replicas target the same scheduled tick timestamp so their
    // idempotency keys collide; only the first `push_task` wins.
    let tick = shared_tick();
    let replica_one = FixedTicks::once(tick);
    let replica_two = FixedTicks::once(tick);
    let worker_one = WorkerBuilder::singleton_queue(
        queue,
        replica_one.clone(),
        SqliteStorage::<CronConformanceJob, (), ()>::new_in_queue(&pool, queue),
        conformance_job(),
    )
    .max_ticks(1);
    let worker_two = WorkerBuilder::singleton_queue(
        queue,
        replica_two.clone(),
        SqliteStorage::<CronConformanceJob, (), ()>::new_in_queue(&pool, queue),
        conformance_job(),
    )
    .max_ticks(1);

    let (result_one, result_two) = tokio::join!(worker_one.run(), worker_two.run());
    result_one?;
    result_two?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
        .bind(queue)
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    assert!(replica_one.poll_count() > 0);
    assert!(replica_two.poll_count() > 0);
    Ok(())
}

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;
    use apalis_postgres::{Config, PgPool, PostgresStorage};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr as _;
    use uuid::Uuid;

    fn is_safe_database_url(url: &str) -> bool {
        url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")
    }

    async fn run_in_temporary_database(url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let database_name = format!("cc_lb_scheduler_cron_{}", Uuid::new_v4().simple());
        let admin_options = PgConnectOptions::from_str(url)?;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(admin_options.clone())
            .await?;

        let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
        if let Err(error) = sqlx::query(&create_database).execute(&admin_pool).await {
            eprintln!("SKIP: could not create temporary postgres database: {error}");
            admin_pool.close().await;
            return Ok(());
        }

        let test_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(admin_options.database(&database_name))
            .await?;
        let test_result = assert_postgres_cron_behavior(&test_pool).await;
        test_pool.close().await;

        let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}""#);
        let drop_result = sqlx::query(&drop_database).execute(&admin_pool).await;
        admin_pool.close().await;

        test_result?;
        drop_result?;
        Ok(())
    }

    async fn assert_postgres_cron_behavior(
        pool: &PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let queue = "cron_conformance_postgres";
        PostgresStorage::setup(pool).await?;

        let config = Config::new(queue);
        let tick = shared_tick();
        let replica_one = FixedTicks::once(tick);
        let replica_two = FixedTicks::once(tick);
        let worker_one = WorkerBuilder::singleton_queue(
            queue,
            replica_one.clone(),
            PostgresStorage::<CronConformanceJob>::new_with_config(pool, &config),
            conformance_job(),
        )
        .max_ticks(1);
        let worker_two = WorkerBuilder::singleton_queue(
            queue,
            replica_two.clone(),
            PostgresStorage::<CronConformanceJob>::new_with_config(pool, &config),
            conformance_job(),
        )
        .max_ticks(1);

        let (result_one, result_two) = tokio::join!(worker_one.run(), worker_two.run());
        result_one?;
        result_two?;

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1")
            .bind(queue)
            .fetch_one(pool)
            .await?;
        assert_eq!(count, 1);
        assert!(replica_one.poll_count() > 0);
        assert!(replica_two.poll_count() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn cron_postgres_records_one_tick_across_replicas()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP: DATABASE_URL not set; skipping postgres cron conformance test");
            return Ok(());
        };
        if !is_safe_database_url(&url) {
            eprintln!("SKIP: DATABASE_URL is not a recognized local test database");
            return Ok(());
        }
        run_in_temporary_database(&url).await
    }
}
