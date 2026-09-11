use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use apalis_cron::Schedule;
use cc_lb_scheduler::cron::WorkerBuilder;
use chrono::{DateTime, Utc};

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
    Utc::now() + chrono::Duration::milliseconds(50)
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn t3__cron_sqlite_records_one_tick_across_replicas() -> Result<(), Box<dyn std::error::Error>>
{
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
    use sqlx::postgres::PgPoolOptions;

    fn is_safe_database_url(url: &str) -> bool {
        url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")
    }

    async fn run_in_temporary_database(
        admin_pool: &PgPool,
        isolation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let suffix = isolation_name.trim_start_matches("cc_lb_test_");
        let database_name = format!("cc_lb_scheduler_cron_{suffix}");
        let admin_options = admin_pool.connect_options().as_ref().clone();

        let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
        sqlx::query(&create_database).execute(admin_pool).await?;

        let test_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(admin_options.database(&database_name))
            .await?;
        let test_result = assert_postgres_cron_behavior(&test_pool).await;
        test_pool.close().await;

        // apalis-postgres spawns background LISTEN tasks that may still hold
        // pooled sessions after `.close()`. WITH (FORCE) tells Postgres to
        // terminate any lingering sessions on this database before drop.
        // Requires PostgreSQL 13+.
        let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)"#);
        let drop_result = sqlx::query(&drop_database).execute(admin_pool).await;

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
    async fn t3_postgres__records_one_tick_across_replicas()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = crate::postgres_fixture().await?;
        let admin_pool = crate::scheduler_postgres_pool(&fixture).await?;
        let options = admin_pool.connect_options();
        let endpoint = format!(
            "{} {}",
            options.get_host(),
            options.get_database().unwrap_or_default()
        );
        assert!(
            is_safe_database_url(&endpoint),
            "CI_POSTGRES_URL must identify localhost, 127.0.0.1, or cc_lb_test"
        );
        let test_result = run_in_temporary_database(&admin_pool, fixture.schema_name()).await;
        admin_pool.close().await;
        let teardown_result = fixture.teardown().await;
        test_result?;
        teardown_result?;
        Ok(())
    }
}
