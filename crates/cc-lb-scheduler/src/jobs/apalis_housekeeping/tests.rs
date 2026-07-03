const DAY_SECS: u64 = 86_400;
const NOW_SECS: u64 = 2_000_000;

use super::{
    ApalisHousekeepingConfig, ApalisHousekeepingJob, ApalisHousekeepingJobHandler,
    ApalisHousekeepingJobResult,
};

#[cfg(feature = "sqlite")]
mod sqlite {
    use apalis_sqlite::SqliteStorage;
    use sqlx::SqlitePool;

    use super::{
        ApalisHousekeepingConfig, ApalisHousekeepingJob, ApalisHousekeepingJobHandler,
        ApalisHousekeepingJobResult, NOW_SECS, expected_job_ids, expected_result, jobs, workers,
    };

    #[tokio::test]
    async fn prunes_old_workers_done_and_failed_jobs() -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqlitePool::connect(":memory:").await?;
        SqliteStorage::setup(&pool).await?;
        seed_sqlite(&pool).await?;

        let result =
            ApalisHousekeepingJobHandler::new(pool.clone(), ApalisHousekeepingConfig::new(1))
                .handle(ApalisHousekeepingJob::default(), NOW_SECS)
                .await;

        assert_eq!(result, expected_result());
        assert_eq!(
            ids(&pool, "Workers").await?,
            vec!["worker-cutoff", "worker-live"]
        );
        assert_eq!(ids(&pool, "Jobs").await?, expected_job_ids());
        Ok(())
    }

    #[tokio::test]
    async fn reaps_stale_running_locks_and_leaves_fresh_locks_alone()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqlitePool::connect(":memory:").await?;
        SqliteStorage::setup(&pool).await?;
        seed_stale_lock_scenario(&pool).await?;

        let config = ApalisHousekeepingConfig::new(30).with_stale_lock_threshold_secs(120);
        let result = ApalisHousekeepingJobHandler::new(pool.clone(), config)
            .handle(ApalisHousekeepingJob::default(), NOW_SECS)
            .await;

        match result {
            ApalisHousekeepingJobResult::Done {
                stale_locks_reaped, ..
            } => assert_eq!(stale_locks_reaped, 1),
            other => panic!("expected Done with reaped=1, got {other:?}"),
        }

        let (status, lock_by, lock_at, attempts, last_result): (
            String,
            Option<String>,
            Option<i64>,
            i64,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT status, lock_by, lock_at, attempts, last_result FROM Jobs WHERE id = 'stuck-running'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(status, "Pending");
        assert!(lock_by.is_none(), "lock_by must be cleared after reap");
        assert!(lock_at.is_none(), "lock_at must be cleared after reap");
        assert_eq!(attempts, 1, "attempts must advance by 1 after reap");
        let last_result = last_result.expect("last_result must be set after reap");
        assert!(
            last_result.contains("cc-lb housekeeping") && last_result.contains("stale"),
            "last_result must record the reap reason, got: {last_result}",
        );

        let (fresh_status, fresh_lock_by, fresh_attempts): (String, Option<String>, i64) =
            sqlx::query_as("SELECT status, lock_by, attempts FROM Jobs WHERE id = 'fresh-running'")
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            fresh_status, "Running",
            "fresh Running row must be untouched"
        );
        assert_eq!(fresh_lock_by.as_deref(), Some("worker-live"));
        assert_eq!(fresh_attempts, 0);
        Ok(())
    }

    async fn seed_sqlite(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        for (id, last_seen) in workers() {
            sqlx::query("INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) VALUES (?1, 'housekeeping', 'default', '', ?2, ?2)")
                .bind(id)
                .bind(i64::try_from(last_seen).expect("test timestamp fits i64"))
                .execute(pool)
                .await?;
        }
        for (id, status, done_at) in jobs() {
            sqlx::query("INSERT INTO Jobs (job, id, job_type, status, run_at, done_at) VALUES (?1, ?2, 'housekeeping', ?3, ?4, ?5)")
                .bind(vec![0_u8])
                .bind(id)
                .bind(status)
                .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
                .bind(done_at.map(|value| i64::try_from(value).expect("test timestamp fits i64")))
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    async fn seed_stale_lock_scenario(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) VALUES ('worker-live', 'cron', 'default', '', ?1, ?1)")
            .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
            .execute(pool)
            .await?;
        let stuck_lock_at = i64::try_from(NOW_SECS - 300).expect("stuck timestamp fits i64");
        sqlx::query("INSERT INTO Jobs (job, id, job_type, status, run_at, lock_by, lock_at) VALUES (?1, 'stuck-running', 'cron', 'Running', ?2, 'worker-live', ?3)")
            .bind(vec![0_u8])
            .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
            .bind(stuck_lock_at)
            .execute(pool)
            .await?;
        let fresh_lock_at = i64::try_from(NOW_SECS - 10).expect("fresh timestamp fits i64");
        sqlx::query("INSERT INTO Jobs (job, id, job_type, status, run_at, lock_by, lock_at) VALUES (?1, 'fresh-running', 'cron', 'Running', ?2, 'worker-live', ?3)")
            .bind(vec![0_u8])
            .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
            .bind(fresh_lock_at)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn ids(pool: &SqlitePool, table: &str) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>(&format!("SELECT id FROM {table} ORDER BY id"))
            .fetch_all(pool)
            .await
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use std::str::FromStr as _;

    use apalis_postgres::PostgresStorage;
    use chrono::{DateTime, Utc};
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::{
        ApalisHousekeepingConfig, ApalisHousekeepingJob, ApalisHousekeepingJobHandler, NOW_SECS,
        expected_job_ids, expected_result, jobs, workers,
    };

    #[tokio::test]
    async fn prunes_old_workers_done_and_failed_jobs() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP: DATABASE_URL not set; skipping postgres housekeeping test");
            return Ok(());
        };
        if !is_safe_database_url(&url) {
            eprintln!("SKIP: DATABASE_URL is not a recognized local test database");
            return Ok(());
        }
        let db = format!("cc_lb_scheduler_housekeeping_{}", Uuid::new_v4().simple());
        let options = PgConnectOptions::from_str(&url)?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await?;
        if let Err(error) = sqlx::query(&format!(r#"CREATE DATABASE "{db}""#))
            .execute(&admin)
            .await
        {
            eprintln!("SKIP: could not create temporary postgres database: {error}");
            admin.close().await;
            return Ok(());
        }
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.database(&db))
            .await?;
        let test_result = assert_postgres_housekeeping(&pool).await;
        pool.close().await;
        let drop_result = sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{db}""#))
            .execute(&admin)
            .await;
        admin.close().await;
        test_result?;
        drop_result?;
        Ok(())
    }

    async fn assert_postgres_housekeeping(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
        PostgresStorage::setup(pool).await?;
        seed_postgres(pool).await?;
        let result =
            ApalisHousekeepingJobHandler::new(pool.clone(), ApalisHousekeepingConfig::new(1))
                .handle(ApalisHousekeepingJob::default(), NOW_SECS)
                .await;
        assert_eq!(result, expected_result());
        assert_eq!(
            ids(pool, "apalis.workers").await?,
            vec!["worker-cutoff", "worker-live"]
        );
        assert_eq!(ids(pool, "apalis.jobs").await?, expected_job_ids());
        Ok(())
    }

    async fn seed_postgres(pool: &PgPool) -> Result<(), sqlx::Error> {
        for (id, last_seen) in workers() {
            sqlx::query("INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at) VALUES ($1, 'housekeeping', 'default', '', $2, $2)")
                .bind(id)
                .bind(ts(last_seen))
                .execute(pool)
                .await?;
        }
        for (id, status, done_at) in jobs() {
            sqlx::query("INSERT INTO apalis.jobs (job, id, job_type, status, run_at, done_at) VALUES ($1, $2, 'housekeeping', $3, $4, $5)")
                .bind(vec![0_u8])
                .bind(id)
                .bind(status)
                .bind(ts(NOW_SECS))
                .bind(done_at.map(ts))
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    async fn ids(pool: &PgPool, table: &str) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>(&format!("SELECT id FROM {table} ORDER BY id"))
            .fetch_all(pool)
            .await
    }

    fn is_safe_database_url(url: &str) -> bool {
        url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")
    }

    fn ts(secs: u64) -> DateTime<Utc> {
        DateTime::from_timestamp(i64::try_from(secs).expect("test timestamp fits i64"), 0)
            .expect("test timestamp is valid")
    }
}

fn expected_result() -> ApalisHousekeepingJobResult {
    ApalisHousekeepingJobResult::Done {
        workers_removed: 1,
        jobs_removed: 2,
        stale_locks_reaped: 0,
        cutoff_unix_secs: NOW_SECS - DAY_SECS,
    }
}

fn expected_job_ids() -> Vec<&'static str> {
    vec![
        "active-pending",
        "active-queued",
        "active-running",
        "recent-done",
    ]
}

fn workers() -> [(&'static str, u64); 3] {
    [
        ("worker-dead", NOW_SECS - 3_601),
        ("worker-cutoff", NOW_SECS - 3_600),
        ("worker-live", NOW_SECS - 1),
    ]
}

fn jobs() -> [(&'static str, &'static str, Option<u64>); 6] {
    [
        ("active-pending", "Pending", Some(NOW_SECS - (2 * DAY_SECS))),
        ("active-queued", "Queued", Some(NOW_SECS - (2 * DAY_SECS))),
        ("active-running", "Running", Some(NOW_SECS - (2 * DAY_SECS))),
        ("old-done", "Done", Some(NOW_SECS - (2 * DAY_SECS))),
        ("old-failed", "Failed", Some(NOW_SECS - (2 * DAY_SECS))),
        ("recent-done", "Done", Some(NOW_SECS - 60)),
    ]
}
