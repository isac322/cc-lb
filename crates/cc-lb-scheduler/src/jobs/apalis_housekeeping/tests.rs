const DAY_SECS: u64 = 86_400;
const NOW_SECS: u64 = 2_000_000;
const HOUSEKEEPING_REPEAT_COUNT: usize = 100;

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
        ApalisHousekeepingJobResult, DAY_SECS, HOUSEKEEPING_REPEAT_COUNT, NOW_SECS,
        cache_keepalive_jobs, expected_job_ids, expected_result, jobs, sessions, workers,
    };

    #[tokio::test]
    async fn prunes_old_workers_done_and_failed_jobs() -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqlitePool::connect(":memory:").await?;
        SqliteStorage::setup(&pool).await?;
        create_cache_keepalive_sessions_table(&pool).await?;
        seed_sqlite(&pool).await?;
        seed_sqlite_cache_keepalive_cleanup(&pool).await?;

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
        assert_eq!(
            session_hashes(&pool).await?,
            vec!["live-enqueued", "live-pending"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn remains_successful_across_repeated_runs() -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqlitePool::connect(":memory:").await?;
        SqliteStorage::setup(&pool).await?;
        create_cache_keepalive_sessions_table(&pool).await?;
        seed_sqlite(&pool).await?;
        seed_sqlite_cache_keepalive_cleanup(&pool).await?;

        let handler = ApalisHousekeepingJobHandler::new(pool, ApalisHousekeepingConfig::new(1));
        for _ in 0..HOUSEKEEPING_REPEAT_COUNT {
            assert!(
                matches!(
                    handler
                        .handle(ApalisHousekeepingJob::default(), NOW_SECS)
                        .await,
                    ApalisHousekeepingJobResult::Done { .. }
                ),
                "housekeeping must remain successful across repeated runs"
            );
        }
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

    #[tokio::test]
    async fn terminalizes_stale_running_keepalive_sessions_without_reenqueuing_jobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqlitePool::connect(":memory:").await?;
        SqliteStorage::setup(&pool).await?;
        create_cache_keepalive_sessions_table(&pool).await?;
        seed_running_keepalive_sessions(&pool).await?;

        let result = ApalisHousekeepingJobHandler::new(
            pool.clone(),
            ApalisHousekeepingConfig::new(30).with_stale_lock_threshold_secs(120),
        )
        .handle(ApalisHousekeepingJob::default(), NOW_SECS)
        .await;

        match result {
            ApalisHousekeepingJobResult::Done {
                cache_keepalive_sessions_removed,
                ..
            } => assert_eq!(cache_keepalive_sessions_removed, 1),
            other => panic!("expected completed housekeeping, got {other:?}"),
        }
        let stale: (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT status, terminal_reason, running_since_unix_secs
             FROM cache_keepalive_sessions WHERE session_key_hash = 'stale-running'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(stale.0, "terminal");
        assert_eq!(stale.1.as_deref(), Some("stale"));
        assert_eq!(stale.2, None);
        let fresh: (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT status, terminal_reason, running_since_unix_secs
             FROM cache_keepalive_sessions WHERE session_key_hash = 'fresh-running'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(fresh.0, "active");
        assert_eq!(fresh.1, None);
        assert_eq!(fresh.2, Some(i64::try_from(NOW_SECS - 10)?));
        let (job_status, lock_by, lock_at): (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT status, lock_by, lock_at FROM Jobs WHERE id = 'stale-keepalive-job'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(job_status, "Killed");
        assert!(lock_by.is_none());
        assert!(lock_at.is_none());
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
        sqlx::query("UPDATE Jobs SET lock_by = 'worker-dead' WHERE id = 'old-done'")
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn create_cache_keepalive_sessions_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE cache_keepalive_sessions (
                session_key_hash TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                enqueue_state TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                terminal_reason TEXT,
                running_since_unix_secs INTEGER
            )",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_sqlite_cache_keepalive_cleanup(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        for (session_key_hash, status, enqueue_state, expires_at, updated_at) in sessions() {
            sqlx::query(
                "INSERT INTO cache_keepalive_sessions (session_key_hash, status, enqueue_state, expires_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(session_key_hash)
            .bind(status)
            .bind(enqueue_state)
            .bind(i64::try_from(expires_at).expect("test timestamp fits i64"))
            .bind(i64::try_from(updated_at).expect("test timestamp fits i64"))
            .execute(pool)
            .await?;
        }
        for (id, status, run_at, done_at) in cache_keepalive_jobs() {
            sqlx::query(
                "INSERT INTO Jobs (job, id, job_type, status, run_at, done_at, idempotency_key)
                 VALUES (?1, ?2, 'cache_keepalive', ?3, ?4, ?5, ?6)",
            )
            .bind(Vec::<u8>::new())
            .bind(id)
            .bind(status)
            .bind(i64::try_from(run_at).expect("test timestamp fits i64"))
            .bind(done_at.map(|value| i64::try_from(value).expect("test timestamp fits i64")))
            .bind(format!("cache_keepalive:{id}:1"))
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

    async fn seed_running_keepalive_sessions(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at)
             VALUES ('worker-live', 'cron', 'default', '', ?1, ?1)",
        )
        .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
        .execute(pool)
        .await?;
        for (session_key_hash, running_since_unix_secs) in [
            ("stale-running", NOW_SECS - 300),
            ("fresh-running", NOW_SECS - 10),
        ] {
            sqlx::query(
                "INSERT INTO cache_keepalive_sessions
                 (session_key_hash, status, enqueue_state, expires_at, updated_at, terminal_reason, running_since_unix_secs)
                 VALUES (?1, 'active', 'running', ?2, ?2, NULL, ?3)",
            )
            .bind(session_key_hash)
            .bind(i64::try_from(NOW_SECS + DAY_SECS).expect("test timestamp fits i64"))
            .bind(i64::try_from(running_since_unix_secs).expect("test timestamp fits i64"))
            .execute(pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, run_at, lock_by, lock_at, idempotency_key)
             VALUES (?1, 'stale-keepalive-job', 'adaptive', 'Running', ?2, 'worker-live', ?3, 'cache_keepalive:stale-running:1')",
        )
        .bind(Vec::<u8>::new())
        .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
        .bind(i64::try_from(NOW_SECS - 300).expect("test timestamp fits i64"))
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn ids(pool: &SqlitePool, table: &str) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>(&format!("SELECT id FROM {table} ORDER BY id"))
            .fetch_all(pool)
            .await
    }

    async fn session_hashes(pool: &SqlitePool) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>(
            "SELECT session_key_hash FROM cache_keepalive_sessions ORDER BY session_key_hash",
        )
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
        ApalisHousekeepingConfig, ApalisHousekeepingJob, ApalisHousekeepingJobHandler, DAY_SECS,
        HOUSEKEEPING_REPEAT_COUNT, NOW_SECS, cache_keepalive_jobs, expected_job_ids,
        expected_result, jobs, sessions, workers,
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
        create_cache_keepalive_sessions_table(pool).await?;
        seed_postgres(pool).await?;
        seed_postgres_cache_keepalive_cleanup(pool).await?;
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
        assert_eq!(
            session_hashes(pool).await?,
            vec!["live-enqueued", "live-pending"]
        );
        assert_postgres_stale_running_keepalive_recovery(pool).await?;
        assert_postgres_stale_running_lock_recovery(pool).await?;
        let handler =
            ApalisHousekeepingJobHandler::new(pool.clone(), ApalisHousekeepingConfig::new(30));
        for _ in 0..HOUSEKEEPING_REPEAT_COUNT {
            assert!(
                matches!(
                    handler
                        .handle(ApalisHousekeepingJob::default(), NOW_SECS)
                        .await,
                    super::ApalisHousekeepingJobResult::Done { .. }
                ),
                "housekeeping must remain successful across repeated runs"
            );
        }
        Ok(())
    }

    async fn assert_postgres_stale_running_keepalive_recovery(
        pool: &PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (session_key_hash, running_since_unix_secs) in [
            ("stale-running", NOW_SECS - 300),
            ("fresh-running", NOW_SECS - 10),
        ] {
            sqlx::query(
                "INSERT INTO cache_keepalive_sessions
                 (session_key_hash, status, enqueue_state, expires_at, updated_at, terminal_reason, running_since_unix_secs)
                 VALUES ($1, 'active', 'running', $2, $2, NULL, $3)",
            )
            .bind(session_key_hash)
            .bind(i64::try_from(NOW_SECS + DAY_SECS)?)
            .bind(i64::try_from(running_since_unix_secs)?)
            .execute(pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO apalis.jobs (job, id, job_type, status, run_at, lock_by, lock_at, idempotency_key)
             VALUES ($1, 'stale-keepalive-job', 'cache_keepalive', 'Running', $2, 'worker-live', $3, 'cache_keepalive:stale-running:1')",
        )
        .bind(Vec::<u8>::new())
        .bind(ts(NOW_SECS))
        .bind(ts(NOW_SECS - 300))
        .execute(pool)
        .await?;

        let result = ApalisHousekeepingJobHandler::new(
            pool.clone(),
            ApalisHousekeepingConfig::new(30).with_stale_lock_threshold_secs(120),
        )
        .handle(ApalisHousekeepingJob::default(), NOW_SECS)
        .await;
        match result {
            super::ApalisHousekeepingJobResult::Done {
                cache_keepalive_sessions_removed,
                ..
            } => assert_eq!(cache_keepalive_sessions_removed, 1),
            other => panic!("expected completed housekeeping, got {other:?}"),
        }
        let stale: (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT status, terminal_reason, running_since_unix_secs
             FROM cache_keepalive_sessions WHERE session_key_hash = 'stale-running'",
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(stale.0, "terminal");
        assert_eq!(stale.1.as_deref(), Some("stale"));
        assert_eq!(stale.2, None);
        let fresh: (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT status, terminal_reason, running_since_unix_secs
             FROM cache_keepalive_sessions WHERE session_key_hash = 'fresh-running'",
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(fresh.0, "active");
        assert_eq!(fresh.1, None);
        assert_eq!(fresh.2, Some(i64::try_from(NOW_SECS - 10)?));
        let (job_status, lock_by, lock_at): (String, Option<String>, Option<DateTime<Utc>>) =
            sqlx::query_as(
                "SELECT status, lock_by, lock_at FROM apalis.jobs WHERE id = 'stale-keepalive-job'",
            )
            .fetch_one(pool)
            .await?;
        assert_eq!(job_status, "Killed");
        assert!(lock_by.is_none());
        assert!(lock_at.is_none());
        Ok(())
    }
    async fn assert_postgres_stale_running_lock_recovery(
        pool: &PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(
            "INSERT INTO apalis.jobs
             (job, id, job_type, status, run_at, lock_by, lock_at, idempotency_key)
             VALUES ($1, 'stale-running-job', 'housekeeping', 'Running', $2, 'worker-live', $3, 'regular:stale-running:1')",
        )
        .bind(Vec::<u8>::new())
        .bind(ts(NOW_SECS))
        .bind(ts(NOW_SECS - 300))
        .execute(pool)
        .await?;

        let result = ApalisHousekeepingJobHandler::new(
            pool.clone(),
            ApalisHousekeepingConfig::new(30).with_stale_lock_threshold_secs(120),
        )
        .handle(ApalisHousekeepingJob::default(), NOW_SECS)
        .await;
        match result {
            super::ApalisHousekeepingJobResult::Done {
                stale_locks_reaped, ..
            } => assert_eq!(stale_locks_reaped, 1),
            other => panic!("expected completed housekeeping, got {other:?}"),
        }

        let (status, lock_by, lock_at, attempts): (
            String,
            Option<String>,
            Option<DateTime<Utc>>,
            i32,
        ) = sqlx::query_as(
            "SELECT status, lock_by, lock_at, attempts
             FROM apalis.jobs WHERE id = 'stale-running-job'",
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(status, "Pending");
        assert!(lock_by.is_none());
        assert!(lock_at.is_none());
        assert_eq!(attempts, 1);
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
        sqlx::query("UPDATE apalis.jobs SET lock_by = 'worker-dead' WHERE id = 'old-done'")
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn create_cache_keepalive_sessions_table(pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE cache_keepalive_sessions (
                session_key_hash TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                enqueue_state TEXT NOT NULL,
                expires_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                terminal_reason TEXT,
                running_since_unix_secs BIGINT
            )",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_postgres_cache_keepalive_cleanup(pool: &PgPool) -> Result<(), sqlx::Error> {
        for (session_key_hash, status, enqueue_state, expires_at, updated_at) in sessions() {
            sqlx::query(
                "INSERT INTO cache_keepalive_sessions (session_key_hash, status, enqueue_state, expires_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(session_key_hash)
            .bind(status)
            .bind(enqueue_state)
            .bind(i64::try_from(expires_at).expect("test timestamp fits i64"))
            .bind(i64::try_from(updated_at).expect("test timestamp fits i64"))
            .execute(pool)
            .await?;
        }
        for (id, status, run_at, done_at) in cache_keepalive_jobs() {
            sqlx::query(
                "INSERT INTO apalis.jobs (job, id, job_type, status, run_at, done_at, idempotency_key)
                 VALUES ($1, $2, 'cache_keepalive', $3, $4, $5, $6)",
            )
            .bind(Vec::<u8>::new())
            .bind(id)
            .bind(status)
            .bind(ts(run_at))
            .bind(done_at.map(ts))
            .bind(format!("cache_keepalive:{id}:1"))
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

    async fn session_hashes(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>(
            "SELECT session_key_hash FROM cache_keepalive_sessions ORDER BY session_key_hash",
        )
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
        cache_keepalive_sessions_removed: 3,
        cache_keepalive_jobs_removed: 2,
        cutoff_unix_secs: NOW_SECS - DAY_SECS,
    }
}

fn expected_job_ids() -> Vec<&'static str> {
    vec![
        "active-pending",
        "active-queued",
        "active-running",
        "keepalive-active-queued",
        "keepalive-active-running",
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

fn sessions() -> [(&'static str, &'static str, &'static str, u64, u64); 5] {
    [
        (
            "expired-terminal",
            "terminal",
            "enqueued",
            NOW_SECS - 1,
            NOW_SECS - 10,
        ),
        (
            "expired-active",
            "active",
            "enqueued",
            NOW_SECS - 1,
            NOW_SECS - 10,
        ),
        (
            "stale-pending",
            "active",
            "pending",
            NOW_SECS + DAY_SECS,
            NOW_SECS - 3_700,
        ),
        (
            "live-pending",
            "active",
            "pending",
            NOW_SECS + DAY_SECS,
            NOW_SECS - 10,
        ),
        (
            "live-enqueued",
            "active",
            "enqueued",
            NOW_SECS + DAY_SECS,
            NOW_SECS - 3_700,
        ),
    ]
}

fn cache_keepalive_jobs() -> [(&'static str, &'static str, u64, Option<u64>); 4] {
    [
        (
            "keepalive-old-done",
            "Done",
            NOW_SECS - 3_700,
            Some(NOW_SECS - 3_700),
        ),
        ("keepalive-stale-pending", "Pending", NOW_SECS - 3_700, None),
        ("keepalive-active-queued", "Queued", NOW_SECS - 3_700, None),
        (
            "keepalive-active-running",
            "Running",
            NOW_SECS - 3_700,
            None,
        ),
    ]
}
