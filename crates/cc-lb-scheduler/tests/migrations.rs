#[cfg(feature = "sqlite")]
mod sqlite {
    use apalis_sqlite::SqliteStorage;
    use cc_lb_scheduler::migrations::apply_post_setup_migrations;
    use sqlx::SqlitePool;

    async fn insert_job(
        pool: &SqlitePool,
        id: &str,
        job_type: &str,
        key: &str,
    ) -> Result<u64, sqlx::Error> {
        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, idempotency_key)
             VALUES (?, ?, ?, 'Pending', ?)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(job_type)
        .bind(key)
        .execute(pool)
        .await
        .map(|result| result.rows_affected())
    }

    #[tokio::test]
    async fn sqlite_migrations_restore_full_unique_after_active_priority_vacuum()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqlitePool::connect(":memory:").await?;

        SqliteStorage::setup(&pool).await?;
        apply_post_setup_migrations(&pool).await?;
        apply_post_setup_migrations(&pool).await?;

        let index_sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_jobs_idempotency_key'",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!index_sql.contains("WHERE"));

        sqlx::raw_sql(
            "DROP INDEX IF EXISTS idx_jobs_idempotency_key;
             CREATE UNIQUE INDEX idx_jobs_idempotency_key
             ON Jobs(job_type, idempotency_key)
             WHERE status IN ('Pending','Running','Queued');",
        )
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO Jobs (job, id, job_type, status, idempotency_key, priority) VALUES (?, ?, ?, 'Done', ?, 0)")
            .bind(vec![0_u8])
            .bind("old-done")
            .bind("q::email")
            .bind("vacuum-key")
            .execute(&pool)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO Jobs (job, id, job_type, status, idempotency_key, priority) VALUES (?, ?, ?, 'Pending', ?, 10)")
            .bind(vec![0_u8])
            .bind("new-pending")
            .bind("q::email")
            .bind("vacuum-key")
        .execute(&pool)
        .await?;
        apply_post_setup_migrations(&pool).await?;
        let index_sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_jobs_idempotency_key'",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!index_sql.contains("WHERE"));
        let kept_id: String = sqlx::query_scalar("SELECT id FROM Jobs WHERE idempotency_key = ?")
            .bind("vacuum-key")
            .fetch_one(&pool)
            .await?;
        assert_eq!(kept_id, "new-pending");

        assert_eq!(
            insert_job(&pool, "job-1", "q::email", "key-reuse").await?,
            1
        );
        sqlx::query(
            "UPDATE Jobs SET status = 'Done', done_at = strftime('%s', 'now') WHERE id = ?",
        )
        .bind("job-1")
        .execute(&pool)
        .await?;

        let done_duplicate = insert_job(&pool, "job-2", "q::email", "key-reuse").await;
        assert!(done_duplicate.is_err());

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?")
            .bind("key-reuse")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1);

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use std::str::FromStr as _;

    use apalis_postgres::PostgresStorage;
    use cc_lb_scheduler::migrations::apply_post_setup_migrations;
    use sqlx::PgPool;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use uuid::Uuid;

    fn is_safe_database_url(url: &str) -> bool {
        url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")
    }

    async fn insert_job(
        pool: &PgPool,
        id: &str,
        job_type: &str,
        key: &str,
    ) -> Result<u64, sqlx::Error> {
        sqlx::query(
            "INSERT INTO apalis.jobs (id, job_type, job, status, idempotency_key)
             VALUES ($1, $2, $3, 'Pending', $4)",
        )
        .bind(id)
        .bind(job_type)
        .bind(vec![0_u8])
        .bind(key)
        .execute(pool)
        .await
        .map(|result| result.rows_affected())
    }

    async fn run_in_temporary_database(url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let database_name = format!("cc_lb_scheduler_migrations_{}", Uuid::new_v4().simple());
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
            .max_connections(1)
            .connect_with(admin_options.database(&database_name))
            .await?;
        let test_result = assert_postgres_migration_behavior(&test_pool).await;
        test_pool.close().await;

        let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}""#);
        let drop_result = sqlx::query(&drop_database).execute(&admin_pool).await;
        admin_pool.close().await;

        test_result?;
        drop_result?;
        Ok(())
    }

    async fn assert_postgres_migration_behavior(
        pool: &PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        PostgresStorage::setup(pool).await?;
        apply_post_setup_migrations(pool).await?;
        apply_post_setup_migrations(pool).await?;

        let predicate: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_expr(indexes.indpred, indexes.indrelid)
             FROM pg_index indexes
             JOIN pg_class classes ON classes.oid = indexes.indexrelid
             JOIN pg_namespace namespaces ON namespaces.oid = classes.relnamespace
             WHERE namespaces.nspname = 'apalis'
               AND classes.relname = 'idx_jobs_idempotency_key'",
        )
        .fetch_one(pool)
        .await?;
        assert!(predicate.is_none());

        sqlx::raw_sql(
            "DROP INDEX IF EXISTS apalis.idx_jobs_idempotency_key;
             CREATE UNIQUE INDEX idx_jobs_idempotency_key
             ON apalis.jobs(job_type, idempotency_key)
             WHERE status IN ('Pending','Running','Queued');",
        )
        .execute(pool)
        .await?;
        sqlx::query("INSERT INTO apalis.jobs (id, job_type, job, status, idempotency_key, priority) VALUES ($1, $2, $3, 'Done', $4, 0)")
            .bind("old-done")
            .bind("q::email")
            .bind(vec![0_u8])
            .bind("vacuum-key")
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO apalis.jobs (id, job_type, job, status, idempotency_key, priority) VALUES ($1, $2, $3, 'Pending', $4, 10) ON CONFLICT DO NOTHING")
            .bind("new-pending")
            .bind("q::email")
            .bind(vec![0_u8])
            .bind("vacuum-key")
            .execute(pool)
            .await?;
        apply_post_setup_migrations(pool).await?;
        let predicate: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_expr(indexes.indpred, indexes.indrelid)
             FROM pg_index indexes
             JOIN pg_class classes ON classes.oid = indexes.indexrelid
             JOIN pg_namespace namespaces ON namespaces.oid = classes.relnamespace
             WHERE namespaces.nspname = 'apalis'
               AND classes.relname = 'idx_jobs_idempotency_key'",
        )
        .fetch_one(pool)
        .await?;
        assert!(predicate.is_none());
        let kept_id: String =
            sqlx::query_scalar("SELECT id FROM apalis.jobs WHERE idempotency_key = $1")
                .bind("vacuum-key")
                .fetch_one(pool)
                .await?;
        assert_eq!(kept_id, "new-pending");

        assert_eq!(insert_job(pool, "job-1", "q::email", "key-reuse").await?, 1);
        sqlx::query("UPDATE apalis.jobs SET status = 'Done', done_at = now() WHERE id = $1")
            .bind("job-1")
            .execute(pool)
            .await?;

        let done_duplicate = insert_job(pool, "job-2", "q::email", "key-reuse").await;
        assert!(done_duplicate.is_err());

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM apalis.jobs WHERE idempotency_key = $1")
                .bind("key-reuse")
                .fetch_one(pool)
                .await?;
        assert_eq!(count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn postgres_migrations_restore_full_unique_after_active_priority_vacuum()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP: DATABASE_URL not set; skipping postgres migrations test");
            return Ok(());
        };
        if !is_safe_database_url(&url) {
            eprintln!("SKIP: DATABASE_URL is not a recognized local test database");
            return Ok(());
        }
        run_in_temporary_database(&url).await
    }
}
