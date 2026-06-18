//! Spike: apalis-sqlite vs apalis-postgres job-uniqueness divergence (Postgres half).
//!
//! Confirms two behavioral facts at the raw-SQL level:
//!
//!   1. apalis-postgres's sink.sql has **no** `ON CONFLICT` clause, so a duplicate
//!      push raises a UNIQUE VIOLATION (SQLSTATE 23505) instead of silently no-oping.
//!
//!   2. A partial unique index `WHERE status NOT IN ('Done')` allows re-pushing
//!      Done jobs (same fix as for SQLite).
//!
//! Requires `DATABASE_URL` env var pointing to a live Postgres instance.
//! Tests skip gracefully (pass) when DATABASE_URL is unset.
//!
//! SQL sources (apalis-dev/apalis-postgres @ main):
//!   migrations/20220530084123_jobs_workers.sql
//!   migrations/20260508093314_idempotency_key.sql   (full unique index)
//!   queries/task/sink.sql                           (bare INSERT, no ON CONFLICT)

use sqlx::PgPool;

async fn connect() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    Some(PgPool::connect(&url).await.expect("PgPool::connect failed"))
}

async fn setup_full_index(pool: &PgPool) {
    sqlx::query("DROP SCHEMA IF EXISTS apalis_spike_full CASCADE")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA apalis_spike_full")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE apalis_spike_full.jobs (
            id              TEXT        NOT NULL,
            job_type        TEXT        NOT NULL,
            job             BYTEA       NOT NULL DEFAULT '\\x00',
            status          TEXT        NOT NULL DEFAULT 'Pending',
            attempts        INTEGER     NOT NULL DEFAULT 0,
            max_attempts    INTEGER     NOT NULL DEFAULT 25,
            run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            idempotency_key TEXT
        )",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("CREATE UNIQUE INDEX idx_idempotency ON apalis_spike_full.jobs(job_type, idempotency_key)")
        .execute(pool)
        .await
        .unwrap();
}

async fn setup_partial_index(pool: &PgPool) {
    sqlx::query("DROP SCHEMA IF EXISTS apalis_spike_partial CASCADE")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA apalis_spike_partial")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE apalis_spike_partial.jobs (
            id              TEXT        NOT NULL,
            job_type        TEXT        NOT NULL,
            job             BYTEA       NOT NULL DEFAULT '\\x00',
            status          TEXT        NOT NULL DEFAULT 'Pending',
            attempts        INTEGER     NOT NULL DEFAULT 0,
            max_attempts    INTEGER     NOT NULL DEFAULT 25,
            run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            idempotency_key TEXT
        )",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE UNIQUE INDEX idx_idempotency ON apalis_spike_partial.jobs(job_type, idempotency_key)
         WHERE status NOT IN ('Done')",
    )
    .execute(pool)
    .await
    .unwrap();
}

/// Mirrors apalis-postgres `queries/task/sink.sql`:
/// plain INSERT with no ON CONFLICT clause — will raise 23505 on duplicate.
async fn push_job_pg_full(
    pool: &PgPool,
    id: &str,
    job_type: &str,
    key: &str,
) -> Result<u64, sqlx::Error> {
    sqlx::query(
        "INSERT INTO apalis_spike_full.jobs (id, job_type, idempotency_key)
         VALUES ($1, $2, $3)"
    )
    .bind(id)
    .bind(job_type)
    .bind(key)
    .execute(pool)
    .await
    .map(|r| r.rows_affected())
}

async fn push_job_pg_partial(
    pool: &PgPool,
    id: &str,
    job_type: &str,
    key: &str,
) -> Result<u64, sqlx::Error> {
    sqlx::query(
        "INSERT INTO apalis_spike_partial.jobs (id, job_type, idempotency_key)
         VALUES ($1, $2, $3)"
    )
    .bind(id)
    .bind(job_type)
    .bind(key)
    .execute(pool)
    .await
    .map(|r| r.rows_affected())
}

async fn mark_done_pg(pool: &PgPool, id: &str) {
    sqlx::query("UPDATE apalis_spike_partial.jobs SET status = 'Done' WHERE id = $1")
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

async fn teardown_full(pool: &PgPool) {
    sqlx::query("DROP SCHEMA IF EXISTS apalis_spike_full CASCADE")
        .execute(pool)
        .await
        .unwrap();
}

async fn teardown_partial(pool: &PgPool) {
    sqlx::query("DROP SCHEMA IF EXISTS apalis_spike_partial CASCADE")
        .execute(pool)
        .await
        .unwrap();
}

// ── Test 1: Postgres raises unique constraint error on duplicate push ─────────

#[tokio::test]
async fn postgres_duplicate_idempotency_key_raises_unique_violation() {
    let Some(pool) = connect().await else {
        eprintln!("SKIP: DATABASE_URL not set — skipping postgres spike");
        return;
    };
    setup_full_index(&pool).await;

    // First push: succeeds
    push_job_pg_full(&pool, "job-1", "q::email", "key-pg")
        .await
        .expect("first push must succeed");

    // Duplicate push: apalis-postgres has no ON CONFLICT → unique violation (23505)
    let err = push_job_pg_full(&pool, "job-2", "q::email", "key-pg")
        .await
        .expect_err("second push must raise a unique violation");

    let is_unique_violation = err
        .as_database_error()
        .and_then(|dbe| dbe.code())
        .map(|code| code == "23505")
        .unwrap_or(false);

    assert!(
        is_unique_violation,
        "expected PostgreSQL unique_violation (SQLSTATE 23505), got: {err}"
    );

    teardown_full(&pool).await;
}

// ── Test 2: Partial index fix allows re-push of Done jobs (Postgres) ─────────

#[tokio::test]
async fn postgres_partial_index_allows_repush_after_done() {
    let Some(pool) = connect().await else {
        eprintln!("SKIP: DATABASE_URL not set — skipping postgres spike");
        return;
    };
    setup_partial_index(&pool).await;

    push_job_pg_partial(&pool, "job-1", "q::email", "key-done-pg")
        .await
        .unwrap();
    mark_done_pg(&pool, "job-1").await;

    // Re-push after Done: partial index does not cover Done rows → succeeds
    push_job_pg_partial(&pool, "job-2", "q::email", "key-done-pg")
        .await
        .expect("FIX: re-push after Done must succeed with partial index");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM apalis_spike_partial.jobs WHERE idempotency_key = 'key-done-pg'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2, "should have 2 rows: original Done + new Pending");

    teardown_partial(&pool).await;
}
