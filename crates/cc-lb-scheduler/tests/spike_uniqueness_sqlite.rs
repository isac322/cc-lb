#![cfg(all(feature = "sqlite", not(feature = "postgres")))]

//! Spike: apalis-sqlite idempotency-key uniqueness divergence (SQLite half).
//!
//! Confirms three behavioral facts at the raw-SQL level:
//!
//!   1. `ON CONFLICT(job_type, idempotency_key) DO NOTHING` silently swallows
//!      duplicate pushes — the caller sees Ok(()), 0 rows_affected.
//!
//!   2. The current apalis-sqlite **non-partial** unique index blocks re-push of
//!      Done jobs (the bug): a completed job occupies the key forever.
//!
//!   3. A **partial** unique index `WHERE status NOT IN ('Done')` fixes the bug:
//!      Done jobs release their key and can be re-enqueued.
//!
//! SQL sources (apalis-dev/apalis-sqlite @ main):
//!   migrations/20260506101935_idempotency_key.sql
//!   queries/task/sink.sql

use sqlx::SqlitePool;

// ── Schema helpers ────────────────────────────────────────────────────────────

const CREATE_JOBS: &str = "
    CREATE TABLE IF NOT EXISTS Jobs (
        job             BLOB    NOT NULL,
        id              TEXT    NOT NULL UNIQUE,
        job_type        TEXT    NOT NULL,
        status          TEXT    NOT NULL DEFAULT 'Pending',
        attempts        INTEGER NOT NULL DEFAULT 0,
        max_attempts    INTEGER NOT NULL DEFAULT 25,
        run_at          INTEGER NOT NULL DEFAULT (strftime('%s','now')),
        last_error      TEXT,
        lock_at         INTEGER,
        lock_by         TEXT,
        done_at         INTEGER,
        priority        INTEGER NOT NULL DEFAULT 0,
        metadata        TEXT,
        idempotency_key TEXT
    )
";

/// Full (non-partial) unique index — what apalis-sqlite ships today.
/// `migrations/20260506101935_idempotency_key.sql`:
///   CREATE UNIQUE INDEX idx_jobs_idempotency_key ON Jobs(job_type, idempotency_key);
const FULL_UNIQUE_IDX: &str = "
    CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency
    ON Jobs(job_type, idempotency_key)
";

/// Partial unique index — the proposed migration fix.
/// Only non-Done rows participate; Done rows release their idempotency key.
const PARTIAL_UNIQUE_IDX: &str = "
    CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency
    ON Jobs(job_type, idempotency_key)
    WHERE status NOT IN ('Done')
";

/// Mirrors `queries/task/sink.sql` from apalis-sqlite:
///
///   INSERT INTO Jobs VALUES (...)
///   ON CONFLICT(job_type, idempotency_key) DO NOTHING
///
/// Returns rows_affected (1 = inserted, 0 = silently skipped).
async fn push_job(pool: &SqlitePool, id: &str, job_type: &str, key: &str) -> u64 {
    sqlx::query(
        "INSERT INTO Jobs
         VALUES (X'00', ?, ?, 'Pending', 0, 25,
                 strftime('%s','now'), NULL, NULL, NULL, NULL, 0, NULL, ?)
         ON CONFLICT(job_type, idempotency_key) DO NOTHING",
    )
    .bind(id)
    .bind(job_type)
    .bind(key)
    .execute(pool)
    .await
    .expect("push_job failed")
    .rows_affected()
}

async fn push_job_with_partial_conflict_target(
    pool: &SqlitePool,
    id: &str,
    job_type: &str,
    key: &str,
) -> u64 {
    sqlx::query(
        "INSERT INTO Jobs
         VALUES (X'00', ?, ?, 'Pending', 0, 25,
                 strftime('%s','now'), NULL, NULL, NULL, NULL, 0, NULL, ?)
         ON CONFLICT(job_type, idempotency_key) WHERE status NOT IN ('Done') DO NOTHING",
    )
    .bind(id)
    .bind(job_type)
    .bind(key)
    .execute(pool)
    .await
    .expect("push_job_with_partial_conflict_target failed")
    .rows_affected()
}

async fn mark_done(pool: &SqlitePool, id: &str) {
    sqlx::query("UPDATE Jobs SET status = 'Done', done_at = strftime('%s','now') WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await
        .expect("mark_done failed");
}

// ── Test 1: duplicate push is silently ignored ────────────────────────────────

#[tokio::test]
async fn sqlite_duplicate_idempotency_key_is_silently_ignored() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    sqlx::query(CREATE_JOBS).execute(&pool).await.unwrap();
    sqlx::query(FULL_UNIQUE_IDX).execute(&pool).await.unwrap();

    // First push: 1 row inserted
    assert_eq!(push_job(&pool, "job-1", "q::email", "key-abc").await, 1);

    // Duplicate push — same (job_type, idempotency_key): DO NOTHING → 0 rows
    assert_eq!(
        push_job(&pool, "job-2", "q::email", "key-abc").await,
        0,
        "SQLite must silently no-op on duplicate idempotency_key"
    );

    // Exactly 1 row in the table — job-2 was not inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

// ── Test 2: non-partial index blocks re-push of Done jobs (the bug) ──────────

#[tokio::test]
async fn sqlite_non_partial_index_blocks_repush_of_done_jobs() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    sqlx::query(CREATE_JOBS).execute(&pool).await.unwrap();
    sqlx::query(FULL_UNIQUE_IDX).execute(&pool).await.unwrap();

    // Push and complete a job
    assert_eq!(push_job(&pool, "job-1", "q::email", "key-xyz").await, 1);
    mark_done(&pool, "job-1").await;

    // Attempt to re-enqueue the same idempotency key after Done
    let rows = push_job(&pool, "job-2", "q::email", "key-xyz").await;

    // BUG: non-partial index still sees the Done row in the constraint,
    // so DO NOTHING fires and the re-push is silently dropped.
    assert_eq!(
        rows, 0,
        "BUG: non-partial index silently blocks re-push of Done jobs"
    );
}

// ── Test 3: partial index fix allows re-push after Done ───────────────────────

#[tokio::test]
async fn sqlite_partial_index_allows_repush_after_done() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    sqlx::query(CREATE_JOBS).execute(&pool).await.unwrap();
    // Use the partial index migration instead
    sqlx::query(PARTIAL_UNIQUE_IDX)
        .execute(&pool)
        .await
        .unwrap();

    // Push and complete the first job
    assert_eq!(
        push_job_with_partial_conflict_target(&pool, "job-1", "q::email", "key-repush").await,
        1
    );
    mark_done(&pool, "job-1").await;

    // Re-push after Done: Done row is excluded from the partial index → no conflict
    assert_eq!(
        push_job_with_partial_conflict_target(&pool, "job-2", "q::email", "key-repush").await,
        1,
        "FIX: partial index allows re-push once the prior job is Done"
    );

    // Active-job deduplication still enforced for the new Pending job
    assert_eq!(
        push_job_with_partial_conflict_target(&pool, "job-3", "q::email", "key-repush").await,
        0,
        "FIX: active deduplication still works — job-2 is Pending, blocks job-3"
    );

    // Two rows: job-1 (Done) and job-2 (Pending)
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}
