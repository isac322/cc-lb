//! Rehearse the additive principal cache-mode migration through sqlx on a
//! disposable database with 5,000 principals and production-scale JSON volume.
use anyhow::{Context, Result, ensure};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use std::str::FromStr;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[test]
fn principal_prompt_cache_mode_migration_is_metadata_only() {
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        eprintln!("skip: requires an isolated PostgreSQL test DSN");
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = Fixture::create(&url).await?;
            let result = rehearse(&fixture.pool).await;
            fixture.drop_database().await?;
            result
        })
        .expect("migration rehearsal");
}

async fn rehearse(pool: &PgPool) -> Result<()> {
    let migrator = sqlx::migrate!("./migrations");
    let mut connection = pool.acquire().await?;
    sqlx::query("SET statement_timeout = '30s'")
        .execute(&mut *connection)
        .await?;
    migrator
        .run_direct(Some(133), &mut *connection, false)
        .await?;
    let logical_bytes = seed_principals(pool, 5_000).await?;
    ensure!(
        logical_bytes >= 16 * 1024 * 1024,
        "insufficient rehearsal byte volume"
    );
    let before = table_facts(pool).await?;
    let started = Instant::now();
    migrator.run_direct(None, &mut *connection, false).await?;
    let elapsed = started.elapsed();
    let after = table_facts(pool).await?;
    let column = column_facts(pool).await?;
    ensure!(
        before.rows == 5_000 && after.rows == before.rows,
        "row count changed"
    );
    ensure!(
        before.relfilenode == after.relfilenode,
        "table was rewritten"
    );
    ensure!(
        column.has_missing_value && column.check_validated,
        "default must be metadata-only with a validated CHECK"
    );
    ensure!(
        elapsed < Duration::from_secs(15),
        "migration exceeded advisory-lock budget"
    );
    let defaults: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM principals_v1 WHERE prompt_cache_mode = 'preserve_client'",
    )
    .fetch_one(pool)
    .await?;
    ensure!(defaults == 5_000, "existing rows must retain client policy");
    let invalid = sqlx::query(
        "UPDATE principals_v1 SET prompt_cache_mode = 'invalid' WHERE name = 'principal-00000001'",
    )
    .execute(pool)
    .await;
    ensure!(
        matches!(invalid, Err(sqlx::Error::Database(_))),
        "unknown modes must be rejected"
    );
    let started = Instant::now();
    migrator.run_direct(None, &mut *connection, false).await?;
    println!(
        "migration 0134: rows=5000 logical_bytes={logical_bytes} elapsed={elapsed:?} noop={:?} heap={} -> {} total={} -> {} rewrite=false",
        started.elapsed(),
        before.heap_bytes,
        after.heap_bytes,
        before.total_bytes,
        after.total_bytes
    );
    Ok(())
}

async fn seed_principals(pool: &PgPool, rows: i64) -> Result<i64> {
    sqlx::query(
        "INSERT INTO principals_v1 (
            id, name, kind, allowed_models, default_limits, enabled, revision,
            created_at, updated_at, allowed_upstreams, router_terminal_strategy, cache_keepalive
         )
         SELECT
            ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
            'principal-' || lpad(g::text, 8, '0'),
            'machine',
            (SELECT jsonb_agg(jsonb_build_object('model', 'claude-model-' || i, 'note', repeat('m', 128)))
               FROM generate_series(1, 32) AS i),
            (SELECT jsonb_agg(jsonb_build_object('kind', 'input_tokens', 'limit', 1000000 + i, 'note', repeat('l', 256)))
               FROM generate_series(1, 16) AS i),
            TRUE,
            1,
            NOW(),
            NOW(),
            '{}'::uuid[],
            'first-pick',
            jsonb_build_object(
                'enabled', TRUE,
                'refresh_lead_time_5m_secs', 30,
                'classifier', jsonb_build_object('note', repeat('c', 2048))
            )
         FROM generate_series(1, $1) AS g",
    )
    .bind(rows)
    .execute(pool)
    .await
    .context("seed principals")?;
    // JSONB values large enough to be stored out of line are TOAST-compressed,
    // so the logical text length is the byte volume a deployment carries.
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(
                    octet_length(allowed_models::text)
                  + octet_length(default_limits::text)
                  + COALESCE(octet_length(cache_keepalive::text), 0)
                ), 0)::bigint
           FROM principals_v1",
    )
    .fetch_one(pool)
    .await
    .context("measure seeded principal bytes")
}

struct TableFacts {
    rows: i64,
    relfilenode: i64,
    heap_bytes: i64,
    total_bytes: i64,
}

/// Facts that only exist once migration 0134 has added the column.
struct ColumnFacts {
    has_missing_value: bool,
    check_validated: bool,
}

async fn table_facts(pool: &PgPool) -> Result<TableFacts> {
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM principals_v1")
        .fetch_one(pool)
        .await?;
    let relfilenode: i64 = sqlx::query_scalar(
        "SELECT relfilenode::bigint FROM pg_class WHERE oid = 'principals_v1'::regclass",
    )
    .fetch_one(pool)
    .await?;
    let heap_bytes: i64 = sqlx::query_scalar("SELECT pg_relation_size('principals_v1'::regclass)")
        .fetch_one(pool)
        .await?;
    // Principal JSON columns are large enough to be stored out of line, so the
    // total size (heap + TOAST + indexes) is the honest byte volume.
    let total_bytes: i64 =
        sqlx::query_scalar("SELECT pg_total_relation_size('principals_v1'::regclass)")
            .fetch_one(pool)
            .await?;
    Ok(TableFacts {
        rows,
        relfilenode,
        heap_bytes,
        total_bytes,
    })
}

async fn column_facts(pool: &PgPool) -> Result<ColumnFacts> {
    let has_missing_value: bool = sqlx::query_scalar(
        "SELECT attmissingval IS NOT NULL
           FROM pg_attribute
          WHERE attrelid = 'principals_v1'::regclass AND attname = 'prompt_cache_mode'",
    )
    .fetch_one(pool)
    .await?;
    let check_validated: bool = sqlx::query_scalar(
        "SELECT convalidated
           FROM pg_constraint
          WHERE conrelid = 'principals_v1'::regclass
            AND contype = 'c'
            AND pg_get_constraintdef(oid) LIKE '%prompt_cache_mode%'",
    )
    .fetch_one(pool)
    .await?;
    Ok(ColumnFacts {
        has_missing_value,
        check_validated,
    })
}

struct Fixture {
    database: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    /// Creates a disposable database of its own.
    ///
    /// The migrator takes a *database-wide* advisory lock for the whole
    /// migration run, so a rehearsal sharing the CI database would contend with
    /// every other test's `initialize()`. A database per rehearsal matches how a deployment
    /// applies migrations and removes that contention.
    async fn create(url: &str) -> Result<Self> {
        let database = format!("cc_lb_0134_rehearsal_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE DATABASE {}",
            quote_ident(&database)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(url)?.database(&database);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        Ok(Self {
            database,
            admin_pool,
            pool,
        })
    }

    async fn drop_database(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(&self.database)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
