// Tests that 2 independent tokio runtimes with separate PgPools
// writing to the same quota row produce exactly 10000 increments.
// This simulates 2 cc-lb instances sharing a Postgres backend.

use std::{error::Error, str::FromStr, sync::Arc, thread};

use cc_lb_storage_api::{AuditEntry, AuditStore, BucketKind, QuotaStore};
use cc_lb_storage_postgres::PostgresStorage;
use chrono::Utc;
use sqlx::AssertSqlSafe;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use uuid::Uuid;

const ITERATIONS: usize = 100;
const RUNTIMES: usize = 2;
const WRITERS_PER_RUNTIME: usize = 50;
const INCREMENTS_PER_WRITER: usize = 100;
const EXPECTED_TOTAL: u64 = (RUNTIMES * WRITERS_PER_RUNTIME * INCREMENTS_PER_WRITER) as u64;
const WINDOW_START: u64 = 1_800_000_000;
const MIGRATIONS: &[&str] = &[
    include_str!("../migrations/0001_meta.sql"),
    include_str!("../migrations/0002_killswitch.sql"),
    include_str!("../migrations/0003_audit_log.sql"),
    include_str!("../migrations/0004_request_events.sql"),
    include_str!("../migrations/0005_quotas.sql"),
    include_str!("../migrations/0006_principal_limit_states.sql"),
    include_str!("../migrations/0007_oauth_credentials.sql"),
    include_str!("../migrations/0008_api_keys.sql"),
    include_str!("../migrations/0009_anthropic_api_keys.sql"),
    include_str!("../migrations/0010_usage_rollups.sql"),
    include_str!("../migrations/0011_config_draft.sql"),
    include_str!("../migrations/0012_config_history.sql"),
];

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn multi_instance_quota_no_lost_updates() {
    let url = match get_postgres_url() {
        Some(u) => u,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    for iter in 0..ITERATIONS {
        let (counter, audit_count) = run_iteration(&url, iter).unwrap_or_else(|error| {
            panic!("iter {iter}: multi-instance quota test failed: {error}")
        });

        println!("iter {iter}: counter={counter} audit_rows={audit_count}");
        assert_eq!(counter, EXPECTED_TOTAL, "iter {iter}: lost update detected");
        assert_eq!(
            audit_count, EXPECTED_TOTAL as i64,
            "iter {iter}: audit rows mismatch"
        );
    }
}

fn run_iteration(url: &str, iter: usize) -> TestResult<(u64, i64)> {
    let schema = format!("test_quota_{iter}_{}", rand_suffix());
    let principal_id = format!("quota-principal-{iter}");
    let runtime_start = Arc::new(std::sync::Barrier::new(RUNTIMES));

    with_runtime("multi-instance-quota-setup", |runtime| {
        runtime.block_on(async {
            create_schema(url, &schema).await?;
            let pool = connect_schema_pool(url, &schema, 1).await?;
            apply_migrations(&pool).await?;
            pool.close().await;
            Ok(())
        })
    })?;

    let runtime_a = spawn_runtime_writers(
        url.to_owned(),
        schema.clone(),
        principal_id.clone(),
        Arc::clone(&runtime_start),
        0,
    );
    let runtime_b = spawn_runtime_writers(
        url.to_owned(),
        schema.clone(),
        principal_id.clone(),
        runtime_start,
        1,
    );

    join_runtime(runtime_a, "runtime A")?;
    join_runtime(runtime_b, "runtime B")?;

    with_runtime("multi-instance-quota-verify", |runtime| {
        runtime.block_on(async {
            let pool = connect_schema_pool(url, &schema, 1).await?;
            let storage = PostgresStorage::new(pool.clone());
            let counter = storage
                .get_quota(&principal_id, WINDOW_START, BucketKind::Requests)
                .await?;
            let audit_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM audit_log_v1")
                .fetch_one(&pool)
                .await?;

            pool.close().await;
            drop_schema(url, &schema).await?;

            Ok((counter, audit_count))
        })
    })
}

fn spawn_runtime_writers(
    url: String,
    schema: String,
    principal_id: String,
    runtime_start: Arc<std::sync::Barrier>,
    runtime_index: usize,
) -> thread::JoinHandle<TestResult<()>> {
    thread::spawn(move || {
        with_runtime(
            &format!("multi-instance-quota-{runtime_index}"),
            |runtime| {
                let pool = runtime.block_on(connect_schema_pool(&url, &schema, 16))?;
                let storage = PostgresStorage::new(pool.clone());

                runtime_start.wait();

                let result = runtime.block_on(run_writers(storage, principal_id, runtime_index));
                runtime.block_on(async { pool.close().await });
                result
            },
        )
    })
}

async fn run_writers(
    storage: PostgresStorage,
    principal_id: String,
    runtime_index: usize,
) -> TestResult<()> {
    let mut handles = Vec::with_capacity(WRITERS_PER_RUNTIME);

    for writer_index in 0..WRITERS_PER_RUNTIME {
        let storage = storage.clone();
        let principal_id = principal_id.clone();

        handles.push(tokio::spawn(async move {
            for increment_index in 0..INCREMENTS_PER_WRITER {
                storage
                    .incr_quota(&principal_id, WINDOW_START, BucketKind::Requests, 1)
                    .await?;
                storage
                    .append_audit(&audit_entry(
                        &principal_id,
                        runtime_index,
                        writer_index,
                        increment_index,
                    ))
                    .await?;
            }

            TestResult::Ok(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

fn audit_entry(
    principal_id: &str,
    runtime_index: usize,
    writer_index: usize,
    increment_index: usize,
) -> AuditEntry {
    AuditEntry {
        ts: Utc::now().timestamp() as u64,
        request_id: Uuid::new_v4().to_string(),
        principal_id: principal_id.to_owned(),
        route: "messages".to_owned(),
        upstream: format!("runtime-{runtime_index}"),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(0),
        output_tokens: Some(0),
        duration_ms: 0,
        agent_label: Some(format!("writer-{writer_index}")),
        api_key_id: None,
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: None,
        actor: None,
        kind: Some(format!("increment-{increment_index}")),
        payload: None,
    }
}

async fn create_schema(url: &str, schema: &str) -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(url)?)
        .await?;

    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await?;
    pool.close().await;

    Ok(())
}

async fn drop_schema(url: &str, schema: &str) -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(url)?)
        .await?;

    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&pool)
    .await?;
    pool.close().await;

    Ok(())
}

async fn connect_schema_pool(
    url: &str,
    schema: &str,
    max_connections: u32,
) -> TestResult<sqlx::PgPool> {
    let url_with_search_path = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);

    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(url_with_search_path)
        .await?)
}

async fn apply_migrations(pool: &sqlx::PgPool) -> TestResult<()> {
    for migration in MIGRATIONS {
        sqlx::raw_sql(*migration).execute(pool).await?;
    }

    Ok(())
}

fn with_runtime<T>(
    thread_name: &str,
    f: impl FnOnce(&tokio::runtime::Runtime) -> TestResult<T>,
) -> TestResult<T> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .thread_name(thread_name)
        .enable_all()
        .build()?;

    f(&runtime)
}

fn join_runtime(handle: thread::JoinHandle<TestResult<()>>, label: &str) -> TestResult<()> {
    handle.join().map_err(|_| format!("{label} panicked"))?
}

fn rand_suffix() -> String {
    Uuid::new_v4().simple().to_string()
}
