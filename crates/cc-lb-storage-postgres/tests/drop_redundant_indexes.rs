//! Redundant indexes remain removed after the relational request-event cutover.
//! Request events have no JSON payload column; diagnostics remain out of SQL.

use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
}

#[test]
fn redundant_indexes_are_dropped_and_request_events_are_relational() {
    let Some(url) = postgres_url() else {
        eprintln!(
            "skip: CI_POSTGRES_URL or PG_URL not set; requires isolated local/test postgres DSN"
        );
        return;
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = Fixture::create(&url).await?;
            let result = run(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop isolated redundant-index schema")?;
            result
        })
        .expect("postgres redundant-index and relational schema regression");
}

async fn run(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await?;

    assert_index_set(pool).await?;
    assert_checkpoint_queries_avoid_seq_scan(pool).await?;
    assert_relational_schema(pool, &storage).await?;
    Ok(())
}

async fn indexes_on(pool: &PgPool, table: &str) -> Result<BTreeSet<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes \
         WHERE schemaname = current_schema() AND tablename = $1",
    )
    .bind(table)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

async fn assert_index_set(pool: &PgPool) -> Result<()> {
    let request_event_indexes = indexes_on(pool, "request_events_v1").await?;
    ensure!(
        !request_event_indexes.contains("request_events_v1_thread_ts"),
        "0134 must drop request_events_v1_thread_ts: {request_event_indexes:?}"
    );
    for kept in [
        "request_events_v1_thread_list_order_idx",
        "request_events_v1_upstream_id_idx",
        "request_events_v1_list_order_idx",
    ] {
        ensure!(
            request_event_indexes.contains(kept),
            "{kept} must remain: {request_event_indexes:?}"
        );
    }

    let checkpoint_indexes = indexes_on(pool, "upstream_subscription_quota_checkpoints_v1").await?;
    for dropped in [
        "upstream_subscription_quota_checkpoints_range_idx",
        "upstream_subscription_quota_checkpoints_anchor_idx",
    ] {
        ensure!(
            !checkpoint_indexes.contains(dropped),
            "0135 must drop {dropped}: {checkpoint_indexes:?}"
        );
    }
    for kept in [
        "upstream_subscription_quota_checkpoints_v1_pkey",
        "upstream_subscription_quota_checkpoints_slim_cover_idx",
    ] {
        ensure!(
            checkpoint_indexes.contains(kept),
            "{kept} must remain: {checkpoint_indexes:?}"
        );
    }
    for (table, dropped) in [
        (
            "usage_rollups_v2",
            "usage_rollups_v2_interval_tokens_cover_idx",
        ),
        ("audit_log_v1", "audit_log_v1_actor_subject"),
    ] {
        let indexes = indexes_on(pool, table).await?;
        ensure!(
            !indexes.contains(dropped),
            "0140 must drop {dropped}: {indexes:?}"
        );
    }
    Ok(())
}

const LATEST_FINGERPRINT_SQL: &str = "SELECT semantic_fingerprint \
     FROM upstream_subscription_quota_checkpoints_v1 \
     WHERE upstream_id = $1 AND \"window\" = $2 AND source = $3 \
     ORDER BY changed_at_unix_millis DESC, sample_id DESC \
     LIMIT 1";

const SLIM_AGGREGATE_SQL: &str = "WITH anchors AS ( \
         SELECT DISTINCT ON (upstream_id, \"window\", source) \
                upstream_id, \"window\", source, changed_at_unix_millis, sample_id, \
                utilization, status, resets_at_unix_secs \
         FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ANY($1::uuid[]) \
           AND \"window\" = ANY($2::text[]) \
           AND source = ANY($3::text[]) \
           AND changed_at_unix_millis < $4 \
         ORDER BY upstream_id ASC, \"window\" ASC, source ASC, \
                  changed_at_unix_millis DESC, sample_id DESC \
     ), ranged AS ( \
         SELECT upstream_id, \"window\", source, changed_at_unix_millis, sample_id, \
                utilization, status, resets_at_unix_secs \
         FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ANY($1::uuid[]) \
           AND \"window\" = ANY($2::text[]) \
           AND source = ANY($3::text[]) \
           AND changed_at_unix_millis >= $4 \
           AND changed_at_unix_millis <= $5 \
     ) \
     SELECT * FROM anchors UNION ALL SELECT * FROM ranged";

/// The DESC latest-fingerprint lookup (formerly `anchor_idx`) and the slim
/// aggregate (formerly `range_idx`/`anchor_idx`) must stay index-served once
/// the duplicates are gone: the primary key serves both orders.
async fn assert_checkpoint_queries_avoid_seq_scan(pool: &PgPool) -> Result<()> {
    let upstream_id = Uuid::from_u128(0x0134);
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await?;
    let latest_plan = sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
        "EXPLAIN (COSTS OFF) {LATEST_FINGERPRINT_SQL}"
    )))
    .bind(upstream_id)
    .bind("5h")
    .bind("header")
    .fetch_all(&mut *tx)
    .await?;
    let slim_plan = sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
        "EXPLAIN (COSTS OFF) {SLIM_AGGREGATE_SQL}"
    )))
    .bind(vec![upstream_id])
    .bind(vec!["5h".to_owned(), "7d".to_owned()])
    .bind(vec!["header".to_owned()])
    .bind(1_000_i64)
    .bind(604_800_000_i64)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;

    for (shape, plan) in [
        ("latest fingerprint", &latest_plan),
        ("slim aggregate", &slim_plan),
    ] {
        ensure!(
            !plan.iter().any(|line| line.contains("Seq Scan")),
            "{shape} must not fall back to a sequential scan: {plan:?}"
        );
        ensure!(
            plan.iter().any(|line| {
                line.contains("upstream_subscription_quota_checkpoints_v1_pkey")
                    || line.contains("upstream_subscription_quota_checkpoints_slim_cover_idx")
            }),
            "{shape} must use the primary key or the slim covering index: {plan:?}"
        );
    }
    Ok(())
}

async fn assert_relational_schema(pool: &PgPool, storage: &PostgresStorage) -> Result<()> {
    let has_payload = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'request_events_v1'
           AND column_name = 'payload')",
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        !has_payload,
        "0139 must remove the request event JSON column"
    );
    let event = RequestEvent {
        ts: 1_800_000_000,
        ts_ms: Some(1_800_000_000_123),
        request_id: "relational-request".to_owned(),
        event_id: Some("relational-event".to_owned()),
        status: 200,
        duration_ms: 42,
        iterations: Some(serde_json::json!({ "large": "x".repeat(20_000) })),
        internal_errors: Vec::new(),
        ..Default::default()
    };
    storage.append_request_event(&event).await?;
    let restored = storage
        .get_request_event("relational-event")
        .await?
        .context("relational event was not stored")?;
    ensure!(restored.request_id == event.request_id && restored.duration_ms == 42);
    ensure!(restored.iterations.is_none() && restored.routing_trace.is_none());
    ensure!(
        restored.cache_breakpoints.is_empty()
            && restored.cache_control_message_indices.is_empty()
            && restored.internal_errors.is_empty()
    );
    Ok(())
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("drop_redundant_indexes_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        match PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
        {
            Ok(pool) => Ok(Self {
                schema,
                admin_pool,
                pool,
            }),
            Err(error) => {
                sqlx::query(AssertSqlSafe(format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    quote_ident(&schema)
                )))
                .execute(&admin_pool)
                .await
                .context("drop schema after isolated pool creation failed")?;
                admin_pool.close().await;
                Err(error.into())
            }
        }
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        }),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
