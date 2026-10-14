//! Migrations 0134–0135: the redundant `request_events_v1_thread_ts` and the
//! checkpoint `range_idx`/`anchor_idx` duplicates are gone and the queries they
//! used to serve still avoid a sequential scan. Migrations leave
//! `request_events_v1.payload` on the default `EXTENDED` storage; the operator
//! step `SET STORAGE MAIN` (docs/runbook/request-diagnostics.md, step 4) keeps
//! mid-sized payloads inline once applied.

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
fn redundant_indexes_are_dropped_and_payload_storage_main_is_operator_step() {
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
        .expect("postgres redundant-index and payload storage regression");
}

async fn run(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await?;

    assert_index_set(pool).await?;
    assert_checkpoint_queries_avoid_seq_scan(pool).await?;
    assert_payload_storage_operator_step(pool, &storage).await?;
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

/// Deterministic, pglz-incompressible ASCII: a 64-symbol alphabet driven by
/// xorshift leaves pglz no 25% saving, so the value is stored uncompressed.
fn incompressible_ascii(len: usize, mut state: u64) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let symbol = ALPHABET.get((state % 64) as usize).copied().unwrap_or(b'A');
        out.push(char::from(symbol));
    }
    out
}

async fn append_with_blob(
    storage: &PostgresStorage,
    event_id: &str,
    offset_ms: u64,
    blob: String,
) -> Result<()> {
    let ts_ms = 1_800_000_000_000 + offset_ms;
    storage
        .append_request_event(&RequestEvent {
            ts: ts_ms / 1_000,
            ts_ms: Some(ts_ms),
            request_id: format!("{event_id}-request"),
            event_id: Some(event_id.to_owned()),
            status: 200,
            // `iterations` lives only in `payload`, so the blob grows exactly
            // the column under test.
            iterations: Some(serde_json::json!({ "blob": blob })),
            ..Default::default()
        })
        .await?;
    Ok(())
}

async fn payload_storage(pool: &PgPool, event_id: &str) -> Result<(i32, bool)> {
    let (bytes, inline) = sqlx::query_as::<_, (i32, bool)>(
        "SELECT octet_length(payload), pg_column_toast_chunk_id(payload) IS NULL \
         FROM request_events_v1 WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    Ok((bytes, inline))
}

async fn payload_attstorage(pool: &PgPool) -> Result<String> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT attstorage::text FROM pg_attribute \
         WHERE attrelid = 'request_events_v1'::regclass AND attname = 'payload'",
    )
    .fetch_one(pool)
    .await?)
}

/// QA 21: migrations must leave `payload` on `EXTENDED`, because `STORAGE MAIN`
/// while the strip is off keeps full 2–8 KB payloads inline and bloats the
/// heap. A ~5 KB incompressible payload is moved to TOAST under `EXTENDED`.
/// After the runbook's operator step `SET STORAGE MAIN`, a ~2 KB payload and a
/// ~5 KB incompressible payload stay in the heap, while a 20 KB incompressible
/// payload that cannot fit a heap page is still externalized.
async fn assert_payload_storage_operator_step(
    pool: &PgPool,
    storage: &PostgresStorage,
) -> Result<()> {
    let attstorage = payload_attstorage(pool).await?;
    ensure!(
        attstorage == "x",
        "migrations must leave request_events_v1.payload on STORAGE EXTENDED, \
         got attstorage={attstorage}"
    );
    append_with_blob(
        storage,
        "toast-5kb-extended",
        0,
        incompressible_ascii(5_000, 0x0136),
    )
    .await?;
    let (extended_bytes, extended_inline) = payload_storage(pool, "toast-5kb-extended").await?;
    ensure!(
        extended_bytes >= 5_000,
        "5 KB payload is {extended_bytes} bytes"
    );
    ensure!(
        !extended_inline,
        "a ~5 KB incompressible payload must move to TOAST under STORAGE EXTENDED"
    );

    // The operator step from docs/runbook/request-diagnostics.md.
    sqlx::query("ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE MAIN")
        .execute(pool)
        .await?;
    let attstorage = payload_attstorage(pool).await?;
    ensure!(
        attstorage == "m",
        "the operator step must set STORAGE MAIN, got attstorage={attstorage}"
    );

    append_with_blob(storage, "toast-2kb", 1, "x".repeat(1_800)).await?;
    append_with_blob(storage, "toast-5kb", 2, incompressible_ascii(5_000, 0x0135)).await?;
    append_with_blob(
        storage,
        "toast-20kb",
        3,
        incompressible_ascii(20_000, 0x2135),
    )
    .await?;

    let (small_bytes, small_inline) = payload_storage(pool, "toast-2kb").await?;
    ensure!(small_bytes >= 1_800, "2 KB payload is {small_bytes} bytes");
    ensure!(small_inline, "a ~2 KB payload must stay inline");

    let (mid_bytes, mid_inline) = payload_storage(pool, "toast-5kb").await?;
    ensure!(mid_bytes >= 5_000, "5 KB payload is {mid_bytes} bytes");
    ensure!(
        mid_inline,
        "a ~5 KB incompressible payload must stay inline under STORAGE MAIN"
    );

    let (large_bytes, large_inline) = payload_storage(pool, "toast-20kb").await?;
    ensure!(
        large_bytes >= 20_000,
        "20 KB payload is {large_bytes} bytes"
    );
    ensure!(
        !large_inline,
        "a 20 KB incompressible payload exceeds a heap page and must still be externalized"
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
