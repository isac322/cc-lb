//! P1-2 gate for migration 0137 and the kept `source_kind` index.
//!
//! `request_events_v1_lower_model_list_order_idx` (0105) and
//! `request_events_v1_source_kind_list_order_idx` (0100) are dropped only if
//! the planner never chooses them for a selective request-log filter. This
//! test decides that on synthetic, deployment-independent data: it seeds a
//! model/source mix, recreates each index with its original definition,
//! `EXPLAIN`s the production list and histogram SQL (built by the adapter's
//! own query builders, filters via `model_filter_like_patterns`) and records
//! whether any plan node references the index.
//!
//! Outcome A (planner never uses the index): the drop migration ships, the
//! constant below is `true`, and the test also proves the remaining plans
//! still avoid a sequential scan once the index is gone. This is the result
//! for `lower_model` (0137).
//! Outcome B (planner uses it for a selective case): no drop migration, the
//! constant is `false`, and the test keeps guarding that the index is present
//! and chosen. This is the result for `source_kind`: the planner chooses it
//! for list and histogram `source_kind` filters, so it stays on both backends.

use std::{collections::BTreeSet, fmt::Write as _, str::FromStr, sync::Arc};

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    MetaStore, RequestEventHistogramQuery, RequestEventListQuery, RequestEventStreamFilters,
};
use cc_lb_storage_postgres::{
    PostgresStorage,
    adapter::request_events::{explain_request_event_histogram, explain_request_event_list},
};
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

/// Outcome of the gate for 0137: `true` = Outcome A (index dropped).
const LOWER_MODEL_INDEX_DROPPED: bool = true;
/// Outcome of the gate for the `source_kind` index: `false` = Outcome B
/// (index kept on Postgres and SQLite).
const SOURCE_KIND_INDEX_DROPPED: bool = false;

const LOWER_MODEL_INDEX: &str = "request_events_v1_lower_model_list_order_idx";
const SOURCE_KIND_INDEX: &str = "request_events_v1_source_kind_list_order_idx";
/// Original definitions from 0105 and 0100.
const LOWER_MODEL_INDEX_DDL: &str = "CREATE INDEX IF NOT EXISTS \
     request_events_v1_lower_model_list_order_idx ON request_events_v1 \
     (lower(model) text_pattern_ops, list_ts_ms DESC, list_event_key DESC)";
const SOURCE_KIND_INDEX_DDL: &str = "CREATE INDEX IF NOT EXISTS \
     request_events_v1_source_kind_list_order_idx ON request_events_v1 \
     (source_kind, list_ts_ms DESC, list_event_key DESC)";

const ROWS: i64 = 200_000;
const SEED_BATCH_ROWS: i64 = 10_000;
const DAY_MS: u64 = 86_400_000;
const SPAN_DAYS: u64 = 90;
const BASE_MS: u64 = 1_790_000_000_000;

/// 200k rows, 211 distinct models over a uniform 90-day `list_ts_ms` span:
/// - 40% `claude-*` across eight families (`opus` cores ≈ 2%),
/// - 60% non-claude: `gpt-4o-*`, `llama-3-*`, `mistral-*`, `model-xxxx`,
///   `gemini-*` (≈ 1%) and exactly one `gpt-4o-mini-2024-07-18` (≈ 0.1%),
/// - `source_kind`: proxy ≈ 98%, renewal ≈ 1%, warmup ≈ 1%.
const SEED_SQL: &str = "\
INSERT INTO request_events_v1 \
    (ts, event_id, list_ts_ms, list_event_key, list_status, model, source_kind, principal_id) \
SELECT to_timestamp(t.ts_ms / 1000.0), 'gate-' || g, t.ts_ms, 'gate-' || lpad(g::text, 7, '0'), \
       200, m.model, s.kind, 'gate-principal-' || (g % 50) \
FROM generate_series($4::bigint, $5::bigint) AS g \
CROSS JOIN LATERAL (SELECT $2::bigint + (g * $3::bigint / $1::bigint) AS ts_ms) AS t \
CROSS JOIN LATERAL (SELECT CASE \
    WHEN g % 1000 = 7 THEN 'gpt-4o-mini-2024-07-18' \
    WHEN g % 1000 < 20 THEN (ARRAY['claude-opus-4-1', 'claude-opus-4', 'claude-3-opus'])\
        [(1 + (g / 1000) % 3)::int] || '-2025' || lpad(((g / 3000) % 10)::text, 4, '0') \
    WHEN g % 1000 < 400 THEN (ARRAY['claude-sonnet-4-5', 'claude-3-5-haiku', 'claude-3-7-sonnet', \
        'claude-sonnet-4', 'claude-3-5-sonnet'])\
        [(1 + (g / 1000) % 5)::int] || '-2025' || lpad(((g / 5000) % 10)::text, 4, '0') \
    WHEN g % 1000 < 410 THEN 'gemini-' || ((g / 1000) % 10) || '-flash' \
    WHEN g % 1000 < 560 THEN 'gpt-4o-' || lpad(((g / 1000) % 30)::text, 2, '0') \
    WHEN g % 1000 < 710 THEN 'llama-3-' || ((g / 1000) % 30) || 'b' \
    WHEN g % 1000 < 850 THEN 'mistral-' || ((g / 1000) % 30) \
    ELSE 'model-' || lpad(((g / 1000) % 30)::text, 4, '0') \
    END AS model) AS m \
CROSS JOIN LATERAL (SELECT CASE (g * 7919) % 100 \
    WHEN 0 THEN 'renewal' WHEN 1 THEN 'warmup' ELSE 'proxy' END AS kind) AS s";

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
}

#[test]
fn request_event_filter_indexes_match_p1_2_gate_outcome() {
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
            let result = run_gate(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop isolated filter-index plan schema")?;
            result
        })
        .expect("postgres request-event filter index P1-2 gate");
}

#[derive(Clone, Copy)]
enum Shape {
    List,
    Histogram { window_days: u64 },
}

struct Case {
    name: String,
    shape: Shape,
    model: Option<&'static str>,
    source_kind: Option<&'static str>,
}

impl Case {
    /// Whether the time window alone is selective enough that a sequential
    /// scan would be a regression (LIMITed list pages and short histograms).
    fn expects_index_access(&self) -> bool {
        match self.shape {
            Shape::List => true,
            Shape::Histogram { window_days } => window_days <= 1,
        }
    }
}

fn gate_cases() -> Vec<Case> {
    let mut cases = Vec::new();
    let shapes = [
        ("list", Shape::List),
        (
            "histogram-90d",
            Shape::Histogram {
                window_days: SPAN_DAYS,
            },
        ),
        ("histogram-1d", Shape::Histogram { window_days: 1 }),
    ];
    for (shape_name, shape) in shapes {
        for needle in ["gpt-4o-mini-2024", "gemini", "opus"] {
            cases.push(Case {
                name: format!("{shape_name} model={needle}"),
                shape,
                model: Some(needle),
                source_kind: None,
            });
        }
        for source_kind in ["renewal", "warmup"] {
            cases.push(Case {
                name: format!("{shape_name} source_kind={source_kind}"),
                shape,
                model: None,
                source_kind: Some(source_kind),
            });
        }
    }
    cases
}

async fn explain_case(storage: &PostgresStorage, case: &Case) -> Result<Value> {
    let filters = RequestEventStreamFilters {
        model: case.model.map(str::to_owned),
        ..Default::default()
    };
    let source_kind = case.source_kind.map(str::to_owned);
    let span_end_secs = (BASE_MS + SPAN_DAYS * DAY_MS) / 1_000;
    let plan = match case.shape {
        Shape::List => {
            explain_request_event_list(
                storage,
                &RequestEventListQuery {
                    since_unix_secs: BASE_MS / 1_000,
                    until_unix_secs: span_end_secs,
                    until_ts_ms: None,
                    until_event_id: None,
                    limit: 50,
                    filters,
                    source_kind,
                },
            )
            .await?
        }
        Shape::Histogram { window_days } => {
            let since_unix_secs = span_end_secs - window_days * DAY_MS / 1_000;
            explain_request_event_histogram(
                storage,
                &RequestEventHistogramQuery {
                    since_unix_secs,
                    until_unix_secs: span_end_secs,
                    bucket_ms: window_days * DAY_MS / 60,
                    bucket_count: 60,
                    filters,
                    source_kind,
                },
            )
            .await?
        }
    };
    plan.with_context(|| format!("{} produced no statement to explain", case.name))
}

#[derive(Default)]
struct PlanSummary {
    indexes: BTreeSet<String>,
    seq_scan_on_request_events: bool,
}

fn summarize(plan: &Value) -> PlanSummary {
    let mut summary = PlanSummary::default();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        match node {
            Value::Array(items) => stack.extend(items),
            Value::Object(map) => {
                if let Some(Value::String(index)) = map.get("Index Name") {
                    summary.indexes.insert(index.clone());
                }
                if map.get("Node Type").and_then(Value::as_str) == Some("Seq Scan")
                    && map.get("Relation Name").and_then(Value::as_str) == Some("request_events_v1")
                {
                    summary.seq_scan_on_request_events = true;
                }
                stack.extend(map.values());
            }
            _ => {}
        }
    }
    summary
}

async fn request_event_indexes(pool: &PgPool) -> Result<BTreeSet<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes \
         WHERE schemaname = current_schema() AND tablename = 'request_events_v1'",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

async fn run_gate(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await?;

    let migrated = request_event_indexes(pool).await?;
    for (index, dropped) in [
        (LOWER_MODEL_INDEX, LOWER_MODEL_INDEX_DROPPED),
        (SOURCE_KIND_INDEX, SOURCE_KIND_INDEX_DROPPED),
    ] {
        ensure!(
            migrated.contains(index) != dropped,
            "migrated schema {} {index}, but the gate outcome constant says dropped={dropped}",
            if migrated.contains(index) {
                "has"
            } else {
                "lacks"
            }
        );
    }

    // Autocommitted batches: the xmin horizon is cluster-wide, so one long
    // 200k-row transaction would hold back xmin-guarded reads (tail cursor,
    // delta, usage rollup) in every concurrently running test process.
    for batch_start in (1..=ROWS).step_by(usize::try_from(SEED_BATCH_ROWS)?) {
        let batch_end = (batch_start + SEED_BATCH_ROWS - 1).min(ROWS);
        sqlx::query(SEED_SQL)
            .bind(ROWS)
            .bind(i64::try_from(BASE_MS)?)
            .bind(i64::try_from(SPAN_DAYS * DAY_MS)?)
            .bind(batch_start)
            .bind(batch_end)
            .execute(pool)
            .await
            .with_context(|| {
                format!("seed synthetic request events {batch_start}..={batch_end}")
            })?;
    }
    // (Index builds below are each one short transaction on this schema's
    // private table; nothing here touches another schema.)
    let distinct_models =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(DISTINCT model) FROM request_events_v1")
            .fetch_one(pool)
            .await?;
    ensure!(
        distinct_models >= 200,
        "synthetic data needs >= 200 models, got {distinct_models}"
    );

    // Phase 1: both indexes present with their original definitions.
    for ddl in [LOWER_MODEL_INDEX_DDL, SOURCE_KIND_INDEX_DDL] {
        sqlx::query(ddl).execute(pool).await?;
    }
    sqlx::query("ANALYZE request_events_v1")
        .execute(pool)
        .await?;

    let cases = gate_cases();
    let mut report = String::new();
    let mut lower_model_users = Vec::new();
    let mut source_kind_users = Vec::new();
    for case in &cases {
        let summary = summarize(&explain_case(&storage, case).await?);
        writeln!(
            report,
            "{}: indexes={:?} seq_scan={}",
            case.name, summary.indexes, summary.seq_scan_on_request_events
        )?;
        if summary.indexes.contains(LOWER_MODEL_INDEX) {
            lower_model_users.push(case.name.clone());
        }
        if summary.indexes.contains(SOURCE_KIND_INDEX) {
            source_kind_users.push(case.name.clone());
        }
    }
    eprintln!("P1-2 gate plans with both indexes present:\n{report}");

    for (index, dropped, users, drop_files) in [
        (
            LOWER_MODEL_INDEX,
            LOWER_MODEL_INDEX_DROPPED,
            &lower_model_users,
            "postgres 0137",
        ),
        (
            SOURCE_KIND_INDEX,
            SOURCE_KIND_INDEX_DROPPED,
            &source_kind_users,
            "a new highest-numbered postgres + sqlite drop migration",
        ),
    ] {
        if dropped {
            ensure!(
                users.is_empty(),
                "Outcome B for {index}: the planner chose it for {users:?}. Delete {drop_files} \
                 and flip its *_DROPPED constant.\n{report}"
            );
        } else {
            ensure!(
                !users.is_empty(),
                "Outcome A for {index}: the planner never chose it. Add (or restore) {drop_files} \
                 and flip its *_DROPPED constant.\n{report}"
            );
        }
    }

    // Phase 2 (Outcome A only): with the dropped indexes gone again, the
    // time-selective plans still use an index instead of a sequential scan.
    let mut dropped_any = false;
    for (index, dropped) in [
        (LOWER_MODEL_INDEX, LOWER_MODEL_INDEX_DROPPED),
        (SOURCE_KIND_INDEX, SOURCE_KIND_INDEX_DROPPED),
    ] {
        if dropped {
            sqlx::query(AssertSqlSafe(format!("DROP INDEX {index}")))
                .execute(pool)
                .await?;
            dropped_any = true;
        }
    }
    if !dropped_any {
        return Ok(());
    }
    let mut after_report = String::new();
    let mut regressions = Vec::new();
    for case in cases.iter().filter(|case| case.expects_index_access()) {
        let summary = summarize(&explain_case(&storage, case).await?);
        writeln!(
            after_report,
            "{}: indexes={:?} seq_scan={}",
            case.name, summary.indexes, summary.seq_scan_on_request_events
        )?;
        if summary.seq_scan_on_request_events || summary.indexes.is_empty() {
            regressions.push(case.name.clone());
        }
    }
    eprintln!("P1-2 gate plans after the drops:\n{after_report}");
    ensure!(
        regressions.is_empty(),
        "time-selective request-log plans fell back to a sequential scan after the drop: \
         {regressions:?}\n{after_report}"
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
        let schema = format!("request_event_filter_plans_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        match PgPoolOptions::new()
            .max_connections(2)
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
