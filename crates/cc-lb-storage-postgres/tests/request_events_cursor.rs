use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, Utc};

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    BackendKind, MetaStore, RequestEvent, RequestEventHistogramQuery, RequestEventListQuery,
    RequestEventStore, RequestEventStreamFilters, RequestEventUpstream,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

const FALLBACK_TS_SECS: u64 = 1_800_000_000;
const TIED_TS_SECS: u64 = FALLBACK_TS_SECS + 1;
const TIED_TS_MS: u64 = TIED_TS_SECS * 1_000 + 777;
const PAGE_LIMIT: usize = 2;

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
}

#[test]
fn request_event_history_uses_materialized_cursor_columns_and_index() {
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
            let result = run_regression(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop isolated request-event cursor schema")?;
            result
        })
        .expect("postgres request-event materialized cursor regression");
}

async fn run_regression(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize(BackendKind::Postgres).await?;

    assert_materialized_schema(pool).await?;
    let (expected_keys, fallback_event_key) = append_events_and_assert_columns(&storage).await?;
    assert_cursor_pages(&storage, &expected_keys).await?;
    assert_raw_payload_list_semantics(&storage).await?;
    assert_order_index_is_usable(pool).await?;
    assert_histogram_matches_list(&storage).await?;

    ensure!(
        fallback_event_key.starts_with("1800000000-legacy-live-"),
        "generated storage event ID was not used as the fallback list key: {fallback_event_key}"
    );
    Ok(())
}

async fn assert_materialized_schema(pool: &PgPool) -> Result<()> {
    let columns = sqlx::query_as::<_, (String, String, String, Option<String>)>(
        "SELECT column_name, data_type, is_nullable, column_default \
         FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'request_events_v1'",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|(name, data_type, nullable, default)| (name, (data_type, nullable, default)))
    .collect::<BTreeMap<_, _>>();

    let expected_columns = [
        ("list_ts_ms", "bigint", "NO", true),
        ("list_event_key", "text", "NO", true),
        ("list_upstream", "text", "YES", false),
        ("list_status", "integer", "YES", false),
    ];
    for (name, expected_type, expected_nullable, expects_default) in expected_columns {
        let Some((data_type, nullable, default)) = columns.get(name) else {
            anyhow::bail!("missing materialized request-event column {name}");
        };
        ensure!(
            data_type == expected_type,
            "column {name} has type {data_type}, expected {expected_type}"
        );
        ensure!(
            nullable == expected_nullable,
            "column {name} nullable={nullable}, expected {expected_nullable}"
        );
        ensure!(
            default.is_some() == expects_default,
            "column {name} default {default:?} did not match expectation"
        );
    }

    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes \
         WHERE schemaname = current_schema() AND tablename = 'request_events_v1'",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect::<BTreeSet<_>>();
    for index in [
        "request_events_v1_list_order_idx",
        "request_events_v1_principal_list_order_idx",
        "request_events_v1_lower_model_list_order_idx",
        "request_events_v1_upstream_list_order_idx",
    ] {
        ensure!(
            indexes.contains(index),
            "missing request-event index {index}"
        );
    }
    Ok(())
}

async fn append_events_and_assert_columns(
    storage: &PostgresStorage,
) -> Result<(Vec<String>, String)> {
    let upstream_id = Uuid::from_u128(7);
    let mut tied_keys = Vec::new();
    for index in 0..5u16 {
        let event_id = format!("cursor-event-{index:02}");
        storage
            .append_request_event(&RequestEvent {
                ts: TIED_TS_SECS,
                ts_ms: Some(TIED_TS_MS),
                request_id: format!("cursor-request-{index:02}"),
                event_id: Some(event_id.clone()),
                principal_id: Some("cursor-principal".to_owned()),
                upstream: Some(RequestEventUpstream::AnthropicDirect),
                upstream_id: Some(upstream_id),
                model: Some("cursor-model".to_owned()),
                status: 200 + index,
                duration_ms: u64::from(index),
                ..Default::default()
            })
            .await?;
        tied_keys.push(event_id);
    }

    let fallback_request_id = "cursor-request-fallback";
    storage
        .append_request_event(&RequestEvent {
            ts: FALLBACK_TS_SECS,
            request_id: fallback_request_id.to_owned(),
            principal_id: Some("cursor-principal".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            upstream_id: Some(upstream_id),
            model: Some("cursor-model".to_owned()),
            status: 503,
            duration_ms: 99,
            ..Default::default()
        })
        .await?;

    let tied_row = sqlx::query_as::<_, (i64, String, Option<String>, Option<i32>)>(
        "SELECT list_ts_ms, list_event_key, list_upstream, list_status \
         FROM request_events_v1 WHERE event_id = $1",
    )
    .bind(&tied_keys[0])
    .fetch_one(storage.pool())
    .await?;
    ensure!(
        tied_row.0 == TIED_TS_MS as i64,
        "new write did not store event.ts_ms"
    );
    ensure!(
        tied_row.1 == tied_keys[0],
        "new write did not store its event key"
    );
    ensure!(
        tied_row.2.as_deref() == Some("anthropic_direct"),
        "new write did not store canonical upstream: {:?}",
        tied_row.2
    );
    ensure!(
        tied_row.3 == Some(200),
        "new write did not store integer status"
    );

    let fallback_row = sqlx::query_as::<_, (String, i64, String, Option<String>, Option<i32>)>(
        "SELECT event_id, list_ts_ms, list_event_key, list_upstream, list_status \
         FROM request_events_v1 \
         WHERE convert_from(payload, 'UTF8')::jsonb ->> 'request_id' = $1",
    )
    .bind(fallback_request_id)
    .fetch_one(storage.pool())
    .await?;
    ensure!(
        fallback_row.1 == (FALLBACK_TS_SECS * 1_000) as i64,
        "new write did not fall back from seconds for list_ts_ms"
    );
    ensure!(
        fallback_row.2 == fallback_row.0,
        "list_event_key must equal the stored event_id, got event_id={} list_event_key={}",
        fallback_row.0,
        fallback_row.2
    );
    ensure!(
        fallback_row.3.as_deref() == Some("anthropic_direct") && fallback_row.4 == Some(503),
        "fallback write did not populate upstream/status materialization"
    );

    tied_keys.sort_by(|left, right| right.cmp(left));
    tied_keys.push(fallback_row.0.clone());
    Ok((tied_keys, fallback_row.0))
}

async fn assert_raw_payload_list_semantics(storage: &PostgresStorage) -> Result<()> {
    let event_id = "raw-payload-list-event";
    let principal_id = "raw-payload-list-principal";
    storage
        .append_request_event(&RequestEvent {
            ts: TIED_TS_SECS + 10,
            ts_ms: Some((TIED_TS_SECS + 10) * 1_000 + 123),
            request_id: "raw-payload-list-request".to_owned(),
            event_id: Some(event_id.to_owned()),
            source_kind: Some("proxy".to_owned()),
            principal_id: Some(principal_id.to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            status: 206,
            duration_ms: 41,
            auth_ms: Some(42),
            connection_reused: Some(true),
            cost_usd_micros: Some(43),
            ..Default::default()
        })
        .await?;

    let payload = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT payload FROM request_events_v1 WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(storage.pool())
    .await?;
    let mut payload = serde_json::from_slice::<serde_json::Value>(&payload)?;
    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .remove("request_id");
    sqlx::query("UPDATE request_events_v1 SET payload = $1 WHERE event_id = $2")
        .bind(serde_json::to_vec(&payload)?)
        .bind(event_id)
        .execute(storage.pool())
        .await?;

    let query = || RequestEventListQuery {
        since_unix_secs: TIED_TS_SECS,
        until_unix_secs: TIED_TS_SECS + 20,
        until_ts_ms: None,
        until_event_id: None,
        limit: 10,
        filters: RequestEventStreamFilters {
            principal_id: Some(principal_id.to_owned()),
            ..Default::default()
        },
        source_kind: Some("all".to_owned()),
    };

    let page = storage.list_request_events(&query()).await?;
    ensure!(page.len() == 1, "missing-request-id page was {page:?}");
    let item = &page[0];
    ensure!(
        item.request_id.is_empty()
            && item.duration_ms == 41
            && item.auth_ms == Some(42)
            && item.connection_reused == Some(true)
            && item.cost_usd_micros == Some(43),
        "raw payload list projection changed unexpectedly: {item:?}"
    );
    let detail = storage
        .get_request_event(event_id)
        .await?
        .context("missing-request-id detail")?;
    ensure!(
        detail.request_id.is_empty()
            && detail.duration_ms == item.duration_ms
            && detail.auth_ms == item.auth_ms
            && detail.connection_reused == item.connection_reused
            && detail.cost_usd_micros == item.cost_usd_micros,
        "list/detail payload projection diverged: item={item:?} detail={detail:?}"
    );

    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .remove("duration_ms");
    sqlx::query("UPDATE request_events_v1 SET payload = $1 WHERE event_id = $2")
        .bind(serde_json::to_vec(&payload)?)
        .bind(event_id)
        .execute(storage.pool())
        .await?;
    let page = storage.list_request_events(&query()).await?;
    ensure!(
        page.len() == 1 && page[0].duration_ms == 0,
        "missing duration did not default to zero: {page:?}"
    );

    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .insert("duration_ms".to_owned(), serde_json::json!(-1));
    sqlx::query("UPDATE request_events_v1 SET payload = $1 WHERE event_id = $2")
        .bind(serde_json::to_vec(&payload)?)
        .bind(event_id)
        .execute(storage.pool())
        .await?;
    ensure!(
        storage.list_request_events(&query()).await.is_err(),
        "negative duration unexpectedly decoded"
    );

    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .insert("duration_ms".to_owned(), serde_json::json!(41));
    sqlx::query(
        "UPDATE request_events_v1 SET payload = $1, list_status = NULL WHERE event_id = $2",
    )
    .bind(serde_json::to_vec(&payload)?)
    .bind(event_id)
    .execute(storage.pool())
    .await?;
    ensure!(
        matches!(
            storage.list_request_events(&query()).await,
            Err(cc_lb_storage_api::StorageError::Corrupted { .. })
        ),
        "missing materialized status was not reported as corrupted"
    );

    Ok(())
}

async fn assert_cursor_pages(storage: &PostgresStorage, expected_keys: &[String]) -> Result<()> {
    let mut cursor_ts_ms = None;
    let mut cursor_event_key = None;
    let mut actual_keys = Vec::new();
    let mut seen = HashSet::new();
    let mut previous: Option<(u64, String)> = None;

    loop {
        let page = storage
            .list_request_events(&RequestEventListQuery {
                since_unix_secs: FALLBACK_TS_SECS,
                until_unix_secs: TIED_TS_SECS + 1,
                until_ts_ms: cursor_ts_ms,
                until_event_id: cursor_event_key.clone(),
                limit: PAGE_LIMIT,
                filters: RequestEventStreamFilters::default(),
                source_kind: Some("all".to_owned()),
            })
            .await?;
        if page.is_empty() {
            break;
        }
        ensure!(page.len() <= PAGE_LIMIT, "page exceeded requested limit");

        for event in &page {
            let ts_ms = event
                .ts_ms
                .unwrap_or_else(|| event.ts.saturating_mul(1_000));
            let event_key = event
                .event_id
                .clone()
                .unwrap_or_else(|| event.request_id.clone());
            ensure!(
                seen.insert(event_key.clone()),
                "duplicate event key {event_key}"
            );
            if let Some((previous_ts_ms, previous_key)) = previous.as_ref() {
                ensure!(
                    (*previous_ts_ms, previous_key.as_str()) > (ts_ms, event_key.as_str()),
                    "page order was not strictly descending: previous=({previous_ts_ms}, {previous_key}) current=({ts_ms}, {event_key})"
                );
            }
            previous = Some((ts_ms, event_key.clone()));
            actual_keys.push(event_key);
        }

        let last = page.last().context("non-empty page has no last row")?;
        cursor_ts_ms = Some(last.ts_ms.unwrap_or_else(|| last.ts.saturating_mul(1_000)));
        cursor_event_key = Some(
            last.event_id
                .clone()
                .unwrap_or_else(|| last.request_id.clone()),
        );
    }

    ensure!(
        actual_keys == expected_keys,
        "cursor pages had gaps, duplicates, or wrong order: actual={actual_keys:?} expected={expected_keys:?}"
    );
    Ok(())
}

async fn assert_order_index_is_usable(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await?;
    let since = DateTime::<Utc>::from_timestamp(FALLBACK_TS_SECS as i64, 0)
        .context("invalid timestamp seconds")?;
    let plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF) \
         SELECT list_ts_ms, list_event_key \
         FROM request_events_v1 \
         WHERE ts >= $1 \
           AND (source_kind IS NULL OR source_kind <> 'renewal') \
         ORDER BY list_ts_ms DESC, list_event_key DESC \
         LIMIT $2",
    )
    .bind(since)
    .bind(PAGE_LIMIT as i64)
    .fetch_all(&mut *tx)
    .await?;
    let principal_plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF) \
         SELECT list_ts_ms, list_event_key \
         FROM request_events_v1 \
         WHERE ts >= $1 \
           AND principal_id = $2 \
           AND (source_kind IS NULL OR source_kind <> 'renewal') \
           AND (list_ts_ms < $3 \
                OR (list_ts_ms = $3 AND list_event_key < $4)) \
         ORDER BY list_ts_ms DESC, list_event_key DESC \
         LIMIT $5",
    )
    .bind(since)
    .bind("cursor-principal")
    .bind(TIED_TS_MS as i64)
    .bind("cursor-event-03")
    .bind(PAGE_LIMIT as i64)
    .fetch_all(&mut *tx)
    .await?;
    let principal_cost_plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF) \
         SELECT principal_id, ((list_ts_ms - $1) / $2)::bigint AS bucket_index, \
                convert_from(payload, 'UTF8')::jsonb \
         FROM ( \
             SELECT principal_id, list_ts_ms, payload \
             FROM request_events_v1 \
             WHERE list_ts_ms >= $1 AND list_ts_ms < $3 \
               AND ($4::uuid IS NULL OR upstream_id = $4) \
               AND principal_id = ANY($5::text[]) \
             UNION ALL \
             SELECT principal_id, list_ts_ms, payload \
             FROM request_events_v1 \
             WHERE list_ts_ms >= $1 AND list_ts_ms < $3 \
               AND ($4::uuid IS NULL OR upstream_id = $4) \
               AND (principal_id IS NULL OR principal_id !~* \
                   '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') \
               AND LEFT(REGEXP_REPLACE(COALESCE(NULLIF(BTRIM(principal_id COLLATE \"C\", \
                   U&'\\0009\\000A\\000B\\000C\\000D\\0020\\0085\\00A0\\1680\\2000\\2001\
                   \\2002\\2003\\2004\\2005\\2006\\2007\\2008\\2009\\200A\\2028\\2029\\202F\
                   \\205F\\3000'), ''), 'unknown'), '[^A-Za-z0-9_.:@-]', '_', 'g'), 64) \
                   = ANY($6::text[]) \
         ) selected \
         OFFSET 0",
    )
    .bind((FALLBACK_TS_SECS * 1_000) as i64)
    .bind(60_000_i64)
    .bind(((FALLBACK_TS_SECS + 120) * 1_000) as i64)
    .bind(Option::<Uuid>::None)
    .bind(vec![
        Uuid::from_u128(0x51).to_string(),
        Uuid::from_u128(0x52).to_string(),
    ])
    .bind(vec!["team_A".to_owned()])
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;

    ensure!(
        plan.iter()
            .any(|line| line.contains("request_events_v1_list_order_idx")),
        "materialized request-event order index was not structurally usable: {plan:?}"
    );
    ensure!(
        principal_plan
            .iter()
            .any(|line| line.contains("request_events_v1_principal_list_order_idx")),
        "principal request-event list index was not structurally usable: {principal_plan:?}"
    );
    ensure!(
        principal_cost_plan
            .iter()
            .any(|line| line.contains("request_events_v1_principal_list_order_idx")),
        "exact principal cost source could not use the principal-leading range index: {principal_cost_plan:?}"
    );
    ensure!(
        principal_cost_plan.iter().any(|line| {
            line.contains("request_events_v1_normalized_non_uuid_principal_cost_idx")
        }),
        "normalized fallback could not use the non-UUID partial index: {principal_cost_plan:?}"
    );
    Ok(())
}

/// Mirrors the SQLite histogram regression: the aggregate must count exactly
/// the rows the list returns for the same window, keep 4xx out of the error
/// lane, and zero-fill a continuous axis.
async fn assert_histogram_matches_list(storage: &PostgresStorage) -> Result<()> {
    let base = FALLBACK_TS_SECS + 10_000;
    let rows: [(u64, u16, Option<&str>, Option<&str>); 6] = [
        (base, 200, None, None),
        (base + 10, 429, None, None),
        (base + 20, 503, Some("upstream_5xx"), None),
        (base + 30, 200, Some("upstream_stream_error"), None),
        (base + 40, 200, None, Some("renewal")),
        (base + 180, 200, None, None),
    ];
    for (index, (ts, status, error_code, source_kind)) in rows.iter().enumerate() {
        storage
            .append_request_event(&RequestEvent {
                ts: *ts,
                ts_ms: Some(ts * 1_000 + 500),
                request_id: format!("req-pg-histogram-{index}"),
                event_id: Some(format!("0193a7b8-1234-7e2f-9012-pghist{index:06}")),
                principal_id: Some("principal-pg-histogram".to_owned()),
                model: Some("claude-sonnet-4-5".to_owned()),
                status: *status,
                error_code: error_code.map(str::to_owned),
                source_kind: source_kind.map(str::to_owned),
                duration_ms: 10,
                ..Default::default()
            })
            .await?;
    }

    let window_end = base + 180;
    let filters = RequestEventStreamFilters {
        principal_id: Some("principal-pg-histogram".to_owned()),
        ..Default::default()
    };
    let buckets = storage
        .request_event_histogram(&RequestEventHistogramQuery {
            since_unix_secs: base,
            until_unix_secs: window_end,
            bucket_ms: 60_000,
            bucket_count: 4,
            filters: filters.clone(),
            source_kind: None,
        })
        .await?;
    ensure!(buckets.len() == 4, "zero-filled buckets are never omitted");
    for (index, bucket) in buckets.iter().enumerate() {
        ensure!(
            bucket.bucket_start_unix_secs == base + (index as u64) * 60,
            "bucket axis is continuous: {bucket:?}"
        );
    }
    ensure!(
        buckets[0].error_count == 2,
        "error_count is 5xx plus 2xx semantic failures, never 4xx: {:?}",
        buckets[0]
    );
    ensure!(
        buckets[1].total_count == 0 && buckets[2].total_count == 0,
        "empty buckets are reported as zero"
    );
    ensure!(
        buckets[3].total_count == 1,
        "an event in the final second of the window is kept"
    );

    let listed = storage
        .list_request_events(&RequestEventListQuery {
            since_unix_secs: base,
            until_unix_secs: window_end,
            limit: 100,
            filters,
            ..Default::default()
        })
        .await?;
    let histogram_total: u64 = buckets.iter().map(|bucket| bucket.total_count).sum();
    ensure!(
        histogram_total == listed.len() as u64,
        "histogram total {histogram_total} must equal the list count {}",
        listed.len()
    );

    // The `ts` predicate alone cannot seek, so the aggregate carries a widened
    // `list_ts_ms` bound. Confirm the planner can actually use it as a range.
    let mut tx = storage.pool().begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await?;
    let plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF) \
         SELECT LEAST(GREATEST((list_ts_ms - $1) / $2, 0), $3)::bigint AS bucket_index, \
                COUNT(*) \
         FROM request_events_v1 \
         WHERE ts >= $4 AND ts <= $5 AND list_ts_ms >= $6 AND list_ts_ms < $7 \
         GROUP BY bucket_index",
    )
    .bind((base * 1_000) as i64)
    .bind(60_000_i64)
    .bind(3_i64)
    .bind(DateTime::<Utc>::from_timestamp(base as i64, 0).context("invalid since")?)
    .bind(DateTime::<Utc>::from_timestamp(window_end as i64, 0).context("invalid until")?)
    .bind((base * 1_000 - 60_000) as i64)
    .bind(((window_end + 1) * 1_000 + 60_000) as i64)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    ensure!(
        plan.iter().any(|line| line.contains("Index")),
        "histogram aggregate must be able to use a list_ts_ms index: {plan:?}"
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
        let schema = format!("request_events_cursor_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        match schema_pool(url, &schema).await {
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
                Err(error)
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

async fn schema_pool(url: &str, schema: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
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
