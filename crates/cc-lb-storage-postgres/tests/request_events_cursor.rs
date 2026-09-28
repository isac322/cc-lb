use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, Utc};

use anyhow::{Context, Result, ensure};

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventHistogramQuery, RequestEventKind, RequestEventListQuery,
    RequestEventStore, RequestEventStreamFilters,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

const BASE_TS_SECS: u64 = 1_800_000_000;
const TIED_TS_SECS: u64 = BASE_TS_SECS + 1;
const TIED_TS_MS: u64 = TIED_TS_SECS * 1_000 + 777;
const CURRENT_REQUEST_EVENT_CURSOR_SQL: &str =
    include_str!("../src/adapter/current_request_event_cursor.sql");
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

#[test]
fn request_event_principal_cost_shapes_use_covering_indexes() {
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
            let result = assert_principal_cost_shapes_use_covering_indexes(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop isolated principal-cost plan schema")?;
            result
        })
        .expect("postgres principal-cost covering-index regression");
}

#[test]
fn request_event_event_kind_filters_match_effective_kind() {
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
            let result = assert_event_kind_filters(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop isolated event-kind schema")?;
            result
        })
        .expect("postgres request-event event_kind regression");
}

const UUID_PRINCIPAL_COST_PLAN_SQL: &str =
    include_str!("../src/adapter/request_event_principal_cost_uuid.sql");
const FILTERED_UUID_PRINCIPAL_COST_PLAN_SQL: &str =
    include_str!("../src/adapter/request_event_principal_cost_uuid_filtered.sql");
const NULL_PRINCIPAL_COST_PLAN_SQL: &str =
    include_str!("../src/adapter/request_event_principal_cost_null.sql");
const FILTERED_NULL_PRINCIPAL_COST_PLAN_SQL: &str =
    include_str!("../src/adapter/request_event_principal_cost_null_filtered.sql");

async fn explain_principal_cost_shape(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &'static str,
    range_start_ms: i64,
    range_end_ms: i64,
    upstream_id: Option<Uuid>,
    principal_keys: Option<&Vec<String>>,
) -> Result<Vec<String>> {
    let mut query = sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF) {sql}"
    )))
    .bind(range_start_ms)
    .bind(60_000_i64)
    .bind(range_end_ms);
    if let Some(upstream_id) = upstream_id {
        query = query.bind(upstream_id);
    }
    if let Some(principal_keys) = principal_keys {
        query = query.bind(principal_keys);
    }
    Ok(query.fetch_all(&mut **tx).await?)
}

async fn assert_principal_cost_shapes_use_covering_indexes(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await?;
    let upstream_id = Uuid::from_u128(0x51);
    let uuid_principal = upstream_id.to_string();
    for index in 0..128_u64 {
        for (suffix, principal_id) in [("uuid", Some(uuid_principal.as_str())), ("null", None)] {
            storage
                .append_request_event(&RequestEvent {
                    ts: BASE_TS_SECS + index % 120,
                    ts_ms: Some((BASE_TS_SECS + index % 120) * 1_000),
                    request_id: format!("principal-plan-{suffix}-request-{index}"),
                    event_id: Some(format!("principal-plan-{suffix}-event-{index}")),
                    principal_id: principal_id.map(ToOwned::to_owned),
                    upstream_id: Some(upstream_id),
                    status: 200,
                    cost_usd_micros: Some(11),
                    cost_input_micros: Some(2),
                    cost_output_micros: Some(3),
                    ..Default::default()
                })
                .await?;
        }
    }
    sqlx::query("VACUUM (ANALYZE) request_events_v1")
        .execute(pool)
        .await?;

    let range_start_ms = (BASE_TS_SECS * 1_000) as i64;
    let range_end_ms = ((BASE_TS_SECS + 120) * 1_000) as i64;
    let uuid_keys = vec![uuid_principal];
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *tx)
        .await?;

    let plans = [
        (
            "unfiltered UUID",
            explain_principal_cost_shape(
                &mut tx,
                UUID_PRINCIPAL_COST_PLAN_SQL,
                range_start_ms,
                range_end_ms,
                None,
                Some(&uuid_keys),
            )
            .await?,
            &[
                "request_events_v1_principal_list_order_idx",
                "request_events_v1_upstream_id_idx",
            ][..],
        ),
        (
            "filtered UUID",
            explain_principal_cost_shape(
                &mut tx,
                FILTERED_UUID_PRINCIPAL_COST_PLAN_SQL,
                range_start_ms,
                range_end_ms,
                Some(upstream_id),
                Some(&uuid_keys),
            )
            .await?,
            &["request_events_v1_upstream_id_idx"][..],
        ),
        (
            "unfiltered NULL principal",
            explain_principal_cost_shape(
                &mut tx,
                NULL_PRINCIPAL_COST_PLAN_SQL,
                range_start_ms,
                range_end_ms,
                None,
                None,
            )
            .await?,
            &[
                "request_events_v1_principal_list_order_idx",
                "request_events_v1_upstream_id_idx",
            ][..],
        ),
        (
            "filtered NULL principal",
            explain_principal_cost_shape(
                &mut tx,
                FILTERED_NULL_PRINCIPAL_COST_PLAN_SQL,
                range_start_ms,
                range_end_ms,
                Some(upstream_id),
                None,
            )
            .await?,
            &["request_events_v1_upstream_id_idx"][..],
        ),
    ];
    tx.rollback().await?;
    // Measure heap fetches only with a scratch EXPLAIN after an isolated VACUUM.
    // Parallel tests can hold snapshots that keep new pages out of the all-visible map.

    for (shape, plan, covering_indexes) in plans {
        ensure!(
            covering_indexes.iter().any(|index| {
                plan.iter()
                    .any(|line| line.contains(&format!("Index Only Scan using {index}")))
            }),
            "{shape} principal-cost shape did not use a covering index \
             from {covering_indexes:?}: {plan:?}"
        );
    }
    Ok(())
}

async fn run_regression(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await?;

    assert_materialized_schema(pool).await?;
    let expected_keys = append_events_and_assert_columns(&storage).await?;
    assert_cursor_pages(&storage, &expected_keys).await?;
    assert_raw_payload_list_semantics(&storage).await?;
    assert_order_index_is_usable(pool).await?;
    assert_histogram_matches_list(&storage).await?;

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
        ("list_status", "integer", "YES", false),
        ("event_kind", "text", "YES", false),
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

async fn append_events_and_assert_columns(storage: &PostgresStorage) -> Result<Vec<String>> {
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
                upstream_id: Some(upstream_id),
                model: Some("cursor-model".to_owned()),
                status: 200 + index,
                duration_ms: u64::from(index),
                ..Default::default()
            })
            .await?;
        tied_keys.push(event_id);
    }

    let tied_row = sqlx::query_as::<_, (i64, String, Option<i32>)>(
        "SELECT list_ts_ms, list_event_key, list_status \
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
        tied_row.2 == Some(200),
        "new write did not store integer status"
    );

    tied_keys.sort_by(|left, right| right.cmp(left));
    Ok(tied_keys)
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
            status: 206,
            duration_ms: 41,
            auth_ms: Some(42),
            connection_reused: Some(true),
            cost_usd_micros: Some(43),
            request_body_first_chunk_ms: Some(0.125),
            request_body_receive_ms: None,
            request_body_wait_ms: Some(0.0),
            request_body_process_ms: Some(0.25),
            request_body_chunk_count: Some(0),
            response_body_wait_ms: Some(0.5),
            response_body_process_ms: Some(0.0),
            response_body_downstream_poll_gap_ms: Some(0.75),
            retry_overhead_ms: Some(1.25),
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
            && item.cost_usd_micros == Some(43)
            && item.request_body_first_chunk_ms == Some(0.125)
            && item.request_body_receive_ms.is_none()
            && item.request_body_wait_ms == Some(0.0)
            && item.request_body_process_ms == Some(0.25)
            && item.request_body_chunk_count == Some(0)
            && item.response_body_wait_ms == Some(0.5)
            && item.response_body_process_ms == Some(0.0)
            && item.response_body_downstream_poll_gap_ms == Some(0.75)
            && item.retry_overhead_ms == Some(1.25),
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
            && detail.cost_usd_micros == item.cost_usd_micros
            && detail.request_body_first_chunk_ms == item.request_body_first_chunk_ms
            && detail.request_body_receive_ms == item.request_body_receive_ms
            && detail.request_body_wait_ms == item.request_body_wait_ms
            && detail.request_body_process_ms == item.request_body_process_ms
            && detail.request_body_chunk_count == item.request_body_chunk_count
            && detail.response_body_wait_ms == item.response_body_wait_ms
            && detail.response_body_process_ms == item.response_body_process_ms
            && detail.response_body_downstream_poll_gap_ms
                == item.response_body_downstream_poll_gap_ms
            && detail.retry_overhead_ms == item.retry_overhead_ms,
        "list/detail payload projection diverged: item={item:?} detail={detail:?}"
    );
    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .remove("request_body_chunk_count");
    sqlx::query("UPDATE request_events_v1 SET payload = $1 WHERE event_id = $2")
        .bind(serde_json::to_vec(&payload)?)
        .bind(event_id)
        .execute(storage.pool())
        .await?;
    let page = storage.list_request_events(&query()).await?;
    ensure!(
        page.len() == 1
            && page[0].request_body_chunk_count.is_none()
            && page[0].request_body_wait_ms == Some(0.0),
        "missing chunk count was fabricated or zero timing was lost: {page:?}"
    );
    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .insert("request_body_chunk_count".to_owned(), serde_json::json!(-1));
    sqlx::query("UPDATE request_events_v1 SET payload = $1 WHERE event_id = $2")
        .bind(serde_json::to_vec(&payload)?)
        .bind(event_id)
        .execute(storage.pool())
        .await?;
    ensure!(
        storage.list_request_events(&query()).await.is_err(),
        "negative request body chunk count unexpectedly decoded"
    );
    payload
        .as_object_mut()
        .context("request event payload must be an object")?
        .remove("request_body_chunk_count");

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
                since_unix_secs: BASE_TS_SECS,
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
    let since = DateTime::<Utc>::from_timestamp(BASE_TS_SECS as i64, 0)
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
    sqlx::query("SET LOCAL enable_seqscan = on")
        .execute(&mut *tx)
        .await?;
    let cursor_plan = sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
        "EXPLAIN (COSTS OFF) {CURRENT_REQUEST_EVENT_CURSOR_SQL}"
    )))
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
        cursor_plan
            .iter()
            .any(|line| { line.contains("Index Scan Backward using request_events_v1_pkey") }),
        "current request-event cursor must stop at the first visible row in descending seq order: {cursor_plan:?}"
    );
    ensure!(
        cursor_plan.iter().all(|line| !line.contains("Aggregate")),
        "current request-event cursor must not aggregate the full table: {cursor_plan:?}"
    );
    Ok(())
}

/// Mirrors the SQLite histogram regression: the aggregate must count exactly
/// the rows the list returns for the same window, keep 4xx out of the error
/// lane, and zero-fill a continuous axis.
async fn assert_histogram_matches_list(storage: &PostgresStorage) -> Result<()> {
    let base = BASE_TS_SECS + 10_000;
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

/// Mirrors the SQLite event-kind regression: `source_kind = 'renewal'` wins
/// over the materialized `event_kind`, historical NULL rows classify as
/// `unclassified`, an explicit `event_kind` filter bypasses only the implicit
/// renewal exclusion, and an explicit `source_kind` stays conjunctive. New
/// writes materialize `event_kind`; rows written without one stay NULL and
/// are never backfilled.
async fn assert_event_kind_filters(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await?;

    let base = BASE_TS_SECS + 20_000;
    let principal = "principal-pg-event-kind";
    let rows: [(&str, Option<RequestEventKind>, Option<&str>); 5] = [
        ("ek-messages", Some(RequestEventKind::Messages), None),
        ("ek-count-tokens", Some(RequestEventKind::CountTokens), None),
        ("ek-historical", None, None),
        ("ek-renewal", None, Some("renewal")),
        (
            "ek-renewal-typed",
            Some(RequestEventKind::Files),
            Some("renewal"),
        ),
    ];
    for (index, (request_id, event_kind, source_kind)) in rows.iter().enumerate() {
        storage
            .append_request_event(&RequestEvent {
                ts: base + index as u64,
                ts_ms: Some((base + index as u64) * 1_000 + 500),
                request_id: (*request_id).to_owned(),
                event_id: Some(format!("0193a7b8-1234-7e2f-9012-pgek{index:07}")),
                principal_id: Some(principal.to_owned()),
                model: Some("claude-sonnet-4-5".to_owned()),
                status: 200,
                duration_ms: 10,
                event_kind: *event_kind,
                source_kind: source_kind.map(str::to_owned),
                ..Default::default()
            })
            .await?;
    }

    let materialized = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT event_id, event_kind FROM request_events_v1 \
         WHERE principal_id = $1",
    )
    .bind(principal)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect::<BTreeMap<_, _>>();

    // Borrow so the `async move` closures below capture a Copy reference and
    // stay callable for every assertion.
    let storage = &storage;
    ensure!(
        materialized.len() == 5,
        "expected five materialized event rows: {materialized:?}"
    );
    for (index, (request_id, event_kind, _)) in rows.iter().enumerate() {
        let event_id = format!("0193a7b8-1234-7e2f-9012-pgek{index:07}");
        ensure!(
            materialized.get(&event_id).map(Option::as_deref)
                == Some(event_kind.map(RequestEventKind::as_str)),
            "row {request_id} materialized event_kind {:?}, expected {event_kind:?}",
            materialized.get(&event_id)
        );
    }

    let list_ids = |event_kind: Option<RequestEventKind>, source_kind: Option<&str>| {
        // Own the filter before the async block so the returned future does
        // not borrow the per-call `&str` argument.
        let source_kind = source_kind.map(str::to_owned);
        async move {
            let page = storage
                .list_request_events(&RequestEventListQuery {
                    since_unix_secs: base,
                    until_unix_secs: base + 4,
                    limit: 100,
                    filters: RequestEventStreamFilters {
                        principal_id: Some(principal.to_owned()),
                        event_kind,
                        ..Default::default()
                    },
                    source_kind,
                    ..Default::default()
                })
                .await?;
            let mut ids = page
                .into_iter()
                .map(|item| item.request_id)
                .collect::<Vec<_>>();
            ids.sort();
            Ok::<_, anyhow::Error>(ids)
        }
    };

    // No event_kind param: the pre-existing default exclusion still applies.
    ensure!(
        list_ids(None, None).await? == ["ek-count-tokens", "ek-historical", "ek-messages"],
        "default list must keep excluding renewals"
    );
    ensure!(
        list_ids(None, Some("all")).await?
            == [
                "ek-count-tokens",
                "ek-historical",
                "ek-messages",
                "ek-renewal",
                "ek-renewal-typed"
            ],
        "source_kind=all must keep returning every row"
    );
    ensure!(
        list_ids(None, Some("renewal")).await? == ["ek-renewal", "ek-renewal-typed"],
        "explicit source_kind=renewal must keep working"
    );

    // Explicit event_kind bypasses only the implicit exclusion and matches the
    // effective kind: renewal source wins over a stored kind, NULL reads as
    // unclassified.
    ensure!(
        list_ids(Some(RequestEventKind::Messages), None).await? == ["ek-messages"],
        "event_kind=messages must match only the messages row"
    );
    ensure!(
        list_ids(Some(RequestEventKind::Unclassified), None).await? == ["ek-historical"],
        "event_kind=unclassified must match the historical NULL row"
    );
    ensure!(
        list_ids(Some(RequestEventKind::Renewal), None).await?
            == ["ek-renewal", "ek-renewal-typed"],
        "event_kind=renewal must match renewal sources even with a stored kind"
    );
    ensure!(
        list_ids(Some(RequestEventKind::Files), None)
            .await?
            .is_empty(),
        "a stored files kind under a renewal source must not match files"
    );
    ensure!(
        list_ids(Some(RequestEventKind::Messages), Some("all")).await? == ["ek-messages"],
        "source_kind=all must stay conjunctive with event_kind"
    );
    ensure!(
        list_ids(Some(RequestEventKind::Messages), Some("renewal"))
            .await?
            .is_empty(),
        "explicit source_kind must stay conjunctive with event_kind"
    );

    // The histogram applies the same effective-kind predicate.
    let histogram_total = |event_kind: Option<RequestEventKind>, source_kind: Option<&str>| {
        let source_kind = source_kind.map(str::to_owned);
        async move {
            let buckets = storage
                .request_event_histogram(&RequestEventHistogramQuery {
                    since_unix_secs: base,
                    until_unix_secs: base + 4,
                    bucket_ms: 60_000,
                    bucket_count: 1,
                    filters: RequestEventStreamFilters {
                        principal_id: Some(principal.to_owned()),
                        event_kind,
                        ..Default::default()
                    },
                    source_kind,
                })
                .await?;
            Ok::<_, anyhow::Error>(buckets.iter().map(|bucket| bucket.total_count).sum::<u64>())
        }
    };
    ensure!(
        histogram_total(None, None).await? == 3,
        "default histogram must keep excluding renewals"
    );
    ensure!(
        histogram_total(Some(RequestEventKind::Renewal), None).await? == 2,
        "histogram event_kind=renewal must match both renewal sources"
    );
    ensure!(
        histogram_total(Some(RequestEventKind::Unclassified), None).await? == 1,
        "histogram event_kind=unclassified must match the historical row"
    );

    // The delta/cursor path applies the same effective-kind filter.
    let until_cursor = storage.current_request_event_cursor().await?;
    let delta_ids = |event_kind: Option<RequestEventKind>| async move {
        let events = storage
            .query_request_events_between_cursors(
                0,
                until_cursor,
                500,
                &RequestEventStreamFilters {
                    event_kind,
                    ..Default::default()
                },
            )
            .await?;
        let mut ids = events
            .into_iter()
            .map(|(_, event)| event.request_id)
            .collect::<Vec<_>>();
        ids.sort();
        Ok::<_, anyhow::Error>(ids)
    };
    ensure!(
        delta_ids(Some(RequestEventKind::Messages)).await? == ["ek-messages"],
        "delta event_kind=messages must match only the messages row"
    );
    ensure!(
        delta_ids(Some(RequestEventKind::Renewal)).await? == ["ek-renewal", "ek-renewal-typed"],
        "delta event_kind=renewal must match both renewal sources"
    );
    ensure!(
        delta_ids(Some(RequestEventKind::Unclassified)).await? == ["ek-historical"],
        "delta event_kind=unclassified must match the historical row"
    );

    // Payload reads surface the raw kind: stored on new writes, absent on
    // historical rows, never synthesized.
    let events = storage.query_request_events(base, base + 4, 100).await?;
    let kinds = events
        .into_iter()
        .map(|event| (event.request_id.clone(), event.event_kind))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        kinds.get("ek-messages") == Some(&Some(RequestEventKind::Messages)),
        "payload read lost the stored event_kind: {kinds:?}"
    );
    ensure!(
        kinds.get("ek-historical") == Some(&None),
        "historical row gained a synthesized event_kind: {kinds:?}"
    );
    ensure!(
        kinds.get("ek-renewal-typed") == Some(&Some(RequestEventKind::Files)),
        "renewal source must not rewrite the stored event_kind: {kinds:?}"
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
