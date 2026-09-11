use std::{
    collections::{BTreeSet, HashSet},
    str::FromStr,
    sync::Arc,
};

use cc_lb_storage_api::{
    BackendKind, MetaStore, RequestEvent, RequestEventHistogramQuery, RequestEventKeyLastUsedQuery,
    RequestEventKeyUsageQuery, RequestEventListQuery, RequestEventStore, RequestEventStreamFilters,
    RequestEventUpstream, StatusClass,
};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use uuid::Uuid;

#[tokio::test]
async fn t3__request_event_list_uses_materialized_sort_columns() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-list-plan.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    let columns = sqlx::query("PRAGMA table_info(request_events_v1)")
        .fetch_all(storage.pool())
        .await
        .expect("request_events table info")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<HashSet<_>>();
    for column in [
        "list_ts_ms",
        "list_event_key",
        "list_upstream",
        "list_status",
        "list_duration_ms",
        "list_cost_usd_micros",
    ] {
        assert!(
            columns.contains(column),
            "missing materialized column {column}"
        );
    }

    let plan = sqlx::query(
        "EXPLAIN QUERY PLAN \
         SELECT ts, list_ts_ms, request_id, event_id, principal_id, list_upstream, list_status \
         FROM request_events_v1 \
         WHERE ts >= ?1 AND ts <= ?2 \
         ORDER BY list_ts_ms DESC, list_event_key DESC, id DESC \
         LIMIT ?3",
    )
    .bind(0_i64)
    .bind(i64::MAX)
    .bind(100_i64)
    .fetch_all(storage.pool())
    .await
    .expect("explain materialized request-event list");
    let details = plan
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>();
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("request_events_v1_list_order_idx")),
        "request-event list must use the materialized list-order index: {details:?}"
    );
    assert!(
        details
            .iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE FOR ORDER BY")),
        "request-event list must not build a temporary sort b-tree: {details:?}"
    );
}

#[tokio::test]
async fn t3__request_event_principal_costs_use_principal_range_indexes() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-event-principal-cost-plan.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    let plan = sqlx::query(
        "EXPLAIN QUERY PLAN \
         SELECT principal_id, ((list_ts_ms - ?) / ?) AS bucket_index, \
                SUM(list_cost_usd_micros) \
         FROM ( \
             SELECT principal_id, list_ts_ms, list_cost_usd_micros \
             FROM request_events_v1 \
             WHERE list_ts_ms >= ? AND list_ts_ms < ? \
               AND (? IS NULL OR upstream_id = ?) \
               AND principal_id IN (?, ?) \
             UNION ALL \
             SELECT principal_id, list_ts_ms, list_cost_usd_micros \
             FROM request_events_v1 \
             WHERE list_ts_ms >= ? AND list_ts_ms < ? \
               AND (? IS NULL OR upstream_id = ?) \
               AND (principal_id IS NULL \
                 OR length(principal_id) <> 36 \
                 OR length(replace(principal_id, '-', '')) <> 32 \
                 OR substr(principal_id, 9, 1) <> '-' \
                 OR substr(principal_id, 14, 1) <> '-' \
                 OR substr(principal_id, 19, 1) <> '-' \
                 OR substr(principal_id, 24, 1) <> '-' \
                 OR lower(replace(principal_id, '-', '')) GLOB '*[^0-9a-f]*') \
               AND (principal_id IS NOT NULL OR ?) \
         ) matched \
         GROUP BY principal_id, bucket_index",
    )
    .bind(1_900_500_000_000_i64)
    .bind(60_000_i64)
    .bind(1_900_500_000_000_i64)
    .bind(1_900_500_120_000_i64)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind(Uuid::from_u128(0x51).to_string())
    .bind(Uuid::from_u128(0x52).to_string())
    .bind(1_900_500_000_000_i64)
    .bind(1_900_500_120_000_i64)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind(false)
    .fetch_all(storage.pool())
    .await
    .expect("explain composed principal cost aggregate")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>();

    assert!(
        plan.iter()
            .any(|detail| detail.contains("request_events_v1_principal_list_order_idx")),
        "exact principal cost source must use the principal-leading range index: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|detail| detail.contains("request_events_v1_non_uuid_principal_cost_idx")),
        "normalized fallback must use the non-UUID partial index: {plan:?}"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.starts_with("SCAN request_events_v1")),
        "composed principal cost aggregate must not scan the request-event table: {plan:?}"
    );
}

#[tokio::test]
async fn t3__request_event_key_aggregates_use_normalized_columns() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-key-aggregates.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    for event in [
        RequestEvent {
            ts: 1_800_000_000,
            ts_ms: Some(1_800_000_000_000),
            request_id: "req-key-aggregate-a".to_owned(),
            event_id: Some("0193a7b8-1234-7e2f-9012-keyagg000001".to_owned()),
            principal_id: Some("principal-aggregate".to_owned()),
            key_id: Some("key-aggregate".to_owned()),
            input_tokens: Some(10),
            cache_creation_input_tokens: Some(20),
            cache_read_input_tokens: Some(30),
            output_tokens: Some(40),
            cost_usd_micros: Some(50),
            status: 200,
            duration_ms: 10,
            ..Default::default()
        },
        RequestEvent {
            ts: 1_800_000_001,
            ts_ms: Some(1_800_000_001_000),
            request_id: "req-key-aggregate-b".to_owned(),
            event_id: Some("0193a7b8-1234-7e2f-9012-keyagg000002".to_owned()),
            principal_id: Some("principal-aggregate".to_owned()),
            key_id: Some("key-aggregate".to_owned()),
            input_tokens: Some(1),
            cache_creation_input_tokens: Some(2),
            cache_read_input_tokens: Some(3),
            output_tokens: Some(4),
            cost_usd_micros: Some(5),
            status: 200,
            duration_ms: 11,
            ..Default::default()
        },
        RequestEvent {
            ts: 1_800_000_002,
            ts_ms: Some(1_800_000_002_000),
            request_id: "req-key-aggregate-other".to_owned(),
            event_id: Some("0193a7b8-1234-7e2f-9012-keyagg000003".to_owned()),
            principal_id: Some("principal-aggregate".to_owned()),
            key_id: Some("other-key".to_owned()),
            input_tokens: Some(100),
            output_tokens: Some(100),
            cost_usd_micros: Some(100),
            status: 200,
            duration_ms: 12,
            ..Default::default()
        },
    ] {
        storage
            .append_request_event(&event)
            .await
            .expect("append aggregate event");
    }

    let last_used = storage
        .request_event_key_last_used(&RequestEventKeyLastUsedQuery {
            principal_id: "principal-aggregate".to_owned(),
            since_unix_secs: 1_800_000_000,
            until_unix_secs: 1_800_000_010,
        })
        .await
        .expect("key last-used aggregation");
    assert_eq!(last_used.len(), 2);
    assert!(last_used.iter().any(|row| {
        row.key_id == "key-aggregate" && row.last_used_at_unix_secs == 1_800_000_001
    }));

    let usage = storage
        .request_event_key_usage(&RequestEventKeyUsageQuery {
            principal_id: "principal-aggregate".to_owned(),
            key_id: "key-aggregate".to_owned(),
            range_start_ms: 1_800_000_000_000,
            range_end_ms: 1_800_000_002_000,
            step_ms: 1_000,
            bucket_count: 2,
        })
        .await
        .expect("key usage aggregation");
    assert_eq!(usage.len(), 2);
    assert_eq!(usage[0].bucket_start_unix_secs, 1_800_000_000);
    assert_eq!(usage[0].request_count, 1);
    assert_eq!(usage[0].input_tokens, 60);
    assert_eq!(usage[0].output_tokens, 40);
    assert_eq!(usage[0].cost_usd_micros, 50);
    assert_eq!(usage[1].bucket_start_unix_secs, 1_800_000_001);
    assert_eq!(usage[1].request_count, 1);
    assert_eq!(usage[1].input_tokens, 6);
    assert_eq!(usage[1].output_tokens, 4);
    assert_eq!(usage[1].cost_usd_micros, 5);

    let last_used_plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT key_id, MAX(ts) FROM request_events_v1 WHERE principal_id = ? AND key_id IS NOT NULL AND key_id <> '' AND ts >= ? AND ts <= ? GROUP BY key_id",
    )
    .bind("principal-aggregate")
    .bind(1_800_000_000_i64)
    .bind(1_800_000_010_i64)
    .fetch_all(storage.pool())
    .await
    .expect("explain key last-used aggregate")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>();
    assert!(
        last_used_plan
            .iter()
            .any(|detail| detail.contains("request_events_v1_principal_key")),
        "key last-used aggregate must use a principal/key covering index: {last_used_plan:?}"
    );

    let usage_plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT MIN(((list_ts_ms - ?) / ?), ?) AS bucket_index, COUNT(*) FROM request_events_v1 WHERE principal_id = ? AND key_id = ? AND list_ts_ms >= ? AND list_ts_ms <= ? GROUP BY bucket_index",
    )
    .bind(1_800_000_000_000_i64)
    .bind(1_000_i64)
    .bind(1_i64)
    .bind("principal-aggregate")
    .bind("key-aggregate")
    .bind(1_800_000_000_000_i64)
    .bind(1_800_000_002_000_i64)
    .fetch_all(storage.pool())
    .await
    .expect("explain key usage aggregate")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>();
    assert!(
        usage_plan
            .iter()
            .any(|detail| detail.contains("request_events_v1_principal_key")),
        "key usage aggregate must use a principal/key covering index: {usage_plan:?}"
    );
}

#[tokio::test]
async fn t3__request_event_cursor_api_returns_stable_duplicate_cursor_and_filters_backfill() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-cursor.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    let upstream_id = Uuid::from_u128(7);
    let event = RequestEvent {
        ts: 1_800_000_000,
        request_id: "req-cursor-1".to_owned(),
        event_id: Some("0193a7b8-1234-7e2f-9012-cursor000001".to_owned()),
        principal_id: Some("principal-a".to_owned()),
        thread_id: Some("thread-a".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(upstream_id),
        model: Some("claude-sonnet-4-5".to_owned()),
        matched_v3_cache_key: Some("v3-cache-key".to_owned()),
        breakpoint_content_block_index: Some(12),
        matched_content_block_index: Some(11),
        lookback_distance: Some(1),
        predicted_cache_read_tokens: Some(20_000),
        predicted_cache_creation_tokens_5m: Some(1_000),
        predicted_cache_creation_tokens_1h: Some(2_000),
        token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        cache_value_micros: Some(123_456),
        formula_winner_upstream_id: Some(upstream_id),
        kept_upstream_id: Some(upstream_id),
        lineage_would_have_predicted_read_tokens: Some(15_000),
        lineage_would_have_picked_upstream_id: Some(upstream_id),
        status: 200,
        duration_ms: 10,
        ..Default::default()
    };

    let first_cursor = storage
        .append_request_event(&event)
        .await
        .expect("append first event");
    let duplicate_cursor = storage
        .append_request_event(&event)
        .await
        .expect("append duplicate event");
    let current_cursor = storage
        .current_request_event_cursor()
        .await
        .expect("current cursor");

    assert_eq!(duplicate_cursor, first_cursor);
    assert_eq!(current_cursor, first_cursor);

    let matching = storage
        .query_request_events_between_cursors(
            0,
            current_cursor,
            500,
            &RequestEventStreamFilters {
                principal_id: Some("principal-a".to_owned()),
                thread_id: Some("thread-a".to_owned()),
                model: Some("claude-sonnet-4-5".to_owned()),
                upstream: Some(RequestEventUpstream::AnthropicDirect),
                upstream_id: Some(upstream_id),
                status_class: Some(StatusClass::TwoXx),
            },
        )
        .await
        .expect("cursor backfill");
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].0, first_cursor);
    assert_eq!(matching[0].1.request_id, "req-cursor-1");

    let row = sqlx::query(
        "SELECT matched_v3_cache_key, breakpoint_content_block_index, matched_content_block_index, \
                lookback_distance, predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, \
                predicted_cache_creation_tokens_1h, token_estimate_source, cache_value_micros, \
                formula_winner_upstream_id, kept_upstream_id, \
                lineage_would_have_predicted_read_tokens, lineage_would_have_picked_upstream_id \
         FROM request_events_v1 WHERE event_id = ?",
    )
    .bind(event.event_id.as_deref())
    .fetch_one(storage.pool())
    .await
    .expect("v3 request-event columns stored");
    assert_eq!(row.get::<String, _>("matched_v3_cache_key"), "v3-cache-key");
    assert_eq!(row.get::<i64, _>("breakpoint_content_block_index"), 12);
    assert_eq!(row.get::<i64, _>("matched_content_block_index"), 11);
    assert_eq!(row.get::<i64, _>("lookback_distance"), 1);
    assert_eq!(row.get::<i64, _>("predicted_cache_read_tokens"), 20_000);
    assert_eq!(
        row.get::<i64, _>("predicted_cache_creation_tokens_5m"),
        1_000
    );
    assert_eq!(
        row.get::<i64, _>("predicted_cache_creation_tokens_1h"),
        2_000
    );
    assert_eq!(
        row.get::<String, _>("token_estimate_source"),
        "local_tiktoken_v1"
    );
    assert_eq!(row.get::<i64, _>("cache_value_micros"), 123_456);
    assert_eq!(
        row.get::<String, _>("formula_winner_upstream_id"),
        upstream_id.to_string()
    );
    assert_eq!(
        row.get::<String, _>("kept_upstream_id"),
        upstream_id.to_string()
    );
    assert_eq!(
        row.get::<i64, _>("lineage_would_have_predicted_read_tokens"),
        15_000
    );
    assert_eq!(
        row.get::<String, _>("lineage_would_have_picked_upstream_id"),
        upstream_id.to_string()
    );

    let filtered = storage
        .query_request_events_between_cursors(
            0,
            current_cursor,
            500,
            &RequestEventStreamFilters {
                status_class: Some(StatusClass::FiveXx),
                ..Default::default()
            },
        )
        .await
        .expect("filtered cursor backfill");
    assert!(filtered.is_empty());
}

#[tokio::test]
async fn t3__cursor_pages_cover_two_hundred_rows_without_gaps_or_duplicates() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-pagination.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    let mut expected = BTreeSet::new();
    for index in 0..200u64 {
        let cursor = storage
            .append_request_event(&RequestEvent {
                ts: 1_800_000_000 + index,
                request_id: format!("req-page-{index}"),
                event_id: Some(format!("0193a7b8-1234-7e2f-9012-page{index:06}")),
                status: 200,
                duration_ms: index,
                ..Default::default()
            })
            .await
            .expect("append paginated event");
        expected.insert(cursor);
    }

    let current_cursor = storage
        .current_request_event_cursor()
        .await
        .expect("current cursor");
    let mut after = 0;
    let mut seen = BTreeSet::new();
    loop {
        let page = storage
            .query_request_events_between_cursors(
                after,
                current_cursor,
                50,
                &RequestEventStreamFilters::default(),
            )
            .await
            .expect("cursor page");
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 50);
        for (cursor, event) in page {
            assert!(seen.insert(cursor), "duplicate cursor {cursor}");
            assert_eq!(
                event.event_id.as_deref(),
                Some(format!("0193a7b8-1234-7e2f-9012-page{:06}", cursor - 1).as_str())
            );
            after = cursor;
        }
    }

    assert_eq!(seen, expected);
}

/// The histogram exists to say the same thing the list says. These cases lock
/// the invariants that make that true: the same `ts` window, a zero-filled
/// continuous axis, the UI's danger set for `error_count`, and an index range
/// scan instead of a table scan.
#[tokio::test]
async fn t3__request_event_histogram_matches_list_and_uses_index() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-histogram.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    let base = 1_800_000_000_u64;
    let events = [
        (base, 200_u16, None, None),
        // 4xx is warn in the UI, so it must stay out of the error lane.
        (base + 10, 429, None, None),
        (base + 20, 503, Some("upstream_5xx"), None),
        // A 2xx carrying a semantic stream failure is an error.
        (base + 30, 200, Some("upstream_stream_error"), None),
        // Renewal traffic is excluded unless asked for.
        (base + 40, 200, None, Some("renewal")),
        // Bucket 1 and 2 stay empty so zero-fill has something to prove.
        (base + 180, 200, None, None),
    ];
    for (index, (ts, status, error_code, source_kind)) in events.iter().enumerate() {
        storage
            .append_request_event(&RequestEvent {
                ts: *ts,
                // Offset into the second on purpose: the window is filtered on
                // `ts`, so a sub-second remainder must not drop the row.
                ts_ms: Some(ts * 1_000 + 500),
                request_id: format!("req-histogram-{index}"),
                event_id: Some(format!("0193a7b8-1234-7e2f-9012-hist{index:08}")),
                principal_id: Some("principal-histogram".to_owned()),
                model: Some("claude-sonnet-4-5".to_owned()),
                status: *status,
                error_code: error_code.map(str::to_owned),
                source_kind: source_kind.map(str::to_owned),
                duration_ms: 10,
                ..Default::default()
            })
            .await
            .expect("append histogram event");
    }

    // Today's writer derives `ts` from `ts_ms` (request_events.rs:800), so the
    // two cannot drift apart through the public API. This row is written
    // directly to simulate a future writer that timestamps them separately:
    // `ts` puts it inside the window, `list_ts_ms` sits 30s before the window
    // start. It survives only while the `list_ts_ms` bound stays a widened seek
    // hint; turning that bound into a real filter drops the row and fails here.
    sqlx::query(
        "INSERT INTO request_events_v1 \
            (request_id, ts, event_type, payload, event_id, principal_id, model, \
             cache_breakpoints, list_ts_ms, list_event_key, list_status, list_duration_ms) \
         VALUES (?, ?, 'request_completed', ?, ?, ?, ?, '[]', ?, ?, 200, 10)",
    )
    .bind("req-histogram-skewed")
    .bind(base as i64)
    .bind(format!(
        "{{\"ts_ms\":{},\"status\":200,\"duration_ms\":10}}",
        base * 1_000 - 30_000
    ))
    .bind("0193a7b8-1234-7e2f-9012-histskewed0")
    .bind("principal-histogram")
    .bind("claude-sonnet-4-5")
    .bind((base * 1_000 - 30_000) as i64)
    .bind("0193a7b8-1234-7e2f-9012-histskewed0")
    .execute(storage.pool())
    .await
    .expect("insert skewed histogram row");

    let window_end = base + 180;
    let query = |source_kind: Option<&str>, filters: RequestEventStreamFilters| {
        RequestEventHistogramQuery {
            since_unix_secs: base,
            until_unix_secs: window_end,
            bucket_ms: 60_000,
            bucket_count: 4,
            filters,
            source_kind: source_kind.map(str::to_owned),
        }
    };

    let buckets = storage
        .request_event_histogram(&query(None, RequestEventStreamFilters::default()))
        .await
        .expect("histogram");
    assert_eq!(buckets.len(), 4, "zero-filled buckets are never omitted");
    for (index, bucket) in buckets.iter().enumerate() {
        assert_eq!(
            bucket.bucket_start_unix_secs,
            base + (index as u64) * 60,
            "bucket axis is continuous"
        );
    }
    assert_eq!(
        buckets[0].total_count, 5,
        "renewal stays out by default, and a row whose ts_ms precedes the \
         window start is still counted because `ts` decides membership"
    );
    assert_eq!(
        buckets[0].error_count, 2,
        "error_count is 5xx plus 2xx semantic failures, never 4xx"
    );
    assert_eq!(buckets[1].total_count, 0);
    assert_eq!(buckets[2].total_count, 0);
    assert_eq!(
        buckets[3].total_count, 1,
        "an event in the final second of the window is kept"
    );

    let listed = storage
        .list_request_events(&RequestEventListQuery {
            since_unix_secs: base,
            until_unix_secs: window_end,
            limit: 100,
            ..Default::default()
        })
        .await
        .expect("list request events");
    let histogram_total: u64 = buckets.iter().map(|bucket| bucket.total_count).sum();
    assert_eq!(
        histogram_total,
        listed.len() as u64,
        "histogram total must equal the list count for the same window"
    );

    let with_renewal = storage
        .request_event_histogram(&query(Some("all"), RequestEventStreamFilters::default()))
        .await
        .expect("histogram including renewal");
    assert_eq!(
        with_renewal
            .iter()
            .map(|bucket| bucket.total_count)
            .sum::<u64>(),
        histogram_total + 1,
        "source_kind=all includes renewal rows"
    );

    let other_model = storage
        .request_event_histogram(&query(
            None,
            RequestEventStreamFilters {
                model: Some("claude-haiku".to_owned()),
                ..Default::default()
            },
        ))
        .await
        .expect("histogram with model filter");
    assert_eq!(
        other_model
            .iter()
            .map(|bucket| bucket.total_count)
            .sum::<u64>(),
        0,
        "filters reach the histogram, not just the list"
    );

    // The `ts` predicate cannot use an index on its own, so the query carries a
    // deliberately wider `list_ts_ms` bound purely as a seek hint.
    let plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT MIN(MAX((list_ts_ms - ?) / ?, 0), ?) AS bucket_index, \
                COUNT(*) \
         FROM request_events_v1 \
         WHERE ts >= ? AND ts <= ? AND list_ts_ms >= ? AND list_ts_ms < ? \
         GROUP BY bucket_index",
    )
    .bind(1_800_000_000_000_i64)
    .bind(60_000_i64)
    .bind(3_i64)
    .bind(base as i64)
    .bind(window_end as i64)
    .bind(1_799_999_940_000_i64)
    .bind(1_800_000_241_000_i64)
    .fetch_all(storage.pool())
    .await
    .expect("explain histogram")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>();
    assert!(
        plan.iter().any(|detail| detail.contains("USING INDEX")),
        "histogram must range-scan an index rather than scan the table: {plan:?}"
    );
    assert!(
        !plan
            .iter()
            .any(|detail| detail.starts_with("SCAN request_events_v1")),
        "histogram must not fall back to a full table scan: {plan:?}"
    );
}

#[tokio::test]
async fn t3__request_setup_timings_roundtrip_through_sqlite_payload() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-setup-timings.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    let columns = sqlx::query("PRAGMA table_info(request_events_v1)")
        .fetch_all(storage.pool())
        .await
        .expect("request events schema")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<HashSet<_>>();
    for column in [
        "list_json_parse_ms",
        "list_cache_structure_ms",
        "list_cache_token_key_ms",
        "list_cache_count_lookup_ms",
        "list_cache_tokenizer_queue_ms",
        "list_cache_serialize_ms",
        "list_cache_tokenize_ms",
        "list_prepare_signer_ms",
    ] {
        assert!(
            columns.contains(column),
            "missing materialized column {column}"
        );
    }
    let event = RequestEvent {
        ts: 1_800_000_000,
        request_id: "req-setup-timings".to_owned(),
        event_id: Some("event-setup-timings".to_owned()),
        status: 502,
        duration_ms: 10,
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.0),
        cache_token_key_ms: Some(0.25),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.75),
        cache_serialize_ms: Some(1.0),
        cache_tokenize_ms: None,
        prepare_signer_ms: Some(2.0),
        ..RequestEvent::default()
    };

    storage
        .append_request_event(&event)
        .await
        .expect("append request setup timings");
    let payload: String =
        sqlx::query_scalar("SELECT payload FROM request_events_v1 WHERE event_id = ?")
            .bind(event.event_id.as_deref())
            .fetch_one(storage.pool())
            .await
            .expect("read stored payload");
    let restored: RequestEvent =
        serde_json::from_str(&payload).expect("deserialize stored request event");

    assert_eq!(restored.json_parse_ms, Some(0.125));
    assert_eq!(restored.cache_structure_ms, Some(0.0));
    assert_eq!(restored.cache_token_key_ms, Some(0.25));
    assert_eq!(restored.cache_count_lookup_ms, Some(0.5));
    assert_eq!(restored.cache_tokenizer_queue_ms, Some(0.75));
    assert_eq!(restored.cache_serialize_ms, Some(1.0));
    assert_eq!(restored.cache_tokenize_ms, None);
    assert_eq!(restored.prepare_signer_ms, Some(2.0));

    let page = storage
        .list_request_events(&RequestEventListQuery {
            since_unix_secs: 0,
            until_unix_secs: u64::MAX,
            limit: 1,
            ..RequestEventListQuery::default()
        })
        .await
        .expect("list request setup timings");
    let listed = page.first().expect("listed timing row");
    assert_eq!(listed.json_parse_ms, Some(0.125));
    assert_eq!(listed.cache_structure_ms, Some(0.0));
    assert_eq!(listed.cache_token_key_ms, Some(0.25));
    assert_eq!(listed.cache_count_lookup_ms, Some(0.5));
    assert_eq!(listed.cache_tokenizer_queue_ms, Some(0.75));
    assert_eq!(listed.cache_serialize_ms, Some(1.0));
    assert_eq!(listed.cache_tokenize_ms, None);
    assert_eq!(listed.prepare_signer_ms, Some(2.0));
}

#[tokio::test]
async fn request_io_timing_list_fields_preserve_legacy_nulls_and_fractional_values() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-io-timing-list-fields.sqlite")
            .display()
    );
    let options = SqliteConnectOptions::from_str(&database_url)
        .expect("sqlite connect options")
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite database");
    let migrator = sqlx::migrate!("./migrations");
    migrator
        .run_direct(Some(81), &mut connection, false)
        .await
        .expect("apply migrations through version 81");

    sqlx::query(
        "INSERT INTO request_events_v1 \
            (request_id, ts, event_type, payload, event_id, cache_breakpoints, \
             list_ts_ms, list_event_key, list_status, list_duration_ms) \
         VALUES (?, ?, 'request_completed', ?, ?, '[]', ?, ?, 200, 10)",
    )
    .bind("legacy-request")
    .bind(1_i64)
    .bind(r#"{"ts":1,"request_id":"legacy-request","status":200,"duration_ms":10}"#)
    .bind("legacy-event")
    .bind(1_000_i64)
    .bind("legacy-event")
    .execute(&mut connection)
    .await
    .expect("insert pre-migration request event");

    migrator
        .run_direct(Some(82), &mut connection, false)
        .await
        .expect("apply request IO timing list migration");

    let legacy = sqlx::query(
        "SELECT list_request_body_first_chunk_ms, list_request_body_receive_ms, \
                list_request_body_wait_ms, list_request_body_process_ms, \
                list_request_body_chunk_count, list_response_body_wait_ms, \
                list_response_body_process_ms, \
                list_response_body_downstream_poll_gap_ms, list_retry_overhead_ms \
         FROM request_events_v1 WHERE event_id = ?",
    )
    .bind("legacy-event")
    .fetch_one(&mut connection)
    .await
    .expect("read migrated legacy timing columns");
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_request_body_first_chunk_ms"),
        None
    );
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_request_body_receive_ms"),
        None
    );
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_request_body_wait_ms"),
        None
    );
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_request_body_process_ms"),
        None
    );
    assert_eq!(
        legacy.get::<Option<i64>, _>("list_request_body_chunk_count"),
        None
    );
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_response_body_wait_ms"),
        None
    );
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_response_body_process_ms"),
        None
    );
    assert_eq!(
        legacy.get::<Option<f64>, _>("list_response_body_downstream_poll_gap_ms"),
        None
    );
    assert_eq!(legacy.get::<Option<f64>, _>("list_retry_overhead_ms"), None);
    drop(connection);

    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open migrated sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize migrated sqlite");
    storage
        .append_request_event(&RequestEvent {
            ts: 2,
            ts_ms: Some(2_000),
            request_id: "current-request".to_owned(),
            event_id: Some("current-event".to_owned()),
            status: 200,
            duration_ms: 10,
            request_body_first_chunk_ms: Some(0.125),
            request_body_receive_ms: Some(1.375),
            request_body_wait_ms: Some(2.625),
            request_body_process_ms: Some(0.875),
            request_body_chunk_count: Some(3),
            response_body_wait_ms: Some(3.125),
            response_body_process_ms: Some(0.625),
            response_body_downstream_poll_gap_ms: Some(4.875),
            retry_overhead_ms: Some(5.5),
            ..RequestEvent::default()
        })
        .await
        .expect("append current request IO timings");

    let page = storage
        .list_request_events(&RequestEventListQuery {
            since_unix_secs: 0,
            until_unix_secs: u64::MAX,
            limit: 2,
            ..RequestEventListQuery::default()
        })
        .await
        .expect("list current and legacy request IO timings");
    assert_eq!(page.len(), 2);
    let current = &page[0];
    assert_eq!(current.event_id.as_deref(), Some("current-event"));
    assert_eq!(current.request_body_first_chunk_ms, Some(0.125));
    assert_eq!(current.request_body_receive_ms, Some(1.375));
    assert_eq!(current.request_body_wait_ms, Some(2.625));
    assert_eq!(current.request_body_process_ms, Some(0.875));
    assert_eq!(current.request_body_chunk_count, Some(3));
    assert_eq!(current.response_body_wait_ms, Some(3.125));
    assert_eq!(current.response_body_process_ms, Some(0.625));
    assert_eq!(current.response_body_downstream_poll_gap_ms, Some(4.875));
    assert_eq!(current.retry_overhead_ms, Some(5.5));

    let legacy = &page[1];
    assert_eq!(legacy.event_id.as_deref(), Some("legacy-event"));
    assert_eq!(legacy.request_body_first_chunk_ms, None);
    assert_eq!(legacy.request_body_receive_ms, None);
    assert_eq!(legacy.request_body_wait_ms, None);
    assert_eq!(legacy.request_body_process_ms, None);
    assert_eq!(legacy.request_body_chunk_count, None);
    assert_eq!(legacy.response_body_wait_ms, None);
    assert_eq!(legacy.response_body_process_ms, None);
    assert_eq!(legacy.response_body_downstream_poll_gap_ms, None);
    assert_eq!(legacy.retry_overhead_ms, None);
}
