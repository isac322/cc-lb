use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use cc_lb_storage_api::{
    BackendKind, MetaStore, RequestEvent, RequestEventKeyLastUsedQuery, RequestEventKeyUsageQuery,
    RequestEventStore, RequestEventStreamFilters, RequestEventUpstream, StatusClass,
};
use sqlx::Row;
use uuid::Uuid;

#[tokio::test]
async fn request_event_list_uses_materialized_sort_columns() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-list-plan.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
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
async fn request_event_key_aggregates_use_normalized_columns() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-key-aggregates.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
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
async fn request_event_cursor_api_returns_stable_duplicate_cursor_and_filters_backfill() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-cursor.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
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
async fn cursor_pages_cover_two_hundred_rows_without_gaps_or_duplicates() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-events-pagination.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
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
