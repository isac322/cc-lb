use std::collections::BTreeSet;
use std::sync::Arc;

use cc_lb_storage_api::{
    BackendKind, MetaStore, RequestEvent, RequestEventStore, RequestEventStreamFilters,
    RequestEventUpstream, StatusClass,
};
use sqlx::Row;
use uuid::Uuid;

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
        wrh_key_source: Some("cache_hash".to_owned()),
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
        "SELECT matched_v3_cache_key, breakpoint_content_block_index, matched_content_block_index, lookback_distance, predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, token_estimate_source, cache_value_micros, formula_winner_upstream_id, kept_upstream_id, wrh_key_source, lineage_would_have_predicted_read_tokens, lineage_would_have_picked_upstream_id FROM request_events_v1 WHERE event_id = ?",
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
    assert_eq!(row.get::<String, _>("wrh_key_source"), "cache_hash");
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
