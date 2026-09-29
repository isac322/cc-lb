use std::sync::Arc;

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventHistogramQuery, RequestEventKind, RequestEventListQuery,
    RequestEventStore, RequestEventStreamFilters,
};
use sqlx::Row;

const BASE: u64 = 1_800_000_000;

async fn migrated_storage(name: &str) -> (tempfile::TempDir, cc_lb_storage_sqlite::SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage)
}

fn classified_event(
    index: usize,
    ts: u64,
    event_kind: Option<RequestEventKind>,
    source_kind: Option<&str>,
) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        request_id: format!("req-kind-{index}"),
        event_id: Some(format!("0193a7b8-1234-7e2f-9012-kind{index:08}")),
        source_kind: source_kind.map(str::to_owned),
        event_kind,
        status: 200,
        duration_ms: 10,
        ..Default::default()
    }
}

/// Inserts a row the way a pre-migration writer did: every column the list
/// query needs, but no `event_kind`, so the materialized column stays NULL.
async fn insert_legacy_row(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    index: usize,
    ts: u64,
    source_kind: Option<&str>,
) {
    let event_id = format!("0193a7b8-1234-7e2f-9012-legacy{index:06}");
    sqlx::query(
        "INSERT INTO request_events_v1 \
            (request_id, ts, source_kind, payload, event_id, \
             list_ts_ms, list_event_key, list_status, list_duration_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, 200, 10)",
    )
    .bind(format!("req-legacy-{index}"))
    .bind(ts as i64)
    .bind(source_kind)
    .bind(format!(
        "{{\"ts\":{ts},\"request_id\":\"req-legacy-{index}\",\"status\":200,\"duration_ms\":10}}"
    ))
    .bind(&event_id)
    .bind((ts * 1_000) as i64)
    .bind(&event_id)
    .execute(storage.pool())
    .await
    .expect("insert legacy row");
}

fn list_query(
    event_kind: Option<RequestEventKind>,
    source_kind: Option<&str>,
    limit: usize,
) -> RequestEventListQuery {
    RequestEventListQuery {
        since_unix_secs: BASE - 10,
        until_unix_secs: BASE + 1_000,
        limit,
        filters: RequestEventStreamFilters {
            event_kind,
            ..Default::default()
        },
        source_kind: source_kind.map(str::to_owned),
        ..Default::default()
    }
}

async fn listed_ids(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    query: &RequestEventListQuery,
) -> Vec<String> {
    storage
        .list_request_events(query)
        .await
        .expect("list request events")
        .into_iter()
        .map(|item| item.request_id)
        .collect()
}

#[tokio::test]
async fn event_kind_migration_is_nullable_and_non_destructive() {
    let (_dir, storage) = migrated_storage("request-events-kind-migration.sqlite").await;

    insert_legacy_row(&storage, 0, BASE, None).await;
    storage
        .append_request_event(&classified_event(
            1,
            BASE + 1,
            Some(RequestEventKind::Messages),
            None,
        ))
        .await
        .expect("append classified event");

    let row =
        sqlx::query("SELECT event_kind FROM request_events_v1 WHERE request_id = 'req-legacy-0'")
            .fetch_one(storage.pool())
            .await
            .expect("legacy row");
    assert!(
        row.get::<Option<String>, _>("event_kind").is_none(),
        "rows written before the migration keep NULL event_kind"
    );

    let row =
        sqlx::query("SELECT event_kind FROM request_events_v1 WHERE request_id = 'req-kind-1'")
            .fetch_one(storage.pool())
            .await
            .expect("classified row");
    assert_eq!(
        row.get::<Option<String>, _>("event_kind").as_deref(),
        Some("messages"),
        "new writes bind the classified kind"
    );
}

#[tokio::test]
async fn event_kind_filter_partitions_list_and_histogram() {
    let (_dir, storage) = migrated_storage("request-events-kind-partition.sqlite").await;

    // One row per classified kind, plus a renewal whose column disagrees with
    // its source, plus two legacy NULL rows (one plain, one renewal-sourced).
    let seeds = [
        (RequestEventKind::Messages, None),
        (RequestEventKind::CountTokens, None),
        (RequestEventKind::Models, None),
        (RequestEventKind::Files, None),
        (RequestEventKind::Other, None),
        // Renewal precedence: the column says messages, the source wins.
        (RequestEventKind::Messages, Some("renewal")),
    ];
    for (index, (kind, source)) in seeds.iter().enumerate() {
        storage
            .append_request_event(&classified_event(
                index,
                BASE + index as u64,
                Some(*kind),
                *source,
            ))
            .await
            .expect("append seeded event");
    }
    insert_legacy_row(&storage, 10, BASE + 10, None).await;
    insert_legacy_row(&storage, 11, BASE + 11, Some("renewal")).await;

    for (kind, expected) in [
        (RequestEventKind::Messages, vec!["req-kind-0"]),
        (RequestEventKind::CountTokens, vec!["req-kind-1"]),
        (RequestEventKind::Models, vec!["req-kind-2"]),
        (RequestEventKind::Files, vec!["req-kind-3"]),
        (RequestEventKind::Other, vec!["req-kind-4"]),
        // Renewal precedence covers both the classified renewal and the
        // legacy renewal whose column is NULL.
        (
            RequestEventKind::Renewal,
            vec!["req-legacy-11", "req-kind-5"],
        ),
        // Only the plain legacy NULL row is unclassified.
        (RequestEventKind::Unclassified, vec!["req-legacy-10"]),
    ] {
        assert_eq!(
            listed_ids(&storage, &list_query(Some(kind), None, 100)).await,
            expected,
            "event_kind={kind:?} partition"
        );
    }

    // No event_kind param: byte-for-byte old behavior — renewal excluded.
    let mut default_ids = listed_ids(&storage, &list_query(None, None, 100)).await;
    default_ids.sort();
    assert_eq!(
        default_ids,
        vec![
            "req-kind-0",
            "req-kind-1",
            "req-kind-2",
            "req-kind-3",
            "req-kind-4",
            "req-legacy-10",
        ],
        "absent event_kind keeps the implicit renewal exclusion"
    );

    // Explicit source_kind stays conjunctive with event_kind.
    assert_eq!(
        listed_ids(
            &storage,
            &list_query(Some(RequestEventKind::Renewal), Some("renewal"), 100)
        )
        .await,
        vec!["req-legacy-11", "req-kind-5"],
        "explicit source_kind=renewal still applies alongside event_kind"
    );
    assert!(
        listed_ids(
            &storage,
            &list_query(Some(RequestEventKind::Messages), Some("renewal"), 100)
        )
        .await
        .is_empty(),
        "renewal rows are effective-renewal even when the column says messages"
    );
    assert_eq!(
        listed_ids(
            &storage,
            &list_query(Some(RequestEventKind::Messages), Some("all"), 100)
        )
        .await,
        vec!["req-kind-0"],
        "source_kind=all drops the source predicate but keeps event_kind"
    );

    // Histogram counts the same partition as the list for the same window.
    for (kind, expected_total) in [
        (RequestEventKind::Messages, 1_u64),
        (RequestEventKind::Renewal, 2),
        (RequestEventKind::Unclassified, 1),
    ] {
        let buckets = storage
            .request_event_histogram(&RequestEventHistogramQuery {
                since_unix_secs: BASE - 10,
                until_unix_secs: BASE + 1_000,
                bucket_ms: 60_000,
                bucket_count: 20,
                filters: RequestEventStreamFilters {
                    event_kind: Some(kind),
                    ..Default::default()
                },
                source_kind: None,
            })
            .await
            .expect("histogram");
        assert_eq!(
            buckets.iter().map(|bucket| bucket.total_count).sum::<u64>(),
            expected_total,
            "histogram total for event_kind={kind:?}"
        );
    }
}

#[tokio::test]
async fn event_kind_filter_applies_before_pagination() {
    let (_dir, storage) = migrated_storage("request-events-kind-pagination.sqlite").await;

    // Three messages rows oldest, then five non-messages rows newest. If the
    // filter ran after LIMIT the first page would be starved by the newer rows.
    for index in 0..3usize {
        storage
            .append_request_event(&classified_event(
                index,
                BASE + index as u64,
                Some(RequestEventKind::Messages),
                None,
            ))
            .await
            .expect("append messages event");
    }
    for index in 3..8usize {
        storage
            .append_request_event(&classified_event(
                index,
                BASE + index as u64,
                Some(RequestEventKind::Other),
                None,
            ))
            .await
            .expect("append other event");
    }

    let first_page = storage
        .list_request_events(&list_query(Some(RequestEventKind::Messages), None, 2))
        .await
        .expect("first page");
    assert_eq!(first_page.len(), 2, "filter runs before LIMIT");
    assert_eq!(first_page[0].request_id, "req-kind-2");
    assert_eq!(first_page[1].request_id, "req-kind-1");

    let mut second_query = list_query(Some(RequestEventKind::Messages), None, 2);
    second_query.until_ts_ms = first_page[1].ts_ms;
    second_query.until_event_id = first_page[1].event_id.clone();
    let second_page = storage
        .list_request_events(&second_query)
        .await
        .expect("second page");
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].request_id, "req-kind-0");
    assert_eq!(
        second_page[0].event_kind,
        Some(RequestEventKind::Messages),
        "list items project the materialized column"
    );
}

#[tokio::test]
async fn event_kind_cursor_backfill_uses_effective_kind() {
    let (_dir, storage) = migrated_storage("request-events-kind-cursor.sqlite").await;

    storage
        .append_request_event(&classified_event(
            0,
            BASE,
            Some(RequestEventKind::Messages),
            None,
        ))
        .await
        .expect("append messages event");
    storage
        .append_request_event(&classified_event(
            1,
            BASE + 1,
            Some(RequestEventKind::Messages),
            Some("renewal"),
        ))
        .await
        .expect("append renewal event");
    insert_legacy_row(&storage, 2, BASE + 2, None).await;

    let cursor = storage
        .current_request_event_cursor()
        .await
        .expect("current cursor");

    let backfill = |event_kind| {
        let storage = &storage;
        async move {
            storage
                .query_request_events_between_cursors(
                    0,
                    cursor,
                    500,
                    &RequestEventStreamFilters {
                        event_kind: Some(event_kind),
                        ..Default::default()
                    },
                )
                .await
                .expect("cursor backfill")
                .into_iter()
                .map(|(_, event)| event.request_id)
                .collect::<Vec<_>>()
        }
    };

    assert_eq!(
        backfill(RequestEventKind::Messages).await,
        vec!["req-kind-0"]
    );
    assert_eq!(
        backfill(RequestEventKind::Renewal).await,
        vec!["req-kind-1"]
    );
    assert_eq!(
        backfill(RequestEventKind::Unclassified).await,
        vec!["req-legacy-2"],
        "NULL event_kind is unclassified in the delta path too"
    );
}

#[tokio::test]
async fn event_kind_roundtrips_through_event_reads() {
    let (_dir, storage) = migrated_storage("request-events-kind-read.sqlite").await;

    let event = classified_event(0, BASE, Some(RequestEventKind::CountTokens), None);
    storage
        .append_request_event(&event)
        .await
        .expect("append event");

    let stored = storage
        .get_request_event(event.event_id.as_deref().expect("event id"))
        .await
        .expect("get event")
        .expect("event present");
    assert_eq!(stored.event_kind, Some(RequestEventKind::CountTokens));

    let cursor = storage
        .current_request_event_cursor()
        .await
        .expect("current cursor");
    let streamed = storage
        .query_request_events_between_cursors(0, cursor, 10, &RequestEventStreamFilters::default())
        .await
        .expect("cursor events");
    assert_eq!(streamed.len(), 1);
    assert_eq!(
        streamed[0].1.event_kind,
        Some(RequestEventKind::CountTokens)
    );
}
