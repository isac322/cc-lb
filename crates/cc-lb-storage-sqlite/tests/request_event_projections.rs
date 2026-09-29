use std::sync::Arc;

use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveTurnRow, CacheTtl, MetaStore, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use uuid::Uuid;

async fn storage() -> (tempfile::TempDir, cc_lb_storage_sqlite::SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-event-projections.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage)
}

fn event(event_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000,
        ts_ms: Some(1_800_000_000_000),
        request_id: "renewal-request".to_owned(),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("session-hash:7".to_owned()),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("principal-a".to_owned()),
        upstream_id: Some(Uuid::from_u128(7)),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 12,
        ..Default::default()
    }
}

fn projections(source_ref_id: &str) -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: source_ref_id.to_owned(),
            session_key_hash: "session-hash".to_owned(),
            principal_id: "principal-a".to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(7),
            model: "claude-sonnet-4-5".to_owned(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_input_tokens: 10,
            cache_creation_input_tokens_5m: 10,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 70,
            cost_micros: 123_456,
            hit_miss: "hit".to_owned(),
            ts: 1_800_000_000,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: "principal-a".to_owned(),
            session_key_hash: Some("session-hash".to_owned()),
            upstream_id: Uuid::from_u128(7),
            decision: "reschedule".to_owned(),
            reason: "cache_hit".to_owned(),
            error: None,
            generation: 7,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: None,
            last_message_at_ms: 1_800_000_000_000,
            ts: 1_800_000_000,
        },
    }
}

async fn row_counts(storage: &cc_lb_storage_sqlite::SqliteStorage) -> (i64, i64, i64) {
    let request_events = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(storage.pool())
        .await
        .expect("count request events");
    let turns = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(storage.pool())
        .await
        .expect("count keepalive turns");
    let decisions = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
        .fetch_one(storage.pool())
        .await
        .expect("count keepalive decisions");
    (request_events, turns, decisions)
}

#[tokio::test]
async fn append_with_projections_commits_all_rows_when_valid() {
    let (_temp_dir, storage) = storage().await;
    let event = event("renewal-event-success");
    let projections = projections("session-hash:7");

    let cursor = storage
        .append_request_event_with_projections(&event, &projections)
        .await
        .expect("append projections");

    assert_eq!(cursor, 1);
    assert_eq!(row_counts(&storage).await, (1, 1, 1));
}

#[tokio::test]
async fn append_with_projections_rolls_back_all_rows_when_turn_insert_fails() {
    let (_temp_dir, storage) = storage().await;
    sqlx::query(
        "CREATE TRIGGER reject_keepalive_turn BEFORE INSERT ON cache_keepalive_turns \
         BEGIN SELECT RAISE(ABORT, 'forced keepalive turn failure'); END",
    )
    .execute(storage.pool())
    .await
    .expect("install failure trigger");

    let error = storage
        .append_request_event_with_projections(
            &event("renewal-event-rollback"),
            &projections("session-hash:7"),
        )
        .await
        .expect_err("turn insert must fail");

    assert!(error.to_string().contains("forced keepalive turn failure"));
    assert_eq!(row_counts(&storage).await, (0, 0, 0));
}

#[tokio::test]
async fn append_with_projections_inserts_one_projection_set_when_base_event_exists() {
    let (_temp_dir, storage) = storage().await;
    let event = event("renewal-event-existing-base");
    storage
        .append_request_event(&event)
        .await
        .expect("append base event");

    let first_cursor = storage
        .append_request_event_with_projections(&event, &projections("session-hash:7"))
        .await
        .expect("append projections after base event");
    let second_cursor = storage
        .append_request_event_with_projections(&event, &projections("session-hash:7"))
        .await
        .expect("replay projections");

    assert_eq!(first_cursor, 1);
    assert_eq!(second_cursor, first_cursor);
    assert_eq!(row_counts(&storage).await, (1, 1, 1));
}

#[tokio::test]
async fn append_request_event_leaves_projection_tables_empty_for_proxy_rows() {
    let (_temp_dir, storage) = storage().await;
    let mut event = event("proxy-event-single-insert");
    event.source_kind = Some("proxy".to_owned());
    event.source_ref_id = None;

    storage
        .append_request_event(&event)
        .await
        .expect("append proxy event");

    assert_eq!(row_counts(&storage).await, (1, 0, 0));
}
