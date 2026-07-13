use std::sync::Arc;

use cc_lb_storage_api::{BackendKind, MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::Row;

#[tokio::test]
async fn migrated_schema_has_nullable_text_reasoning_effort() {
    let (_temp_dir, storage) = test_storage("reasoning-effort-schema.sqlite").await;

    let columns = sqlx::query("PRAGMA table_info('request_events_v1')")
        .fetch_all(storage.pool())
        .await
        .expect("read request_events_v1 schema");
    let column = columns
        .iter()
        .find(|row| row.get::<String, _>("name") == "reasoning_effort")
        .expect("reasoning_effort column");

    assert_eq!(column.get::<String, _>("type"), "TEXT");
    assert_eq!(column.get::<i64, _>("notnull"), 0);
}

#[tokio::test]
async fn append_request_event_persists_reasoning_effort_value() {
    let (_temp_dir, storage) = test_storage("reasoning-effort-some.sqlite").await;
    let event = request_event("reasoning-effort-some", Some("max".to_owned()));

    storage
        .append_request_event(&event)
        .await
        .expect("append event with reasoning effort");
    let stored = sqlx::query_scalar::<_, Option<String>>(
        "SELECT reasoning_effort FROM request_events_v1 WHERE event_id = ?",
    )
    .bind(event.event_id.as_deref())
    .fetch_one(storage.pool())
    .await
    .expect("read persisted reasoning effort");

    assert_eq!(stored.as_deref(), Some("max"));
}

#[tokio::test]
async fn append_request_event_accepts_null_reasoning_effort() {
    let (_temp_dir, storage) = test_storage("reasoning-effort-none.sqlite").await;
    let event = request_event("reasoning-effort-none", None);

    storage
        .append_request_event(&event)
        .await
        .expect("append event without reasoning effort");
    let stored = sqlx::query_scalar::<_, Option<String>>(
        "SELECT reasoning_effort FROM request_events_v1 WHERE event_id = ?",
    )
    .bind(event.event_id.as_deref())
    .fetch_one(storage.pool())
    .await
    .expect("read null reasoning effort");

    assert_eq!(stored, None);
}

async fn test_storage(filename: &str) -> (tempfile::TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(filename).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    (temp_dir, storage)
}

fn request_event(event_id: &str, reasoning_effort: Option<String>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        reasoning_effort,
        status: 200,
        duration_ms: 89,
        ..Default::default()
    }
}
