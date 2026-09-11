use cc_lb_storage_api::{BackendKind, MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::Row;

#[tokio::test]
async fn t3__migrated_schema_has_nullable_integer_thinking_budget_tokens() {
    let (_temp_dir, storage) = test_storage("thinking-budget-schema.sqlite").await;

    let columns = sqlx::query("PRAGMA table_info('request_events_v1')")
        .fetch_all(storage.pool())
        .await
        .expect("read request_events_v1 schema");
    let column = columns
        .iter()
        .find(|row| row.get::<String, _>("name") == "thinking_budget_tokens")
        .expect("thinking_budget_tokens column");

    assert_eq!(column.get::<String, _>("type"), "INTEGER");
    assert_eq!(column.get::<i64, _>("notnull"), 0);
}

#[tokio::test]
async fn t3__append_request_event_persists_thinking_budget_tokens_value() {
    let (_temp_dir, storage) = test_storage("thinking-budget-some.sqlite").await;
    let event = request_event("thinking-budget-some", Some(18_000));

    storage
        .append_request_event(&event)
        .await
        .expect("append event with thinking budget");
    let stored = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT thinking_budget_tokens FROM request_events_v1 WHERE event_id = ?",
    )
    .bind(event.event_id.as_deref())
    .fetch_one(storage.pool())
    .await
    .expect("read persisted thinking budget");

    assert_eq!(stored, Some(18_000));
}

#[tokio::test]
async fn t3__append_request_event_accepts_null_thinking_budget_tokens() {
    let (_temp_dir, storage) = test_storage("thinking-budget-none.sqlite").await;
    let event = request_event("thinking-budget-none", None);

    storage
        .append_request_event(&event)
        .await
        .expect("append event without thinking budget");
    let stored = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT thinking_budget_tokens FROM request_events_v1 WHERE event_id = ?",
    )
    .bind(event.event_id.as_deref())
    .fetch_one(storage.pool())
    .await
    .expect("read null thinking budget");

    assert_eq!(stored, None);
}

async fn test_storage(filename: &str) -> (tempfile::TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(filename).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    (temp_dir, storage)
}

fn request_event(event_id: &str, thinking_budget_tokens: Option<u64>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        thinking_budget_tokens,
        status: 200,
        duration_ms: 89,
        ..Default::default()
    }
}
