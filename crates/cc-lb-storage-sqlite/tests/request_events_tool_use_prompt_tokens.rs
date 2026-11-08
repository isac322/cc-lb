use std::{str::FromStr, sync::Arc};

use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

#[tokio::test]
async fn migrated_schema_has_nullable_nonnegative_integer_tool_use_prompt_tokens() {
    let (_temp_dir, storage) = test_storage("tool-use-prompt-tokens-schema.sqlite").await;

    let columns = sqlx::query("PRAGMA table_info('request_events_v1')")
        .fetch_all(storage.pool())
        .await
        .expect("read request_events_v1 schema");
    let column = columns
        .iter()
        .find(|row| row.get::<String, _>("name") == "tool_use_prompt_tokens")
        .expect("tool_use_prompt_tokens column");

    assert_eq!(column.get::<String, _>("type"), "INTEGER");
    assert_eq!(column.get::<i64, _>("notnull"), 0);
}

#[tokio::test]
async fn append_and_read_request_event_round_trips_tool_use_prompt_tokens() {
    let (_temp_dir, storage) = test_storage("tool-use-prompt-tokens-some.sqlite").await;
    let event = request_event("tool-use-prompt-tokens-some", Some(18_000));

    storage
        .append_request_event(&event)
        .await
        .expect("append event with tool-use prompt tokens");
    let stored = storage
        .get_request_event(event.event_id.as_deref().expect("event id"))
        .await
        .expect("read request event")
        .expect("stored request event");

    assert_eq!(stored.tool_use_prompt_tokens, Some(18_000));
}

#[tokio::test]
async fn append_and_read_request_event_round_trips_null_tool_use_prompt_tokens() {
    let (_temp_dir, storage) = test_storage("tool-use-prompt-tokens-none.sqlite").await;
    let event = request_event("tool-use-prompt-tokens-none", None);

    storage
        .append_request_event(&event)
        .await
        .expect("append event without tool-use prompt tokens");
    let stored = storage
        .get_request_event(event.event_id.as_deref().expect("event id"))
        .await
        .expect("read request event")
        .expect("stored request event");

    assert_eq!(stored.tool_use_prompt_tokens, None);
}

#[tokio::test]
async fn migration_preserves_legacy_request_event_with_null_tool_use_prompt_tokens() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("tool-use-prompt-tokens-migration.sqlite")
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
        .run_direct(Some(103), &mut connection, false)
        .await
        .expect("apply migrations through version 103");
    sqlx::query(
        "INSERT INTO request_events_v1 (
            request_id, ts, event_id, input_tokens, output_tokens,
            list_status, list_duration_ms
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("legacy-request")
    .bind(1_800_300_100_i64)
    .bind("legacy-event")
    .bind(12_i64)
    .bind(34_i64)
    .bind(200_i64)
    .bind(89_i64)
    .execute(&mut connection)
    .await
    .expect("insert pre-migration request event");
    connection.close().await.expect("close seed connection");

    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open pre-migration sqlite");
    storage
        .initialize()
        .await
        .expect("apply current migrations");
    let stored = storage
        .get_request_event("legacy-event")
        .await
        .expect("read migrated request event")
        .expect("migrated request event");

    assert_eq!(stored.input_tokens, Some(12));
    assert_eq!(stored.output_tokens, Some(34));
    assert_eq!(stored.tool_use_prompt_tokens, None);
}

async fn test_storage(filename: &str) -> (tempfile::TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(filename).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage)
}

fn request_event(event_id: &str, tool_use_prompt_tokens: Option<u64>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        ts_ms: Some(1_800_300_100_000),
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        tool_use_prompt_tokens,
        status: 200,
        duration_ms: 89,
        ..Default::default()
    }
}
