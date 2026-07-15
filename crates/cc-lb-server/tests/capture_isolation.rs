use crate::common;

use std::path::Path;
use std::time::Duration;

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use tempfile::TempDir;

const REQUEST_ID: &str = "capture-isolation-equivalent-request";

#[derive(Debug, PartialEq, Eq)]
struct StorageSnapshot {
    schema: String,
    request_event_count: i64,
}

async fn open_sqlite_pool(path: &Path) -> SqlitePool {
    let url = format!("sqlite://{}", path.display());
    SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .expect("open sqlite pool")
}

async fn storage_snapshot(path: &Path) -> StorageSnapshot {
    let pool = open_sqlite_pool(path).await;
    let schema_rows = sqlx::query(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name, tbl_name",
    )
    .fetch_all(&pool)
    .await
    .expect("read storage schema");
    let schema = schema_rows
        .iter()
        .map(|row| {
            let object_type: String = row.try_get("type").expect("schema type");
            let name: String = row.try_get("name").expect("schema name");
            let table: String = row.try_get("tbl_name").expect("schema table");
            let sql: Option<String> = row.try_get("sql").expect("schema SQL");
            format!(
                "{object_type}\0{name}\0{table}\0{}",
                sql.unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let request_event_count = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(&pool)
        .await
        .expect("count request events");
    StorageSnapshot {
        schema,
        request_event_count,
    }
}

fn payloads_contain_marker(payloads: &[String], marker: &str) -> bool {
    payloads.iter().any(|payload| payload.contains(marker))
}

async fn drive_request(
    capture_enabled: bool,
    capture_path: &Path,
    body: &str,
) -> common::TestServer {
    let extra_toml = format!(
        r#"[capture]
enabled = {capture_enabled}
path = "{}"
"#,
        capture_path.display()
    );
    let mut server = common::spawn_test_server_with_extra_config(&extra_toml).await;
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        body,
        &[("request-id", REQUEST_ID)],
    )
    .await
    .expect("post messages");
    assert_eq!(response.status, 200);
    server.graceful_shutdown();
    server
}

#[tokio::test]
async fn capture_preserves_main_storage_and_excludes_prompt_marker() {
    // Given: equivalent fresh servers and a prompt marker unique to this test run.
    let capture_on_dir = TempDir::new().expect("create capture-on directory");
    let capture_off_dir = TempDir::new().expect("create capture-off directory");
    let capture_on_path = capture_on_dir.path().join("capture.sqlite");
    let capture_off_path = capture_off_dir.path().join("capture.sqlite");
    let marker = format!("UNIQUE_PII_MARKER_{}", uuid::Uuid::new_v4());
    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "messages": [{"role": "user", "content": marker}],
        "max_tokens": 10
    })
    .to_string();

    // When: the same routed request runs once with capture on and once with capture off.
    let capture_on = drive_request(true, &capture_on_path, &body).await;
    let capture_off = drive_request(false, &capture_off_path, &body).await;
    let capture_on_storage = storage_snapshot(&capture_on.sqlite_path).await;
    let capture_off_storage = storage_snapshot(&capture_off.sqlite_path).await;
    let capture_pool = open_sqlite_pool(&capture_on_path).await;
    let payloads: Vec<String> = sqlx::query_scalar("SELECT payload_json FROM capture_v1")
        .fetch_all(&capture_pool)
        .await
        .expect("read capture payloads");
    let routed_request_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1 WHERE request_id = ?")
            .bind(REQUEST_ID)
            .fetch_one(&capture_pool)
            .await
            .expect("count routed capture rows");

    // Then: capture changes neither main schema nor row count and stores no raw prompt marker.
    assert_eq!(capture_on_storage.schema, capture_off_storage.schema);
    assert_eq!(
        capture_on_storage.request_event_count,
        capture_off_storage.request_event_count
    );
    assert_eq!(routed_request_count, 1);
    assert!(!payloads.is_empty());
    assert!(!payloads_contain_marker(&payloads, &marker));
    assert!(!capture_off_path.exists());
}

#[test]
fn payload_marker_scan_detects_synthetic_raw_body_leak() {
    // Given: a synthetic payload containing the exact marker a raw-body leak would preserve.
    let marker = "UNIQUE_PII_MARKER_NEGATIVE_CONTROL";
    let payloads = vec![format!(r#"{{"raw_body":"{marker}"}}"#)];

    // When/Then: the same detector used by the integration test flags the leak.
    assert!(payloads_contain_marker(&payloads, marker));
}
