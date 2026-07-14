use crate::common;

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use std::fs;
use std::time::Duration;
use tempfile::TempDir;

async fn open_sqlite_pool(sqlite_path: &std::path::Path) -> SqlitePool {
    let url = format!("sqlite://{}", sqlite_path.display());
    SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .expect("open sqlite pool")
}

async fn count_capture_records_by_request_id(pool: &SqlitePool, request_id: &str) -> i64 {
    let row = sqlx::query("SELECT COUNT(*) FROM capture_v1 WHERE request_id = ?")
        .bind(request_id)
        .fetch_one(pool)
        .await
        .expect("count query");
    row.try_get::<i64, _>(0).expect("count column")
}

// (1) enabled config under temp data_dir drives one real routed request and, after graceful server shutdown/drain, creates separate capture.sqlite with exactly one joined row; prove capture DB is distinct from main storage and avoid a brittle claim if legitimate request logging mutates storage.sqlite;
#[tokio::test]
async fn test_capture_enabled() {
    // Given: A temporary directory and a capture database path configured with enabled=true
    let temp_dir = TempDir::new().expect("create temp dir");
    let capture_db_path = temp_dir.path().join("capture.sqlite");

    let extra_toml = format!(
        r#"[capture]
enabled = true
path = "{}"
"#,
        capture_db_path.display()
    );

    let mut server = common::spawn_test_server_with_extra_config(&extra_toml).await;

    // When: Driving one real routed request with a unique request ID
    let unique_request_id = format!("test-req-{}", uuid::Uuid::new_v4());
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
        &[("request-id", &unique_request_id)],
    )
    .await
    .expect("post messages");

    // Then: The request succeeds, and after graceful shutdown, capture.sqlite is created with exactly one row
    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""type":"message""#));

    // Gracefully shut down the server to flush/drain capture records
    server.graceful_shutdown();

    // Prove capture DB is distinct from main storage
    assert_ne!(capture_db_path, server.sqlite_path);

    // Verify capture DB exists and has exactly one row for our request
    assert!(
        capture_db_path.exists(),
        "capture database file should exist"
    );

    let pool = open_sqlite_pool(&capture_db_path).await;
    let count = count_capture_records_by_request_id(&pool, &unique_request_id).await;
    assert_eq!(
        count, 1,
        "capture database should contain exactly one row for our request"
    );

    // Prove one joined row for one capturable routed request
    let row = sqlx::query(
        "SELECT canonical_model, client_status, payload_json FROM capture_v1 WHERE request_id = ?",
    )
    .bind(&unique_request_id)
    .fetch_one(&pool)
    .await
    .expect("fetch captured row");
    let canonical_model: Option<String> = row.try_get("canonical_model").expect("canonical_model");
    let client_status: Option<i32> = row.try_get("client_status").expect("client_status");

    assert_eq!(
        canonical_model.as_deref(),
        Some("claude-3-5-sonnet-20241022")
    );
    assert_eq!(client_status, Some(200));
}

// (2) enabled=false serves request and creates no capture DB;
#[tokio::test]
async fn test_capture_disabled() {
    // Given: A temporary directory and a capture database path configured with enabled=false
    let temp_dir = TempDir::new().expect("create temp dir");
    let capture_db_path = temp_dir.path().join("capture.sqlite");

    let extra_toml = format!(
        r#"[capture]
enabled = false
path = "{}"
"#,
        capture_db_path.display()
    );

    let mut server = common::spawn_test_server_with_extra_config(&extra_toml).await;

    // When: Driving one real routed request
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
        &[],
    )
    .await
    .expect("post messages");

    // Then: The request succeeds, and after graceful shutdown, no capture database is created
    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""type":"message""#));

    // Gracefully shut down the server
    server.graceful_shutdown();

    // Verify capture DB does NOT exist
    assert!(
        !capture_db_path.exists(),
        "capture database file should not exist when disabled"
    );
}

// (3) deterministic open failure (e.g. capture path under a missing/non-directory parent or an existing directory path) still boots and serves normal request, creates no capture DB, and logs a capture-disabled warning if harness exposes logs.
#[tokio::test]
async fn test_capture_open_failure() {
    // Given: A capture path under a non-directory parent to cause a deterministic open failure
    let temp_dir = TempDir::new().expect("create temp dir");
    let file_path = temp_dir.path().join("file.txt");
    fs::write(&file_path, "not a directory").expect("write file");
    let capture_db_path = file_path.join("capture.sqlite");

    let extra_toml = format!(
        r#"[capture]
enabled = true
path = "{}"
"#,
        capture_db_path.display()
    );

    let mut server = common::spawn_test_server_with_extra_config(&extra_toml).await;

    // When: Driving one real routed request
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
        &[],
    )
    .await
    .expect("post messages");

    // Then: The server still boots and serves the request successfully, creates no capture DB, and logs a warning
    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""type":"message""#));

    // Gracefully shut down the server
    server.graceful_shutdown();

    // Verify capture DB does NOT exist
    assert!(
        !capture_db_path.exists(),
        "capture database file should not exist on open failure"
    );

    // Verify warning log is exposed and directly assert stable lowercase warning substring contract
    let stderr = server.finish_stderr().to_lowercase();
    println!("Server stderr:\n{}", stderr);
    assert!(stderr.contains("capture"), "stderr should contain capture");
    assert!(
        stderr.contains("disabled") || stderr.contains("failed to open"),
        "stderr should contain disabled or failed to open"
    );
}
