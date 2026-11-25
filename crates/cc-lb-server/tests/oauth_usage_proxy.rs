use crate::common;

use serde_json::Value;
use sqlx::{Row as _, sqlite::SqlitePoolOptions};
use std::time::Duration;

#[tokio::test]
async fn api_oauth_usage_returns_anthropic_usage_shape() {
    let server = common::spawn_inprocess_server().await;

    let response = common::proxy_get(
        server.proxy_addr,
        "/api/oauth/usage",
        &server.managed_key.plaintext,
    )
    .await
    .expect("usage route");

    assert_eq!(response.status, 200);
    let json: Value = serde_json::from_str(&response.body).expect("usage json");
    assert!(json.get("5h").is_some());
    assert!(json.get("7d").is_some());
    assert!(json.get("five_hour").is_none());
    assert!(json.get("windows").is_none());
    assert!(json.get("caveats").is_none());
}

#[tokio::test]
async fn api_oauth_usage_unauthenticated_persists_classified_failure_row() {
    let server = common::spawn_inprocess_server().await;

    let response = common::proxy_get(server.proxy_addr, "/api/oauth/usage", "invalid-key")
        .await
        .expect("unauthenticated usage route");

    assert_eq!(response.status, 401);
    let json: Value = serde_json::from_str(&response.body).expect("error json");
    assert_eq!(json["error"], "authentication_error");

    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .expect("open request event database");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query(
            "SELECT COUNT(*) AS count, MAX(list_status) AS status, MAX(error_code) AS error_code
             FROM request_events_v1
             WHERE error_code = 'authentication_failed'",
        )
        .fetch_one(&pool)
        .await
        .expect("query unauthenticated request event");
        if row.get::<i64, _>("count") == 1 {
            assert_eq!(row.get::<Option<i64>, _>("status"), Some(401));
            assert_eq!(
                row.get::<Option<String>, _>("error_code").as_deref(),
                Some("authentication_failed")
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for authentication_failed row on /api/oauth/usage"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
