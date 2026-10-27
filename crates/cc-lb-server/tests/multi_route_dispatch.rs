use std::time::Duration;

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::common;

#[tokio::test]
async fn multi_route_dispatch() {
    let server = common::spawn_test_server().await;
    let messages = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[],"max_tokens":1}"#,
        &[],
    )
    .await
    .expect("messages route");
    let models = common::proxy_get(
        server.proxy_addr,
        "/v1/models",
        &server.managed_key.plaintext,
    )
    .await
    .expect("models route");
    let files = common::proxy_get(
        server.proxy_addr,
        "/v1/files",
        &server.managed_key.plaintext,
    )
    .await
    .expect("files route");
    let delete_file = common::http_delete(
        server.proxy_addr,
        "/v1/files/file_abc123",
        &server.managed_key.plaintext,
    )
    .await
    .expect("delete file route");
    let count_tokens_body = r#"{"model":"claude-3-5-sonnet-20241022","messages":[]}"#;
    let count_tokens = common::http_post(
        server.proxy_addr,
        "/v1/messages/count_tokens",
        &server.managed_key.plaintext,
        count_tokens_body,
        &[],
    )
    .await
    .expect("count tokens route");
    let unknown = common::http_post(
        server.proxy_addr,
        "/v1/unknown",
        &server.managed_key.plaintext,
        "{}",
        &[],
    )
    .await
    .expect("unknown route");

    assert_eq!(unknown.status, 404);
    assert!(unknown.body.contains(r#""type":"operation_not_supported""#));

    assert_eq!(messages.status, 200);
    assert!(messages.body.contains(r#""type":"message""#));
    assert_eq!(count_tokens.status, 200);
    assert_eq!(count_tokens.body, r#"{"input_tokens":100}"#);
    for response in [models, files, delete_file] {
        assert_eq!(response.status, 404);
        assert!(
            response
                .body
                .contains(r#""type":"operation_not_supported""#)
        );
    }

    let pool = open_test_sqlite_pool(&server).await;
    let event = wait_for_count_tokens_event(&pool).await;
    assert_eq!(event.get::<i64, _>("event_count"), 1);
    assert_eq!(event.get::<String, _>("event_kind"), "count_tokens");
    assert_eq!(event.get::<String, _>("upstream_name"), "fake_anthropic");
    assert_eq!(event.get::<i64, _>("list_status"), 200);
    assert_eq!(
        event.get::<i64, _>("list_request_body_bytes"),
        count_tokens_body.len() as i64
    );
    assert_eq!(event.get::<i64, _>("input_tokens"), 0);
    assert_eq!(event.get::<i64, _>("output_tokens"), 0);
    assert_eq!(event.get::<i64, _>("list_cost_usd_micros"), 0);
}

async fn open_test_sqlite_pool(server: &common::TestServer) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .expect("open test sqlite")
}

async fn wait_for_count_tokens_event(pool: &SqlitePool) -> sqlx::sqlite::SqliteRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(event) = sqlx::query(
            "SELECT request_id, event_kind, upstream_name, list_status,
                    list_request_body_bytes, input_tokens, output_tokens, list_cost_usd_micros,
                    (SELECT COUNT(*) FROM request_events_v1
                     WHERE event_kind = 'count_tokens') AS event_count
             FROM request_events_v1
             WHERE event_kind = 'count_tokens'
             ORDER BY id DESC
             LIMIT 1",
        )
        .fetch_optional(pool)
        .await
        .expect("query count tokens event")
        {
            return event;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for count_tokens request event"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
