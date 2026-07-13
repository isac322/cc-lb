#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

#[derive(Debug)]
pub struct JoinedRow {
    pub disposition: String,
    pub attempt_num: Option<i64>,
    pub upstream_status: Option<i64>,
    pub client_status: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub cache_creation_5m: Option<i64>,
    pub cache_creation_1h: Option<i64>,
}

pub const MESSAGE_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"capture matrix"}],"max_tokens":10}"#;

pub fn capture_config(path: &Path, channel_capacity: usize) -> String {
    format!(
        r#"[capture]
enabled = true
path = "{}"
channel_capacity = {channel_capacity}
"#,
        path.display()
    )
}

pub async fn open_capture_pool(path: &Path) -> SqlitePool {
    let url = format!("sqlite://{}", path.display());
    SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .expect("open capture sqlite pool")
}

pub async fn count_rows(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
        .fetch_one(pool)
        .await
        .expect("count capture rows")
}

pub async fn count_distinct_event_ids(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(DISTINCT event_id) FROM capture_v1")
        .fetch_one(pool)
        .await
        .expect("count distinct capture event ids")
}

pub async fn count_request_id(pool: &SqlitePool, request_id: &str) -> i64 {
    let row = sqlx::query("SELECT COUNT(*) AS count FROM capture_v1 WHERE request_id = ?")
        .bind(request_id)
        .fetch_one(pool)
        .await
        .expect("count matching capture request ids");
    row.try_get("count").expect("capture request-id count")
}

pub async fn joined_row(pool: &SqlitePool, request_id: &str) -> JoinedRow {
    let row = sqlx::query(
        "SELECT disposition, attempt_num, upstream_status, client_status, input_tokens, \
         output_tokens, cache_read_input_tokens, cache_creation_5m, cache_creation_1h \
         FROM capture_v1 WHERE request_id = ?",
    )
    .bind(request_id)
    .fetch_one(pool)
    .await
    .expect("fetch joined capture row");
    JoinedRow {
        disposition: row.try_get("disposition").expect("capture disposition"),
        attempt_num: row.try_get("attempt_num").expect("capture attempt number"),
        upstream_status: row
            .try_get("upstream_status")
            .expect("capture upstream status"),
        client_status: row.try_get("client_status").expect("capture client status"),
        input_tokens: row.try_get("input_tokens").expect("capture input tokens"),
        output_tokens: row.try_get("output_tokens").expect("capture output tokens"),
        cache_read_input_tokens: row
            .try_get("cache_read_input_tokens")
            .expect("capture cache-read tokens"),
        cache_creation_5m: row
            .try_get("cache_creation_5m")
            .expect("capture five-minute cache-creation tokens"),
        cache_creation_1h: row
            .try_get("cache_creation_1h")
            .expect("capture one-hour cache-creation tokens"),
    }
}

pub async fn capture_record(
    pool: &SqlitePool,
    request_id: &str,
) -> cc_lb_capture::schema::CaptureRecord {
    let payload: String =
        sqlx::query_scalar("SELECT payload_json FROM capture_v1 WHERE request_id = ?")
            .bind(request_id)
            .fetch_one(pool)
            .await
            .expect("fetch capture payload");
    serde_json::from_str(&payload).expect("decode capture record")
}

pub async fn disconnect_after_message_start(
    addr: std::net::SocketAddr,
    request_id: &str,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let body = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"disconnect"}],"max_tokens":10,"stream":true}"#;
    let request = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\naccept: text/event-stream\r\nx-fake-mode: slow\r\nrequest-id: {request_id}\r\ncontent-length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut buffer = [0_u8; 1_024];
        while !received
            .windows(b"event: message_start".len())
            .any(|window| window == b"event: message_start")
        {
            let read = stream.read(&mut buffer).await?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "stream ended before message_start",
                ));
            }
            received.extend_from_slice(&buffer[..read]);
        }
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(std::io::Error::other)??;
    stream.shutdown().await
}
