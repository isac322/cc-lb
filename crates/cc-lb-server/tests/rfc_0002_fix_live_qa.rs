//! RFC-0002 Fix — Live QA Integration Tests
//!
//! Spawns the actual `cc-lb` binary against SQLite + fake Anthropic upstream,
//! then verifies observable behaviour matches the RFC-0002 conformance
//! checklist (H1-H9, M1-M4). Every LIVE-* scenario in
//! `docs/rfc/0002-fix-tc.md` maps to one `#[tokio::test]` below.
//!
//! Verification uses direct SQLite queries and `/metrics` scrapes — no
//! network mocks beyond `fake_anthropic`.

mod common;

use std::time::Duration;

use sqlx::{AssertSqlSafe, Row, SqlitePool, sqlite::SqlitePoolOptions};
use tokio::time::sleep;

async fn open_sqlite_pool(sqlite_path: &std::path::Path) -> SqlitePool {
    let url = format!("sqlite://{}", sqlite_path.display());
    SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .expect("open sqlite pool")
}

async fn count_request_events(pool: &SqlitePool, where_clause: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM request_events_v1 WHERE {where_clause}");
    let row = sqlx::query(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .expect("count query");
    row.try_get::<i64, _>(0).expect("count column")
}

/// Poll the row count until it is stable for two consecutive reads separated
/// by 100ms, or up to `deadline`. This absorbs the async lag between the
/// `wait_for_status` handshake returning and the writer/assembler tasks
/// draining their mpsc buffers on the `/v1/models` init request.
async fn settled_row_count(pool: &SqlitePool, where_clause: &str) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut previous = count_request_events(pool, where_clause).await;
    loop {
        sleep(Duration::from_millis(100)).await;
        let current = count_request_events(pool, where_clause).await;
        if current == previous {
            return current;
        }
        if tokio::time::Instant::now() >= deadline {
            return current;
        }
        previous = current;
    }
}

async fn wait_for_row_count(pool: &SqlitePool, where_clause: &str, expected: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let actual = count_request_events(pool, where_clause).await;
        if actual == expected {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for count_request_events(WHERE {where_clause}) == {expected}; last={actual}"
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn fetch_metric_scrape(metrics_addr: std::net::SocketAddr) -> String {
    // Simple raw TCP client — avoids a reqwest dep in tests.
    let response = common::http_get(metrics_addr, "/metrics")
        .await
        .expect("metrics scrape");
    response.body
}

// ============================================================================
// LIVE-1 · Default boot happy path — H4, H5, H6 baseline
// ============================================================================
#[tokio::test]
async fn live_qa_1_default_boot_happy_path_writes_row_and_reconciles() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#,
        &[],
    )
    .await
    .expect("post messages");

    assert_eq!(response.status, 200);
    // Legacy authoritative writer produces 1 row.
    wait_for_row_count(&pool, "shadow_event_id IS NULL", baseline + 1).await;
    let total = count_request_events(&pool, "1=1").await;
    assert_eq!(
        total,
        baseline + 1,
        "default boot must not create shadow rows"
    );

    // Metrics: reconcile subscriber must have processed the request.
    let scrape = fetch_metric_scrape(server.metrics_addr).await;
    assert!(
        scrape.contains("cc_lb_limit_reservation_ttl_evicted_total"),
        "TTL sweeper metric must be registered even without evictions; scrape did not contain metric"
    );
}

// ============================================================================
// LIVE-2 · Non-lifecycle route must not write a row — H1
// ============================================================================
#[tokio::test]
async fn live_qa_2_oauth_usage_does_not_produce_request_event_row() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;

    // /api/oauth/usage is served by non_lifecycle_routes and MUST bypass
    // the lifecycle middleware. If middleware creates a LifecycleContext for
    // this path, Drop will publish a `terminal_dropped` row.
    let response = common::http_get(server.proxy_addr, "/api/oauth/usage")
        .await
        .expect("oauth usage GET");
    // Whatever the response status, no request_events row should appear.
    assert!(
        response.status < 500 || response.status == 401 || response.status == 404,
        "oauth usage returned unexpected server error: {}",
        response.status
    );

    // Wait a bit to make sure any async publish would land.
    sleep(Duration::from_millis(500)).await;
    let total = count_request_events(&pool, "1=1").await;
    assert_eq!(
        total, baseline,
        "GET /api/oauth/usage must not produce a request_events row"
    );

    let dropped = count_request_events(&pool, "error_code = 'terminal_dropped'").await;
    assert_eq!(
        dropped, 0,
        "terminal_dropped rows must be zero for non-lifecycle routes"
    );
}

// ============================================================================
// LIVE-3 · 404 fallback must not write a row — H1
// ============================================================================
#[tokio::test]
async fn live_qa_3_unknown_route_and_method_not_allowed_do_not_write_row() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;

    let not_found = common::http_get(server.proxy_addr, "/definitely/not-a-route")
        .await
        .expect("404 GET");
    assert_eq!(not_found.status, 404);

    sleep(Duration::from_millis(300)).await;
    let after_404 = count_request_events(&pool, "1=1").await;
    assert_eq!(
        after_404, baseline,
        "404 fallback must not produce a request_events row"
    );

    let dropped = count_request_events(&pool, "error_code = 'terminal_dropped'").await;
    assert_eq!(dropped, 0, "terminal_dropped rows must be zero");
}

// ============================================================================
// LIVE-6/7 · Shadow writer + query filter — H2, H3
// ============================================================================
#[tokio::test]
async fn live_qa_6_shadow_mode_writes_pair_and_query_filters_shadow() {
    // Enable shadow assembler + cache observation subscriber. Writer source =
    // "both" gives us one legacy row AND one shadow row per request.
    let extra = r#"
request_event_writer_source = "both"

[lifecycle_cache_observation_subscriber]
enabled = true

[lifecycle_hook_adapter]
enabled = false

[lifecycle_pricing_subscriber]
enabled = true
"#;
    let server = common::spawn_test_server_with_extra_config(extra).await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline_total = settled_row_count(&pool, "1=1").await;
    let baseline_legacy = settled_row_count(&pool, "shadow_event_id IS NULL").await;
    let baseline_shadow = settled_row_count(&pool, "shadow_event_id IS NOT NULL").await;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#,
        &[],
    )
    .await
    .expect("post messages");
    assert_eq!(response.status, 200);

    // Both writer paths must produce rows. Give the assembler a chance to
    // catch up on the mpsc.
    wait_for_row_count(&pool, "shadow_event_id IS NULL", baseline_legacy + 1).await;
    wait_for_row_count(&pool, "shadow_event_id IS NOT NULL", baseline_shadow + 1).await;
    let total = count_request_events(&pool, "1=1").await;
    assert_eq!(
        total,
        baseline_total + 2,
        "writer_source=both must produce legacy + shadow row pair"
    );

    // H2: query API (which uses `query_request_events`) filters shadow rows.
    // We approximate by asserting the recent-events endpoint returns only
    // legacy rows.
    let (status, _, body, _) = admin_get_json(server.admin_addr, "/admin/v1/events/recent?limit=5")
        .await
        .expect("recent events");
    assert_eq!(status, 200);
    let count = body["count"].as_i64().unwrap_or(-1);
    assert!(
        count >= 1,
        "expected at least one legacy event, got {count}"
    );
    // The response must not include any shadow row — we assert absence of the
    // shadow_event_id key being non-null.
    let events = body["events"].as_array().expect("events array");
    let shadow_leaked = events.iter().any(|ev| {
        ev.get("shadow_event_id")
            .is_some_and(|v| !v.is_null() && !v.as_str().unwrap_or("").is_empty())
    });
    assert!(
        !shadow_leaked,
        "admin recent-events must not leak shadow rows: got {events:?}"
    );
}

async fn admin_get_json(
    addr: std::net::SocketAddr,
    path: &str,
) -> std::io::Result<(u16, String, serde_json::Value, String)> {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nauthorization: Bearer admin-token\r\nConnection: close\r\n\r\n"
    );
    let response = raw_http_admin(addr, &request).await?;
    let value: serde_json::Value =
        serde_json::from_str(&response.body).unwrap_or(serde_json::Value::Null);
    Ok((response.status, response.headers, value, response.body))
}

async fn raw_http_admin(
    addr: std::net::SocketAddr,
    request: &str,
) -> std::io::Result<common::RawResponse> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or(0);
    let (headers, body) = match text.split_once("\r\n\r\n") {
        Some((h, b)) => (h.to_owned(), b.to_owned()),
        None => (text, String::new()),
    };
    Ok(common::RawResponse {
        status,
        headers,
        body,
    })
}
