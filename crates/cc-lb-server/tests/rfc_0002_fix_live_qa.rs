//! RFC-0002 Fix — Live QA Integration Tests
//!
//! Spawns the actual `cc-lb` binary against SQLite + fake Anthropic upstream,
//! then verifies observable behaviour matches the RFC-0002 conformance
//! checklist (H1-H9, M1-M4). Every LIVE-* scenario in
//! `docs/rfc/0002-fix-tc.md` maps to one `#[tokio::test]` below.
//!
//! Verification uses direct SQLite queries and `/metrics` scrapes — no
//! network mocks beyond `fake_anthropic`.

use crate::common;

use std::net::SocketAddr;
use std::time::Duration;

use cc_lb_storage_api::principal::{Limit, LimitKind};
use fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse};
use http::StatusCode;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row, SqlitePool, sqlite::SqlitePoolOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::sleep;
use uuid::Uuid;

const HAPPY_MODEL: &str = "claude-sonnet-4-5-20250929";
const HAPPY_BODY: &str = r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;
const STREAM_BODY: &str = r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#;

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

/// Poll the row count until it is stable for THREE consecutive reads separated
/// by 300ms (i.e. 600ms of quiescence). Absorbs the async lag between the
/// `wait_for_status` handshake returning and the writer/assembler tasks
/// draining their mpsc buffers on the `/v1/models` init request; the
/// 300ms window must exceed the assembler's finalization grace (200ms)
/// so late Priced/CacheObserved arrivals are counted before we treat the
/// baseline as stable. Coverage-instrumented CI runs can pause the
/// assembler between arrivals longer than a single 300ms window, so the
/// two-consecutive check would return prematurely and race a delayed write;
/// requiring three tightens the quiescence signal without loosening the
/// downstream assertion. The 15s failsafe only fires when the assembler
/// truly cannot drain.
async fn settled_row_count(pool: &SqlitePool, where_clause: &str) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut previous = count_request_events(pool, where_clause).await;
    let mut stable_streak: u32 = 0;
    loop {
        sleep(Duration::from_millis(300)).await;
        let current = count_request_events(pool, where_clause).await;
        if current == previous {
            stable_streak += 1;
            if stable_streak >= 2 {
                return current;
            }
        } else {
            stable_streak = 0;
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

async fn fetch_payload_json(pool: &SqlitePool, where_clause: &str) -> serde_json::Value {
    let sql = format!(
        "SELECT payload FROM request_events_v1 WHERE {where_clause} ORDER BY id DESC LIMIT 1"
    );
    let row = sqlx::query(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .expect("payload row fetch");
    let payload: String = row.try_get("payload").expect("payload column");
    serde_json::from_str(&payload).expect("payload json parse")
}

async fn fetch_i64(pool: &SqlitePool, sql: &str) -> i64 {
    let row = sqlx::query(AssertSqlSafe(sql.to_owned()))
        .fetch_one(pool)
        .await
        .expect("fetch i64");
    row.try_get::<i64, _>(0).expect("i64 column")
}

async fn fetch_text(pool: &SqlitePool, sql: &str) -> String {
    let row = sqlx::query(AssertSqlSafe(sql.to_owned()))
        .fetch_one(pool)
        .await
        .expect("fetch text");
    row.try_get::<String, _>(0).expect("text column")
}

fn json_i64(json: &serde_json::Value, field: &str) -> i64 {
    json.get(field)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("missing integer field {field}: {json}"))
}

fn json_str<'a>(json: &'a serde_json::Value, field: &str) -> &'a str {
    json.get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field}: {json}"))
}

fn assert_json_field_populated(json: &serde_json::Value, field: &str) {
    assert!(
        json.get(field).is_some_and(|value| !value.is_null()),
        "payload missing populated field {field}: {}",
        serde_json::to_string_pretty(json).unwrap_or_default()
    );
}

fn diff_counter(pre: &str, post: &str, metric_line_prefix: &str) -> i64 {
    fn value(scrape: &str, prefix: &str) -> i64 {
        scrape
            .lines()
            .filter(|line| line.starts_with(prefix))
            .filter_map(|line| line.split_whitespace().last())
            .filter_map(|raw| raw.parse::<f64>().ok())
            .map(|value| value as i64)
            .sum()
    }

    value(post, metric_line_prefix) - value(pre, metric_line_prefix)
}

async fn wait_metric_delta(
    metrics_addr: SocketAddr,
    pre: &str,
    metric_line_prefix: &str,
    expected_delta: i64,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let post = fetch_metric_scrape(metrics_addr).await;
        if diff_counter(pre, &post, metric_line_prefix) == expected_delta {
            return post;
        }
        if tokio::time::Instant::now() >= deadline {
            return post;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn post_happy(server: &common::TestServer) -> common::RawResponse {
    common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        HAPPY_BODY,
        &[],
    )
    .await
    .expect("post happy message")
}

async fn open_admin_sse(addr: SocketAddr) -> BufReader<TcpStream> {
    let mut stream = TcpStream::connect(addr).await.expect("connect admin sse");
    let request = format!(
        "GET /admin/events/stream HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer admin-token\r\nAccept: text/event-stream\r\nConnection: keep-alive\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write admin sse request");
    let mut reader = BufReader::new(stream);
    loop {
        let line = read_sse_line(&mut reader).await;
        if line.starts_with(": connected") {
            break;
        }
    }
    reader
}

async fn read_sse_line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    let bytes = tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .expect("timed out reading sse line")
        .expect("read sse line");
    assert!(bytes > 0, "sse stream closed before expected frame");
    line.trim_end().to_owned()
}

async fn next_sse_data(reader: &mut BufReader<TcpStream>) -> serde_json::Value {
    loop {
        let line = read_sse_line(reader).await;
        if let Some(data) = line.strip_prefix("data: ") {
            return serde_json::from_str(data).expect("sse data json");
        }
    }
}

fn assert_assembler_field_populated(row: &serde_json::Value, field: &str, ctx: &str) {
    let value = row.get(field);
    assert!(
        value.is_some_and(|v| !v.is_null()),
        "[{ctx}] assembler row missing {field} — indicates an assembler ordering bug (event dropped after RequestTerminated). value={value:?}\nfull payload:\n{}",
        serde_json::to_string_pretty(row).unwrap_or_default()
    );
}

// ============================================================================
// LIVE-1 · Default boot happy path — H4, H5, H6 baseline
// ============================================================================
#[tokio::test]
async fn live_qa_1_default_boot_happy_path_writes_row_and_reconciles() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;

    let response = common::http_post(server.proxy_addr, "/v1/messages", &server.managed_key.plaintext, r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#, &[])
    .await
    .expect("post messages");

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let total = count_request_events(&pool, "1=1").await;
    assert_eq!(
        total,
        baseline + 1,
        "default boot must produce exactly one assembler row"
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

    // /api/oauth/usage bypasses the proxy lifecycle. If middleware creates a
    // LifecycleContext for this path, Drop will publish a `terminal_dropped` row.
    let response = common::proxy_get(
        server.proxy_addr,
        "/api/oauth/usage",
        &server.managed_key.plaintext,
    )
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

    let not_found = common::proxy_get(
        server.proxy_addr,
        "/definitely/not-a-route",
        &server.managed_key.plaintext,
    )
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
// LIVE-6b · Assembler completeness — assembler ordering (Oracle B bug)
// ============================================================================
/// Regression for the assembler-ordering issue documented in the RFC-0002
/// synthesis analysis: the row written by LifecycleEventAssembler
/// must carry cost, cache, and usage fields. If `Priced` / `CacheObserved`
/// are consumed by their subscribers on separate async tasks, they can
/// arrive at the assembler AFTER `RequestTerminated` and be dropped,
/// leaving the row with NULL cost/cache and only partial usage.
#[tokio::test]
async fn live_qa_6b_assembler_populates_cost_cache_usage_fields() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;

    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = common::http_post(server.proxy_addr, "/v1/messages", &server.managed_key.plaintext, r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#, &[])
    .await
    .expect("post messages");
    assert_eq!(response.status, 200);

    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    sleep(Duration::from_millis(500)).await;

    let assembler_payload = fetch_payload_json(&pool, "event_id IS NOT NULL").await;

    let ctx = "non-stream happy path";
    assert_assembler_field_populated(&assembler_payload, "input_tokens", ctx);
    assert_assembler_field_populated(&assembler_payload, "output_tokens", ctx);
    assert_assembler_field_populated(&assembler_payload, "cost_usd_micros", ctx);
    assert_assembler_field_populated(&assembler_payload, "cost_input_micros", ctx);
    assert_assembler_field_populated(&assembler_payload, "cost_output_micros", ctx);
    assert_assembler_field_populated(&assembler_payload, "cache_state", ctx);
    assert_assembler_field_populated(&assembler_payload, "auth_ms", ctx);
    assert_assembler_field_populated(&assembler_payload, "route_ms", ctx);
    assert_assembler_field_populated(&assembler_payload, "upstream_ttfb_ms", ctx);
    assert_assembler_field_populated(&assembler_payload, "upstream_body_ms", ctx);
    assert_assembler_field_populated(&assembler_payload, "body_bytes", ctx);
    assert_assembler_field_populated(&assembler_payload, "body_chunk_count", ctx);
}

// ============================================================================
// LIVE-6c · Assembler — streaming request must populate stream fields
// ============================================================================
/// Streaming complements 6b: `StreamSuccess`-only fields (body_bytes,
/// stream_* timings) are None on non-stream and would hide divergences.
#[tokio::test]
async fn live_qa_6c_assembler_populates_stream_fields() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = common::http_post(server.proxy_addr, "/v1/messages", &server.managed_key.plaintext, r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#, &[("accept", "text/event-stream")])
    .await
    .expect("post streaming messages");
    assert_eq!(response.status, 200);

    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    sleep(Duration::from_millis(500)).await;

    let assembler_payload = fetch_payload_json(&pool, "event_id IS NOT NULL").await;

    let ctx = "streaming happy path";
    assert_assembler_field_populated(&assembler_payload, "body_bytes", ctx);
    assert_assembler_field_populated(&assembler_payload, "stream_message_start_ms", ctx);
    assert_assembler_field_populated(&assembler_payload, "stream_total_ms", ctx);
    assert_assembler_field_populated(&assembler_payload, "sse_event_count", ctx);
    assert_assembler_field_populated(&assembler_payload, "cost_usd_micros", ctx);
    assert_assembler_field_populated(&assembler_payload, "input_tokens", ctx);
    assert_assembler_field_populated(&assembler_payload, "output_tokens", ctx);
}

#[tokio::test]
async fn lqa_1a_proxy_path_non_stream_returns_message_and_assembler_row() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let total_baseline = settled_row_count(&pool, "1=1").await;
    let assembler_baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    let body: Value = serde_json::from_str(&response.body).expect("message json");
    assert_eq!(body["type"], "message");
    assert!(body["usage"]["input_tokens"].as_i64().unwrap_or_default() > 0);
    assert!(body["usage"]["output_tokens"].as_i64().unwrap_or_default() > 0);
    wait_for_row_count(&pool, "event_id IS NOT NULL", assembler_baseline + 1).await;
    assert_eq!(count_request_events(&pool, "1=1").await, total_baseline + 1);
}

#[tokio::test]
async fn lqa_1b_proxy_path_streaming_returns_sse_and_stream_payload_fields() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        STREAM_BODY,
        &[("accept", "text/event-stream")],
    )
    .await
    .expect("post stream message");

    assert_eq!(response.status, 200);
    assert!(response.body.contains("event: message_start"));
    assert!(response.body.contains("event: message_delta"));
    assert!(response.body.contains("event: message_stop"));
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let payload = fetch_payload_json(&pool, "event_id IS NOT NULL").await;
    for field in [
        "sse_event_count",
        "stream_message_start_ms",
        "stream_total_ms",
        "input_tokens",
        "output_tokens",
    ] {
        assert_json_field_populated(&payload, field);
    }
}

#[tokio::test]
async fn lqa_2a_event_emission_happy_row_has_columns_and_payload() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let where_clause = "event_id IS NOT NULL";
    assert_eq!(fetch_text(&pool, "SELECT principal_id FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await, "api-key");
    assert_eq!(
        fetch_text(&pool, "SELECT key_id FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await,
        server.managed_key.key_id
    );
    assert_eq!(fetch_text(&pool, "SELECT upstream_name FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await, "fake_anthropic");
    assert!(!fetch_text(&pool, "SELECT model FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await.is_empty());
    assert!(fetch_i64(&pool, "SELECT input_tokens FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await > 0);
    assert!(fetch_i64(&pool, "SELECT output_tokens FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await > 0);
    assert!(fetch_i64(&pool, "SELECT COUNT(*) FROM request_events_v1 WHERE event_id IS NOT NULL AND error_code IS NULL").await > 0);
    assert!(fetch_i64(&pool, "SELECT COUNT(*) FROM request_events_v1 WHERE event_id IS NOT NULL AND cache_state IS NOT NULL").await > 0);

    let payload = fetch_payload_json(&pool, where_clause).await;
    assert_eq!(json_i64(&payload, "status"), 200);
    assert_eq!(json_str(&payload, "principal_kind"), "machine");
    assert_eq!(json_str(&payload, "upstream"), "anthropic_direct");
    for field in [
        "cost_usd_micros",
        "cost_input_micros",
        "cost_output_micros",
        "auth_ms",
        "route_ms",
        "upstream_ttfb_ms",
        "upstream_body_ms",
        "body_bytes",
        "body_chunk_count",
    ] {
        assert_json_field_populated(&payload, field);
    }
    assert!(json_i64(&payload, "cost_usd_micros") > 0);
    assert!(json_i64(&payload, "cost_input_micros") > 0);
    assert!(json_i64(&payload, "cost_output_micros") > 0);
}

#[tokio::test]
async fn lqa_2b_lifecycle_metrics_increment_for_happy_non_stream() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let post = wait_metric_delta(
        server.metrics_addr,
        &pre,
        r#"cc_lb_lifecycle_events_total{kind="request_terminated"}"#,
        1,
    )
    .await;
    for kind in [
        "request_started",
        "parse_completed",
        "auth_completed",
        "authentication_completed",
        "route_completed",
        "limit_decision",
        "upstream_attempt",
        "upstream_response_started",
        "usage_observed",
        "request_terminated",
        "priced",
        "cache_observed",
    ] {
        let prefix = format!(r#"cc_lb_lifecycle_events_total{{kind="{kind}"}}"#);
        assert_eq!(diff_counter(&pre, &post, &prefix), 1, "metric {kind}");
    }
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_lifecycle_events_total{kind="provider_error_observed"}"#,
        ),
        0
    );
}

#[tokio::test]
async fn lqa_3a_bus_drop_counters_do_not_increment_under_burst() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    for _batch in 0..4 {
        let mut set = JoinSet::new();
        for _ in 0..50 {
            let addr = server.proxy_addr;
            let api_key = server.managed_key.plaintext.clone();
            set.spawn(async move {
                common::http_post(addr, "/v1/messages", &api_key, HAPPY_BODY, &[]).await
            });
        }
        while let Some(result) = set.join_next().await {
            let response = result.expect("join request").expect("burst request");
            assert_eq!(response.status, 200);
        }
    }

    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 200).await;
    let post = fetch_metric_scrape(server.metrics_addr).await;
    for reason in [
        "lifecycle_writer_full",
        "lifecycle_assembler_full",
        "lifecycle_pricing_full",
        "lifecycle_limit_reconcile_full",
        "lifecycle_cache_obs_full",
        "lifecycle_rate_limit_header_full",
        "lifecycle_subscription_quota_full",
        "lifecycle_limit_rejection_audit_full",
        "lifecycle_api_key_metrics_full",
        "lifecycle_cache_hit_miss_full",
        "sse_lagged",
    ] {
        let prefix = format!(r#"cc_lb_dropped_events_total{{reason="{reason}"}}"#);
        assert_eq!(
            diff_counter(&pre, &post, &prefix),
            0,
            "drop reason {reason}"
        );
    }
}

#[tokio::test]
async fn lqa_4a_admin_sse_stream_emits_final_request_event_update() {
    let server = common::spawn_test_server().await;
    let mut reader = open_admin_sse(server.admin_addr).await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    // The live-tail redesign (plan §3.9) precedes the final frame with a cursor
    // bookmark, optional partial snapshots, and heartbeats. Skip everything that
    // is not phase="final" so this test continues to assert on the terminal row.
    //
    // The SSE stream also carries `phase="final"` frames unrelated to this POST:
    // `common::spawn_test_server` runs a `GET /v1/models` readiness probe (see
    // `wait_for_proxy_ready`) that goes through the lifecycle pipeline and
    // publishes its own terminal frame with no model, no usage, and no cost —
    // sometimes still in flight (via backfill or the live bus) when this test
    // subscribes. Filter on `model == HAPPY_MODEL` so we assert on the frame
    // produced by `post_happy` rather than the readiness probe.
    let frame = loop {
        let frame = next_sse_data(&mut reader).await;
        if frame["phase"] == "final"
            && frame["payload"]["event"]["model"].as_str() == Some(HAPPY_MODEL)
        {
            break frame;
        }
    };
    let event = &frame["payload"]["event"];
    assert_eq!(event["status"], 200);
    assert!(event["input_tokens"].as_i64().unwrap_or_default() > 0);
    assert!(event["output_tokens"].as_i64().unwrap_or_default() > 0);
    assert!(event["cost_usd_micros"].as_i64().unwrap_or_default() > 0);
    assert_eq!(event["upstream_name"], "fake_anthropic");
    assert_eq!(event["model"], HAPPY_MODEL);
}

#[tokio::test]
async fn lqa_5b_api_key_auth_invalid_key_returns_401_metrics_and_row() {
    let server =
        common::spawn_test_server_with_principal_limits("", Vec::new(), AppConfig::default()).await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        "not-a-cclb-key",
        HAPPY_BODY,
        &[],
    )
    .await
    .expect("post invalid key");

    assert_eq!(response.status, 401);
    let body: Value = serde_json::from_str(&response.body).expect("auth error json");
    assert!(
        body["error"]["type"]
            .as_str()
            .unwrap_or_default()
            .contains("authentication")
    );
    wait_for_row_count(&pool, "1=1", baseline + 1).await;
    let payload = fetch_payload_json(&pool, "error_code = 'authentication_failed'").await;
    assert_eq!(json_i64(&payload, "status"), 401);
    let post = wait_metric_delta(
        server.metrics_addr,
        &pre,
        r#"cclb_key_auth_failures_total{reason="InvalidKey"}"#,
        1,
    )
    .await;
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cclb_key_auth_failures_total{reason="InvalidKey"}"#,
        ),
        1
    );
}

#[tokio::test]
async fn lqa_5c_concurrent_limit_rejects_second_request_and_audits() {
    let script = MessageScript::new();
    script.push_response(ScriptedMessageResponse::ok().with_delay(Duration::from_millis(500)));
    let fake_config = AppConfig {
        message_script: Some(script),
        ..AppConfig::default()
    };
    let limits = vec![Limit {
        kind: LimitKind::Concurrent,
        window_secs: 60,
        cap_micros: 1,
    }];
    let server = common::spawn_test_server_with_principal_limits("", limits, fake_config).await;
    let key = server.managed_key.clone();
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let mut set = JoinSet::new();
    for _ in 0..2 {
        let addr = server.proxy_addr;
        let plaintext = key.plaintext.clone();
        set.spawn(async move {
            common::http_post(addr, "/v1/messages", &plaintext, HAPPY_BODY, &[]).await
        });
    }
    let mut statuses = Vec::new();
    while let Some(result) = set.join_next().await {
        statuses.push(
            result
                .expect("join concurrent request")
                .expect("concurrent request")
                .status,
        );
    }
    statuses.sort_unstable();
    assert_eq!(statuses, vec![200, 429]);
    wait_for_row_count(&pool, "1=1", baseline + 2).await;
    let post = wait_metric_delta(
        server.metrics_addr,
        &pre,
        &format!(
            r#"cclb_concurrent_rejects_total{{key_id="{}"}}"#,
            key.key_id
        ),
        1,
    )
    .await;
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            &format!(
                r#"cclb_limit_hits_total{{kind="Concurrent",key_id="{}"}}"#,
                key.key_id
            ),
        ),
        1
    );
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            &format!(
                r#"cclb_concurrent_rejects_total{{key_id="{}"}}"#,
                key.key_id
            ),
        ),
        1
    );
    let audit_count = fetch_i64(
        &pool,
        "SELECT COUNT(*) FROM audit_log_v1 WHERE limit_violation = 'Concurrent'",
    )
    .await;
    assert_eq!(audit_count, 1);

    let rejected_payload =
        fetch_payload_json(&pool, "json_extract(payload, '$.status') = 429").await;
    assert!(
        rejected_payload
            .get("proxy_setup_ms")
            .and_then(serde_json::Value::as_i64)
            .is_some(),
        "429 limit-reject row must record proxy_setup_ms so the admin UI does not \
         attribute the setup time to Unaccounted; payload was: {rejected_payload}"
    );
}

#[tokio::test]
async fn lqa_5d_limit_reconcile_records_successful_reservation() {
    let limits = vec![Limit {
        kind: LimitKind::Requests,
        window_secs: 60,
        cap_micros: 100,
    }];
    let server =
        common::spawn_test_server_with_principal_limits("", limits, AppConfig::default()).await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let post = wait_metric_delta(
        server.metrics_addr,
        &pre,
        r#"cc_lb_limit_reconcile_subscriber_rows_total{outcome="reconciled"}"#,
        1,
    )
    .await;
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_limit_reconcile_subscriber_rows_total{outcome="reconciled"}"#
        ),
        1
    );
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_limit_reconcile_subscriber_rows_total{outcome="id_unknown"}"#
        ),
        0
    );
    assert_eq!(
        diff_counter(&pre, &post, "cc_lb_limit_reservation_ttl_evicted_total"),
        0
    );
}

#[tokio::test]
async fn lqa_5e_assembler_row_populates_identity_upstream_and_latency_stages() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    assert_eq!(fetch_text(&pool, "SELECT upstream_name FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await, "fake_anthropic");
    assert!(fetch_text(&pool, "SELECT upstream_id FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await.parse::<Uuid>().is_ok());
    let payload = fetch_payload_json(&pool, "event_id IS NOT NULL").await;
    assert_eq!(json_str(&payload, "principal_kind"), "machine");
    for field in [
        "auth_ms",
        "route_ms",
        "shape_ms",
        "sign_ms",
        "upstream_ttfb_ms",
    ] {
        assert_json_field_populated(&payload, field);
    }
}

#[tokio::test]
async fn lqa_5f_assembler_row_records_anthropic_upstream_uuid_name_and_model() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let upstream_id = fetch_text(&pool, "SELECT upstream_id FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await;
    assert!(upstream_id.parse::<Uuid>().is_ok());
    assert_eq!(fetch_text(&pool, "SELECT upstream_name FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await, "fake_anthropic");
    assert_eq!(fetch_text(&pool, "SELECT model FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 1").await, HAPPY_MODEL);
    let payload = fetch_payload_json(&pool, "event_id IS NOT NULL").await;
    assert_eq!(json_str(&payload, "upstream"), "anthropic_direct");
}

#[tokio::test]
async fn lqa_6a_upstream_rate_limit_state_persists_request_and_token_observations() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 1).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count = fetch_i64(&pool, "SELECT COUNT(*) FROM upstream_rate_limit_state_v1").await;
        if count >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for upstream_rate_limit_state_v1 rows; last={count}");
        }
        sleep(Duration::from_millis(50)).await;
    }
    let rows = sqlx::query("SELECT value FROM upstream_rate_limit_state_v1 ORDER BY key ASC")
        .fetch_all(&pool)
        .await
        .expect("rate limit rows");
    let values = rows
        .iter()
        .map(|row| {
            let raw: String = row.try_get("value").expect("value column");
            serde_json::from_str::<Value>(&raw).expect("rate limit value json")
        })
        .collect::<Vec<_>>();
    assert!(
        values
            .iter()
            .any(|value| value["kind"] == "requests" && value["remaining"].as_i64().is_some())
    );
    assert!(
        values
            .iter()
            .any(|value| value["kind"] == "tokens" && value["remaining"].as_i64().is_some())
    );
}

#[tokio::test]
async fn lqa_6b_non_stream_and_stream_rows_have_cache_and_cost_fields() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "event_id IS NOT NULL").await;

    assert_eq!(post_happy(&server).await.status, 200);
    let stream = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        STREAM_BODY,
        &[("accept", "text/event-stream")],
    )
    .await
    .expect("post stream message");
    assert_eq!(stream.status, 200);
    wait_for_row_count(&pool, "event_id IS NOT NULL", baseline + 2).await;
    let rows = sqlx::query(
        "SELECT payload FROM request_events_v1 WHERE event_id IS NOT NULL ORDER BY id DESC LIMIT 2",
    )
    .fetch_all(&pool)
    .await
    .expect("payload rows");
    assert_eq!(rows.len(), 2);
    for row in rows {
        let raw: String = row.try_get("payload").expect("payload column");
        let payload: Value = serde_json::from_str(&raw).expect("payload json");
        for field in [
            "cache_state",
            "cost_usd_micros",
            "cost_input_micros",
            "cost_output_micros",
        ] {
            assert_json_field_populated(&payload, field);
        }
    }
}

#[tokio::test]
async fn lqa_6c_upstream_rate_limit_error_records_provider_error_metric() {
    let script = MessageScript::new();
    script.push_response(ScriptedMessageResponse::error(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limit_error",
        "Test rate-limited",
    ));
    let server = common::spawn_test_server_with_fake_config(
        "",
        AppConfig {
            message_script: Some(script),
            ..AppConfig::default()
        },
    )
    .await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let response = post_happy(&server).await;

    assert_eq!(response.status, 429);
    wait_for_row_count(&pool, "1=1", baseline + 1).await;
    let payload = fetch_payload_json(&pool, "1=1").await;
    assert_eq!(json_i64(&payload, "status"), 429);
    let error_code = json_str(&payload, "error_code");
    assert_eq!(error_code, "upstream_4xx");
    let post = wait_metric_delta(
        server.metrics_addr,
        &pre,
        r#"cc_lb_lifecycle_events_total{kind="provider_error_observed"}"#,
        1,
    )
    .await;
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_lifecycle_events_total{kind="provider_error_observed"}"#
        ),
        1
    );
}

#[tokio::test]
async fn lqa_6e_admin_sse_keeps_up_with_fifty_final_frames_without_lag() {
    let server = common::spawn_test_server().await;
    let mut reader = open_admin_sse(server.admin_addr).await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let mut set = JoinSet::new();
    for _batch in 0..5 {
        for _ in 0..10 {
            let addr = server.proxy_addr;
            let api_key = server.managed_key.plaintext.clone();
            set.spawn(async move {
                common::http_post(addr, "/v1/messages", &api_key, HAPPY_BODY, &[]).await
            });
        }
        while let Some(result) = set.join_next().await {
            let response = result
                .expect("join sse burst request")
                .expect("sse burst request");
            assert_eq!(response.status, 200);
        }
    }

    let mut final_count = 0;
    while final_count < 50 {
        let frame = next_sse_data(&mut reader).await;
        if frame["phase"] == "final" {
            final_count += 1;
        }
    }
    let post = fetch_metric_scrape(server.metrics_addr).await;
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_dropped_events_total{reason="sse_lagged"}"#
        ),
        0
    );
}

#[tokio::test]
async fn lqa_6f_invalid_json_records_400_and_stops_before_routing() {
    let server = common::spawn_test_server().await;
    let pool = open_sqlite_pool(&server.sqlite_path).await;
    let baseline = settled_row_count(&pool, "1=1").await;
    let pre = fetch_metric_scrape(server.metrics_addr).await;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        "not json at all",
        &[],
    )
    .await
    .expect("post invalid json");

    assert_eq!(response.status, 400);
    wait_for_row_count(&pool, "1=1", baseline + 1).await;
    let payload = fetch_payload_json(&pool, "1=1").await;
    assert_eq!(json_i64(&payload, "status"), 400);
    assert_json_field_populated(&payload, "error_code");
    let post = wait_metric_delta(
        server.metrics_addr,
        &pre,
        r#"cc_lb_lifecycle_events_total{kind="request_terminated"}"#,
        1,
    )
    .await;
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_lifecycle_events_total{kind="parse_completed"}"#
        ),
        1
    );
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_lifecycle_events_total{kind="request_terminated"}"#
        ),
        1
    );
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_lifecycle_events_total{kind="route_completed"}"#
        ),
        0
    );
    assert_eq!(
        diff_counter(
            &pre,
            &post,
            r#"cc_lb_lifecycle_events_total{kind="upstream_attempt"}"#
        ),
        0
    );
}
