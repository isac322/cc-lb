#![cfg(feature = "postgres")]

use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_config::{AdminAuthProviderConfig, Config, PostgresPoolConfig, StorageConfig};
use cc_lb_server::app::{App, build_app_with_storage, seed_app_testing_storage};
use cc_lb_storage_api::{ManagedKeyStore, MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::adapter::retry::RetryPolicy;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage};
use http_body_util::BodyExt;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = env!("CARGO_PKG_NAME");
const CLUSTER_TOKEN_ENV: &str = "CI_POSTGRES_URL";
const STORAGE_TAIL_TIMEOUT_DEFAULT: Duration = Duration::from_secs(30);

// Hang-guard ceiling for cross-instance SSE propagation. The read is
// event-driven; under coverage on a contended CI runner it can exceed a few
// seconds, so scale by CC_LB_TEST_READY_TIMEOUT_SECS (120 in CI) like the
// sibling managed_key_multi_instance test. A hardcoded 5s here flaked.
fn storage_tail_timeout() -> Duration {
    std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(STORAGE_TAIL_TIMEOUT_DEFAULT)
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_tail_reaches_second_instance_over_pg_notify() -> TestResult<()> {
    let Some(database_url) = ci_postgres_url() else {
        eprintln!("SKIP: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _serial = crate::common::postgres_test_lock(&database_url).await?;

    reset_request_event_tables(&database_url).await?;
    let instance_a = build_running_app(&database_url, "instance-a").await?;
    let instance_b = build_running_app(&database_url, "instance-b").await?;

    let response = instance_b
        .app
        .admin_router
        .clone()
        .oneshot(admin_stream_request()?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    let mut sse_buffer = String::new();
    consume_initial_bookmark(&mut body, &mut sse_buffer).await?;

    let event = request_event();
    let event_id = event
        .event_id
        .clone()
        .ok_or_else(|| error("event id missing"))?;
    let cursor = instance_a.storage.append_request_event(&event).await?;

    let update = tokio::time::timeout(
        storage_tail_timeout(),
        read_final_update(&mut body, &mut sse_buffer, &event_id),
    )
    .await
    .map_err(|_| {
        error(format!(
            "timed out waiting for final SSE frame for {event_id}"
        ))
    })??;

    assert_eq!(update["payload"]["cursor"].as_u64(), Some(cursor));
    assert_eq!(
        update["payload"]["event"]["event_id"].as_str(),
        Some(event_id.as_str())
    );

    Ok(())
}

fn ci_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

async fn reset_request_event_tables(database_url: &str) -> TestResult<()> {
    let clock: ClockHandle = Arc::new(SystemClock);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let storage = PostgresStorage::new(pool.clone(), clock);
    storage.initialize().await?;
    sqlx::query("TRUNCATE request_events_v1 RESTART IDENTITY")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

async fn build_running_app(database_url: &str, label: &'static str) -> TestResult<RunningApp> {
    let clock: ClockHandle = Arc::new(SystemClock);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(database_url)
        .await?;
    let storage = Arc::new(PostgresStorage::new(pool.clone(), clock.clone()));
    storage.initialize().await?;
    seed_app_testing_storage(storage.as_ref(), None, &*clock).await?;

    let managed_store: Arc<dyn ManagedKeyStore> = Arc::new(PostgresManagedKeyStore::new(
        pool,
        Arc::new(RetryPolicy::default()),
        clock.clone(),
    ));
    let app = build_app_with_storage(
        test_config(database_url, label),
        None,
        managed_store,
        storage.clone(),
        Arc::new(AeadService::from_master_key([0; 32])),
        clock,
    )
    .await?;

    Ok(RunningApp { app, storage })
}

fn test_config(database_url: &str, label: &str) -> Config {
    let mut config = Config::default();
    config.listener.proxy_addr = "127.0.0.1:0".parse().expect("valid proxy addr");
    config.listener.admin_addr = "127.0.0.1:0".parse().expect("valid admin addr");
    config.storage = StorageConfig::Postgres {
        url: database_url.to_owned(),
        pool: PostgresPoolConfig::default(),
    };
    config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
        id: "test".to_owned(),
        token_env: crate::common::TEST_NONEMPTY_ENV.to_owned(),
    }];
    config.event_bus.storage_tail_poll_interval_ms = 10;
    config.cluster.instance_url = Some(format!("http://{label}.example.test"));
    config.cluster.token_env = CLUSTER_TOKEN_ENV.to_owned();
    config
}

fn admin_stream_request() -> TestResult<Request<Body>> {
    Ok(Request::builder()
        .method("GET")
        .uri("/admin/v1/events/stream")
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::empty())?)
}

async fn consume_initial_bookmark(body: &mut Body, buffer: &mut String) -> TestResult<()> {
    let mut saw_connected = false;
    loop {
        let frame = next_sse_frame(body, buffer).await?;
        saw_connected |= frame.starts_with(": connected");
        if saw_connected && frame.lines().any(|line| line == "event: cursor") {
            return Ok(());
        }
    }
}

async fn read_final_update(
    body: &mut Body,
    buffer: &mut String,
    event_id: &str,
) -> TestResult<Value> {
    loop {
        let frame = next_sse_frame(body, buffer).await?;
        if !frame.lines().any(|line| line == "event: message") {
            continue;
        }
        let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data: ")) else {
            continue;
        };
        let update: Value = serde_json::from_str(data)?;
        if update["phase"] == "final"
            && update["payload"]["event"]["event_id"].as_str() == Some(event_id)
        {
            return Ok(update);
        }
    }
}

async fn next_sse_frame(body: &mut Body, buffer: &mut String) -> TestResult<String> {
    loop {
        if let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_owned();
            buffer.drain(..index + 2);
            return Ok(frame);
        }

        let frame = body
            .frame()
            .await
            .ok_or_else(|| error("SSE stream ended before expected frame"))??;
        if let Ok(data) = frame.into_data() {
            buffer.push_str(std::str::from_utf8(&data)?);
        }
    }
}

fn request_event() -> RequestEvent {
    let event_id = format!("event-multi-instance-tail-{}", uuid::Uuid::now_v7());
    RequestEvent {
        ts: 1_800_000_000,
        ts_ms: Some(1_800_000_000_000),
        request_id: "req-multi-instance-tail".to_owned(),
        event_id: Some(event_id),
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 10,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..RequestEvent::default()
    }
}

struct RunningApp {
    app: App,
    storage: Arc<PostgresStorage>,
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
