#![cfg(feature = "postgres")]

use std::error::Error;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cc_lb_contract::{BusReceiver, RequestEventBus};
use cc_lb_engine::{DEFAULT_PG_NOTIFY_CHANNEL, InMemoryBus, PgListener};
use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use secrecy::SecretString;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, timeout};

const CLUSTER_TOKEN: &str = "test-cluster-token";
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const PROBE_INTERVAL: Duration = Duration::from_millis(100);

static POSTGRES_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_listener_reconnects_after_backend_close() -> TestResult<()> {
    let Some(database_url) = ci_postgres_url() else {
        eprintln!("SKIP: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _serial = postgres_test_lock().lock().await;

    let handle = metrics_handle();
    let app_name = format!("pg-listener-recovery-{}", uuid::Uuid::now_v7());
    let listener_pool = pg_pool_with_application_name(&database_url, 2, &app_name).await?;
    let publisher_pool = pg_pool(&database_url, 2).await?;
    let consumer_bus = Arc::new(InMemoryBus::new());
    let mut consumer_rx = consumer_bus.subscribe();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener_task = PgListener::spawn(
        listener_pool.clone(),
        consumer_bus,
        reqwest::Client::new(),
        SecretString::new(CLUSTER_TOKEN.to_owned().into()),
        shutdown_rx,
    );

    publish_until_received(&publisher_pool, &mut consumer_rx, partial("before-close")).await?;

    let before = reconnect_counter_value(handle, "recv_failed");
    terminate_listener_backends(&publisher_pool, &app_name).await?;
    wait_for_counter_at_least(handle, "recv_failed", before + 1.0, RECOVERY_TIMEOUT).await?;
    assert_eq!(reconnect_counter_value(handle, "recv_failed"), before + 1.0);

    publish_until_received(&publisher_pool, &mut consumer_rx, partial("after-close")).await?;

    let _ = shutdown_tx.send(true);
    listener_pool.close().await;
    publisher_pool.close().await;
    listener_task.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_listener_shuts_down_during_reconnect_sleep() -> TestResult<()> {
    let Some(database_url) = ci_postgres_url() else {
        eprintln!("SKIP: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _serial = postgres_test_lock().lock().await;

    let handle = metrics_handle();
    let before = reconnect_counter_value(handle, "connect_failed");
    let invalid_pool = invalid_pg_pool(&database_url)?;
    let consumer_bus = Arc::new(InMemoryBus::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener_task = PgListener::spawn(
        invalid_pool.clone(),
        consumer_bus,
        reqwest::Client::new(),
        SecretString::new(CLUSTER_TOKEN.to_owned().into()),
        shutdown_rx,
    );

    wait_for_counter_at_least(handle, "connect_failed", before + 2.0, RECOVERY_TIMEOUT).await?;
    let _ = shutdown_tx.send(true);
    timeout(SHUTDOWN_TIMEOUT, listener_task).await??;
    invalid_pool.close().await;
    Ok(())
}

fn ci_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

fn postgres_test_lock() -> &'static tokio::sync::Mutex<()> {
    POSTGRES_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn metrics_handle() -> &'static PrometheusHandle {
    METRICS_HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("prometheus recorder installs")
    })
}

async fn pg_pool(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
}

async fn pg_pool_with_application_name(
    database_url: &str,
    max_connections: u32,
    app_name: &str,
) -> TestResult<PgPool> {
    let options = database_url
        .parse::<PgConnectOptions>()?
        .application_name(app_name);
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await?)
}

fn invalid_pg_pool(database_url: &str) -> TestResult<PgPool> {
    let mut url = url::Url::parse(database_url)?;
    let missing_database = format!("{}_missing", url.path().trim_start_matches('/'));
    url.set_path(&missing_database);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(url.as_str())?;
    Ok(pool)
}

async fn terminate_listener_backends(pool: &PgPool, app_name: &str) -> TestResult<()> {
    let terminated = sqlx::query_scalar::<_, bool>(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1 AND pid <> pg_backend_pid()",
    )
    .bind(app_name)
    .fetch_all(pool)
    .await?;
    if !terminated.into_iter().any(|terminated| terminated) {
        return Err(error(format!(
            "no listener backend was terminated for application_name={app_name}"
        )));
    }
    Ok(())
}

async fn publish_until_received(
    pool: &PgPool,
    receiver: &mut BusReceiver,
    partial: RequestEventPartial,
) -> TestResult<()> {
    let event_id = partial.event_id.clone();
    let payload = serde_json::to_string(&RequestEventUpdate::Partial(partial))?;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut interval = tokio::time::interval(PROBE_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while Instant::now() < deadline {
        publish_payload(pool, &payload).await?;
        if timeout(PROBE_INTERVAL, receive_partial(receiver, &event_id))
            .await
            .is_ok()
        {
            return Ok(());
        }
        interval.tick().await;
    }
    Err(error(format!("timed out waiting for partial {event_id}")))
}

async fn publish_payload(pool: &PgPool, payload: &str) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(DEFAULT_PG_NOTIFY_CHANNEL)
        .bind(payload)
        .execute(pool)
        .await?;
    Ok(())
}

async fn receive_partial(receiver: &mut BusReceiver, expected_event_id: &str) -> TestResult<()> {
    let BusReceiver::InMemory(rx) = receiver else {
        return Err(error("remote bus receiver unsupported in this test"));
    };
    loop {
        if let RequestEventUpdate::Partial(partial) = rx.recv().await?
            && partial.event_id == expected_event_id
        {
            return Ok(());
        }
    }
}

async fn wait_for_counter_at_least(
    handle: &PrometheusHandle,
    reason: &str,
    expected: f64,
    timeout_after: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout_after;
    let mut interval = tokio::time::interval(PROBE_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while Instant::now() < deadline {
        if reconnect_counter_value(handle, reason) >= expected {
            return Ok(());
        }
        interval.tick().await;
    }
    Err(error(format!(
        "timed out waiting for reconnect counter reason={reason} to reach {expected}"
    )))
}

fn reconnect_counter_value(handle: &PrometheusHandle, reason: &str) -> f64 {
    let label_fragment = format!("reason=\"{reason}\"");
    handle
        .render()
        .lines()
        .find(|line| {
            line.starts_with("sse_pg_listener_reconnects_total{") && line.contains(&label_fragment)
        })
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn partial(name: &str) -> RequestEventPartial {
    RequestEventPartial {
        event_id: format!("event-{name}-{}", uuid::Uuid::now_v7()),
        request_id: format!("req-{name}"),
        ts: 1_800_000_000,
        ts_ms: 1_800_000_000_000,
        last_update_ms: 1_800_000_000_010,
        elapsed_ms: 10,
        stream: true,
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        ..RequestEventPartial::default()
    }
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(message.into()))
}
