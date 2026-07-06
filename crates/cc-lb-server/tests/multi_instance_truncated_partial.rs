#![cfg(feature = "postgres")]

// A producer stores oversized partials in local retention and emits a small PG
// NOTIFY marker. A second instance receives that marker, fetches the retained
// payload over the internal HTTP endpoint, then republishes it to its local bus.

use std::error::Error;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use cc_lb_admin::internal_partials::{InternalPartialsState, router as internal_partials_router};
use cc_lb_contract::{BusReceiver, RequestEventBus, RequestEventPartial, RequestEventUpdate};
use cc_lb_engine::{
    ClockHandle, InMemoryBus, PartialRetentionCache, PgListener, PgNotifier, SystemClock,
};
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_postgres::PostgresStorage;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use secrecy::SecretString;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

const CLUSTER_TOKEN: &str = "test-cluster-token";
const LARGE_PAYLOAD_MIN_BYTES: usize = 7_500;
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);
const NEGATIVE_RECEIVE_TIMEOUT: Duration = Duration::from_millis(500);
const WARMUP_TIMEOUT: Duration = Duration::from_secs(10);
const WARMUP_PROBE_INTERVAL: Duration = Duration::from_millis(100);

static POSTGRES_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_partial_http_fallback_reaches_consumer_bus() -> TestResult<()> {
    run_truncated_partial_case(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_partial_http_fallback_rejects_wrong_token() -> TestResult<()> {
    run_truncated_partial_case(false).await
}

async fn run_truncated_partial_case(expect_delivery: bool) -> TestResult<()> {
    let (producer_token, consumer_token, metric_name, metric_outcome, partial_name) =
        if expect_delivery {
            (
                CLUSTER_TOKEN,
                CLUSTER_TOKEN,
                "sse_partial_notify_sent_total",
                "truncated_sent",
                "fallback-happy",
            )
        } else {
            (
                "right-token",
                "wrong-token",
                "sse_notify_http_fetches_total",
                "unauthorized",
                "fallback-wrong-token",
            )
        };
    let Some(database_url) = ci_postgres_url() else {
        eprintln!("SKIP: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _serial = postgres_test_lock().lock().await;

    reset_request_event_tables(&database_url).await?;
    let handle = metrics_handle();
    let producer_pool = pg_pool(&database_url, 4).await?;
    let consumer_pool = pg_pool(&database_url, 4).await?;
    let retention = PartialRetentionCache::new(Duration::from_secs(300), 256);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let producer_url = format!("http://{}", listener.local_addr()?);
    let app = internal_partials_router(InternalPartialsState {
        retention: retention.clone(),
        cluster_token: producer_token.to_owned(),
    });
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel();
    let http_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = http_shutdown_rx.await;
            })
            .await
    });
    let (producer_tx, producer_rx) = mpsc::channel(16);
    let (notifier_shutdown_tx, notifier_shutdown_rx) = watch::channel(false);
    let notifier_task = PgNotifier::spawn(
        producer_pool.clone(),
        producer_rx,
        retention,
        producer_url,
        notifier_shutdown_rx,
    );
    let consumer_bus = Arc::new(InMemoryBus::new());
    let mut consumer_rx = consumer_bus.subscribe();
    let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
    let listener_task = PgListener::spawn(
        consumer_pool.clone(),
        consumer_bus,
        reqwest::Client::new(),
        SecretString::new(consumer_token.to_owned().into()),
        listener_shutdown_rx,
    );

    publish_until_received(
        &producer_tx,
        &mut consumer_rx,
        small_partial("listener-ready"),
    )
    .await?;

    let before = labeled_counter_value(handle, metric_name, metric_outcome);
    let partial = large_partial(partial_name);
    let expected_event_id = partial.event_id.clone();
    let update = RequestEventUpdate::Partial(partial);
    let payload = serde_json::to_vec(&update)?;
    assert!(
        payload.len() >= LARGE_PAYLOAD_MIN_BYTES,
        "large partial fixture serialized to {} bytes, below {LARGE_PAYLOAD_MIN_BYTES}",
        payload.len()
    );

    producer_tx.send(update).await?;
    wait_for_partial(&mut consumer_rx, &expected_event_id, expect_delivery).await?;
    assert_eq!(
        labeled_counter_value(handle, metric_name, metric_outcome),
        before + 1.0
    );

    notifier_shutdown_tx.send(true)?;
    listener_shutdown_tx.send(true)?;
    let _ = http_shutdown_tx.send(());
    producer_pool.close().await;
    consumer_pool.close().await;
    notifier_task.await?;
    listener_task.await?;
    http_task.await??;
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

async fn reset_request_event_tables(database_url: &str) -> TestResult<()> {
    let clock: ClockHandle = Arc::new(SystemClock);
    let pool = pg_pool(database_url, 1).await?;
    let storage = PostgresStorage::new(pool.clone(), clock);
    storage.initialize(BackendKind::Postgres).await?;
    sqlx::query("TRUNCATE request_events_v1 RESTART IDENTITY")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

async fn pg_pool(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
}

// Postgres LISTEN/NOTIFY only delivers to sessions subscribed at NOTIFY time.
// `PgListener::spawn` returns before the underlying `LISTEN` runs inside the
// spawned task, so a single warmup NOTIFY can race the registration and be
// silently dropped by Postgres. Resend the warmup partial at
// `WARMUP_PROBE_INTERVAL` until the consumer bus actually observes it — this
// mirrors `pg_listener_recovery::publish_until_received`.
async fn publish_until_received(
    producer_tx: &mpsc::Sender<RequestEventUpdate>,
    receiver: &mut BusReceiver,
    partial: RequestEventPartial,
) -> TestResult<()> {
    let event_id = partial.event_id.clone();
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    while Instant::now() < deadline {
        producer_tx
            .send(RequestEventUpdate::Partial(partial.clone()))
            .await?;
        if try_receive_matching(receiver, &event_id, WARMUP_PROBE_INTERVAL).await? {
            return Ok(());
        }
    }
    Err(error(format!(
        "timed out warming up PgListener delivery for partial {event_id}"
    )))
}

async fn try_receive_matching(
    receiver: &mut BusReceiver,
    expected_event_id: &str,
    timeout: Duration,
) -> TestResult<bool> {
    let BusReceiver::InMemory(rx) = receiver else {
        return Err(error("remote bus receiver unsupported in this test"));
    };
    match tokio::time::timeout(timeout, async {
        loop {
            if let RequestEventUpdate::Partial(partial) = rx.recv().await?
                && partial.event_id == expected_event_id
            {
                return Ok::<(), Box<dyn Error + Send + Sync>>(());
            }
        }
    })
    .await
    {
        Ok(Ok(())) => Ok(true),
        Ok(Err(error)) => Err(error),
        Err(_) => Ok(false),
    }
}

async fn wait_for_partial(
    receiver: &mut BusReceiver,
    expected_event_id: &str,
    expect_delivery: bool,
) -> TestResult<()> {
    let BusReceiver::InMemory(rx) = receiver else {
        return Err(error("remote bus receiver unsupported in this test"));
    };
    let timeout = if expect_delivery {
        RECEIVE_TIMEOUT
    } else {
        NEGATIVE_RECEIVE_TIMEOUT
    };
    let received = tokio::time::timeout(timeout, async {
        loop {
            if let RequestEventUpdate::Partial(partial) = rx.recv().await?
                && partial.event_id == expected_event_id
            {
                return Ok(partial.event_id);
            }
        }
    })
    .await;
    match received {
        Ok(Ok(_)) if expect_delivery => Ok(()),
        Err(_) if !expect_delivery => Ok(()),
        Err(_) => Err(error(format!(
            "timed out waiting for partial {expected_event_id}"
        ))),
        Ok(Ok(event_id)) => Err(error(format!(
            "unexpected partial {event_id} reached consumer bus"
        ))),
        Ok(Err(error)) => Err(error),
    }
}

fn small_partial(name: &str) -> RequestEventPartial {
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

fn large_partial(name: &str) -> RequestEventPartial {
    RequestEventPartial {
        cache_prefix_hash: Some(format!("sha256:{}", "x".repeat(8_192))),
        upstream_name: Some(format!("upstream-{name}")),
        input_tokens: Some(100),
        output_tokens: Some(25),
        ..small_partial(name)
    }
}

fn labeled_counter_value(handle: &PrometheusHandle, name: &str, outcome: &str) -> f64 {
    let metric_prefix = format!("{name}{{");
    let label_fragment = format!("outcome=\"{outcome}\"");
    handle
        .render()
        .lines()
        .find(|line| line.starts_with(&metric_prefix) && line.contains(&label_fragment))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(message.into()))
}
