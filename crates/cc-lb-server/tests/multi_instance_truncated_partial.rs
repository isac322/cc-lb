#![cfg(feature = "postgres")]

// A producer stores oversized partials in local retention and emits a small PG
// NOTIFY marker. A second instance receives that marker, fetches the retained
// payload over the internal HTTP endpoint, then republishes it to its local bus.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_admin::internal_partials::{InternalPartialsState, router as internal_partials_router};
use cc_lb_admin::ports::{RetainedPartialPort, RetainedPartialSnapshot};
use cc_lb_control::{BusReceiver, RequestEventBus};
use cc_lb_engine::{InMemoryBus, PartialRetentionCache, PgListener, PgNotifier};
use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_postgres::PostgresStorage;
use metrics_util::debugging::Snapshotter;
use secrecy::SecretString;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

const CLUSTER_TOKEN: &str = "test-cluster-token";
const LARGE_PAYLOAD_MIN_BYTES: usize = 7_500;
// CI latency budgets (not correctness bounds): positive-path SSE receipt +
// warmup normally finish fast, but under llvm-cov plus a co-scheduled heavy
// build on the shared runner they can overrun. Scale by
// CC_LB_TEST_READY_TIMEOUT_SECS (120 in CI), the repo's convention.
// NEGATIVE_RECEIVE_TIMEOUT stays fixed: it bounds a "confirm nothing arrives"
// wait, which contention cannot make flake (an absent event never appears).
fn receive_timeout() -> Duration {
    Duration::from_secs(15)
}
const NEGATIVE_RECEIVE_TIMEOUT: Duration = Duration::from_millis(500);
fn warmup_timeout() -> Duration {
    Duration::from_secs(10)
}
const WARMUP_PROBE_INTERVAL: Duration = Duration::from_millis(100);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct TestRetainedPartialPort {
    cache: PartialRetentionCache,
}

impl TestRetainedPartialPort {
    const fn new(cache: PartialRetentionCache) -> Self {
        Self { cache }
    }
}

impl RetainedPartialPort for TestRetainedPartialPort {
    fn retained_partial(&self, event_id: &str) -> Option<RetainedPartialSnapshot> {
        self.cache
            .get(event_id)
            .map(|payload| RetainedPartialSnapshot { payload })
    }
}

#[tokio::test]
async fn t3_postgres__truncated_partial_http_fallback_reaches_consumer_bus() -> TestResult<()> {
    run_truncated_partial_case(true).await
}

#[tokio::test]
async fn t3_postgres__truncated_partial_http_fallback_rejects_wrong_token() -> TestResult<()> {
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
    let (recorder, metrics) = cc_lb_testkit::local_recorder();
    let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
    let database_url = crate::required_ci_postgres_url().await?;
    let _serial = crate::common::postgres_test_lock(&database_url).await?;

    reset_request_event_tables(&database_url).await?;
    let handle = &metrics;
    let producer_pool = pg_pool(&database_url, 4).await?;
    let consumer_pool = pg_pool(&database_url, 4).await?;
    let retention = PartialRetentionCache::new(Duration::from_secs(300), 256);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let producer_url = format!("http://{}", listener.local_addr()?);
    let app = internal_partials_router(InternalPartialsState {
        retention: Arc::new(TestRetainedPartialPort::new(retention.clone())),
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
    // Each test run uses a unique NOTIFY channel so a concurrently-running sibling
    // test's producer cannot deliver into this consumer over the shared Postgres
    // database (Postgres NOTIFY broadcasts to every session listening on a channel).
    let channel = format!(
        "cc_lb_test_partial_{}",
        uuid::Uuid::from_u128(if expect_delivery { 1 } else { 2 }).simple()
    );
    let (producer_tx, producer_rx) = mpsc::channel(16);
    let (notifier_shutdown_tx, notifier_shutdown_rx) = watch::channel(false);
    let notifier_task = PgNotifier::spawn_with_channel(
        producer_pool.clone(),
        producer_rx,
        retention,
        producer_url,
        channel.clone(),
        notifier_shutdown_rx,
    );
    let consumer_bus = Arc::new(InMemoryBus::new());
    let mut consumer_rx = consumer_bus.subscribe();
    let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
    let listener_task = PgListener::spawn_with_channel(
        consumer_pool.clone(),
        consumer_bus,
        reqwest::Client::new(),
        SecretString::new(consumer_token.to_owned().into()),
        channel,
        "http://consumer.local".to_owned(),
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
    // The producer increments the counter only after its `pg_notify` await, which
    // is not ordered against the consumer-side delivery `wait_for_partial` sees, so
    // under scheduler starvation delivery can beat the increment. Poll to a bound;
    // the assertion stays exact (precisely `before + 1.0`).
    let observed = wait_for_counter(handle, metric_name, metric_outcome, before + 1.0).await;
    assert_eq!(observed, before + 1.0);

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

async fn reset_request_event_tables(database_url: &str) -> TestResult<()> {
    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
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
    let deadline = Instant::now() + warmup_timeout();
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
        receive_timeout()
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
        event_id: format!("event-{name}-{}", uuid::Uuid::from_u128(3)),
        request_id: format!("req-{name}"),
        ts: 1_800_000_000,
        ts_ms: 1_800_000_000_000,
        last_update_ms: 1_800_000_000_010,
        elapsed_ms: 10,
        stream: true,
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..RequestEventPartial::default()
    }
}

fn large_partial(name: &str) -> RequestEventPartial {
    RequestEventPartial {
        cache_prefix_hash: Some(format!("sha256:{}", "x".repeat(8_192))),
        upstream_name: Some(format!("upstream-{name}")),
        input_tokens: Some(100),
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        output_tokens: Some(25),
        ..small_partial(name)
    }
}

async fn wait_for_counter(handle: &Snapshotter, name: &str, outcome: &str, expected: f64) -> f64 {
    let deadline = Instant::now() + receive_timeout();
    loop {
        let value = labeled_counter_value(handle, name, outcome);
        if value >= expected || Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(WARMUP_PROBE_INTERVAL).await;
    }
}

fn labeled_counter_value(snapshotter: &Snapshotter, name: &str, outcome: &str) -> f64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, metric)| {
            (key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == outcome))
            .then(|| format!("{metric:?}"))
            .and_then(|rendered| {
                rendered
                    .strip_prefix("Counter(")?
                    .strip_suffix(')')?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0) as f64
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(message.into()))
}
