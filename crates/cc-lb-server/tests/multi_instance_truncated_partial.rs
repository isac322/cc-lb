#![cfg(feature = "postgres")]

// Producers trim oversized optional metadata from partials so every event fits
// within Postgres NOTIFY's 8KB wire budget and is delivered inline without HTTP
// callbacks; irreducibly oversized partials are dropped and counted.

use std::error::Error;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_control::{InMemoryBus, RequestEventBus};
use cc_lb_engine::{PgListener, PgNotifier};
use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};
use futures_util::FutureExt as _;
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::{broadcast, mpsc, watch};

const LARGE_PAYLOAD_MIN_BYTES: usize = 7_500;
const NEGATIVE_RECEIVE_TIMEOUT: Duration = Duration::from_millis(500);
const WARMUP_PROBE_INTERVAL: Duration = Duration::from_millis(100);

fn receive_timeout() -> Duration {
    std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(15))
}

fn warmup_timeout() -> Duration {
    std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(10))
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trimmed_oversized_partial_delivers_inline_and_irreducible_drops() -> TestResult<()> {
    let Some(admin_url) = ci_postgres_url() else {
        eprintln!("SKIP: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let db = crate::common::IsolatedPostgresDb::create(
        &admin_url,
        "cc_lb_partial",
        "CC_LB_PG_FULL_TEMPLATE",
    )
    .await?;
    let database_url = db.url.clone();

    let outcome = AssertUnwindSafe(async move {
        let handle = metrics_handle();
        let producer_pool = pg_pool(&database_url, 4).await?;
        let consumer_pool = pg_pool(&database_url, 4).await?;
        let channel = format!("cc_lb_test_partial_{}", uuid::Uuid::now_v7().simple());
        let (producer_tx, producer_rx) = mpsc::channel(16);
        let (notifier_shutdown_tx, notifier_shutdown_rx) = watch::channel(false);
        let notifier_task = PgNotifier::spawn_with_channel(
            producer_pool.clone(),
            producer_rx,
            "producer-a".to_owned(),
            channel.clone(),
            notifier_shutdown_rx,
        );
        let consumer_bus = Arc::new(InMemoryBus::new());
        let mut consumer_rx = consumer_bus.subscribe();
        let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
        let listener_task = PgListener::spawn_with_channel(
            consumer_pool.clone(),
            consumer_bus,
            channel,
            "consumer-b".to_owned(),
            listener_shutdown_rx,
        );

        publish_until_received(
            &producer_tx,
            &mut consumer_rx,
            small_partial("listener-ready"),
        )
        .await?;

        let trimmed_before = labeled_counter_value(
            handle,
            "sse_partial_notify_sent_total",
            "outcome",
            "trimmed_sent",
        );
        let partial = large_partial("trimmed-inline");
        let expected_event_id = partial.event_id.clone();
        let expected_request_id = partial.request_id.clone();
        let update = RequestEventUpdate::Partial(partial);
        let payload = serde_json::to_vec(&update)?;
        assert!(
            payload.len() >= LARGE_PAYLOAD_MIN_BYTES,
            "large partial fixture serialized to {} bytes, below {LARGE_PAYLOAD_MIN_BYTES}",
            payload.len()
        );

        producer_tx.send(update).await?;
        let delivered = wait_for_partial(&mut consumer_rx, &expected_event_id)
            .await?
            .expect("trimmed partial must reach consumer bus");
        assert_eq!(delivered.request_id, expected_request_id);
        assert_eq!(delivered.input_tokens, Some(100));
        assert_eq!(delivered.output_tokens, Some(25));
        assert!(delivered.thread_id.is_none());

        let observed_trimmed = wait_for_counter(
            handle,
            "sse_partial_notify_sent_total",
            "outcome",
            "trimmed_sent",
            trimmed_before + 1.0,
        )
        .await;
        assert_eq!(observed_trimmed, trimmed_before + 1.0);

        let dropped_before = labeled_counter_value(
            handle,
            "sse_partial_notify_dropped_total",
            "reason",
            "payload_too_large",
        );
        let mut irreducible = small_partial("irreducible");
        irreducible.event_id = "e".repeat(8_000);
        let irreducible_id = irreducible.event_id.clone();
        producer_tx
            .send(RequestEventUpdate::Partial(irreducible))
            .await?;
        let received_irreducible =
            try_receive_matching(&mut consumer_rx, &irreducible_id, NEGATIVE_RECEIVE_TIMEOUT)
                .await?;
        assert!(
            received_irreducible.is_none(),
            "irreducibly oversized partial must not be delivered"
        );
        let observed_dropped = wait_for_counter(
            handle,
            "sse_partial_notify_dropped_total",
            "reason",
            "payload_too_large",
            dropped_before + 1.0,
        )
        .await;
        assert_eq!(observed_dropped, dropped_before + 1.0);

        notifier_shutdown_tx.send(true)?;
        listener_shutdown_tx.send(true)?;
        producer_pool.close().await;
        consumer_pool.close().await;
        notifier_task.await?;
        listener_task.await?;
        Ok(())
    })
    .catch_unwind()
    .await;
    let _ = db.drop().await;
    match outcome {
        Ok(res) => res,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn ci_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

fn metrics_handle() -> &'static PrometheusHandle {
    crate::common::install_prometheus()
}

async fn pg_pool(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
}

async fn publish_until_received(
    producer_tx: &mpsc::Sender<RequestEventUpdate>,
    receiver: &mut broadcast::Receiver<RequestEventUpdate>,
    partial: RequestEventPartial,
) -> TestResult<()> {
    let event_id = partial.event_id.clone();
    let deadline = Instant::now() + warmup_timeout();
    while Instant::now() < deadline {
        producer_tx
            .send(RequestEventUpdate::Partial(partial.clone()))
            .await?;
        if try_receive_matching(receiver, &event_id, WARMUP_PROBE_INTERVAL)
            .await?
            .is_some()
        {
            return Ok(());
        }
    }
    Err(error(format!(
        "timed out warming up PgListener delivery for partial {event_id}"
    )))
}

async fn try_receive_matching(
    rx: &mut broadcast::Receiver<RequestEventUpdate>,
    expected_event_id: &str,
    timeout: Duration,
) -> TestResult<Option<RequestEventPartial>> {
    match tokio::time::timeout(timeout, async {
        loop {
            if let RequestEventUpdate::Partial(partial) = rx.recv().await?
                && partial.event_id == expected_event_id
            {
                return Ok::<_, Box<dyn Error + Send + Sync>>(partial);
            }
        }
    })
    .await
    {
        Ok(Ok(partial)) => Ok(Some(partial)),
        Ok(Err(error)) => Err(error),
        Err(_) => Ok(None),
    }
}

async fn wait_for_partial(
    rx: &mut broadcast::Receiver<RequestEventUpdate>,
    expected_event_id: &str,
) -> TestResult<Option<RequestEventPartial>> {
    try_receive_matching(rx, expected_event_id, receive_timeout()).await
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
        thread_id: Some(format!("thread:{}", "x".repeat(8_192))),
        upstream_name: Some(format!("upstream-{name}")),
        input_tokens: Some(100),
        output_tokens: Some(25),
        ..small_partial(name)
    }
}

async fn wait_for_counter(
    handle: &PrometheusHandle,
    name: &str,
    label_key: &str,
    label_val: &str,
    expected: f64,
) -> f64 {
    let deadline = Instant::now() + receive_timeout();
    loop {
        let value = labeled_counter_value(handle, name, label_key, label_val);
        if value >= expected || Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(WARMUP_PROBE_INTERVAL).await;
    }
}

fn labeled_counter_value(
    handle: &PrometheusHandle,
    name: &str,
    label_key: &str,
    label_val: &str,
) -> f64 {
    let metric_prefix = format!("{name}{{");
    let label_fragment = format!("{label_key}=\"{label_val}\"");
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
