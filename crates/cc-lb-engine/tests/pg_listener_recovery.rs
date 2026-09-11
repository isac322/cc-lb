#![cfg(feature = "postgres")]

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_control::{BusReceiver, RequestEventBus};
use cc_lb_engine::{DEFAULT_PG_NOTIFY_CHANNEL, InMemoryBus, PgListener};
use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};
use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use secrecy::SecretString;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const CLUSTER_TOKEN: &str = "test-cluster-token";
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn t3_postgres__pg_listener_reconnects_after_backend_close() -> TestResult<()> {
    let (recorder, reconnect_rx) = ReconnectRecorder::new();
    metrics::with_local_recorder(&recorder, || {
        current_thread_runtime()?.block_on(reconnect_after_backend_close(&recorder, reconnect_rx))
    })
}

#[test]
fn t3_postgres__pg_listener_shuts_down_during_reconnect_sleep() -> TestResult<()> {
    let (recorder, reconnect_rx) = ReconnectRecorder::new();
    metrics::with_local_recorder(&recorder, || {
        current_thread_runtime()?.block_on(shutdown_during_reconnect_sleep(&recorder, reconnect_rx))
    })
}

fn current_thread_runtime() -> TestResult<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

async fn reconnect_after_backend_close(
    recorder: &ReconnectRecorder,
    mut reconnect_rx: mpsc::Receiver<String>,
) -> TestResult<()> {
    let fixture = crate::postgres_fixture::postgres_fixture().await?;
    let database_url = fixture.database_url().to_owned();
    let app_name = format!(
        "cclb-pg-listener-{}",
        fixture.schema_name().trim_start_matches("cc_lb_test_")
    );
    let (connection_tx, mut connection_rx) = mpsc::unbounded_channel();
    let listener_pool = pg_pool_with_application_name(&database_url, &app_name, connection_tx)?;
    let publisher_pool = fixture.pool().clone();
    let consumer_bus = Arc::new(InMemoryBus::new());
    let mut consumer_rx = consumer_bus.subscribe();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut listener_task = PgListener::spawn(
        listener_pool.clone(),
        consumer_bus,
        reqwest::Client::new(),
        SecretString::new(CLUSTER_TOKEN.to_owned().into()),
        "http://listener.local".to_owned(),
        shutdown_rx,
    );

    let test_result = async {
        await_listener_ready(&mut connection_rx).await?;
        publish_and_receive_once(&publisher_pool, &mut consumer_rx, partial("before-close"))
            .await?;

        terminate_listener_backends(&publisher_pool, &app_name).await?;
        await_reconnect(&mut reconnect_rx, "recv_failed").await?;
        assert!(recorder.count_matching("recv_failed") >= 1);

        publish_and_receive_once(&publisher_pool, &mut consumer_rx, partial("after-close")).await
    }
    .await;

    let shutdown_result = shutdown_listener(shutdown_tx, &mut listener_task, &listener_pool).await;
    let teardown_result: TestResult<()> = fixture.teardown().await.map_err(Into::into);

    test_result?;
    shutdown_result?;
    teardown_result
}

async fn shutdown_during_reconnect_sleep(
    recorder: &ReconnectRecorder,
    mut reconnect_rx: mpsc::Receiver<String>,
) -> TestResult<()> {
    let database_url = crate::postgres_fixture::required_postgres_url();
    let invalid_pool = invalid_pg_pool(&database_url)?;
    let consumer_bus = Arc::new(InMemoryBus::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut listener_task = PgListener::spawn(
        invalid_pool.clone(),
        consumer_bus,
        reqwest::Client::new(),
        SecretString::new(CLUSTER_TOKEN.to_owned().into()),
        "http://listener.local".to_owned(),
        shutdown_rx,
    );

    let test_result: TestResult<()> = async {
        await_reconnect(&mut reconnect_rx, "connect_failed").await?;
        assert_eq!(recorder.count_matching("connect_failed"), 1);
        Ok(())
    }
    .await;

    let shutdown_result = shutdown_listener(shutdown_tx, &mut listener_task, &invalid_pool).await;

    test_result?;
    shutdown_result
}

fn pg_pool_with_application_name(
    database_url: &str,
    app_name: &str,
    connection_tx: mpsc::UnboundedSender<()>,
) -> TestResult<PgPool> {
    let options = database_url
        .parse::<PgConnectOptions>()?
        .application_name(app_name);
    Ok(PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |_connection, _metadata| {
            let connection_tx = connection_tx.clone();
            Box::pin(async move {
                connection_tx
                    .send(())
                    .expect("listener connection receiver remains open");
                Ok(())
            })
        })
        .connect_lazy_with(options))
}

fn invalid_pg_pool(database_url: &str) -> TestResult<PgPool> {
    let options = database_url
        .parse::<PgConnectOptions>()?
        .password("invalid-pg-listener-password");
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy_with(options))
}

async fn await_listener_ready(receiver: &mut mpsc::UnboundedReceiver<()>) -> TestResult<()> {
    // PgListener holds the first pool connection. Its initial queue-usage poll opens the
    // second connection only after PgListener::listen has completed.
    for _ in 0..2 {
        tokio::time::timeout(RECOVERY_TIMEOUT, receiver.recv())
            .await
            .map_err(|_| error("timed out waiting for PostgreSQL LISTEN readiness"))?
            .ok_or_else(|| error("PostgreSQL LISTEN readiness channel closed"))?;
    }
    Ok(())
}

async fn shutdown_listener(
    shutdown_tx: watch::Sender<bool>,
    listener_task: &mut JoinHandle<()>,
    pool: &PgPool,
) -> TestResult<()> {
    let send_result = shutdown_tx
        .send(true)
        .map_err(|_| error("pg listener shutdown receiver dropped"));
    let join_result = match tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut *listener_task).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => {
            listener_task.abort();
            let _ = listener_task.await;
            Err(error("timed out waiting for pg listener shutdown"))
        }
    };
    pool.close().await;
    send_result?;
    join_result
}

async fn await_reconnect(
    receiver: &mut mpsc::Receiver<String>,
    expected_reason: &str,
) -> TestResult<()> {
    tokio::time::timeout(RECOVERY_TIMEOUT, async {
        loop {
            let reason = receiver
                .recv()
                .await
                .ok_or_else(|| error("reconnect metric channel closed"))?;
            if reason == expected_reason {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| {
        error(format!(
            "timed out waiting for reconnect reason={expected_reason}"
        ))
    })?
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

async fn publish_and_receive_once(
    pool: &PgPool,
    receiver: &mut BusReceiver,
    partial: RequestEventPartial,
) -> TestResult<()> {
    let event_id = partial.event_id.clone();
    let payload = serde_json::to_string(&RequestEventUpdate::Partial(partial))?;
    publish_payload(pool, &payload).await?;
    tokio::time::timeout(RECOVERY_TIMEOUT, receive_partial(receiver, &event_id))
        .await
        .map_err(|_| error(format!("timed out waiting for partial {event_id}")))??;
    Ok(())
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

#[derive(Clone)]
struct ReconnectRecorder {
    counts: Arc<Mutex<HashMap<String, u64>>>,
    reconnect_tx: mpsc::Sender<String>,
}

impl ReconnectRecorder {
    fn new() -> (Self, mpsc::Receiver<String>) {
        let (reconnect_tx, reconnect_rx) = mpsc::channel(8);
        (
            Self {
                counts: Arc::new(Mutex::new(HashMap::new())),
                reconnect_tx,
            },
            reconnect_rx,
        )
    }

    fn count_matching(&self, reason: &str) -> u64 {
        self.counts
            .lock()
            .expect("reconnect recorder lock")
            .get(reason)
            .copied()
            .unwrap_or(0)
    }
}

struct ReconnectCounter {
    reason: String,
    counts: Arc<Mutex<HashMap<String, u64>>>,
    reconnect_tx: mpsc::Sender<String>,
}

impl CounterFn for ReconnectCounter {
    fn increment(&self, value: u64) {
        let mut counts = self.counts.lock().expect("reconnect recorder lock");
        *counts.entry(self.reason.clone()).or_insert(0) += value;
        drop(counts);
        self.reconnect_tx
            .try_send(self.reason.clone())
            .expect("reconnect metric receiver remains ready");
    }

    fn absolute(&self, value: u64) {
        self.counts
            .lock()
            .expect("reconnect recorder lock")
            .insert(self.reason.clone(), value);
    }
}

impl Recorder for ReconnectRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        if key.name() != "sse_pg_listener_reconnects_total" {
            return Counter::noop();
        }
        let reason = key
            .labels()
            .find(|label| label.key() == "reason")
            .map(|label| label.value().to_owned())
            .expect("reconnect metric carries reason label");
        Counter::from_arc(Arc::new(ReconnectCounter {
            reason,
            counts: Arc::clone(&self.counts),
            reconnect_tx: self.reconnect_tx.clone(),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn partial(name: &str) -> RequestEventPartial {
    RequestEventPartial {
        event_id: format!("event-{name}"),
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
