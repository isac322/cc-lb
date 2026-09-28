use crate::common;

use std::collections::VecDeque;
use std::io::Write as _;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::{Bytes, BytesMut};
use cc_lb_control::RequestEventBus;
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_storage_api::RequestEvent;
use cc_lb_storage_api::{MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::SignedRequest;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Response, StatusCode};
use http_body_util::BodyExt as _;
use tokio::sync::broadcast;
use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};
use tracing_subscriber::{Layer, Registry};

use common::{TestAuthn, TestLifecycleBus, TestState, messages_request};

const BOUNDED_WAIT: Duration = Duration::from_secs(2);
const SPAN_CHILD_ENV: &str = "CC_LB_RESPONSE_SPAN_REGRESSION_CHILD";
const SPAN_TEST_NAME: &str =
    "response_observation_regressions::response_span_closes_at_eos_and_body_drop";
const CHILD_WAIT: Duration = Duration::from_secs(15);

fn run_span_regression_child() {
    let executable = std::env::current_exe().expect("locate integration test executable");
    let child = Command::new(executable)
        .args(["--exact", SPAN_TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(SPAN_CHILD_ENV, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn isolated response-span regression child");
    let child_id = child.id();
    let (output_tx, output_rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let _ = output_tx.send(child.wait_with_output());
    });

    let output = match output_rx.recv_timeout(CHILD_WAIT) {
        Ok(output) => output.expect("wait for response-span regression child"),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            signal_child(child_id, "-TERM");
            let output = match output_rx.recv_timeout(BOUNDED_WAIT) {
                Ok(output) => output.expect("wait for terminated response-span regression child"),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    signal_child(child_id, "-KILL");
                    output_rx
                        .recv_timeout(BOUNDED_WAIT)
                        .expect("killed response-span regression child exits")
                        .expect("wait for killed response-span regression child")
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("response-span regression child waiter disconnected after timeout")
                }
            };
            waiter.join().expect("response-span child waiter joins");
            panic!(
                "isolated response-span regression child timed out\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("response-span regression child waiter disconnected")
        }
    };
    waiter.join().expect("response-span child waiter joins");
    assert_child_success(output);
}

fn signal_child(child_id: u32, signal: &str) {
    let child_id = child_id.to_string();
    let status = Command::new("kill")
        .args([signal, child_id.as_str()])
        .status()
        .expect("invoke kill for response-span regression child");
    assert!(
        status.success(),
        "failed to {signal} child process {child_id}"
    );
}

fn assert_child_success(output: Output) {
    assert!(
        output.status.success(),
        "isolated response-span regression child failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn response_span_closes_at_eos_and_body_drop() {
    if std::env::var_os(SPAN_CHILD_ENV).is_none() {
        run_span_regression_child();
        return;
    }

    let (span_layer, span_closed_rx) = ResponseSpanCloseLayer::new();
    let created_count = Arc::clone(&span_layer.created_count);
    let closed_count = Arc::clone(&span_layer.closed_count);
    let seen_spans = Arc::clone(&span_layer.seen_spans);
    tracing::subscriber::set_global_default(Registry::default().with(span_layer))
        .expect("child installs its process-global tracing subscriber once");

    finite_gzip_eos_case(&span_closed_rx, &created_count, &closed_count, &seen_spans).await;
    delivered_body_drop_case(&span_closed_rx, &created_count, &closed_count, &seen_spans).await;
}

async fn finite_gzip_eos_case(
    span_closed_rx: &Receiver<()>,
    created_count: &AtomicUsize,
    closed_count: &AtomicUsize,
    seen_spans: &Mutex<Vec<String>>,
) {
    let dir = tempfile::tempdir().expect("temporary directory");
    let storage = Arc::new(sqlite_storage(&dir, "gzip-eos.sqlite").await);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let mut update_rx = test_bus.bus.subscribe();
    let expected_plaintext = Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}\n\n",
    );
    let expected_body = gzip_bytes(&expected_plaintext);
    let lifecycle = lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec::gzip(
            expected_body.clone(),
        )])),
        &test_bus,
    );

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle returns the streaming response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_LENGTH),
        Some(&HeaderValue::from_str(&expected_body.len().to_string()).expect("content length"))
    );
    assert_eq!(
        response.headers().get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    let body_join = collect_body_on_thread(response.into_body());
    let collected_body = body_join.join();
    let span_closed = span_closed_rx.recv_timeout(BOUNDED_WAIT);
    let terminal = receive_terminal(&mut lifecycle_rx).await;
    let final_event = receive_final(&mut update_rx).await;
    lifecycle.shutdown().await;

    assert_eq!(
        created_count.load(Ordering::SeqCst),
        1,
        "EOS response span was not created exactly once; seen={:?}",
        seen_spans.lock().expect("seen spans lock")
    );
    assert!(span_closed.is_ok(), "response span must close at EOS");
    assert_eq!(closed_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        collected_body
            .expect("body polling thread joins")
            .expect("body reaches EOS"),
        expected_body
    );
    assert_eq!(
        terminal.expect("request terminates"),
        (StatusCode::OK.as_u16(), TerminationReason::Success)
    );
    let final_event = final_event.expect("assembler publishes final request event");
    assert_eq!(final_event.status, StatusCode::OK.as_u16());
    assert_eq!(final_event.error_code, None);
    let persisted = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10)
        .await
        .expect("query persisted request event");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].status, StatusCode::OK.as_u16());
    assert_eq!(persisted[0].error_code, None);
}

async fn delivered_body_drop_case(
    span_closed_rx: &Receiver<()>,
    created_count: &AtomicUsize,
    closed_count: &AtomicUsize,
    seen_spans: &Mutex<Vec<String>>,
) {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let expected_body = gzip_bytes(&Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":7,\"output_tokens\":11}}\n\n",
    ));
    let lifecycle = lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec::gzip(
            expected_body.clone(),
        )])),
        &test_bus,
    );

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle returns the streaming response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_LENGTH),
        Some(&HeaderValue::from_str(&expected_body.len().to_string()).expect("content length"))
    );
    assert_eq!(
        response.headers().get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    let body_join = take_first_frame_and_drop_on_thread(response.into_body());
    let delivered = body_join.join();
    let span_closed = span_closed_rx.recv_timeout(BOUNDED_WAIT);
    let terminal = receive_terminal(&mut lifecycle_rx).await;
    lifecycle.shutdown().await;

    assert_eq!(
        created_count.load(Ordering::SeqCst),
        2,
        "drop response span was not created exactly once; seen={:?}",
        seen_spans.lock().expect("seen spans lock")
    );
    assert!(
        span_closed.is_ok(),
        "dropping the delivered body must close its span"
    );
    assert_eq!(closed_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        delivered
            .expect("body polling thread joins")
            .expect("first body frame is delivered"),
        expected_body
    );
    assert_eq!(
        terminal.expect("request terminates"),
        (StatusCode::OK.as_u16(), TerminationReason::Success)
    );
}

fn lifecycle(dispatcher: Arc<dyn UpstreamDispatch>, test_bus: &TestLifecycleBus) -> Lifecycle {
    common::lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        dispatcher,
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc())
}

fn stream_request() -> http::Request<Bytes> {
    messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ))
}

struct ResponseSpec {
    body: Bytes,
    content_encoding: Option<&'static str>,
}

impl ResponseSpec {
    fn gzip(body: Bytes) -> Self {
        Self {
            body,
            content_encoding: Some("gzip"),
        }
    }
}

struct SequenceDispatch {
    responses: Mutex<VecDeque<ResponseSpec>>,
}

impl SequenceDispatch {
    fn new(responses: impl IntoIterator<Item = ResponseSpec>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for SequenceDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let spec = self
            .responses
            .lock()
            .map_err(|_| DispatchError::Transport {
                source: Box::new(std::io::Error::other("test response queue lock poisoned")),
            })?
            .pop_front()
            .ok_or_else(|| DispatchError::Transport {
                source: Box::new(std::io::Error::other("test response queue exhausted")),
            })?;
        let mut response = Response::new(Body::from(spec.body.clone()));
        response.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&spec.body.len().to_string()).expect("content length header"),
        );
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        if let Some(content_encoding) = spec.content_encoding {
            response
                .headers_mut()
                .insert(CONTENT_ENCODING, HeaderValue::from_static(content_encoding));
        }
        Ok(response)
    }
}

#[derive(Clone)]
struct ResponseSpanCloseLayer {
    span_ids: Arc<Mutex<Vec<Id>>>,
    seen_spans: Arc<Mutex<Vec<String>>>,
    created_count: Arc<AtomicUsize>,
    closed_count: Arc<AtomicUsize>,
    closed_tx: Sender<()>,
}

impl ResponseSpanCloseLayer {
    fn new() -> (Self, Receiver<()>) {
        let (closed_tx, closed_rx) = mpsc::channel();
        (
            Self {
                span_ids: Arc::new(Mutex::new(Vec::new())),
                seen_spans: Arc::new(Mutex::new(Vec::new())),
                created_count: Arc::new(AtomicUsize::new(0)),
                closed_count: Arc::new(AtomicUsize::new(0)),
                closed_tx,
            },
            closed_rx,
        )
    }
}

impl<S> Layer<S> for ResponseSpanCloseLayer
where
    S: Subscriber,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: LayerContext<'_, S>) {
        self.seen_spans
            .lock()
            .expect("seen spans lock")
            .push(format!(
                "{}@{:?}",
                attrs.metadata().name(),
                std::thread::current().id()
            ));
        if attrs.metadata().name() == "proxy.response_stream" {
            self.span_ids
                .lock()
                .expect("response span id lock")
                .push(id.clone());
            self.created_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn on_close(&self, id: Id, _ctx: LayerContext<'_, S>) {
        let mut span_ids = self.span_ids.lock().expect("response span id lock");
        let Some(index) = span_ids.iter().position(|candidate| candidate == &id) else {
            return;
        };
        span_ids.swap_remove(index);
        self.closed_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.closed_tx.send(());
    }
}

fn collect_body_on_thread(body: Body) -> JoinHandle<Result<Bytes, String>> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(async move {
            let mut body = body;
            let mut output = BytesMut::new();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|error| error.to_string())?;
                if let Ok(data) = frame.into_data() {
                    output.extend_from_slice(&data);
                }
            }
            Ok(output.freeze())
        })
    })
}

fn take_first_frame_and_drop_on_thread(body: Body) -> JoinHandle<Result<Bytes, String>> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(async move {
            let mut body = body;
            let frame = body
                .frame()
                .await
                .ok_or_else(|| "body ended before its declared frame".to_owned())?
                .map_err(|error| error.to_string())?;
            let data = frame
                .into_data()
                .map_err(|_| "first response frame was not data".to_owned())?;
            drop(body);
            Ok(data)
        })
    })
}

async fn receive_terminal(
    rx: &mut broadcast::Receiver<LifecycleEvent>,
) -> Result<(u16, TerminationReason), String> {
    let terminals = receive_terminals(rx, 1).await?;
    terminals
        .into_iter()
        .next()
        .ok_or_else(|| "request did not terminate".to_owned())
}

async fn receive_terminals(
    rx: &mut broadcast::Receiver<LifecycleEvent>,
    expected: usize,
) -> Result<Vec<(u16, TerminationReason)>, String> {
    tokio::time::timeout(BOUNDED_WAIT, async {
        let mut terminals = Vec::with_capacity(expected);
        while terminals.len() < expected {
            if let LifecycleEvent::RequestTerminated {
                client_status,
                reason,
                ..
            } = rx.recv().await.map_err(|error| error.to_string())?
            {
                terminals.push((client_status, reason));
            }
        }
        Ok(terminals)
    })
    .await
    .map_err(|_| format!("timed out waiting for {expected} request termination(s)"))?
}

async fn receive_final(
    rx: &mut broadcast::Receiver<RequestEventUpdate>,
) -> Result<RequestEvent, String> {
    tokio::time::timeout(BOUNDED_WAIT, async {
        loop {
            match rx.recv().await.map_err(|error| error.to_string())? {
                RequestEventUpdate::Final(update) => return Ok(update.event),
                RequestEventUpdate::Partial(_) => {}
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for final request event".to_owned())?
}

async fn sqlite_storage(dir: &tempfile::TempDir, file_name: &str) -> SqliteStorage {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite storage");
    storage
        .initialize()
        .await
        .expect("initialize sqlite storage");
    storage
}

fn gzip_bytes(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    Bytes::from(encoder.finish().expect("gzip finish succeeds"))
}
