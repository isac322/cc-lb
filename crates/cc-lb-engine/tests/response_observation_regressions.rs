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
use cc_lb_control::{BusReceiver, LifecycleBusReceiver, RequestEventBus};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_observability::{ObservabilityError, ObservabilityHook, ObserveEvent};
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_storage_api::types::RequestEvent;
use cc_lb_storage_api::{BackendKind, MetaStore, RequestEventStore, Storage as StorageTrait};
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
use url::Url;

use common::{TestAuthn, TestLifecycleBus, TestRouter, TestState, messages_request};

const BOUNDED_WAIT: Duration = Duration::from_secs(2);
const SPAN_CHILD_ENV: &str = "CC_LB_RESPONSE_SPAN_REGRESSION_CHILD";
const SPAN_TEST_NAME: &str = "response_observation_regressions::response_span_closes_before_blocked_callback_at_eos_and_body_drop";
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
async fn response_span_closes_before_blocked_callback_at_eos_and_body_drop() {
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
    let storage = Arc::new(sqlite_storage(&dir, "blocked-finish.sqlite").await);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = test_bus.bus.subscribe_lifecycle()
    else {
        panic!("expected in-memory lifecycle receiver");
    };
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory request-event receiver");
    };
    let (hook, hook_entered_rx, release_hook_tx, callback_rx) = BlockingFinishedHook::channels();
    let expected_plaintext = Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}\n\n",
    );
    let expected_body = gzip_bytes(&expected_plaintext);
    let lifecycle = lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec::gzip(
            expected_body.clone(),
        )])),
        hook,
        &test_bus,
    );

    let response = lifecycle
        .handle(stream_request())
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
    let (first_data_rx, body_join) = collect_body_on_thread(response.into_body());

    let hook_entered = hook_entered_rx.recv_timeout(BOUNDED_WAIT);
    let first_data_before_release = first_data_rx.recv_timeout(BOUNDED_WAIT);
    let span_closed_before_release = span_closed_rx.recv_timeout(BOUNDED_WAIT);

    let _ = release_hook_tx.send(());
    let collected_body = body_join.join();
    let callback = callback_rx.recv_timeout(BOUNDED_WAIT);
    let terminal = receive_terminal(&mut lifecycle_rx).await;
    let final_event = receive_final(&mut update_rx).await;
    lifecycle.shutdown().await;

    assert!(
        hook_entered.is_ok(),
        "RequestFinished callback did not start"
    );
    assert_eq!(
        first_data_before_release.expect("body data must arrive while callback is blocked"),
        expected_body
    );
    assert_eq!(
        created_count.load(Ordering::SeqCst),
        1,
        "EOS response span was not created exactly once; seen={:?}",
        seen_spans.lock().expect("seen spans lock")
    );
    assert!(
        span_closed_before_release.is_ok(),
        "response span must close at EOS while callback is blocked"
    );
    assert_eq!(closed_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        collected_body
            .expect("body polling thread joins")
            .expect("body reaches EOS"),
        expected_body
    );
    assert_finished_event(
        callback.expect("callback returns after release"),
        StatusCode::OK,
        Some(3),
        Some(5),
    );
    assert!(
        callback_rx.try_recv().is_err(),
        "RequestFinished callback ran more than once"
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
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = test_bus.bus.subscribe_lifecycle()
    else {
        panic!("expected in-memory lifecycle receiver");
    };
    let (hook, hook_entered_rx, release_hook_tx, callback_rx) = BlockingFinishedHook::channels();
    let expected_body = gzip_bytes(&Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":7,\"output_tokens\":11}}\n\n",
    ));
    let lifecycle = lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec::gzip(
            expected_body.clone(),
        )])),
        hook,
        &test_bus,
    );

    let response = lifecycle
        .handle(stream_request())
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
    let (body_dropped_rx, body_join) = take_first_frame_and_drop_on_thread(response.into_body());

    let hook_entered = hook_entered_rx.recv_timeout(BOUNDED_WAIT);
    let body_dropped_before_release = body_dropped_rx.recv_timeout(BOUNDED_WAIT);
    let span_closed_before_release = span_closed_rx.recv_timeout(BOUNDED_WAIT);

    let _ = release_hook_tx.send(());
    let delivered = body_join.join();
    let callback = callback_rx.recv_timeout(BOUNDED_WAIT);
    let terminal = receive_terminal(&mut lifecycle_rx).await;
    lifecycle.shutdown().await;

    assert!(
        hook_entered.is_ok(),
        "RequestFinished callback did not start"
    );
    assert!(
        body_dropped_before_release.is_ok(),
        "delivered body was not dropped while the callback was blocked"
    );
    assert_eq!(
        created_count.load(Ordering::SeqCst),
        2,
        "drop response span was not created exactly once; seen={:?}",
        seen_spans.lock().expect("seen spans lock")
    );
    assert!(
        span_closed_before_release.is_ok(),
        "dropping the delivered body must close its span while the callback is blocked"
    );
    assert_eq!(closed_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        delivered
            .expect("body polling thread joins")
            .expect("first body frame is delivered"),
        expected_body
    );
    assert_finished_event(
        callback.expect("callback returns after release"),
        StatusCode::OK,
        Some(7),
        Some(11),
    );
    assert!(
        callback_rx.try_recv().is_err(),
        "RequestFinished callback ran more than once"
    );
    assert_eq!(
        terminal.expect("request terminates"),
        (StatusCode::OK.as_u16(), TerminationReason::Success)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn callback_panic_preserves_bodies_and_worker_handles_next_completion() {
    let test_bus = TestLifecycleBus::new();
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = test_bus.bus.subscribe_lifecycle()
    else {
        panic!("expected in-memory lifecycle receiver");
    };
    let (hook, normal_completion_rx) = PanicOnceFinishedHook::new();
    let first_body = Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":13,\"output_tokens\":17}}\n\n",
    );
    let second_body = Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":19,\"output_tokens\":23}}\n\n",
    );
    let lifecycle = lifecycle(
        Arc::new(SequenceDispatch::new([
            ResponseSpec::identity(first_body.clone()),
            ResponseSpec::identity(second_body.clone()),
        ])),
        hook.clone(),
        &test_bus,
    );

    let first_response = lifecycle
        .handle(stream_request())
        .await
        .expect("first response is returned");
    assert_eq!(first_response.status(), StatusCode::OK);
    assert_eq!(
        first_response.headers().get(CONTENT_LENGTH),
        Some(&HeaderValue::from_str(&first_body.len().to_string()).expect("content length"))
    );
    let delivered_first = first_response
        .into_body()
        .collect()
        .await
        .expect("first body reaches EOS despite callback panic")
        .to_bytes();

    let second_response = lifecycle
        .handle(stream_request())
        .await
        .expect("second response is returned");
    assert_eq!(second_response.status(), StatusCode::OK);
    assert_eq!(
        second_response.headers().get(CONTENT_LENGTH),
        Some(&HeaderValue::from_str(&second_body.len().to_string()).expect("content length"))
    );
    let delivered_second = second_response
        .into_body()
        .collect()
        .await
        .expect("second body reaches EOS")
        .to_bytes();

    let normal_completion = normal_completion_rx.recv_timeout(BOUNDED_WAIT);
    let terminals = receive_terminals(&mut lifecycle_rx, 2).await;
    lifecycle.shutdown().await;

    assert_eq!(delivered_first, first_body);

    assert_eq!(delivered_second, second_body);
    assert_eq!(hook.finished_calls.load(Ordering::SeqCst), 2);
    assert_finished_event(
        normal_completion.expect("worker processes completion after callback panic"),
        StatusCode::OK,
        Some(19),
        Some(23),
    );
    assert_eq!(
        terminals.expect("both requests terminate"),
        vec![
            (StatusCode::OK.as_u16(), TerminationReason::Success),
            (StatusCode::OK.as_u16(), TerminationReason::Success),
        ]
    );
}

fn lifecycle(
    dispatcher: Arc<dyn UpstreamDispatch>,
    hook: Arc<dyn ObservabilityHook>,
    test_bus: &TestLifecycleBus,
) -> Lifecycle {
    common::lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        dispatcher,
        vec![hook],
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

    fn identity(body: Bytes) -> Self {
        Self {
            body,
            content_encoding: None,
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
                reason: "test response queue lock poisoned".to_owned(),
            })?
            .pop_front()
            .ok_or_else(|| DispatchError::Transport {
                reason: "test response queue exhausted".to_owned(),
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

struct BlockingFinishedHook {
    entered_tx: Sender<()>,
    release_rx: Mutex<Receiver<()>>,
    completed_tx: Sender<ObserveEvent>,
}

impl BlockingFinishedHook {
    fn channels() -> (
        Arc<dyn ObservabilityHook>,
        Receiver<()>,
        Sender<()>,
        Receiver<ObserveEvent>,
    ) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        (
            Arc::new(Self {
                entered_tx,
                release_rx: Mutex::new(release_rx),
                completed_tx,
            }),
            entered_rx,
            release_tx,
            completed_rx,
        )
    }
}

impl ObservabilityHook for BlockingFinishedHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        if matches!(event, ObserveEvent::RequestFinished { .. }) {
            let _ = self.entered_tx.send(());
            let _ = self
                .release_rx
                .lock()
                .expect("release receiver lock")
                .recv();
            let _ = self.completed_tx.send(event);
        }
        Ok(())
    }
}

struct PanicOnceFinishedHook {
    finished_calls: AtomicUsize,
    normal_completion_tx: Sender<ObserveEvent>,
}

impl PanicOnceFinishedHook {
    fn new() -> (Arc<Self>, Receiver<ObserveEvent>) {
        let (normal_completion_tx, normal_completion_rx) = mpsc::channel();
        (
            Arc::new(Self {
                finished_calls: AtomicUsize::new(0),
                normal_completion_tx,
            }),
            normal_completion_rx,
        )
    }
}

impl ObservabilityHook for PanicOnceFinishedHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        if matches!(event, ObserveEvent::RequestFinished { .. }) {
            let call = self.finished_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                panic!("intentional RequestFinished callback panic");
            }
            let _ = self.normal_completion_tx.send(event);
        }
        Ok(())
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

fn collect_body_on_thread(body: Body) -> (Receiver<Bytes>, JoinHandle<Result<Bytes, String>>) {
    let (first_data_tx, first_data_rx) = mpsc::channel();
    let join = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(async move {
            let mut body = body;
            let mut output = BytesMut::new();
            let mut first = true;
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|error| error.to_string())?;
                if let Ok(data) = frame.into_data() {
                    if first {
                        first = false;
                        let _ = first_data_tx.send(data.clone());
                    }
                    output.extend_from_slice(&data);
                }
            }
            Ok(output.freeze())
        })
    });
    (first_data_rx, join)
}

fn take_first_frame_and_drop_on_thread(
    body: Body,
) -> (Receiver<()>, JoinHandle<Result<Bytes, String>>) {
    let (body_dropped_tx, body_dropped_rx) = mpsc::channel();
    let join = std::thread::spawn(move || {
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
            let _ = body_dropped_tx.send(());
            Ok(data)
        })
    });
    (body_dropped_rx, join)
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

fn assert_finished_event(
    event: ObserveEvent,
    expected_status: StatusCode,
    expected_input_tokens: Option<u64>,
    expected_output_tokens: Option<u64>,
) {
    let ObserveEvent::RequestFinished {
        status,
        input_tokens,
        output_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        ..
    } = event
    else {
        panic!("expected RequestFinished callback payload");
    };
    assert_eq!(status, expected_status);
    assert_eq!(input_tokens, expected_input_tokens);
    assert_eq!(output_tokens, expected_output_tokens);
    assert_eq!(cache_creation_input_tokens, Some(0));
    assert_eq!(cache_read_input_tokens, Some(0));
}

async fn sqlite_storage(dir: &tempfile::TempDir, file_name: &str) -> SqliteStorage {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await
            .expect("open sqlite storage");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite storage");
    storage
}

fn gzip_bytes(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    Bytes::from(encoder.finish().expect("gzip finish succeeds"))
}
