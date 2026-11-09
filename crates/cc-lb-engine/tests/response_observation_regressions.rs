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
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_domain::Provider;
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, ProviderScope, UpstreamDispatch};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_storage_api::RequestEvent;
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{MetaStore, Storage as StorageTrait};
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

use common::{TestAuthn, TestLifecycleBus, TestState, messages_request};

const BOUNDED_WAIT: Duration = Duration::from_secs(2);
const RESPONSE_SPAN_CHILD_ENV: &str = "CC_LB_RESPONSE_SPAN_REGRESSION_CHILD";
const RESPONSE_SPAN_TEST_NAME: &str =
    "response_observation_regressions::response_span_closes_at_eos_and_body_drop";
const NUMERIC_TELEMETRY_CHILD_ENV: &str = "CC_LB_NUMERIC_TELEMETRY_REGRESSION_CHILD";
const NUMERIC_TELEMETRY_TEST_NAME: &str = "response_observation_regressions::lifecycle_exports_queryable_numeric_stage_and_response_stream_attributes";
const CHILD_WAIT: Duration = Duration::from_secs(15);

fn run_span_regression_child(test_name: &str, child_env: &str) {
    let executable = std::env::current_exe().expect("locate integration test executable");
    let child = Command::new(executable)
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(child_env, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn isolated response regression child");
    let child_id = child.id();
    let (output_tx, output_rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let _ = output_tx.send(child.wait_with_output());
    });

    let output = match output_rx.recv_timeout(CHILD_WAIT) {
        Ok(output) => output.expect("wait for isolated response regression child"),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            signal_child(child_id, "-TERM");
            let output = match output_rx.recv_timeout(BOUNDED_WAIT) {
                Ok(output) => {
                    output.expect("wait for terminated isolated response regression child")
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    signal_child(child_id, "-KILL");
                    output_rx
                        .recv_timeout(BOUNDED_WAIT)
                        .expect("killed isolated response regression child exits")
                        .expect("wait for killed isolated response regression child")
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("isolated response regression child waiter disconnected after timeout")
                }
            };
            waiter
                .join()
                .expect("isolated response regression child waiter joins");
            panic!(
                "isolated response regression child timed out\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("isolated response regression child waiter disconnected")
        }
    };
    waiter
        .join()
        .expect("isolated response regression child waiter joins");
    assert_child_success(output);
}

fn signal_child(child_id: u32, signal: &str) {
    let child_id = child_id.to_string();
    let status = Command::new("kill")
        .args([signal, child_id.as_str()])
        .status()
        .expect("invoke kill for isolated response regression child");
    assert!(
        status.success(),
        "failed to {signal} child process {child_id}"
    );
}

fn assert_child_success(output: Output) {
    assert!(
        output.status.success(),
        "isolated response regression child failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn lifecycle_exports_queryable_numeric_stage_and_response_stream_attributes() {
    if std::env::var_os(NUMERIC_TELEMETRY_CHILD_ENV).is_none() {
        run_span_regression_child(NUMERIC_TELEMETRY_TEST_NAME, NUMERIC_TELEMETRY_CHILD_ENV);
        return;
    }
    use opentelemetry::Value;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = Registry::default()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("lifecycle-numeric")));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("numeric telemetry runtime");
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            let test_bus = TestLifecycleBus::new();
            let body = Bytes::from_static(
                b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}\n\n",
            );
            let lifecycle = lifecycle(
                Arc::new(SequenceDispatch::new([ResponseSpec {
                    body: body.clone(),
                    content_encoding: None,
                }])),
                &test_bus,
            );
            let request = stream_request();
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("numeric test authenticates");
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("numeric test response");
            assert_eq!(response.status(), StatusCode::OK);
            let collected = response
                .into_body()
                .collect()
                .await
                .expect("numeric test response body")
                .to_bytes();
            assert_eq!(collected, body);
            lifecycle.shutdown().await;
        });
    });
    provider
        .force_flush()
        .expect("flush lifecycle numeric spans");
    let spans = exporter
        .get_finished_spans()
        .expect("exported lifecycle numeric spans");
    let span = |name| {
        spans
            .iter()
            .find(|span| span.name == name)
            .unwrap_or_else(|| panic!("missing span {name}"))
    };
    for (span_name, fields) in [
        ("proxy.authenticate", &["cc_lb.auth.duration_ms"][..]),
        (
            "proxy.route",
            &["cc_lb.route.candidate_count", "cc_lb.route.duration_ms"][..],
        ),
        (
            "proxy.upstream_attempt",
            &[
                "cc_lb.upstream.shape_ms",
                "cc_lb.upstream.sign_ms",
                "cc_lb.upstream.ttfb_ms",
            ][..],
        ),
        (
            "proxy.handle",
            &[
                "cc_lb.request.duration_ms",
                "cc_lb.request.finalize_ms",
                "cc_lb.request.unaccounted_ms",
            ][..],
        ),
        (
            "proxy.response_stream",
            &[
                "cc_lb.request.duration_ms",
                "cc_lb.request.finalize_ms",
                "cc_lb.response_body_ms",
                "stream_first_chunk_ms",
                "stream_total_ms",
                "sse_event_count",
                "content_delta_count",
                "ping_count",
                "total_bytes",
            ][..],
        ),
    ] {
        let selected_span = span(span_name);
        for field in fields {
            let values = selected_span
                .attributes
                .iter()
                .filter(|attribute| attribute.key.as_str() == *field)
                .map(|attribute| &attribute.value)
                .collect::<Vec<_>>();
            assert!(
                !values.is_empty(),
                "{span_name}.{field} must be present in the exported span"
            );
            assert!(
                values
                    .iter()
                    .all(|value| matches!(value, Value::I64(value) if *value >= 0)),
                "{span_name}.{field} must contain only nonnegative OTLP integers, got {values:?}"
            );
        }
    }
    for span_name in [
        "proxy.handle",
        "proxy.upstream_attempt",
        "proxy.response_stream",
    ] {
        let values = span(span_name)
            .attributes
            .iter()
            .filter(|attribute| attribute.key.as_str() == "http.response.status_code")
            .map(|attribute| &attribute.value)
            .collect::<Vec<_>>();
        assert!(!values.is_empty(), "{span_name} must export a status code");
        assert!(
            values.iter().all(|value| *value == &Value::I64(200)),
            "{span_name} must export only integer 200 status codes, got {values:?}"
        );
    }
    for span_name in ["proxy.handle", "proxy.response_stream"] {
        let span = span(span_name);
        for (field, expected) in [
            ("cc_lb.request.retry_count", 0),
            ("gen_ai.usage.input_tokens", 3),
            ("gen_ai.usage.output_tokens", 5),
        ] {
            let values = span
                .attributes
                .iter()
                .filter(|attribute| attribute.key.as_str() == field)
                .map(|attribute| &attribute.value)
                .collect::<Vec<_>>();
            assert!(!values.is_empty(), "{span_name}.{field} must be present");
            assert!(
                values.iter().all(|value| *value == &Value::I64(expected)),
                "{span_name}.{field} must remain integer {expected}, got {values:?}"
            );
        }
    }
    provider
        .shutdown()
        .expect("shutdown lifecycle numeric provider");
}

#[tokio::test]
async fn openai_responses_lifecycle_observes_content_count_timing_and_terminal() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let body = Bytes::from_static(
        b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n\
event: response.reasoning.delta\ndata: {\"type\":\"response.reasoning.delta\",\"delta\":\"hidden\"}\n\n\
event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\" world\"}\n\n\
event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}}\n\n",
    );
    let lifecycle = openai_lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec {
            body: body.clone(),
            content_encoding: None,
        }])),
        &test_bus,
    );
    let mut request = http::Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(
            br#"{"model":"gpt-test","input":"hello","stream":true}"#,
        ))
        .expect("OpenAI Responses request builds");
    request
        .extensions_mut()
        .insert(ProviderScope(Provider::Openai));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("OpenAI Responses request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("OpenAI Responses request is handled");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("OpenAI Responses body collects")
            .to_bytes(),
        body
    );

    let (stream, terminal) = tokio::time::timeout(BOUNDED_WAIT, async {
        let mut stream = None;
        let mut terminal = None;
        while stream.is_none() || terminal.is_none() {
            match lifecycle_rx
                .recv()
                .await
                .expect("lifecycle event delivered")
            {
                LifecycleEvent::StreamCompleted {
                    result: Ok(success),
                    ..
                } => stream = Some(success),
                LifecycleEvent::RequestTerminated {
                    client_status,
                    reason,
                    ..
                } => terminal = Some((client_status, reason)),
                _ => {}
            }
        }
        (
            stream.expect("stream completion observed"),
            terminal.expect("terminal observed"),
        )
    })
    .await
    .expect("OpenAI Responses lifecycle events arrive");
    lifecycle.shutdown().await;

    assert_eq!(stream.sse_event_count, 4);
    assert_eq!(stream.content_delta_count, Some(2));
    let first = stream
        .stream_first_content_delta_ms
        .expect("first content timing observed");
    let last = stream
        .stream_last_content_delta_ms
        .expect("last content timing observed");
    assert!(first <= last, "content timing must be monotonic");
    assert!(
        stream.inter_token_avg_ms.is_some(),
        "inter-token average observed for multiple content frames"
    );
    assert_eq!(
        terminal,
        (StatusCode::OK.as_u16(), TerminationReason::Success)
    );
}

#[tokio::test]
async fn native_gemini_lifecycle_observes_content_count_timing_and_terminal() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let body = Bytes::from_static(
        b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"},{\"thought\":true,\"text\":\"hidden\"}]}}]}\n\n\
data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" world\"}]}}]}\n\n\
data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n",
    );
    let lifecycle = provider_lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec {
            body: body.clone(),
            content_encoding: None,
        }])),
        &test_bus,
        StorageUpstreamKind::GeminiApiKey,
    );
    let request = http::Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-test:streamGenerateContent?alt=sse")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(br#"{"contents":[],"stream":true}"#))
        .expect("native Gemini request builds");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("native Gemini request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("native Gemini request is handled");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("native Gemini body collects")
            .to_bytes(),
        body
    );

    let (stream, terminal) = tokio::time::timeout(BOUNDED_WAIT, async {
        let mut stream = None;
        let mut terminal = None;
        while stream.is_none() || terminal.is_none() {
            match lifecycle_rx
                .recv()
                .await
                .expect("lifecycle event delivered")
            {
                LifecycleEvent::StreamCompleted {
                    result: Ok(success),
                    ..
                } => stream = Some(success),
                LifecycleEvent::RequestTerminated {
                    client_status,
                    reason,
                    ..
                } => terminal = Some((client_status, reason)),
                _ => {}
            }
        }
        (
            stream.expect("stream completion observed"),
            terminal.expect("terminal observed"),
        )
    })
    .await
    .expect("native Gemini lifecycle events arrive");
    lifecycle.shutdown().await;

    assert_eq!(stream.sse_event_count, 3);
    assert_eq!(stream.content_delta_count, Some(2));
    let first = stream
        .stream_first_content_delta_ms
        .expect("first content timing observed");
    let last = stream
        .stream_last_content_delta_ms
        .expect("last content timing observed");
    assert!(first <= last, "content timing must be monotonic");
    assert!(
        stream.inter_token_avg_ms.is_some(),
        "inter-token average observed for multiple content frames"
    );
    assert_eq!(
        terminal,
        (StatusCode::OK.as_u16(), TerminationReason::Success)
    );
}

#[tokio::test]
async fn gemini_oauth_lifecycle_observes_wrapped_content_count_timing_and_terminal() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let body = Bytes::from_static(
        b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"},{\"thought\":true,\"text\":\"hidden\"}]}}]}}\n\n\
data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" world\"}]}}]}}\n\n\
data: {\"response\":{\"candidates\":[{\"finishReason\":\"STOP\"}]}}\n\n",
    );
    let expected_body = Bytes::from_static(
        b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"},{\"text\":\"hidden\",\"thought\":true}]}}]}\n\n\
data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" world\"}]}}]}\n\n\
data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n",
    );
    let lifecycle = provider_lifecycle(
        Arc::new(SequenceDispatch::new([ResponseSpec {
            body: body.clone(),
            content_encoding: None,
        }])),
        &test_bus,
        StorageUpstreamKind::GeminiOauth,
    );
    let request = http::Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-test:streamGenerateContent?alt=sse")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(br#"{"contents":[],"stream":true}"#))
        .expect("Gemini OAuth request builds");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("Gemini OAuth request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("Gemini OAuth request is handled");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("Gemini OAuth body collects")
            .to_bytes(),
        expected_body
    );

    let (stream, terminal) = tokio::time::timeout(BOUNDED_WAIT, async {
        let mut stream = None;
        let mut terminal = None;
        while stream.is_none() || terminal.is_none() {
            match lifecycle_rx
                .recv()
                .await
                .expect("lifecycle event delivered")
            {
                LifecycleEvent::StreamCompleted {
                    result: Ok(success),
                    ..
                } => stream = Some(success),
                LifecycleEvent::RequestTerminated {
                    client_status,
                    reason,
                    ..
                } => terminal = Some((client_status, reason)),
                _ => {}
            }
        }
        (
            stream.expect("stream completion observed"),
            terminal.expect("terminal observed"),
        )
    })
    .await
    .expect("Gemini OAuth lifecycle events arrive");
    lifecycle.shutdown().await;

    assert_eq!(stream.sse_event_count, 3);
    assert_eq!(stream.content_delta_count, Some(2));
    let first = stream
        .stream_first_content_delta_ms
        .expect("first content timing observed");
    let last = stream
        .stream_last_content_delta_ms
        .expect("last content timing observed");
    assert!(first <= last, "content timing must be monotonic");
    assert!(
        stream.inter_token_avg_ms.is_some(),
        "inter-token average observed for multiple content frames"
    );
    assert_eq!(
        terminal,
        (StatusCode::OK.as_u16(), TerminationReason::Success)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn response_span_closes_at_eos_and_body_drop() {
    if std::env::var_os(RESPONSE_SPAN_CHILD_ENV).is_none() {
        run_span_regression_child(RESPONSE_SPAN_TEST_NAME, RESPONSE_SPAN_CHILD_ENV);
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
    let persisted = common::stored_request_events(storage.as_ref())
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
fn openai_lifecycle(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
) -> Lifecycle {
    provider_lifecycle(dispatcher, test_bus, StorageUpstreamKind::OpenaiApiKey)
}

fn provider_lifecycle(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    kind: StorageUpstreamKind,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
                .expect("provider test upstream id parses"),
            name: "provider-test-upstream".to_owned(),
            kind,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        }])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
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
