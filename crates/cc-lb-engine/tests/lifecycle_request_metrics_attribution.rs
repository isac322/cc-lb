//! Request-level metrics must carry the real principal, upstream, and model
//! assembled by the lifecycle path, not placeholder attribution.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_control::RequestEventBus as _;
use cc_lb_lifecycle::LifecycleEvent;
use http::StatusCode;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tokio::time::timeout;

use crate::common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    lifecycle_with, messages_request, settle,
};

const PRINCIPAL: &str = "principal-test";
const UPSTREAM: &str = "test-upstream";
const MODEL: &str = "claude-test";

struct Sample {
    name: String,
    labels: BTreeMap<String, String>,
    value: DebugValue,
}

/// Take one snapshot: histogram values are drained by each snapshot call.
fn snapshot(snapshotter: &Snapshotter) -> Vec<Sample> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| Sample {
            name: key.key().name().to_owned(),
            labels: key
                .key()
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect(),
            value,
        })
        .collect()
}

fn named<'a>(samples: &'a [Sample], name: &str) -> Vec<&'a Sample> {
    samples
        .iter()
        .filter(|sample| sample.name == name)
        .collect()
}

fn label<'a>(sample: &'a Sample, key: &str) -> Option<&'a str> {
    sample.labels.get(key).map(String::as_str)
}

#[tokio::test(flavor = "current_thread")]
async fn authenticated_request_completion_metric_carries_real_attribution() {
    // Given the real lifecycle-event logger subscribed to a proxy lifecycle.
    // The current-thread runtime runs the spawned logger on this thread, so
    // the thread-local recorder observes everything it emits.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let test_bus = TestLifecycleBus::new();
    let logger =
        cc_lb_engine::spawn_lifecycle_event_logger(test_bus.bus.attach_lifecycle_writer(256));
    let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
    )
    .with_event_bus(test_bus.bus_arc());

    // When an authenticated request crosses the proxy lifecycle.
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _, _) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    drop(lifecycle);
    settle().await;
    // The writer channel and the broadcast receive each event in the same
    // publish call, so the terminal event is queued for the logger once the
    // broadcast delivers it.
    timeout(Duration::from_secs(3), async {
        while !matches!(
            lifecycle_events
                .recv()
                .await
                .expect("lifecycle broadcast stays open"),
            LifecycleEvent::RequestTerminated { .. }
        ) {}
    })
    .await
    .expect("terminal lifecycle event is published");
    logger.shutdown().await;

    // Then the request is counted once with its real attribution.
    let samples = snapshot(&snapshotter);
    let completed = named(&samples, "cc_lb_requests_completed_total");
    assert_eq!(completed.len(), 1, "one completed request series expected");
    let completed = completed[0];
    assert_eq!(label(completed, "principal"), Some(PRINCIPAL));
    assert_eq!(label(completed, "upstream"), Some(UPSTREAM));
    assert_eq!(label(completed, "model"), Some(MODEL));
    assert_eq!(label(completed, "status"), Some("200"));
    assert!(matches!(completed.value, DebugValue::Counter(1)));

    let duration = named(&samples, "cc_lb_request_completion_duration_seconds");
    assert_eq!(duration.len(), 1, "one completion duration series expected");
    let duration = duration[0];
    assert_eq!(label(duration, "upstream"), Some(UPSTREAM));
    assert_eq!(label(duration, "model"), Some(MODEL));
    assert_eq!(label(duration, "status"), Some("200"));
    assert_eq!(
        label(duration, "principal"),
        None,
        "the duration histogram must not be keyed by principal"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn streamed_response_termination_metric_carries_routed_upstream_and_model() {
    // Given a streaming upstream response relayed by the real proxy path.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::StreamingOk,
        },
    );

    // When the client consumes the stream to its end.
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _, _) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);

    // Then the stream termination names the routed upstream and requested model.
    let samples = snapshot(&snapshotter);
    let terminations = named(&samples, "cc_lb_stream_terminations_total");
    assert_eq!(
        terminations.len(),
        1,
        "one stream termination series expected"
    );
    let termination = terminations[0];
    assert_eq!(label(termination, "upstream"), Some(UPSTREAM));
    assert_eq!(label(termination, "model"), Some(MODEL));
    assert!(matches!(termination.value, DebugValue::Counter(1)));
}
