use crate::common;

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, LifecycleConfig, UpstreamDispatch};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_upstream::SignedRequest;
use http::StatusCode;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    lifecycle_with_parts, messages_request,
};

const FIRST_ATTEMPT_DELAY_MS: u64 = 8;

#[derive(Clone)]
struct TimedDispatch {
    inner: MockDispatch,
    attempts: Arc<Mutex<VecDeque<Duration>>>,
}

#[async_trait]
impl UpstreamDispatch for TimedDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        let delay = self
            .attempts
            .lock()
            .expect("attempt timing lock")
            .pop_front()
            .expect("attempt timing remains");
        cc_lb_engine::request_timing::record_bulkhead_wait(delay);
        let deadline = Instant::now() + delay;
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
        self.inner.dispatch(request).await
    }
}

#[derive(Clone)]
struct FailingSecondAttemptDispatch {
    first: TimedDispatch,
    attempt_count: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl UpstreamDispatch for FailingSecondAttemptDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        let count = self.attempt_count.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            self.first.dispatch(request).await
        } else {
            Err(DispatchError::Transport {
                source: Box::new(std::io::Error::other("simulated retry connection reset")),
            })
        }
    }
}

#[tokio::test]
async fn unauthorized_refresh_retries_once_then_stops() {
    let state = TestState::default();
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(32);
    let dispatcher = MockDispatch {
        state: state.clone(),
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(VecDeque::from([
            StatusCode::UNAUTHORIZED,
            StatusCode::UNAUTHORIZED,
            StatusCode::OK,
        ])))),
    };
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state.clone()),
        Arc::new(TimedDispatch {
            inner: dispatcher,
            attempts: Arc::new(Mutex::new(VecDeque::from([
                Duration::from_millis(FIRST_ATTEMPT_DELAY_MS),
                Duration::ZERO,
            ]))),
        }),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

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
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 2);
    assert_eq!(state.refresh_count.load(Ordering::Relaxed), 1);

    let (responses, retry_overhead_ms) = tokio::time::timeout(Duration::from_secs(1), async {
        let mut responses = Vec::new();
        loop {
            match lifecycle_events
                .recv()
                .await
                .expect("lifecycle event channel remains open")
            {
                LifecycleEvent::UpstreamResponseStarted {
                    status,
                    bulkhead_wait_ms,
                    upstream_ttfb_ms,
                    ..
                } => responses.push((status, bulkhead_wait_ms, upstream_ttfb_ms)),
                LifecycleEvent::RequestTerminated { io_timings, .. } => {
                    break (responses, io_timings.retry_overhead_ms);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("terminal lifecycle event arrives");

    assert_eq!(
        responses.len(),
        2,
        "each upstream attempt must retain its own timing event"
    );
    assert_eq!(
        (responses[0].0, responses[0].1),
        (
            StatusCode::UNAUTHORIZED.as_u16(),
            Some(FIRST_ATTEMPT_DELAY_MS),
        )
    );
    assert_eq!(
        (responses[1].0, responses[1].1),
        (StatusCode::UNAUTHORIZED.as_u16(), Some(0)),
        "the final attempt timing must not be replaced by retry overhead"
    );
    let first_attempt_ttfb_ms = responses[0].2.expect("first attempt TTFB is recorded");
    assert!(
        first_attempt_ttfb_ms >= FIRST_ATTEMPT_DELAY_MS,
        "first attempt delay must be observable before retry"
    );
    let _final_attempt_ttfb_ms = responses[1]
        .2
        .expect("final attempt TTFB remains separately recorded");
    let retry_overhead_ms = retry_overhead_ms.expect("retry overhead is recorded");
    assert!(retry_overhead_ms.is_finite());
    assert!(retry_overhead_ms >= 0.0);
    assert!(
        retry_overhead_ms >= first_attempt_ttfb_ms as f64,
        "retry overhead must include the completed first attempt and refresh before attempt two"
    );
}

#[tokio::test]
async fn unauthorized_refresh_second_attempt_dispatch_error_clears_stale_stage_timings() {
    let state = TestState::default();
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(32);
    let dispatcher = MockDispatch {
        state: state.clone(),
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(VecDeque::from([
            StatusCode::UNAUTHORIZED,
        ])))),
    };
    let timed_dispatch = TimedDispatch {
        inner: dispatcher,
        attempts: Arc::new(Mutex::new(VecDeque::from([Duration::from_millis(
            FIRST_ATTEMPT_DELAY_MS,
        )]))),
    };
    let failing_dispatch = FailingSecondAttemptDispatch {
        first: timed_dispatch,
        attempt_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state.clone()),
        Arc::new(failing_dispatch),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

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
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(state.refresh_count.load(Ordering::Relaxed), 1);

    let (first_attempt_ttfb, terminal_event) =
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut first_ttfb = None;
            loop {
                match lifecycle_events
                    .recv()
                    .await
                    .expect("lifecycle event channel remains open")
                {
                    LifecycleEvent::UpstreamResponseStarted {
                        upstream_ttfb_ms, ..
                    } if first_ttfb.is_none() => {
                        first_ttfb = upstream_ttfb_ms;
                    }
                    LifecycleEvent::RequestTerminated {
                        client_status,
                        upstream_body_ms,
                        io_timings,
                        ..
                    } => {
                        break (first_ttfb, (client_status, upstream_body_ms, io_timings));
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("terminal lifecycle event arrives");

    assert!(first_attempt_ttfb.is_some());
    let (client_status, upstream_body_ms, io_timings) = terminal_event;
    assert_eq!(client_status, StatusCode::BAD_GATEWAY.as_u16());
    assert_eq!(upstream_body_ms, None);
    let retry_overhead_ms = io_timings
        .retry_overhead_ms
        .expect("retry overhead is recorded");
    assert!(retry_overhead_ms.is_finite());
    assert!(retry_overhead_ms >= 0.0);
}
