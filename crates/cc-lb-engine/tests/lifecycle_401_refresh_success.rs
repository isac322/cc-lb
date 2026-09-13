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
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState,
    collect_body, lifecycle_with_parts, messages_request,
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

#[tokio::test]
async fn t2__unauthorized_refresh_retries_once_then_succeeds() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(32);
    let dispatcher = MockDispatch {
        state: state.clone(),
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(VecDeque::from([
            StatusCode::UNAUTHORIZED,
            StatusCode::OK,
        ])))),
    };
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state.clone()),
        Arc::new(TestRouter {
            base_url: url::Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        Arc::new(TimedDispatch {
            inner: dispatcher,
            attempts: Arc::new(Mutex::new(VecDeque::from([
                Duration::from_millis(FIRST_ATTEMPT_DELAY_MS),
                Duration::ZERO,
            ]))),
        }),
        vec![hook],
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    println!(
        "refresh_count={}",
        state.refresh_count.load(Ordering::Relaxed)
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 2);
    assert_eq!(state.refresh_count.load(Ordering::Relaxed), 1);

    let mut responses = Vec::new();
    let retry_overhead_ms = loop {
        match lifecycle_events
            .try_recv()
            .expect("terminal lifecycle event is queued after response collection")
        {
            LifecycleEvent::UpstreamResponseStarted {
                status,
                bulkhead_wait_ms,
                upstream_ttfb_ms,
                ..
            } => responses.push((status, bulkhead_wait_ms, upstream_ttfb_ms)),
            LifecycleEvent::RequestTerminated { io_timings, .. } => {
                break io_timings.retry_overhead_ms;
            }
            _ => {}
        }
    };

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
        (StatusCode::OK.as_u16(), Some(0)),
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
