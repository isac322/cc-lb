mod common;
mod reservation_refund_invariant_support;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::lifecycle_limit_reconcile_subscriber::spawn_lifecycle_limit_reconcile_subscriber;
use cc_lb_engine::{DispatchError, Lifecycle, UpstreamDispatch};
use cc_lb_plugin_api::SignedRequest;
use cc_lb_storage_api::types::{
    KeyStatus, Limit as StoredLimit, LimitKind as StoredLimitKind, StoredApiKeyRecord,
};
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::Notify;

use common::{TestLifecycleBus, messages_request};
use reservation_refund_invariant_support::{
    json_dispatch, normal_sse_frame, pending_sse, sse_dispatch,
};

const PRINCIPAL_ID: &str = "principal-test";
const KEY_ID: &str = "reservation-refund-key";
const RESERVATION_AMOUNT: i64 = 100;

struct BlockingDispatch {
    started: Arc<Notify>,
}

#[async_trait]
impl UpstreamDispatch for BlockingDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.started.notify_waiters();
        std::future::pending().await
    }
}

fn engine_and_subject() -> (Arc<LimitEngine>, Arc<PrincipalView>, StoredApiKeyRecord) {
    let view = Arc::new(PrincipalView::for_tests(
        PRINCIPAL_ID,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::new(),
    ));
    let record = StoredApiKeyRecord {
        key_hash_b64: KEY_ID.to_owned(),
        status: KeyStatus::Active,
        limit_overrides: vec![StoredLimit {
            kind: StoredLimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: RESERVATION_AMOUNT,
        }],
        ..StoredApiKeyRecord::default()
    };
    let engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_engine::SystemClock),
    );

    (engine, view, record)
}

fn reserve(
    engine: &Arc<LimitEngine>,
    view: &PrincipalView,
    record: &StoredApiKeyRecord,
    amount: i64,
) -> cc_lb_engine::api_keys::limit_engine::Reservation {
    engine
        .reserve(view, record, PRINCIPAL_ID, "claude-test", amount, 0, None)
        .expect("reservation succeeds")
}

fn assert_exactly_one_refund(
    engine: &Arc<LimitEngine>,
    view: &PrincipalView,
    record: &StoredApiKeyRecord,
) {
    let verifier = reserve(engine, view, record, RESERVATION_AMOUNT);
    assert!(
        engine
            .reserve(view, record, PRINCIPAL_ID, "claude-test", 1, 0, None)
            .is_err(),
        "a missing refund rejects the verifier and a double refund admits one extra token"
    );
    drop(verifier);
}

fn lifecycle_with_reservation(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    engine: Arc<LimitEngine>,
    record: StoredApiKeyRecord,
) -> Lifecycle {
    let state = common::TestState::default();
    common::lifecycle_with_parts(
        common::TestAuthn::new(state),
        Arc::new(common::TestRouter {
            base_url: url::Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        dispatcher,
        vec![Arc::new(common::RecordingHook::default())],
        cc_lb_engine::LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc())
    .with_static_limit_subject(engine, PRINCIPAL_ID.to_owned(), KEY_ID.to_owned(), record)
}

fn request(stream: bool) -> http::Request<Bytes> {
    let stream = if stream { "true" } else { "false" };
    messages_request(Bytes::from(format!(
        r#"{{"model":"claude-test","max_tokens":{RESERVATION_AMOUNT},"messages":[],"stream":{stream}}}"#,
    )))
}

#[test]
fn reservation_refund_invariant_drop_before_dispatch_refunds_exactly_once() {
    let (engine, view, record) = engine_and_subject();

    drop(reserve(&engine, &view, &record, RESERVATION_AMOUNT));

    assert_exactly_one_refund(&engine, &view, &record);
}

#[tokio::test]
async fn reservation_refund_invariant_drop_after_reservation_before_completion_refunds_once() {
    let (engine, view, record) = engine_and_subject();
    let test_bus = TestLifecycleBus::new();
    let started = Arc::new(Notify::new());
    let started_wait = started.notified();
    let lifecycle = lifecycle_with_reservation(
        Arc::new(BlockingDispatch {
            started: Arc::clone(&started),
        }),
        &test_bus,
        Arc::clone(&engine),
        record.clone(),
    );

    let request_task = tokio::spawn(async move { lifecycle.handle(request(false)).await });
    started_wait.await;
    request_task.abort();
    assert!(
        request_task
            .await
            .expect_err("aborted request task returns a join error")
            .is_cancelled(),
        "request future is cancelled after reservation and before response completion"
    );

    assert_exactly_one_refund(&engine, &view, &record);
}

#[tokio::test]
async fn reservation_refund_invariant_buffered_success_forgets_and_reconciles_once() {
    let (engine, view, record) = engine_and_subject();
    let test_bus = TestLifecycleBus::new();
    let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(16);
    let subscriber = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, Arc::clone(&engine));
    let lifecycle = lifecycle_with_reservation(
        json_dispatch(
            StatusCode::OK,
            Bytes::from_static(
                br#"{"type":"message","usage":{"input_tokens":0,"output_tokens":40}}"#,
            ),
        ),
        &test_bus,
        Arc::clone(&engine),
        record.clone(),
    );

    let downstream = lifecycle
        .handle(request(false))
        .await
        .expect("buffered request succeeds");
    let body = downstream
        .into_body()
        .collect()
        .await
        .expect("buffered downstream body collects")
        .to_bytes();
    assert!(body.contains(&b'4'));
    subscriber.shutdown().await;

    let verifier = reserve(&engine, &view, &record, 60);
    assert!(
        engine
            .reserve(&view, &record, PRINCIPAL_ID, "claude-test", 1, 0, None)
            .is_err(),
        "one reconciliation leaves the 40-token actual plus the 60-token verifier"
    );
    drop(verifier);
}

#[tokio::test]
async fn reservation_refund_invariant_stream_end_forgets_and_reconciles_once() {
    let (engine, view, record) = engine_and_subject();
    let test_bus = TestLifecycleBus::new();
    let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(16);
    let subscriber = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, Arc::clone(&engine));
    let lifecycle = lifecycle_with_reservation(
        sse_dispatch(
            StatusCode::OK,
            Body::from(Bytes::from_static(
                b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":0,\"output_tokens\":40}}\n\n",
            )),
        ),
        &test_bus,
        Arc::clone(&engine),
        record.clone(),
    );

    let downstream = lifecycle
        .handle(request(true))
        .await
        .expect("streaming request returns headers");
    let body = downstream
        .into_body()
        .collect()
        .await
        .expect("stream reaches EOF")
        .to_bytes();
    assert!(body.starts_with(b"event: message_stop"));
    subscriber.shutdown().await;

    let verifier = reserve(&engine, &view, &record, 60);
    assert!(
        engine
            .reserve(&view, &record, PRINCIPAL_ID, "claude-test", 1, 0, None)
            .is_err(),
        "stream EOF reconciles exactly once before the verifier consumes the remainder"
    );
    drop(verifier);
}

#[tokio::test]
async fn reservation_refund_invariant_post_headers_stream_disconnect_refunds_without_reconcile() {
    let (engine, view, record) = engine_and_subject();
    let test_bus = TestLifecycleBus::new();
    let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(16);
    let subscriber = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, Arc::clone(&engine));
    let waiting = Arc::new(Notify::new());
    let lifecycle = lifecycle_with_reservation(
        sse_dispatch(StatusCode::OK, pending_sse(waiting)),
        &test_bus,
        Arc::clone(&engine),
        record.clone(),
    );

    let mut downstream = lifecycle
        .handle(request(true))
        .await
        .expect("streaming request returns headers")
        .into_body();
    let first = downstream
        .frame()
        .await
        .expect("stream yields a post-header frame")
        .expect("frame is successful")
        .into_data()
        .expect("frame contains data");
    assert_eq!(first, normal_sse_frame());
    drop(downstream);
    subscriber.shutdown().await;

    assert_exactly_one_refund(&engine, &view, &record);
}
