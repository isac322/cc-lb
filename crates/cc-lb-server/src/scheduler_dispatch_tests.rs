use super::*;

#[path = "scheduler_dispatch_tests/support.rs"]
mod support;

use cc_lb_engine::cache_keepalive::CacheKeepaliveEnqueuer;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_storage_api::{
    CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
};
use serde_json::Value;

use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;

use self::support::{FailingPusher, Fixture};

#[tokio::test]
async fn durable_cache_keepalive_job_decrypts_resigns_dispatches_and_reschedules() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let first_job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    let outcome = dispatch
        .dispatch_cache_keepalive(first_job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    fixture.pending_keepalive_job(2).await;
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url, "http://fake-upstream.local/v1/messages");
    assert_eq!(requests[0].headers["x-api-key"], "sk-ant-rotated");
    assert_eq!(requests[0].headers["anthropic-version"], "2023-06-01");
    let body: Value = serde_json::from_slice(&requests[0].body).expect("body json");
    assert_eq!(body["max_tokens"], 0);
    assert!(body.get("stream").is_none());
    let signer_calls = fixture.signer_calls.lock().expect("signer calls lock");
    assert_eq!(signer_calls.as_slice(), &["fake-upstream:"]);
}

#[tokio::test]
async fn hit_reschedule_enqueue_failure_terminalizes_new_generation() {
    let fixture = Fixture::new().await;
    let enqueuer = fixture.enqueuer(Arc::new(fixture.backend.backend.clone()));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let first_job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));

    let result = dispatch.dispatch_cache_keepalive(first_job).await;

    assert!(result.is_err());
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}

#[tokio::test]
async fn cache_keepalive_durable_cancel_makes_queued_old_generation_noop() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let queued_job = fixture.pending_keepalive_job(1).await;
    enqueuer
        .cancel_cache_keepalive(cc_lb_engine::cache_keepalive::CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            principal_id: "principal".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
        })
        .await
        .expect("cancel durable keepalive");
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(queued_job)
        .await
        .expect("dispatch terminalized keepalive job");

    assert!(matches!(outcome, JobOutcome::Noop));
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}
