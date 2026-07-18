use super::*;

#[path = "scheduler_dispatch_tests/support.rs"]
mod support;

use ::http::StatusCode;
use cc_lb_contract::{Limit, LimitKind};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::cache_keepalive::CacheKeepaliveEnqueuer;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_storage_api::upstream::UpstreamKind;
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
async fn cache_keepalive_missing_accounting_key_terminalizes_without_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET accounting_key_id = ? WHERE session_key_hash = ?",
    )
    .bind("missing-accounting-key")
    .bind("session-hash")
    .execute(fixture.storage.pool())
    .await
    .expect("set missing accounting key");
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn cache_keepalive_accounting_key_uses_real_reserve_before_dispatch() {
    let fixture = Fixture::new().await;
    let key_store = KeyStore::new(fixture.storage.clone());
    let (record, _) = key_store
        .create(
            "principal",
            CreateParams {
                upstream_kind: cc_lb_storage_api::types::UpstreamKind::AnthropicKey,
                label: "renewal accounting".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 0,
                }],
                principal_kind: cc_lb_contract::PrincipalKindLite::Machine,
            },
        )
        .await
        .expect("create accounting key");
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(record.key_hash_b64);
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn cache_keepalive_redelivered_job_dispatches_once() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let first = dispatch
        .dispatch_cache_keepalive(job.clone())
        .await
        .expect("dispatch first delivery");
    let redelivery = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch redelivery");

    assert!(matches!(first, JobOutcome::Done));
    assert!(matches!(redelivery, JobOutcome::Noop));
    assert_eq!(
        fixture.http.requests.lock().expect("requests lock").len(),
        1
    );
    assert_eq!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .len(),
        1
    );
}

#[tokio::test]
async fn cache_keepalive_running_turn_does_not_redispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET enqueue_state = 'running', running_since_unix_secs = 1 WHERE session_key_hash = ?",
    )
    .bind("session-hash")
    .execute(fixture.storage.pool())
    .await
    .expect("mark turn running");
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch running turn");

    assert!(matches!(outcome, JobOutcome::Noop));
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
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
async fn cache_keepalive_job_terminalizes_on_decrypt_failure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.replace_encrypted_payload(vec![0; 32]).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DecryptFailed)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_job_terminalizes_on_deserialization_failure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let encrypted = fixture.encrypt_generation_payload(1, br#"{}"#);
    fixture.replace_encrypted_payload(encrypted).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DecryptFailed)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_miss_terminalizes_session() {
    let fixture = Fixture::new().await;
    fixture.http.return_cache_miss();
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::CacheMiss)
    );
}

#[tokio::test]
async fn cache_keepalive_unsupported_provider_terminalizes_session() {
    let fixture = Fixture::new_with_upstream_kind(UpstreamKind::AnthropicApiKey).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::UnsupportedProvider)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_dispatch_error_terminalizes_session() {
    let fixture = Fixture::new().await;
    fixture.http.return_status(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({"error":"boom"}),
    );
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}

#[tokio::test]
async fn cache_keepalive_hit_terminalizes_when_max_refreshes_reached() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let mut job = fixture.pending_keepalive_job(1).await;
    job.max_refreshes = 1;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::MaxRefreshes)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn cache_keepalive_hit_terminalizes_when_max_duration_reached() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let mut job = fixture.pending_keepalive_job(1).await;
    job.max_total_duration_secs = 0;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::MaxDuration)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
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

#[tokio::test]
async fn cache_keepalive_new_real_request_makes_queued_old_generation_noop() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial durable keepalive");
    let queued_old_job = fixture.pending_keepalive_job(1).await;
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("replace durable keepalive from new real request");
    fixture.pending_keepalive_job(2).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(queued_old_job)
        .await
        .expect("dispatch stale keepalive job");

    assert!(matches!(outcome, JobOutcome::Noop));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.refresh_count, 0);
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_expired_session_terminalizes_before_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.expire_session().await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Expired)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_revoked_principal_terminalizes_before_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.revoke_principal();
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}
