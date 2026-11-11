use super::*;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest, CancelReason,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::{CacheKeepaliveCompletionTicket, CacheKeepaliveConfig, ClassifierConfig};

fn completion_ticket(sequence: u64) -> CacheKeepaliveCompletionTicket {
    CacheKeepaliveCompletionTicket {
        epoch: Uuid::from_u128(1),
        sequence,
        valid_until_unix_secs: u64::MAX,
    }
}

#[derive(Default)]
struct Session {
    ticket: u64,
    owner: Option<CacheKeepaliveCompletionTicket>,
    in_request: bool,
    terminal_reason: Option<&'static str>,
    upstream: Option<Uuid>,
}

#[derive(Default)]
struct State {
    sessions: HashMap<String, Session>,
    enqueues: Vec<(String, Option<CacheKeepaliveCompletionTicket>)>,
    cancellations: Vec<(String, Option<CacheKeepaliveCompletionTicket>, &'static str)>,
    preserve_newer_start: bool,
}

#[derive(Default)]
struct IdentityEnqueuer {
    state: Mutex<State>,
    completed: tokio::sync::Notify,
}

// Models the storage boundary, not lifecycle logic: independently allocated
// per-key tickets, provisional-marker owner equality, and terminal state.
// Contract: CacheKeepaliveSessionStore plus ServerCacheKeepaliveEnqueuer.
// Both identities represent pre-existing queued sessions, seeded lazily when
// their canonical key is first observed. Counters intentionally differ.
#[async_trait]
impl CacheKeepaliveEnqueuer for IdentityEnqueuer {
    async fn observe_session_generation(
        &self,
        key: &str,
    ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().unwrap();
        let initial_ticket = if state.sessions.is_empty() { 40 } else { 0 };
        let session = state
            .sessions
            .entry(key.to_owned())
            .or_insert_with(|| Session {
                ticket: initial_ticket,
                owner: Some(completion_ticket(initial_ticket)),
                ..Session::default()
            });
        session.ticket += 1;
        Ok(Some(completion_ticket(session.ticket)))
    }

    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().unwrap();
        state.enqueues.push((
            request.session_key_hash.clone(),
            request.expected_generation,
        ));
        let session = state.sessions.get_mut(&request.session_key_hash).unwrap();
        let ticket = request.expected_generation.expect("fenced completion");
        assert_eq!(ticket, completion_ticket(ticket.sequence));
        let accepted = if session.in_request {
            Some(ticket) == session.owner
        } else {
            Some(ticket.sequence) > session.owner.map(|owner| owner.sequence)
        };
        if accepted {
            session.owner = Some(ticket);
            session.in_request = false;
            session.terminal_reason = None;
            session.upstream = Some(request.snapshot.upstream_id);
        }
        self.completed.notify_one();
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().unwrap();
        state.cancellations.push((
            request.session_key_hash.clone(),
            request.expected_generation,
            request.reason.as_str(),
        ));
        let preserve_newer = state.preserve_newer_start;
        let session = state.sessions.get_mut(&request.session_key_hash).unwrap();
        let ticket = request.expected_generation.expect("fenced cancellation");
        assert_eq!(ticket, completion_ticket(ticket.sequence));
        let begins_request = request.reason == CancelReason::NewRequest;
        let accepted = if begins_request || !session.in_request {
            Some(ticket.sequence) > session.owner.map(|owner| owner.sequence)
        } else {
            Some(ticket) == session.owner
        };
        if accepted {
            session.owner = Some(ticket);
            session.in_request = request.reason == CancelReason::NewRequest;
            if session.in_request && preserve_newer && session.ticket == 1 {
                // A newer authenticated start has acquired this same K1 while
                // the older request is in flight. Its ticket must survive.
                session.ticket += 1;
                session.owner = Some(completion_ticket(session.ticket));
            } else if !session.in_request {
                session.terminal_reason = Some(request.reason.as_str());
            }
        }
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        panic!("agent response must remain eligible");
    }
}

fn keepalive_principal_view() -> Arc<PrincipalView> {
    let principal = PrincipalRecord {
        id: Uuid::from_u128(99),
        name: PRINCIPAL.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: Some(CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            min_cache_tokens: 10_000,
            classifier: ClassifierConfig::default(),
        }),
    };
    Arc::new(PrincipalView::from_db(&[principal], HashMap::new()))
}

fn request() -> Request<Bytes> {
    messages_request(Bytes::from_static(br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"x-anthropic-billing-header: cron","cache_control":{"type":"ephemeral"}},{"type":"text","text":"cached instructions","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}"#))
}

fn reply(streaming: bool) -> (Reply, Bytes) {
    let message = serde_json::json!({
        "type": "message", "id": "msg_identity", "role": "assistant",
        "content": [{"type":"tool_use","id":"toolu_identity","name":"Bash","input":{}}],
        "stop_reason":"tool_use",
        "usage":{"input_tokens":0,"output_tokens":4,"cache_read_input_tokens":12000}
    });
    let body = if streaming {
        Bytes::from(format!(
            "event: message_start\ndata: {}\n\nevent: content_block_start\ndata: {}\n\nevent: message_delta\ndata: {}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
            serde_json::json!({"type":"message_start","message":{"id":"msg_identity","type":"message","role":"assistant","content":[],"usage":message["usage"]}}),
            serde_json::json!({"type":"content_block_start","index":0,"content_block":message["content"][0]}),
            serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
        ))
    } else {
        Bytes::from(serde_json::to_vec(&message).unwrap())
    };
    (
        Reply::Body {
            status: StatusCode::OK,
            content_type: if streaming {
                "text/event-stream"
            } else {
                "application/json"
            },
            body: body.clone(),
        },
        body,
    )
}

async fn assert_identity_change(streaming: bool, preserve_newer_start: bool) {
    let enqueuer = Arc::new(IdentityEnqueuer::default());
    let (success, expected_body) = reply(streaming);
    // Warm literal/custom K2 first, giving its independent counter ticket 41.
    let warm = Fixture::with_principal_view(
        vec![api_key(2, "custom").replies(vec![success.clone()])],
        keepalive_principal_view(),
    )
    .with_lifecycle(|lifecycle| lifecycle.with_cache_keepalive_enqueuer(enqueuer.clone()));
    let (status, body) = warm.send_request(request()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, expected_body);
    tokio::time::timeout(Duration::from_secs(5), enqueuer.completed.notified())
        .await
        .unwrap();
    let k2 = enqueuer.state.lock().unwrap().enqueues[0].0.clone();
    enqueuer.state.lock().unwrap().preserve_newer_start = preserve_newer_start;

    let mut official =
        oauth(1, "official").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]);
    official.record.base_url = None;
    let fixture = Fixture::with_principal_view(
        vec![official, api_key(2, "custom").replies(vec![success])],
        keepalive_principal_view(),
    )
    .with_lifecycle(|lifecycle| lifecycle.with_cache_keepalive_enqueuer(enqueuer.clone()));
    let replies = fixture
        .dispatch
        .replies
        .lock()
        .unwrap()
        .remove(&host("official"))
        .unwrap();
    fixture
        .dispatch
        .replies
        .lock()
        .unwrap()
        .insert("api.anthropic.com".to_owned(), replies);
    let (status, body) = fixture.send_request(request()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, expected_body,
        "response fidelity, streaming={streaming}"
    );
    assert_eq!(
        fixture.hosts(),
        vec!["api.anthropic.com".to_owned(), host("custom")]
    );
    tokio::time::timeout(Duration::from_secs(5), enqueuer.completed.notified())
        .await
        .unwrap();

    let state = enqueuer.state.lock().unwrap();
    let k1 = state
        .cancellations
        .iter()
        .find(|(key, _, reason)| key != &k2 && *reason == "new_request")
        .expect("real first attempt acquires official K1")
        .0
        .clone();
    assert_ne!(
        k1, k2,
        "billing-header policy changes threadless fallback identity"
    );
    assert_eq!(
        state.enqueues.last(),
        Some(&(k2.clone(), Some(completion_ticket(42)))),
        "K1's ticket 1 is not valid for K2; completion must acquire K2 ticket 42"
    );
    assert_eq!(state.sessions[&k2].owner, Some(completion_ticket(42)));
    assert_eq!(state.sessions[&k2].upstream, Some(Uuid::from_u128(2)));
    assert!(!state.sessions[&k2].in_request);
    assert!(
        state
            .cancellations
            .iter()
            .any(|(key, ticket, reason)| key == &k1
                && *ticket == Some(completion_ticket(1))
                && *reason == "session_identity_changed")
    );
    let old = &state.sessions[&k1];
    if preserve_newer_start {
        assert_eq!(
            old.owner,
            Some(completion_ticket(2)),
            "retiring K1 must not acquire its newer owner's ticket"
        );
        assert!(old.in_request);
        assert_eq!(old.terminal_reason, None);
    } else {
        assert!(
            !old.in_request,
            "old identity must not linger until timeout"
        );
        assert_eq!(old.terminal_reason, Some("session_identity_changed"));
    }
}

#[tokio::test]
async fn official_custom_failover_uses_fresh_identity_ticket_json() {
    assert_identity_change(false, false).await;
}

#[tokio::test]
async fn official_custom_failover_uses_fresh_identity_ticket_sse() {
    assert_identity_change(true, false).await;
}

#[tokio::test]
async fn official_custom_failover_preserves_newer_original_identity_owner() {
    assert_identity_change(false, true).await;
    assert_identity_change(true, true).await;
}
