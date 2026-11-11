use super::*;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::dynamic_view::{DynamicView, DynamicViewBuilder};
use cc_lb_domain::Upstream;
use cc_lb_storage_api::principal::{
    PrincipalKind as StoragePrincipalKind, PrincipalRecord as StoragePrincipalRecord,
};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};
use cc_lb_upstream::{ApiKeyAwareSignerFactory, Signer, SignerError, SignerFactory};
use std::sync::Mutex;
use tokio::sync::{Notify, mpsc};

#[derive(Clone, Copy, Debug)]
enum CompletionGap {
    ConfigRemoved,
    ConfigDisabled,
    ResponsesDisabled,
    MissingRequestJson,
    InvalidShapedJson,
    SnapshotNotObject,
    SnapshotInvalidJson,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Marker {
    owner: CacheKeepaliveCompletionTicket,
    final_reason: Option<String>,
}

#[derive(Debug)]
enum Event {
    Observe(String),
    Enqueue(String),
    NotTracked(String),
    Cancel {
        key: String,
        owner: Option<CacheKeepaliveCompletionTicket>,
        reason: String,
        accepted: bool,
    },
}

struct CompletionState {
    markers: Mutex<HashMap<String, Marker>>,
    settled: Notify,
}

struct CompletionEnqueuer {
    state: Arc<CompletionState>,
    events: mpsc::UnboundedSender<Event>,
}

impl Drop for CompletionEnqueuer {
    fn drop(&mut self) {
        // The test drops its LifecycleKeepalive after dispatch. The enqueuer's
        // last owner is then the detached completion task, if one was spawned.
        // This also settles synchronously-returning paths without a timeout.
        self.state.settled.notify_one();
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for CompletionEnqueuer {
    async fn observe_session_generation(
        &self,
        key: &str,
    ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
        self.events.send(Event::Observe(key.to_owned())).unwrap();
        Ok(self
            .state
            .markers
            .lock()
            .unwrap()
            .get(key)
            .map(|row| row.owner))
    }

    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.events
            .send(Event::Enqueue(request.session_key_hash))
            .unwrap();
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        // CacheKeepaliveSessionStore::cancel_cache_keepalive_fenced requires
        // the marker's exact original start ticket, not a freshly issued one.
        // Rejected writes leave a newer provisional marker unchanged.
        let mut markers = self.state.markers.lock().unwrap();
        let accepted = markers
            .get_mut(&request.session_key_hash)
            .is_some_and(|row| {
                if request.expected_generation != Some(row.owner) || row.final_reason.is_some() {
                    return false;
                }
                row.final_reason = Some(request.reason.as_str().to_owned());
                true
            });
        self.events
            .send(Event::Cancel {
                key: request.session_key_hash,
                owner: request.expected_generation,
                reason: request.reason.as_str().to_owned(),
                accepted,
            })
            .unwrap();
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.events
            .send(Event::NotTracked(request.session_key_hash))
            .unwrap();
        Ok(())
    }
}

struct UnusedSignerFactory;

impl ApiKeyAwareSignerFactory for UnusedSignerFactory {
    fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl SignerFactory for UnusedSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        panic!("completion persistence must not build a signer")
    }
}

fn ticket(sequence: u64) -> CacheKeepaliveCompletionTicket {
    CacheKeepaliveCompletionTicket {
        epoch: Uuid::from_u128(71),
        sequence,
        valid_until_unix_secs: u64::MAX,
    }
}

fn principal_record() -> StoragePrincipalRecord {
    StoragePrincipalRecord {
        id: Uuid::from_u128(72),
        name: "completed-not-renewable".to_owned(),
        kind: StoragePrincipalKind::Machine,
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
            classifier: ClassifierConfig {
                openai_responses_enabled: true,
                ..ClassifierConfig::default()
            },
        }),
    }
}

fn view(record: &StoragePrincipalRecord, generation: u64) -> Arc<DynamicView> {
    DynamicViewBuilder::new(generation)
        .signer_factory(Arc::new(UnusedSignerFactory))
        .principal_view(Arc::new(PrincipalView::from_db(
            std::slice::from_ref(record),
            HashMap::new(),
        )))
        .build()
}

async fn assert_completion_gap(gap: CompletionGap, newer_primary: bool, newer_sibling: bool) {
    let mut record = principal_record();
    let dynamic_view = Arc::new(DynamicViewHolder::new(view(&record, 0)));
    let principal = Principal {
        id: record.id.to_string(),
        kind: cc_lb_domain::PrincipalKind::InternalKey,
    };
    let principal_id = record.id;
    let primary = SessionKey::from_thread_id(principal_id, "completion-gap");
    let sibling = SessionKey::from_thread_and_agent(principal_id, "completion-gap", "subagent");
    let captured_primary = ticket(3);
    let captured_sibling = ticket(7);
    let primary_owner = if newer_primary {
        ticket(4)
    } else {
        captured_primary
    };
    let sibling_owner = if newer_sibling {
        ticket(8)
    } else {
        captured_sibling
    };
    let state = Arc::new(CompletionState {
        markers: Mutex::new(HashMap::from([
            (
                primary.to_string(),
                Marker {
                    owner: primary_owner,
                    final_reason: None,
                },
            ),
            (
                sibling.to_string(),
                Marker {
                    owner: sibling_owner,
                    final_reason: None,
                },
            ),
        ])),
        settled: Notify::new(),
    });
    let (events, mut received) = mpsc::unbounded_channel();
    let keepalive = LifecycleKeepalive::new(
        Some(Arc::new(CompletionEnqueuer {
            state: Arc::clone(&state),
            events,
        })),
        Arc::clone(&dynamic_view),
    );
    // Model the already-captured dispatch boundary. No new observation is
    // permitted at completion, even when either durable owner has advanced.
    keepalive.record_issued_seq(&primary, 1);
    keepalive.next_completion_seq.store(1, Ordering::Relaxed);
    let order = RequestStartOrder {
        session_key: primary.clone(),
        sequence: 1,
        expected_generation: Some(captured_primary),
        sibling_generation: Some(captured_sibling),
    };
    let request_json = serde_json::json!({
        "model": "claude-test", "max_tokens": 32,
        "system": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": "hello"}]
    });
    let mut metadata = RequestCacheMetadata::default();
    metadata.thread_id = Some("completion-gap".to_owned());
    metadata.request_json = Some(request_json.clone());
    metadata.cache_breakpoints = vec![cc_lb_request_log::RequestCacheBreakpoint {
        block_index: 0,
        source: cc_lb_request_log::RequestCacheBreakpointSource::System,
        path: "system[0]".to_owned(),
        message_index: None,
        ttl: Some("5m".to_owned()),
        prefix_hash: "completion-gap-prefix".to_owned(),
        prefix_token_count: 12_000,
        lookback_prefixes: Vec::new(),
        token_estimate_source: None,
    }];
    assert_eq!(
        cache_keepalive_session_key(&principal, &metadata).as_ref(),
        Some(&primary),
        "fixture must capture the real dispatch identity",
    );
    let mut original_body = Bytes::from(serde_json::to_vec(&request_json).unwrap());
    let mut shaped_body = original_body.clone();
    match gap {
        CompletionGap::ConfigRemoved => record.cache_keepalive = None,
        CompletionGap::ConfigDisabled => record.cache_keepalive.as_mut().unwrap().enabled = false,
        CompletionGap::ResponsesDisabled => {
            metadata.cache_keepalive_surface = Some("responses".to_owned());
            record
                .cache_keepalive
                .as_mut()
                .unwrap()
                .classifier
                .openai_responses_enabled = false;
        }
        CompletionGap::MissingRequestJson => metadata.request_json = None,
        CompletionGap::InvalidShapedJson => shaped_body = Bytes::from_static(b"{"),
        CompletionGap::SnapshotNotObject => {
            shaped_body = Bytes::from_static(b"[]");
            original_body = shaped_body.clone();
        }
        CompletionGap::SnapshotInvalidJson => {
            // Equal byte bodies intentionally bypass eligibility re-parsing;
            // capture itself must report the non-size InvalidJson failure.
            shaped_body = Bytes::from_static(b"{");
            original_body = shaped_body.clone();
        }
    }
    dynamic_view.store(view(&record, dynamic_view.generation()));
    keepalive.on_response_completed(
        &serde_json::json!({
            "type": "message", "stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": "tool-gap", "name": "Bash", "input": {}}]
        }),
        &principal,
        "accounting-key".to_owned(),
        &metadata,
        Uuid::nil(),
        shaped_body,
        original_body,
        &HeaderMap::new(),
        Duration::from_secs(2),
        12_000,
        Some(order),
    );
    drop(keepalive);
    state.settled.notified().await;

    let mut primary_finalizations = 0;
    let mut sibling_finalizations = 0;
    while let Ok(event) = received.try_recv() {
        match event {
            Event::Cancel {
                key,
                owner,
                reason,
                accepted,
            } => {
                assert_eq!(reason, "completed_not_renewable", "{gap:?}: {key}");
                if key == primary.to_string() {
                    assert_eq!(owner, Some(captured_primary));
                    assert_eq!(accepted, !newer_primary);
                    primary_finalizations += 1;
                } else {
                    assert_eq!(key, sibling.to_string());
                    assert_eq!(owner, Some(captured_sibling));
                    assert_eq!(accepted, !newer_sibling);
                    sibling_finalizations += 1;
                }
            }
            Event::Observe(key) => panic!("{gap:?}: completion observed a fresh owner for {key}"),
            Event::Enqueue(key) => panic!("{gap:?}: non-renewable completion enqueued {key}"),
            Event::NotTracked(key) => panic!("{gap:?}: unexpected not-tracked write for {key}"),
        }
    }
    assert_eq!(
        primary_finalizations, 1,
        "{gap:?}: finalize captured primary exactly once"
    );
    assert_eq!(
        sibling_finalizations, 1,
        "{gap:?}: finalize captured sibling exactly once"
    );
    let markers = state.markers.lock().unwrap();
    for (key, owner, newer) in [
        (&primary, primary_owner, newer_primary),
        (&sibling, sibling_owner, newer_sibling),
    ] {
        let row = markers.get(&key.to_string()).unwrap();
        assert_eq!(
            row.owner, owner,
            "{gap:?}: completion changed captured ownership"
        );
        assert_eq!(
            row.final_reason.as_deref(),
            if newer {
                None
            } else {
                Some("completed_not_renewable")
            },
            "{gap:?}: preserve newer marker, finalize only captured owner for {key}"
        );
    }
}

macro_rules! completion_gap_tests {
    ($captured:ident, $newer:ident, $gap:ident) => {
        #[tokio::test]
        async fn $captured() {
            assert_completion_gap(CompletionGap::$gap, false, false).await;
        }

        #[tokio::test]
        async fn $newer() {
            for (primary, sibling) in [(true, false), (false, true), (true, true)] {
                assert_completion_gap(CompletionGap::$gap, primary, sibling).await;
            }
        }
    };
}

completion_gap_tests!(
    config_removed_finalizes_captured_owners,
    config_removed_preserves_newer_owners,
    ConfigRemoved
);
completion_gap_tests!(
    config_disabled_finalizes_captured_owners,
    config_disabled_preserves_newer_owners,
    ConfigDisabled
);
completion_gap_tests!(
    responses_disabled_finalizes_captured_owners,
    responses_disabled_preserves_newer_owners,
    ResponsesDisabled
);
completion_gap_tests!(
    missing_request_json_finalizes_captured_owners,
    missing_request_json_preserves_newer_owners,
    MissingRequestJson
);
completion_gap_tests!(
    invalid_shaped_json_finalizes_captured_owners,
    invalid_shaped_json_preserves_newer_owners,
    InvalidShapedJson
);
completion_gap_tests!(
    snapshot_not_object_finalizes_captured_owners,
    snapshot_not_object_preserves_newer_owners,
    SnapshotNotObject
);
completion_gap_tests!(
    snapshot_invalid_json_finalizes_captured_owners,
    snapshot_invalid_json_preserves_newer_owners,
    SnapshotInvalidJson
);
