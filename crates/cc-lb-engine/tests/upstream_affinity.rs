#[path = "upstream_affinity/codex_compaction.rs"]
mod codex_compaction;

use crate::common::{TestAuthn, TestLifecycleBus, TestState, collect_body, messages_request};
use crate::lifecycle_client_disconnect::client_disconnect_support::{
    assert_error_terminal, assert_one_final, assert_responses_failed, assert_stream_failure_once,
    lifecycle_receiver, sqlite_storage,
};

use std::collections::{HashMap, VecDeque};
use std::io;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_clock::TestClock;
use cc_lb_control::{
    ApplyStatus, DynamicViewBuilder, DynamicViewHolder, RequestEventBus, UpstreamStatusEntry,
    UpstreamStatusSnapshot,
};
use cc_lb_engine::{
    DispatchError, Lifecycle, LifecycleConfig, ProviderScope, ProxyError, UpstreamDispatch,
};
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamAffinityBinding, UpstreamAffinityKey,
    UpstreamAffinityKind, UpstreamAffinityStore, UpstreamCreate, UpstreamStore,
    upstream::UpstreamKind,
};
use cc_lb_upstream::SignedRequest;
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

const TEST_NOW_UNIX_SECS: u64 = 4_100_000_000;

#[derive(Clone)]
enum ResponseSpec {
    Json(Value),
    JsonThenError(Bytes),
    Sse(Bytes),
    SseChunks(Vec<Bytes>),
    /// Yields `first`, waits for `release`, then yields `second`.
    SseGated {
        first: Bytes,
        release: Arc<tokio::sync::Notify>,
        second: Bytes,
    },
    SseWithEncoding {
        body: Bytes,
        content_encoding: &'static str,
    },
    Status(StatusCode, Value),
    GzipStatus(StatusCode, Value),
}

#[derive(Default)]
struct RecordingDispatch {
    responses: Mutex<VecDeque<ResponseSpec>>,
    urls: Mutex<Vec<String>>,
    captured: Mutex<Vec<(String, Value)>>,
}

impl RecordingDispatch {
    fn with_response(response: ResponseSpec) -> Arc<Self> {
        Self::with_responses(vec![response])
    }

    fn with_responses(responses: Vec<ResponseSpec>) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(VecDeque::from(responses)),
            urls: Mutex::new(Vec::new()),
            captured: Mutex::new(Vec::new()),
        })
    }

    fn call_count(&self) -> usize {
        self.urls.lock().expect("dispatch URL lock").len()
    }

    fn captured_requests(&self) -> Vec<(String, Value)> {
        self.captured
            .lock()
            .expect("captured requests lock")
            .clone()
    }
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let url = request.url().to_string();
        let parsed: Value =
            serde_json::from_slice(request.body()).unwrap_or_else(|_| json!({"raw": true}));
        self.urls
            .lock()
            .expect("dispatch URL lock")
            .push(url.clone());
        self.captured
            .lock()
            .expect("captured requests lock")
            .push((url, parsed));
        let response = self
            .responses
            .lock()
            .expect("dispatch response lock")
            .pop_front()
            .unwrap_or_else(|| ResponseSpec::Json(json!({"type":"message"})));
        let mut status = StatusCode::OK;
        let (content_type, content_encoding, body) = match response {
            ResponseSpec::Json(value) => (
                "application/json",
                None,
                Body::from(Bytes::from(value.to_string())),
            ),
            ResponseSpec::JsonThenError(partial) => {
                let stream = async_stream::stream! {
                    yield Ok::<Bytes, io::Error>(partial);
                    yield Err::<Bytes, io::Error>(io::Error::other("forced upstream body failure"));
                };
                ("application/json", None, Body::from_stream(stream))
            }
            ResponseSpec::Sse(body) => ("text/event-stream", None, Body::from(body)),
            ResponseSpec::SseChunks(chunks) => {
                let stream = async_stream::stream! {
                    for chunk in chunks {
                        yield Ok::<Bytes, io::Error>(chunk);
                    }
                };
                ("text/event-stream", None, Body::from_stream(stream))
            }
            ResponseSpec::SseGated {
                first,
                release,
                second,
            } => {
                let stream = async_stream::stream! {
                    yield Ok::<Bytes, io::Error>(first);
                    release.notified().await;
                    yield Ok::<Bytes, io::Error>(second);
                };
                ("text/event-stream", None, Body::from_stream(stream))
            }
            ResponseSpec::SseWithEncoding {
                body,
                content_encoding,
            } => (
                "text/event-stream",
                Some(content_encoding),
                Body::from(body),
            ),
            ResponseSpec::Status(response_status, value) => {
                status = response_status;
                (
                    "application/json",
                    None,
                    Body::from(Bytes::from(value.to_string())),
                )
            }
            ResponseSpec::GzipStatus(response_status, value) => {
                status = response_status;
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(value.to_string().as_bytes())
                    .expect("gzip status body writes");
                let compressed = encoder.finish().expect("gzip status body finishes");
                ("application/json", Some("gzip"), Body::from(compressed))
            }
        };
        let mut response = Response::new(body);
        *response.status_mut() = status;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        if let Some(content_encoding) = content_encoding {
            response.headers_mut().insert(
                http::header::CONTENT_ENCODING,
                HeaderValue::from_static(content_encoding),
            );
        }
        Ok(response)
    }
}

#[derive(Default)]
pub(super) struct MemoryAffinityStore {
    bindings: Mutex<HashMap<UpstreamAffinityKey, UpstreamAffinityBinding>>,
    owner_epochs: Mutex<HashMap<Uuid, u64>>,
    resolve_failure: Mutex<Option<String>>,
    bind_failure: Mutex<Option<String>>,
}

impl MemoryAffinityStore {
    pub(crate) fn fail_bind(&self, message: &str) {
        *self.bind_failure.lock().expect("bind failure lock") = Some(message.to_owned());
    }
    fn insert(&self, key: UpstreamAffinityKey, upstream_id: Uuid) {
        self.insert_binding(UpstreamAffinityBinding {
            key,
            upstream_id,
            owner_epoch: 0,
            observed_at_unix_secs: TEST_NOW_UNIX_SECS,
            expires_at_unix_secs: None,
        });
    }

    fn insert_binding(&self, binding: UpstreamAffinityBinding) {
        self.bindings
            .lock()
            .expect("affinity binding lock")
            .insert(binding.key.clone(), binding);
    }

    fn upstream_for(&self, key: &UpstreamAffinityKey) -> Option<Uuid> {
        self.bindings
            .lock()
            .expect("affinity binding lock")
            .get(key)
            .map(|binding| binding.upstream_id)
    }
    fn binding_epoch_for(&self, key: &UpstreamAffinityKey) -> Option<u64> {
        self.bindings
            .lock()
            .expect("affinity binding lock")
            .get(key)
            .map(|binding| binding.owner_epoch)
    }

    fn len(&self) -> usize {
        self.bindings.lock().expect("affinity binding lock").len()
    }
    fn set_owner_epoch(&self, upstream_id: Uuid, owner_epoch: u64) {
        self.owner_epochs
            .lock()
            .expect("owner epoch lock")
            .insert(upstream_id, owner_epoch);
    }

    fn owner_epoch(&self, upstream_id: Uuid) -> u64 {
        self.owner_epochs
            .lock()
            .expect("owner epoch lock")
            .get(&upstream_id)
            .copied()
            .unwrap_or_default()
    }
}

fn binding_is_active(binding: &UpstreamAffinityBinding, now_unix_secs: u64, ttl_secs: u64) -> bool {
    let retained = now_unix_secs
        .checked_sub(ttl_secs)
        .is_none_or(|cutoff| binding.observed_at_unix_secs > cutoff);
    retained
        && binding
            .expires_at_unix_secs
            .is_none_or(|expires_at| expires_at > now_unix_secs)
}

#[async_trait]
impl UpstreamAffinityStore for MemoryAffinityStore {
    async fn resolve_upstream_affinities(
        &self,
        keys: &[UpstreamAffinityKey],
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<Vec<UpstreamAffinityBinding>> {
        if let Some(message) = self
            .resolve_failure
            .lock()
            .expect("resolve failure lock")
            .clone()
        {
            return Err(StorageError::Unavailable { message });
        }
        let bindings = self.bindings.lock().expect("affinity binding lock");
        Ok(keys
            .iter()
            .filter_map(|key| bindings.get(key))
            .filter(|binding| binding_is_active(binding, now_unix_secs, ttl_secs))
            .filter(|binding| binding.owner_epoch == self.owner_epoch(binding.upstream_id))
            .cloned()
            .collect())
    }
    async fn read_upstream_owner_epoch(
        &self,
        upstream_id: Uuid,
        _expected_spec_revision: u64,
    ) -> StorageResult<Option<u64>> {
        Ok(Some(self.owner_epoch(upstream_id)))
    }

    async fn bind_upstream_affinities(
        &self,
        incoming: &[UpstreamAffinityBinding],
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<()> {
        if let Some(message) = self.bind_failure.lock().expect("bind failure lock").clone() {
            return Err(StorageError::Unavailable { message });
        }
        for binding in incoming {
            if binding.owner_epoch != self.owner_epoch(binding.upstream_id) {
                return Err(StorageError::Conflict {
                    message: "upstream owner epoch differs".to_owned(),
                });
            }
        }

        let mut prepared = HashMap::<UpstreamAffinityKey, UpstreamAffinityBinding>::new();
        for binding in incoming {
            match prepared.entry(binding.key.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(binding.clone());
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    if existing.upstream_id != binding.upstream_id {
                        return Err(StorageError::Conflict {
                            message: "affinity target differs".to_owned(),
                        });
                    }
                    existing.observed_at_unix_secs = existing
                        .observed_at_unix_secs
                        .max(binding.observed_at_unix_secs);
                    existing.expires_at_unix_secs =
                        match (existing.expires_at_unix_secs, binding.expires_at_unix_secs) {
                            (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                            _ => None,
                        };
                }
            }
        }

        let mut bindings = self.bindings.lock().expect("affinity binding lock");
        for binding in prepared.values() {
            if let Some(existing) = bindings.get(&binding.key)
                && binding_is_active(existing, now_unix_secs, ttl_secs)
                && existing.upstream_id != binding.upstream_id
            {
                return Err(StorageError::Conflict {
                    message: "affinity target differs".to_owned(),
                });
            }
        }
        bindings.extend(prepared);
        Ok(())
    }

    async fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let mut bindings = self.bindings.lock().expect("affinity binding lock");
        let expired = bindings
            .iter()
            .filter(|(_, binding)| !binding_is_active(binding, now_unix_secs, ttl_secs))
            .map(|(key, _)| key.clone())
            .take(batch_size)
            .collect::<Vec<_>>();
        let purged = expired.len() as u64;
        for key in expired {
            bindings.remove(&key);
        }
        Ok(purged)
    }
}

fn affinity_key(ciphertext: &str) -> UpstreamAffinityKey {
    UpstreamAffinityKey {
        principal_id: "principal-test".to_owned(),
        provider: "anthropic".to_owned(),
        kind: UpstreamAffinityKind::AnthropicWebSearchEncryptedContent,
        value_sha256: Sha256::digest(ciphertext.as_bytes()).into(),
    }
}

fn upstream_record(id: Uuid, name: &str, enabled: bool) -> cc_lb_storage_api::UpstreamRecord {
    cc_lb_storage_api::UpstreamRecord {
        owner_epoch: 0,
        vertex: None,
        vertex_credential_type: None,
        vertex_quota_project_id: None,
        id,
        name: name.to_owned(),
        kind: cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse(&format!("http://{name}.local/")).expect("test URL parses")),
        enabled,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
        workspace_id: None,
    }
}
fn openai_upstream_record(
    id: Uuid,
    name: &str,
    enabled: bool,
) -> cc_lb_storage_api::UpstreamRecord {
    let mut record = upstream_record(id, name, enabled);
    record.kind = cc_lb_storage_api::upstream::UpstreamKind::OpenaiApiKey;
    record
}

fn gemini_upstream_record(
    id: Uuid,
    name: &str,
    enabled: bool,
) -> cc_lb_storage_api::UpstreamRecord {
    let mut record = upstream_record(id, name, enabled);
    record.kind = cc_lb_storage_api::upstream::UpstreamKind::GeminiApiKey;
    record
}

fn gemini_cached_contents_request(body: Value) -> Request<Bytes> {
    let mut request = Request::builder()
        .method(http::Method::POST)
        .uri("/v1beta/cachedContents")
        .header("x-api-key", crate::common::managed_api_key())
        .body(Bytes::from(body.to_string()))
        .expect("Gemini cached contents request builds");
    request
        .extensions_mut()
        .insert(ProviderScope(cc_lb_domain::Provider::Gemini));
    request
}

fn responses_request(body: Value) -> Request<Bytes> {
    Request::builder()
        .method(http::Method::POST)
        .uri("/v1/responses")
        .header("x-api-key", crate::common::managed_api_key())
        .body(Bytes::from(body.to_string()))
        .expect("Responses request builds")
}

fn lifecycle(
    records: Vec<cc_lb_storage_api::UpstreamRecord>,
    dispatcher: Arc<RecordingDispatch>,
    store: Option<Arc<MemoryAffinityStore>>,
) -> Lifecycle {
    lifecycle_with_config(records, dispatcher, store, LifecycleConfig::default())
}

fn lifecycle_with_config(
    records: Vec<cc_lb_storage_api::UpstreamRecord>,
    dispatcher: Arc<RecordingDispatch>,
    store: Option<Arc<MemoryAffinityStore>>,
    config: LifecycleConfig,
) -> Lifecycle {
    lifecycle_with_config_and_clock(
        records,
        dispatcher,
        store,
        config,
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
}

fn lifecycle_with_config_and_clock(
    records: Vec<cc_lb_storage_api::UpstreamRecord>,
    dispatcher: Arc<RecordingDispatch>,
    store: Option<Arc<MemoryAffinityStore>>,
    config: LifecycleConfig,
    clock: Arc<TestClock>,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(records)
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        config,
        clock,
    );
    match store {
        Some(store) => lifecycle.with_upstream_affinity_store(store),
        None => lifecycle,
    }
}

/// Like `lifecycle`, but the view carries an explicit `UpstreamStatusSnapshot`
/// so tests can pin the apply status the real `apply_upstreams` path would
/// have written.
fn lifecycle_with_status(
    records: Vec<cc_lb_storage_api::UpstreamRecord>,
    dispatcher: Arc<RecordingDispatch>,
    store: Option<Arc<MemoryAffinityStore>>,
    upstream_status_snapshot: UpstreamStatusSnapshot,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(records)
        .upstream_status_snapshot(Arc::new(upstream_status_snapshot))
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    );
    match store {
        Some(store) => lifecycle.with_upstream_affinity_store(store),
        None => lifecycle,
    }
}

async fn handle_authenticated(
    lifecycle: &Lifecycle,
    request: Request<Bytes>,
) -> Result<Response<Body>, ProxyError> {
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    lifecycle.handle(request, &auth).await
}

fn opaque_request(ciphertexts: &[&str], stream: bool) -> Bytes {
    Bytes::from(
        json!({
            "model": "claude-test",
            "stream": stream,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "web_search_tool_result",
                    "content": ciphertexts
                        .iter()
                        .map(|value| json!({"encrypted_content": value}))
                        .collect::<Vec<_>>()
                }]
            }]
        })
        .to_string(),
    )
}

fn web_search_response(ciphertext: &str) -> Value {
    json!({
        "type": "message",
        "content": [{
            "type": "web_search_tool_result",
            "content": [{"encrypted_content": ciphertext}]
        }],
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })
}

#[test]
fn lifecycle_default_affinity_ttl_matches_config_default() {
    assert_eq!(
        LifecycleConfig::default().upstream_affinity_ttl,
        Duration::from_secs(cc_lb_config::UpstreamAffinityConfig::default().ttl_secs())
    );
}

#[tokio::test]
async fn legacy_null_binding_expires_at_retention_boundary_without_read_extension() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("retention-boundary"),
        upstream_id: origin,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS - 9,
        expires_at_unix_secs: None,
    });
    let clock = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "type": "message",
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })));
    let lifecycle = lifecycle_with_config_and_clock(
        vec![
            upstream_record(first, "first", true),
            upstream_record(origin, "origin", true),
        ],
        dispatch.clone(),
        Some(store),
        LifecycleConfig {
            upstream_affinity_ttl: Duration::from_secs(10),
            ..LifecycleConfig::default()
        },
        clock.clone(),
    );

    let unexpired = handle_authenticated(
        &lifecycle,
        messages_request(opaque_request(&["retention-boundary"], false)),
    )
    .await
    .expect("unexpired request handled");
    assert_eq!(unexpired.status(), StatusCode::OK);
    assert_eq!(
        dispatch.urls.lock().expect("dispatch URL lock").as_slice(),
        &["http://origin.local/v1/messages".to_owned()]
    );

    clock.advance_secs(1);
    let expired = handle_authenticated(
        &lifecycle,
        messages_request(opaque_request(&["retention-boundary"], false)),
    )
    .await
    .expect("expired request handled");
    assert_eq!(expired.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn memory_affinity_purge_removes_explicit_and_retention_expiry_in_bounded_batches() {
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = MemoryAffinityStore::default();
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("active"),
        upstream_id: origin,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        expires_at_unix_secs: None,
    });
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("retention-expired"),
        upstream_id: origin,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS - 10,
        expires_at_unix_secs: None,
    });
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("explicit-expired"),
        upstream_id: origin,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        owner_epoch: 0,
        expires_at_unix_secs: Some(TEST_NOW_UNIX_SECS),
    });

    assert_eq!(
        store
            .purge_expired_upstream_affinities(TEST_NOW_UNIX_SECS, 10, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.len(), 2);
    assert_eq!(
        store
            .purge_expired_upstream_affinities(TEST_NOW_UNIX_SECS, 10, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.len(), 1);
    assert_eq!(
        store
            .resolve_upstream_affinities(&[affinity_key("active")], TEST_NOW_UNIX_SECS, 10,)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .purge_expired_upstream_affinities(TEST_NOW_UNIX_SECS, 10, 1)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn memory_affinity_bind_replaces_expired_target_without_waiting_for_purge() {
    let old = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let new = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let key = affinity_key("expired-rebind");
    let store = MemoryAffinityStore::default();
    store.insert_binding(UpstreamAffinityBinding {
        key: key.clone(),
        upstream_id: old,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS - 10,
        expires_at_unix_secs: None,
    });

    store
        .bind_upstream_affinities(
            &[UpstreamAffinityBinding {
                key: key.clone(),
                upstream_id: new,
                owner_epoch: 0,
                observed_at_unix_secs: TEST_NOW_UNIX_SECS,
                expires_at_unix_secs: None,
            }],
            TEST_NOW_UNIX_SECS,
            10,
        )
        .await
        .unwrap();

    assert_eq!(store.upstream_for(&key), Some(new));
}

#[tokio::test]
async fn response_learning_pins_a_later_request_and_learns_pending_keys() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let learning_dispatch =
        RecordingDispatch::with_response(ResponseSpec::Json(web_search_response("opaque-known")));
    let learning = lifecycle(
        vec![upstream_record(origin, "origin", true)],
        learning_dispatch,
        Some(store.clone()),
    );

    let learned_response = handle_authenticated(
        &learning,
        messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )),
    )
    .await
    .expect("learning request handled");
    assert_eq!(learned_response.status(), StatusCode::OK);
    assert_eq!(
        store.upstream_for(&affinity_key("opaque-known")),
        Some(origin)
    );
    assert_eq!(
        store.binding_epoch_for(&affinity_key("opaque-known")),
        Some(0)
    );

    let pinned_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "type": "message",
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })));
    let pinned = lifecycle(
        vec![
            upstream_record(first, "first", true),
            upstream_record(origin, "origin", true),
        ],
        pinned_dispatch.clone(),
        Some(store.clone()),
    );
    let response = handle_authenticated(
        &pinned,
        messages_request(opaque_request(&["opaque-known", "opaque-pending"], false)),
    )
    .await
    .expect("pinned request handled");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        pinned_dispatch
            .urls
            .lock()
            .expect("dispatch URL lock")
            .as_slice(),
        &["http://origin.local/v1/messages".to_owned()]
    );
    assert_eq!(
        store.upstream_for(&affinity_key("opaque-pending")),
        Some(origin)
    );
}
#[tokio::test]
async fn stateless_openai_reasoning_replay_stays_with_owner_and_fails_closed_when_disabled() {
    let origin = Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap();
    let fallback = Uuid::parse_str("00000000-0000-0000-0000-00000000000b").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let learning_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp-origin",
        "object": "response",
        "status": "completed",
        "output": [{
            "type": "reasoning",
            "id": "rs-origin",
            "summary": [],
            "encrypted_content": "opaque-origin-reasoning"
        }]
    })));
    let learning = lifecycle(
        vec![openai_upstream_record(origin, "origin", true)],
        learning_dispatch,
        Some(store.clone()),
    );
    let first = handle_authenticated(
        &learning,
        responses_request(json!({
            "model": "gpt-5",
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "input": [{"role": "user", "content": "first"}]
        })),
    )
    .await
    .expect("OpenAI learning request handled");
    assert_eq!(first.status(), StatusCode::OK);

    let reasoning_replay = json!({
        "model": "gpt-5",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": [{
            "type": "reasoning",
            "id": "rs-origin",
            "summary": [],
            "encrypted_content": "opaque-origin-reasoning"
        }, {
            "role": "user",
            "content": "second"
        }]
    });
    let owner_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp-owner-follow-up",
        "object": "response",
        "output": []
    })));
    let owner = lifecycle(
        vec![
            openai_upstream_record(origin, "origin", true),
            openai_upstream_record(fallback, "fallback", true),
        ],
        owner_dispatch.clone(),
        Some(store.clone()),
    );
    let owner_response = handle_authenticated(&owner, responses_request(reasoning_replay.clone()))
        .await
        .expect("owner replay handled");
    assert_eq!(owner_response.status(), StatusCode::OK);
    assert_eq!(
        owner_dispatch
            .urls
            .lock()
            .expect("dispatch URL lock")
            .as_slice(),
        &["http://origin.local/v1/responses".to_owned()]
    );

    let disabled_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let disabled_owner = lifecycle(
        vec![
            openai_upstream_record(origin, "origin", false),
            openai_upstream_record(fallback, "fallback", true),
        ],
        disabled_dispatch.clone(),
        Some(store),
    );
    let disabled_response =
        handle_authenticated(&disabled_owner, responses_request(reasoning_replay))
            .await
            .expect("disabled owner replay handled");
    assert_eq!(disabled_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(disabled_dispatch.call_count(), 0);
}

#[tokio::test]
async fn no_failover_when_request_affinity_pinned() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("opaque-pinned"),
        upstream_id: origin,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        expires_at_unix_secs: None,
    });
    let rate_limited = json!({
        "type": "error",
        "error": {"type": "rate_limit_error", "message": "quota exhausted"}
    });
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Status(
        StatusCode::TOO_MANY_REQUESTS,
        rate_limited,
    ));
    let pinned = lifecycle(
        vec![
            upstream_record(first, "first", true),
            upstream_record(origin, "origin", true),
        ],
        dispatch.clone(),
        Some(store),
    );

    let response = handle_authenticated(
        &pinned,
        messages_request(opaque_request(&["opaque-pinned"], false)),
    )
    .await
    .expect("pinned request handled");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        dispatch.urls.lock().expect("dispatch URL lock").as_slice(),
        &["http://origin.local/v1/messages".to_owned()]
    );
}

#[tokio::test]
async fn stale_owner_epoch_binding_is_treated_as_unbound() {
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("stale-owner-epoch"),
        upstream_id: owner,
        owner_epoch: 1,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        expires_at_unix_secs: None,
    });
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let lifecycle = lifecycle(
        vec![upstream_record(owner, "owner", true)],
        dispatch.clone(),
        Some(store),
    );

    let response = handle_authenticated(
        &lifecycle,
        messages_request(opaque_request(&["stale-owner-epoch"], false)),
    )
    .await
    .expect("stale affinity request handled");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let (_, _, body) = collect_body(response).await;
    let body: Value = serde_json::from_slice(&body).expect("error JSON");
    assert!(
        body.to_string().contains("api_error") || body.to_string().contains("affinity"),
        "stale affinity response: {body}"
    );
    assert_eq!(dispatch.call_count(), 0);
}

#[tokio::test]
async fn maximum_key_request_resolves_from_one_known_key_and_learns_the_rest() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(affinity_key("opaque-0"), origin);
    let ciphertexts = (0..1024)
        .map(|index| format!("opaque-{index}"))
        .collect::<Vec<_>>();
    let ciphertext_refs = ciphertexts.iter().map(String::as_str).collect::<Vec<_>>();
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "type": "message",
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })));
    let lifecycle = lifecycle(
        vec![
            upstream_record(first, "first", true),
            upstream_record(origin, "origin", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    );

    let response = handle_authenticated(
        &lifecycle,
        messages_request(opaque_request(&ciphertext_refs, false)),
    )
    .await
    .expect("maximum-size affinity request handled");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        dispatch.urls.lock().expect("dispatch URL lock").as_slice(),
        &["http://origin.local/v1/messages".to_owned()]
    );
    assert_eq!(
        store.upstream_for(&affinity_key("opaque-1023")),
        Some(origin)
    );
}

#[tokio::test]
async fn opaque_requests_fail_closed_before_dispatch_when_affinity_is_not_usable() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let second = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

    let unknown_store = Arc::new(MemoryAffinityStore::default());
    let unknown_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let unknown = lifecycle(
        vec![upstream_record(first, "first", true)],
        unknown_dispatch.clone(),
        Some(unknown_store),
    );
    assert_eq!(
        handle_authenticated(
            &unknown,
            messages_request(opaque_request(&["unknown"], false))
        )
        .await
        .expect("unknown request handled")
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(unknown_dispatch.call_count(), 0);

    let conflict_store = Arc::new(MemoryAffinityStore::default());
    conflict_store.insert(affinity_key("to-first"), first);
    conflict_store.insert(affinity_key("to-second"), second);
    let conflict_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let conflict = lifecycle(
        vec![
            upstream_record(first, "first", true),
            upstream_record(second, "second", true),
        ],
        conflict_dispatch.clone(),
        Some(conflict_store),
    );
    assert_eq!(
        handle_authenticated(
            &conflict,
            messages_request(opaque_request(&["to-first", "to-second"], false)),
        )
        .await
        .expect("conflicting request handled")
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(conflict_dispatch.call_count(), 0);

    let unavailable_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let unavailable = lifecycle(
        vec![upstream_record(first, "first", true)],
        unavailable_dispatch.clone(),
        None,
    );
    assert_eq!(
        handle_authenticated(
            &unavailable,
            messages_request(opaque_request(&["no-store"], false)),
        )
        .await
        .expect("store-unavailable request handled")
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(unavailable_dispatch.call_count(), 0);

    let target_store = Arc::new(MemoryAffinityStore::default());
    target_store.insert(affinity_key("disabled-target"), second);
    let target_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let target_unavailable = lifecycle(
        vec![
            upstream_record(first, "first", true),
            upstream_record(second, "second", false),
        ],
        target_dispatch.clone(),
        Some(target_store),
    );
    let target_response = handle_authenticated(
        &target_unavailable,
        messages_request(opaque_request(&["disabled-target"], false)),
    )
    .await
    .expect("target-unavailable request handled");
    assert_eq!(target_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(target_dispatch.call_count(), 0);
}

/// #851: an affinity target whose apply status is `ApplyStatus::Error` is no
/// longer a routing candidate, so the pinned request fails closed with 503
/// `upstream_affinity_unavailable` instead of dispatching to it. Mirrors the
/// disabled-target arm above, which takes the same path through
/// `build_candidates` → `candidates.retain(affinity target)`.
#[tokio::test]
async fn affinity_pinned_to_apply_error_upstream_fails_closed() {
    let errored = Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap();
    let healthy = Uuid::parse_str("00000000-0000-0000-0000-00000000000b").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(affinity_key("to-errored"), errored);
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let lifecycle = lifecycle_with_status(
        vec![
            upstream_record(errored, "errored", true),
            upstream_record(healthy, "healthy", true),
        ],
        dispatch.clone(),
        Some(store),
        UpstreamStatusSnapshot {
            entries: HashMap::from([(
                "errored".to_owned(),
                UpstreamStatusEntry {
                    status: ApplyStatus::Error,
                    last_apply_error: Some("credential rejected".to_owned()),
                    last_apply_at_unix_secs: TEST_NOW_UNIX_SECS,
                },
            )]),
            applied_at_unix_secs: TEST_NOW_UNIX_SECS,
            revision_hash: 1,
        },
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(opaque_request(&["to-errored"], false)),
        )
        .await
        .expect("apply-error-pinned request handled"),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&body).expect("error body json");
    // The owner is excluded before dispatch, so the request fails with an
    // explicit affinity error instead of a generic routing miss.
    assert_eq!(body["error"]["type"], "upstream_affinity_unavailable");
    assert_eq!(dispatch.call_count(), 0);
}

#[tokio::test]
async fn lookup_and_bind_failures_do_not_expose_ciphertext() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let lookup_ciphertext = "raw-lookup-ciphertext";
    let lookup_store = Arc::new(MemoryAffinityStore::default());
    *lookup_store
        .resolve_failure
        .lock()
        .expect("resolve failure lock") = Some(lookup_ciphertext.to_owned());
    let lookup_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let lookup = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        lookup_dispatch.clone(),
        Some(lookup_store),
    );
    let (_, _, lookup_body) = collect_body(
        handle_authenticated(
            &lookup,
            messages_request(opaque_request(&[lookup_ciphertext], false)),
        )
        .await
        .expect("lookup failure handled"),
    )
    .await;
    assert!(!String::from_utf8_lossy(&lookup_body).contains(lookup_ciphertext));
    assert_eq!(lookup_dispatch.call_count(), 0);

    let bind_ciphertext = "raw-bind-ciphertext";
    let bind_store = Arc::new(MemoryAffinityStore::default());
    *bind_store.bind_failure.lock().expect("bind failure lock") = Some(bind_ciphertext.to_owned());
    let bind_dispatch =
        RecordingDispatch::with_response(ResponseSpec::Json(web_search_response(bind_ciphertext)));
    let bind = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        bind_dispatch.clone(),
        Some(bind_store),
    );
    let (status, _, bind_body) = collect_body(
        handle_authenticated(
            &bind,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )),
        )
        .await
        .expect("bind failure handled"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!String::from_utf8_lossy(&bind_body).contains(bind_ciphertext));
    assert_eq!(bind_dispatch.call_count(), 1);
}

#[tokio::test]
async fn sse_bind_failure_suppresses_the_encrypted_content_block() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let ciphertext = "raw-stream-ciphertext";
    let store = Arc::new(MemoryAffinityStore::default());
    *store.bind_failure.lock().expect("bind failure lock") = Some(ciphertext.to_owned());
    let first_chunk = Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"web_search_tool_result\",\"content\":[{{\"encrypted_content\":\"{}",
        &ciphertext[..10]
    ));
    let second_chunk = Bytes::from(format!(
        "{}\"}}]}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        &ciphertext[10..]
    ));
    let dispatch =
        RecordingDispatch::with_response(ResponseSpec::SseChunks(vec![first_chunk, second_chunk]));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(store),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )),
        )
        .await
        .expect("SSE bind failure handled"),
    )
    .await;
    let body = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("event: error"));
    assert!(!body.contains(ciphertext));
    assert!(!body.contains(&ciphertext[..10]));
    assert_eq!(dispatch.call_count(), 1);
}

fn provider_request(provider: cc_lb_domain::Provider, path: &str, body: Value) -> Request<Bytes> {
    let mut request = Request::builder()
        .method(http::Method::POST)
        .uri(path)
        .header("x-api-key", crate::common::managed_api_key())
        .body(Bytes::from(body.to_string()))
        .expect("provider request builds");
    request
        .extensions_mut()
        .insert(cc_lb_engine::ProviderScope(provider));
    request
}

#[tokio::test]
async fn provider_affinity_store_failures_use_native_503() -> Result<(), Box<dyn std::error::Error>>
{
    let cases = [
        (
            cc_lb_domain::Provider::Anthropic,
            upstream_record(Uuid::from_u128(2), "anthropic-owner", true),
            "/v1/messages",
            json!({"model":"claude-test","messages":[]}),
            json!({"type":"message","container":{"id":"container-bind-failure"}}),
            UpstreamAffinityKind::AnthropicContainerId,
            "container-bind-failure",
        ),
        (
            cc_lb_domain::Provider::Openai,
            openai_upstream_record(Uuid::from_u128(2), "openai-owner", true),
            "/v1/responses",
            json!({"model":"gpt-test","input":[]}),
            json!({"id":"resp-bind-failure","object":"response","status":"completed"}),
            UpstreamAffinityKind::OpenaiResponseId,
            "resp-bind-failure",
        ),
        (
            cc_lb_domain::Provider::Gemini,
            {
                let mut record = upstream_record(Uuid::from_u128(2), "gemini-owner", true);
                record.kind = cc_lb_storage_api::upstream::UpstreamKind::GeminiApiKey;
                record
            },
            "/v1beta/models/gemini-test:generateContent",
            json!({"contents":[],"cachedContent":"cachedContents/request-bind-failure"}),
            json!({"candidates":[]}),
            UpstreamAffinityKind::GeminiCachedContent,
            "cachedContents/request-bind-failure",
        ),
    ];

    for (provider, record, path, request_body, response_body, kind, raw_key) in cases {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(sqlite_storage(&dir).await?);
        let bus = TestLifecycleBus::new()
            .with_assembler(storage.clone() as Arc<dyn cc_lb_storage_api::Storage>);
        let mut bus = bus;
        let mut events = lifecycle_receiver(&bus);
        let mut updates = bus.bus.subscribe();
        let store = Arc::new(MemoryAffinityStore::default());
        if provider == cc_lb_domain::Provider::Gemini {
            // Gemini learns no response keys. A valid cachedContent reference
            // can fail at lookup, but cannot produce a buffered pending bind.
            *store.resolve_failure.lock().expect("resolve failure lock") =
                Some("forced affinity store failure".to_owned());
        } else {
            *store.bind_failure.lock().expect("bind failure lock") =
                Some("forced affinity store failure".to_owned());
        }
        let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(response_body));
        let lifecycle = lifecycle(vec![record], dispatch.clone(), Some(store.clone()))
            .with_event_bus(bus.bus_arc());
        let (status, _, body) = collect_body(
            handle_authenticated(&lifecycle, provider_request(provider, path, request_body))
                .await
                .expect("provider affinity bind failure handled"),
        )
        .await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{provider:?}");
        let body: Value = serde_json::from_slice(&body).expect("native affinity error JSON");
        assert_eq!(
            body["error"]["message"],
            "upstream affinity is unavailable for opaque request content",
        );
        match provider {
            cc_lb_domain::Provider::Anthropic => {
                assert_eq!(body["type"], "error");
                assert_eq!(body["error"]["type"], "api_error");
            }
            cc_lb_domain::Provider::Openai => {
                assert!(body.get("type").is_none());
                assert_eq!(body["error"]["type"], "api_error");
                assert!(body["error"].get("param").is_some());
                assert!(body["error"].get("code").is_some());
            }
            cc_lb_domain::Provider::Gemini => {
                assert!(body.get("type").is_none());
                assert_eq!(body["error"]["code"], 503);
                assert_eq!(body["error"]["status"], "UNAVAILABLE");
            }
        }
        assert_eq!(
            dispatch.call_count(),
            usize::from(provider != cc_lb_domain::Provider::Gemini),
        );
        assert_eq!(
            store.upstream_for(&UpstreamAffinityKey {
                principal_id: "principal-test".to_owned(),
                provider: match provider {
                    cc_lb_domain::Provider::Anthropic => "anthropic".to_owned(),
                    cc_lb_domain::Provider::Openai => "openai".to_owned(),
                    cc_lb_domain::Provider::Gemini => "gemini".to_owned(),
                },
                kind,
                value_sha256: Sha256::digest(raw_key.as_bytes()).into(),
            }),
            None,
            "{provider:?} failed bind must not persist affinity",
        );
        if provider == cc_lb_domain::Provider::Gemini {
            assert_error_terminal(&mut events, 503, "upstream_affinity_unavailable").await;
        } else {
            assert_stream_failure_once(
                &mut events,
                "upstream_affinity_error",
                503,
                "upstream_affinity_unavailable",
            )
            .await;
        }
        bus.shutdown().await;
        assert_one_final(&mut updates).await;
        let rows = crate::common::stored_request_events(storage.as_ref()).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, 503);
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("upstream_affinity_unavailable")
        );
    }
    Ok(())
}

#[tokio::test]
async fn buffered_response_affinity_extraction_failure_uses_native_503_and_one_final()
-> Result<(), Box<dyn std::error::Error>> {
    for provider in [
        cc_lb_domain::Provider::Anthropic,
        cc_lb_domain::Provider::Openai,
    ] {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(sqlite_storage(&dir).await?);
        let bus = TestLifecycleBus::new()
            .with_assembler(storage.clone() as Arc<dyn cc_lb_storage_api::Storage>);
        let mut bus = bus;
        let mut events = lifecycle_receiver(&bus);
        let mut updates = bus.bus.subscribe();
        let owner = Uuid::from_u128(2);
        let (record, path, request, response) = match provider {
            cc_lb_domain::Provider::Anthropic => (
                upstream_record(owner, "anthropic-owner", true),
                "/v1/messages",
                json!({"model":"claude-test","messages":[]}),
                json!({"type":"message","content":[{
                    "type":"web_search_tool_result",
                    "content": (0..1025).map(|i| json!({"encrypted_content":format!("private-state-{i}")})).collect::<Vec<_>>(),
                }]}),
            ),
            cc_lb_domain::Provider::Openai => (
                openai_upstream_record(owner, "openai-owner", true),
                "/v1/responses",
                json!({"model":"gpt-test","input":[]}),
                json!({"id":"resp-too-many","object":"response","status":"completed",
                    "output": (0..1025).map(|i| json!({"type":"reasoning","encrypted_content":format!("private-state-{i}")})).collect::<Vec<_>>(),
                }),
            ),
            _ => unreachable!(),
        };
        let store = Arc::new(MemoryAffinityStore::default());
        let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(response));
        let lifecycle = lifecycle(vec![record], dispatch.clone(), Some(store.clone()))
            .with_event_bus(bus.bus_arc());
        let (status, _, body) = collect_body(
            handle_authenticated(&lifecycle, provider_request(provider, path, request)).await?,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let value: Value = serde_json::from_slice(&body)?;
        assert_eq!(
            value["error"]["message"],
            "upstream affinity is unavailable for opaque request content"
        );
        assert_eq!(value["error"]["type"], "api_error");
        if provider == cc_lb_domain::Provider::Anthropic {
            assert_eq!(value["type"], "error");
        } else {
            assert!(
                value.get("type").is_none(),
                "OpenAI has no Anthropic wrapper"
            );
            assert!(value["error"].get("code").is_some());
            assert!(value["error"].get("param").is_some());
        }
        assert!(!String::from_utf8_lossy(&body).contains("private-state"));
        assert_eq!(
            store.len(),
            0,
            "failed extraction never binds response state"
        );
        assert_eq!(dispatch.call_count(), 1);
        assert_stream_failure_once(
            &mut events,
            "upstream_affinity_error",
            503,
            "upstream_affinity_unavailable",
        )
        .await;
        bus.shutdown().await;
        assert_one_final(&mut updates).await;
        let rows = crate::common::stored_request_events(storage.as_ref()).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, 503);
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("upstream_affinity_unavailable")
        );
    }
    Ok(())
}

#[tokio::test]
async fn responses_sse_bind_failure_emits_native_failed_without_binding_pending_state()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let mut bus = TestLifecycleBus::new()
        .with_assembler(storage.clone() as Arc<dyn cc_lb_storage_api::Storage>);
    let mut events = lifecycle_receiver(&bus);
    let mut updates = bus.bus.subscribe();
    let owner = Uuid::from_u128(2);
    let store = Arc::new(MemoryAffinityStore::default());
    let known = UpstreamAffinityKey {
        principal_id: "principal-test".to_owned(),
        provider: "openai".to_owned(),
        kind: UpstreamAffinityKind::OpenaiResponseId,
        value_sha256: Sha256::digest(b"resp-known").into(),
    };
    store.insert(known, owner);
    *store.bind_failure.lock().expect("bind failure lock") =
        Some("private failed state".to_owned());
    let first = Bytes::from_static(
        b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":7,\"delta\":\"partial\"}\n\n",
    );
    let blocked = Bytes::from_static(
        b"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":8,\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"private-response-state\"}}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":9,\"response\":{\"status\":\"completed\"}}\n\n",
    );
    let dispatch =
        RecordingDispatch::with_response(ResponseSpec::SseChunks(vec![first.clone(), blocked]));
    let lifecycle = lifecycle(
        vec![openai_upstream_record(owner, "openai-owner", true)],
        dispatch.clone(),
        Some(store.clone()),
    )
    .with_event_bus(bus.bus_arc());
    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            responses_request(json!({
                "model":"gpt-test","stream":true,"previous_response_id":"resp-known",
                "input":[{"type":"reasoning","encrypted_content":"private-pending-state"}],
            })),
        )
        .await?,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.starts_with(&first));
    let failure = assert_responses_failed(&body, None, 8);
    assert_eq!(
        failure["response"]["error"]["message"],
        "upstream affinity is unavailable for opaque request content"
    );
    assert!(!String::from_utf8_lossy(&body).contains("private-"));
    assert_eq!(
        store.len(),
        1,
        "neither blocked response nor pending request key is bound"
    );
    assert_eq!(dispatch.call_count(), 1);
    assert_stream_failure_once(
        &mut events,
        "upstream_affinity_error",
        200,
        "upstream_stream_error",
    )
    .await;
    bus.shutdown().await;
    assert_one_final(&mut updates).await;
    let rows = crate::common::stored_request_events(storage.as_ref()).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, 200);
    assert_eq!(rows[0].error_code.as_deref(), Some("upstream_stream_error"));
    Ok(())
}

#[tokio::test]
async fn healthy_native_buffered_responses_preserve_bytes_and_cached_content_owner() {
    let owner = Uuid::from_u128(2);
    for provider in [
        cc_lb_domain::Provider::Openai,
        cc_lb_domain::Provider::Gemini,
    ] {
        let store = Arc::new(MemoryAffinityStore::default());
        let (record, path, request, response) = if provider == cc_lb_domain::Provider::Openai {
            (
                openai_upstream_record(owner, "openai-owner", true),
                "/v1/responses",
                json!({"model":"gpt-test","input":[]}),
                json!({"id":"resp-healthy","object":"response","status":"completed","output":[]}),
            )
        } else {
            store.insert(
                UpstreamAffinityKey {
                    principal_id: "principal-test".to_owned(),
                    provider: "gemini".to_owned(),
                    kind: UpstreamAffinityKind::GeminiCachedContent,
                    value_sha256: Sha256::digest(b"cachedContents/healthy").into(),
                },
                owner,
            );
            let mut record = upstream_record(owner, "gemini-owner", true);
            record.kind = cc_lb_storage_api::upstream::UpstreamKind::GeminiApiKey;
            (
                record,
                "/v1beta/models/gemini-test:generateContent",
                json!({"contents":[],"cachedContent":"cachedContents/healthy"}),
                json!({"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}]}),
            )
        };
        let raw = Bytes::from(response.to_string());
        let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(response));
        let lifecycle = lifecycle(vec![record], dispatch.clone(), Some(store.clone()));
        let (status, _, body) = collect_body(
            handle_authenticated(&lifecycle, provider_request(provider, path, request))
                .await
                .expect("healthy native request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, raw, "healthy provider bytes stay unchanged");
        assert_eq!(dispatch.call_count(), 1);
        assert_eq!(store.len(), 1);
    }
}
#[tokio::test]
async fn ordinary_anthropic_sse_without_affinity_state_passes_through() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let raw = Bytes::from_static(
        b"event: ping\ndata: {\"type\":\"ping\"}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Sse(raw.clone()));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true}"#,
            )),
        )
        .await
        .expect("ordinary SSE response handled"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, raw);
    assert_eq!(dispatch.call_count(), 1);
}

/// An Anthropic stream request with no web_search/code_execution tool and no
/// container still binds provider-owned state the upstream introduces in its
/// SSE response, matching the unconditional buffered-response scan.
#[tokio::test]
async fn unsolicited_anthropic_sse_state_binds_to_selected_upstream() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let ciphertext = "unsolicited-stream-ciphertext";
    let store = Arc::new(MemoryAffinityStore::default());
    let raw = Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"web_search_tool_result\",\"content\":[{{\"encrypted_content\":\"{ciphertext}\"}}]}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    ));
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Sse(raw.clone()));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true}"#,
            )),
        )
        .await
        .expect("unsolicited state SSE response handled"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, raw);
    assert_eq!(
        store.upstream_for(&affinity_key(ciphertext)),
        Some(upstream)
    );
    assert_eq!(dispatch.call_count(), 1);
}

/// An unhinted Anthropic stream whose state-carrying `content_block_start` is
/// split across upstream chunks holds the partial event until it completes and
/// binds, then relays exactly the upstream bytes.
#[tokio::test]
async fn unsolicited_anthropic_sse_split_state_event_is_held_until_bound() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let ciphertext = "unsolicited-split-ciphertext";
    let store = Arc::new(MemoryAffinityStore::default());
    let ping = "event: ping\ndata: {\"type\":\"ping\"}\n\n";
    let first_chunk = Bytes::from(format!(
        "{ping}event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"web_search_tool_result\",\"content\":[{{\"encrypted_content\":\"{}",
        &ciphertext[..10]
    ));
    let second_chunk = Bytes::from(format!(
        "{}\"}}]}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        &ciphertext[10..]
    ));
    let release = Arc::new(tokio::sync::Notify::new());
    let dispatch = RecordingDispatch::with_response(ResponseSpec::SseGated {
        first: first_chunk.clone(),
        release: release.clone(),
        second: second_chunk.clone(),
    });
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let response = handle_authenticated(
        &lifecycle,
        messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )),
    )
    .await
    .expect("split unsolicited state SSE response handled");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();

    let first_frame = body
        .frame()
        .await
        .expect("first frame delivered")
        .expect("first frame ok")
        .into_data()
        .expect("first data frame");
    assert_eq!(first_frame, Bytes::from_static(ping.as_bytes()));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), body.frame())
            .await
            .is_err(),
        "partial state-carrying event must not reach the client before it completes"
    );
    assert_eq!(store.upstream_for(&affinity_key(ciphertext)), None);

    release.notify_one();
    let mut delivered = first_frame.to_vec();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.expect("frame ok").into_data() {
            delivered.extend_from_slice(&data);
        }
    }

    let mut upstream_bytes = first_chunk.to_vec();
    upstream_bytes.extend_from_slice(&second_chunk);
    assert_eq!(delivered, upstream_bytes);
    assert_eq!(
        store.upstream_for(&affinity_key(ciphertext)),
        Some(upstream)
    );
    assert_eq!(dispatch.call_count(), 1);
}

/// An unhinted Anthropic stream relays stateless events through the ungated
/// passthrough, then fails closed before the first event whose provider-owned
/// state cannot be bound to the selected upstream.
#[tokio::test]
async fn unsolicited_anthropic_sse_bind_failure_fails_closed_only_for_stateful_event() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let ciphertext = "unsolicited-bind-failure-ciphertext";
    let store = Arc::new(MemoryAffinityStore::default());
    *store.bind_failure.lock().expect("bind failure lock") = Some(ciphertext.to_owned());
    let stateless_chunk = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\n",
    );
    let stateful_chunk = Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"web_search_tool_result\",\"content\":[{{\"encrypted_content\":\"{ciphertext}\"}}]}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    ));
    let dispatch = RecordingDispatch::with_response(ResponseSpec::SseChunks(vec![
        stateless_chunk.clone(),
        stateful_chunk,
    ]));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true}"#,
            )),
        )
        .await
        .expect("unsolicited SSE bind failure handled"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.starts_with(&stateless_chunk[..]));
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("event: error"));
    assert!(body.contains("upstream affinity is unavailable"));
    assert!(!body.contains(ciphertext));
    assert!(!body.contains("message_stop"));
    assert_eq!(store.upstream_for(&affinity_key(ciphertext)), None);
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn request_key_limit_rejects_before_dispatch() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let content = (0..1025)
        .map(|index| json!({"encrypted_content": format!("request-opaque-{index}")}))
        .collect::<Vec<_>>();
    let request = Bytes::from(
        json!({
            "model": "claude-test",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "web_search_tool_result",
                    "content": content,
                }],
            }],
        })
        .to_string(),
    );
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(&lifecycle, messages_request(request))
            .await
            .expect("oversized affinity request handled"),
    )
    .await;
    let body = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("too many upstream affinity keys"));
    assert!(!body.contains("request-opaque-0"));
    assert_eq!(dispatch.call_count(), 0);
}

#[tokio::test]
async fn buffered_response_key_limit_fails_closed() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let content = (0..1025)
        .map(|index| json!({"encrypted_content": format!("response-opaque-{index}")}))
        .collect::<Vec<_>>();
    let response = json!({
        "type": "message",
        "content": [{
            "type": "web_search_tool_result",
            "content": content,
        }],
    });
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(response));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )),
        )
        .await
        .expect("oversized affinity response handled"),
    )
    .await;
    let body = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("upstream affinity is unavailable"));
    assert!(!body.contains("response-opaque-0"));
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn sse_response_key_limit_fails_closed_before_event_delivery() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let content = (0..1025)
        .map(|index| json!({"encrypted_content": format!("sse-opaque-{index}")}))
        .collect::<Vec<_>>();
    let event = json!({
        "type": "content_block_start",
        "content_block": {
            "type": "web_search_tool_result",
            "content": content,
        },
    });
    let sse = Bytes::from(format!("event: content_block_start\ndata: {event}\n\n"));
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Sse(sse));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )),
        )
        .await
        .expect("oversized SSE affinity response handled"),
    )
    .await;
    let body = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("event: error"));
    assert!(!body.contains("sse-opaque-0"));
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn incomplete_sse_budget_overflow_does_not_passthrough_opaque_bytes() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let ciphertext = "incomplete-opaque-ciphertext";
    let incomplete = Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"content_block\":{{\"type\":\"web_search_tool_result\",\"content\":[{{\"encrypted_content\":\"{ciphertext}{}",
        "x".repeat(128)
    ));
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Sse(incomplete));
    let lifecycle = lifecycle_with_config(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        None,
        LifecycleConfig {
            messages_body_cap_bytes: 192,
            ..LifecycleConfig::default()
        },
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )),
        )
        .await
        .expect("incomplete SSE response handled"),
    )
    .await;
    let body = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("event: error"));
    assert!(!body.contains(ciphertext));
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn malformed_and_uninspectable_success_sse_fail_closed() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let malformed_ciphertext = "malformed-opaque-ciphertext";
    let malformed = Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"encrypted_content\":\"{malformed_ciphertext}\"\n\n"
    ));
    let malformed_dispatch = RecordingDispatch::with_response(ResponseSpec::Sse(malformed));
    let malformed_lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        malformed_dispatch.clone(),
        None,
    );
    let (_, _, malformed_body) = collect_body(
        handle_authenticated(
            &malformed_lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )),
        )
        .await
        .expect("malformed SSE response handled"),
    )
    .await;
    let malformed_body = String::from_utf8_lossy(&malformed_body);
    assert!(malformed_body.contains("event: error"));
    assert!(!malformed_body.contains(malformed_ciphertext));

    let encoded_ciphertext = "encoded-opaque-ciphertext";
    let encoded = Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"encrypted_content\":\"{encoded_ciphertext}\"}}\n\n"
    ));
    let encoded_dispatch = RecordingDispatch::with_response(ResponseSpec::SseWithEncoding {
        body: encoded,
        content_encoding: "snappy",
    });
    let encoded_lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        encoded_dispatch.clone(),
        None,
    );
    let (_, _, encoded_body) = collect_body(
        handle_authenticated(
            &encoded_lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )),
        )
        .await
        .expect("unsupported SSE encoding handled"),
    )
    .await;
    let encoded_body = String::from_utf8_lossy(&encoded_body);
    assert!(encoded_body.contains("event: error"));
    assert!(!encoded_body.contains(encoded_ciphertext));
}

#[tokio::test]
async fn buffered_body_frame_error_discards_partial_opaque_body() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let ciphertext = "partial-buffered-opaque-ciphertext";
    let partial = Bytes::from(
        json!({
            "type": "message",
            "content": [{
                "type": "web_search_tool_result",
                "content": [{"encrypted_content": ciphertext}],
            }],
        })
        .to_string(),
    );
    let dispatch = RecordingDispatch::with_response(ResponseSpec::JsonThenError(partial));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )),
        )
        .await
        .expect("buffered frame error handled"),
    )
    .await;
    let body = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("upstream response body could not be read"));
    assert!(!body.contains(ciphertext));
    assert_eq!(dispatch.call_count(), 1);
}

/// #663: the affinity gate decodes gzip and is the SSE emitter of record, so a
/// gzip trailer that cannot be verified after `message_stop` completes the
/// stream with a warning instead of an inspection-failed frame.
#[tokio::test]
async fn gate_gzip_trailer_loss_after_message_stop_is_accepted_and_binds_affinity() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(affinity_key("gate-known-663"), upstream);
    let plaintext = Bytes::from_static(
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let dispatch = RecordingDispatch::with_response(ResponseSpec::SseWithEncoding {
        body: gzip_without_trailer(&plaintext),
        content_encoding: "gzip",
    });
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let (status, headers, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(opaque_request(
                &["gate-known-663", "gate-pending-663"],
                true,
            )),
        )
        .await
        .expect("gated gzip SSE response handled"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(headers.get(http::header::CONTENT_ENCODING).is_none());
    assert_eq!(body, plaintext);
    assert_eq!(
        store.upstream_for(&affinity_key("gate-pending-663")),
        Some(upstream)
    );
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn gate_gzip_truncated_before_message_stop_fails_closed_without_binding() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(affinity_key("gate-known-663-before"), upstream);
    let plaintext = Bytes::from_static(
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    );
    let dispatch = RecordingDispatch::with_response(ResponseSpec::SseWithEncoding {
        body: gzip_without_trailer(&plaintext),
        content_encoding: "gzip",
    });
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            messages_request(opaque_request(
                &["gate-known-663-before", "gate-pending-663-before"],
                true,
            )),
        )
        .await
        .expect("truncated gated gzip SSE response handled"),
    )
    .await;
    let text = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK);
    assert!(text.starts_with(std::str::from_utf8(&plaintext).expect("utf8 fixture")));
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(text.contains("upstream SSE response could not be safely processed"));
    assert_eq!(
        store.upstream_for(&affinity_key("gate-pending-663-before")),
        None
    );
}

fn gzip_without_trailer(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    let compressed = encoder.finish().expect("gzip finish succeeds");
    Bytes::copy_from_slice(&compressed[..compressed.len() - 8])
}

#[tokio::test]
async fn gemini_cached_contents_bind_failure_and_missing_store_fail_closed() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    for missing_store in [false, true] {
        let store = Arc::new(MemoryAffinityStore::default());
        *store.bind_failure.lock().expect("bind failure lock") =
            Some("forced bind failure".to_owned());
        let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
            "name": "cachedContents/test-cache",
            "expireTime": "2100-01-01T00:00:00Z",
        })));
        let lifecycle = lifecycle(
            vec![gemini_upstream_record(upstream, "gemini", true)],
            dispatch.clone(),
            (!missing_store).then(|| store.clone()),
        );

        let (status, _, body) = collect_body(
            handle_authenticated(
                &lifecycle,
                gemini_cached_contents_request(json!({
                    "model": "models/gemini-2.5-flash",
                    "contents": [],
                })),
            )
            .await
            .expect("Gemini cached contents request handled"),
        )
        .await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(dispatch.call_count(), if missing_store { 0 } else { 1 });
        let error_body: Value = serde_json::from_slice(&body).expect("Gemini error body JSON");
        assert_eq!(error_body["error"]["code"], 503);
        assert_eq!(error_body["error"]["status"], "UNAVAILABLE");
        assert_eq!(store.len(), 0);
        assert!(!String::from_utf8_lossy(&body).contains("cachedContents/test-cache"));
    }
}

#[tokio::test]
async fn gemini_cached_contents_create_binds_routed_owner_epoch_before_return() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.set_owner_epoch(upstream, 7);
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "name": "cachedContents/owner-epoch",
        "expireTime": "2100-01-01T00:00:00Z",
    })));
    let lifecycle = lifecycle(
        vec![gemini_upstream_record(upstream, "gemini", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let response = handle_authenticated(
        &lifecycle,
        gemini_cached_contents_request(json!({
            "model": "models/gemini-2.5-flash",
            "contents": [],
        })),
    )
    .await
    .expect("Gemini cached contents request handled");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(dispatch.call_count(), 1);
    let key = UpstreamAffinityKey {
        principal_id: "principal-test".to_owned(),
        provider: "gemini".to_owned(),
        kind: UpstreamAffinityKind::GeminiCachedContent,
        value_sha256: Sha256::digest(b"cachedContents/owner-epoch").into(),
    };
    assert_eq!(store.binding_epoch_for(&key), Some(7));
    assert_eq!(
        store
            .bindings
            .lock()
            .expect("affinity binding lock")
            .get(&key)
            .expect("binding persisted before response is returned")
            .expires_at_unix_secs,
        Some(4_102_444_800),
    );
}

#[tokio::test]
async fn gemini_cached_contents_body_collection_failure_returns_native_error_without_binding() {
    use cc_lb_control::RequestEventBus as _;

    let test_bus = crate::common::TestLifecycleBus::new();
    let mut events = test_bus.bus.subscribe_lifecycle();
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let dispatch =
        RecordingDispatch::with_response(ResponseSpec::JsonThenError(Bytes::from_static(
            br#"{"name":"cachedContents/incomplete","expireTime":"2100-01-01T00:00:00Z"}"#,
        )));
    let lifecycle = lifecycle(
        vec![gemini_upstream_record(upstream, "gemini", true)],
        dispatch.clone(),
        Some(store.clone()),
    )
    .with_event_bus(test_bus.bus.clone());

    let (status, _, body) = collect_body(
        handle_authenticated(
            &lifecycle,
            gemini_cached_contents_request(json!({
                "model": "models/gemini-2.5-flash",
                "contents": [],
            })),
        )
        .await
        .expect("Gemini cached contents request handled"),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(dispatch.call_count(), 1);
    assert_eq!(store.len(), 0);
    let terminal_statuses = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            cc_lb_lifecycle::LifecycleEvent::RequestTerminated { client_status, .. } => {
                Some(client_status)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(terminal_statuses, vec![status.as_u16()]);
    let error_body: Value = serde_json::from_slice(&body).expect("Gemini error body JSON");
    assert_eq!(error_body["error"]["code"], 503);
    assert_eq!(error_body["error"]["status"], "UNAVAILABLE");
    assert!(!String::from_utf8_lossy(&body).contains("cachedContents/incomplete"));
}

#[tokio::test]
async fn gemini_sqlite_affinity_rejects_stale_dynamic_view_before_dispatch() {
    let temp_dir = tempfile::tempdir().expect("create sqlite tempdir");
    let storage = Arc::new(
        crate::common::templated_sqlite_storage(
            &temp_dir.path().join("affinity.sqlite"),
            Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
        )
        .await
        .expect("open sqlite affinity storage"),
    );
    let old_record = storage
        .create(UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: Uuid::new_v4(),
            name: "gemini-stale-view".to_owned(),
            kind: UpstreamKind::GeminiApiKey,
            base_url: Some(Url::parse("http://gemini.local/").expect("test URL")),
            api_key_ciphertext: Some(vec![1]),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        })
        .await
        .expect("create Gemini upstream");
    let current_record = storage
        .update_api_key_secret(old_record.id, Some(vec![2]))
        .await
        .expect("replace Gemini credential");
    assert_ne!(old_record.revision, current_record.revision);

    let create_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "name": "cachedContents/current-owner",
        "expireTime": "2100-01-01T00:00:00Z",
    })));
    let current = lifecycle(vec![current_record.clone()], create_dispatch.clone(), None)
        .with_upstream_affinity_store(storage.clone());
    let created = handle_authenticated(
        &current,
        gemini_cached_contents_request(json!({
            "model": "models/gemini-2.5-flash",
            "contents": [],
        })),
    )
    .await
    .expect("current owner create handled");
    assert_eq!(created.status(), StatusCode::OK);
    assert_eq!(create_dispatch.call_count(), 1);

    let cached_request = |action: &str| {
        let mut request = gemini_cached_contents_request(json!({
            "cachedContent": "cachedContents/current-owner",
            "contents": [],
        }));
        *request.uri_mut() = format!("/v1beta/models/gemini-2.5-flash:{action}")
            .parse()
            .expect("Gemini action URI");
        request
    };
    let stale_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let stale = lifecycle(vec![old_record], stale_dispatch.clone(), None)
        .with_upstream_affinity_store(storage.clone());
    for request in [
        gemini_cached_contents_request(json!({
            "model": "models/gemini-2.5-flash",
            "contents": [],
        })),
        cached_request("generateContent"),
        cached_request("countTokens"),
    ] {
        let response = handle_authenticated(&stale, request)
            .await
            .expect("stale view request handled");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    assert_eq!(stale_dispatch.call_count(), 0);

    let healthy_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "candidates": [],
    })));
    let healthy = lifecycle(vec![current_record], healthy_dispatch.clone(), None)
        .with_upstream_affinity_store(storage);
    let response = handle_authenticated(&healthy, cached_request("generateContent"))
        .await
        .expect("current view cached generation handled");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(healthy_dispatch.call_count(), 1);
}

// These fixtures retain the captured Responses event shape; ciphertext and ids
// are deterministic synthetic values, not provider-owned state from the capture.
const WS1296_REASONING: &str = "synthetic-ws1296-encrypted-reasoning";
const WS1296_RESPONSE_ID: &str = "resp_ws1296_unstored";

fn ws1296_openai_key(value: &str) -> UpstreamAffinityKey {
    UpstreamAffinityKey {
        principal_id: "principal-test".to_owned(),
        provider: "openai".to_owned(),
        kind: UpstreamAffinityKind::OpenaiResponseId,
        value_sha256: Sha256::digest(value.as_bytes()).into(),
    }
}

fn ws1296_reasoning_item(ciphertext: &str) -> Value {
    json!({
        "type": "reasoning",
        "id": "rs_ws1296",
        "summary": [],
        "content": [],
        "encrypted_content": ciphertext,
    })
}

fn ws1296_create(previous: Option<&str>, ciphertext: Option<&str>) -> Bytes {
    let mut input = Vec::new();
    if let Some(ciphertext) = ciphertext {
        input.push(ws1296_reasoning_item(ciphertext));
    }
    input.push(json!({"role": "user", "content": "continue"}));
    let mut body = json!({
        "type": "response.create",
        "model": "gpt-5-codex",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": input,
    });
    if let Some(previous) = previous {
        body["previous_response_id"] = json!(previous);
    }
    Bytes::from(body.to_string())
}

fn ws1296_created(id: &str) -> Bytes {
    Bytes::from(
        json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": {
                "id": id,
                "object": "response",
                "model": "gpt-5-codex",
                "status": "in_progress",
                "store": false,
                "output": [],
                "usage": null,
            },
        })
        .to_string(),
    )
}

fn ws1296_item_done() -> Bytes {
    // In particular, the real provider supplied neither response_id nor stream_id.
    Bytes::from(
        json!({
            "type": "response.output_item.done",
            "sequence_number": 3,
            "output_index": 0,
            "item": ws1296_reasoning_item(WS1296_REASONING),
        })
        .to_string(),
    )
}

fn ws1296_completed(id: &str, output: Vec<Value>) -> Bytes {
    Bytes::from(
        json!({
            "type": "response.completed",
            "sequence_number": 106,
            "response": {
                "id": id,
                "object": "response",
                "model": "gpt-5-codex",
                "status": "completed",
                "store": false,
                "output": output,
                "usage": null,
            },
        })
        .to_string(),
    )
}

async fn ws1296_gateway(
    lifecycle: Lifecycle,
) -> cc_lb_engine::responses_websocket::ResponsesWebSocketGateway {
    let lifecycle = Arc::new(lifecycle);
    let headers = responses_request(json!({})).headers().clone();
    let auth = lifecycle
        .authenticate(&headers)
        .await
        .expect("WebSocket upgrade authenticates");
    cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
        lifecycle,
        &auth,
        headers,
        1 << 20,
        8,
    )
}

async fn ws1296_admit(
    gateway: &mut cc_lb_engine::responses_websocket::ResponsesWebSocketGateway,
    frame: Bytes,
) -> Result<
    Option<cc_lb_engine::responses_websocket::WebSocketDispatch>,
    cc_lb_engine::responses_websocket::WebSocketError,
> {
    let auth = gateway
        .authenticate()
        .await
        .expect("fresh turn authenticates");
    gateway.client_message(auth, frame).await
}

async fn ws1296_send(
    gateway: &mut cc_lb_engine::responses_websocket::ResponsesWebSocketGateway,
    dispatch: &cc_lb_engine::responses_websocket::WebSocketDispatch,
    connection_id: u64,
) {
    let owner = dispatch.candidates.first().expect("turn has a route");
    gateway.assign_connection(
        dispatch.turn_id,
        connection_id,
        owner.upstream_id,
        &owner.upstream_name,
    );
    gateway
        .validate_send(dispatch.turn_id)
        .await
        .expect("owner remains eligible at send");
    assert!(gateway.sent(dispatch.turn_id).is_empty());
}

fn ws1296_assert_raw_forward(result: &cc_lb_engine::responses_websocket::WebSocketEventResult) {
    assert!(
        result.forward,
        "the original provider frame must be forwarded"
    );
    assert!(
        result.frames.is_none(),
        "provider bytes must not be replaced"
    );
    assert!(
        result.errors.is_empty(),
        "healthy state must not emit an error"
    );
    assert!(result.dispatches.is_empty());
}

async fn ws1296_learn_reasoning(store: Arc<MemoryAffinityStore>, owner: Uuid, terminal_only: bool) {
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![openai_upstream_record(owner, "owner", true)],
        dispatch.clone(),
        Some(store.clone()),
    ))
    .await;
    let turn = ws1296_admit(&mut gateway, ws1296_create(None, None))
        .await
        .expect("first turn admitted")
        .expect("first turn dispatches");
    ws1296_send(&mut gateway, &turn, 1).await;
    let created = gateway
        .provider_message(1, &ws1296_created(WS1296_RESPONSE_ID))
        .await
        .expect("empty created snapshot is accepted");
    ws1296_assert_raw_forward(&created);
    assert_eq!(
        store.len(),
        0,
        "an unstored response id is connection-local"
    );
    if !terminal_only {
        let item_done = ws1296_item_done();
        for _ in 0..2 {
            let observed = gateway
                .provider_message(1, &item_done)
                .await
                .expect("unattributed item.done belongs to the sole admitted turn");
            assert_eq!(
                store.upstream_for(&ws1296_openai_key(WS1296_REASONING)),
                Some(owner),
                "reasoning must be durable before item.done can be forwarded",
            );
            assert_eq!(store.len(), 1, "duplicate item.done is idempotent");
            ws1296_assert_raw_forward(&observed);
        }
    }
    let completed = ws1296_completed(
        WS1296_RESPONSE_ID,
        vec![ws1296_reasoning_item(WS1296_REASONING)],
    );
    let observed = gateway
        .provider_message(1, &completed)
        .await
        .expect("completed snapshot is accepted after created");
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(WS1296_REASONING)),
        Some(owner),
        "terminal-only reasoning must be durable before completion is forwarded",
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(WS1296_RESPONSE_ID)),
        None
    );
    assert_eq!(
        store.len(),
        1,
        "snapshot and item.done share the legacy digest key"
    );
    ws1296_assert_raw_forward(&observed);
    let duplicate = gateway
        .provider_message(1, &completed)
        .await
        .expect("duplicate completion is harmless");
    assert!(
        !duplicate.forward,
        "a terminal frame is delivered only once"
    );
    assert!(duplicate.frames.is_none());
    assert!(duplicate.errors.is_empty());
    assert_eq!(store.len(), 1);
    assert_eq!(
        dispatch.call_count(),
        0,
        "gateway admission does not use HTTP dispatch"
    );
    gateway.close();
}

#[tokio::test]
async fn ws1296_item_done_binds_reasoning_before_forward_and_deduplicates() {
    ws1296_learn_reasoning(
        Arc::new(MemoryAffinityStore::default()),
        Uuid::from_u128(0x129601),
        false,
    )
    .await;
}

#[tokio::test]
async fn ws1296_terminal_only_reasoning_binds_after_empty_created_snapshot() {
    ws1296_learn_reasoning(
        Arc::new(MemoryAffinityStore::default()),
        Uuid::from_u128(0x129601),
        true,
    )
    .await;
}

#[tokio::test]
async fn ws1296_websocket_learned_reasoning_routes_new_websocket_and_http_to_owner() {
    let owner = Uuid::from_u128(0x129601);
    let fallback = Uuid::from_u128(0x129602);
    let store = Arc::new(MemoryAffinityStore::default());
    ws1296_learn_reasoning(store.clone(), owner, false).await;
    let records = vec![
        openai_upstream_record(fallback, "fallback", true),
        openai_upstream_record(owner, "owner", true),
    ];
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_ws1296_http",
        "object": "response",
        "output": [],
    })));
    let mut gateway = ws1296_gateway(lifecycle(
        records.clone(),
        dispatch.clone(),
        Some(store.clone()),
    ))
    .await;
    let turn = ws1296_admit(&mut gateway, ws1296_create(None, Some(WS1296_REASONING)))
        .await
        .expect("reasoning continuation admits on a new WebSocket")
        .expect("reasoning continuation dispatches");
    assert_eq!(turn.candidates.len(), 1);
    assert_eq!(turn.candidates[0].upstream_id, owner);
    ws1296_send(&mut gateway, &turn, 2).await;
    gateway.close();
    let mut body: Value =
        serde_json::from_slice(&ws1296_create(None, Some(WS1296_REASONING))).unwrap();
    body.as_object_mut().unwrap().remove("type");
    let http = lifecycle(records, dispatch.clone(), Some(store));
    let response = handle_authenticated(&http, responses_request(body))
        .await
        .expect("HTTP continuation is handled");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        dispatch.urls.lock().expect("dispatch URL lock").as_slice(),
        &["http://owner.local/v1/responses".to_owned()],
    );
}

#[tokio::test]
async fn ws1296_legacy_reasoning_owner_disabled_fails_closed_for_websocket_and_http() {
    let owner = Uuid::from_u128(0x129601);
    let fallback = Uuid::from_u128(0x129602);
    let store = Arc::new(MemoryAffinityStore::default());
    // Existing rows must remain usable without a new kind or a migration.
    store.insert(ws1296_openai_key(WS1296_REASONING), owner);
    let records = vec![
        openai_upstream_record(owner, "owner", false),
        openai_upstream_record(fallback, "fallback", true),
    ];
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        records.clone(),
        dispatch.clone(),
        Some(store.clone()),
    ))
    .await;
    let error = ws1296_admit(&mut gateway, ws1296_create(None, Some(WS1296_REASONING)))
        .await
        .expect_err("disabled reasoning owner must not fall back");
    assert_eq!(error.status, 503);
    assert_eq!(error.code, "upstream_affinity_unavailable");
    let mut body: Value =
        serde_json::from_slice(&ws1296_create(None, Some(WS1296_REASONING))).unwrap();
    body.as_object_mut().unwrap().remove("type");
    let http = lifecycle(records, dispatch.clone(), Some(store));
    let response = handle_authenticated(&http, responses_request(body))
        .await
        .expect("disabled owner HTTP continuation is handled");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(dispatch.call_count(), 0);
    gateway.close();
}

#[tokio::test]
async fn ws1296_live_previous_response_and_other_reasoning_owner_conflict_before_dispatch() {
    let reasoning_owner = Uuid::from_u128(0x129601);
    let live_owner = Uuid::from_u128(0x129602);
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(WS1296_REASONING), reasoning_owner);
    store.insert(
        ws1296_openai_key("synthetic-ws1296-live-owner-seed"),
        live_owner,
    );
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![
            openai_upstream_record(reasoning_owner, "reasoning-owner", true),
            openai_upstream_record(live_owner, "live-owner", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    ))
    .await;
    let initial = ws1296_admit(
        &mut gateway,
        ws1296_create(None, Some("synthetic-ws1296-live-owner-seed")),
    )
    .await
    .expect("seeded live-owner turn is admitted")
    .expect("seeded live-owner turn dispatches");
    assert_eq!(initial.candidates[0].upstream_id, live_owner);
    ws1296_send(&mut gateway, &initial, 2).await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(2, &ws1296_created("resp_ws1296_live_u2"))
            .await
            .expect("U2 response is connection-owned"),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(2, &ws1296_completed("resp_ws1296_live_u2", vec![]))
            .await
            .expect("U2 turn completes"),
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key("resp_ws1296_live_u2")),
        None
    );
    let error = ws1296_admit(
        &mut gateway,
        ws1296_create(Some("resp_ws1296_live_u2"), Some(WS1296_REASONING)),
    )
    .await
    .expect_err("live U2 response must not hide the durable U1 reasoning constraint");
    assert_eq!(error.status, 400);
    assert_eq!(error.code, "upstream_affinity_unavailable");
    assert_eq!(dispatch.call_count(), 0);
    gateway.close();
}

#[tokio::test]
async fn ws1296_reasoning_persistence_failure_suppresses_item_and_terminal_frames() {
    for terminal_only in [false, true] {
        let owner = Uuid::from_u128(0x129601);
        let store = Arc::new(MemoryAffinityStore::default());
        let mut gateway = ws1296_gateway(lifecycle(
            vec![openai_upstream_record(owner, "owner", true)],
            Arc::new(RecordingDispatch::default()),
            Some(store.clone()),
        ))
        .await;
        let turn = ws1296_admit(&mut gateway, ws1296_create(None, None))
            .await
            .expect("turn admitted before persistence fails")
            .expect("turn dispatches");
        ws1296_send(&mut gateway, &turn, 1).await;
        ws1296_assert_raw_forward(
            &gateway
                .provider_message(1, &ws1296_created(WS1296_RESPONSE_ID))
                .await
                .expect("empty snapshot has no durable state"),
        );
        store.fail_bind("synthetic persistence outage");
        let frame = if terminal_only {
            ws1296_completed(
                WS1296_RESPONSE_ID,
                vec![ws1296_reasoning_item(WS1296_REASONING)],
            )
        } else {
            ws1296_item_done()
        };
        let error = match gateway.provider_message(1, &frame).await {
            Err(error) => error,
            Ok(mut result) => {
                assert!(
                    !result.forward,
                    "unowned reasoning must never reach the client"
                );
                assert!(result.frames.as_ref().is_none_or(Vec::is_empty));
                assert!(result.dispatches.is_empty());
                assert_eq!(result.errors.len(), 1);
                result.errors.remove(0)
            }
        };
        assert_eq!(error.status, 503);
        assert_eq!(error.code, "upstream_affinity_unavailable");
        assert!(!String::from_utf8_lossy(&error.to_bytes()).contains(WS1296_REASONING));
        assert_eq!(store.len(), 0);
        gateway.close();
    }
}

#[tokio::test]
async fn ws1296_terminal_bind_failure_preserves_measured_usage_when_connection_closes() {
    use cc_lb_lifecycle::LifecycleEvent;

    let owner = Uuid::from_u128(0x129601);
    let store = Arc::new(MemoryAffinityStore::default());
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let mut gateway = ws1296_gateway(
        lifecycle(
            vec![openai_upstream_record(owner, "owner", true)],
            Arc::new(RecordingDispatch::default()),
            Some(store.clone()),
        )
        .with_event_bus(bus),
    )
    .await;
    let turn = ws1296_admit(&mut gateway, ws1296_create(None, None))
        .await
        .expect("turn is admitted")
        .expect("turn dispatches");
    ws1296_send(&mut gateway, &turn, 1).await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &ws1296_created(WS1296_RESPONSE_ID))
            .await
            .expect("empty created snapshot is accepted"),
    );
    store.fail_bind("synthetic terminal persistence outage");
    let mut terminal: Value = serde_json::from_slice(&ws1296_completed(
        WS1296_RESPONSE_ID,
        vec![ws1296_reasoning_item(WS1296_REASONING)],
    ))
    .expect("terminal fixture is JSON");
    terminal["response"]["usage"] = json!({
        "input_tokens": 23,
        "output_tokens": 17,
        "total_tokens": 40,
        "input_tokens_details": {"cached_tokens": 7},
        "output_tokens_details": {"reasoning_tokens": 5},
    });
    let error = gateway
        .provider_message(1, &Bytes::from(terminal.to_string()))
        .await
        .err()
        .expect("terminal persistence failure suppresses the provider frame");
    assert_eq!(error.status, 503);
    assert_eq!(error.code, "upstream_affinity_unavailable");
    assert!(!String::from_utf8_lossy(&error.to_bytes()).contains(WS1296_REASONING));
    assert_eq!(store.len(), 0);

    // The relay retires the provider connection after provider_message fails.
    // That cleanup must not discard already measured, billable terminal usage.
    let closed = gateway.connection_closed(1, "terminal affinity persistence failed");
    assert!(!closed.forward);
    assert!(closed.frames.is_none());
    assert!(closed.dispatches.is_empty());
    gateway.close();
    drop(gateway);

    let mut starts = Vec::new();
    let mut usages = Vec::new();
    let mut completions = Vec::new();
    let mut terminations = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            LifecycleEvent::RequestStarted { event_id, .. } => starts.push(event_id),
            LifecycleEvent::UsageObserved {
                event_id, usage, ..
            } => usages.push((
                event_id,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_input_tokens,
                usage.thinking_tokens,
            )),
            LifecycleEvent::StreamCompleted {
                event_id, result, ..
            } => {
                assert!(
                    result.is_err(),
                    "suppressed completion is not a successful stream"
                );
                completions.push(event_id);
            }
            LifecycleEvent::RequestTerminated { event_id, .. } => terminations.push(event_id),
            _ => {}
        }
    }
    assert_eq!(starts.len(), 1);
    assert_eq!(
        usages,
        vec![(starts[0].clone(), 16, 17, 7, 5)],
        "affinity failure must retain uncached input, output, cached input, and reasoning usage exactly once",
    );
    assert_eq!(
        completions, starts,
        "the failed stream settles exactly once"
    );
    assert_eq!(
        terminations, starts,
        "connection cleanup must not duplicate settlement"
    );
}

#[tokio::test]
async fn ws1296_live_previous_response_accepts_same_reasoning_owner_on_original_connection() {
    let owner = Uuid::from_u128(0x129601);
    let fallback = Uuid::from_u128(0x129602);
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(WS1296_REASONING), owner);
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![
            openai_upstream_record(fallback, "fallback", true),
            openai_upstream_record(owner, "owner", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    ))
    .await;
    let initial = ws1296_admit(&mut gateway, ws1296_create(None, Some(WS1296_REASONING)))
        .await
        .expect("owner turn is admitted")
        .expect("owner turn dispatches");
    assert_eq!(initial.candidates[0].upstream_id, owner);
    ws1296_send(&mut gateway, &initial, 1).await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &ws1296_created(WS1296_RESPONSE_ID))
            .await
            .expect("owner response is connection-local"),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &ws1296_completed(WS1296_RESPONSE_ID, vec![]))
            .await
            .expect("owner response completes"),
    );
    let continuation = ws1296_create(Some(WS1296_RESPONSE_ID), Some(WS1296_REASONING));
    let turn = ws1296_admit(&mut gateway, continuation.clone())
        .await
        .expect("matching durable and live owners are compatible")
        .expect("same-owner continuation dispatches");
    assert_eq!(turn.connection_id, Some(1));
    assert_eq!(turn.candidates.len(), 1);
    assert_eq!(turn.candidates[0].upstream_id, owner);
    assert_eq!(
        turn.frame, continuation,
        "the continuation remains byte-identical"
    );
    ws1296_send(&mut gateway, &turn, 1).await;
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(WS1296_RESPONSE_ID)),
        None
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(WS1296_REASONING)),
        Some(owner)
    );
    assert_eq!(dispatch.call_count(), 0);
    gateway.close();
}

fn openai_oauth_upstream_record(
    id: Uuid,
    name: &str,
    enabled: bool,
) -> cc_lb_storage_api::UpstreamRecord {
    let mut record = upstream_record(id, name, enabled);
    record.kind = UpstreamKind::OpenaiOauth;
    record
}

#[tokio::test]
async fn ws_oauth_reconnect_previous_response_returns_400_with_non_openai_upstreams() {
    let oauth_id = Uuid::from_u128(0x138201);
    let anthropic_id = Uuid::from_u128(0x138202);
    let gemini_id = Uuid::from_u128(0x138203);
    let store = Arc::new(MemoryAffinityStore::default());
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(oauth_id, "oauth-main", true),
            upstream_record(anthropic_id, "anthropic-main", true),
            gemini_upstream_record(gemini_id, "gemini-main", true),
        ],
        dispatch.clone(),
        Some(store),
    ))
    .await;

    let error = ws1296_admit(&mut gateway, ws1296_create(Some("resp_closed_ws"), None))
        .await
        .expect_err("OAuth-only OpenAI pool must convert missing previous_response_id to 400");
    assert_eq!(error.status, 400);
    assert_eq!(error.code, "previous_response_not_found");
    assert_eq!(dispatch.call_count(), 0);
    gateway.close();
}

#[tokio::test]
async fn ws_codex_backend_reconnect_previous_response_returns_400_with_mixed_openai_pool() {
    let oauth_id = Uuid::from_u128(0x138211);
    let api_key_id = Uuid::from_u128(0x138212);
    let anthropic_id = Uuid::from_u128(0x138213);
    let store = Arc::new(MemoryAffinityStore::default());
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(oauth_id, "oauth-main", true),
            openai_upstream_record(api_key_id, "openai-api-key", true),
            upstream_record(anthropic_id, "anthropic-main", true),
        ],
        dispatch.clone(),
        Some(store),
    ))
    .await
    .with_codex_backend(true);

    let error = ws1296_admit(&mut gateway, ws1296_create(Some("resp_closed_ws"), None))
        .await
        .expect_err("codex_backend surface is OAuth-only even with OpenaiApiKey present");
    assert_eq!(error.status, 400);
    assert_eq!(error.code, "previous_response_not_found");
    assert_eq!(dispatch.call_count(), 0);
    gateway.close();
}

#[tokio::test]
async fn ws_previous_response_fallback_controls_preserve_503_boundaries() {
    let oauth_id = Uuid::from_u128(0x138221);
    let api_key_id = Uuid::from_u128(0x138222);
    let anthropic_id = Uuid::from_u128(0x138223);

    // Control A: mixed OpenAI pool on public /v1/responses must keep 503.
    let store_a = Arc::new(MemoryAffinityStore::default());
    let mut mixed_public = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(oauth_id, "oauth-main", true),
            openai_upstream_record(api_key_id, "openai-api-key", true),
        ],
        Arc::new(RecordingDispatch::default()),
        Some(store_a),
    ))
    .await;
    let err_a = ws1296_admit(
        &mut mixed_public,
        ws1296_create(Some("resp_closed_ws"), None),
    )
    .await
    .expect_err("mixed OpenAI public WS must not rewrite unknown previous_response_id");
    assert_eq!(err_a.status, 503);
    assert_eq!(err_a.code, "upstream_affinity_unavailable");
    mixed_public.close();

    // Control B: live previous_response_id owner + unknown encrypted_content must return 503.
    let store_b = Arc::new(MemoryAffinityStore::default());
    let mut live_oauth = ws1296_gateway(lifecycle(
        vec![openai_oauth_upstream_record(oauth_id, "oauth-main", true)],
        Arc::new(RecordingDispatch::default()),
        Some(store_b),
    ))
    .await;
    let turn = ws1296_admit(&mut live_oauth, ws1296_create(None, None))
        .await
        .expect("initial turn admits")
        .expect("initial turn dispatches");
    ws1296_send(&mut live_oauth, &turn, 1).await;
    ws1296_assert_raw_forward(
        &live_oauth
            .provider_message(1, &ws1296_created("resp_live"))
            .await
            .expect("created handled"),
    );
    ws1296_assert_raw_forward(
        &live_oauth
            .provider_message(1, &ws1296_completed("resp_live", vec![]))
            .await
            .expect("completed handled"),
    );
    let err_b = ws1296_admit(
        &mut live_oauth,
        ws1296_create(Some("resp_live"), Some("unknown-foreign-reasoning")),
    )
    .await
    .expect_err("live previous_response_id must not mask unknown encrypted_content");
    assert_eq!(err_b.status, 503);
    assert_eq!(err_b.code, "upstream_affinity_unavailable");
    live_oauth.close();

    // Control C: under FailClosed policy, unknown encrypted_content without
    // previous_response_id or with empty previous_response_id must return 503
    // on an OAuth-only pool.
    let store_c = Arc::new(MemoryAffinityStore::default());
    let mut oauth_only = ws1296_gateway(lifecycle_with_config(
        vec![openai_oauth_upstream_record(oauth_id, "oauth-main", true)],
        Arc::new(RecordingDispatch::default()),
        Some(store_c),
        LifecycleConfig {
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::FailClosed,
            ..LifecycleConfig::default()
        },
    ))
    .await;
    for previous in [None, Some("")] {
        let err_c = ws1296_admit(
            &mut oauth_only,
            ws1296_create(previous, Some("unknown-foreign-reasoning")),
        )
        .await
        .expect_err("unknown reasoning must fail closed with 503");
        assert_eq!(err_c.status, 503);
        assert_eq!(err_c.code, "upstream_affinity_unavailable");
    }
    oauth_only.close();

    // Control D: zero enabled OpenAI upstreams must not vacuously count as OAuth-only.
    let store_d = Arc::new(MemoryAffinityStore::default());
    let mut no_openai = ws1296_gateway(lifecycle(
        vec![upstream_record(anthropic_id, "anthropic-only", true)],
        Arc::new(RecordingDispatch::default()),
        Some(store_d),
    ))
    .await;
    let err_d = ws1296_admit(&mut no_openai, ws1296_create(Some("resp_closed_ws"), None))
        .await
        .expect_err("empty OpenAI pool must not return previous_response_not_found");
    assert_eq!(err_d.status, 503);
    assert_eq!(err_d.code, "upstream_affinity_unavailable");
    no_openai.close();
}

#[tokio::test]
async fn ws_native_codex_oauth_session_rejects_stale_and_cross_lane_previous_response() {
    let oauth_id = Uuid::from_u128(0x138231);
    let api_key_id = Uuid::from_u128(0x138232);
    let store = Arc::new(MemoryAffinityStore::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(oauth_id, "oauth-main", true),
            openai_upstream_record(api_key_id, "openai-api-key", true),
        ],
        Arc::new(RecordingDispatch::default()),
        Some(store),
    ))
    .await
    .with_codex_backend(true);

    let lane_a_create = Bytes::from(
        json!({
            "type": "response.create",
            "stream_id": "a",
            "model": "gpt-5-codex",
            "store": false,
            "input": [{"role": "user", "content": "hello"}],
        })
        .to_string(),
    );
    let turn = ws1296_admit(&mut gateway, lane_a_create)
        .await
        .expect("lane a turn admits")
        .expect("lane a turn dispatches");
    assert_eq!(turn.candidates.len(), 1);
    assert_eq!(turn.candidates[0].upstream_id, oauth_id);
    ws1296_send(&mut gateway, &turn, 1).await;

    let created = Bytes::from(
        json!({
            "type": "response.created",
            "stream_id": "a",
            "sequence_number": 0,
            "response": {
                "id": "resp_lane_a",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "in_progress",
                "store": false,
                "output": [],
                "usage": null,
            },
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &created)
            .await
            .expect("lane a created forwards"),
    );
    let completed = Bytes::from(
        json!({
            "type": "response.completed",
            "stream_id": "a",
            "sequence_number": 1,
            "response": {
                "id": "resp_lane_a",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "completed",
                "store": false,
                "output": [],
                "usage": null,
            },
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &completed)
            .await
            .expect("lane a completed forwards"),
    );

    let cross_lane = Bytes::from(
        json!({
            "type": "response.create",
            "stream_id": "b",
            "previous_response_id": "resp_lane_a",
            "model": "gpt-5-codex",
            "store": false,
            "input": [{"role": "user", "content": "cross lane"}],
        })
        .to_string(),
    );
    let cross_err = ws1296_admit(&mut gateway, cross_lane)
        .await
        .expect_err("cross-lane previous_response_id must be rejected");
    assert_eq!(cross_err.status, 400);
    assert_eq!(cross_err.code, "previous_response_not_found");
    assert_eq!(
        cross_err.message,
        "The previous response belongs to another WebSocket lane"
    );

    let stale = Bytes::from(
        json!({
            "type": "response.create",
            "stream_id": "a",
            "previous_response_id": "resp_evicted",
            "model": "gpt-5-codex",
            "store": false,
            "input": [{"role": "user", "content": "stale"}],
        })
        .to_string(),
    );
    let stale_err = ws1296_admit(&mut gateway, stale)
        .await
        .expect_err("missing previous_response_id on active OAuth WS must return 400");
    assert_eq!(stale_err.status, 400);
    assert_eq!(stale_err.code, "previous_response_not_found");
    assert_eq!(
        stale_err.message,
        "The previous response is not available on this connection"
    );
    gateway.close();
}

fn openai_reasoning_input(ciphertext: &str) -> Value {
    json!({
        "type": "reasoning",
        "id": "rs_item",
        "summary": [],
        "encrypted_content": ciphertext,
    })
}

fn openai_invalid_encrypted_error(code: &str, message: &str) -> Value {
    json!({
        "error": {
            "type": "invalid_request_error",
            "code": code,
            "message": message,
        }
    })
}

async fn ws_hybrid_send(
    gateway: &mut cc_lb_engine::responses_websocket::ResponsesWebSocketGateway,
    dispatch: &cc_lb_engine::responses_websocket::WebSocketDispatch,
    connection_id: u64,
    upstream_id: Uuid,
    upstream_name: &str,
) -> Bytes {
    gateway.assign_connection(dispatch.turn_id, connection_id, upstream_id, upstream_name);
    gateway
        .validate_send(dispatch.turn_id)
        .await
        .expect("owner remains eligible at send");
    let frame = gateway
        .frame_for_send(dispatch)
        .expect("dispatch frame shapes cleanly");
    assert!(gateway.sent(dispatch.turn_id).is_empty());
    frame
}

#[tokio::test]
async fn hybrid_http_multi_upstream_strips_unknown_reasoning_upfront_preserves_include_and_never_binds()
 {
    let upstream_a = Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap();
    let upstream_b = Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_multi",
        "object": "response",
        "status": "completed",
        "output": [],
        "usage": {"input_tokens": 5, "output_tokens": 3}
    })));
    let lifecycle = lifecycle(
        vec![
            openai_upstream_record(upstream_a, "openai-a", true),
            openai_upstream_record(upstream_b, "openai-b", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    );

    let foreign = "gAAAAABforeign_multi_http";
    let req = responses_request(json!({
        "model": "gpt-5-codex",
        "stream": false,
        "include": ["reasoning.encrypted_content"],
        "input": [
            openai_reasoning_input(foreign),
            {"role": "user", "content": "hello"},
        ]
    }));
    let resp = handle_authenticated(&lifecycle, req)
        .await
        .expect("multi-upstream hybrid strips upfront without 503");
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = collect_body(resp).await;

    let sent = dispatch.captured_requests();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].1["include"],
        json!(["reasoning.encrypted_content"]),
        "include list must remain intact when stripping reasoning items"
    );
    let sent_input = sent[0].1["input"].as_array().unwrap();
    assert_eq!(sent_input.len(), 1);
    assert_eq!(sent_input[0]["role"], "user");
    assert_eq!(store.upstream_for(&ws1296_openai_key(foreign)), None);
}

#[tokio::test]
async fn hybrid_http_single_upstream_verbatim_binds_and_retries_invalid_encrypted_content_once() {
    let only_upstream = Uuid::parse_str("10000000-0000-0000-0000-000000000011").unwrap();
    let accepted_cipher = "gAAAAABsingle_verbatim_ok";
    let rejected_plain = "gAAAAABsingle_foreign_plain";
    let rejected_gzip = "gAAAAABsingle_foreign_gzip";
    let store = Arc::new(MemoryAffinityStore::default());
    let dispatch = RecordingDispatch::with_responses(vec![
        // 1) Verbatim match accepted on first attempt.
        ResponseSpec::Json(json!({
            "id": "resp_verbatim_ok",
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 5, "output_tokens": 3}
        })),
        // 2) Plain 400 invalid_encrypted_content -> retry once stripped -> 200 OK.
        ResponseSpec::Status(
            StatusCode::BAD_REQUEST,
            openai_invalid_encrypted_error(
                "invalid_encrypted_content",
                "The encrypted content for item rs_item could not be verified.",
            ),
        ),
        ResponseSpec::Json(json!({
            "id": "resp_plain_retry_ok",
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 5, "output_tokens": 3}
        })),
        // 3) Gzip-encoded 400 invalid_encrypted_content -> retry once stripped -> 200 OK.
        ResponseSpec::GzipStatus(
            StatusCode::BAD_REQUEST,
            openai_invalid_encrypted_error(
                "invalid_encrypted_content",
                "Encrypted content organization_id did not match the target organization",
            ),
        ),
        ResponseSpec::Json(json!({
            "id": "resp_gzip_retry_ok",
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 5, "output_tokens": 3}
        })),
        // 4) Non-matching 400 invalid_value must NOT retry even if message mentions encrypted_content.
        ResponseSpec::Status(
            StatusCode::BAD_REQUEST,
            openai_invalid_encrypted_error(
                "invalid_value",
                "Invalid encrypted_content must be omitted for unsupported model flag",
            ),
        ),
    ]);
    let lifecycle = lifecycle(
        vec![openai_upstream_record(only_upstream, "openai-only", true)],
        dispatch.clone(),
        Some(store.clone()),
    );

    let make_req = |cipher: &str| {
        responses_request(json!({
            "model": "gpt-5-codex",
            "stream": false,
            "include": ["reasoning.encrypted_content"],
            "input": [
                openai_reasoning_input(cipher),
                {"role": "user", "content": "continue"},
            ]
        }))
    };

    let r1 = handle_authenticated(&lifecycle, make_req(accepted_cipher))
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let _ = collect_body(r1).await;
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(accepted_cipher)),
        Some(only_upstream)
    );

    let r2 = handle_authenticated(&lifecycle, make_req(rejected_plain))
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let _ = collect_body(r2).await;
    assert_eq!(store.upstream_for(&ws1296_openai_key(rejected_plain)), None);

    let r3 = handle_authenticated(&lifecycle, make_req(rejected_gzip))
        .await
        .unwrap();
    assert_eq!(r3.status(), StatusCode::OK);
    let _ = collect_body(r3).await;
    assert_eq!(store.upstream_for(&ws1296_openai_key(rejected_gzip)), None);

    let r4 = handle_authenticated(&lifecycle, make_req("gAAAAABnon_matching_400"))
        .await
        .unwrap();
    assert_eq!(r4.status(), StatusCode::BAD_REQUEST);
    let _ = collect_body(r4).await;

    let sent = dispatch.captured_requests();
    assert_eq!(sent.len(), 6);
    assert_eq!(sent[0].1["input"].as_array().unwrap().len(), 2);
    assert_eq!(sent[1].1["input"].as_array().unwrap().len(), 2);
    assert_eq!(sent[2].1["input"].as_array().unwrap().len(), 1);
    assert_eq!(sent[2].1["include"], json!(["reasoning.encrypted_content"]));
    assert_eq!(sent[3].1["input"].as_array().unwrap().len(), 2);
    assert_eq!(sent[4].1["input"].as_array().unwrap().len(), 1);
    assert_eq!(sent[5].1["input"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn hybrid_http_turn2_mixed_history_preserves_owned_and_prunes_foreign_on_retry() {
    let owner = Uuid::parse_str("10000000-0000-0000-0000-000000000021").unwrap();
    let peer = Uuid::parse_str("10000000-0000-0000-0000-000000000022").unwrap();
    let owned_cipher = "gAAAAABowned_turn1";
    let foreign_cipher = "gAAAAABforeign_turn2";
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_cipher), owner);

    let dispatch = RecordingDispatch::with_responses(vec![
        ResponseSpec::Status(
            StatusCode::BAD_REQUEST,
            openai_invalid_encrypted_error(
                "invalid_encrypted_content",
                "The encrypted content for item rs_foreign could not be verified.",
            ),
        ),
        ResponseSpec::Json(json!({
            "id": "resp_turn2_ok",
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 5, "output_tokens": 3}
        })),
    ]);
    let lifecycle = lifecycle(
        vec![
            openai_upstream_record(owner, "owner", true),
            openai_upstream_record(peer, "peer", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    );

    let req = responses_request(json!({
        "model": "gpt-5-codex",
        "stream": false,
        "include": ["reasoning.encrypted_content"],
        "input": [
            openai_reasoning_input(foreign_cipher),
            openai_reasoning_input(owned_cipher),
            {"role": "user", "content": "turn 2"},
        ]
    }));
    let resp = handle_authenticated(&lifecycle, req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = collect_body(resp).await;

    let sent = dispatch.captured_requests();
    assert_eq!(sent.len(), 2);
    assert!(sent[0].0.contains("owner.local"));
    assert!(sent[1].0.contains("owner.local"));
    assert_eq!(sent[0].1["input"].as_array().unwrap().len(), 3);
    let retry_input = sent[1].1["input"].as_array().unwrap();
    assert_eq!(retry_input.len(), 2);
    assert_eq!(retry_input[0]["encrypted_content"], owned_cipher);
    assert_eq!(retry_input[1]["role"], "user");
    assert_eq!(store.upstream_for(&ws1296_openai_key(foreign_cipher)), None);
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(owned_cipher)),
        Some(owner)
    );
}

#[tokio::test]
async fn hybrid_ws_multi_single_and_turn2_mixed_reuse_connection_and_preserve_boundaries() {
    let oauth_a = Uuid::parse_str("10000000-0000-0000-0000-000000000031").unwrap();
    let oauth_b = Uuid::parse_str("10000000-0000-0000-0000-000000000032").unwrap();

    // Part A: multi-upstream WS strips upfront, preserves include, and binds nothing.
    let store_a = Arc::new(MemoryAffinityStore::default());
    let mut gw_multi = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(oauth_a, "oauth-a", true),
            openai_oauth_upstream_record(oauth_b, "oauth-b", true),
        ],
        Arc::new(RecordingDispatch::default()),
        Some(store_a.clone()),
    ))
    .await;
    let foreign_multi = "gAAAAABws_multi_foreign";
    let d_multi = ws1296_admit(&mut gw_multi, ws1296_create(None, Some(foreign_multi)))
        .await
        .expect("WS multi-upstream strips upfront")
        .expect("dispatch queued");
    let parsed_multi: Value = serde_json::from_slice(&d_multi.frame).unwrap();
    assert_eq!(parsed_multi["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        parsed_multi["include"],
        json!(["reasoning.encrypted_content"])
    );
    let routed_multi = d_multi.candidates.first().unwrap().clone();
    ws_hybrid_send(
        &mut gw_multi,
        &d_multi,
        10,
        routed_multi.upstream_id,
        &routed_multi.upstream_name,
    )
    .await;
    ws1296_assert_raw_forward(
        &gw_multi
            .provider_message(10, &ws1296_created("resp_ws_multi"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &gw_multi
            .provider_message(10, &ws1296_completed("resp_ws_multi", vec![]))
            .await
            .unwrap(),
    );
    assert_eq!(
        store_a.upstream_for(&ws1296_openai_key(foreign_multi)),
        None
    );
    gw_multi.close();

    // Part B: single-upstream WS sends verbatim first, retries on same connection_id on
    // pre-creation invalid_encrypted_content, and keeps foreign reasoning unbound.
    let store_b = Arc::new(MemoryAffinityStore::default());
    let mut gw_single = ws1296_gateway(lifecycle(
        vec![openai_oauth_upstream_record(oauth_a, "oauth-a", true)],
        Arc::new(RecordingDispatch::default()),
        Some(store_b.clone()),
    ))
    .await;
    let foreign_single = "gAAAAABws_single_foreign";
    let d_single = ws1296_admit(&mut gw_single, ws1296_create(None, Some(foreign_single)))
        .await
        .expect("WS single-upstream admits verbatim")
        .expect("dispatch queued");
    let sent_frame_1 = ws_hybrid_send(&mut gw_single, &d_single, 77, oauth_a, "oauth-a").await;
    let sent_json_1: Value = serde_json::from_slice(&sent_frame_1).unwrap();
    assert_eq!(sent_json_1["input"].as_array().unwrap().len(), 2);

    let ws_err = Bytes::from(
        json!({
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_encrypted_content",
                "message": "The encrypted content for item rs_ws1296 could not be verified."
            }
        })
        .to_string(),
    );
    let err_res = gw_single
        .provider_message(77, &ws_err)
        .await
        .expect("WS error triggers retry instead of failing turn");
    assert!(!err_res.forward);
    assert!(err_res.errors.is_empty());
    assert_eq!(err_res.dispatches.len(), 1);
    let retry_dispatch = &err_res.dispatches[0];
    assert_eq!(retry_dispatch.connection_id, Some(77));
    assert!(retry_dispatch.candidates.is_empty());
    let sent_frame_2 = ws_hybrid_send(&mut gw_single, retry_dispatch, 77, oauth_a, "oauth-a").await;
    let sent_json_2: Value = serde_json::from_slice(&sent_frame_2).unwrap();
    assert_eq!(sent_json_2["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        sent_json_2["include"],
        json!(["reasoning.encrypted_content"])
    );
    ws1296_assert_raw_forward(
        &gw_single
            .provider_message(77, &ws1296_created("resp_ws_retry_ok"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &gw_single
            .provider_message(77, &ws1296_completed("resp_ws_retry_ok", vec![]))
            .await
            .unwrap(),
    );
    assert!(!gw_single.has_active_turns());
    assert_eq!(
        store_b.upstream_for(&ws1296_openai_key(foreign_single)),
        None
    );

    // Part C: single-upstream WS verbatim match binds the accepted reasoning key.
    let accepted_ws = "gAAAAABws_single_accepted";
    let d_acc = ws1296_admit(&mut gw_single, ws1296_create(None, Some(accepted_ws)))
        .await
        .expect("WS verbatim accepted")
        .expect("dispatch queued");
    ws_hybrid_send(&mut gw_single, &d_acc, 77, oauth_a, "oauth-a").await;
    ws1296_assert_raw_forward(
        &gw_single
            .provider_message(77, &ws1296_created("resp_ws_acc_ok"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &gw_single
            .provider_message(77, &ws1296_completed("resp_ws_acc_ok", vec![]))
            .await
            .unwrap(),
    );
    assert_eq!(
        store_b.upstream_for(&ws1296_openai_key(accepted_ws)),
        Some(oauth_a)
    );
    gw_single.close();

    // Part D: Turn 2 WS mixed history [foreign, owned, user] pins to owner, retries once
    // stripping only foreign while keeping owned intact on live connection_id=88.
    let store_d = Arc::new(MemoryAffinityStore::default());
    let owned_ws = "gAAAAABws_turn1_owned";
    let foreign_turn2 = "gAAAAABws_turn2_foreign";
    store_d.insert(ws1296_openai_key(owned_ws), oauth_a);
    let mut gw_mixed = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(oauth_a, "oauth-a", true),
            openai_oauth_upstream_record(oauth_b, "oauth-b", true),
        ],
        Arc::new(RecordingDispatch::default()),
        Some(store_d.clone()),
    ))
    .await;
    let mixed_frame = Bytes::from(
        json!({
            "type": "response.create",
            "model": "gpt-5-codex",
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "input": [
                ws1296_reasoning_item(foreign_turn2),
                ws1296_reasoning_item(owned_ws),
                {"role": "user", "content": "turn 2"},
            ]
        })
        .to_string(),
    );
    let d_mixed = ws1296_admit(&mut gw_mixed, mixed_frame)
        .await
        .expect("Turn 2 mixed WS admits")
        .expect("dispatch queued");
    assert_eq!(d_mixed.candidates[0].upstream_id, oauth_a);
    let sent_mixed_1 = ws_hybrid_send(&mut gw_mixed, &d_mixed, 88, oauth_a, "oauth-a").await;
    let json_mixed_1: Value = serde_json::from_slice(&sent_mixed_1).unwrap();
    assert_eq!(json_mixed_1["input"].as_array().unwrap().len(), 3);

    let err_res_mixed = gw_mixed.provider_message(88, &ws_err).await.unwrap();
    assert_eq!(err_res_mixed.dispatches.len(), 1);
    assert_eq!(err_res_mixed.dispatches[0].connection_id, Some(88));
    let sent_mixed_2 = ws_hybrid_send(
        &mut gw_mixed,
        &err_res_mixed.dispatches[0],
        88,
        oauth_a,
        "oauth-a",
    )
    .await;
    let json_mixed_2: Value = serde_json::from_slice(&sent_mixed_2).unwrap();
    let input_mixed_2 = json_mixed_2["input"].as_array().unwrap();
    assert_eq!(input_mixed_2.len(), 2);
    assert_eq!(input_mixed_2[0]["encrypted_content"], owned_ws);
    ws1296_assert_raw_forward(
        &gw_mixed
            .provider_message(88, &ws1296_created("resp_ws_mixed_ok"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &gw_mixed
            .provider_message(88, &ws1296_completed("resp_ws_mixed_ok", vec![]))
            .await
            .unwrap(),
    );
    assert_eq!(
        store_d.upstream_for(&ws1296_openai_key(foreign_turn2)),
        None
    );
    assert_eq!(
        store_d.upstream_for(&ws1296_openai_key(owned_ws)),
        Some(oauth_a)
    );
    gw_mixed.close();
}

#[tokio::test]
async fn hybrid_negative_controls_keep_fail_closed_boundaries() {
    let openai_id = Uuid::parse_str("10000000-0000-0000-0000-000000000041").unwrap();
    let disabled_owner = Uuid::parse_str("10000000-0000-0000-0000-000000000042").unwrap();
    let anthropic_id = Uuid::parse_str("10000000-0000-0000-0000-000000000043").unwrap();
    let gemini_id = Uuid::parse_str("10000000-0000-0000-0000-000000000044").unwrap();
    let owned_reasoning = "gAAAAABdisabled_owner_reasoning";

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_reasoning), disabled_owner);
    let shared_dispatch = Arc::new(RecordingDispatch::default());
    let lc = lifecycle(
        vec![
            openai_upstream_record(openai_id, "openai-main", true),
            openai_upstream_record(disabled_owner, "openai-disabled", false),
            upstream_record(anthropic_id, "anthropic-main", true),
            gemini_upstream_record(gemini_id, "gemini-main", true),
        ],
        shared_dispatch.clone(),
        Some(store),
    );

    let cases: Vec<Request<Bytes>> = vec![
        // Compaction encrypted item (even alongside reasoning) stays fail-closed.
        responses_request(json!({
            "model": "gpt-5-codex",
            "input": [
                openai_reasoning_input("gAAAAABunknown_rs"),
                {"type": "compaction", "id": "cmp_1", "encrypted_content": "gAAAAABunknown_cmp"},
            ]
        })),
        // compaction_summary encrypted item stays fail-closed.
        responses_request(json!({
            "model": "gpt-5-codex",
            "input": [
                {"type": "compaction_summary", "id": "cmp_2", "encrypted_content": "gAAAAABunknown_cmps"},
            ]
        })),
        // Unknown conversation stays fail-closed even with reasoning.
        responses_request(json!({
            "model": "gpt-5-codex",
            "conversation": "conv_unknown_hybrid",
            "input": [openai_reasoning_input("gAAAAABunknown_rs")]
        })),
        // Unknown HTTP previous_response_id stays fail-closed even with reasoning.
        responses_request(json!({
            "model": "gpt-5-codex",
            "previous_response_id": "resp_unknown_hybrid",
            "input": [openai_reasoning_input("gAAAAABunknown_rs")]
        })),
        // Disabled known owner stays fail-closed (no cross-upstream failover).
        responses_request(json!({
            "model": "gpt-5-codex",
            "input": [openai_reasoning_input(owned_reasoning)]
        })),
        // Anthropic web-search encrypted_content stays fail-closed.
        messages_request(opaque_request(&["anthropic-ws-cipher"], false)),
        // Gemini cachedContent stays fail-closed.
        provider_request(
            cc_lb_domain::Provider::Gemini,
            "/v1beta/models/gemini-2.5-pro:generateContent",
            json!({
                "cachedContent": "cachedContents/unknown",
                "contents": [{"parts": [{"text": "hi"}]}]
            }),
        ),
    ];

    for (idx, req) in cases.into_iter().enumerate() {
        let resp = handle_authenticated(&lc, req)
            .await
            .unwrap_or_else(|e| panic!("case {idx} should produce HTTP 503 response, got {e:?}"));
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "case {idx} must fail closed with 503"
        );
    }
    assert!(
        shared_dispatch.captured_requests().is_empty(),
        "fail-closed negative controls must never dispatch upstream"
    );

    // Explicit FailClosed policy rejects unknown reasoning with 503 on both HTTP and WS.
    let fail_closed_lc = lifecycle_with_config(
        vec![openai_oauth_upstream_record(openai_id, "oauth-main", true)],
        shared_dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
        LifecycleConfig {
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::FailClosed,
            ..LifecycleConfig::default()
        },
    );
    let http_fc = handle_authenticated(
        &fail_closed_lc,
        responses_request(json!({
            "model": "gpt-5-codex",
            "input": [openai_reasoning_input("gAAAAABfc_http")]
        })),
    )
    .await
    .unwrap();
    assert_eq!(http_fc.status(), StatusCode::SERVICE_UNAVAILABLE);

    let mut gw_fc = ws1296_gateway(fail_closed_lc).await;
    let ws_fc_err = ws1296_admit(&mut gw_fc, ws1296_create(None, Some("gAAAAABfc_ws")))
        .await
        .expect_err("FailClosed WS rejects unknown reasoning with 503");
    assert_eq!(ws_fc_err.status, 503);
    assert_eq!(ws_fc_err.code, "upstream_affinity_unavailable");
    gw_fc.close();
}

#[tokio::test]
async fn hybrid_ws_rejects_non_400_and_post_queued_errors_without_retry() {
    let oauth_a = Uuid::parse_str("10000000-0000-0000-0000-000000000051").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![openai_oauth_upstream_record(oauth_a, "oauth-a", true)],
        Arc::new(RecordingDispatch::default()),
        Some(store.clone()),
    ))
    .await;

    // 1) Non-400 (500, 429) or statusless WS error frames with code="invalid_encrypted_content"
    // must be forwarded without retry or replay.
    for (idx, status_field) in [Some(500u64), Some(429u64), None].into_iter().enumerate() {
        let foreign = format!("gAAAAABws_non400_{idx}");
        let dispatch = ws1296_admit(&mut gateway, ws1296_create(None, Some(&foreign)))
            .await
            .expect("WS single-upstream admits")
            .expect("dispatch queued");
        ws_hybrid_send(&mut gateway, &dispatch, 90 + idx as u64, oauth_a, "oauth-a").await;
        let mut err_frame = json!({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_encrypted_content",
                "message": "The encrypted content could not be verified."
            }
        });
        if let Some(status) = status_field {
            err_frame["status"] = json!(status);
        }
        let res = gateway
            .provider_message(90 + idx as u64, &Bytes::from(err_frame.to_string()))
            .await
            .expect("non-400 error forwards cleanly");
        assert!(res.forward, "case {idx} must forward original error frame");
        assert!(
            res.dispatches.is_empty(),
            "case {idx} must not schedule a retry dispatch"
        );
        assert!(res.errors.is_empty());
        assert!(!gateway.has_active_turns());
        assert_eq!(store.upstream_for(&ws1296_openai_key(&foreign)), None);
    }

    // 2) Once response.queued has been forwarded for the turn, a subsequent 400
    // invalid_encrypted_content error must NOT retry (response identity is already live).
    let foreign_queued = "gAAAAABws_post_queued";
    let dispatch = ws1296_admit(&mut gateway, ws1296_create(None, Some(foreign_queued)))
        .await
        .expect("WS turn admits")
        .expect("dispatch queued");
    ws_hybrid_send(&mut gateway, &dispatch, 99, oauth_a, "oauth-a").await;

    let queued_frame = Bytes::from(
        json!({
            "type": "response.queued",
            "sequence_number": 0,
            "response": {
                "id": "resp_queued_live",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "queued",
                "store": false,
                "output": [],
                "usage": null
            }
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(99, &queued_frame)
            .await
            .expect("response.queued forwards"),
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(foreign_queued)),
        None,
        "response.queued must not bind pending unverified input reasoning"
    );

    let err_400 = Bytes::from(
        json!({
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_encrypted_content",
                "message": "The encrypted content for item rs_ws1296 could not be verified."
            }
        })
        .to_string(),
    );
    let post_queued_res = gateway
        .provider_message(99, &err_400)
        .await
        .expect("post-queued error forwards without panic");
    assert!(
        post_queued_res.forward,
        "post-queued 400 must forward rather than re-creating after response.queued"
    );
    assert!(
        post_queued_res.dispatches.is_empty(),
        "post-queued 400 must not retry"
    );
    assert!(!gateway.has_active_turns());
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(foreign_queued)),
        None,
        "rejected post-queued turn must leave foreign reasoning unbound"
    );

    // 3) Same for response.in_progress followed by 400 invalid_encrypted_content.
    let foreign_in_progress = "gAAAAABws_post_in_progress";
    let dispatch_ip = ws1296_admit(&mut gateway, ws1296_create(None, Some(foreign_in_progress)))
        .await
        .expect("WS turn admits")
        .expect("dispatch queued");
    ws_hybrid_send(&mut gateway, &dispatch_ip, 100, oauth_a, "oauth-a").await;
    let in_progress_frame = Bytes::from(
        json!({
            "type": "response.in_progress",
            "sequence_number": 1,
            "response": {
                "id": "resp_in_progress_live",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "in_progress",
                "store": false,
                "output": [],
                "usage": null
            }
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(100, &in_progress_frame)
            .await
            .expect("response.in_progress forwards"),
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(foreign_in_progress)),
        None,
        "response.in_progress must not bind pending unverified input reasoning"
    );
    let post_ip_res = gateway
        .provider_message(100, &err_400)
        .await
        .expect("post-in_progress error forwards without panic");
    assert!(
        post_ip_res.forward,
        "post-in_progress 400 must forward without retrying"
    );
    assert!(
        post_ip_res.dispatches.is_empty(),
        "post-in_progress 400 must not retry"
    );
    assert!(!gateway.has_active_turns());
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(foreign_in_progress)),
        None,
        "rejected post-in_progress turn must leave foreign reasoning unbound"
    );
    gateway.close();
}
