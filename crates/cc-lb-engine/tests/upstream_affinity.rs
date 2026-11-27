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
    StatusWithHeaders(StatusCode, Vec<(&'static str, &'static str)>, Value),
    OpenBreaker,
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
        let mut extra_headers: Vec<(&'static str, &'static str)> = Vec::new();
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
            ResponseSpec::StatusWithHeaders(response_status, headers, value) => {
                status = response_status;
                extra_headers = headers;
                (
                    "application/json",
                    None,
                    Body::from(Bytes::from(value.to_string())),
                )
            }
            ResponseSpec::OpenBreaker => {
                return Err(DispatchError::Transport {
                    source: Box::new(cc_lb_engine::BreakerError::Open {
                        retry_after: Duration::from_secs(30),
                    }),
                });
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
        for (name, value) in extra_headers {
            response.headers_mut().insert(
                http::header::HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }
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
    expected_spec_revisions: Mutex<HashMap<Uuid, u64>>,
    read_epoch_overrides: Mutex<HashMap<Uuid, u64>>,
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

    fn set_spec_revision(&self, upstream_id: Uuid, spec_revision: u64) {
        self.expected_spec_revisions
            .lock()
            .expect("spec revision lock")
            .insert(upstream_id, spec_revision);
    }

    fn set_read_epoch_override(&self, upstream_id: Uuid, epoch: u64) {
        self.read_epoch_overrides
            .lock()
            .expect("read epoch override lock")
            .insert(upstream_id, epoch);
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
        expected_spec_revision: u64,
    ) -> StorageResult<Option<u64>> {
        if self
            .expected_spec_revisions
            .lock()
            .expect("spec revision lock")
            .get(&upstream_id)
            .is_some_and(|current| *current != expected_spec_revision)
        {
            return Ok(None);
        }
        let override_epoch = self
            .read_epoch_overrides
            .lock()
            .expect("read epoch override lock")
            .get(&upstream_id)
            .copied();
        Ok(Some(
            override_epoch.unwrap_or_else(|| self.owner_epoch(upstream_id)),
        ))
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
    assert_eq!(expired.status(), StatusCode::CONFLICT);
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

    let disabled_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp-fallback-follow-up",
        "object": "response",
        "output": []
    })));
    let disabled_owner = lifecycle(
        vec![
            openai_upstream_record(origin, "origin", false),
            openai_upstream_record(fallback, "fallback", true),
        ],
        disabled_dispatch.clone(),
        Some(store.clone()),
    );
    let failover_response =
        handle_authenticated(&disabled_owner, responses_request(reasoning_replay.clone()))
            .await
            .expect("disabled owner replay strips and fails over by default");
    assert_eq!(failover_response.status(), StatusCode::OK);
    assert_eq!(
        disabled_dispatch
            .urls
            .lock()
            .expect("dispatch URL lock")
            .as_slice(),
        &["http://fallback.local/v1/responses".to_owned()]
    );
    let captured_failover = disabled_dispatch.captured_requests();
    assert_eq!(captured_failover.len(), 1);
    let forwarded_payload = &captured_failover[0].1;
    assert_eq!(forwarded_payload["input"].as_array().unwrap().len(), 1);
    assert_eq!(forwarded_payload["input"][0]["role"], "user");

    let fail_closed_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let fail_closed_owner = lifecycle_with_config(
        vec![
            openai_upstream_record(origin, "origin", false),
            openai_upstream_record(fallback, "fallback", true),
        ],
        fail_closed_dispatch.clone(),
        Some(store),
        LifecycleConfig {
            unavailable_openai_reasoning_owner:
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::FailClosed,
            ..LifecycleConfig::default()
        },
    );
    let disabled_response =
        handle_authenticated(&fail_closed_owner, responses_request(reasoning_replay))
            .await
            .expect("disabled owner replay handled under FailClosed");
    assert_eq!(disabled_response.status(), StatusCode::CONFLICT);
    assert_eq!(fail_closed_dispatch.call_count(), 0);
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

    assert_eq!(response.status(), StatusCode::CONFLICT);
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
        StatusCode::CONFLICT
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
    assert_eq!(target_response.status(), StatusCode::CONFLICT);
    assert_eq!(target_dispatch.call_count(), 0);
}

/// #851: an affinity target whose apply status is `ApplyStatus::Error` is no
/// longer a routing candidate, so the pinned request fails closed with 409
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

    assert_eq!(status, StatusCode::CONFLICT);
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
        .update(
            old_record.id,
            old_record.revision,
            cc_lb_storage_api::UpstreamUpdate {
                api_key_ciphertext: Some(vec![2]),
                ..Default::default()
            },
        )
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
async fn ws1296_legacy_reasoning_owner_disabled_fails_closed_when_configured() {
    let owner = Uuid::from_u128(0x129601);
    let fallback = Uuid::from_u128(0x129602);
    let store = Arc::new(MemoryAffinityStore::default());
    // Existing rows must remain usable without a new kind or a migration.
    store.insert(ws1296_openai_key(WS1296_REASONING), owner);
    let records = vec![
        openai_upstream_record(owner, "owner", false),
        openai_upstream_record(fallback, "fallback", true),
    ];
    let config = LifecycleConfig {
        unavailable_openai_reasoning_owner:
            cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::FailClosed,
        ..LifecycleConfig::default()
    };
    let dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle_with_config(
        records.clone(),
        dispatch.clone(),
        Some(store.clone()),
        config.clone(),
    ))
    .await;
    let error = ws1296_admit(&mut gateway, ws1296_create(None, Some(WS1296_REASONING)))
        .await
        .expect_err("disabled reasoning owner must not fall back under FailClosed");
    assert_eq!(error.status, 409);
    assert_eq!(error.code, "upstream_affinity_unavailable");
    let mut body: Value =
        serde_json::from_slice(&ws1296_create(None, Some(WS1296_REASONING))).unwrap();
    body.as_object_mut().unwrap().remove("type");
    let http = lifecycle_with_config(records, dispatch.clone(), Some(store), config);
    let response = handle_authenticated(&http, responses_request(body))
        .await
        .expect("disabled owner HTTP continuation is handled");
    assert_eq!(response.status(), StatusCode::CONFLICT);
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
async fn ws_previous_response_fallback_controls_preserve_409_boundaries() {
    let oauth_id = Uuid::from_u128(0x138221);
    let api_key_id = Uuid::from_u128(0x138222);
    let anthropic_id = Uuid::from_u128(0x138223);

    // Control A: mixed OpenAI pool on public /v1/responses must keep 409.
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
    assert_eq!(err_a.status, 409);
    assert_eq!(err_a.code, "upstream_affinity_unavailable");
    mixed_public.close();

    // Control B: live previous_response_id owner + unknown encrypted_content must return 409.
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
    assert_eq!(err_b.status, 409);
    assert_eq!(err_b.code, "upstream_affinity_unavailable");
    live_oauth.close();

    // Control C: under FailClosed policy, unknown encrypted_content without
    // previous_response_id or with empty previous_response_id must return 409
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
        .expect_err("unknown reasoning must fail closed with 409");
        assert_eq!(err_c.status, 409);
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
    assert_eq!(err_d.status, 409);
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
        // Disabled known owner + compaction item stays fail-closed (only pure reasoning strips & fails over).
        responses_request(json!({
            "model": "gpt-5-codex",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": "compaction", "id": "cmp_disabled", "encrypted_content": "gAAAAABunknown_cmp"},
            ]
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
            .unwrap_or_else(|e| panic!("case {idx} should produce HTTP 409 response, got {e:?}"));
        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "case {idx} must fail closed with 409"
        );
    }
    assert!(
        shared_dispatch.captured_requests().is_empty(),
        "fail-closed negative controls must never dispatch upstream"
    );

    // Explicit FailClosed policy rejects unknown reasoning with 409 on both HTTP and WS.
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
    assert_eq!(http_fc.status(), StatusCode::CONFLICT);

    let mut gw_fc = ws1296_gateway(fail_closed_lc).await;
    let ws_fc_err = ws1296_admit(&mut gw_fc, ws1296_create(None, Some("gAAAAABfc_ws")))
        .await
        .expect_err("FailClosed WS rejects unknown reasoning with 409");
    assert_eq!(ws_fc_err.status, 409);
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

#[tokio::test]
async fn hybrid_ws_pending_affinity_binds_clean_incomplete_and_skips_error_incomplete() {
    let oauth_a = Uuid::parse_str("10000000-0000-0000-0000-000000000052").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![openai_oauth_upstream_record(oauth_a, "oauth-a", true)],
        Arc::new(RecordingDispatch::default()),
        Some(store.clone()),
    ))
    .await;

    // 1) Clean response.incomplete (no error / null error) completes the turn cleanly and
    // must bind accepted unknown input reasoning to oauth_a.
    let clean_incomplete_cipher = "gAAAAABws_clean_incomplete";
    let d_clean = ws1296_admit(
        &mut gateway,
        ws1296_create(None, Some(clean_incomplete_cipher)),
    )
    .await
    .expect("WS single-upstream admits clean incomplete turn")
    .expect("dispatch queued");
    ws_hybrid_send(&mut gateway, &d_clean, 101, oauth_a, "oauth-a").await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(101, &ws1296_created("resp_clean_inc"))
            .await
            .unwrap(),
    );
    let clean_incomplete_frame = Bytes::from(
        json!({
            "type": "response.incomplete",
            "sequence_number": 2,
            "response": {
                "id": "resp_clean_inc",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "incomplete",
                "store": false,
                "error": null,
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [],
                "usage": {"input_tokens": 5, "output_tokens": 3}
            }
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(101, &clean_incomplete_frame)
            .await
            .expect("clean response.incomplete forwards"),
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(clean_incomplete_cipher)),
        Some(oauth_a),
        "clean response.incomplete must persist accepted input reasoning affinity"
    );

    // 2) Error-bearing response.incomplete must NOT bind pending input reasoning.
    let err_incomplete_cipher = "gAAAAABws_err_incomplete";
    let d_err_inc = ws1296_admit(
        &mut gateway,
        ws1296_create(None, Some(err_incomplete_cipher)),
    )
    .await
    .expect("WS admits turn")
    .expect("dispatch queued");
    ws_hybrid_send(&mut gateway, &d_err_inc, 102, oauth_a, "oauth-a").await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(102, &ws1296_created("resp_err_inc"))
            .await
            .unwrap(),
    );
    let err_incomplete_frame = Bytes::from(
        json!({
            "type": "response.incomplete",
            "sequence_number": 2,
            "response": {
                "id": "resp_err_inc",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "incomplete",
                "store": false,
                "error": {"code": "server_error", "message": "failed"},
                "output": [],
                "usage": null
            }
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(102, &err_incomplete_frame)
            .await
            .expect("error response.incomplete forwards"),
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(err_incomplete_cipher)),
        None,
        "error-bearing response.incomplete must leave pending input reasoning unbound"
    );

    // 3) response.failed must NOT bind pending input reasoning.
    let failed_cipher = "gAAAAABws_failed";
    let d_failed = ws1296_admit(&mut gateway, ws1296_create(None, Some(failed_cipher)))
        .await
        .expect("WS admits turn")
        .expect("dispatch queued");
    ws_hybrid_send(&mut gateway, &d_failed, 103, oauth_a, "oauth-a").await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(103, &ws1296_created("resp_failed"))
            .await
            .unwrap(),
    );
    let failed_frame = Bytes::from(
        json!({
            "type": "response.failed",
            "sequence_number": 2,
            "response": {
                "id": "resp_failed",
                "object": "response",
                "model": "gpt-5-codex",
                "status": "failed",
                "store": false,
                "error": {"code": "server_error", "message": "failed"},
                "output": [],
                "usage": null
            }
        })
        .to_string(),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(103, &failed_frame)
            .await
            .expect("response.failed forwards"),
    );
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(failed_cipher)),
        None,
        "response.failed must leave pending input reasoning unbound"
    );
    assert!(!gateway.has_active_turns());
    gateway.close();
}

#[tokio::test]
async fn strip_and_failover_disabled_reasoning_owner_http_and_ws_failover_upfront() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000101").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000102").unwrap();

    // 1. HTTP: disabled owner A strips encrypted reasoning upfront without a wasted round-trip,
    // preserves `include`, dispatches to fallback B, and binds B's new reasoning item.
    let owned_http_a = "gAAAAABhttp_owned_by_a";
    let new_http_b = "gAAAAABhttp_owned_by_b";
    let http_store = Arc::new(MemoryAffinityStore::default());
    http_store.insert(ws1296_openai_key(owned_http_a), owner_a);
    let http_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_http_b",
        "object": "response",
        "status": "completed",
        "output": [openai_reasoning_input(new_http_b)],
        "usage": {"input_tokens": 5, "output_tokens": 3}
    })));
    let http_lc = lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ],
        http_dispatch.clone(),
        Some(http_store.clone()),
    );
    let http_resp = handle_authenticated(
        &http_lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "input": [
                openai_reasoning_input(owned_http_a),
                {"role": "user", "content": "http-failover"}
            ]
        })),
    )
    .await
    .expect("HTTP failover succeeds");
    assert_eq!(http_resp.status(), StatusCode::OK);
    let captured = http_dispatch.captured_requests();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].0.contains("oauth-b.local"), "{}", captured[0].0);
    assert_eq!(
        captured[0].1["include"],
        json!(["reasoning.encrypted_content"])
    );
    let forwarded_input = captured[0].1["input"].as_array().unwrap();
    assert_eq!(forwarded_input.len(), 1);
    assert_eq!(forwarded_input[0]["content"], "http-failover");
    assert_eq!(
        http_store.upstream_for(&ws1296_openai_key(owned_http_a)),
        Some(owner_a)
    );
    assert_eq!(
        http_store.upstream_for(&ws1296_openai_key(new_http_b)),
        Some(fallback_b)
    );

    // 2. Fresh WS (`WebSocketTurnOwner::None`): disabled owner A strips upfront and binds B.
    let owned_ws_a = "gAAAAABws_owned_by_a";
    let new_ws_b = "gAAAAABws_owned_by_b";
    let ws_store = Arc::new(MemoryAffinityStore::default());
    ws_store.insert(ws1296_openai_key(owned_ws_a), owner_a);
    let mut fresh_gw = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ],
        Arc::new(RecordingDispatch::default()),
        Some(ws_store.clone()),
    ))
    .await;
    let d_fresh = ws1296_admit(&mut fresh_gw, ws1296_create(None, Some(owned_ws_a)))
        .await
        .expect("fresh WS admits")
        .expect("fresh WS dispatches");
    assert_eq!(d_fresh.candidates.len(), 1);
    assert_eq!(d_fresh.candidates[0].upstream_id, fallback_b);
    let sent_fresh: Value = serde_json::from_slice(
        &ws_hybrid_send(&mut fresh_gw, &d_fresh, 1, fallback_b, "oauth-b").await,
    )
    .unwrap();
    assert_eq!(sent_fresh["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        sent_fresh["include"],
        json!(["reasoning.encrypted_content"])
    );
    ws1296_assert_raw_forward(
        &fresh_gw
            .provider_message(1, &ws1296_created("resp_ws_fresh"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &fresh_gw
            .provider_message(
                1,
                &ws1296_completed("resp_ws_fresh", vec![ws1296_reasoning_item(new_ws_b)]),
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        ws_store.upstream_for(&ws1296_openai_key(new_ws_b)),
        Some(fallback_b)
    );
    assert_eq!(
        ws_store.upstream_for(&ws1296_openai_key(owned_ws_a)),
        Some(owner_a)
    );
    fresh_gw.close();

    // 3. Open WS `SessionHint(B)` (session pinned to healthy B carrying reasoning from disabled A):
    // must strip A's reasoning and stay on B without session-pin 409.
    let mut pinned_b_gw = ws1296_gateway(lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ],
        Arc::new(RecordingDispatch::default()),
        Some(ws_store.clone()),
    ))
    .await;
    let turn_b1 = ws1296_admit(&mut pinned_b_gw, ws1296_create(None, None))
        .await
        .unwrap()
        .unwrap();
    ws_hybrid_send(&mut pinned_b_gw, &turn_b1, 2, fallback_b, "oauth-b").await;
    ws1296_assert_raw_forward(
        &pinned_b_gw
            .provider_message(2, &ws1296_created("resp_pin_b1"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &pinned_b_gw
            .provider_message(2, &ws1296_completed("resp_pin_b1", vec![]))
            .await
            .unwrap(),
    );
    let turn_b2 = ws1296_admit(&mut pinned_b_gw, ws1296_create(None, Some(owned_ws_a)))
        .await
        .expect("SessionHint(B) + disabled A reasoning must admit cleanly")
        .expect("Turn 2 dispatches to B");
    assert_eq!(turn_b2.candidates[0].upstream_id, fallback_b);
    let sent_b2: Value = serde_json::from_slice(
        &ws_hybrid_send(&mut pinned_b_gw, &turn_b2, 2, fallback_b, "oauth-b").await,
    )
    .unwrap();
    assert_eq!(sent_b2["input"].as_array().unwrap().len(), 1);
    pinned_b_gw.close();

    // 4. Open WS `SessionHint(A)` (session pinned on A, then A disabled + B enabled via dynamic view):
    // must fail over to B with A's reasoning stripped and update `session_upstream` to B.
    let authn = TestAuthn::new(TestState::default());
    let initial_view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(fallback_b, "oauth-b", false),
        ])
        .build();
    let holder = Arc::new(DynamicViewHolder::new(initial_view));
    let dynamic_store = Arc::new(MemoryAffinityStore::default());
    let dynamic_lc = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        holder.clone(),
        Arc::new(RecordingDispatch::default()),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(dynamic_store.clone());
    let mut open_a_gw = ws1296_gateway(dynamic_lc).await;
    let turn_a1 = ws1296_admit(&mut open_a_gw, ws1296_create(None, None))
        .await
        .unwrap()
        .unwrap();
    ws_hybrid_send(&mut open_a_gw, &turn_a1, 10, owner_a, "oauth-a").await;
    ws1296_assert_raw_forward(
        &open_a_gw
            .provider_message(10, &ws1296_created("resp_open_a1"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &open_a_gw
            .provider_message(
                10,
                &ws1296_completed("resp_open_a1", vec![ws1296_reasoning_item(owned_ws_a)]),
            )
            .await
            .unwrap(),
    );
    assert_eq!(open_a_gw.session_upstream(), Some(owner_a));

    let swapped_view = DynamicViewBuilder::new(1)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ])
        .build();
    holder.store(swapped_view);

    let turn_a2 = ws1296_admit(&mut open_a_gw, ws1296_create(None, Some(owned_ws_a)))
        .await
        .expect("open SessionHint(A) with disabled A must fail over to B")
        .expect("Turn a2 dispatches to B");
    assert_eq!(turn_a2.candidates.len(), 1);
    assert_eq!(turn_a2.candidates[0].upstream_id, fallback_b);
    let sent_a2: Value = serde_json::from_slice(
        &ws_hybrid_send(&mut open_a_gw, &turn_a2, 11, fallback_b, "oauth-b").await,
    )
    .unwrap();
    assert_eq!(sent_a2["input"].as_array().unwrap().len(), 1);
    assert_eq!(open_a_gw.session_upstream(), Some(fallback_b));
    ws1296_assert_raw_forward(
        &open_a_gw
            .provider_message(11, &ws1296_created("resp_open_b2"))
            .await
            .unwrap(),
    );
    ws1296_assert_raw_forward(
        &open_a_gw
            .provider_message(
                11,
                &ws1296_completed("resp_open_b2", vec![ws1296_reasoning_item(new_ws_b)]),
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        dynamic_store.upstream_for(&ws1296_openai_key(new_ws_b)),
        Some(fallback_b)
    );
    open_a_gw.close();
}

#[tokio::test]
async fn strip_and_failover_fail_closed_boundaries_and_gemini_epoch_status_split() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000201").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000202").unwrap();
    let api_key_b = Uuid::parse_str("10000000-0000-0000-0000-000000000203").unwrap();
    let gemini_id = Uuid::parse_str("10000000-0000-0000-0000-000000000204").unwrap();

    let owned_reasoning = "gAAAAABowned_disabled_rs";
    let owned_resp = "resp_owned_disabled";
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_reasoning), owner_a);
    store.insert(ws1296_openai_key(owned_resp), owner_a);

    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let lc = lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    );

    // Non-strippable state alongside owned reasoning keeps 409 fail-closed on HTTP.
    let non_strippable_http = [
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": "compaction", "id": "cmp_1", "encrypted_content": "gAAAAABcmp_1"}
            ]
        }),
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": "compaction_summary", "id": "cmp_2", "encrypted_content": "gAAAAABcmp_2"}
            ]
        }),
        json!({
            "model": "gpt-5.4",
            "previous_response_id": owned_resp,
            "input": [openai_reasoning_input(owned_reasoning)]
        }),
        json!({
            "model": "gpt-5.4",
            "conversation": "conv_unknown_fail_closed",
            "input": [openai_reasoning_input(owned_reasoning)]
        }),
    ];
    for body in non_strippable_http {
        let resp = handle_authenticated(&lc, responses_request(body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    // Non-strippable WS frames keep 409 fail-closed.
    let mut gw = ws1296_gateway(lc).await;
    for frame in [
        Bytes::from(
            json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "input": [
                    ws1296_reasoning_item(owned_reasoning),
                    {"type": "compaction", "id": "cmp_ws", "encrypted_content": "gAAAAABcmp_ws"}
                ]
            })
            .to_string(),
        ),
        ws1296_create(Some(owned_resp), Some(owned_reasoning)),
    ] {
        let err = ws1296_admit(&mut gw, frame)
            .await
            .expect_err("non-strippable WS payload must stay 409");
        assert_eq!(err.status, 409);
        assert_eq!(err.code, "upstream_affinity_unavailable");
    }
    gw.close();
    assert_eq!(dispatch.call_count(), 0);

    // Zero eligible OpenAI upstreams -> 409 on both HTTP and WS.
    let no_fallback_lc = lifecycle(
        vec![openai_oauth_upstream_record(owner_a, "oauth-a", false)],
        dispatch.clone(),
        Some(store.clone()),
    );
    let no_fallback_http = handle_authenticated(
        &no_fallback_lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "input": [openai_reasoning_input(owned_reasoning)]
        })),
    )
    .await
    .unwrap();
    assert_eq!(no_fallback_http.status(), StatusCode::CONFLICT);
    let mut no_fallback_gw = ws1296_gateway(no_fallback_lc).await;
    let no_fallback_ws = ws1296_admit(
        &mut no_fallback_gw,
        ws1296_create(None, Some(owned_reasoning)),
    )
    .await
    .expect_err("zero OpenAI fallback must return 409");
    assert_eq!(no_fallback_ws.status, 409);
    assert_eq!(no_fallback_ws.code, "upstream_affinity_unavailable");
    no_fallback_gw.close();

    // `codex_backend` with disabled OAuth owner and only `openai_api_key` enabled -> 409 (HTTP + WS).
    let api_only_lc = lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_upstream_record(api_key_b, "openai-api-key", true),
        ],
        dispatch.clone(),
        Some(store.clone()),
    );
    let mut codex_http_req = responses_request(json!({
        "model": "gpt-5.4",
        "input": [openai_reasoning_input(owned_reasoning)]
    }));
    codex_http_req
        .extensions_mut()
        .insert(cc_lb_engine::CodexBackendSurface);
    let codex_http_resp = handle_authenticated(&api_only_lc, codex_http_req)
        .await
        .unwrap();
    assert_eq!(codex_http_resp.status(), StatusCode::CONFLICT);
    let mut codex_gw = ws1296_gateway(api_only_lc).await.with_codex_backend(true);
    let codex_ws_err = ws1296_admit(&mut codex_gw, ws1296_create(None, Some(owned_reasoning)))
        .await
        .expect_err("codex_backend must not fail over onto openai_api_key");
    assert_eq!(codex_ws_err.status, 409);
    assert_eq!(codex_ws_err.code, "upstream_affinity_unavailable");
    codex_gw.close();
    assert_eq!(dispatch.call_count(), 0);

    // Gemini cachedContent owner_epoch mismatch -> 409, while affinity store error -> 503.
    let gemini_store = Arc::new(MemoryAffinityStore::default());
    let gemini_key = UpstreamAffinityKey {
        principal_id: "principal-test".to_owned(),
        provider: "gemini".to_owned(),
        kind: UpstreamAffinityKind::GeminiCachedContent,
        value_sha256: Sha256::digest(b"cachedContents/epoch-check").into(),
    };
    gemini_store.insert_binding(UpstreamAffinityBinding {
        key: gemini_key,
        upstream_id: gemini_id,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        expires_at_unix_secs: None,
    });
    gemini_store.set_owner_epoch(gemini_id, 1);
    let gemini_lc = lifecycle(
        vec![gemini_upstream_record(gemini_id, "gemini-main", true)],
        dispatch.clone(),
        Some(gemini_store.clone()),
    );
    let epoch_resp = handle_authenticated(
        &gemini_lc,
        provider_request(
            cc_lb_domain::Provider::Gemini,
            "/v1beta/models/gemini-2.5-pro:generateContent",
            json!({
                "cachedContent": "cachedContents/epoch-check",
                "contents": [{"parts": [{"text": "hi"}]}]
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(epoch_resp.status(), StatusCode::CONFLICT);

    *gemini_store
        .resolve_failure
        .lock()
        .expect("resolve failure lock") = Some("transient storage outage".to_owned());
    let outage_resp = handle_authenticated(
        &gemini_lc,
        provider_request(
            cc_lb_domain::Provider::Gemini,
            "/v1beta/models/gemini-2.5-pro:generateContent",
            json!({
                "cachedContent": "cachedContents/epoch-check",
                "contents": [{"parts": [{"text": "hi"}]}]
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(outage_resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(dispatch.call_count(), 0);
}

#[tokio::test]
async fn strip_and_failover_rejects_account_owned_references_409() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000301").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000302").unwrap();
    let api_key_c = Uuid::parse_str("10000000-0000-0000-0000-000000000303").unwrap();
    let owned_reasoning = "gAAAAABowned_ref_rs";

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_reasoning), owner_a);

    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_ok",
        "object": "response",
        "status": "completed",
        "output": []
    })));
    let make_lc = || {
        lifecycle(
            vec![
                openai_oauth_upstream_record(owner_a, "oauth-a", false),
                openai_oauth_upstream_record(fallback_b, "oauth-b", true),
                openai_upstream_record(api_key_c, "openai-api-key", true),
            ],
            dispatch.clone(),
            Some(store.clone()),
        )
    };
    let lc = make_lc();

    // Every request carrying an account-owned file/container/vector-store/prompt/item reference
    // alongside disabled-owner reasoning must stay fail-closed with 409 on HTTP and WS.
    let blocked_payloads = [
        // input_image.file_id
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"role": "user", "content": [{"type": "input_image", "file_id": "file_img_1"}]}
            ]
        }),
        // input_file.file_id
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"role": "user", "content": [{"type": "input_file", "file_id": "file_doc_1"}]}
            ]
        }),
        // file_search.vector_store_ids
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "file_search", "vector_store_ids": ["vs_1"]}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "q"}]
        }),
        // code_interpreter with explicit container id
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "code_interpreter", "container": "cntr_owned_1"}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "q"}]
        }),
        // code_interpreter with container.file_ids
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "code_interpreter", "container": {"type": "auto", "file_ids": ["file_1"]}}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "q"}]
        }),
        // prompt template reference
        json!({
            "model": "gpt-5.4",
            "prompt": {"id": "pmpt_1"},
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "q"}]
        }),
        // item_reference
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": "item_reference", "id": "item_123"}
            ]
        }),
        // container_id in input item
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": "code_interpreter_call", "id": "ci_1", "container_id": "cntr_call_1"}
            ]
        }),
        // image_generation.input_image_mask.file_id
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "image_generation", "input_image_mask": {"file_id": "file_mask_1"}}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "edit"}]
        }),
        // shell.environment container_reference
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "shell", "environment": {"type": "container_reference", "container_id": "cntr_shell_1"}}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "run"}]
        }),
        // shell.environment container_auto with uploaded file_ids
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "shell", "environment": {"type": "container_auto", "file_ids": ["file_shell_1"]}}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "run"}]
        }),
        // function_call_output with array output carrying input_file.file_id
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": [{"type": "input_file", "file_id": "file_out_1"}]
                }
            ]
        }),
        // computer_call_output with object output carrying file_id
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "computer_call_output",
                    "call_id": "call_comp_1",
                    "output": {"type": "computer_screenshot", "file_id": "file_screenshot_1"}
                }
            ]
        }),
        // additional_tools carrying image_generation.input_image_mask.file_id
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "additional_tools",
                    "tools": [{"type": "image_generation", "input_image_mask": {"file_id": "file_add_mask_1"}}]
                },
                {"role": "user", "content": "edit"}
            ]
        }),
        // additional_tools carrying shell.environment container_reference
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "additional_tools",
                    "tools": [{"type": "shell", "environment": {"type": "container_reference", "container_id": "cntr_add_1"}}]
                },
                {"role": "user", "content": "run"}
            ]
        }),
        // tool_search_output carrying image_generation.input_image_mask.file_id
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "tool_search_output",
                    "tools": [{"type": "image_generation", "input_image_mask": {"file_id": "file_search_mask_1"}}]
                },
                {"role": "user", "content": "edit"}
            ]
        }),
        // top-level mcp tool with tunnel_id
        json!({
            "model": "gpt-5.4",
            "tools": [{"type": "mcp", "server_label": "priv", "tunnel_id": "tun_1"}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "call"}]
        }),
        // namespace tool wrapping nested mcp tool with tunnel_id
        json!({
            "model": "gpt-5.4",
            "tools": [{
                "type": "namespace",
                "name": "priv_ns",
                "tools": [{"type": "mcp", "server_label": "priv", "tunnel_id": "tun_nested_1"}]
            }],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "call"}]
        }),
        // org-scoped prompt_cache_options.comparison_response_id
        json!({
            "model": "gpt-5.4",
            "prompt_cache_options": {"ttl": "30m", "comparison_response_id": "resp_cmp_1"},
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "compare"}]
        }),
        // shorthand ItemReference without type field
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"id": "item_shorthand_1"}
            ]
        }),
        // shorthand ItemReference with explicit null type
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": null, "id": "item_shorthand_null_1"}
            ]
        }),
    ];

    for (idx, body) in blocked_payloads.iter().enumerate() {
        let http_resp = handle_authenticated(&lc, responses_request(body.clone()))
            .await
            .unwrap();
        assert_eq!(
            http_resp.status(),
            StatusCode::CONFLICT,
            "HTTP payload {idx} must stay 409"
        );
    }
    assert_eq!(dispatch.call_count(), 0);

    // Controls: `code_interpreter` (`{"type": "auto"}`), `shell` (`{"type": "container_auto"}`),
    // `image_generation` (`input_image_mask.image_url`), `function` schema `examples`, replayed
    // `mcp_list_tools.input_schema`, `additional_tools` schemas, `prompt_cache_options.ttl`,
    // complete replay items carrying `id`, and assistant `annotations` must remain
    // failover-eligible on HTTP and WS.
    let control_bodies = [
        json!({
            "model": "gpt-5.4",
            "tools": [
                {"type": "code_interpreter", "container": {"type": "auto"}},
                {"type": "shell", "environment": {"type": "container_auto"}},
                {"type": "image_generation", "input_image_mask": {"image_url": "data:image/png;base64,AA=="}},
                {
                    "type": "function",
                    "name": "read_doc",
                    "parameters": {
                        "type": "object",
                        "properties": {"file_id": {"type": "string"}},
                        "examples": [{"file_id": "file_ex"}]
                    }
                }
            ],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "run"}]
        }),
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "mcp_list_tools",
                    "server_label": "docs",
                    "tools": [{
                        "name": "inspect",
                        "input_schema": {
                            "type": "object",
                            "examples": [{"file_id": "file_schema_example"}]
                        }
                    }]
                },
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "See source.",
                        "annotations": [{"type": "file_citation", "file_id": "file_ann_1"}]
                    }]
                },
                {"role": "user", "content": "continue"}
            ]
        }),
        json!({
            "model": "gpt-5.4",
            "prompt_cache_options": {"ttl": "30m", "mode": "implicit"},
            "input": [
                openai_reasoning_input(owned_reasoning),
                {
                    "type": "additional_tools",
                    "tools": [{
                        "type": "function",
                        "name": "schema_only",
                        "parameters": {
                            "type": "object",
                            "examples": [{"file_id": "file_add_ex", "tunnel_id": "tun_ex"}]
                        }
                    }]
                },
                {
                    "type": "message",
                    "id": "msg_complete_1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "Prior turn"}]
                },
                {
                    "id": "msg_easy_1",
                    "role": "user",
                    "content": "follow up"
                },
                {
                    "type": "function_call",
                    "id": "fc_1",
                    "call_id": "call_1",
                    "name": "read_doc",
                    "arguments": "{}"
                },
                {"role": "user", "content": "continue"}
            ]
        }),
    ];
    for (idx, body) in control_bodies.iter().enumerate() {
        let control_resp = handle_authenticated(&lc, responses_request(body.clone()))
            .await
            .unwrap();
        assert_eq!(
            control_resp.status(),
            StatusCode::OK,
            "HTTP control {idx} should fail over"
        );
    }
    let captured = dispatch.captured_requests();
    assert_eq!(captured.len(), 3);
    assert!(
        captured[0].0.contains("openai-api-key.local"),
        "{}",
        captured[0].0
    );
    assert_eq!(captured[0].1["input"].as_array().unwrap().len(), 1);
    assert_eq!(captured[1].1["input"].as_array().unwrap().len(), 3);
    assert_eq!(captured[2].1["input"].as_array().unwrap().len(), 5);

    let mut gw = ws1296_gateway(lc).await;
    for (idx, body) in blocked_payloads.iter().enumerate() {
        let mut ws_body = body.clone();
        ws_body
            .as_object_mut()
            .unwrap()
            .insert("type".to_owned(), json!("response.create"));
        let err = ws1296_admit(&mut gw, Bytes::from(ws_body.to_string()))
            .await
            .expect_err(&format!("WS payload {idx} must stay 409"));
        assert_eq!(err.status, 409);
        assert_eq!(err.code, "upstream_affinity_unavailable");
    }
    gw.close();
    for (idx, body) in control_bodies.iter().enumerate() {
        let mut ctrl_gw = ws1296_gateway(make_lc()).await;
        let mut ws_body = body.clone();
        ws_body
            .as_object_mut()
            .unwrap()
            .insert("type".to_owned(), json!("response.create"));
        let admitted = ws1296_admit(&mut ctrl_gw, Bytes::from(ws_body.to_string()))
            .await
            .unwrap_or_else(|err| panic!("WS control {idx} should admit: {err:?}"))
            .unwrap_or_else(|| panic!("WS control {idx} should dispatch"));
        let parsed: Value = serde_json::from_slice(&admitted.frame).unwrap();
        assert_eq!(
            parsed["input"].as_array().unwrap().len(),
            captured[idx].1["input"].as_array().unwrap().len()
        );
        ctrl_gw.close();
    }
    assert_eq!(dispatch.call_count(), 3);
}

#[tokio::test]
async fn strip_and_failover_public_ws_code_interpreter_auto_prefers_api_key_over_oauth() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000311").unwrap();
    let oauth_b = Uuid::parse_str("10000000-0000-0000-0000-000000000312").unwrap();
    let api_key_c = Uuid::parse_str("10000000-0000-0000-0000-000000000313").unwrap();
    let owned_reasoning = "gAAAAABowned_ws_code_interp";

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_reasoning), owner_a);

    let make_mixed_lc = || {
        lifecycle(
            vec![
                openai_oauth_upstream_record(owner_a, "oauth-a", false),
                openai_oauth_upstream_record(oauth_b, "oauth-b", true),
                openai_upstream_record(api_key_c, "openai-api-key", true),
            ],
            Arc::new(RecordingDispatch::default()),
            Some(store.clone()),
        )
    };

    let mut public_gw = ws1296_gateway(make_mixed_lc()).await;
    let frame = Bytes::from(
        json!({
            "type": "response.create",
            "model": "gpt-5.4",
            "store": false,
            "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}],
            "input": [ws1296_reasoning_item(owned_reasoning), {"role": "user", "content": "calc"}]
        })
        .to_string(),
    );
    let dispatch = ws1296_admit(&mut public_gw, frame.clone())
        .await
        .expect("public WS failover admits")
        .expect("public WS failover dispatches");
    assert_eq!(dispatch.candidates.len(), 1);
    assert_eq!(
        dispatch.candidates[0].upstream_id, api_key_c,
        "public WS code_interpreter failover must prefer API key over OAuth"
    );
    let parsed_frame: Value = serde_json::from_slice(&dispatch.frame).unwrap();
    assert_eq!(parsed_frame["input"].as_array().unwrap().len(), 1);
    public_gw.close();

    // OAuth-only subcase (no API key enabled): both HTTP and public WS code_interpreter owner
    // failover must fail closed with 409 rather than reaching OAuth request shaping.
    let oauth_only_store = Arc::new(MemoryAffinityStore::default());
    oauth_only_store.insert(ws1296_openai_key(owned_reasoning), owner_a);
    let oauth_only_dispatch = Arc::new(RecordingDispatch::default());
    let oauth_only_lc = lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(oauth_b, "oauth-b", true),
        ],
        oauth_only_dispatch.clone(),
        Some(oauth_only_store),
    );
    let http_err_resp = handle_authenticated(
        &oauth_only_lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}],
            "input": [openai_reasoning_input(owned_reasoning), {"role": "user", "content": "calc"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(
        http_err_resp.status(),
        StatusCode::CONFLICT,
        "HTTP public code_interpreter with only OAuth fallback must return 409"
    );
    assert_eq!(oauth_only_dispatch.call_count(), 0);

    let mut oauth_only_gw = ws1296_gateway(oauth_only_lc).await;
    let err = ws1296_admit(&mut oauth_only_gw, frame)
        .await
        .expect_err("public WS code_interpreter with only OAuth fallback must return 409");
    assert_eq!(err.status, 409);
    assert_eq!(err.code, "upstream_affinity_unavailable");
    oauth_only_gw.close();

    // Queued race subcase: Turn 2 (public code_interpreter failover requiring API key) queues on
    // a busy lane while Turn 1 is connecting to OAuth B. Even after Turn 1 pins `session_upstream`
    // to OAuth B, Turn 2 must keep its admitted API-key candidate and reject OAuth B with 409;
    // subsequent Turn 3 admitted under `SessionHint(oauth_b)` must likewise route to API key C.
    let mut queued_gw = ws1296_gateway(make_mixed_lc()).await;
    let turn1_frame = Bytes::from(
        json!({
            "type": "response.create",
            "model": "gpt-5.4",
            "stream_id": "lane-1",
            "store": false,
            "input": [{"role": "user", "content": "warm oauth session"}]
        })
        .to_string(),
    );
    let d1 = ws1296_admit(&mut queued_gw, turn1_frame)
        .await
        .expect("Turn 1 admits cleanly")
        .expect("Turn 1 dispatches immediately");

    let turn2_frame = Bytes::from(
        json!({
            "type": "response.create",
            "model": "gpt-5.4",
            "stream_id": "lane-1",
            "store": false,
            "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}],
            "input": [ws1296_reasoning_item(owned_reasoning), {"role": "user", "content": "queued calc"}]
        })
        .to_string(),
    );
    assert!(
        ws1296_admit(&mut queued_gw, turn2_frame)
            .await
            .expect("queued Turn 2 admits cleanly")
            .is_none(),
        "Turn 2 must queue behind active Turn 1 on lane-1"
    );

    queued_gw.assign_connection(d1.turn_id, 1, oauth_b, "oauth-b");
    queued_gw.commit_send_attempt(d1.turn_id);
    let _ = queued_gw.sent(d1.turn_id);
    let _ = queued_gw
        .provider_message(
            1,
            &Bytes::from(
                json!({
                    "type": "response.created",
                    "stream_id": "lane-1",
                    "response": {"id": "resp_warm_oauth", "status": "in_progress", "store": false}
                })
                .to_string(),
            ),
        )
        .await
        .unwrap();
    let popped = queued_gw
        .provider_message(
            1,
            &Bytes::from(
                json!({
                    "type": "response.completed",
                    "stream_id": "lane-1",
                    "response": {
                        "id": "resp_warm_oauth",
                        "status": "completed",
                        "store": false,
                        "usage": {"input_tokens": 10, "output_tokens": 2}
                    }
                })
                .to_string(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(queued_gw.session_upstream(), Some(oauth_b));
    let d2 = popped
        .dispatches
        .into_iter()
        .find(|d| d.turn_id != d1.turn_id && !d.control)
        .expect("completing Turn 1 dispatches queued Turn 2");
    assert_eq!(d2.candidates.len(), 1);
    assert_eq!(
        d2.candidates[0].upstream_id, api_key_c,
        "queued API-key-only owner failover must not be overwritten by OAuth session pin"
    );
    let oauth_err = queued_gw
        .validate_candidate_for_send(d2.turn_id, oauth_b, None, false)
        .await
        .expect_err("OAuth candidate must be rejected for queued API-key-only owner failover");
    assert_eq!(oauth_err.status, 409);
    assert_eq!(oauth_err.code, "upstream_affinity_unavailable");
    queued_gw
        .validate_candidate_for_send(d2.turn_id, api_key_c, None, false)
        .await
        .expect("API-key candidate validates cleanly for queued Turn 2");

    let turn3_frame = Bytes::from(
        json!({
            "type": "response.create",
            "model": "gpt-5.4",
            "stream_id": "lane-2",
            "store": false,
            "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}],
            "input": [ws1296_reasoning_item(owned_reasoning), {"role": "user", "content": "later calc"}]
        })
        .to_string(),
    );
    let d3 = ws1296_admit(&mut queued_gw, turn3_frame)
        .await
        .expect("Turn 3 under SessionHint(oauth_b) admits cleanly")
        .expect("Turn 3 dispatches immediately");
    assert_eq!(d3.candidates.len(), 1);
    assert_eq!(
        d3.candidates[0].upstream_id, api_key_c,
        "SessionHint(oauth_b) must not override API-key requirement on owner failover"
    );
    queued_gw.close();

    // `additional_tools` declaring `code_interpreter` with a fresh auto container must receive
    // the exact same API-key credential protection on HTTP and WS: route to API key when mixed,
    // fail closed with 409 when only OAuth candidates exist, while `function` tools in
    // `additional_tools` stay eligible to fail over to OAuth.
    let additional_code_interp_http = json!({
        "model": "gpt-5.4",
        "store": false,
        "input": [
            openai_reasoning_input(owned_reasoning),
            {
                "type": "additional_tools",
                "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
            },
            {"role": "user", "content": "calc"}
        ]
    });
    let additional_code_interp_ws = Bytes::from(
        json!({
            "type": "response.create",
            "model": "gpt-5.4",
            "store": false,
            "input": [
                ws1296_reasoning_item(owned_reasoning),
                {
                    "type": "additional_tools",
                    "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
                },
                {"role": "user", "content": "calc"}
            ]
        })
        .to_string(),
    );

    let mixed_dispatch = Arc::new(RecordingDispatch::default());
    let mixed_lc = lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(oauth_b, "oauth-b", true),
            openai_upstream_record(api_key_c, "openai-api-key", true),
        ],
        mixed_dispatch.clone(),
        Some(store.clone()),
    );
    let mixed_http_resp = handle_authenticated(
        &mixed_lc,
        responses_request(additional_code_interp_http.clone()),
    )
    .await
    .unwrap();
    assert_eq!(mixed_http_resp.status(), StatusCode::OK);
    let mixed_captured = mixed_dispatch.captured_requests();
    assert_eq!(mixed_captured.len(), 1);
    assert!(mixed_captured[0].0.contains("openai-api-key.local"));

    let mut mixed_ws_gw = ws1296_gateway(mixed_lc).await;
    let mixed_ws_dispatch = ws1296_admit(&mut mixed_ws_gw, additional_code_interp_ws.clone())
        .await
        .expect("mixed pool additional_tools code_interpreter WS admits")
        .expect("mixed pool additional_tools code_interpreter WS dispatches");
    assert_eq!(mixed_ws_dispatch.candidates.len(), 1);
    assert_eq!(mixed_ws_dispatch.candidates[0].upstream_id, api_key_c);
    mixed_ws_gw.close();

    let oauth_only_add_dispatch = Arc::new(RecordingDispatch::default());
    let oauth_only_add_lc = lifecycle(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(oauth_b, "oauth-b", true),
        ],
        oauth_only_add_dispatch.clone(),
        Some(store),
    );
    let oauth_add_http_resp = handle_authenticated(
        &oauth_only_add_lc,
        responses_request(additional_code_interp_http),
    )
    .await
    .unwrap();
    assert_eq!(
        oauth_add_http_resp.status(),
        StatusCode::CONFLICT,
        "HTTP additional_tools code_interpreter with only OAuth fallback must return 409"
    );

    // Control on OAuth-only pool: `additional_tools` with a `function` tool (no hosted tool)
    // still fails over cleanly to `oauth_b` on both HTTP and WS.
    let oauth_fn_ctrl_resp = handle_authenticated(
        &oauth_only_add_lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [
                openai_reasoning_input(owned_reasoning),
                {"type": "additional_tools", "tools": [{"type": "function", "name": "lookup"}]},
                {"role": "user", "content": "calc"}
            ]
        })),
    )
    .await
    .unwrap();
    assert_eq!(oauth_fn_ctrl_resp.status(), StatusCode::OK);
    assert_eq!(oauth_only_add_dispatch.call_count(), 1);

    let mut oauth_only_add_gw = ws1296_gateway(oauth_only_add_lc).await;
    let add_ws_err = ws1296_admit(&mut oauth_only_add_gw, additional_code_interp_ws)
        .await
        .expect_err(
            "WS additional_tools code_interpreter with only OAuth fallback must return 409",
        );
    assert_eq!(add_ws_err.status, 409);
    assert_eq!(add_ws_err.code, "upstream_affinity_unavailable");
    let fn_ctrl_ws_dispatch = ws1296_admit(
        &mut oauth_only_add_gw,
        Bytes::from(
            json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "store": false,
                "input": [
                    ws1296_reasoning_item(owned_reasoning),
                    {"type": "additional_tools", "tools": [{"type": "function", "name": "lookup"}]},
                    {"role": "user", "content": "calc"}
                ]
            })
            .to_string(),
        ),
    )
    .await
    .expect("WS additional_tools function control admits")
    .expect("WS additional_tools function control dispatches");
    assert_eq!(fn_ctrl_ws_dispatch.candidates[0].upstream_id, oauth_b);
    oauth_only_add_gw.close();
}

#[tokio::test]
async fn strip_and_failover_filters_inspect_stripped_body_and_updated_headers_http_and_ws() {
    use cc_lb_control::api_keys::principal_view::{
        DialectCache, PrincipalView, RouterPipelineCache,
    };
    use cc_lb_domain::{Principal, TerminalStrategy, UpstreamCandidate};
    use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RoutingContext};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct InspectBodyFilter {
        id: Uuid,
        reject: AtomicBool,
        seen: Mutex<Vec<(usize, String, Option<String>)>>,
    }

    impl FilterPlugin for InspectBodyFilter {
        fn filter(
            &self,
            ctx: &RoutingContext,
            _principal: &Principal,
            candidates: &[UpstreamCandidate],
        ) -> Result<FilterOutput, FilterError> {
            let body_str = String::from_utf8_lossy(&ctx.body_bytes).into_owned();
            let cl = ctx
                .downstream_headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            self.seen
                .lock()
                .unwrap()
                .push((ctx.body_bytes.len(), body_str, cl));
            let kept = if self.reject.load(Ordering::SeqCst) {
                Vec::new()
            } else {
                candidates.iter().map(|c| c.upstream_id).collect()
            };
            Ok(FilterOutput {
                kept_upstream_ids: kept,
                reason: "inspect-body".to_owned(),
                per_candidate_reasons: Vec::new(),
                subscription_preference: None,
            })
        }

        fn plugin_id(&self) -> Uuid {
            self.id
        }

        fn plugin_name(&self) -> &str {
            "inspect-body"
        }
    }

    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000321").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000322").unwrap();
    let owned_cipher = "gAAAAABowned_filter_rs";

    let filter = Arc::new(InspectBodyFilter {
        id: Uuid::from_u128(0xF11738),
        reject: AtomicBool::new(false),
        seen: Mutex::new(Vec::new()),
    });
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter.clone()],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let chains = HashMap::from([(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    )]);
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ));

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_cipher), owner_a);

    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_filtered",
        "object": "response",
        "status": "completed",
        "output": []
    })));
    let make_lc = || {
        let authn = TestAuthn::with_principal_view(TestState::default(), principal_view.clone());
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(authn.clone()))
            .principal_view(authn.principal_view.clone())
            .upstream_records(vec![
                openai_oauth_upstream_record(owner_a, "oauth-a", false),
                openai_oauth_upstream_record(fallback_b, "oauth-b", true),
            ])
            .build();
        Lifecycle::new_with_dynamic_view(
            authn.authn,
            Arc::new(DynamicViewHolder::new(view)),
            dispatch.clone(),
            LifecycleConfig::default(),
            Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
        )
        .with_upstream_affinity_store(store.clone())
    };
    let lc = make_lc();

    // HTTP pass: primary filter sees original unstripped body; fallback filter sees stripped
    // body AND updated Content-Length matching the stripped length.
    let raw_http_body = json!({
        "model": "gpt-5.4",
        "input": [openai_reasoning_input(owned_cipher), {"role": "user", "content": "hi"}]
    });
    let mut http_req = responses_request(raw_http_body);
    let orig_len = http_req.body().len();
    http_req.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(&orig_len.to_string()).unwrap(),
    );
    let resp = handle_authenticated(&lc, http_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    {
        let seen = filter.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[0].1.contains(owned_cipher));
        assert!(!seen[1].1.contains(owned_cipher), "{}", seen[1].1);
        assert_eq!(seen[1].2.as_deref(), Some(seen[1].0.to_string().as_str()));
        assert!(seen[1].0 < orig_len);
    }

    // WS pass: fallback filter sees stripped WS frame.
    filter.seen.lock().unwrap().clear();
    let mut gw = ws1296_gateway(make_lc()).await;
    let d = ws1296_admit(&mut gw, ws1296_create(None, Some(owned_cipher)))
        .await
        .expect("WS admits after filter approves stripped body")
        .expect("WS dispatches");
    assert_eq!(d.candidates[0].upstream_id, fallback_b);
    {
        let seen = filter.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[0].1.contains(owned_cipher));
        assert!(!seen[1].1.contains(owned_cipher), "{}", seen[1].1);
    }

    // When fallback filter rejects the stripped body, both HTTP and WS return 409 without dispatching.
    filter.reject.store(true, Ordering::SeqCst);
    let rej_http = handle_authenticated(
        &lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "input": [openai_reasoning_input(owned_cipher), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(rej_http.status(), StatusCode::CONFLICT);
    let rej_ws = ws1296_admit(&mut gw, ws1296_create(None, Some(owned_cipher)))
        .await
        .expect_err("WS filter rejecting stripped body must return 409");
    assert_eq!(rej_ws.status, 409);
    assert_eq!(rej_ws.code, "upstream_affinity_unavailable");
    gw.close();
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn strip_and_failover_excludes_exhausted_subscription_before_stripping_http_and_ws() {
    use cc_lb_control::SubscriptionQuotaCacheLike;
    use cc_lb_control::api_keys::principal_view::{
        DialectCache, PrincipalView, RouterPipelineCache,
    };
    use cc_lb_domain::{
        SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, TerminalStrategy,
    };
    use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
    use cc_lb_routing::FilterPlugin;

    struct StaticQuotaCache {
        snapshots: HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>,
    }

    impl SubscriptionQuotaCacheLike for StaticQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000331").unwrap();
    let exhausted_b = Uuid::parse_str("10000000-0000-0000-0000-000000000332").unwrap();
    let owned_cipher = "gAAAAABowned_exhausted_rs";

    let quota_cache = Arc::new(StaticQuotaCache {
        snapshots: HashMap::from([(
            exhausted_b,
            vec![
                SubscriptionQuotaCandidateSnapshot {
                    window: "5h".to_owned(),
                    state: SubscriptionQuotaDataState::Fresh,
                    source: Some("test".to_owned()),
                    utilization: Some(1.0),
                    status: Some("rejected".to_owned()),
                    resets_at_unix_secs: Some(TEST_NOW_UNIX_SECS + 1800),
                    surpassed_threshold: None,
                    representative_claim: None,
                    disabled_reason: None,
                    extra_usage_enabled: None,
                    extra_usage_monthly_limit: None,
                    extra_usage_used_credits: None,
                    credits: None,
                    observed_at_unix_millis: Some(TEST_NOW_UNIX_SECS * 1000),
                    max_staleness_secs: 60,
                    fallback_available: None,
                    overage_in_use: None,
                    overage_period_monthly_utilization: None,
                    upgrade_paths: None,
                },
                SubscriptionQuotaCandidateSnapshot {
                    window: "7d".to_owned(),
                    state: SubscriptionQuotaDataState::Fresh,
                    source: Some("test".to_owned()),
                    utilization: Some(1.0),
                    status: Some("rejected".to_owned()),
                    resets_at_unix_secs: Some(TEST_NOW_UNIX_SECS + 86400),
                    surpassed_threshold: None,
                    representative_claim: None,
                    disabled_reason: None,
                    extra_usage_enabled: None,
                    extra_usage_monthly_limit: None,
                    extra_usage_used_credits: None,
                    credits: None,
                    observed_at_unix_millis: Some(TEST_NOW_UNIX_SECS * 1000),
                    max_staleness_secs: 60,
                    fallback_available: None,
                    overage_in_use: None,
                    overage_period_monthly_utilization: None,
                    upgrade_paths: None,
                },
            ],
        )]),
    });
    let filter: Arc<dyn FilterPlugin> = Arc::new(SubscriptionPreferenceFilter::with_clock(
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    ));
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let chains = HashMap::from([(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    )]);
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ));

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_cipher), owner_a);

    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .subscription_quota_cache(quota_cache)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(exhausted_b, "oauth-exhausted-b", true),
        ])
        .build();
    let lc = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatch.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store);

    let http_resp = handle_authenticated(
        &lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "input": [openai_reasoning_input(owned_cipher), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(http_resp.status(), StatusCode::CONFLICT);

    let mut gw = ws1296_gateway(lc).await;
    let ws_err = ws1296_admit(&mut gw, ws1296_create(None, Some(owned_cipher)))
        .await
        .expect_err("sole quota-exhausted OAuth fallback must return 409, not strip/dispatch");
    assert_eq!(ws_err.status, 409);
    assert_eq!(ws_err.code, "upstream_affinity_unavailable");
    gw.close();
    assert_eq!(dispatch.call_count(), 0);
}

#[tokio::test]
async fn gemini_stale_snapshot_returns_503_while_confirmed_epoch_mismatch_returns_409() {
    let gemini_id = Uuid::parse_str("10000000-0000-0000-0000-000000000341").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    let gemini_key = UpstreamAffinityKey {
        principal_id: "principal-test".to_owned(),
        provider: "gemini".to_owned(),
        kind: UpstreamAffinityKind::GeminiCachedContent,
        value_sha256: Sha256::digest(b"cachedContents/snapshot-vs-epoch").into(),
    };
    store.insert_binding(UpstreamAffinityBinding {
        key: gemini_key,
        upstream_id: gemini_id,
        owner_epoch: 0,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        expires_at_unix_secs: None,
    });

    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let lc = lifecycle(
        vec![gemini_upstream_record(gemini_id, "gemini-main", true)],
        dispatch.clone(),
        Some(store.clone()),
    );
    let req = || {
        provider_request(
            cc_lb_domain::Provider::Gemini,
            "/v1beta/models/gemini-2.5-pro:generateContent",
            json!({
                "cachedContent": "cachedContents/snapshot-vs-epoch",
                "contents": [{"parts": [{"text": "hi"}]}]
            }),
        )
    };

    // 1. Concurrent upstream rename bumps spec_revision while preserving owner_epoch=0 ->
    // `read_upstream_owner_epoch` sees revision mismatch (`Ok(None)`) -> retryable 503.
    store.set_spec_revision(gemini_id, 2);
    let stale_snapshot_resp = handle_authenticated(&lc, req()).await.unwrap();
    assert_eq!(
        stale_snapshot_resp.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    // 2. Snapshot revision matches DB (`spec_revision=1`), but dispatch sees `owner_epoch=1 != 0`
    // (`Ok(Some(1))`) -> confirmed epoch conflict -> 409 Conflict.
    store.set_spec_revision(gemini_id, 1);
    store.set_read_epoch_override(gemini_id, 1);
    let epoch_conflict_resp = handle_authenticated(&lc, req()).await.unwrap();
    assert_eq!(epoch_conflict_resp.status(), StatusCode::CONFLICT);
    assert_eq!(dispatch.call_count(), 0);
}

#[tokio::test]
async fn strip_and_failover_respects_unknown_openai_reasoning_fail_closed_on_mixed_replay_http_and_ws()
 {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000351").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000352").unwrap();
    let known_cipher = "gAAAAABowned_known_mixed";
    let unknown_cipher = "gAAAAABunknown_unestablished_mixed";

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(known_cipher), owner_a);

    let mixed_body = || {
        json!({
            "model": "gpt-5.4",
            "input": [
                openai_reasoning_input(known_cipher),
                openai_reasoning_input(unknown_cipher),
                {"role": "user", "content": "continue"}
            ]
        })
    };
    let mut mixed_ws = mixed_body();
    mixed_ws
        .as_object_mut()
        .unwrap()
        .insert("type".to_owned(), json!("response.create"));
    let mixed_ws_frame = Bytes::from(mixed_ws.to_string());

    // 1. `unknown_openai_reasoning = FailClosed` with `StripAndFailover`: mixed bound + unbound
    // replay must fail closed with 409 and 0 dispatches on HTTP and WS.
    let dispatch_fc = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let strict_lc = lifecycle_with_config(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ],
        dispatch_fc.clone(),
        Some(store.clone()),
        LifecycleConfig {
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::FailClosed,
            unavailable_openai_reasoning_owner:
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            ..LifecycleConfig::default()
        },
    );
    let http_err = handle_authenticated(&strict_lc, responses_request(mixed_body()))
        .await
        .unwrap();
    assert_eq!(http_err.status(), StatusCode::CONFLICT);
    let mut strict_gw = ws1296_gateway(strict_lc).await;
    let ws_err = ws1296_admit(&mut strict_gw, mixed_ws_frame.clone())
        .await
        .expect_err("mixed replay under unknown_openai_reasoning=fail_closed must stay 409");
    assert_eq!(ws_err.status, 409);
    assert_eq!(ws_err.code, "upstream_affinity_unavailable");
    strict_gw.close();
    assert_eq!(dispatch_fc.call_count(), 0);

    // 2. Under `unknown_openai_reasoning = Hybrid`, the same mixed replay strips and fails over.
    let dispatch_hybrid = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_ok",
        "object": "response",
        "status": "completed",
        "output": []
    })));
    let hybrid_lc = lifecycle_with_config(
        vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ],
        dispatch_hybrid.clone(),
        Some(store),
        LifecycleConfig {
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid,
            unavailable_openai_reasoning_owner:
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            ..LifecycleConfig::default()
        },
    );
    let http_ok = handle_authenticated(&hybrid_lc, responses_request(mixed_body()))
        .await
        .unwrap();
    assert_eq!(http_ok.status(), StatusCode::OK);
    let captured = dispatch_hybrid.captured_requests();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].1["input"].as_array().unwrap().len(), 1);

    let mut hybrid_gw = ws1296_gateway(hybrid_lc).await;
    let d = ws1296_admit(&mut hybrid_gw, mixed_ws_frame)
        .await
        .expect("mixed replay under Hybrid admits failover")
        .expect("dispatches");
    assert_eq!(d.candidates[0].upstream_id, fallback_b);
    let parsed: Value = serde_json::from_slice(&d.frame).unwrap();
    assert_eq!(parsed["input"].as_array().unwrap().len(), 1);
    hybrid_gw.close();
}

#[tokio::test]
async fn strip_and_failover_ws_rechecks_sole_replacement_quota_before_dispatch() {
    use cc_lb_control::SubscriptionQuotaCacheLike;
    use cc_lb_control::api_keys::principal_view::{
        DialectCache, PrincipalView, RouterPipelineCache,
    };
    use cc_lb_domain::{
        SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, TerminalStrategy,
    };
    use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
    use cc_lb_routing::FilterPlugin;

    #[derive(Default)]
    struct MutableQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl SubscriptionQuotaCacheLike for MutableQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .unwrap()
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000361").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000362").unwrap();
    let owned_cipher = "gAAAAABowned_ws_sole_recheck";

    let quota_cache = Arc::new(MutableQuotaCache::default());
    let filter: Arc<dyn FilterPlugin> = Arc::new(SubscriptionPreferenceFilter::with_clock(
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    ));
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let chains = HashMap::from([(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    )]);
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ));

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_cipher), owner_a);

    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", false),
            openai_oauth_upstream_record(fallback_b, "oauth-fallback-b", true),
        ])
        .build();
    let lc = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        RecordingDispatch::with_response(ResponseSpec::Json(json!({}))),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store);

    let mut gw = ws1296_gateway(lc).await;
    let dispatch = ws1296_admit(&mut gw, ws1296_create(None, Some(owned_cipher)))
        .await
        .expect("sole healthy fallback B admits")
        .expect("dispatches");
    assert_eq!(dispatch.candidates.len(), 1);
    assert_eq!(dispatch.candidates[0].upstream_id, fallback_b);

    // Exhaust sole replacement B between admission and upstream connection/send.
    {
        let exhausted = |window: &str, reset_delta: u64| SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: SubscriptionQuotaDataState::Fresh,
            source: Some("test".to_owned()),
            utilization: Some(1.0),
            status: Some("rejected".to_owned()),
            resets_at_unix_secs: Some(TEST_NOW_UNIX_SECS + reset_delta),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(TEST_NOW_UNIX_SECS * 1000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        };
        quota_cache.snapshots.lock().unwrap().insert(
            fallback_b,
            vec![exhausted("5h", 1800), exhausted("7d", 86400)],
        );
    }

    let validate_err = gw
        .validate_fallback_candidate(&dispatch, &dispatch.candidates[0])
        .await
        .expect_err("validate_fallback_candidate must recheck exhausted replacement");
    assert_eq!(validate_err.status, 503);
    assert_eq!(validate_err.code, "route_no_upstream_after_filter");

    let connect_job = gw
        .begin_connection_prepare(&dispatch, &dispatch.candidates[0])
        .expect("prepare job builds");
    let connect_err = connect_job
        .run()
        .await
        .err()
        .expect("PreparedWsConnectJob::run must recheck exhausted replacement");
    assert_eq!(connect_err.status, 503);
    assert_eq!(connect_err.code, "route_no_upstream_after_filter");

    let send_err = gw
        .validate_candidate_for_send(dispatch.turn_id, fallback_b, None, false)
        .await
        .expect_err("validate_candidate_for_send must recheck exhausted replacement");
    assert_eq!(send_err.status, 503);
    assert_eq!(send_err.code, "route_no_upstream_after_filter");
    gw.close();
}

#[derive(Default)]
struct QaSubscriptionQuotaCache {
    snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    ingested: Arc<tokio::sync::Notify>,
}

impl QaSubscriptionQuotaCache {
    fn snapshot_with_utilization(
        window: &str,
        utilization: f64,
        status: &str,
        reset_delta_secs: u64,
    ) -> cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
        cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("test".to_owned()),
            utilization: Some(utilization),
            status: Some(status.to_owned()),
            resets_at_unix_secs: Some(TEST_NOW_UNIX_SECS + reset_delta_secs),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(TEST_NOW_UNIX_SECS * 1000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }
    }

    fn set_exhausted(&self, upstream_id: Uuid) {
        self.snapshots.lock().unwrap().insert(
            upstream_id,
            vec![
                Self::snapshot_with_utilization("5h", 1.0, "rejected", 1800),
                Self::snapshot_with_utilization("7d", 1.0, "rejected", 86400),
            ],
        );
    }

    fn set_healthy(&self, upstream_id: Uuid) {
        self.snapshots.lock().unwrap().insert(
            upstream_id,
            vec![
                Self::snapshot_with_utilization("5h", 0.1, "allowed", 1800),
                Self::snapshot_with_utilization("7d", 0.1, "allowed", 86400),
            ],
        );
    }
}

impl cc_lb_control::SubscriptionQuotaCacheLike for QaSubscriptionQuotaCache {
    fn upsert_observation(&self, record: &cc_lb_storage_api::SubscriptionQuotaSample) {
        let mut guard = self.snapshots.lock().unwrap();
        let entry = guard.entry(record.upstream_id).or_default();
        entry.retain(|s| s.window != record.window.as_str());
        entry.push(cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: record.window.as_str().to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some(record.source.as_str().to_owned()),
            utilization: record.utilization,
            status: record.status.map(|s| s.as_str().to_owned()),
            resets_at_unix_secs: record.resets_at_unix_secs,
            surpassed_threshold: record.surpassed_threshold,
            representative_claim: record.representative_claim.clone(),
            disabled_reason: record.disabled_reason.clone(),
            extra_usage_enabled: record.extra_usage_enabled,
            extra_usage_monthly_limit: record.extra_usage_monthly_limit,
            extra_usage_used_credits: record.extra_usage_used_credits,
            credits: record.credits.clone(),
            observed_at_unix_millis: Some(record.observed_at_unix_millis),
            max_staleness_secs: 60,
            fallback_available: record.fallback_available,
            overage_in_use: record.overage_in_use,
            overage_period_monthly_utilization: record.overage_period_monthly_utilization,
            upgrade_paths: record.upgrade_paths.clone(),
        });
        self.ingested.notify_waiters();
    }

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
        self.snapshots
            .lock()
            .unwrap()
            .get(&upstream_id)
            .cloned()
            .unwrap_or_default()
    }
}

fn sub_pref_principal_view() -> Arc<cc_lb_control::api_keys::principal_view::PrincipalView> {
    use cc_lb_control::api_keys::principal_view::{
        DialectCache, PrincipalView, RouterPipelineCache,
    };
    use cc_lb_domain::TerminalStrategy;
    use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
    use cc_lb_routing::FilterPlugin;

    let filter: Arc<dyn FilterPlugin> = Arc::new(SubscriptionPreferenceFilter::with_clock(
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    ));
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let chains = HashMap::from([(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    )]);
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

#[tokio::test]
async fn strip_and_failover_http_quota_exhausted_owner_strips_and_routes_to_healthy_b() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000401").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000402").unwrap();
    let owned_a = "gAAAAAB_http_exhausted_a";
    let new_b = "gAAAAAB_http_fresh_b";

    let quota_cache = Arc::new(QaSubscriptionQuotaCache::default());
    quota_cache.set_exhausted(owner_a);
    quota_cache.set_healthy(fallback_b);

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_a), owner_a);
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_b_ok",
        "object": "response",
        "status": "completed",
        "output": [openai_reasoning_input(new_b)],
        "usage": {"input_tokens": 5, "output_tokens": 3}
    })));
    let authn = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .subscription_quota_cache(quota_cache)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ])
        .build();
    let lc = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatch.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());

    let resp = handle_authenticated(
        &lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let captured = dispatch.captured_requests();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].0.contains("oauth-b.local"), "{}", captured[0].0);
    assert_eq!(captured[0].1["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(new_b)),
        Some(fallback_b)
    );
}

#[tokio::test]
async fn strip_and_failover_http_live_429_strips_ingests_quota_and_routes_to_healthy_b() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000411").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000412").unwrap();
    let owned_a = "gAAAAAB_http_live_429_a";
    let new_b = "gAAAAAB_http_live_429_b";

    let quota_cache = Arc::new(QaSubscriptionQuotaCache::default());
    quota_cache.set_healthy(owner_a);
    quota_cache.set_healthy(fallback_b);

    let test_bus = TestLifecycleBus::new();
    let quota_rx = test_bus.bus.attach_lifecycle_subscription_quota(64);
    let _subscriber = cc_lb_event_pipeline::spawn_lifecycle_subscription_quota_subscriber(
        quota_rx,
        Some(quota_cache.clone() as Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike>),
        None,
    );

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_a), owner_a);

    let dispatch = RecordingDispatch::with_responses(vec![
        ResponseSpec::StatusWithHeaders(
            StatusCode::TOO_MANY_REQUESTS,
            vec![
                ("x-codex-primary-used-percent", "100"),
                ("x-codex-primary-window-minutes", "300"),
                ("x-codex-secondary-used-percent", "100"),
                ("x-codex-secondary-window-minutes", "10080"),
            ],
            json!({"error": {"type": "rate_limit_error", "code": "rate_limit_exceeded", "message": "429"}}),
        ),
        ResponseSpec::Json(json!({
            "id": "resp_b_429_recover",
            "object": "response",
            "status": "completed",
            "output": [openai_reasoning_input(new_b)],
            "usage": {"input_tokens": 6, "output_tokens": 4}
        })),
        ResponseSpec::Json(json!({
            "id": "resp_b_next_direct",
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 5, "output_tokens": 2}
        })),
    ]);

    let authn = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ])
        .build();
    let lc = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatch.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone())
    .with_event_bus(test_bus.bus_arc());

    let resp = handle_authenticated(
        &lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "turn2"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let captured = dispatch.captured_requests();
    assert_eq!(captured.len(), 2);
    assert!(captured[0].0.contains("oauth-a.local"));
    assert!(captured[1].0.contains("oauth-b.local"));
    assert_eq!(captured[1].1["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        store.upstream_for(&ws1296_openai_key(new_b)),
        Some(fallback_b)
    );

    tokio::time::timeout(Duration::from_secs(3), quota_cache.ingested.notified())
        .await
        .expect("429 quota headers ingested");

    // Next request referencing `owned_a` must route directly to B without hitting A again.
    let resp2 = handle_authenticated(
        &lc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "turn3"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let captured = dispatch.captured_requests();
    assert_eq!(captured.len(), 3);
    assert!(captured[2].0.contains("oauth-b.local"));
}

#[tokio::test]
async fn strip_and_failover_http_401_and_open_breaker_fail_over_to_b() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000421").unwrap();
    let fallback_b = Uuid::parse_str("10000000-0000-0000-0000-000000000422").unwrap();
    let owned_a = "gAAAAAB_http_401_breaker_a";

    let quota_cache = Arc::new(QaSubscriptionQuotaCache::default());
    quota_cache.set_healthy(owner_a);
    quota_cache.set_healthy(fallback_b);

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_a), owner_a);

    // 1. Live 401 initial + refreshed 401 on A -> strips & fails over to B.
    let dispatch_401 = RecordingDispatch::with_responses(vec![
        ResponseSpec::Status(
            StatusCode::UNAUTHORIZED,
            json!({"error": {"type": "invalid_request_error", "message": "expired"}}),
        ),
        ResponseSpec::Status(
            StatusCode::UNAUTHORIZED,
            json!({"error": {"type": "invalid_request_error", "message": "still expired"}}),
        ),
        ResponseSpec::Json(
            json!({"id": "resp_b_401", "object": "response", "status": "completed", "output": []}),
        ),
    ]);
    let authn_401 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_401 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_401.clone()))
        .principal_view(authn_401.principal_view.clone())
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ])
        .build();
    let lc_401 = Lifecycle::new_with_dynamic_view(
        authn_401.authn,
        Arc::new(DynamicViewHolder::new(view_401)),
        dispatch_401.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_401 = handle_authenticated(
        &lc_401,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp_401.status(), StatusCode::OK);
    let captured_401 = dispatch_401.captured_requests();
    assert_eq!(captured_401.len(), 3);
    assert!(captured_401[0].0.contains("oauth-a.local"));
    assert!(captured_401[1].0.contains("oauth-a.local"));
    assert!(captured_401[2].0.contains("oauth-b.local"));
    assert_eq!(captured_401[2].1["input"].as_array().unwrap().len(), 1);

    // 2. Open circuit breaker (`BreakerError::Open`) on A -> strips & fails over to B.
    let dispatch_breaker = RecordingDispatch::with_responses(vec![
        ResponseSpec::OpenBreaker,
        ResponseSpec::Json(
            json!({"id": "resp_b_breaker", "object": "response", "status": "completed", "output": []}),
        ),
    ]);
    let authn_brk = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_brk = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_brk.clone()))
        .principal_view(authn_brk.principal_view.clone())
        .subscription_quota_cache(quota_cache)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(fallback_b, "oauth-b", true),
        ])
        .build();
    let lc_brk = Lifecycle::new_with_dynamic_view(
        authn_brk.authn,
        Arc::new(DynamicViewHolder::new(view_brk)),
        dispatch_breaker.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store);
    let resp_brk = handle_authenticated(
        &lc_brk,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp_brk.status(), StatusCode::OK);
    let captured_brk = dispatch_breaker.captured_requests();
    assert_eq!(captured_brk.len(), 2);
    assert!(captured_brk[0].0.contains("oauth-a.local"));
    assert!(captured_brk[1].0.contains("oauth-b.local"));
    assert_eq!(captured_brk[1].1["input"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn strip_and_failover_http_negative_controls_relay_429_never_409_or_downgrade() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000431").unwrap();
    let exhausted_b = Uuid::parse_str("10000000-0000-0000-0000-000000000432").unwrap();
    let api_key_a = Uuid::parse_str("10000000-0000-0000-0000-000000000433").unwrap();
    let owned_a = "gAAAAAB_http_neg_a";
    let owned_api = "gAAAAAB_http_neg_api";
    let rate_limit_body = json!({"error": {"type": "rate_limit_error", "code": "rate_limit_exceeded", "message": "real 429"}});

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(owned_a), owner_a);
    store.insert(ws1296_openai_key(owned_api), api_key_a);

    // 1. Both A and B exhausted: must NOT strip, must NOT return 409, must dispatch A verbatim & relay 429.
    let quota_cache = Arc::new(QaSubscriptionQuotaCache::default());
    quota_cache.set_exhausted(owner_a);
    quota_cache.set_exhausted(exhausted_b);
    let dispatch_both_exhausted = RecordingDispatch::with_response(ResponseSpec::Status(
        StatusCode::TOO_MANY_REQUESTS,
        rate_limit_body.clone(),
    ));
    let authn_1 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_1 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_1.clone()))
        .principal_view(authn_1.principal_view.clone())
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(exhausted_b, "oauth-b", true),
        ])
        .build();
    let lc_1 = Lifecycle::new_with_dynamic_view(
        authn_1.authn,
        Arc::new(DynamicViewHolder::new(view_1)),
        dispatch_both_exhausted.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_1 = handle_authenticated(
        &lc_1,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp_1.status(), StatusCode::TOO_MANY_REQUESTS);
    let cap_1 = dispatch_both_exhausted.captured_requests();
    assert_eq!(cap_1.len(), 1);
    assert!(cap_1[0].0.contains("oauth-a.local"));
    assert_eq!(cap_1[0].1["input"].as_array().unwrap().len(), 2);

    // 2. `unavailable_openai_reasoning_owner = FailClosed` with healthy B stays on A & relays 429.
    quota_cache.set_healthy(owner_a);
    quota_cache.set_healthy(exhausted_b);
    let dispatch_fc = RecordingDispatch::with_response(ResponseSpec::Status(
        StatusCode::TOO_MANY_REQUESTS,
        rate_limit_body.clone(),
    ));
    let authn_2 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_2 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_2.clone()))
        .principal_view(authn_2.principal_view.clone())
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(exhausted_b, "oauth-b", true),
        ])
        .build();
    let lc_fc = Lifecycle::new_with_dynamic_view(
        authn_2.authn,
        Arc::new(DynamicViewHolder::new(view_2)),
        dispatch_fc.clone(),
        LifecycleConfig {
            unavailable_openai_reasoning_owner:
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::FailClosed,
            ..LifecycleConfig::default()
        },
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_fc = handle_authenticated(
        &lc_fc,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [openai_reasoning_input(owned_a), {"role": "user", "content": "hi"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp_fc.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(dispatch_fc.captured_requests().len(), 1);

    // 3. Public `/v1/responses` with `code_interpreter`: API-key A returning 429 must never downgrade onto OAuth B.
    let dispatch_ci = RecordingDispatch::with_response(ResponseSpec::Status(
        StatusCode::TOO_MANY_REQUESTS,
        rate_limit_body.clone(),
    ));
    let authn_3 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_3 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_3.clone()))
        .principal_view(authn_3.principal_view.clone())
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_upstream_record(api_key_a, "api-a", true),
            openai_oauth_upstream_record(exhausted_b, "oauth-b", true),
        ])
        .build();
    let lc_ci = Lifecycle::new_with_dynamic_view(
        authn_3.authn,
        Arc::new(DynamicViewHolder::new(view_3)),
        dispatch_ci.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_ci = handle_authenticated(
        &lc_ci,
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}],
            "input": [openai_reasoning_input(owned_api), {"role": "user", "content": "calc"}]
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp_ci.status(), StatusCode::TOO_MANY_REQUESTS);
    let cap_ci = dispatch_ci.captured_requests();
    assert_eq!(cap_ci.len(), 1);
    assert!(cap_ci[0].0.contains("api-a.local"));

    // 4. Account-owned state (`compaction`, `file_id`, `previous_response_id`) stays pinned to A on 429 / quota exhaustion.
    for owned_payload in [
        json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [
                {"type": "compaction", "id": "cmp_1", "encrypted_content": owned_a},
                {"role": "user", "content": "hi"}
            ]
        }),
        json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [
                openai_reasoning_input(owned_a),
                {"role": "user", "content": [{"type": "input_file", "file_id": "file-123"}]}
            ]
        }),
    ] {
        let dispatch_owned = RecordingDispatch::with_response(ResponseSpec::Status(
            StatusCode::TOO_MANY_REQUESTS,
            rate_limit_body.clone(),
        ));
        let authn_4 =
            TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
        let view_4 = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(authn_4.clone()))
            .principal_view(authn_4.principal_view.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![
                openai_oauth_upstream_record(owner_a, "oauth-a", true),
                openai_oauth_upstream_record(exhausted_b, "oauth-b", true),
            ])
            .build();
        let lc_4 = Lifecycle::new_with_dynamic_view(
            authn_4.authn,
            Arc::new(DynamicViewHolder::new(view_4)),
            dispatch_owned.clone(),
            LifecycleConfig::default(),
            Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
        )
        .with_upstream_affinity_store(store.clone());
        let resp_owned = handle_authenticated(&lc_4, responses_request(owned_payload))
            .await
            .unwrap();
        assert_eq!(resp_owned.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(dispatch_owned.captured_requests().len(), 1);
    }
}

#[tokio::test]
async fn multi_owner_openai_reasoning_reconciliation_http() {
    let owner_a = Uuid::parse_str("10000000-0000-0000-0000-000000000601").unwrap();
    let owner_b = Uuid::parse_str("10000000-0000-0000-0000-000000000602").unwrap();
    let owner_c = Uuid::parse_str("10000000-0000-0000-0000-000000000603").unwrap();
    let cipher_a = "gAAAAAB_multi_http_a";
    let cipher_b = "gAAAAAB_multi_http_b";

    let store = Arc::new(MemoryAffinityStore::default());
    store.insert(ws1296_openai_key(cipher_a), owner_a);
    store.insert(ws1296_openai_key(cipher_b), owner_b);

    let multi_payload = || {
        responses_request(json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [
                openai_reasoning_input(cipher_a),
                {"role": "user", "content": "turn 1"},
                openai_reasoning_input(cipher_b),
                {"role": "user", "content": "turn 2"}
            ]
        }))
    };

    // 1. `[rs_A (exhausted), rs_B (healthy)]`: routes directly to B, strips ONLY rs_A, keeps rs_B verbatim.
    let quota_1 = Arc::new(QaSubscriptionQuotaCache::default());
    quota_1.set_exhausted(owner_a);
    quota_1.set_healthy(owner_b);
    let dispatch_1 = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_reconcile_1",
        "object": "response",
        "status": "completed",
        "output": []
    })));
    let authn_1 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_1 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_1.clone()))
        .principal_view(authn_1.principal_view.clone())
        .subscription_quota_cache(quota_1)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(owner_b, "oauth-b", true),
        ])
        .build();
    let lc_1 = Lifecycle::new_with_dynamic_view(
        authn_1.authn,
        Arc::new(DynamicViewHolder::new(view_1)),
        dispatch_1.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_1 = handle_authenticated(&lc_1, multi_payload()).await.unwrap();
    assert_eq!(resp_1.status(), StatusCode::OK);
    let cap_1 = dispatch_1.captured_requests();
    assert_eq!(cap_1.len(), 1);
    assert!(cap_1[0].0.contains("oauth-b.local"));
    let in_1 = cap_1[0].1["input"].as_array().unwrap();
    assert_eq!(in_1.len(), 3);
    assert_eq!(in_1[1]["encrypted_content"], cipher_b);

    // 2. `[rs_A (healthy), rs_B (healthy)]`: picks later owner B, strips ONLY rs_A, keeps rs_B verbatim.
    let quota_2 = Arc::new(QaSubscriptionQuotaCache::default());
    quota_2.set_healthy(owner_a);
    quota_2.set_healthy(owner_b);
    let dispatch_2 = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_reconcile_2",
        "object": "response",
        "status": "completed",
        "output": []
    })));
    let authn_2 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_2 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_2.clone()))
        .principal_view(authn_2.principal_view.clone())
        .subscription_quota_cache(quota_2)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(owner_b, "oauth-b", true),
        ])
        .build();
    let lc_2 = Lifecycle::new_with_dynamic_view(
        authn_2.authn,
        Arc::new(DynamicViewHolder::new(view_2)),
        dispatch_2.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_2 = handle_authenticated(&lc_2, multi_payload()).await.unwrap();
    assert_eq!(resp_2.status(), StatusCode::OK);
    let cap_2 = dispatch_2.captured_requests();
    assert_eq!(cap_2.len(), 1);
    assert!(cap_2[0].0.contains("oauth-b.local"));
    let in_2 = cap_2[0].1["input"].as_array().unwrap();
    assert_eq!(in_2.len(), 3);
    assert_eq!(in_2[1]["encrypted_content"], cipher_b);

    // 3. `[rs_A (exhausted), rs_B (exhausted)]` with healthy C: strips ALL and fails over cleanly to C.
    let quota_3 = Arc::new(QaSubscriptionQuotaCache::default());
    quota_3.set_exhausted(owner_a);
    quota_3.set_exhausted(owner_b);
    quota_3.set_healthy(owner_c);
    let dispatch_3 = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_reconcile_3",
        "object": "response",
        "status": "completed",
        "output": []
    })));
    let authn_3 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_3 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_3.clone()))
        .principal_view(authn_3.principal_view.clone())
        .subscription_quota_cache(quota_3)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(owner_b, "oauth-b", true),
            openai_oauth_upstream_record(owner_c, "oauth-c", true),
        ])
        .build();
    let lc_3 = Lifecycle::new_with_dynamic_view(
        authn_3.authn,
        Arc::new(DynamicViewHolder::new(view_3)),
        dispatch_3.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_3 = handle_authenticated(&lc_3, multi_payload()).await.unwrap();
    assert_eq!(resp_3.status(), StatusCode::OK);
    let cap_3 = dispatch_3.captured_requests();
    assert_eq!(cap_3.len(), 1);
    assert!(cap_3[0].0.contains("oauth-c.local"));
    let in_3 = cap_3[0].1["input"].as_array().unwrap();
    assert_eq!(in_3.len(), 2);
    assert_eq!(in_3[0]["content"], "turn 1");
    assert_eq!(in_3[1]["content"], "turn 2");

    // 4. `[rs_A (exhausted), rs_B (exhausted)]` with NO healthy replacement: keeps B (rs_A stripped, rs_B kept) and relays B's real 429.
    let rate_limit_body = json!({"error": {"type": "rate_limit_error", "code": "rate_limit_exceeded", "message": "real 429"}});
    let quota_4 = Arc::new(QaSubscriptionQuotaCache::default());
    quota_4.set_exhausted(owner_a);
    quota_4.set_exhausted(owner_b);
    let dispatch_4 = RecordingDispatch::with_response(ResponseSpec::Status(
        StatusCode::TOO_MANY_REQUESTS,
        rate_limit_body.clone(),
    ));
    let authn_4 = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_4 = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_4.clone()))
        .principal_view(authn_4.principal_view.clone())
        .subscription_quota_cache(quota_4)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(owner_b, "oauth-b", true),
        ])
        .build();
    let lc_4 = Lifecycle::new_with_dynamic_view(
        authn_4.authn,
        Arc::new(DynamicViewHolder::new(view_4)),
        dispatch_4.clone(),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let resp_4 = handle_authenticated(&lc_4, multi_payload()).await.unwrap();
    assert_eq!(resp_4.status(), StatusCode::TOO_MANY_REQUESTS);
    let cap_4 = dispatch_4.captured_requests();
    assert_eq!(cap_4.len(), 1);
    assert!(cap_4[0].0.contains("oauth-b.local"));
    let in_4 = cap_4[0].1["input"].as_array().unwrap();
    assert_eq!(in_4.len(), 3);
    assert_eq!(in_4[1]["encrypted_content"], cipher_b);

    // 5. Negative controls: `FailClosed` OR `compaction` / `previous_response_id` rejects 400 "opaque request content resolves to multiple upstreams".
    let dispatch_neg_fc = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let authn_fc = TestAuthn::with_principal_view(TestState::default(), sub_pref_principal_view());
    let view_fc = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn_fc.clone()))
        .principal_view(authn_fc.principal_view.clone())
        .upstream_records(vec![
            openai_oauth_upstream_record(owner_a, "oauth-a", true),
            openai_oauth_upstream_record(owner_b, "oauth-b", true),
        ])
        .build();
    let lc_neg_fc = Lifecycle::new_with_dynamic_view(
        authn_fc.authn,
        Arc::new(DynamicViewHolder::new(view_fc)),
        dispatch_neg_fc.clone(),
        LifecycleConfig {
            unavailable_openai_reasoning_owner:
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::FailClosed,
            ..LifecycleConfig::default()
        },
        Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS)),
    )
    .with_upstream_affinity_store(store.clone());
    let neg_fc_resp = handle_authenticated(&lc_neg_fc, multi_payload())
        .await
        .unwrap();
    assert_eq!(neg_fc_resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(dispatch_neg_fc.captured_requests().len(), 0);

    for bad_payload in [
        json!({
            "model": "gpt-5.4",
            "store": false,
            "previous_response_id": "resp_prev",
            "input": [openai_reasoning_input(cipher_a), openai_reasoning_input(cipher_b)]
        }),
        json!({
            "model": "gpt-5.4",
            "store": false,
            "input": [
                {"type": "compaction", "id": "cmp_1", "encrypted_content": cipher_a},
                openai_reasoning_input(cipher_b)
            ]
        }),
    ] {
        let neg_bad = handle_authenticated(&lc_2, responses_request(bad_payload))
            .await
            .unwrap();
        assert_eq!(neg_bad.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(dispatch_2.captured_requests().len(), 1);
}
