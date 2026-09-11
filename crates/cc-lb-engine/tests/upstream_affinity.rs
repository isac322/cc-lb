use crate::common::{
    RecordingHook, TestAuthn, TestRouter, TestState, collect_body, messages_request,
};

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{
    DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig, TestClock,
    UpstreamDispatch,
};
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamAffinityBinding, UpstreamAffinityKey,
    UpstreamAffinityKind, UpstreamAffinityStore,
};
use cc_lb_upstream::SignedRequest;
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
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
    SseWithEncoding {
        body: Bytes,
        content_encoding: &'static str,
    },
}

#[derive(Default)]
struct RecordingDispatch {
    responses: Mutex<VecDeque<ResponseSpec>>,
    urls: Mutex<Vec<String>>,
}

impl RecordingDispatch {
    fn with_response(response: ResponseSpec) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(VecDeque::from([response])),
            urls: Mutex::new(Vec::new()),
        })
    }

    fn call_count(&self) -> usize {
        self.urls.lock().expect("dispatch URL lock").len()
    }
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.urls
            .lock()
            .expect("dispatch URL lock")
            .push(request.url().to_string());
        let response = self
            .responses
            .lock()
            .expect("dispatch response lock")
            .pop_front()
            .unwrap_or_else(|| ResponseSpec::Json(json!({"type":"message"})));
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
            ResponseSpec::SseWithEncoding {
                body,
                content_encoding,
            } => (
                "text/event-stream",
                Some(content_encoding),
                Body::from(body),
            ),
        };
        let mut response = Response::new(body);
        *response.status_mut() = StatusCode::OK;
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
struct MemoryAffinityStore {
    bindings: Mutex<HashMap<UpstreamAffinityKey, UpstreamAffinityBinding>>,
    resolve_failure: Mutex<Option<String>>,
    bind_failure: Mutex<Option<String>>,
}

impl MemoryAffinityStore {
    fn insert(&self, key: UpstreamAffinityKey, upstream_id: Uuid) {
        self.insert_binding(UpstreamAffinityBinding {
            key,
            upstream_id,
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

    fn len(&self) -> usize {
        self.bindings.lock().expect("affinity binding lock").len()
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
            .cloned()
            .collect())
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
        id,
        name: name.to_owned(),
        kind: cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse(&format!("http://{name}.local/")).expect("test URL parses")),
        enabled,
        oauth_credentials: None,
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
    }
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
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://unused.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
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
async fn t2__legacy_null_binding_expires_at_retention_boundary_without_read_extension() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = Arc::new(MemoryAffinityStore::default());
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("retention-boundary"),
        upstream_id: origin,
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

    let unexpired = lifecycle
        .handle(messages_request(opaque_request(
            &["retention-boundary"],
            false,
        )))
        .await
        .expect("unexpired request handled");
    assert_eq!(unexpired.status(), StatusCode::OK);
    assert_eq!(
        dispatch.urls.lock().expect("dispatch URL lock").as_slice(),
        &["http://origin.local/v1/messages".to_owned()]
    );

    clock.advance_secs(1);
    let expired = lifecycle
        .handle(messages_request(opaque_request(
            &["retention-boundary"],
            false,
        )))
        .await
        .expect("expired request handled");
    assert_eq!(expired.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn t2__memory_affinity_purge_removes_explicit_and_retention_expiry_in_bounded_batches() {
    let origin = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let store = MemoryAffinityStore::default();
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("active"),
        upstream_id: origin,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
        expires_at_unix_secs: None,
    });
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("retention-expired"),
        upstream_id: origin,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS - 10,
        expires_at_unix_secs: None,
    });
    store.insert_binding(UpstreamAffinityBinding {
        key: affinity_key("explicit-expired"),
        upstream_id: origin,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS,
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
async fn t2__memory_affinity_bind_replaces_expired_target_without_waiting_for_purge() {
    let old = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let new = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let key = affinity_key("expired-rebind");
    let store = MemoryAffinityStore::default();
    store.insert_binding(UpstreamAffinityBinding {
        key: key.clone(),
        upstream_id: old,
        observed_at_unix_secs: TEST_NOW_UNIX_SECS - 10,
        expires_at_unix_secs: None,
    });

    store
        .bind_upstream_affinities(
            &[UpstreamAffinityBinding {
                key: key.clone(),
                upstream_id: new,
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
async fn t2__response_learning_pins_a_later_request_and_learns_pending_keys() {
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

    let learned_response = learning
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("learning request handled");
    assert_eq!(learned_response.status(), StatusCode::OK);
    assert_eq!(
        store.upstream_for(&affinity_key("opaque-known")),
        Some(origin)
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
    let response = pinned
        .handle(messages_request(opaque_request(
            &["opaque-known", "opaque-pending"],
            false,
        )))
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
async fn t2__maximum_key_request_resolves_from_one_known_key_and_learns_the_rest() {
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

    let response = lifecycle
        .handle(messages_request(opaque_request(&ciphertext_refs, false)))
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
async fn t2__opaque_requests_fail_closed_before_dispatch_when_affinity_is_not_usable() {
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
        unknown
            .handle(messages_request(opaque_request(&["unknown"], false)))
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
        conflict
            .handle(messages_request(opaque_request(
                &["to-first", "to-second"],
                false,
            )))
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
        unavailable
            .handle(messages_request(opaque_request(&["no-store"], false)))
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
    let target_response = target_unavailable
        .handle(messages_request(opaque_request(
            &["disabled-target"],
            false,
        )))
        .await
        .expect("target-unavailable request handled");
    assert_eq!(target_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(target_dispatch.call_count(), 0);
}

#[tokio::test]
async fn t2__lookup_and_bind_failures_do_not_expose_ciphertext() {
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
        lookup
            .handle(messages_request(opaque_request(
                &[lookup_ciphertext],
                false,
            )))
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
        bind.handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("bind failure handled"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!String::from_utf8_lossy(&bind_body).contains(bind_ciphertext));
    assert_eq!(bind_dispatch.call_count(), 1);
}

#[tokio::test]
async fn t2__sse_bind_failure_suppresses_the_encrypted_content_block() {
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
        lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )))
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
#[tokio::test]
async fn t2__ordinary_non_json_sse_without_affinity_signal_passes_through() {
    let upstream = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let raw = Bytes::from_static(b"event: ping\ndata: hello\n\n");
    let dispatch = RecordingDispatch::with_response(ResponseSpec::Sse(raw.clone()));
    let lifecycle = lifecycle(
        vec![upstream_record(upstream, "origin", true)],
        dispatch.clone(),
        Some(Arc::new(MemoryAffinityStore::default())),
    );

    let (status, _, body) = collect_body(
        lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true}"#,
            )))
            .await
            .expect("ordinary SSE response handled"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, raw);
    assert_eq!(dispatch.call_count(), 1);
}

#[tokio::test]
async fn t2__request_key_limit_rejects_before_dispatch() {
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
        lifecycle
            .handle(messages_request(request))
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
async fn t2__buffered_response_key_limit_fails_closed() {
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
        lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )))
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
async fn t2__sse_response_key_limit_fails_closed_before_event_delivery() {
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
        lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )))
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
async fn t2__incomplete_sse_budget_overflow_does_not_passthrough_opaque_bytes() {
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
        lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )))
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
async fn t2__malformed_and_uninspectable_success_sse_fail_closed() {
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
        malformed_lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )))
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
        encoded_lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
            )))
            .await
            .expect("unsupported SSE encoding handled"),
    )
    .await;
    let encoded_body = String::from_utf8_lossy(&encoded_body);
    assert!(encoded_body.contains("event: error"));
    assert!(!encoded_body.contains(encoded_ciphertext));
}

#[tokio::test]
async fn t2__buffered_body_frame_error_discards_partial_opaque_body() {
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
        lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )))
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
