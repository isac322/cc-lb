use super::*;
use crate::attempt_rail::AttemptIntent;
use crate::cache_keepalive::request_snapshot::RequestSnapshot;
use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use cc_lb_control::PromptCacheObservationSinkLike;
use cc_lb_domain::TtlClass;
use cc_lb_storage_api::PromptCacheObservationRecord;

// Fixed v5 identity and byte length for the three-block fixture below.
const KEEPALIVE_IDENTITY_KEY: &str =
    "5941e0168a65b3e6aa9b3921ee75015614419bbaa1a2f4c3a4c5d002bce58739";

fn accounting_guard() -> ResponseAccountingGuard {
    AttemptIntent::observe_only()
        .into_reserved()
        .into_response_accounting_guard()
}

#[derive(Clone)]
struct TestUpstreamStore {
    record: UpstreamRecord,
}

impl TestUpstreamStore {
    fn new(record: UpstreamRecord) -> Self {
        Self { record }
    }
}

fn unsupported_store_operation<T>() -> cc_lb_storage_api::StorageResult<T> {
    Err(cc_lb_storage_api::StorageError::Unavailable {
        message: "unused test store operation".to_owned(),
    })
}

#[async_trait::async_trait]
impl cc_lb_storage_api::UpstreamStore for TestUpstreamStore {
    async fn create(
        &self,
        _create: cc_lb_storage_api::UpstreamCreate,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn get_by_name(
        &self,
        _name: &str,
    ) -> cc_lb_storage_api::StorageResult<Option<UpstreamRecord>> {
        unsupported_store_operation()
    }

    async fn get_by_id(
        &self,
        id: Uuid,
    ) -> cc_lb_storage_api::StorageResult<Option<UpstreamRecord>> {
        Ok((self.record.id == id).then(|| self.record.clone()))
    }

    async fn list(
        &self,
        _after: Option<Uuid>,
        _limit: usize,
    ) -> cc_lb_storage_api::StorageResult<Vec<UpstreamRecord>> {
        unsupported_store_operation()
    }

    async fn update(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: cc_lb_storage_api::UpstreamUpdate,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn set_enabled(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _enabled: bool,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn update_spec(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: cc_lb_storage_api::UpstreamUpdate,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn update_api_key_secret(
        &self,
        _id: Uuid,
        _api_key_ciphertext: Option<Vec<u8>>,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn update_oauth_token(
        &self,
        _id: Uuid,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn set_status(
        &self,
        _id: Uuid,
        _status: cc_lb_storage_api::UpstreamStatusUpdate,
    ) -> cc_lb_storage_api::StorageResult<()> {
        unsupported_store_operation()
    }

    async fn store_oauth_tokens(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
        _never_refresh: bool,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn complete_refresh(
        &self,
        _id: Uuid,
        _lease_id: Uuid,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> cc_lb_storage_api::StorageResult<UpstreamRecord> {
        unsupported_store_operation()
    }

    async fn set_last_apply_error(
        &self,
        _id: Uuid,
        _error: Option<String>,
    ) -> cc_lb_storage_api::StorageResult<()> {
        unsupported_store_operation()
    }

    async fn soft_delete(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> cc_lb_storage_api::StorageResult<()> {
        unsupported_store_operation()
    }

    async fn hard_delete(&self, _id: Uuid) -> cc_lb_storage_api::StorageResult<()> {
        unsupported_store_operation()
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> cc_lb_storage_api::StorageResult<Option<UpstreamRecord>> {
        unsupported_store_operation()
    }
}

#[derive(Default)]
struct RecordingUpstreamDispatch {
    requests: Mutex<Vec<cc_lb_upstream::SignedRequest>>,
    response: Option<(StatusCode, &'static [u8])>,
}

impl RecordingUpstreamDispatch {
    fn with_response(status: StatusCode, body: &'static [u8]) -> Self {
        Self {
            response: Some((status, body)),
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl UpstreamDispatch for RecordingUpstreamDispatch {
    async fn dispatch(
        &self,
        request: cc_lb_upstream::SignedRequest,
    ) -> Result<Response<crate::lifecycle::Body>, crate::lifecycle::DispatchError> {
        self.requests.lock().unwrap().push(request);
        let (status, body) = self.response.unwrap_or((
            StatusCode::OK,
            br#"{"usage":{"cache_read_input_tokens":1,"input_tokens":1,"output_tokens":0}}"#,
        ));
        let mut response = Response::new(crate::lifecycle::Body::from(Bytes::from_static(body)));
        *response.status_mut() = status;
        Ok(response)
    }
}

struct TestSignerFactory;

impl cc_lb_upstream::ApiKeyAwareSignerFactory for TestSignerFactory {
    fn with_router_choice(
        &self,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn cc_lb_upstream::SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl cc_lb_upstream::SignerFactory for TestSignerFactory {
    async fn build(
        &self,
        _upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
        Ok(Arc::new(TestSigner))
    }
}

struct TestSigner;

#[async_trait::async_trait]
impl cc_lb_upstream::Signer for TestSigner {
    async fn sign(
        &self,
        shaped: cc_lb_upstream::ShapedRequest,
        capability: &mut cc_lb_upstream::SigningCapability,
    ) -> Result<cc_lb_upstream::SignedRequest, cc_lb_upstream::SignerError> {
        Ok(cc_lb_upstream::SignedRequest::from_shaped(
            shaped, capability,
        ))
    }

    async fn on_unauthorized(
        &self,
        _error: &cc_lb_upstream::UpstreamError,
    ) -> cc_lb_upstream::RetryDecision {
        cc_lb_upstream::RetryDecision::Fail
    }
}

#[test]
fn classifies_successful_cache_read_as_hit() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":1234,"input_tokens":0,"output_tokens":0}}"#;

    let outcome = classify_success_body(
        body,
        Duration::from_secs(2),
        Duration::from_millis(50),
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheHit {
            cache_anchor_age,
            finalization,
        } => {
            assert_eq!(cache_anchor_age, Duration::from_secs(2));
            assert_eq!(finalization.usage.cache_read_input_tokens, 1234);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(50));
        }
        DispatchOutcome::CacheMiss { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("expected cache hit"),
    }
}

#[test]
fn classifies_zero_cache_read_as_miss() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":0,"input_tokens":0,"output_tokens":0}}"#;

    let outcome = classify_success_body(
        body,
        Duration::ZERO,
        Duration::from_millis(25),
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheMiss { finalization } => {
            assert_eq!(finalization.usage.cache_read_input_tokens, 0);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(25));
        }
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("expected cache miss"),
    }
}

#[test]
fn classifies_missing_usage_as_miss() {
    let body = br#"{"content":[],"stop_reason":"max_tokens"}"#;

    let outcome = classify_success_body(
        body,
        Duration::ZERO,
        Duration::from_millis(10),
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheMiss { finalization } => {
            assert_eq!(finalization.usage.cache_read_input_tokens, 0);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(10));
        }
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("expected cache miss"),
    }
}

#[test]
fn api_key_upstream_without_stored_key_is_not_keepalive_safe() {
    let upstream = UpstreamRecord {
        kind: UpstreamKind::AnthropicApiKey,
        api_key_ciphertext: None,
        ..UpstreamRecord::default()
    };

    let error = keepalive_upstream_supported(&upstream).unwrap_err();

    assert!(error.contains("AnthropicApiKey"));
    assert!(error.contains("storage-backed signing"));
}

#[test]
fn unsupported_upstream_maps_to_terminal_business_outcome() {
    let error = "unsupported".to_owned();

    let outcome = DispatchOutcome::UnsupportedProvider(error.clone());

    assert!(matches!(
        outcome,
        DispatchOutcome::UnsupportedProvider(reason) if reason == error
    ));
}

#[test]
fn api_key_upstream_with_stored_key_is_not_keepalive_safe_until_storage_signing() {
    let upstream = UpstreamRecord {
        kind: UpstreamKind::AnthropicApiKey,
        api_key_ciphertext: Some(vec![1, 2, 3]),
        ..UpstreamRecord::default()
    };

    let error = keepalive_upstream_supported(&upstream).unwrap_err();

    assert!(error.contains("AnthropicApiKey"));
    assert!(error.contains("storage-backed signing"));
}

#[test]
fn oauth_upstream_is_keepalive_safe_without_api_key_ciphertext() {
    let upstream = UpstreamRecord {
        kind: UpstreamKind::AnthropicOauth,
        api_key_ciphertext: None,
        ..UpstreamRecord::default()
    };

    assert!(keepalive_upstream_supported(&upstream).is_ok());
}

#[test]
fn resolve_keepalive_url_oauth_beta_and_replaced_path() {
    let snapshot = RequestSnapshot::capture(
        "https://api.anthropic.com/v1/messages?foo=1"
            .parse()
            .unwrap(),
        Method::POST,
        HeaderMap::new(),
        Bytes::from_static(br#"{"model":"m"}"#),
        Uuid::nil(),
        cc_lb_storage_api::CacheTtl::Ttl5m,
        usize::MAX,
    )
    .unwrap();
    let base_url = "https://proxy.example.com/prefix".parse().unwrap();

    let oauth = UpstreamRecord {
        kind: UpstreamKind::AnthropicOauth,
        base_url: Some(base_url),
        ..UpstreamRecord::default()
    };
    let resolved = resolve_keepalive_url(&snapshot, &oauth).unwrap();
    assert_eq!(
        resolved.as_str(),
        "https://proxy.example.com/v1/messages?foo=1&beta=true"
    );

    let mut existing_beta = snapshot.clone();
    existing_beta.url = "https://api.anthropic.com/v1/messages?beta=true"
        .parse()
        .unwrap();
    let resolved = resolve_keepalive_url(&existing_beta, &oauth).unwrap();
    assert_eq!(
        resolved.as_str(),
        "https://proxy.example.com/v1/messages?beta=true"
    );

    let api_key = UpstreamRecord {
        kind: UpstreamKind::AnthropicApiKey,
        base_url: Some("https://proxy.example.com/prefix".parse().unwrap()),
        ..UpstreamRecord::default()
    };
    let resolved = resolve_keepalive_url(&snapshot, &api_key).unwrap();
    assert_eq!(
        resolved.as_str(),
        "https://proxy.example.com/v1/messages?foo=1"
    );
}

#[test]
fn resolve_keepalive_url_oauth_none_base_url_adds_beta() {
    let snapshot = RequestSnapshot::capture(
        "https://api.anthropic.com/v1/messages".parse().unwrap(),
        Method::POST,
        HeaderMap::new(),
        Bytes::from_static(br#"{"model":"m"}"#),
        Uuid::nil(),
        cc_lb_storage_api::CacheTtl::Ttl5m,
        usize::MAX,
    )
    .unwrap();

    let oauth = UpstreamRecord {
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        ..UpstreamRecord::default()
    };
    assert_eq!(
        resolve_keepalive_url(&snapshot, &oauth).unwrap().as_str(),
        "https://api.anthropic.com/v1/messages?beta=true"
    );

    let api_key = UpstreamRecord {
        kind: UpstreamKind::AnthropicApiKey,
        base_url: None,
        ..UpstreamRecord::default()
    };
    assert_eq!(
        resolve_keepalive_url(&snapshot, &api_key).unwrap().as_str(),
        "https://api.anthropic.com/v1/messages"
    );
}

#[tokio::test]
async fn oauth_keepalive_dispatch_records_beta_url_and_preserves_thinking() {
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};

    let upstream_id = Uuid::from_u128(17);
    let oauth_record = UpstreamRecord {
        id: upstream_id,
        name: "oauth".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        enabled: true,
        ..UpstreamRecord::default()
    };
    let store = Arc::new(TestUpstreamStore::new(oauth_record.clone()));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(PrincipalView::for_tests(
            "test",
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        )))
        .upstream_records(vec![oauth_record.clone()])
        .build();
    let http = Arc::new(RecordingUpstreamDispatch::default());
    let dispatcher = AnthropicKeepaliveDispatcher::new(
        Arc::new(DynamicViewHolder::new(view)),
        store,
        http.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let snapshot = RequestSnapshot::capture(
        "https://api.anthropic.com/v1/messages".parse().unwrap(),
        Method::POST,
        HeaderMap::new(),
        Bytes::from(
            serde_json::to_vec(&json!({
                "model": "claude-opus-5-5",
                "max_tokens": 8193,
                "thinking": {
                    "type": "adaptive",
                    "display": "summarized"
                },
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "cached",
                        "cache_control": {"type": "ephemeral"}
                    }]
                }]
            }))
            .unwrap(),
        ),
        upstream_id,
        cc_lb_storage_api::CacheTtl::Ttl5m,
        usize::MAX,
    )
    .unwrap();

    let outcome = dispatcher
        .dispatch(
            &snapshot,
            KeepaliveDispatchContext::new(None, "source".to_owned()),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::CacheHit { .. }));
    let requests = http.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].url().as_str(),
        "https://api.anthropic.com/v1/messages?beta=true"
    );
    let body: Value = serde_json::from_slice(requests[0].body()).unwrap();
    assert_eq!(
        body["thinking"],
        json!({"type": "adaptive", "display": "summarized"})
    );
    assert_eq!(body["max_tokens"], json!(0));

    let api_key_record = UpstreamRecord {
        id: upstream_id,
        name: "api-key".to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        enabled: true,
        ..UpstreamRecord::default()
    };
    let api_key_dispatcher = AnthropicKeepaliveDispatcher::new(
        Arc::new(DynamicViewHolder::new(
            DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(TestSignerFactory))
                .principal_view(Arc::new(PrincipalView::for_tests(
                    "test",
                    true,
                    vec!["*".to_owned()],
                    Vec::new(),
                    HashMap::new(),
                )))
                .upstream_records(vec![api_key_record.clone()])
                .build(),
        )),
        Arc::new(TestUpstreamStore::new(api_key_record)),
        http.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let before = requests.len();
    let outcome = api_key_dispatcher
        .dispatch(
            &snapshot,
            KeepaliveDispatchContext::new(None, "source".to_owned()),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::UnsupportedProvider(_)));
    assert_eq!(http.requests.lock().unwrap().len(), before);
}

#[derive(Default)]
struct RecordingKeepaliveObservationSink {
    records: parking_lot::Mutex<Vec<PromptCacheObservationRecord>>,
}

impl PromptCacheObservationSinkLike for RecordingKeepaliveObservationSink {
    fn enqueue(
        &self,
        record: PromptCacheObservationRecord,
    ) -> Result<(), cc_lb_control::PromptCacheObservationEnqueueError> {
        self.records.lock().push(record);
        Ok(())
    }
}

struct KeepaliveObservationFixture {
    dispatcher: AnthropicKeepaliveDispatcher,
    snapshot: RequestSnapshot,
    http: Arc<RecordingUpstreamDispatch>,
    observations: Arc<RecordingKeepaliveObservationSink>,
}

fn keepalive_observation_fixture(
    status: StatusCode,
    body: &'static [u8],
) -> Result<KeepaliveObservationFixture, Box<dyn std::error::Error>> {
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::dynamic_view::DynamicViewBuilder;

    let upstream = UpstreamRecord {
        id: Uuid::from_u128(1_003),
        name: "keepalive-observation-oauth".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        enabled: true,
        ..UpstreamRecord::default()
    };
    let observations = Arc::new(RecordingKeepaliveObservationSink::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(PrincipalView::for_tests(
            "test",
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        )))
        .upstream_records(vec![upstream.clone()])
        .prompt_cache_observation_sink(observations.clone())
        .prompt_cache_grace_margin_secs(30)
        .build();
    let http = Arc::new(RecordingUpstreamDispatch::with_response(status, body));
    let dispatcher = AnthropicKeepaliveDispatcher::new(
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(TestUpstreamStore::new(upstream.clone())),
        http.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let snapshot = RequestSnapshot::capture(
        "https://api.anthropic.com/v1/messages".parse()?,
        Method::POST,
        HeaderMap::new(),
        Bytes::from(serde_json::to_vec(&json!({
            "model": "claude-sonnet-4-5",
            "max_tokens": 8193,
            "stream": true,
            "thinking": {"type": "adaptive", "display": "summarized"},
            "system": [{
                "type": "text",
                "text": "system ".repeat(1024),
                "cache_control": {"type": "ephemeral", "ttl": "5m"}
            }],
            "messages": [
                {"role": "user", "content": [
                    {
                        "type": "text",
                        "text": "alpha ".repeat(1024),
                        "cache_control": {"type": "ephemeral", "ttl": "5m"}
                    },
                    {
                        "type": "text",
                        "text": "omega ".repeat(1024),
                        "cache_control": {"type": "ephemeral", "ttl": "1h"}
                    }
                ]},
                {"role": "assistant", "content": "uncached trailing message"}
            ]
        }))?),
        upstream.id,
        // The resolved deepest breakpoint, not this scheduling TTL, owns the observation.
        cc_lb_storage_api::CacheTtl::Ttl5m,
        usize::MAX,
    )?;
    Ok(KeepaliveObservationFixture {
        dispatcher,
        snapshot,
        http,
        observations,
    })
}

fn expected_keepalive_identity_body() -> Value {
    // This oracle does not call build_keepalive_body or the prompt-cache analyzer.
    json!({
        "model": "claude-sonnet-4-5",
        "max_tokens": 0,
        "thinking": {"type": "adaptive", "display": "summarized"},
        "system": [{
            "type": "text",
            "text": "system ".repeat(1024),
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }],
        "messages": [{"role": "user", "content": [
            {
                "type": "text",
                "text": "alpha ".repeat(1024),
                "cache_control": {"type": "ephemeral", "ttl": "5m"}
            },
            {
                "type": "text",
                "text": "omega ".repeat(1024),
                "cache_control": {"type": "ephemeral", "ttl": "1h"}
            }
        ]}]
    })
}

#[tokio::test]
async fn oauth_keepalive_publishes_exact_dispatched_prefix_before_finalization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = keepalive_observation_fixture(
        StatusCode::OK,
        br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0,"input_tokens":7,"output_tokens":0}}"#,
    )?;
    let before_dispatch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "observation-identity".to_owned()),
        )
        .await;
    let after_dispatch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let DispatchOutcome::CacheHit { finalization, .. } = &outcome else {
        panic!("affirmative read-only usage must remain a cache hit");
    };
    assert_eq!(finalization.usage.cache_read_input_tokens, 1234);
    assert_eq!(finalization.usage.cache_creation_input_tokens, 0);
    assert_eq!(finalization.usage.input_tokens, 7);
    assert_eq!(finalization.usage.output_tokens, 0);
    assert_eq!(finalization.status, 200);

    let requests = fixture
        .http
        .requests
        .lock()
        .map_err(|_| "recording HTTP requests lock poisoned")?
        .clone();
    let [request] = requests.as_slice() else {
        panic!("keepalive must dispatch exactly once");
    };
    assert_eq!(request.method(), &Method::POST);
    assert_eq!(
        request.url().as_str(),
        "https://api.anthropic.com/v1/messages?beta=true"
    );
    assert_eq!(
        request.body().as_ref(),
        serde_json::to_vec(&expected_keepalive_identity_body())?.as_slice()
    );

    // No finalizer or scheduler runs in this fixture; publication belongs to dispatch.
    let records = fixture.observations.records.lock();
    let [record] = records.as_slice() else {
        panic!(
            "read-only keepalive must publish exactly one resolved breakpoint before finalization; got {}",
            records.len()
        );
    };
    assert_eq!(record.upstream_id, Uuid::from_u128(1_003));
    assert_eq!(record.canonical_model_id, "claude-sonnet-4-5-20250929");
    assert_eq!(record.v3_prefix_key, KEEPALIVE_IDENTITY_KEY);
    assert_eq!(record.ttl_class, TtlClass::Ephemeral1h);
    assert_eq!(record.prefix_content_block_index, 2);
    assert_eq!(record.estimated_prefix_tokens, 19_821);
    assert_eq!(record.token_estimate_source, "serialized_prefix_bytes_v1");
    assert_eq!(record.hash_schema_version, 5);
    assert!(record.last_observed_at_unix_secs >= before_dispatch);
    assert!(record.last_observed_at_unix_secs <= after_dispatch);
    assert_eq!(
        record.expires_at_unix_secs,
        record.last_observed_at_unix_secs + 3600 - 30
    );
    Ok(())
}

#[tokio::test]
async fn oauth_keepalive_malformed_unrelated_usage_never_publishes_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = keepalive_observation_fixture(
        StatusCode::OK,
        br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0,"input_tokens":null,"output_tokens":0}}"#,
    )?;
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "malformed-unrelated-usage".to_owned()),
        )
        .await;

    assert!(
        matches!(outcome, DispatchOutcome::Error(_)),
        "malformed unrelated usage must remain an error"
    );
    assert_eq!(
        fixture
            .http
            .requests
            .lock()
            .map_err(|_| "recording HTTP requests lock poisoned")?
            .len(),
        1,
        "malformed success must dispatch exactly once"
    );
    assert!(
        fixture.observations.records.lock().is_empty(),
        "malformed unrelated usage must not publish a cache observation"
    );
    Ok(())
}

#[tokio::test]
async fn oauth_keepalive_suffix_cache_creation_evidence_controls_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let zero_suffix = keepalive_observation_fixture(
        StatusCode::OK,
        br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":0}}"#,
    )?;
    let outcome = zero_suffix
        .dispatcher
        .dispatch(
            &zero_suffix.snapshot,
            KeepaliveDispatchContext::new(None, "suffix-zero".to_owned()),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::CacheHit { .. }));
    assert_eq!(zero_suffix.observations.records.lock().len(), 1);

    let suffix_cases: [(&str, &[u8]); 3] = [
        (
            "suffix-positive",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens_5m":2,"cache_creation_input_tokens_1h":3}}"#,
        ),
        (
            "suffix-partial",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens_5m":0}}"#,
        ),
        (
            "suffix-contradictory-flat",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0,"cache_creation_input_tokens_5m":1,"cache_creation_input_tokens_1h":0}}"#,
        ),
    ];
    for (name, body) in suffix_cases {
        let fixture = keepalive_observation_fixture(StatusCode::OK, body)?;
        let outcome = fixture
            .dispatcher
            .dispatch(
                &fixture.snapshot,
                KeepaliveDispatchContext::new(None, name.to_owned()),
            )
            .await;
        assert!(
            matches!(outcome, DispatchOutcome::CacheHit { .. }),
            "{name}"
        );
        assert!(
            fixture.observations.records.lock().is_empty(),
            "{name} must not publish cache observations"
        );
    }
    Ok(())
}

#[tokio::test]
async fn oauth_keepalive_ambiguous_usage_preserves_classification_without_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let cases: &[(&str, &'static [u8], &str, u64, u64)] = &[
        (
            "mixed read and creation",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":48}}"#,
            "hit",
            1234,
            48,
        ),
        (
            "read without explicit creation",
            br#"{"usage":{"cache_read_input_tokens":1234}}"#,
            "hit",
            1234,
            0,
        ),
        (
            "zero read",
            br#"{"usage":{"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
            "miss",
            0,
            0,
        ),
        ("missing usage", br#"{}"#, "miss", 0, 0),
        ("null usage", br#"{"usage":null}"#, "miss", 0, 0),
        ("empty usage", br#"{"usage":{}}"#, "miss", 0, 0),
        (
            "missing read",
            br#"{"usage":{"cache_creation_input_tokens":0}}"#,
            "miss",
            0,
            0,
        ),
        (
            "null creation",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":null}}"#,
            "error",
            0,
            0,
        ),
        (
            "string creation",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":"0"}}"#,
            "error",
            0,
            0,
        ),
        (
            "negative creation",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":-1}}"#,
            "error",
            0,
            0,
        ),
        (
            "null read",
            br#"{"usage":{"cache_read_input_tokens":null,"cache_creation_input_tokens":0}}"#,
            "error",
            0,
            0,
        ),
        (
            "string read",
            br#"{"usage":{"cache_read_input_tokens":"1234","cache_creation_input_tokens":0}}"#,
            "error",
            0,
            0,
        ),
        (
            "negative read",
            br#"{"usage":{"cache_read_input_tokens":-1,"cache_creation_input_tokens":0}}"#,
            "error",
            0,
            0,
        ),
        (
            "fractional read",
            br#"{"usage":{"cache_read_input_tokens":1.5,"cache_creation_input_tokens":0}}"#,
            "error",
            0,
            0,
        ),
        ("invalid usage shape", br#"{"usage":true}"#, "error", 0, 0),
        ("invalid response JSON", b"{", "error", 0, 0),
    ];

    for &(name, body, expected, expected_read, expected_creation) in cases {
        let fixture = keepalive_observation_fixture(StatusCode::OK, body)?;
        let outcome = fixture
            .dispatcher
            .dispatch(
                &fixture.snapshot,
                KeepaliveDispatchContext::new(None, name.to_owned()),
            )
            .await;
        let classification = match &outcome {
            DispatchOutcome::CacheHit { finalization, .. } => {
                assert_eq!(
                    finalization.usage.cache_read_input_tokens, expected_read,
                    "{name}"
                );
                assert_eq!(
                    finalization.usage.cache_creation_input_tokens, expected_creation,
                    "{name}"
                );
                assert_eq!(finalization.status, 200, "{name}");
                "hit"
            }
            DispatchOutcome::CacheMiss { finalization } => {
                assert_eq!(
                    finalization.usage.cache_read_input_tokens, expected_read,
                    "{name}"
                );
                assert_eq!(
                    finalization.usage.cache_creation_input_tokens, expected_creation,
                    "{name}"
                );
                assert_eq!(finalization.status, 200, "{name}");
                "miss"
            }
            DispatchOutcome::Error(_) => "error",
            DispatchOutcome::UnsupportedProvider(_) => "unsupported",
        };
        assert_eq!(classification, expected, "{name}");
        assert!(
            fixture.observations.records.lock().is_empty(),
            "{name} must not invent a cache observation"
        );
        assert_eq!(
            fixture
                .http
                .requests
                .lock()
                .map_err(|_| "recording HTTP requests lock poisoned")?
                .len(),
            1,
            "{name}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn oauth_keepalive_provider_errors_never_publish_success_shaped_usage()
-> Result<(), Box<dyn std::error::Error>> {
    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
    ] {
        let fixture = keepalive_observation_fixture(
            status,
            br#"{"error":{"type":"api_error","message":"provider failure"},"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0}}"#,
        )?;
        let outcome = fixture
            .dispatcher
            .dispatch(
                &fixture.snapshot,
                KeepaliveDispatchContext::new(None, "provider-error".to_owned()),
            )
            .await;
        let DispatchOutcome::Error(reason) = outcome else {
            panic!("non-200 provider response must remain an error");
        };
        assert!(reason.contains(&format!("status={}", status.as_u16())));
        assert!(reason.contains("provider failure"));
        assert!(
            fixture.observations.records.lock().is_empty(),
            "{status} must not publish even when its body contains affirmative usage"
        );
        assert_eq!(
            fixture
                .http
                .requests
                .lock()
                .map_err(|_| "recording HTTP requests lock poisoned")?
                .len(),
            1,
            "provider errors must not introduce an authorization retry"
        );
    }
    Ok(())
}
