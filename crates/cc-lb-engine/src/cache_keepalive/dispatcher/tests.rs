use super::*;
use crate::attempt_rail::AttemptIntent;
use crate::cache_keepalive::request_snapshot::RequestSnapshot;
use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

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
}

#[async_trait::async_trait]
impl UpstreamDispatch for RecordingUpstreamDispatch {
    async fn dispatch(
        &self,
        request: cc_lb_upstream::SignedRequest,
    ) -> Result<Response<crate::lifecycle::Body>, crate::lifecycle::DispatchError> {
        self.requests.lock().unwrap().push(request);
        Ok(Response::new(crate::lifecycle::Body::from(
            Bytes::from_static(
                br#"{"usage":{"cache_read_input_tokens":1,"input_tokens":1,"output_tokens":0}}"#,
            ),
        )))
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
fn nested_anthropic_cache_creation_preserves_one_hour_accounting() {
    let body = br#"{
        "content": [],
        "stop_reason": "max_tokens",
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 42,
            "cache_creation": {
                "ephemeral_5m_input_tokens": 0,
                "ephemeral_1h_input_tokens": 42
            }
        }
    }"#;

    let outcome = classify_success_body(
        body,
        Duration::ZERO,
        Duration::from_millis(1),
        accounting_guard(),
    );
    let finalization = match outcome {
        DispatchOutcome::CacheMiss { finalization } => finalization,
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("nested cache creation should be a miss"),
    };
    let canonical = crate::usage_parser::observe_non_stream_json_body(body);
    println!(
        "keepalive classifier split: total={} 5m={} 1h={}; canonical: total={} 5m={} 1h={}",
        finalization.usage.cache_creation_input_tokens,
        finalization.usage.cache_creation_input_tokens_5m,
        finalization.usage.cache_creation_input_tokens_1h,
        canonical.usage.cache_creation_input_tokens,
        canonical.usage.cache_creation_input_tokens_5m,
        canonical.usage.cache_creation_input_tokens_1h
    );

    assert_eq!(canonical.usage.cache_creation_input_tokens, 42);
    assert_eq!(canonical.usage.cache_creation_input_tokens_5m, 0);
    assert_eq!(canonical.usage.cache_creation_input_tokens_1h, 42);
    assert_eq!(finalization.usage.cache_creation_input_tokens, 42);
    assert_eq!(finalization.usage.cache_creation_input_tokens_5m, 0);
    assert_eq!(finalization.usage.cache_creation_input_tokens_1h, 42);
}

#[test]
fn classifies_cache_creation_with_canonical_ttl_split() {
    let cases = [
        (
            "mixed TTLs",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":49,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":42}}}"#,
            (49, 7, 42),
        ),
        (
            "nested only",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":42}}}"#,
            (49, 7, 42),
        ),
        (
            "nested overrides conflicting flat total",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":999,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":42}}}"#,
            (49, 7, 42),
        ),
        (
            "flat only",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":42}}"#,
            (42, 42, 0),
        ),
        (
            "empty nested falls back",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":42,"cache_creation":{}}}"#,
            (42, 42, 0),
        ),
        (
            "zero nested falls back",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":42,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}"#,
            (42, 42, 0),
        ),
        (
            "all zero",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}"#,
            (0, 0, 0),
        ),
        (
            "nested sum saturates",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation":{"ephemeral_5m_input_tokens":18446744073709551615,"ephemeral_1h_input_tokens":42}}}"#,
            (u64::MAX, u64::MAX, 42),
        ),
        (
            "null nested falls back",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":42,"cache_creation":null}}"#,
            (42, 42, 0),
        ),
        (
            "nonobject nested falls back",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":42,"cache_creation":[]}}"#,
            (42, 42, 0),
        ),
        (
            "invalid nested counters fall back",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":42,"cache_creation":{"ephemeral_5m_input_tokens":"7","ephemeral_1h_input_tokens":-42}}}"#,
            (42, 42, 0),
        ),
        (
            "invalid nested counters without flat stay zero",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation":{"ephemeral_5m_input_tokens":1.5,"ephemeral_1h_input_tokens":null}}}"#,
            (0, 0, 0),
        ),
        (
            "valid one-hour counter overrides invalid five-minute counter",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":999,"cache_creation":{"ephemeral_5m_input_tokens":{},"ephemeral_1h_input_tokens":42}}}"#,
            (42, 0, 42),
        ),
        (
            "valid five-minute counter overrides invalid one-hour counter",
            r#"{"usage":{"input_tokens":11,"output_tokens":3,"cache_creation_input_tokens":999,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":true}}}"#,
            (7, 7, 0),
        ),
    ];

    for (name, body, expected) in cases {
        let outcome = classify_success_body(
            body.as_bytes(),
            Duration::ZERO,
            Duration::from_millis(25),
            accounting_guard(),
        );
        let finalization = match outcome {
            DispatchOutcome::CacheMiss { finalization } => finalization,
            DispatchOutcome::CacheHit { .. }
            | DispatchOutcome::UnsupportedProvider(_)
            | DispatchOutcome::Error(_) => panic!("{name}: expected cache miss"),
        };
        let usage = &finalization.usage;
        assert_eq!(
            (
                usage.cache_creation_input_tokens,
                usage.cache_creation_input_tokens_5m,
                usage.cache_creation_input_tokens_1h,
            ),
            expected,
            "{name}"
        );
        let canonical = crate::usage_parser::observe_non_stream_json_body(body.as_bytes());
        assert_eq!(
            (
                canonical.usage.cache_creation_input_tokens,
                canonical.usage.cache_creation_input_tokens_5m,
                canonical.usage.cache_creation_input_tokens_1h,
            ),
            expected,
            "{name}: canonical contract"
        );
        assert_eq!(
            (
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_input_tokens
            ),
            (11, 3, 0),
            "{name}"
        );
        assert_eq!(finalization.status, StatusCode::OK.as_u16(), "{name}");
        assert_eq!(finalization.duration, Duration::from_millis(25), "{name}");
    }
}

#[test]
fn classifies_null_usage_as_zero_usage_miss() {
    let outcome = classify_success_body(
        br#"{"content":[],"stop_reason":"max_tokens","usage":null}"#,
        Duration::ZERO,
        Duration::from_millis(10),
        accounting_guard(),
    );
    let finalization = match outcome {
        DispatchOutcome::CacheMiss { finalization } => finalization,
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("null usage should be a miss"),
    };
    let usage = &finalization.usage;
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_creation_input_tokens,
            usage.cache_creation_input_tokens_5m,
            usage.cache_creation_input_tokens_1h,
            usage.cache_read_input_tokens,
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(finalization.status, StatusCode::OK.as_u16());
    assert_eq!(finalization.duration, Duration::from_millis(10));
}

#[test]
fn classifies_mixed_cache_read_and_creation_as_hit() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"input_tokens":11,"output_tokens":3,"cache_read_input_tokens":1234,"cache_creation_input_tokens":49,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":42}}}"#;
    let outcome = classify_success_body(
        body,
        Duration::from_secs(2),
        Duration::from_millis(50),
        accounting_guard(),
    );
    let finalization = match outcome {
        DispatchOutcome::CacheHit {
            cache_anchor_age,
            finalization,
        } => {
            assert_eq!(cache_anchor_age, Duration::from_secs(2));
            finalization
        }
        DispatchOutcome::CacheMiss { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("positive cache read should remain a hit"),
    };
    let usage = &finalization.usage;
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_creation_input_tokens,
            usage.cache_creation_input_tokens_5m,
            usage.cache_creation_input_tokens_1h,
            usage.cache_read_input_tokens,
        ),
        (11, 3, 49, 7, 42, 1234)
    );
    assert_eq!(finalization.status, StatusCode::OK.as_u16());
    assert_eq!(finalization.duration, Duration::from_millis(50));
    assert_eq!(finalization.accounting_guard.reservation_id(), None);
}

#[test]
fn rejects_malformed_success_usage_without_relaxing_flat_counters() {
    let cases = [
        ("invalid JSON", r#"{"usage":"#),
        ("nonobject usage", r#"{"usage":true}"#),
        ("string input", r#"{"usage":{"input_tokens":"11"}}"#),
        ("null input", r#"{"usage":{"input_tokens":null}}"#),
        ("negative output", r#"{"usage":{"output_tokens":-3}}"#),
        (
            "fractional read",
            r#"{"usage":{"cache_read_input_tokens":1.5}}"#,
        ),
        (
            "string flat creation",
            r#"{"usage":{"cache_creation_input_tokens":"42"}}"#,
        ),
        (
            "out-of-range flat creation",
            r#"{"usage":{"cache_creation_input_tokens":18446744073709551616}}"#,
        ),
        (
            "null flat five-minute",
            r#"{"usage":{"cache_creation_input_tokens_5m":null}}"#,
        ),
        (
            "string flat one-hour",
            r#"{"usage":{"cache_creation_input_tokens_1h":"42"}}"#,
        ),
    ];
    for (name, body) in cases {
        let outcome = classify_success_body(
            body.as_bytes(),
            Duration::ZERO,
            Duration::from_millis(1),
            accounting_guard(),
        );
        assert!(matches!(outcome, DispatchOutcome::Error(_)), "{name}");
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
            let usage = &finalization.usage;
            assert_eq!(
                (
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_creation_input_tokens,
                    usage.cache_creation_input_tokens_5m,
                    usage.cache_creation_input_tokens_1h,
                    usage.cache_read_input_tokens,
                ),
                (0, 0, 0, 0, 0, 0)
            );
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
