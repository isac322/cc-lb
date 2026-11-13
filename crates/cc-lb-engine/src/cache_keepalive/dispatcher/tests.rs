use super::*;
use crate::attempt_rail::AttemptIntent;
use crate::cache_keepalive::request_snapshot::RequestSnapshot;
use crate::prompt_cache_simulator::analyze_v3_prompt_cache;
use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use cc_lb_control::PromptCacheObservationSinkLike;
use cc_lb_domain::{Provider, TtlClass};
use cc_lb_storage_api::PromptCacheObservationRecord;

// Schema-6 identity independently derived from the three-block golden body below.
const KEEPALIVE_IDENTITY_KEY: &str =
    "a9dbee437076709a34484806d86886bf7c0d944be8ef721bad98659895c52036";

fn accounting_guard() -> ResponseAccountingGuard {
    AttemptIntent::observe_only()
        .into_reserved()
        .into_response_accounting_guard()
}

#[test]
fn responses_renewal_uses_normalized_usage_and_preserves_native_write_presence() {
    use cc_lb_request_log::RequestCacheEvidence;

    for (details, input, writes, evidence) in [
        (
            serde_json::json!({"cached_tokens": 1024}),
            1024,
            (None, None, None),
            RequestCacheEvidence::Unreported,
        ),
        (
            serde_json::json!({"cached_tokens": 1024, "cache_write_tokens": null}),
            1024,
            (None, None, None),
            RequestCacheEvidence::Unreported,
        ),
        (
            serde_json::json!({"cached_tokens": 1024, "cache_write_tokens": 0}),
            1024,
            (Some(0), None, None),
            RequestCacheEvidence::Zero,
        ),
        (
            serde_json::json!({"cached_tokens": 1024, "cache_write_tokens": 100, "cache_write_tokens_details": {"ttl_30m_tokens": 60, "ttl_12h_tokens": 40}}),
            924,
            (Some(100), Some(60), Some(40)),
            RequestCacheEvidence::Positive,
        ),
        (
            serde_json::json!({"cached_tokens": 1024, "cache_write_tokens": 100}),
            924,
            (Some(100), None, None),
            RequestCacheEvidence::Positive,
        ),
        (
            serde_json::json!({"cached_tokens": 1024, "cache_write_tokens": "invalid"}),
            1024,
            (None, None, None),
            RequestCacheEvidence::Invalid,
        ),
        (
            serde_json::json!({"cached_tokens": 0}),
            2048,
            (None, None, None),
            RequestCacheEvidence::Unreported,
        ),
    ] {
        let mut details = details;
        details["image_tokens"] = serde_json::json!(12);
        let body = serde_json::to_vec(&serde_json::json!({
            "service_tier": "priority",
            "usage": {
                "input_tokens": 2048,
                "output_tokens": 20,
                "input_tokens_details": details,
                "output_tokens_details": {"reasoning_tokens": 7, "image_tokens": 3}
            }
        }))
        .expect("Responses JSON");
        let outcome = classify_success_body_with_surface(
            &body,
            Duration::ZERO,
            Duration::from_millis(7),
            42,
            accounting_guard(),
            Some("responses"),
        );
        let (hit, finalization) = match outcome {
            DispatchOutcome::CacheHit { finalization, .. } => (true, finalization),
            DispatchOutcome::CacheMiss { finalization } => (false, finalization),
            _ => panic!("expected successful Responses renewal"),
        };
        let usage = finalization.usage;
        assert_eq!(hit, usage.cache_read_input_tokens > 0);
        assert_eq!(usage.input_tokens, input);
        assert_eq!(usage.output_tokens, 20);
        assert_eq!(
            usage.cache_read_evidence,
            if hit {
                RequestCacheEvidence::Positive
            } else {
                RequestCacheEvidence::Zero
            }
        );
        assert_eq!(usage.input_image_tokens, Some(12));
        assert_eq!(usage.output_image_tokens, Some(3));
        assert_eq!(usage.thinking_tokens, 7);
        assert_eq!(usage.service_tier.as_deref(), Some("priority"));
        assert_eq!(
            (
                usage.cache_write_input_tokens,
                usage.cache_write_input_tokens_30m,
                usage.cache_write_input_tokens_12h
            ),
            writes
        );
        assert_eq!(usage.cache_write_evidence, evidence);
        assert_eq!(
            usage.provider_cost_unknown,
            evidence == RequestCacheEvidence::Invalid
        );
        assert!(usage.usage_present);
        assert!(usage.is_openai_responses);
        assert_eq!(usage.cache_creation_input_tokens, 0);
        assert_eq!(usage.cache_creation_input_tokens_5m, 0);
        assert_eq!(usage.cache_creation_input_tokens_1h, 0);
    }
}

#[test]
fn responses_renewal_absent_usage_is_not_a_measured_zero() {
    for body in [b"{}".as_slice(), b"{\"usage\":null}".as_slice()] {
        let outcome = classify_success_body_with_surface(
            body,
            Duration::ZERO,
            Duration::ZERO,
            0,
            accounting_guard(),
            Some("responses"),
        );
        let DispatchOutcome::CacheMiss { finalization } = outcome else {
            panic!("absent usage must remain a miss");
        };
        assert!(!finalization.usage.usage_present);
        assert!(finalization.usage.provider_cost_unknown);
        assert_eq!(finalization.usage.cache_write_input_tokens, None);
    }
    assert!(matches!(
        classify_success_body_with_surface(
            b"{", Duration::ZERO, Duration::ZERO, 0, accounting_guard(), Some("responses"),
        ),
        DispatchOutcome::Error(message) if message == "invalid OpenAI Responses JSON"
    ));
    let DispatchOutcome::CacheHit { finalization, .. } = classify_success_body(
        br#"{"usage":{"input_tokens":100,"output_tokens":3,"cache_read_input_tokens":42}}"#,
        Duration::ZERO,
        Duration::ZERO,
        0,
        accounting_guard(),
    ) else {
        panic!("Anthropic cache reads remain hits");
    };
    assert_eq!(finalization.usage.input_tokens, 100);
    assert!(finalization.usage.usage_present);
    assert!(!finalization.usage.is_openai_responses);
    assert!(!finalization.usage.provider_cost_unknown);
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
    delay: Mutex<Option<Duration>>,
}

impl RecordingUpstreamDispatch {
    fn with_response(status: StatusCode, body: &'static [u8]) -> Self {
        Self {
            response: Some((status, body)),
            ..Self::default()
        }
    }

    fn set_delay(&self, delay: Duration) {
        *self.delay.lock().unwrap() = Some(delay);
    }
}

#[async_trait::async_trait]
impl UpstreamDispatch for RecordingUpstreamDispatch {
    async fn dispatch(
        &self,
        request: cc_lb_upstream::SignedRequest,
    ) -> Result<Response<crate::lifecycle::Body>, crate::lifecycle::DispatchError> {
        let delay = *self.delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
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
        0,
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
        | DispatchOutcome::MaxDuration
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
        0,
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheMiss { finalization } => {
            assert_eq!(finalization.usage.cache_read_input_tokens, 0);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(25));
        }
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::MaxDuration
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
        0,
        accounting_guard(),
    );
    let finalization = match outcome {
        DispatchOutcome::CacheMiss { finalization } => finalization,
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::MaxDuration
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
            0,
            accounting_guard(),
        );
        let finalization = match outcome {
            DispatchOutcome::CacheMiss { finalization } => finalization,
            DispatchOutcome::CacheHit { .. }
            | DispatchOutcome::MaxDuration
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
        0,
        accounting_guard(),
    );
    let finalization = match outcome {
        DispatchOutcome::CacheMiss { finalization } => finalization,
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::MaxDuration
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
fn partial_cache_read_and_creation_is_terminal_miss_with_usage_preserved() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"input_tokens":11,"output_tokens":3,"cache_read_input_tokens":1234,"cache_creation_input_tokens":49,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":42}}}"#;
    let outcome = classify_success_body(
        body,
        Duration::from_secs(2),
        Duration::from_millis(50),
        0,
        accounting_guard(),
    );
    let DispatchOutcome::CacheMiss { finalization } = outcome else {
        panic!("rebuilding any protected cached prefix must stop renewals");
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
            0,
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
        0,
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
        | DispatchOutcome::MaxDuration
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
            KeepaliveDispatchContext::new(None, "source".to_owned(), u64::MAX),
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
            KeepaliveDispatchContext::new(None, "source".to_owned(), u64::MAX),
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

fn billing_keepalive_observation_fixture(
    kind: UpstreamKind,
    base_url: Option<&str>,
    header_only: bool,
) -> Result<KeepaliveObservationFixture, Box<dyn std::error::Error>> {
    let mut fixture = keepalive_observation_fixture(
        StatusCode::OK,
        br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0,"input_tokens":7,"output_tokens":0}}"#,
    )?;
    let upstream = UpstreamRecord {
        id: fixture.snapshot.upstream_id,
        name: "keepalive-observation-oauth".to_owned(),
        kind,
        base_url: base_url.map(str::parse).transpose()?,
        enabled: true,
        ..UpstreamRecord::default()
    };
    fixture.dispatcher.upstream_store = Arc::new(TestUpstreamStore::new(upstream));
    fixture.dispatcher.clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
    let mut system = vec![json!({
        "type": "text",
        "text": format!("x-anthropic-billing-header: {}", "attribution ".repeat(2_000)),
        "cache_control": {"type":"ephemeral","ttl":"5m"}
    })];
    if !header_only {
        system.push(json!({
            "type":"text",
            "text":"stable renewal context ".repeat(1_024),
            "cache_control":{"type":"ephemeral","ttl":"1h"}
        }));
    }
    fixture.snapshot = RequestSnapshot::capture(
        "https://api.anthropic.com/v1/messages".parse()?,
        Method::POST,
        HeaderMap::new(),
        Bytes::from(serde_json::to_vec(&json!({
            "model":"claude-sonnet-4-5",
            "max_tokens":1,
            "system":system,
            "messages":[{"role":"user","content":"uncached tail"}]
        }))?),
        fixture.snapshot.upstream_id,
        cc_lb_storage_api::CacheTtl::Ttl5m,
        usize::MAX,
    )?;
    Ok(fixture)
}

async fn assert_billing_keepalive_observation(
    fixture: KeepaliveObservationFixture,
    literal: bool,
    expected_ttl: TtlClass,
) -> Result<(), Box<dyn std::error::Error>> {
    let dispatched_body = fixture.snapshot.build_keepalive_body()?;
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "billing-policy-renewal".to_owned(), u64::MAX),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::CacheHit { .. }));
    let requests = fixture
        .http
        .requests
        .lock()
        .map_err(|_| "HTTP requests poisoned")?;
    let [request] = requests.as_slice() else {
        panic!("renewal must dispatch exactly once");
    };
    assert_eq!(request.body(), &dispatched_body);
    let request_value: Value = serde_json::from_slice(request.body())?;
    let analysis = analyze_v3_prompt_cache(
        &request_value,
        canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5"),
    );
    let expected = if literal {
        analysis.literal_breakpoints.last()
    } else {
        analysis.breakpoints.last()
    }
    .ok_or("expected dispatched-body prefix missing")?;
    if let Some(official) = analysis.breakpoints.last()
        && let Some(custom) = analysis.literal_breakpoints.last()
    {
        assert_ne!(
            official.prefix_key, custom.prefix_key,
            "policy discriminator"
        );
        assert_eq!(
            official.prefix_bytes, custom.prefix_bytes,
            "raw prefix bytes"
        );
    }
    let records = fixture.observations.records.lock();
    let [record] = records.as_slice() else {
        panic!("read-only renewal must publish exactly one eligible owner prefix");
    };
    assert_eq!(record.upstream_id, fixture.snapshot.upstream_id);
    assert_eq!(record.v3_prefix_key, expected.prefix_key);
    assert_eq!(
        record.prefix_content_block_index,
        u32::try_from(expected.block_index)?
    );
    assert_eq!(record.estimated_prefix_tokens, expected.prefix_bytes.get());
    assert_eq!(record.ttl_class, expected_ttl);
    assert_eq!(
        record.hash_schema_version,
        crate::lifecycle::HASH_SCHEMA_VERSION
    );
    assert_eq!(record.last_observed_at_unix_secs, 1_800_000_000);
    let ttl_secs = match expected_ttl {
        TtlClass::Ephemeral5m => 300,
        TtlClass::Ephemeral1h => 3_600,
    };
    assert_eq!(record.expires_at_unix_secs, 1_800_000_000 + ttl_secs - 30);
    Ok(())
}

#[tokio::test]
async fn read_only_keepalive_custom_oauth_refreshes_literal_owner_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    assert_billing_keepalive_observation(
        billing_keepalive_observation_fixture(
            UpstreamKind::AnthropicOauth,
            Some("https://proxy.example.com/anthropic"),
            false,
        )?,
        true,
        TtlClass::Ephemeral1h,
    )
    .await
}

#[tokio::test]
async fn read_only_keepalive_official_oauth_refreshes_excluded_header_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    for base_url in [
        None,
        Some("https://api.anthropic.com/proxy?tenant=one#fragment"),
    ] {
        assert_billing_keepalive_observation(
            billing_keepalive_observation_fixture(UpstreamKind::AnthropicOauth, base_url, false)?,
            false,
            TtlClass::Ephemeral1h,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn read_only_keepalive_custom_header_only_refreshes_literal_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    assert_billing_keepalive_observation(
        billing_keepalive_observation_fixture(
            UpstreamKind::AnthropicOauth,
            Some("https://proxy.example.com"),
            true,
        )?,
        true,
        TtlClass::Ephemeral5m,
    )
    .await
}

#[tokio::test]
async fn read_only_keepalive_official_header_only_has_no_eligible_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = billing_keepalive_observation_fixture(UpstreamKind::AnthropicOauth, None, true)?;
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "official-header-only".to_owned(), u64::MAX),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::CacheHit { .. }));
    assert_eq!(
        fixture
            .http
            .requests
            .lock()
            .map_err(|_| "HTTP requests poisoned")?
            .len(),
        1
    );
    assert!(fixture.observations.records.lock().is_empty());
    Ok(())
}

#[tokio::test]
async fn read_only_keepalive_non_anthropic_rejects_without_dispatch_or_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = billing_keepalive_observation_fixture(
        UpstreamKind::OpenaiOauth,
        Some("https://api.openai.com"),
        false,
    )?;
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "non-anthropic-owner".to_owned(), u64::MAX),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::UnsupportedProvider(_)));
    assert!(
        fixture
            .http
            .requests
            .lock()
            .map_err(|_| "HTTP requests poisoned")?
            .is_empty()
    );
    assert!(fixture.observations.records.lock().is_empty());
    Ok(())
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
            KeepaliveDispatchContext::new(None, "observation-identity".to_owned(), u64::MAX),
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
    assert_eq!(record.hash_schema_version, 6);
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
            KeepaliveDispatchContext::new(None, "malformed-unrelated-usage".to_owned(), u64::MAX),
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
            KeepaliveDispatchContext::new(None, "suffix-zero".to_owned(), u64::MAX),
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
                KeepaliveDispatchContext::new(None, name.to_owned(), u64::MAX),
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
async fn oauth_keepalive_nested_ttl_accounting_preserves_publication_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    type Case = (&'static str, &'static [u8], (u64, u64, u64), usize);
    let cases: &[Case] = &[
        (
            "complete nested zero",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}"#,
            (0, 0, 0),
            1,
        ),
        (
            "nested one-hour creation",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":42,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":42}}}"#,
            (42, 0, 42),
            0,
        ),
        (
            "nested creation overrides zero flat total",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":42}}}"#,
            (49, 7, 42),
            0,
        ),
        (
            "incomplete nested zero is not affirmative evidence",
            br#"{"usage":{"cache_read_input_tokens":1234,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0}}}"#,
            (0, 0, 0),
            0,
        ),
    ];
    for &(name, body, expected_split, expected_observations) in cases {
        let fixture = keepalive_observation_fixture(StatusCode::OK, body)?;
        let outcome = fixture
            .dispatcher
            .dispatch(
                &fixture.snapshot,
                KeepaliveDispatchContext::new(None, name.to_owned(), u64::MAX),
            )
            .await;
        let finalization = match outcome {
            DispatchOutcome::CacheHit { finalization, .. } if expected_split.0 == 0 => finalization,
            DispatchOutcome::CacheMiss { finalization } if expected_split.0 > 0 => finalization,
            _ => panic!("{name}: creation must stop partial preservation"),
        };
        assert_eq!(finalization.usage.cache_read_input_tokens, 1234, "{name}");
        assert_eq!(
            (
                finalization.usage.cache_creation_input_tokens,
                finalization.usage.cache_creation_input_tokens_5m,
                finalization.usage.cache_creation_input_tokens_1h,
            ),
            expected_split,
            "{name}"
        );
        assert_eq!(
            fixture.observations.records.lock().len(),
            expected_observations,
            "{name}: publication must use complete evidence, not accounting defaults"
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
            "miss",
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
                KeepaliveDispatchContext::new(None, name.to_owned(), u64::MAX),
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
            DispatchOutcome::MaxDuration => "capped",
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
                KeepaliveDispatchContext::new(None, "provider-error".to_owned(), u64::MAX),
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

#[tokio::test(start_paused = true)]
async fn keepalive_dispatch_timeout_allows_fifteen_second_provider() {
    let mut fixture = keepalive_observation_fixture(
        StatusCode::OK,
        br#"{"usage":{"cache_read_input_tokens":1,"input_tokens":1,"output_tokens":0}}"#,
    )
    .expect("keepalive fixture");
    fixture.http.set_delay(Duration::from_secs(15));
    fixture.dispatcher = fixture.dispatcher.with_timeout(Duration::from_secs(30));
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "slow".to_owned(), u64::MAX),
        )
        .await;
    assert!(matches!(outcome, DispatchOutcome::CacheHit { .. }));
}

#[tokio::test(start_paused = true)]
async fn keepalive_dispatch_timeout_has_distinct_error() {
    let mut fixture = keepalive_observation_fixture(
        StatusCode::OK,
        br#"{"usage":{"cache_read_input_tokens":1,"input_tokens":1,"output_tokens":0}}"#,
    )
    .expect("keepalive fixture");
    fixture.http.set_delay(Duration::from_secs(31));
    fixture.dispatcher = fixture.dispatcher.with_timeout(Duration::from_secs(30));
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "timeout".to_owned(), u64::MAX),
        )
        .await;
    let DispatchOutcome::Error(error) = outcome else {
        panic!("expected timeout error");
    };
    assert_eq!(error, "renewal timed out after 30s");
}
