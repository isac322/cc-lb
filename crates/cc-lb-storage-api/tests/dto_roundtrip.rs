#![cfg(feature = "dto-roundtrip")]

use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::types::UpstreamKind;
use cc_lb_storage_api::types::{Limit as TypesLimit, LimitKind as TypesLimitKind};
use cc_lb_storage_api::{
    AnthropicApiKeyCredential, ApiKeyRecord, AuditEntry, BackendKind, BucketKind, ConfigDraftState,
    HistoryEntry, HistorySummary, IssuedKey, KeyStatus, OAuthCredentials, PrincipalCreate,
    PrincipalKind, PrincipalKindLite, PrincipalLimitIdentityKind, PrincipalLimitKind,
    PrincipalLimitState, RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheState,
    RequestEvent, RequestEventUpstream, StorageError, StoredApiKeyRecord, StoredHistoryEntry,
    UsageRollup, UsageRollupKey, UsageRollupResolution, UsageRollupRun,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use uuid::Uuid;

#[test]
fn retryable_classification_matches_storage_error_intent() {
    let retryable_transient = StorageError::Transient {
        retryable: true,
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "temporary timeout",
        )),
    };
    assert!(retryable_transient.is_retryable());

    let non_retryable_transient = StorageError::Transient {
        retryable: false,
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "permanent failure",
        )),
    };
    assert!(!non_retryable_transient.is_retryable());

    let unavailable = StorageError::Unavailable {
        message: "storage is temporarily unavailable".to_owned(),
    };
    assert!(unavailable.is_retryable());

    let serialization_error = serde_json::from_str::<serde_json::Value>("not-json").unwrap_err();
    let non_retryable_errors = [
        StorageError::Conflict {
            message: "revision changed".to_owned(),
        },
        StorageError::SchemaMismatch {
            found: 0,
            expected: 1,
        },
        StorageError::Corrupted {
            message: "checksum mismatch".to_owned(),
        },
        StorageError::Fatal {
            message: "invariant failed".to_owned(),
        },
        StorageError::BackendKindMismatch {
            stored: BackendKind::Redb,
            configured: BackendKind::Postgres,
        },
        StorageError::Serialization(serialization_error),
        StorageError::Aead("authentication failed".to_owned()),
    ];

    for error in non_retryable_errors {
        assert!(!error.is_retryable(), "{error} should not be retryable");
    }
}

#[test]
fn backend_kind_serde_uses_lowercase_wire_format_and_as_str() {
    assert_eq!(BackendKind::Redb.as_str(), "redb");
    assert_eq!(BackendKind::Postgres.as_str(), "postgres");
    assert_eq!(
        serde_json::to_string(&BackendKind::Redb).unwrap(),
        "\"redb\""
    );
    assert_eq!(
        serde_json::to_string(&BackendKind::Postgres).unwrap(),
        "\"postgres\""
    );
    assert_eq!(
        serde_json::from_str::<BackendKind>("\"redb\"").unwrap(),
        BackendKind::Redb
    );
    assert_eq!(
        serde_json::from_str::<BackendKind>("\"postgres\"").unwrap(),
        BackendKind::Postgres
    );
}

#[test]
fn dto_roundtrip_preserves_representative_storage_domain_shapes() {
    assert_json_roundtrip(AuditEntry {
        ts: 1_716_000_000,
        request_id: "req_001".to_owned(),
        principal_id: "principal_a".to_owned(),
        route: "/v1/messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-3-5-sonnet".to_owned()),
        status: 200,
        input_tokens: Some(120),
        output_tokens: Some(45),
        duration_ms: 873,
        agent_label: Some("billing-agent".to_owned()),
        kind: Some("request_completed".to_owned()),
        payload: Some(json!({
            "nested": { "cache": true },
            "tags": ["t6", "dto"]
        })),
        ..Default::default()
    });

    assert_json_roundtrip(RequestEvent {
        ts: 1_716_000_001,
        request_id: "req_002".to_owned(),
        principal_id: Some("principal_a".to_owned()),
        principal_kind: Some("account".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(Uuid::from_u128(0x11111111111111111111111111111111)),
        upstream_name: Some("anthropic_direct".to_owned()),
        model: Some("claude-3-haiku".to_owned()),
        status: 429,
        input_tokens: Some(35),
        output_tokens: Some(0),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        cache_state: Some(RequestCacheState::Miss),
        thread_id: Some("thread-dto".to_owned()),
        message_id: Some("msg-dto".to_owned()),
        message_index: Some(4),
        message_count: Some(5),
        cache_control_block_count: Some(1),
        cache_control_message_indices: vec![4],
        cache_breakpoints: vec![RequestCacheBreakpoint {
            block_index: 0,
            source: RequestCacheBreakpointSource::Message,
            path: "messages[4].content[0]".to_owned(),
            message_index: Some(4),
            ttl: Some("5m".to_owned()),
            prefix_hash: "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
                .to_owned(),
            prefix_token_count: 2048,
        }],
        cache_prefix_hash: Some(
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
        ),
        cost_usd_micros: None,
        duration_ms: 42,
        error_code: Some("rate_limit".to_owned()),
        ..Default::default()
    });

    assert_json_roundtrip(PrincipalLimitState {
        principal_id: "principal_a".to_owned(),
        identity_kind: PrincipalLimitIdentityKind::Credential,
        identity_value: Some("credential_123".to_owned()),
        account_observed: true,
        window: "2026-05-24T10:00:00Z/PT1H".to_owned(),
        kind: PrincipalLimitKind::InputTokens,
        limit: Some(10_000),
        remaining: Some(9_700),
        reset: Some("2026-05-24T11:00:00Z".to_owned()),
        observed_at_unix_secs: 1_716_000_002,
        stored_at_unix_secs: 1_716_000_003,
    });

    assert_json_roundtrip(ConfigDraftState {
        draft: Some(json!({
            "upstreams": [{ "name": "anthropic", "kind": "direct" }],
            "limits": { "requests": 100 }
        })),
        revision: 7,
        last_validated_revision: Some(6),
        last_validation_error: Some("plugin missing".to_owned()),
        saved_at_unix_secs: Some(1_716_000_004),
    });

    let summary = HistorySummary {
        upstreams: 2,
        principals: 3,
        plugin_count: 1,
        tls_enabled: true,
    };
    assert_json_roundtrip(summary.clone());
    assert_json_roundtrip(HistoryEntry {
        revision: 8,
        config_toml: "[server]
listen = '127.0.0.1:8080'"
            .to_owned(),
        applied_at_unix_secs: 1_716_000_005,
        summary: summary.clone(),
    });
    assert_json_roundtrip(StoredHistoryEntry {
        config_toml: "[storage]
backend = 'redb'"
            .to_owned(),
        applied_at_unix_secs: 1_716_000_006,
        summary,
    });

    assert_json_roundtrip(UsageRollupKey {
        resolution: UsageRollupResolution::Hour,
        bucket_start: 1_716_000_000,
        principal: "principal_a".to_owned(),
        upstream_id: Uuid::from_u128(0x22222222222222222222222222222222),
        upstream_name: "anthropic_direct".to_owned(),
        model: "claude-3-haiku".to_owned(),
    });
    assert_json_roundtrip(UsageRollup {
        resolution: UsageRollupResolution::Minute,
        bucket_start: 1_716_000_060,
        principal: "principal_a".to_owned(),
        upstream_id: Uuid::from_u128(0x33333333333333333333333333333333),
        upstream_name: "anthropic_direct".to_owned(),
        model: "claude-3-5-sonnet".to_owned(),
        request_count: 4,
        input_tokens: 500,
        output_tokens: 240,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        error_count: 1,
        latency_count: 4,
        latency_ms_sum: 1_000,
        latency_ms_min: Some(100),
        latency_ms_max: Some(450),
        proxy_setup_ms_count: 0,
        proxy_setup_ms_sum: 0,
        shape_ms_count: 0,
        shape_ms_sum: 0,
        sign_ms_count: 0,
        sign_ms_sum: 0,
        upstream_ttfb_ms_count: 0,
        upstream_ttfb_ms_sum: 0,
        upstream_body_ms_count: 0,
        upstream_body_ms_sum: 0,
        virtual_cost_micros: 123_456,
    });
    assert_json_roundtrip(UsageRollupRun {
        processed_events: 10,
        updated_rollups: 2,
        checkpoint: Some(1_716_000_099),
    });

    assert_json_roundtrip(OAuthCredentials {
        access_token: "access-token".to_owned(),
        refresh_token: "refresh-token".to_owned(),
        expires_at: 1_716_086_400,
        scopes: vec!["openid".to_owned(), "profile".to_owned()],
    });
    assert_json_roundtrip(AnthropicApiKeyCredential {
        anthropic_api_key: "sk-ant-api03-example".to_owned(),
    });
    assert_json_roundtrip(IssuedKey {
        key_id: "key_123".to_owned(),
        plaintext: "plain_credential".to_owned(),
        issued_at_unix_secs: 1_716_000_010,
    });
    assert_json_roundtrip(ApiKeyRecord {
        key_id: "key_123".to_owned(),
        label: Some("default".to_owned()),
        issued_at_unix_secs: 1_716_000_010,
        revoked_at_unix_secs: Some(1_716_000_020),
    });
    assert_json_roundtrip(StoredApiKeyRecord {
        label: "default".to_owned(),
        issued_at_unix_secs: 1_716_000_010,
        revoked_at_unix_secs: Some(1_716_000_020),
        key_hash_b64: "YWJjMTIz".to_owned(),
        verify_hash: [1; 32],
        secret_salt: [2; 16],
        upstream_kind: UpstreamKind::AnthropicKey,
        limit_overrides: vec![TypesLimit {
            kind: TypesLimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
        status: KeyStatus::Active,
        expires_at_unix_secs: Some(1_800_000_000),
        last_4: "c123".to_owned(),
        description: Some("default key".to_owned()),
        principal_kind: PrincipalKindLite::Machine,
        index_hash: [3; 32],
    });

    assert_json_roundtrip(BucketKind::OutputTokens);

    assert_json_roundtrip(PrincipalCreate {
        name: "test-principal".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-3-5-sonnet".to_owned()],
        allowed_upstreams: vec![Uuid::new_v4()],
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 1000,
        }],
    });
}

fn assert_json_roundtrip<T>(value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_string(&value).unwrap();
    let decoded = serde_json::from_str::<T>(&encoded).unwrap();
    assert_eq!(decoded, value);
}
