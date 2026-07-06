#![cfg(feature = "dto-roundtrip")]
#![recursion_limit = "256"]

use cc_lb_contract::CostBreakdown;
use cc_lb_engine::event_bus::RequestEventUpdate;
use cc_lb_plugin_api::{
    InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace,
    types::{StageDecision, TerminalDecision, TerminalStrategy},
};
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
use serde_json::{Value, json};
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
            stored: BackendKind::Sqlite,
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
    assert_eq!(BackendKind::Sqlite.as_str(), "sqlite");
    assert_eq!(BackendKind::Postgres.as_str(), "postgres");
    assert_eq!(
        serde_json::to_string(&BackendKind::Sqlite).unwrap(),
        "\"sqlite\""
    );
    assert_eq!(
        serde_json::to_string(&BackendKind::Postgres).unwrap(),
        "\"postgres\""
    );
    assert_eq!(
        serde_json::from_str::<BackendKind>("\"sqlite\"").unwrap(),
        BackendKind::Sqlite
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
backend = 'sqlite'"
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

#[test]
fn batch_b_wire_snapshots_are_stable() {
    assert_wire(PrincipalKindLite::Machine, json!("machine"));
    assert_wire(PrincipalKindLite::Human, json!("human"));

    assert_wire(
        RequestEventUpstream::AnthropicDirect,
        json!("anthropic_direct"),
    );

    assert_wire(RequestCacheState::Hit, json!("hit"));
    assert_wire(RequestCacheState::Write, json!("write"));
    assert_wire(RequestCacheState::Refresh, json!("refresh"));
    assert_wire(RequestCacheState::Miss, json!("miss"));
    assert_wire(RequestCacheState::None, json!("none"));
    assert_wire(RequestCacheState::Unknown, json!("unknown"));

    assert_wire(RequestCacheBreakpointSource::Message, json!("message"));
    assert_wire(
        RequestCacheBreakpoint {
            block_index: 7,
            source: RequestCacheBreakpointSource::Message,
            path: "messages[2].content[0]".to_owned(),
            message_index: Some(2),
            ttl: Some("5m".to_owned()),
            prefix_hash: "abcdef0123456789".to_owned(),
            prefix_token_count: 4096,
        },
        json!({
            "block_index": 7,
            "source": "message",
            "path": "messages[2].content[0]",
            "message_index": 2,
            "ttl": "5m",
            "prefix_hash": "abcdef0123456789",
            "prefix_token_count": 4096
        }),
    );

    let request_event = RequestEvent {
        ts: 1_700_000_000,
        request_id: "req_batch_b".to_owned(),
        ts_ms: Some(1_700_000_000_123),
        principal_id: Some("principal_batch_b".to_owned()),
        key_id: Some("key_batch_b".to_owned()),
        principal_kind: Some("machine".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(Uuid::from_u128(0x11111111111111111111111111111111)),
        upstream_name: Some("anthropic-primary".to_owned()),
        model: Some("claude-3-5-sonnet".to_owned()),
        status: 200,
        input_tokens: Some(111),
        output_tokens: Some(222),
        cache_creation_input_tokens: Some(333),
        cache_creation_input_tokens_5m: Some(123),
        cache_creation_input_tokens_1h: Some(210),
        cache_read_input_tokens: Some(444),
        cache_state: Some(RequestCacheState::Refresh),
        thread_id: Some("thread_batch_b".to_owned()),
        message_id: Some("msg_batch_b".to_owned()),
        message_index: Some(2),
        message_count: Some(5),
        cache_control_block_count: Some(1),
        cache_control_message_indices: vec![2, 4],
        cache_breakpoints: vec![RequestCacheBreakpoint {
            block_index: 7,
            source: RequestCacheBreakpointSource::Message,
            path: "messages[2].content[0]".to_owned(),
            message_index: Some(2),
            ttl: Some("5m".to_owned()),
            prefix_hash: "abcdef0123456789".to_owned(),
            prefix_token_count: 4096,
        }],
        cache_prefix_hash: Some("prefix_hash_batch_b".to_owned()),
        cost_usd_micros: Some(987_654),
        cost_input_micros: Some(111_000),
        cost_output_micros: Some(222_000),
        cost_cache_creation_5m_micros: Some(333_000),
        cost_cache_creation_1h_micros: Some(444_000),
        cost_cache_read_micros: Some(55_000),
        auth_ms: Some(5),
        route_ms: Some(6),
        limit_reserve_ms: Some(7),
        bulkhead_wait_ms: Some(8),
        dns_ms: Some(9),
        connect_ms: Some(10),
        connection_reused: Some(true),
        limit_reconcile_ms: Some(11),
        observability_post_ms: Some(12),
        duration_ms: 1234,
        proxy_setup_ms: Some(13),
        shape_ms: Some(14),
        sign_ms: Some(15),
        upstream_ttfb_ms: Some(16),
        upstream_body_ms: Some(17),
        first_body_chunk_ms: Some(18),
        body_chunk_count: Some(19),
        body_bytes: Some(2048),
        stream_message_start_ms: Some(20),
        stream_content_block_start_ms: Some(21),
        stream_first_content_delta_ms: Some(22),
        stream_last_content_delta_ms: Some(23),
        stream_message_stop_ms: Some(24),
        stream_last_chunk_ms: Some(25),
        stream_total_ms: Some(26),
        sse_event_count: Some(27),
        content_delta_count: Some(28),
        ping_count: Some(29),
        inter_token_avg_ms: Some(30),
        error_code: Some("upstream_5xx".to_owned()),
        routing_trace: Some(RoutingTrace {
            stages: vec![StageDecision {
                stage_name: "router".to_owned(),
                upstream_id: Some(Uuid::from_u128(0x22222222222222222222222222222222)),
                reason: Some("selected".to_owned()),
                duration_us: 321,
                subscription_preference: None,
                cache_affinity: None,
            }],
            terminal_decision: Some(TerminalDecision {
                upstream_id: Some(Uuid::from_u128(0x33333333333333333333333333333333)),
                strategy: TerminalStrategy::FirstPick,
            }),
        }),
        internal_errors: vec![InternalError {
            stage: InternalErrorStage::Router,
            kind: InternalErrorKind::PluginError,
            message: Some("plugin warning".to_owned()),
        }],
        event_id: Some("0193f76b-1ab2-7a4d-8a3c-44ab3c5e1f0a".to_owned()),
        thinking_tokens: Some(64),
        web_search_requests: Some(3),
        web_fetch_requests: Some(1),
        service_tier: Some("priority".to_owned()),
        inference_geo: Some("us-east".to_owned()),
        upstream_error_type: Some("overloaded_error".to_owned()),
        upstream_error_message: Some("upstream overloaded; retry".to_owned()),
        iterations: Some(json!([{ "type": "message", "input_tokens": 100 }])),
    };
    let request_event_json = json!({
        "ts": 1_700_000_000u64,
        "request_id": "req_batch_b",
        "ts_ms": 1_700_000_000_123u64,
        "principal_id": "principal_batch_b",
        "key_id": "key_batch_b",
        "principal_kind": "machine",
        "upstream": "anthropic_direct",
        "upstream_id": "11111111-1111-1111-1111-111111111111",
        "upstream_name": "anthropic-primary",
        "model": "claude-3-5-sonnet",
        "status": 200,
        "input_tokens": 111,
        "output_tokens": 222,
        "cache_creation_input_tokens": 333,
        "cache_creation_input_tokens_5m": 123,
        "cache_creation_input_tokens_1h": 210,
        "cache_read_input_tokens": 444,
        "cache_state": "refresh",
        "thread_id": "thread_batch_b",
        "message_id": "msg_batch_b",
        "message_index": 2,
        "message_count": 5,
        "cache_control_block_count": 1,
        "cache_control_message_indices": [2, 4],
        "cache_breakpoints": [{
            "block_index": 7,
            "source": "message",
            "path": "messages[2].content[0]",
            "message_index": 2,
            "ttl": "5m",
            "prefix_hash": "abcdef0123456789",
            "prefix_token_count": 4096
        }],
        "cache_prefix_hash": "prefix_hash_batch_b",
        "cost_usd_micros": 987_654,
        "cost_input_micros": 111_000,
        "cost_output_micros": 222_000,
        "cost_cache_creation_5m_micros": 333_000,
        "cost_cache_creation_1h_micros": 444_000,
        "cost_cache_read_micros": 55_000,
        "auth_ms": 5,
        "route_ms": 6,
        "limit_reserve_ms": 7,
        "bulkhead_wait_ms": 8,
        "dns_ms": 9,
        "connect_ms": 10,
        "connection_reused": true,
        "limit_reconcile_ms": 11,
        "observability_post_ms": 12,
        "duration_ms": 1234,
        "proxy_setup_ms": 13,
        "shape_ms": 14,
        "sign_ms": 15,
        "upstream_ttfb_ms": 16,
        "upstream_body_ms": 17,
        "first_body_chunk_ms": 18,
        "body_chunk_count": 19,
        "body_bytes": 2048,
        "stream_message_start_ms": 20,
        "stream_content_block_start_ms": 21,
        "stream_first_content_delta_ms": 22,
        "stream_last_content_delta_ms": 23,
        "stream_message_stop_ms": 24,
        "stream_last_chunk_ms": 25,
        "stream_total_ms": 26,
        "sse_event_count": 27,
        "content_delta_count": 28,
        "ping_count": 29,
        "inter_token_avg_ms": 30,
        "error_code": "upstream_5xx",
        "routing_trace": {
            "stages": [{
                "stage_name": "router",
                "upstream_id": "22222222-2222-2222-2222-222222222222",
                "reason": "selected",
                "duration_us": 321
            }],
            "terminal_decision": {
                "upstream_id": "33333333-3333-3333-3333-333333333333",
                "strategy": "first-pick"
            }
        },
        "internal_errors": [{
            "stage": "router",
            "kind": "plugin_error",
            "message": "plugin warning"
        }],
        "event_id": "0193f76b-1ab2-7a4d-8a3c-44ab3c5e1f0a",
        "thinking_tokens": 64,
        "web_search_requests": 3,
        "web_fetch_requests": 1,
        "service_tier": "priority",
        "inference_geo": "us-east",
        "upstream_error_type": "overloaded_error",
        "upstream_error_message": "upstream overloaded; retry",
        "iterations": [{ "type": "message", "input_tokens": 100 }]
    });
    assert_wire(request_event.clone(), request_event_json.clone());
    assert_wire(
        RequestEventUpdate::final_(request_event, 7),
        json!({
            "phase": "final",
            "payload": {
                "event": request_event_json,
                "cursor": 7
            }
        }),
    );

    assert_wire(
        AuditEntry {
            ts: 1_700_000_001,
            request_id: "req_audit".to_owned(),
            principal_id: "principal_audit".to_owned(),
            route: "/v1/messages".to_owned(),
            upstream: "anthropic-primary".to_owned(),
            model: Some("claude-3-haiku".to_owned()),
            status: 429,
            input_tokens: Some(10),
            output_tokens: Some(20),
            duration_ms: 345,
            agent_label: Some("agent-a".to_owned()),
            api_key_id: Some("key_audit".to_owned()),
            cost_usd_micros: Some(456),
            limit_violation: Some("requests".to_owned()),
            admin_action: Some("disable_key".to_owned()),
            actor: Some("admin@example.test".to_owned()),
            kind: Some("admin_action".to_owned()),
            payload: Some(json!({ "key_id": "key_audit", "enabled": false })),
        },
        json!({
            "ts": 1_700_000_001u64,
            "request_id": "req_audit",
            "principal_id": "principal_audit",
            "route": "/v1/messages",
            "upstream": "anthropic-primary",
            "model": "claude-3-haiku",
            "status": 429,
            "input_tokens": 10,
            "output_tokens": 20,
            "duration_ms": 345,
            "agent_label": "agent-a",
            "api_key_id": "key_audit",
            "cost_usd_micros": 456,
            "limit_violation": "requests",
            "admin_action": "disable_key",
            "actor": "admin@example.test",
            "kind": "admin_action",
            "payload": { "key_id": "key_audit", "enabled": false }
        }),
    );

    assert_wire(TypesLimitKind::CostUsd, json!("cost_usd"));
    assert_wire(PrincipalLimitKind::Requests, json!("requests"));
    assert_wire(
        TypesLimit {
            kind: TypesLimitKind::CostUsd,
            window_secs: 3600,
            cap_micros: 12_345,
        },
        json!({
            "kind": "cost_usd",
            "window_secs": 3600,
            "cap_micros": 12_345
        }),
    );
    assert_wire(
        Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 1000,
        },
        json!({
            "kind": "requests",
            "window_secs": 60,
            "cap_micros": 1000
        }),
    );

    assert_wire(KeyStatus::Active, json!("active"));
    assert_wire(KeyStatus::Disabled, json!("disabled"));
    assert_wire(KeyStatus::Revoked, json!("revoked"));

    assert_wire(
        CostBreakdown {
            total_micros: Some(999),
            input_micros: Some(111),
            output_micros: Some(222),
            cache_creation_5m_micros: Some(333),
            cache_creation_1h_micros: Some(444),
            cache_read_micros: Some(55),
        },
        json!({
            "total_micros": 999,
            "input_micros": 111,
            "output_micros": 222,
            "cache_creation_5m_micros": 333,
            "cache_creation_1h_micros": 444,
            "cache_read_micros": 55
        }),
    );
}

fn assert_json_roundtrip<T>(value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_string(&value).unwrap();
    let decoded = serde_json::from_str::<T>(&encoded).unwrap();
    assert_eq!(decoded, value);
}

fn assert_wire<T>(value: T, expected: Value)
where
    T: Serialize + DeserializeOwned,
{
    let encoded = serde_json::to_value(&value).unwrap();
    assert_eq!(encoded, expected);

    let decoded = serde_json::from_value::<T>(encoded).unwrap();
    let reencoded = serde_json::to_value(decoded).unwrap();
    assert_eq!(reencoded, expected);
}
