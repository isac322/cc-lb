#![cfg(feature = "dto-roundtrip")]

use cc_lb_domain::{
    InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace, StageDecision,
    TerminalDecision, TerminalStrategy,
};
use cc_lb_request_log::{CostBreakdown, RequestCacheLookbackPrefix, RequestEventUpdate};
use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::types::UpstreamKind;
use cc_lb_storage_api::types::{Limit as TypesLimit, LimitKind as TypesLimitKind};
use cc_lb_storage_api::{
    ApiKeyRecord, AuditEntry, BackendKind, BucketKind, CacheKeepaliveConfig,
    CacheKeepaliveConfigSnapshot, CacheKeepaliveEnqueueState, CacheKeepaliveSessionRecord,
    CacheKeepaliveSessionStatus, CacheKeepaliveTerminalReason, CacheTtl, ClassifierConfig,
    ConfigDraftState, HistoryEntry, HistorySummary, IssuedKey, JudgeResponseFormat, KeyStatus,
    LlmJudgeConfig, OAuthCredentials, PrincipalCreate, PrincipalKind, PrincipalKindLite,
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, RequestCacheBreakpoint,
    RequestCacheBreakpointSource, RequestCacheState, RequestEvent, RequestEventUpstream,
    StorageError, StoredApiKeyRecord, StoredHistoryEntry, UsageRollup, UsageRollupKey,
    UsageRollupResolution, UsageRollupRun,
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
        actor: Some("admin@example.test".to_owned()),
        actor_authority: Some("https://identity.example.test".to_owned()),
        actor_subject: Some("user-123".to_owned()),
        actor_kind: Some("human".to_owned()),
        actor_email: Some("admin@example.test".to_owned()),
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
            lookback_prefixes: vec![RequestCacheLookbackPrefix {
                prefix_hash: "fedcba9876543210".to_owned(),
                content_block_index: 0,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        }],
        cache_prefix_hash: Some(
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
        ),
        cost_usd_micros: None,
        duration_ms: 42,
        request_body_read_ms: Some(3),
        request_body_bytes: Some(512),
        finalize_ms: Some(4),
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
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["openid".to_owned(), "profile".to_owned()],
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
        allowed_upstreams: vec![Uuid::from_u128(1)],
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 1000,
        }],
        cache_keepalive: None,
    });

    assert_json_roundtrip(CacheKeepaliveConfig {
        enabled: true,
        refresh_lead_time_5m_secs: 45,
        refresh_lead_time_1h_secs: 600,
        max_refreshes_per_session: 8,
        max_total_duration_secs: 7_200,
        snapshot_max_bytes: 262_144,
        classifier: ClassifierConfig {
            extra_wait_for_user_tools: vec!["ask_human".to_owned()],
            treat_end_turn_as_ambiguous: true,
            llm_judge: Some(LlmJudgeConfig {
                provider: "anthropic".to_owned(),
                model: "claude-haiku-4-5".to_owned(),
                api_key_secret_ref: "secret://judge".to_owned(),
                base_url: Some("https://judge.local".to_owned()),
                response_format: JudgeResponseFormat::JsonObject,
                last_n_messages: 3,
                max_tokens: 64,
                temperature: 0.0,
                timeout_secs: 4,
            }),
        },
    });
    assert_json_roundtrip(CacheKeepaliveSessionRecord {
        session_key_hash: "session-hash".to_owned(),
        principal_id: "principal_a".to_owned(),
        accounting_key_id: Some("key_a".to_owned()),
        upstream_id: Uuid::from_u128(0x77777777777777777777777777777777),
        generation: 4,
        refresh_count: 2,
        first_scheduled_at_unix_secs: 1_716_000_100,
        cache_anchor_at_unix_secs: 1_716_000_200,
        run_at_unix_secs: 1_716_000_470,
        ttl: CacheTtl::Ttl5m,
        status: CacheKeepaliveSessionStatus::Terminal,
        enqueue_state: CacheKeepaliveEnqueueState::Enqueued,
        running_since_unix_secs: Some(1_716_000_250),
        current_job_key: "cache_keepalive:session-hash:4".to_owned(),
        encrypted_payload: vec![1, 2, 3, 4],
        terminal_reason: Some(CacheKeepaliveTerminalReason::CacheMiss),
        expires_at_unix_secs: 1_716_000_500,
        created_at_unix_secs: 1_716_000_100,
        updated_at_unix_secs: 1_716_000_300,
        display_reason: "agent-in-turn".to_owned(),
        error: Some("cache miss before follow-up".to_owned()),
        config_snapshot: Some(CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        }),
    });
    assert_json_roundtrip(CacheKeepaliveTerminalReason::UnsupportedProvider);
    assert_json_roundtrip(CacheTtl::Ttl1h);
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
            lookback_prefixes: vec![RequestCacheLookbackPrefix {
                prefix_hash: "lookbackabcdef0123456789".to_owned(),
                content_block_index: 6,
                lookback_distance: 1,
            }],
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        },
        json!({
            "block_index": 7,
            "source": "message",
            "path": "messages[2].content[0]",
            "message_index": 2,
            "ttl": "5m",
            "prefix_hash": "abcdef0123456789",
            "prefix_token_count": 4096,
            "lookback_prefixes": [{
                "prefix_hash": "lookbackabcdef0123456789",
                "content_block_index": 6,
                "lookback_distance": 1
            }],
            "token_estimate_source": "local_tiktoken_v1"
        }),
    );

    let request_event = RequestEvent {
        ts: 1_700_000_000,
        request_id: "req_batch_b".to_owned(),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("session-hash:4".to_owned()),
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
        observed_session_id: Some("session_batch_b".to_owned()),
        request_kind: Some("subagent".to_owned()),
        claude_agent_id: Some("agent_batch_b".to_owned()),
        claude_parent_agent_id: Some("agent_parent_batch_b".to_owned()),
        parent_session_id: Some("session_parent_batch_b".to_owned()),
        client_app: Some("cli-bg".to_owned()),
        session_id_source: Some("x-claude-code-session-id".to_owned()),
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
            lookback_prefixes: vec![RequestCacheLookbackPrefix {
                prefix_hash: "lookbackabcdef0123456789".to_owned(),
                content_block_index: 6,
                lookback_distance: 1,
            }],
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        }],
        cache_prefix_hash: Some("prefix_hash_batch_b".to_owned()),
        matched_v3_cache_key: Some("matched_v3_cache_key_batch_b".to_owned()),
        breakpoint_content_block_index: Some(7),
        matched_content_block_index: Some(6),
        lookback_distance: Some(1),
        predicted_cache_read_tokens: Some(4096),
        predicted_cache_creation_tokens_5m: Some(123),
        predicted_cache_creation_tokens_1h: Some(210),
        token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        cache_value_micros: Some(777_000),
        formula_winner_upstream_id: Some(Uuid::from_u128(0x44444444444444444444444444444444)),
        kept_upstream_id: Some(Uuid::from_u128(0x55555555555555555555555555555555)),
        quota_urgency_5h: None,
        quota_urgency_7d: None,
        quota_urgency_combined: None,
        quota_warning_multiplier: None,
        lineage_would_have_predicted_read_tokens: Some(1024),
        lineage_would_have_picked_upstream_id: Some(Uuid::from_u128(
            0x66666666666666666666666666666666,
        )),
        cost_usd_micros: Some(987_654),
        cost_input_micros: Some(111_000),
        cost_output_micros: Some(222_000),
        cost_cache_creation_5m_micros: Some(333_000),
        cost_cache_creation_1h_micros: Some(444_000),
        cost_cache_read_micros: Some(55_000),
        auth_ms: Some(5),
        route_ms: Some(6),
        limit_reserve_ms: Some(7),
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.25),
        cache_token_key_ms: Some(0.375),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.625),
        cache_serialize_ms: Some(0.75),
        cache_tokenize_ms: Some(0.0),
        prepare_signer_ms: Some(1.25),
        bulkhead_wait_ms: Some(8),
        dns_ms: Some(9),
        connect_ms: Some(10),
        connection_reused: Some(true),
        limit_reconcile_ms: Some(11),
        observability_post_ms: Some(12),
        duration_ms: 1234,
        request_body_read_ms: Some(2),
        request_body_first_chunk_ms: Some(0.0),
        request_body_receive_ms: Some(2.5),
        request_body_wait_ms: Some(2.0),
        request_body_process_ms: Some(0.5),
        request_body_chunk_count: Some(4),
        request_body_bytes: Some(1024),
        finalize_ms: Some(30),
        proxy_setup_ms: Some(13),
        shape_ms: Some(14),
        sign_ms: Some(15),
        upstream_ttfb_ms: Some(16),
        upstream_body_ms: Some(17),
        response_body_wait_ms: Some(15.25),
        response_body_process_ms: Some(1.5),
        response_body_downstream_poll_gap_ms: Some(3.75),
        retry_overhead_ms: Some(20.0),
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
        thinking_budget_tokens: None,
        reasoning_effort: None,
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
        "source_kind": "renewal",
        "source_ref_id": "session-hash:4",
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
        "observed_session_id": "session_batch_b",
        "request_kind": "subagent",
        "claude_agent_id": "agent_batch_b",
        "claude_parent_agent_id": "agent_parent_batch_b",
        "parent_session_id": "session_parent_batch_b",
        "client_app": "cli-bg",
        "session_id_source": "x-claude-code-session-id",
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
            "prefix_token_count": 4096,
            "lookback_prefixes": [{
                "prefix_hash": "lookbackabcdef0123456789",
                "content_block_index": 6,
                "lookback_distance": 1
            }],
            "token_estimate_source": "local_tiktoken_v1"
        }],
        "cache_prefix_hash": "prefix_hash_batch_b",
        "matched_v3_cache_key": "matched_v3_cache_key_batch_b",
        "breakpoint_content_block_index": 7,
        "matched_content_block_index": 6,
        "lookback_distance": 1,
        "predicted_cache_read_tokens": 4096,
        "predicted_cache_creation_tokens_5m": 123,
        "predicted_cache_creation_tokens_1h": 210,
        "token_estimate_source": "local_tiktoken_v1",
        "cache_value_micros": 777_000,
        "formula_winner_upstream_id": "44444444-4444-4444-4444-444444444444",
        "kept_upstream_id": "55555555-5555-5555-5555-555555555555",
        "lineage_would_have_predicted_read_tokens": 1024,
        "lineage_would_have_picked_upstream_id": "66666666-6666-6666-6666-666666666666",
        "cost_usd_micros": 987_654,
        "cost_input_micros": 111_000,
        "cost_output_micros": 222_000,
        "cost_cache_creation_5m_micros": 333_000,
        "cost_cache_creation_1h_micros": 444_000,
        "cost_cache_read_micros": 55_000,
        "auth_ms": 5,
        "route_ms": 6,
        "limit_reserve_ms": 7,
        "json_parse_ms": 0.125,
        "cache_structure_ms": 0.25,
        "cache_token_key_ms": 0.375,
        "cache_count_lookup_ms": 0.5,
        "cache_tokenizer_queue_ms": 0.625,
        "cache_serialize_ms": 0.75,
        "cache_tokenize_ms": 0.0,
        "prepare_signer_ms": 1.25,
        "bulkhead_wait_ms": 8,
        "dns_ms": 9,
        "connect_ms": 10,
        "connection_reused": true,
        "limit_reconcile_ms": 11,
        "observability_post_ms": 12,
        "duration_ms": 1234,
        "request_body_read_ms": 2,
        "request_body_first_chunk_ms": 0.0,
        "request_body_receive_ms": 2.5,
        "request_body_wait_ms": 2.0,
        "request_body_process_ms": 0.5,
        "request_body_chunk_count": 4,
        "request_body_bytes": 1024,
        "finalize_ms": 30,
        "proxy_setup_ms": 13,
        "shape_ms": 14,
        "sign_ms": 15,
        "upstream_ttfb_ms": 16,
        "upstream_body_ms": 17,
        "response_body_wait_ms": 15.25,
        "response_body_process_ms": 1.5,
        "response_body_downstream_poll_gap_ms": 3.75,
        "retry_overhead_ms": 20.0,
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
            actor_authority: Some("https://identity.example.test".to_owned()),
            actor_subject: Some("user-123".to_owned()),
            actor_kind: Some("human".to_owned()),
            actor_email: Some("admin@example.test".to_owned()),
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
            "actor_authority": "https://identity.example.test",
            "actor_subject": "user-123",
            "actor_kind": "human",
            "actor_email": "admin@example.test",
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
