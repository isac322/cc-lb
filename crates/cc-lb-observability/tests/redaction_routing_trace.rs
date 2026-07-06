use cc_lb_observability::{
    REDACTED, ROUTING_REASON_MAX_BYTES, ROUTING_TRACE_SIZE_CAP_BYTES, enforce_routing_trace_caps,
    redact_internal_errors, redact_routing_trace, truncate_reason,
};
use cc_lb_plugin_api::types::{
    CacheAffinityCandidate, CacheAffinityTrace, CandidateUrgency, StageDecision,
    SubscriptionPreferenceTrace, SubscriptionTier, TerminalDecision, WrhKeySource,
};
use cc_lb_plugin_api::{
    InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace, TerminalStrategy,
};
use uuid::Uuid;

#[test]
fn routing_trace_reason_secrets_are_redacted() {
    let trace = RoutingTrace {
        stages: vec![StageDecision {
            stage_name: "router-filter".to_owned(),
            upstream_id: None,
            reason: Some("selected after token=plain-secret and sk-ant-oat01-deadbeef".to_owned()),
            duration_us: 0,
            subscription_preference: None,
            cache_affinity: None,
        }],
        terminal_decision: Some(TerminalDecision {
            upstream_id: None,
            strategy: TerminalStrategy::FirstPick,
        }),
    };

    let redacted = redact_routing_trace(&trace);
    let reason = redacted.stages[0].reason.as_deref().unwrap();

    assert!(reason.contains(REDACTED));
    assert!(!reason.contains("plain-secret"));
    assert!(!reason.contains("sk-ant-oat01-deadbeef"));
    assert!(
        trace.stages[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("plain-secret")
    );
}

#[test]
fn internal_error_messages_are_redacted() {
    let errors = vec![InternalError {
        stage: InternalErrorStage::Router,
        kind: InternalErrorKind::PluginError,
        message: Some("plugin returned x-api-key: secret-key and Bearer abc.def".to_owned()),
    }];

    let redacted = redact_internal_errors(&errors);
    let message = redacted[0].message.as_deref().unwrap();

    assert!(message.contains(REDACTED));
    assert!(!message.contains("secret-key"));
    assert!(!message.contains("abc.def"));
    assert!(errors[0].message.as_deref().unwrap().contains("secret-key"));
}

#[test]
fn truncate_reason_caps_long_reason() {
    let reason = "a".repeat(ROUTING_REASON_MAX_BYTES + 128);

    let truncated = truncate_reason(&reason);

    assert!(truncated.len() <= ROUTING_REASON_MAX_BYTES);
    assert!(truncated.ends_with("...[truncated]"));
}

#[test]
fn truncate_reason_preserves_utf8_boundary() {
    let reason = format!("{}한글", "a".repeat(ROUTING_REASON_MAX_BYTES));

    let truncated = truncate_reason(&reason);

    assert!(truncated.len() <= ROUTING_REASON_MAX_BYTES);
    assert!(truncated.is_char_boundary(truncated.len()));
}

#[test]
fn routing_trace_cap_removes_tail_stages_and_adds_marker() {
    let trace = RoutingTrace {
        stages: (0..40)
            .map(|index| StageDecision {
                stage_name: format!("stage-{index}"),
                upstream_id: None,
                reason: Some(format!("decision-{index}-{}", "x".repeat(350))),
                duration_us: 0,
                subscription_preference: None,
                cache_affinity: None,
            })
            .collect(),
        terminal_decision: Some(TerminalDecision {
            upstream_id: None,
            strategy: TerminalStrategy::FirstPick,
        }),
    };

    let capped = enforce_routing_trace_caps(&trace);
    let encoded = serde_json::to_vec(&capped).unwrap();
    let marker = capped.stages.last().unwrap();

    assert!(encoded.len() <= ROUTING_TRACE_SIZE_CAP_BYTES);
    assert_eq!(capped.stages.first().unwrap().stage_name, "stage-0");
    assert_eq!(marker.stage_name, "routing_trace_truncated");
    assert!(marker.reason.as_deref().unwrap().contains("removed"));
    assert!(
        !capped
            .stages
            .iter()
            .any(|stage| stage.stage_name == "stage-39")
    );
}

#[test]
fn routing_trace_cap_respects_extended_stage_payloads() {
    let seeded = |seed: u8| {
        let mut bytes = [0u8; 16];
        bytes[15] = seed;
        Uuid::from_bytes(bytes)
    };
    let build_subscription_stage = |index: u8| StageDecision {
        stage_name: format!("subscription-preference-{index}"),
        upstream_id: Some(seeded(index)),
        reason: Some(format!("selected candidate {index}")),
        duration_us: 40,
        subscription_preference: Some(SubscriptionPreferenceTrace {
            chosen_tier: SubscriptionTier::KnownBase,
            candidates: (0..8u8)
                .map(|c| CandidateUrgency {
                    upstream_id: seeded(100 + c),
                    tier: SubscriptionTier::KnownBase,
                    urgency: 0.123_456_789 * (c as f64 + 1.0),
                })
                .collect(),
            wrh_key_source: WrhKeySource::ThreadId,
            previous_tier: Some(SubscriptionTier::PartialBase),
            rendezvous_salt_version: Some("v4".to_owned()),
        }),
        cache_affinity: None,
    };
    let build_cache_stage = |index: u8| StageDecision {
        stage_name: format!("cache-affinity-{index}"),
        upstream_id: Some(seeded(index)),
        reason: Some("cache-hit-keep".to_owned()),
        duration_us: 5,
        subscription_preference: None,
        cache_affinity: Some(CacheAffinityTrace {
            candidates: (0..8u8)
                .map(|c| CacheAffinityCandidate {
                    upstream_id: seeded(200 + c),
                    kept: c % 2 == 0,
                    predicted_cache_read_tokens: Some(u32::from(c) * 1000),
                    predicted_expires_at_unix_secs: Some(1_700_000_000 + u64::from(c) * 60),
                })
                .collect(),
        }),
    };
    let mut stages = Vec::new();
    for i in 0..30u8 {
        stages.push(build_subscription_stage(i));
        stages.push(build_cache_stage(i));
    }
    let trace = RoutingTrace {
        stages,
        terminal_decision: Some(TerminalDecision {
            upstream_id: Some(seeded(9)),
            strategy: TerminalStrategy::FirstPick,
        }),
    };

    let capped = enforce_routing_trace_caps(&trace);
    let encoded = serde_json::to_vec(&capped).unwrap();

    assert!(
        encoded.len() <= ROUTING_TRACE_SIZE_CAP_BYTES,
        "capped trace with subscription_preference + cache_affinity payloads must fit \
         inside ROUTING_TRACE_SIZE_CAP_BYTES ({} bytes), got {}. If this fails the \
         json_len helpers in redaction.rs are under-counting one of the new fields.",
        ROUTING_TRACE_SIZE_CAP_BYTES,
        encoded.len(),
    );
    assert_eq!(
        capped.stages.last().unwrap().stage_name,
        "routing_trace_truncated",
        "size-driven truncation must terminate with the truncation marker"
    );
}

#[test]
fn subscription_preference_all_fields_survive_serde_roundtrip() {
    let trace = SubscriptionPreferenceTrace {
        chosen_tier: SubscriptionTier::Overage,
        candidates: vec![CandidateUrgency {
            upstream_id: Uuid::from_bytes([1; 16]),
            tier: SubscriptionTier::KnownBase,
            urgency: 0.75,
        }],
        wrh_key_source: WrhKeySource::ThreadId,
        previous_tier: Some(SubscriptionTier::KnownBase),
        rendezvous_salt_version: Some("v4".to_owned()),
    };
    let json = serde_json::to_string(&trace).unwrap();
    let decoded: SubscriptionPreferenceTrace = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, trace);
    assert!(json.contains("\"wrh_key_source\":\"thread_id\""));
    assert!(json.contains("\"previous_tier\":\"known_base\""));
    assert!(json.contains("\"rendezvous_salt_version\":\"v4\""));
}

#[test]
fn cache_affinity_trace_survives_serde_roundtrip() {
    let trace = CacheAffinityTrace {
        candidates: vec![
            CacheAffinityCandidate {
                upstream_id: Uuid::from_bytes([2; 16]),
                kept: true,
                predicted_cache_read_tokens: Some(1234),
                predicted_expires_at_unix_secs: Some(1_700_000_500),
            },
            CacheAffinityCandidate {
                upstream_id: Uuid::from_bytes([3; 16]),
                kept: false,
                predicted_cache_read_tokens: None,
                predicted_expires_at_unix_secs: None,
            },
        ],
    };
    let json = serde_json::to_string(&trace).unwrap();
    let decoded: CacheAffinityTrace = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, trace);
    assert!(json.contains("\"kept\":true"));
    assert!(json.contains("\"kept\":false"));
    assert!(json.contains("\"predicted_cache_read_tokens\":1234"));
}
