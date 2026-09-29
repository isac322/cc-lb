use cc_lb_domain::{
    CandidateUrgency, InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace,
    StageDecision, SubscriptionPreferenceTrace, SubscriptionTier, TerminalDecision,
    TerminalStrategy,
};
use cc_lb_observability::{
    REDACTED, ROUTING_REASON_MAX_BYTES, ROUTING_TRACE_SIZE_CAP_BYTES, enforce_routing_trace_caps,
    redact_internal_errors, redact_routing_trace, truncate_reason,
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
                .map(|c| {
                    let quota = 0.123_456_789 * (c as f64 + 1.0);
                    let cache_ratio = c as f64 / 7.0;
                    CandidateUrgency {
                        upstream_id: seeded(100 + c),
                        tier: SubscriptionTier::KnownBase,
                        quota_urgency: quota,
                        quota_urgency_5h: Some(quota),
                        quota_urgency_7d: Some(0.0),
                        quota_urgency_combined: Some(quota),
                        predicted_cache_read_tokens: u32::from(c) * 30_000,
                        predicted_cache_creation_tokens_5m: u32::from(c) * 10_000,
                        predicted_cache_creation_tokens_1h: 0,
                        predicted_uncached_input_tokens: u32::from(c) * 1_000,
                        cache_ratio,
                        warning_multiplier: 1.0,
                        cache_savings_ratio: cache_ratio,
                        estimated_input_cost_micros: u64::from(c) * 1_000,
                        cache_value_micros: Some(i64::from(c) * 1_000),
                        matched_v3_cache_key: Some(format!("cache-key-{c}")),
                        matched_content_block_index: Some(u32::from(c)),
                        breakpoint_content_block_index: Some(u32::from(c) + 1),
                        lookback_distance: Some(u32::from(c)),
                        token_estimate_source: Some("local_tiktoken_v1".to_owned()),
                    }
                })
                .collect(),
            formula_version: Some("cost-first-v1".to_owned()),
            cache_cost_basis_version: Some("v1".to_owned()),
            formula_winner_upstream_id: Some(seeded(index)),
            kept_upstream_id: Some(seeded(index)),
            switch_gate_reason: Some("formula_winner".to_owned()),
            bucket_v3_cache_key: Some(format!("bucket-cache-key-{index}")),
        }),
    };
    let mut stages = Vec::new();
    for i in 0..30u8 {
        stages.push(build_subscription_stage(i));
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
        "capped trace with subscription_preference payloads must fit \
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
            quota_urgency: 0.25,
            quota_urgency_5h: Some(0.20),
            quota_urgency_7d: Some(0.15),
            quota_urgency_combined: Some(0.25),
            predicted_cache_read_tokens: 100_000,
            predicted_cache_creation_tokens_5m: 25_000,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 10_000,
            cache_ratio: 0.4,
            warning_multiplier: 1.0,
            cache_savings_ratio: 0.4,
            estimated_input_cost_micros: 123_456,
            cache_value_micros: Some(42_000),
            matched_v3_cache_key: Some("cache-key".to_owned()),
            matched_content_block_index: Some(10),
            breakpoint_content_block_index: Some(11),
            lookback_distance: Some(1),
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        }],
        formula_version: Some("cost-first-v1".to_owned()),
        cache_cost_basis_version: Some("v1".to_owned()),
        formula_winner_upstream_id: Some(Uuid::from_bytes([1; 16])),
        kept_upstream_id: Some(Uuid::from_bytes([1; 16])),
        switch_gate_reason: Some("formula_winner".to_owned()),
        bucket_v3_cache_key: Some("cache-key".to_owned()),
    };
    let json = serde_json::to_string(&trace).unwrap();
    let decoded: SubscriptionPreferenceTrace = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, trace);
    assert!(json.contains("\"formula_version\":\"cost-first-v1\""));
    assert!(json.contains("\"quota_urgency\":"));
    assert!(json.contains("\"quota_urgency_5h\":0.2"));
    assert!(json.contains("\"quota_urgency_7d\":0.15"));
    assert!(json.contains("\"quota_urgency_combined\":0.25"));
    assert!(json.contains("\"predicted_cache_read_tokens\":100000"));
    assert!(json.contains("\"bucket_v3_cache_key\":\"cache-key\""));
}
