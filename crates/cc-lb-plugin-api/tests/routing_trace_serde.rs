use cc_lb_plugin_api::types::{
    InternalError, InternalErrorKind, InternalErrorStage, MAX_ERROR_MESSAGE_LEN,
    MAX_ROUTING_TRACE_STAGES, MAX_STAGE_NAME_LEN, PassthroughCause, PerCandidateReason,
    RoutingTrace, StageDecision, TerminalDecision, TerminalStrategy,
};
use uuid::Uuid;

#[test]
fn passthrough_cause_serde_roundtrip() {
    let causes = vec![
        PassthroughCause::HealthyUpstream,
        PassthroughCause::NoAlternative,
        PassthroughCause::PluginDecision,
    ];

    for cause in causes {
        let json = serde_json::to_string(&cause).unwrap();
        let decoded: PassthroughCause = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, cause);
    }
}

#[test]
fn per_candidate_reason_serde_roundtrip() {
    let reasons = vec![
        PerCandidateReason::RateLimited,
        PerCandidateReason::InsufficientQuota,
        PerCandidateReason::Unhealthy,
        PerCandidateReason::RejectedByPlugin,
    ];

    for reason in reasons {
        let json = serde_json::to_string(&reason).unwrap();
        let decoded: PerCandidateReason = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, reason);
    }
}

#[test]
fn terminal_strategy_serde_roundtrip_and_default() {
    let strategies = vec![
        TerminalStrategy::FirstPick,
        TerminalStrategy::Random,
        TerminalStrategy::RoundRobin,
        TerminalStrategy::LeastConnections,
    ];

    for strategy in strategies {
        let json = serde_json::to_string(&strategy).unwrap();
        let decoded: TerminalStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, strategy);
    }

    assert_eq!(TerminalStrategy::default(), TerminalStrategy::FirstPick);

    let default_json = serde_json::to_string(&TerminalStrategy::FirstPick).unwrap();
    assert_eq!(default_json, r#""first-pick""#);
}

#[test]
fn internal_error_stage_serde_roundtrip() {
    let stages = vec![
        InternalErrorStage::Authn,
        InternalErrorStage::Router,
        InternalErrorStage::Shape,
        InternalErrorStage::Signer,
        InternalErrorStage::Relay,
    ];

    for stage in stages {
        let json = serde_json::to_string(&stage).unwrap();
        let decoded: InternalErrorStage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, stage);
    }

    assert_eq!(InternalErrorStage::default(), InternalErrorStage::Router);
}

#[test]
fn internal_error_kind_serde_roundtrip() {
    let kinds = vec![
        InternalErrorKind::PluginError,
        InternalErrorKind::ConfigError,
        InternalErrorKind::Timeout,
        InternalErrorKind::Unavailable,
    ];

    for kind in kinds {
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: InternalErrorKind = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, kind);
    }

    assert_eq!(InternalErrorKind::default(), InternalErrorKind::PluginError);
}

#[test]
fn stage_decision_serde_roundtrip() {
    let upstream_id = Uuid::new_v4();
    let decision = StageDecision {
        stage_name: "routing".to_owned(),
        upstream_id: Some(upstream_id),
        reason: Some("selected_by_policy".to_owned()),
        duration_us: 42,
    };

    let json = serde_json::to_string(&decision).unwrap();
    let decoded: StageDecision = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stage_name, decision.stage_name);
    assert_eq!(decoded.upstream_id, decision.upstream_id);
    assert_eq!(decoded.reason, decision.reason);
}

#[test]
fn stage_decision_with_none_fields() {
    let decision = StageDecision {
        stage_name: "authn".to_owned(),
        upstream_id: None,
        reason: None,
        duration_us: 0,
    };

    let json = serde_json::to_string(&decision).unwrap();
    let decoded: StageDecision = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stage_name, "authn");
    assert!(decoded.upstream_id.is_none());
    assert!(decoded.reason.is_none());
}

#[test]
fn terminal_decision_serde_roundtrip() {
    let upstream_id = Uuid::new_v4();
    let decision = TerminalDecision {
        upstream_id: Some(upstream_id),
        strategy: TerminalStrategy::RoundRobin,
    };

    let json = serde_json::to_string(&decision).unwrap();
    let decoded: TerminalDecision = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.upstream_id, Some(upstream_id));
    assert_eq!(decoded.strategy, TerminalStrategy::RoundRobin);
}

#[test]
fn terminal_decision_default() {
    let decision = TerminalDecision::default();
    assert!(decision.upstream_id.is_none());
    assert_eq!(decision.strategy, TerminalStrategy::FirstPick);
}

#[test]
fn routing_trace_serde_roundtrip() {
    let upstream_id_1 = Uuid::new_v4();
    let upstream_id_2 = Uuid::new_v4();

    let trace = RoutingTrace {
        stages: vec![
            StageDecision {
                stage_name: "authn".to_owned(),
                upstream_id: None,
                reason: None,
                duration_us: 0,
            },
            StageDecision {
                stage_name: "router".to_owned(),
                upstream_id: Some(upstream_id_1),
                reason: Some("healthy".to_owned()),
                duration_us: 7,
            },
        ],
        terminal_decision: Some(TerminalDecision {
            upstream_id: Some(upstream_id_2),
            strategy: TerminalStrategy::FirstPick,
        }),
    };

    let json = serde_json::to_string(&trace).unwrap();
    let decoded: RoutingTrace = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stages.len(), 2);
    assert_eq!(decoded.stages[0].stage_name, "authn");
    assert_eq!(decoded.stages[1].upstream_id, Some(upstream_id_1));
    assert!(decoded.terminal_decision.is_some());
    assert_eq!(
        decoded.terminal_decision.unwrap().upstream_id,
        Some(upstream_id_2)
    );
}

#[test]
fn routing_trace_default() {
    let trace = RoutingTrace::default();
    assert!(trace.stages.is_empty());
    assert!(trace.terminal_decision.is_none());
}

#[test]
fn internal_error_serde_roundtrip() {
    let error = InternalError {
        stage: InternalErrorStage::Signer,
        kind: InternalErrorKind::ConfigError,
        message: Some("missing_api_key".to_owned()),
    };

    let json = serde_json::to_string(&error).unwrap();
    let decoded: InternalError = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stage, InternalErrorStage::Signer);
    assert_eq!(decoded.kind, InternalErrorKind::ConfigError);
    assert_eq!(decoded.message, Some("missing_api_key".to_owned()));
}

#[test]
fn internal_error_without_message() {
    let error = InternalError {
        stage: InternalErrorStage::Router,
        kind: InternalErrorKind::PluginError,
        message: None,
    };

    let json = serde_json::to_string(&error).unwrap();
    let decoded: InternalError = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stage, InternalErrorStage::Router);
    assert_eq!(decoded.kind, InternalErrorKind::PluginError);
    assert!(decoded.message.is_none());
}

#[test]
fn internal_error_default() {
    let error = InternalError::default();
    assert_eq!(error.stage, InternalErrorStage::Router);
    assert_eq!(error.kind, InternalErrorKind::PluginError);
    assert!(error.message.is_none());
}

#[test]
fn cap_constants_defined() {
    assert_eq!(MAX_ROUTING_TRACE_STAGES, 100);
    assert_eq!(MAX_STAGE_NAME_LEN, 256);
    assert_eq!(MAX_ERROR_MESSAGE_LEN, 1024);
}

#[test]
fn complex_routing_trace_with_multiple_stages() {
    let upstream_ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();

    let mut stages = Vec::new();
    for (i, upstream_id) in upstream_ids.iter().copied().enumerate().take(3) {
        stages.push(StageDecision {
            stage_name: format!("stage_{}", i),
            upstream_id: Some(upstream_id),
            reason: Some(format!("reason_{}", i)),
            duration_us: i as u64,
        });
    }

    let trace = RoutingTrace {
        stages,
        terminal_decision: Some(TerminalDecision {
            upstream_id: Some(upstream_ids[2]),
            strategy: TerminalStrategy::LeastConnections,
        }),
    };

    let json = serde_json::to_string(&trace).unwrap();
    let decoded: RoutingTrace = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stages.len(), 3);
    for (i, upstream_id) in upstream_ids.iter().copied().enumerate().take(3) {
        assert_eq!(decoded.stages[i].stage_name, format!("stage_{}", i));
        assert_eq!(decoded.stages[i].upstream_id, Some(upstream_id));
    }
}
