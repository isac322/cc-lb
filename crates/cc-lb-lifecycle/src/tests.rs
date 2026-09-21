use cc_lb_domain::PrincipalKindLite;
use cc_lb_request_log::{CostBreakdown, HeaderSnapshot, RequestCacheState};
use uuid::Uuid;

use crate::*;

fn sample_event_id() -> EventId {
    "01978c00-0000-7000-8000-000000000000".to_owned()
}

#[test]
fn request_started_roundtrip() {
    let event = LifecycleEvent::RequestStarted {
        event_id: sample_event_id(),
        request_id: "req-123".to_owned(),
        ts_ms: 1_730_000_000_000,
        stream: true,
        source_kind: Some("proxy".to_owned()),
        source_ref_id: Some("ingress-123".to_owned()),
        event_kind: None,
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let restored: LifecycleEvent = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(event, restored);
}

#[test]
fn request_started_source_metadata_defaults_without_legacy_json_fields() {
    let legacy_started = r#"{
        "kind":"request_started",
        "event_id":"01978c00-0000-7000-8000-000000000000",
        "request_id":"req-legacy",
        "ts_ms":1730000000000,
        "stream":false
    }"#;

    let started: LifecycleEvent =
        serde_json::from_str(legacy_started).expect("deserialize legacy request started");

    assert!(matches!(
        &started,
        LifecycleEvent::RequestStarted {
            source_kind: None,
            source_ref_id: None,
            ..
        }
    ));
    let started_json = serde_json::to_value(&started).expect("serialize legacy request started");
    let started_object = started_json
        .as_object()
        .expect("request started serializes to object");
    assert!(!started_object.contains_key("source_kind"));
    assert!(!started_object.contains_key("source_ref_id"));
}

#[test]
fn request_terminated_preserves_nested_io_timings_and_defaults_legacy_payloads() {
    let event = LifecycleEvent::RequestTerminated {
        event_id: sample_event_id(),
        reason: TerminationReason::Success,
        client_status: 200,
        duration_ms: 1,
        request_body_read_ms: None,
        request_body_bytes: None,
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: Some(1),
        setup_timings: RequestSetupTimings {
            json_parse_ms: Some(0.125),
            cache_structure_ms: Some(0.0),
            cache_tokenize_ms: Some(0.25),
            ..RequestSetupTimings::default()
        },
        io_timings: RequestIoTimings {
            request_body_first_chunk_ms: Some(0.0),
            request_body_receive_ms: Some(1.25),
            request_body_wait_ms: Some(0.5),
            request_body_process_ms: Some(0.125),
            request_body_chunk_count: Some(0),
            response_body_wait_ms: Some(3.5),
            response_body_process_ms: Some(0.375),
            response_body_downstream_poll_gap_ms: Some(2.0),
            retry_overhead_ms: None,
        },
        upstream_body_ms: None,
        first_body_chunk_ms: None,
        finalize_ms: None,
        internal_errors: Vec::new(),
        event_kind: None,
    };

    let json = serde_json::to_value(&event).expect("serialize terminal setup timings");
    assert_eq!(json["json_parse_ms"], 0.125);
    assert_eq!(json["cache_structure_ms"], 0.0);
    assert!(json.get("setup_timings").is_none());
    assert_eq!(json["io_timings"]["request_body_first_chunk_ms"], 0.0);
    assert_eq!(json["io_timings"]["request_body_chunk_count"], 0);
    assert_eq!(json["io_timings"]["response_body_process_ms"], 0.375);
    assert!(json["io_timings"].get("retry_overhead_ms").is_none());
    assert!(json.get("request_body_first_chunk_ms").is_none());
    let restored: LifecycleEvent =
        serde_json::from_value(json).expect("deserialize terminal setup timings");
    assert_eq!(restored, event);

    let legacy = r#"{
        "kind":"request_terminated",
        "event_id":"01978c00-0000-7000-8000-000000000000",
        "reason":"success",
        "client_status":200,
        "duration_ms":1
    }"#;
    let restored: LifecycleEvent =
        serde_json::from_str(legacy).expect("deserialize legacy terminal event");
    assert!(matches!(
        restored,
        LifecycleEvent::RequestTerminated {
            setup_timings,
            io_timings,
            ..
        } if setup_timings == RequestSetupTimings::default()
            && io_timings == RequestIoTimings::default()
    ));

    let explicit_null = r#"{
        "kind":"request_terminated",
        "event_id":"01978c00-0000-7000-8000-000000000000",
        "reason":"success",
        "client_status":200,
        "duration_ms":1,
        "io_timings":{"request_body_wait_ms":null}
    }"#;
    let restored: LifecycleEvent =
        serde_json::from_str(explicit_null).expect("deserialize null I/O timing");
    assert!(matches!(
        restored,
        LifecycleEvent::RequestTerminated {
            io_timings: RequestIoTimings {
                request_body_wait_ms: None,
                ..
            },
            ..
        }
    ));
}

#[test]
fn kind_labels_cover_every_variant() {
    // Anti-regression: if a new variant is added without updating kind()
    // the compile-checked match in kind() will fail. This runtime test
    // additionally locks the label strings that Prometheus depends on.
    let labels = [
        LifecycleEvent::RequestStarted {
            event_id: sample_event_id(),
            request_id: "r".into(),
            ts_ms: 0,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
        }
        .kind(),
        LifecycleEvent::ParseCompleted {
            event_id: sample_event_id(),
            result: Err(ParseFailure::BodyTooLarge { limit_bytes: 1 }),
        }
        .kind(),
        LifecycleEvent::AuthCompleted {
            event_id: sample_event_id(),
            result: Err(AuthFailure::AuthenticationFailed {
                http_status: 401,
                reason: None,
            }),
        }
        .kind(),
        LifecycleEvent::AuthenticationCompleted {
            event_id: sample_event_id(),
            principal_id: "principal".into(),
            principal_kind: PrincipalKindLite::Machine,
        }
        .kind(),
        LifecycleEvent::RouteCompleted {
            event_id: sample_event_id(),
            result: Err(RouteFailure::RouteNotConfigured),
            routing_trace: None,
        }
        .kind(),
        LifecycleEvent::LimitDecision {
            event_id: sample_event_id(),
            decision: LimitDecisionKind::Rejected {
                reason: "quota".into(),
                subject: None,
                request_summary: None,
                route_summary: None,
                limit_violation: None,
            },
        }
        .kind(),
        LifecycleEvent::UpstreamAttempt {
            event_id: sample_event_id(),
            attempt_num: 1,
            upstream_id: Uuid::nil(),
        }
        .kind(),
        LifecycleEvent::UpstreamResponseStarted {
            event_id: sample_event_id(),
            status: 200,
            headers: HeaderSnapshot::default(),
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            shape_ms: None,
            sign_ms: None,
            upstream_ttfb_ms: None,
        }
        .kind(),
        LifecycleEvent::ProviderErrorObserved {
            event_id: sample_event_id(),
            code: "provider_error".into(),
            message: "redacted".into(),
            source: "provider".into(),
        }
        .kind(),
        LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: sample_event_id(),
            error_type: "rate_limit_error".into(),
            error_message: "bounded".into(),
        }
        .kind(),
        LifecycleEvent::UsageObserved {
            event_id: sample_event_id(),
            usage: UsageSnapshot::default(),
            source: UsageSource::MessageStart,
        }
        .kind(),
        LifecycleEvent::StreamCompleted {
            event_id: sample_event_id(),
            result: Ok(StreamSuccess::default()),
        }
        .kind(),
        LifecycleEvent::RequestTerminated {
            event_id: sample_event_id(),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
            request_body_read_ms: None,
            request_body_bytes: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            finalize_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        }
        .kind(),
        LifecycleEvent::Priced {
            event_id: sample_event_id(),
            cost: CostBreakdown::default(),
        }
        .kind(),
        LifecycleEvent::CacheObserved {
            event_id: sample_event_id(),
            cache_state: RequestCacheState::Unknown,
        }
        .kind(),
    ];
    assert_eq!(
        labels,
        [
            "request_started",
            "parse_completed",
            "auth_completed",
            "authentication_completed",
            "route_completed",
            "limit_decision",
            "upstream_attempt",
            "upstream_response_started",
            "provider_error_observed",
            "request_log_upstream_error_observed",
            "usage_observed",
            "stream_completed",
            "request_terminated",
            "priced",
            "cache_observed",
        ],
    );
}

#[test]
fn event_id_accessor_returns_stable_reference() {
    let id = sample_event_id();
    let event = LifecycleEvent::RequestLogUpstreamErrorObserved {
        event_id: id.clone(),
        error_type: "api_error".into(),
        error_message: "bounded".into(),
    };
    assert_eq!(event.event_id(), &id);
}

#[test]
fn termination_reason_kind_is_stable() {
    assert_eq!(TerminationReason::Success.kind(), "success");
    assert_eq!(
        TerminationReason::ErrorCode("upstream_4xx".into()).kind(),
        "error_code",
    );
    assert_eq!(TerminationReason::Dropped.kind(), "dropped");
}
