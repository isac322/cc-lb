//! Integration tests for v3 filter wire protocol roundtrip serialization.

use cc_lb_plugin_wire::v2::common::{CandidateWire, HeaderWire, Principal};
use cc_lb_plugin_wire::v3::filter::{
    FilterFn, FilterRequest, FilterResponse, PerCandidateReasonWire,
};
use cc_lb_plugin_wire::wire_function::WireFunction;

#[test]
fn filter_fn_implements_wire_function() {
    assert_eq!(FilterFn::NAME, "filter");
    let _req = FilterFn::dry_run_request();
    let _resp = FilterFn::dry_run_response();
}

#[test]
fn filter_request_json_complex_roundtrip() {
    let request = FilterRequest {
        request_id: String::from("test-req-12345"),
        headers: vec![
            HeaderWire {
                name: String::from("content-type"),
                value_base64: String::from("YXBwbGljYXRpb24vanNvbg=="),
            },
            HeaderWire {
                name: String::from("authorization"),
                value_base64: String::from("QmVhcmVyIHRva2Vu"),
            },
        ],
        method: String::from("POST"),
        path: String::from("/v1/messages"),
        query: Some(String::from("model=claude-3-5-sonnet")),
        body_base64: String::from("eyJtb2RlbCI6ImNsYXVkZS0zLTUtc29ubmV0In0="),
        principal: Principal {
            id: String::from("principal-abc123"),
            kind: String::from("api_key"),
            claims: serde_json::json!({
                "org_id": "org-xyz",
                "user_id": "user-123"
            })
            .as_object()
            .unwrap()
            .clone(),
        },
        candidates: vec![
            CandidateWire {
                upstream_id: String::from("upstream-us-east-1"),
                name: String::from("us-east-1-primary"),
                kind: String::from("anthropic_api_key"),
                observed_rate_limits: vec![],
                subscription_quotas: vec![],
                observed_at_unix_secs: 1717171717,
                cache_score: None,
            },
            CandidateWire {
                upstream_id: String::from("upstream-eu-west-1"),
                name: String::from("eu-west-1-fallback"),
                kind: String::from("anthropic_api_key"),
                observed_rate_limits: vec![],
                subscription_quotas: vec![],
                observed_at_unix_secs: 1717171717,
                cache_score: None,
            },
        ],
    };

    let json = serde_json::to_string(&request).expect("failed to encode request");
    let decoded: FilterRequest = serde_json::from_str(&json).expect("failed to decode request");
    assert_eq!(request, decoded);
}

#[test]
fn filter_response_json_complex_roundtrip() {
    let response = FilterResponse {
        results: vec![
            PerCandidateReasonWire {
                upstream_id: String::from("upstream-us-east-1"),
                decision: String::from("accept"),
                reason: String::from("quota available, no rate limits"),
            },
            PerCandidateReasonWire {
                upstream_id: String::from("upstream-eu-west-1"),
                decision: String::from("reject"),
                reason: String::from("rate limit window exceeded"),
            },
        ],
    };

    let json = serde_json::to_string(&response).expect("failed to encode response");
    let decoded: FilterResponse = serde_json::from_str(&json).expect("failed to decode response");
    assert_eq!(response, decoded);
}

#[test]
fn filter_request_json_roundtrip() {
    let request = FilterRequest::dry_run_sample();
    let json = serde_json::to_string(&request).expect("failed to encode JSON");
    let parsed: FilterRequest = serde_json::from_str(&json).expect("failed to parse JSON");
    assert_eq!(request, parsed);
}

#[test]
fn filter_response_json_roundtrip() {
    let response = FilterResponse::dry_run_sample();
    let json = serde_json::to_string(&response).expect("failed to encode JSON");
    let parsed: FilterResponse = serde_json::from_str(&json).expect("failed to parse JSON");
    assert_eq!(response, parsed);
}

#[test]
fn filter_request_with_multiple_candidates_roundtrip() {
    let request = FilterRequest {
        request_id: String::from("multi-candidate-test"),
        headers: vec![],
        method: String::from("POST"),
        path: String::from("/v1/messages"),
        query: None,
        body_base64: String::new(),
        principal: Principal::dry_run_sample(),
        candidates: vec![
            CandidateWire::dry_run_sample(),
            CandidateWire {
                upstream_id: String::from("upstream-2"),
                name: String::from("candidate-2"),
                kind: String::from("anthropic_api_key"),
                observed_rate_limits: vec![],
                subscription_quotas: vec![],
                observed_at_unix_secs: 1717171717,
                cache_score: None,
            },
            CandidateWire {
                upstream_id: String::from("upstream-3"),
                name: String::from("candidate-3"),
                kind: String::from("anthropic_api_key"),
                observed_rate_limits: vec![],
                subscription_quotas: vec![],
                observed_at_unix_secs: 1717171717,
                cache_score: None,
            },
        ],
    };

    let json = serde_json::to_string(&request).expect("failed to encode JSON");
    let parsed: FilterRequest = serde_json::from_str(&json).expect("failed to parse JSON");
    assert_eq!(request, parsed);
}

#[test]
fn filter_response_with_mixed_decisions_roundtrip() {
    let response = FilterResponse {
        results: vec![
            PerCandidateReasonWire {
                upstream_id: String::from("id-1"),
                decision: String::from("accept"),
                reason: String::from("all checks pass"),
            },
            PerCandidateReasonWire {
                upstream_id: String::from("id-2"),
                decision: String::from("reject"),
                reason: String::from("failed filter A"),
            },
            PerCandidateReasonWire {
                upstream_id: String::from("id-3"),
                decision: String::from("accept"),
                reason: String::from("all checks pass"),
            },
        ],
    };

    let json = serde_json::to_string(&response).expect("failed to encode JSON");
    let parsed: FilterResponse = serde_json::from_str(&json).expect("failed to parse JSON");
    assert_eq!(response, parsed);
}

#[test]
fn filter_response_empty_results_roundtrip() {
    let response = FilterResponse { results: vec![] };

    let json = serde_json::to_string(&response).expect("failed to encode JSON");
    let parsed: FilterResponse = serde_json::from_str(&json).expect("failed to parse JSON");
    assert_eq!(response, parsed);
}

#[test]
fn per_candidate_reason_wire_deny_unknown_fields() {
    let json = r#"{
        "upstream_id": "test-id",
        "decision": "accept",
        "reason": "test",
        "unknown_field": "value"
    }"#;
    let result: Result<PerCandidateReasonWire, _> = serde_json::from_str(json);
    assert!(result.is_err(), "should reject unknown fields");
}

#[test]
fn filter_request_deny_unknown_fields() {
    let json = r#"{
        "request_id": "test",
        "headers": [],
        "method": "POST",
        "path": "/v1/messages",
        "query": null,
        "body_base64": "",
        "principal": {
            "id": "test",
            "kind": "api_key",
            "claims": {}
        },
        "candidates": [],
        "unknown_field": "should_fail"
    }"#;
    let result: Result<FilterRequest, _> = serde_json::from_str(json);
    assert!(result.is_err(), "should reject unknown fields");
}

#[test]
fn filter_response_deny_unknown_fields() {
    let json = r#"{
        "results": [],
        "unknown_field": "should_fail"
    }"#;
    let result: Result<FilterResponse, _> = serde_json::from_str(json);
    assert!(result.is_err(), "should reject unknown fields");
}
