//! v3 filter wire protocol for per-candidate filtering decisions.

extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::v2::common::{CandidateWire, HeaderWire, Principal};
use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FilterRequest {
    pub request_id: String,
    pub headers: Vec<HeaderWire>,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body_base64: String,
    pub principal: Principal,
    pub candidates: Vec<CandidateWire>,
}

impl FilterRequest {
    pub fn dry_run_sample() -> Self {
        Self {
            request_id: String::from("dry-run-request"),
            headers: Vec::new(),
            method: String::from("POST"),
            path: String::from("/v1/messages"),
            query: None,
            body_base64: String::new(),
            principal: Principal::dry_run_sample(),
            candidates: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PerCandidateReasonWire {
    pub upstream_id: String,
    pub decision: String,
    pub reason: String,
}

impl PerCandidateReasonWire {
    pub fn dry_run_sample() -> Self {
        Self {
            upstream_id: String::from("00000000-0000-0000-0000-000000000000"),
            decision: String::from("accept"),
            reason: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FilterResponse {
    pub results: Vec<PerCandidateReasonWire>,
}

impl FilterResponse {
    pub fn dry_run_sample() -> Self {
        Self {
            results: alloc::vec![PerCandidateReasonWire::dry_run_sample()],
        }
    }
}

pub struct FilterFn;

impl WireFunction for FilterFn {
    const NAME: &'static str = "filter";
    const FALLBACK: FallbackPolicy = FallbackPolicy::UseDefault;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = FilterRequest;
    type Response = FilterResponse;

    fn dry_run_request() -> Self::Request {
        FilterRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        FilterResponse::dry_run_sample()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;

    #[test]
    fn filter_request_roundtrip() {
        let mut request = FilterRequest::dry_run_sample();
        request.headers = alloc::vec![HeaderWire {
            name: String::from("content-type"),
            value_base64: String::from("YXBwbGljYXRpb24vanNvbg=="),
        }];
        request.candidates = alloc::vec![CandidateWire::dry_run_sample()];

        let json = serde_json::to_string(&request).unwrap();
        let parsed: FilterRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, parsed);
    }

    #[test]
    fn filter_response_roundtrip() {
        let mut response = FilterResponse::dry_run_sample();
        response.results = alloc::vec![
            PerCandidateReasonWire {
                upstream_id: String::from("upstream-1"),
                decision: String::from("accept"),
                reason: String::from("quota available"),
            },
            PerCandidateReasonWire {
                upstream_id: String::from("upstream-2"),
                decision: String::from("reject"),
                reason: String::from("rate limit exceeded"),
            },
        ];

        let json = serde_json::to_string(&response).unwrap();
        let parsed: FilterResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response, parsed);
    }

    #[test]
    fn per_candidate_reason_wire_roundtrip() {
        let reason = PerCandidateReasonWire {
            upstream_id: String::from("test-upstream-id"),
            decision: String::from("accept"),
            reason: String::from("all checks passed"),
        };

        let json = serde_json::to_string(&reason).unwrap();
        let parsed: PerCandidateReasonWire = serde_json::from_str(&json).unwrap();
        assert_eq!(reason, parsed);
    }

    #[test]
    fn filter_request_empty_candidates() {
        let request = FilterRequest::dry_run_sample();
        let json = serde_json::to_string(&request).unwrap();
        let parsed: FilterRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, parsed);
    }

    #[test]
    fn filter_response_empty_results() {
        let response = FilterResponse {
            results: Vec::new(),
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: FilterResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response, parsed);
    }
}
