//! v2 is a fork of v1 to allow additive cache-related fields. v1 is FROZEN.

extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::v2::common::{CandidateWire, HeaderWire, Principal};
use crate::wire_function::{FallbackPolicy, WireFunction};

pub use crate::v2::common::{DialectBinding, UpstreamWire as UpstreamSpec};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RouteRequest {
    pub request_id: String,
    pub headers: Vec<HeaderWire>,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body_base64: String,
    pub principal: Principal,
    pub candidates: Vec<CandidateWire>,
    #[serde(default)]
    pub cache_breakpoints: Vec<crate::v2::common::CacheBreakpointWire>,
    #[serde(default)]
    pub canonical_model_id: String,
}

impl RouteRequest {
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
            cache_breakpoints: Vec::new(),
            canonical_model_id: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RouteResponse {
    pub upstream_id: Option<String>,
    pub dialect: DialectBinding,
    pub upstream: UpstreamSpec,
}

impl RouteResponse {
    pub fn dry_run_sample() -> Self {
        Self {
            upstream_id: None,
            dialect: DialectBinding::dry_run_sample(),
            upstream: UpstreamSpec::dry_run_sample(),
        }
    }
}

pub struct RouteFn;

impl WireFunction for RouteFn {
    const NAME: &'static str = "route";
    const FALLBACK: FallbackPolicy = FallbackPolicy::UseDefault;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = RouteRequest;
    type Response = RouteResponse;

    fn dry_run_request() -> Self::Request {
        RouteRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        RouteResponse::dry_run_sample()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;

    #[test]
    fn route_request_cache_fields_roundtrip() {
        let mut request = RouteRequest::dry_run_sample();
        request.cache_breakpoints = alloc::vec![crate::v2::common::CacheBreakpointWire {
            block_index: 0,
            source: crate::v2::common::CacheBreakpointSourceWire::Tools,
            path: String::from("/v1/messages"),
            message_index: Some(0),
            prefix_hash: String::from("abc123"),
            prefix_token_count: 1024,
            requested_ttl: crate::v2::common::TtlClassWire::Ephemeral5m,
            origin: crate::v2::common::BreakpointOriginWire::Explicit,
        }];
        request.canonical_model_id = String::from("claude-3-5-sonnet-20241022");

        let json = serde_json::to_string(&request).unwrap();
        let parsed: RouteRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, parsed);
    }

    #[test]
    fn route_request_deserialize_without_cache_fields() {
        let json = r#"{
            "request_id": "test-request",
            "headers": [],
            "method": "POST",
            "path": "/v1/messages",
            "query": null,
            "body_base64": "",
            "principal": {
                "id": "test-principal",
                "kind": "api_key",
                "claims": {}
            },
            "candidates": []
        }"#;
        let parsed: RouteRequest = serde_json::from_str(json).unwrap();
        assert!(parsed.cache_breakpoints.is_empty());
        assert!(parsed.canonical_model_id.is_empty());
    }

    #[test]
    fn route_request_with_empty_cache_breakpoints_roundtrip() {
        let request = RouteRequest::dry_run_sample();
        let json = serde_json::to_string(&request).unwrap();

        let parsed: RouteRequest = serde_json::from_str(&json).unwrap();
        assert!(parsed.cache_breakpoints.is_empty());
        assert!(parsed.canonical_model_id.is_empty());

        let reparsed_json = serde_json::to_string(&parsed).unwrap();
        assert_eq!(json, reparsed_json);
    }
}
