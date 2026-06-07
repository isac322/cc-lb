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
