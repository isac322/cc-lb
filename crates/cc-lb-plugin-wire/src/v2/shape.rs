//! v2 is a fork of v1 to allow additive cache-related fields. v1 is FROZEN.

extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::v2::common::{HeaderWire, Principal, RequestWire, UpstreamWire};
use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShapeRequest {
    pub request: RequestWire,
    pub upstream: UpstreamWire,
    pub principal: Principal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_base_url: Option<String>,
}

impl ShapeRequest {
    pub fn dry_run_sample() -> Self {
        Self {
            request: RequestWire::dry_run_sample(),
            upstream: UpstreamWire::dry_run_sample(),
            principal: Principal::dry_run_sample(),
            upstream_base_url: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShapeResponse {
    pub url: String,
    pub method: String,
    pub headers: Vec<HeaderWire>,
    pub body_base64: String,
}

impl ShapeResponse {
    pub fn dry_run_sample() -> Self {
        Self {
            url: String::from("https://api.anthropic.com/v1/messages"),
            method: String::from("POST"),
            headers: Vec::new(),
            body_base64: String::new(),
        }
    }
}

pub struct ShapeFn;

impl WireFunction for ShapeFn {
    const NAME: &'static str = "shape";
    const FALLBACK: FallbackPolicy = FallbackPolicy::FailRequest;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = ShapeRequest;
    type Response = ShapeResponse;

    fn dry_run_request() -> Self::Request {
        ShapeRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        ShapeResponse::dry_run_sample()
    }
}
