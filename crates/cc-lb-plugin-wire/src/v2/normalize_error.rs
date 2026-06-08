//! v2 is a fork of v1 to allow additive cache-related fields. v1 is FROZEN.

extern crate alloc;

use alloc::string::String;
use serde::{Deserialize, Serialize};

use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NormalizeErrorRequest {
    pub status: u16,
    pub body_base64: String,
}

impl NormalizeErrorRequest {
    pub fn dry_run_sample() -> Self {
        Self {
            status: 500,
            body_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NormalizeErrorResponse {
    pub body_base64: Option<String>,
}

impl NormalizeErrorResponse {
    pub fn dry_run_sample() -> Self {
        Self { body_base64: None }
    }
}

pub struct NormalizeErrorFn;

impl WireFunction for NormalizeErrorFn {
    const NAME: &'static str = "normalize_error";
    const FALLBACK: FallbackPolicy = FallbackPolicy::PassThrough;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = NormalizeErrorRequest;
    type Response = NormalizeErrorResponse;

    fn dry_run_request() -> Self::Request {
        NormalizeErrorRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        NormalizeErrorResponse::dry_run_sample()
    }
}
