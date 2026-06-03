extern crate alloc;

use alloc::string::String;
use serde::{Deserialize, Serialize};

use crate::v1::common::UpstreamErrorWire;
use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OnUnauthorizedRequest {
    pub error: UpstreamErrorWire,
    pub signer_state: serde_json::Value,
}

impl OnUnauthorizedRequest {
    pub fn dry_run_sample() -> Self {
        Self {
            error: UpstreamErrorWire::dry_run_sample(),
            signer_state: serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OnUnauthorizedResponse {
    pub decision: String,
    pub signer_state: serde_json::Value,
}

impl OnUnauthorizedResponse {
    pub fn dry_run_sample() -> Self {
        Self {
            decision: String::from("fail"),
            signer_state: serde_json::Value::Null,
        }
    }
}

pub struct OnUnauthorizedFn;

impl WireFunction for OnUnauthorizedFn {
    const NAME: &'static str = "on_unauthorized";
    const FALLBACK: FallbackPolicy = FallbackPolicy::PassThrough;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = OnUnauthorizedRequest;
    type Response = OnUnauthorizedResponse;

    fn dry_run_request() -> Self::Request {
        OnUnauthorizedRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        OnUnauthorizedResponse::dry_run_sample()
    }
}
