extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::v1::common::{HeaderWire, ShapedRequestWire};
use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignRequest {
    pub shaped: ShapedRequestWire,
    pub signer_state: serde_json::Value,
}

impl SignRequest {
    pub fn dry_run_sample() -> Self {
        Self {
            shaped: ShapedRequestWire::dry_run_sample(),
            signer_state: serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignResponse {
    pub url: Option<String>,
    pub method: Option<String>,
    pub headers: Option<Vec<HeaderWire>>,
    pub body_base64: Option<String>,
}

impl SignResponse {
    pub fn dry_run_sample() -> Self {
        Self {
            url: None,
            method: None,
            headers: None,
            body_base64: None,
        }
    }
}

pub struct SignFn;

impl WireFunction for SignFn {
    const NAME: &'static str = "sign";
    const FALLBACK: FallbackPolicy = FallbackPolicy::FailRequest;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = SignRequest;
    type Response = SignResponse;

    fn dry_run_request() -> Self::Request {
        SignRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        SignResponse::dry_run_sample()
    }
}
