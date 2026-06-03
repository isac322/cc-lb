use serde::{Deserialize, Serialize};

use crate::v1::common::UpstreamWire;
use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BuildSignerRequest {
    pub upstream: UpstreamWire,
    pub factory_state: serde_json::Value,
}

impl BuildSignerRequest {
    pub fn dry_run_sample() -> Self {
        Self {
            upstream: UpstreamWire::dry_run_sample(),
            factory_state: serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BuildSignerResponse {
    pub signer_state: serde_json::Value,
}

impl BuildSignerResponse {
    pub fn dry_run_sample() -> Self {
        Self {
            signer_state: serde_json::Value::Null,
        }
    }
}

pub struct BuildSignerFn;

impl WireFunction for BuildSignerFn {
    const NAME: &'static str = "build_signer";
    const FALLBACK: FallbackPolicy = FallbackPolicy::FailRequest;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = BuildSignerRequest;
    type Response = BuildSignerResponse;

    fn dry_run_request() -> Self::Request {
        BuildSignerRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        BuildSignerResponse::dry_run_sample()
    }
}
