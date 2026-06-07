//! v2 is a fork of v1 to allow additive cache-related fields. v1 is FROZEN.

extern crate alloc;

use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

use crate::v2::common::ObserveEventWire;
use crate::wire_function::{FallbackPolicy, WireFunction};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObserveRequest {
    pub events: Vec<ObserveEventWire>,
}

impl ObserveRequest {
    pub fn dry_run_sample() -> Self {
        Self { events: Vec::new() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObserveResponse {}

impl ObserveResponse {
    pub fn dry_run_sample() -> Self {
        Self {}
    }
}

pub struct ObserveFn;

impl WireFunction for ObserveFn {
    const NAME: &'static str = "observe";
    const FALLBACK: FallbackPolicy = FallbackPolicy::SilentSkip;
    const SUPPORTED_VERSIONS: &'static [u32] = &[1];

    type Request = ObserveRequest;
    type Response = ObserveResponse;

    fn dry_run_request() -> Self::Request {
        ObserveRequest::dry_run_sample()
    }

    fn dry_run_response() -> Self::Response {
        ObserveResponse::dry_run_sample()
    }
}
