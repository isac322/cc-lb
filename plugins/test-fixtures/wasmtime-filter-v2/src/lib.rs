#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;

use cc_lb_pdk_wasmtime::types::v2::{FilterRequest, FilterResponse, PerCandidateReason};

#[cc_lb_pdk_wasmtime::plugin(
    name = "wasmtime-filter-v2",
    version = "0.1.0",
    description = "Test fixture that echoes the requested service tier",
    usage = "Testing only"
)]
mod fixture {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(
        filter,
        wire = 2,
        description = "Returns the requested service tier as its reason",
        usage = "Testing only"
    )]
    pub fn filter(request: FilterRequest) -> FilterResponse {
        assert_eq!(request.service_tier.as_deref(), Some("priority"));
        FilterResponse {
            results: Box::new([PerCandidateReason {
                upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
                decision: Box::from("reject"),
                reason: request.service_tier.unwrap_or_else(|| Box::from("none")),
            }]),
        }
    }
}

pub use fixture::filter;
