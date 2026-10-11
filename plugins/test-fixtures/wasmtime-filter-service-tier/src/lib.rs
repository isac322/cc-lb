#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;

#[cfg(not(feature = "quota-v2"))]
use cc_lb_pdk_wasmtime::types::v1::FilterRequest;
use cc_lb_pdk_wasmtime::types::v1::{FilterResponse, PerCandidateReason};

#[cfg(not(feature = "quota-v2"))]
#[cc_lb_pdk_wasmtime::plugin(
    name = "wasmtime-filter-service-tier",
    version = "0.1.0",
    description = "Test fixture that echoes the requested service tier",
    usage = "Testing only"
)]
mod fixture {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(
        filter,
        wire = 1,
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

#[cfg(all(feature = "quota-v2", not(feature = "quota-v2-view")))]
#[cc_lb_pdk_wasmtime::plugin(
    name = "wasmtime-filter-service-tier",
    version = "0.1.0",
    description = "Test fixture that reads V2 subscription quotas",
    usage = "Testing only"
)]
mod fixture {
    use super::*;
    #[cc_lb_pdk_wasmtime::handler(
        filter,
        wire = 2,
        description = "Echoes the number of quota windows",
        usage = "Testing only"
    )]
    pub fn filter(request: cc_lb_pdk_wasmtime::types::v2::FilterRequest) -> FilterResponse {
        assert_eq!(request.routing_now_unix_secs, 1_800_000_000);
        FilterResponse {
            results: request
                .candidates
                .iter()
                .map(|candidate| PerCandidateReason {
                    upstream_id: Box::from(&*candidate.upstream_id),
                    decision: Box::from("accept"),
                    reason: alloc::format!("quota_windows={}", candidate.subscription_quotas.len())
                        .into_boxed_str(),
                })
                .collect::<alloc::vec::Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

#[cfg(feature = "quota-v2-view")]
#[cc_lb_pdk_wasmtime::plugin(
    name = "wasmtime-filter-service-tier",
    version = "0.1.0",
    description = "Test fixture that reads V2 subscription quotas",
    usage = "Testing only"
)]
mod fixture {
    use super::*;
    #[cc_lb_pdk_wasmtime::handler(
        filter,
        wire = 2,
        description = "Echoes the number of quota windows",
        usage = "Testing only",
        view
    )]
    pub fn filter(
        request: &cc_lb_pdk_wasmtime::types::v2::ArchivedFilterRequest,
    ) -> FilterResponse {
        assert_eq!(request.routing_now_unix_secs, 1_800_000_000);
        FilterResponse {
            results: request
                .candidates
                .iter()
                .map(|candidate| PerCandidateReason {
                    upstream_id: Box::from(&*candidate.upstream_id),
                    decision: Box::from("accept"),
                    reason: alloc::format!("quota_windows={}", candidate.subscription_quotas.len())
                        .into_boxed_str(),
                })
                .collect::<alloc::vec::Vec<_>>()
                .into_boxed_slice(),
        }
    }
}
