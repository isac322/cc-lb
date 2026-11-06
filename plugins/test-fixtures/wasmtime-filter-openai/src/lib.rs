#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use cc_lb_pdk_wasmtime::types::v1::{FilterRequest, FilterResponse, PerCandidateReason};

#[cc_lb_pdk_wasmtime::plugin(
    name = "wasmtime-filter-openai",
    version = "0.1.0",
    description = "Test fixture with a provider-qualified OpenAI filter.",
    usage = "Testing only."
)]
mod fixture {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(
        filter,
        provider = "openai",
        wire = 1,
        description = "Accepts OpenAI candidates.",
        usage = "Testing only."
    )]
    pub fn filter_openai(request: FilterRequest) -> FilterResponse {
        let results = request
            .candidates
            .into_vec()
            .into_iter()
            .map(|candidate| PerCandidateReason {
                upstream_id: candidate.upstream_id,
                decision: Box::from("accept"),
                reason: Box::from("openai-qualified-filter"),
            })
            .collect::<alloc::vec::Vec<_>>()
            .into_boxed_slice();
        FilterResponse { results }
    }
}

pub use fixture::filter_openai;
