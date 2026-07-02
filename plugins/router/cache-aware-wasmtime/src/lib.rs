//! Cache-affinity router authored against the wasmtime PDK.
//!
//! Phase 1 W7 — first plugin to exercise the
//! `cc-lb-pdk-wasmtime` (`#[plugin]` + `#[handler]`) + rkyv wire path
//! end-to-end. Algorithm: rank candidates by
//! `predicted_cache_read_tokens` desc, keep the top `keep_k`
//! (default 1).
//!
//! `keep_k` lifts from a `keep_k` claim on the principal because the
//! W4 PDK ships no `cc_lb_init` config wiring yet — Phase 2 will route
//! `PluginManifest::config` through the optional `cc_lb_init` export
//! and replace this lookup.
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::str;

use cc_lb_pdk_wasmtime::types::{
    ArchivedFilterRequest, ArchivedUpstreamCandidate, FilterResponse, PerCandidateReason,
};

const DEFAULT_KEEP_K: usize = 1;
const ACCEPT_DECISION: &str = "accept";
const REJECT_DECISION: &str = "reject";
const ACCEPT_REASON: &str = "top-K by cache_score";
const REJECT_REASON: &str = "below K by cache_score";
const KEEP_K_CLAIM: &str = "keep_k";

#[cc_lb_pdk_wasmtime::plugin(name = "cache-aware-wasmtime", version = "0.1.0")]
mod cache_aware {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(name = "filter", view)]
    pub fn filter(req: &ArchivedFilterRequest) -> FilterResponse {
        let candidates: &[ArchivedUpstreamCandidate] = &req.candidates;
        let keep_k = keep_k_from_principal(req);
        let kept_indices = rank_top_k(candidates, keep_k);

        let mut keep_mask: Vec<bool> = Vec::with_capacity(candidates.len());
        keep_mask.resize(candidates.len(), false);
        for idx in kept_indices {
            keep_mask[idx] = true;
        }

        let results = candidates
            .iter()
            .enumerate()
            .map(|(i, candidate)| candidate_decision(candidate, keep_mask[i]))
            .collect();

        FilterResponse { results }
    }
}

pub use cache_aware::filter;

fn candidate_decision(candidate: &ArchivedUpstreamCandidate, keep: bool) -> PerCandidateReason {
    let upstream_id_str: &str = &candidate.upstream_id;
    PerCandidateReason {
        upstream_id: Box::from(upstream_id_str),
        decision: Box::from(if keep {
            ACCEPT_DECISION
        } else {
            REJECT_DECISION
        }),
        reason: Box::from(if keep { ACCEPT_REASON } else { REJECT_REASON }),
    }
}

fn rank_top_k(candidates: &[ArchivedUpstreamCandidate], keep_k: usize) -> Vec<usize> {
    let n = candidates.len();
    let mut ranked: Vec<usize> = Vec::with_capacity(n);
    ranked.extend(0..n);
    ranked.sort_by(|a, b| {
        // Archived<u32> is the native-endian rkyv form. .to_native() converts
        // to a host-order u32 for ordering.
        let lhs = candidates[*a].predicted_cache_read_tokens.to_native();
        let rhs = candidates[*b].predicted_cache_read_tokens.to_native();
        rhs.cmp(&lhs)
    });
    ranked.truncate(keep_k.max(DEFAULT_KEEP_K));
    ranked
}

fn keep_k_from_principal(req: &ArchivedFilterRequest) -> usize {
    req.principal
        .claims
        .iter()
        .find(|entry| {
            let key: &str = &entry.key;
            key == KEEP_K_CLAIM
        })
        .and_then(|entry| {
            let value: &[u8] = &entry.value;
            str::from_utf8(value).ok()
        })
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_KEEP_K)
}
