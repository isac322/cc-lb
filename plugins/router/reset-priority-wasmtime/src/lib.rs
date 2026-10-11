//! Reset-priority router plugin for cc-lb (wasmtime PDK).
//!
//! Ranks Claude subscription accounts by the quota that expires soonest.
//!
//! Rank expiring subscription quota while preserving comparable warm caches.

extern crate alloc;

/// Pure routing decisions for host tests and the wasm adapter.
///
pub mod algorithm;

use alloc::boxed::Box;
use alloc::vec::Vec;

use cc_lb_pdk_wasmtime::types::v1::{FilterResponse, PerCandidateReason};
use cc_lb_pdk_wasmtime::types::v2::{ArchivedFilterRequest, ArchivedUpstreamCandidate};

use algorithm::{
    CandidateQuotas, CandidateView, DataState, Decision, QuotaWindow, RequestView,
    WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY, decide, model_weekly_window,
};

#[cc_lb_pdk_wasmtime::plugin(
    name = "reset-priority",
    version = "0.1.0",
    description = "Prefers expiring weekly subscription quota, then warm prompt caches",
    usage = "Add as a filter chain entry ahead of the built-in subscription-preference entry. Ranks OAuth candidates by weekly expiry urgency, reuses warm caches before five-hour headroom, and blocks exhausted accounts."
)]
mod reset_priority {
    // The macro reads names from the enclosing scope by design.
    #[allow(clippy::wildcard_imports)]
    use super::*;

    #[must_use]
    #[cc_lb_pdk_wasmtime::handler(
        filter,
        wire = 2,
        description = "Keeps one subscription by weekly urgency and cache affinity",
        usage = "Reads the 5h/7d quota snapshots on each candidate, keeps exactly one OAuth candidate (or the API-key fallback), and reports a per-candidate reason.",
        view
    )]
    pub fn filter(request: &ArchivedFilterRequest) -> FilterResponse {
        let model_window = model_weekly_window(model_id(request));
        let extracted: Vec<ExtractedQuotas> = request
            .candidates
            .iter()
            .map(|candidate| extract_quotas(candidate, model_window))
            .collect();
        let views: Vec<CandidateView<'_>> = request
            .candidates
            .iter()
            .zip(extracted.iter())
            .map(|(candidate, quotas)| {
                let upstream_id: &str = &candidate.upstream_id;
                let kind: &str = &candidate.kind;
                CandidateView {
                    upstream_id,
                    kind,
                    observed_at_unix_secs: candidate.observed_at_unix_secs.to_native(),
                    predicted_cache_read_tokens: candidate.predicted_cache_read_tokens.to_native(),
                    quotas: quotas.as_quotas(),
                }
            })
            .collect();

        let decision = decide(&RequestView {
            thread_id: thread_id(request),
            now_unix_secs: request.routing_now_unix_secs.to_native(),
            candidates: &views,
        });

        encode_response(decision)
    }
}

pub use reset_priority::filter;

/// Owned quota windows for one candidate.
///
/// The algorithm borrows its windows, so they must outlive the `CandidateView`
/// list; keeping them in a `Vec` alongside the views gives them a stable home
/// without a per-candidate allocation.
#[derive(Default)]
struct ExtractedQuotas {
    five_hour: Option<QuotaWindow>,
    shared_weekly: Option<QuotaWindow>,
    model_weekly: Option<QuotaWindow>,
}

impl ExtractedQuotas {
    #[must_use]
    const fn as_quotas(&self) -> CandidateQuotas<'_> {
        CandidateQuotas {
            five_hour: self.five_hour.as_ref(),
            shared_weekly: self.shared_weekly.as_ref(),
            model_weekly: self.model_weekly.as_ref(),
        }
    }
}

fn extract_quotas(
    candidate: &ArchivedUpstreamCandidate,
    model_window: Option<&str>,
) -> ExtractedQuotas {
    let mut extracted = ExtractedQuotas::default();
    for snapshot in candidate.subscription_quotas.iter() {
        let window: &str = &snapshot.window;
        let state: &str = &snapshot.state;
        let status: Option<&str> = snapshot.status.as_ref().map(|value| &**value);
        let quota = QuotaWindow {
            state: DataState::parse(state),
            utilization: snapshot.utilization.as_ref().map(|value| value.to_native()),
            resets_at_unix_secs: snapshot
                .resets_at_unix_secs
                .as_ref()
                .map(|value| value.to_native()),
            observed_at_unix_secs: snapshot
                .observed_at_unix_millis
                .as_ref()
                .map(|value| value.to_native() / 1_000),
            rejected: status == Some("rejected") || snapshot.disabled_reason.as_ref().is_some(),
        };
        let slot = if window == WINDOW_FIVE_HOUR {
            &mut extracted.five_hour
        } else if window == WINDOW_SEVEN_DAY {
            &mut extracted.shared_weekly
        } else if Some(window) == model_window {
            &mut extracted.model_weekly
        } else {
            continue;
        };
        // First snapshot wins: the host emits at most one per window label.
        if slot.is_none() {
            *slot = Some(quota);
        }
    }
    extracted
}

fn model_id(request: &ArchivedFilterRequest) -> &str {
    &request.canonical_model_id
}

fn thread_id(request: &ArchivedFilterRequest) -> Option<&str> {
    request.thread_id.as_ref().map(|value| &**value)
}

fn encode_response(decision: Decision) -> FilterResponse {
    let results = decision
        .per_candidate
        .into_iter()
        .map(|(upstream_id, verdict, reason)| PerCandidateReason {
            upstream_id: upstream_id.into_boxed_str(),
            decision: Box::from(verdict),
            reason: reason.into_boxed_str(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    FilterResponse { results }
}

// `KIND_ANTHROPIC_OAUTH` is re-exported so the host-side conformance fixtures
// and any host test can name the kind string without duplicating it.
pub use algorithm::KIND_ANTHROPIC_OAUTH as SUBSCRIPTION_KIND;
