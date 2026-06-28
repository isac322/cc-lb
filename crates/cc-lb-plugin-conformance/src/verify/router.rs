use std::collections::BTreeSet;

use cc_lb_plugin_wire::v2::common::CandidateWire;
use cc_lb_plugin_wire::v3::filter::{FilterFn, FilterRequest};
use cc_lb_plugin_wire::wire_function::WireFunction;

use crate::dispatch::PluginSession;
use crate::errors::DispatchError;
use crate::fixtures;
use crate::handshake;
use crate::{VerifyError, VerifyReport};

use super::{begin_report, dispatch_ok, push_dispatch_ok, require_function};

pub fn verify_router_plugin(
    wasm: &[u8],
    clock: &dyn cc_lb_clock::Clock,
) -> Result<VerifyReport, VerifyError> {
    verify_router_plugin_with_caps(wasm, &BTreeSet::new(), clock)
}

pub fn verify_router_plugin_with_caps(
    wasm: &[u8],
    host_capabilities: &BTreeSet<String>,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<VerifyReport, VerifyError> {
    let handshake = handshake::run_with_caps(wasm, host_capabilities)?;
    require_function::<FilterFn>(&handshake)?;

    let mut report = begin_report(wasm, handshake, FilterFn::NAME, clock)?;
    let mut session = PluginSession::new_with_caps(wasm, host_capabilities)?;

    let empty_response = dispatch_ok::<FilterFn>(&mut session, FilterRequest::dry_run_sample())?;
    validate_decision_count(0, empty_response.results.len())?;
    push_dispatch_ok(
        &mut report,
        "dispatch:filter:empty_candidates",
        Some("0 candidates -> 0 decisions".to_owned()),
    );

    let candidates = vec![CandidateWire::dry_run_sample()];
    let request = fixtures::filter_request_builder()
        .candidates(candidates.clone())
        .build();
    let response = dispatch_ok::<FilterFn>(&mut session, request)?;
    validate_decision_count(candidates.len(), response.results.len())?;
    push_dispatch_ok(
        &mut report,
        "dispatch:filter:candidate_count",
        Some(format!(
            "{} candidate(s) -> matching decisions",
            candidates.len()
        )),
    );

    Ok(report)
}

fn validate_decision_count(expected: usize, actual: usize) -> Result<(), VerifyError> {
    if expected == actual {
        Ok(())
    } else {
        Err(DispatchError::InvalidResponse {
            function: "filter",
            reason: format!("expected {expected} decision(s), got {actual}"),
        }
        .into())
    }
}
