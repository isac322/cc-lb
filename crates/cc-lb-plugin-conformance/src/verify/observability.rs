use std::collections::BTreeSet;

use cc_lb_plugin_wire::v2::common::ObserveEventWire;
use cc_lb_plugin_wire::v2::observe::ObserveFn;
use cc_lb_plugin_wire::wire_function::WireFunction;

use crate::dispatch::PluginSession;
use crate::fixtures;
use crate::handshake;
use crate::{VerifyError, VerifyReport};

use super::{begin_report, dispatch_ok, push_dispatch_ok, require_function};

pub fn verify_observability_plugin(wasm: &[u8]) -> Result<VerifyReport, VerifyError> {
    verify_observability_plugin_with_caps(wasm, &BTreeSet::new())
}

pub fn verify_observability_plugin_with_caps(
    wasm: &[u8],
    host_capabilities: &BTreeSet<String>,
) -> Result<VerifyReport, VerifyError> {
    let handshake = handshake::run_with_caps(wasm, host_capabilities)?;
    require_function::<ObserveFn>(&handshake)?;

    let mut report = begin_report(wasm, handshake, ObserveFn::NAME)?;
    let mut session = PluginSession::new_with_caps(wasm, host_capabilities)?;

    dispatch_ok::<ObserveFn>(&mut session, fixtures::observe_request_builder().build())?;
    push_dispatch_ok(
        &mut report,
        "dispatch:robustness:empty_events",
        Some("empty event batch accepted".to_owned()),
    );

    let multi_event_request = fixtures::observe_request_builder()
        .add_event(ObserveEventWire::dry_run_sample())
        .add_event(ObserveEventWire::AuthnComplete {
            principal_id: "dry-run-principal".to_owned(),
            principal_kind: "api_key".to_owned(),
        })
        .build();
    dispatch_ok::<ObserveFn>(&mut session, multi_event_request)?;
    push_dispatch_ok(
        &mut report,
        "dispatch:robustness:multi_event_batch",
        Some("multi-event batch accepted".to_owned()),
    );

    Ok(report)
}
