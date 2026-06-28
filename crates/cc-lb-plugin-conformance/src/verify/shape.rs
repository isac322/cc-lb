use std::collections::BTreeSet;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cc_lb_plugin_wire::v2::shape::ShapeFn;
use cc_lb_plugin_wire::wire_function::WireFunction;

use crate::dispatch::PluginSession;
use crate::fixtures;
use crate::handshake;
use crate::{VerifyError, VerifyReport};

use super::{begin_report, dispatch_ok, push_dispatch_ok, require_function};

pub fn verify_shape_plugin(
    wasm: &[u8],
    clock: &dyn cc_lb_clock::Clock,
) -> Result<VerifyReport, VerifyError> {
    verify_shape_plugin_with_caps(wasm, &BTreeSet::new(), clock)
}

pub fn verify_shape_plugin_with_caps(
    wasm: &[u8],
    host_capabilities: &BTreeSet<String>,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<VerifyReport, VerifyError> {
    let handshake = handshake::run_with_caps(wasm, host_capabilities)?;
    require_function::<ShapeFn>(&handshake)?;

    let mut report = begin_report(wasm, handshake, ShapeFn::NAME, clock)?;
    let mut session = PluginSession::new_with_caps(wasm, host_capabilities)?;

    run_shape_case(&mut report, &mut session, "basic", "e30=")?;
    run_shape_case(&mut report, &mut session, "empty_body", "")?;
    run_shape_case(&mut report, &mut session, "malformed_json", base64("{"))?;
    run_shape_case(
        &mut report,
        &mut session,
        "invalid_utf8",
        STANDARD.encode([0xff, 0xfe, 0xfd]),
    )?;
    run_shape_case(
        &mut report,
        &mut session,
        "large_body",
        base64(&"x".repeat(64 * 1024)),
    )?;
    run_shape_case(
        &mut report,
        &mut session,
        "unicode_preservation",
        base64("한글🙂"),
    )?;

    Ok(report)
}

fn run_shape_case(
    report: &mut VerifyReport,
    session: &mut PluginSession,
    name: &'static str,
    body_base64: impl Into<String>,
) -> Result<(), VerifyError> {
    let request = fixtures::shape_request_builder()
        .body_base64(body_base64)
        .build();
    dispatch_ok::<ShapeFn>(session, request)?;
    push_dispatch_ok(
        report,
        robustness_layer(name),
        Some("shape dispatch returned a decodable response".to_owned()),
    );
    Ok(())
}

fn base64(input: &str) -> String {
    STANDARD.encode(input.as_bytes())
}

fn robustness_layer(name: &'static str) -> &'static str {
    match name {
        "basic" => "dispatch:shape:basic",
        "empty_body" => "dispatch:robustness:empty_body",
        "malformed_json" => "dispatch:robustness:malformed_json",
        "invalid_utf8" => "dispatch:robustness:invalid_utf8",
        "large_body" => "dispatch:robustness:large_body",
        "unicode_preservation" => "dispatch:robustness:unicode_preservation",
        _ => "dispatch:robustness:unknown",
    }
}
