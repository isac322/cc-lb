pub mod observability;
pub mod router;
pub mod shape;

use std::collections::BTreeMap;

use cc_lb_plugin_wire::wire_function::WireFunction;

use crate::dispatch::{DispatchOutcome, PluginSession};
use crate::errors::DispatchError;
use crate::handshake::{HandshakeError, HandshakeReport};
use crate::{ExtraInfo, LayerResult, VerifyError, VerifyReport, identity, self_check};

pub use observability::{verify_observability_plugin, verify_observability_plugin_with_caps};
pub use router::{verify_router_plugin, verify_router_plugin_with_caps};
pub use shape::{verify_shape_plugin, verify_shape_plugin_with_caps};

pub(crate) fn begin_report(
    wasm: &[u8],
    handshake: HandshakeReport,
    required_function: &'static str,
) -> Result<VerifyReport, VerifyError> {
    let identity = identity::read(wasm)?;
    self_check::run(wasm)?;

    Ok(VerifyReport {
        identity: LayerResult {
            layer: "identity",
            passed: identity.static_checks.iter().all(static_check_passed),
            detail: Some(identity.identity.plugin_name),
        },
        handshake: LayerResult {
            layer: "handshake",
            passed: true,
            detail: Some(format!(
                "{} function(s) negotiated",
                handshake.chosen_versions.len()
            )),
        },
        self_check: LayerResult {
            layer: "self_check",
            passed: true,
            detail: None,
        },
        dispatch: Vec::new(),
        extras: extra_functions(&handshake.chosen_versions, required_function),
    })
}

pub(crate) fn require_function<F: WireFunction>(
    handshake: &HandshakeReport,
) -> Result<(), VerifyError> {
    let Some(chosen) = handshake.chosen_versions.get(F::NAME) else {
        return Err(HandshakeError::FunctionMissing {
            name: F::NAME.to_owned(),
        }
        .into());
    };
    if F::SUPPORTED_VERSIONS.contains(chosen) {
        Ok(())
    } else {
        Err(HandshakeError::InvalidIdentity {
            field: "chosen_versions",
            reason: format!(
                "function {} chose unsupported version {}; host supports {:?}",
                F::NAME,
                chosen,
                F::SUPPORTED_VERSIONS
            ),
        }
        .into())
    }
}

pub(crate) fn push_dispatch_ok(
    report: &mut VerifyReport,
    layer: &'static str,
    detail: Option<String>,
) {
    report.dispatch.push(LayerResult {
        layer,
        passed: true,
        detail,
    });
}

pub(crate) fn dispatch_ok<F: WireFunction>(
    session: &mut PluginSession,
    request: F::Request,
) -> Result<F::Response, VerifyError> {
    match session.dispatch::<F>(request) {
        DispatchOutcome::Ok(response) => Ok(response),
        DispatchOutcome::Fallback(policy) => Err(DispatchError::Fallback {
            function: F::NAME,
            policy,
        }
        .into()),
    }
}

fn extra_functions(
    chosen_versions: &BTreeMap<String, u32>,
    required_function: &'static str,
) -> Vec<ExtraInfo> {
    chosen_versions
        .keys()
        .filter(|function| function.as_str() != required_function)
        .map(|function| ExtraInfo {
            kind: "extra_function",
            message: format!("plugin also declares '{function}'"),
        })
        .collect()
}

fn static_check_passed(check: &identity::StaticCheck) -> bool {
    match check {
        identity::StaticCheck::CustomSectionExactlyOnce { pass, .. }
        | identity::StaticCheck::CustomSectionSizeWithinLimit { pass, .. }
        | identity::StaticCheck::JsonFieldsExactlyFour { pass, .. }
        | identity::StaticCheck::IdentityFieldsWellFormed { pass, .. }
        | identity::StaticCheck::PluginNameRegexCompliant { pass, .. }
        | identity::StaticCheck::PluginVersionLengthCompliant { pass, .. }
        | identity::StaticCheck::AllRequiredExportsPresent { pass, .. }
        | identity::StaticCheck::ExtismCanInstantiate { pass, .. }
        | identity::StaticCheck::NoWasiImports { pass, .. } => *pass,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn extra_function_info_is_emitted_for_declared_non_required_functions() {
        let chosen_versions = BTreeMap::from([
            ("shape".to_owned(), 1),
            ("observe".to_owned(), 1),
            ("normalize_error".to_owned(), 1),
        ]);

        let extras = extra_functions(&chosen_versions, "shape");

        assert_eq!(extras.len(), 2);
        assert!(extras.iter().any(|extra| {
            extra.kind == "extra_function" && extra.message == "plugin also declares 'observe'"
        }));
        assert!(extras.iter().any(|extra| {
            extra.kind == "extra_function"
                && extra.message == "plugin also declares 'normalize_error'"
        }));
    }
}
