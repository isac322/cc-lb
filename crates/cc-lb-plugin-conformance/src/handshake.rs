use std::collections::{BTreeMap, BTreeSet};

use cc_lb_plugin_wire::handshake::HandshakeError as WireHandshakeError;
use cc_lb_plugin_wire::identity::IdentityError as WireIdentityError;
use cc_lb_runtime_protocol::handshake::{HandshakeExecutionError, build_offer, execute_handshake};
use cc_lb_runtime_protocol::identity::{IdentityReadError, read_identity};
use thiserror::Error;

pub fn run(wasm: &[u8]) -> Result<HandshakeReport, HandshakeError> {
    run_with_caps(wasm, &BTreeSet::new())
}

pub fn run_with_caps(
    wasm: &[u8],
    host_capabilities: &BTreeSet<String>,
) -> Result<HandshakeReport, HandshakeError> {
    let offer = build_offer(host_capabilities);
    let accept = execute_handshake(wasm, &offer)?;
    let identity = read_identity(wasm)?;

    Ok(HandshakeReport {
        chosen_versions: accept.chosen_versions,
        envelope_version: accept.envelope_version,
        identity: PluginIdentity {
            name: identity.plugin_name,
            version: identity.plugin_version,
            abi_envelope: identity.abi_envelope,
        },
        negotiated_capabilities: accept.required_capabilities,
    })
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeReport {
    pub chosen_versions: BTreeMap<String, u32>,
    pub envelope_version: u32,
    pub identity: PluginIdentity,
    pub negotiated_capabilities: BTreeSet<String>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginIdentity {
    pub name: String,
    pub version: String,
    pub abi_envelope: u32,
}

#[non_exhaustive]
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum HandshakeError {
    #[error("plugin instantiation failed: {reason}")]
    Instantiate { reason: String },
    #[error("plugin does not export cc_lb_handshake")]
    MissingExport,
    #[error("invalid identity field {field}: {reason}")]
    InvalidIdentity { field: &'static str, reason: String },
    #[error("declared function missing wasm export: {name}")]
    FunctionMissing { name: String },
    #[error(
        "downgrade attempt for function {function}: host offered max {host_offered_max}, plugin chose {plugin_chose}"
    )]
    DowngradeAttempt {
        function: String,
        host_offered_max: u32,
        plugin_chose: u32,
    },
    #[error("missing host capability: {name}")]
    MissingCapability { name: String },
    #[error("handshake serialization failed: {reason}")]
    Serialization { reason: String },
    #[error("wasm execution failed: {reason}")]
    WasmTrap { reason: String },
}

impl From<HandshakeExecutionError> for HandshakeError {
    fn from(error: HandshakeExecutionError) -> Self {
        let reason = error.to_string();
        match error {
            HandshakeExecutionError::Validation(error) => map_wire_handshake_error(error, reason),
            HandshakeExecutionError::Instantiate { reason } => {
                HandshakeError::Instantiate { reason }
            }
            HandshakeExecutionError::MissingHandshakeExport => HandshakeError::MissingExport,
            HandshakeExecutionError::SerializeOffer { reason }
            | HandshakeExecutionError::DecodeAccept { reason } => {
                HandshakeError::Serialization { reason }
            }
            HandshakeExecutionError::Call { reason } => HandshakeError::WasmTrap { reason },
            HandshakeExecutionError::Timeout => HandshakeError::WasmTrap { reason },
            HandshakeExecutionError::OutputTooLarge { .. } => {
                HandshakeError::Serialization { reason }
            }
            HandshakeExecutionError::ImplementedFunctionCountExceeded { .. } => {
                HandshakeError::InvalidIdentity {
                    field: "implemented_functions",
                    reason,
                }
            }
            HandshakeExecutionError::ImplementedUnknownFunction { .. } => {
                HandshakeError::InvalidIdentity {
                    field: "implemented_functions",
                    reason,
                }
            }
            HandshakeExecutionError::SupportedUnknownFunction { .. }
            | HandshakeExecutionError::SupportedVersionOutOfRange { .. } => {
                HandshakeError::InvalidIdentity {
                    field: "plugin_supported",
                    reason,
                }
            }
            HandshakeExecutionError::ChosenFunctionNotImplemented { function } => {
                HandshakeError::FunctionMissing { name: function }
            }
            HandshakeExecutionError::ChosenVersionNotSupported { .. } => {
                HandshakeError::InvalidIdentity {
                    field: "chosen_versions",
                    reason,
                }
            }
            HandshakeExecutionError::DeclaredFunctionMissing { function } => {
                HandshakeError::FunctionMissing { name: function }
            }
            HandshakeExecutionError::UndeclaredExport { .. } => HandshakeError::InvalidIdentity {
                field: "implemented_functions",
                reason,
            },
        }
    }
}

impl From<IdentityReadError> for HandshakeError {
    fn from(error: IdentityReadError) -> Self {
        let field = match &error {
            IdentityReadError::WasmParseError(_) => "wasm",
            IdentityReadError::MissingCustomSection
            | IdentityReadError::DuplicateCustomSection
            | IdentityReadError::SectionTooLarge { .. } => "custom_section",
            IdentityReadError::MagicMismatch { .. } => "magic",
            IdentityReadError::MalformedPayload(_) => "payload",
            IdentityReadError::Validation(WireIdentityError::MagicMismatch) => "magic",
            IdentityReadError::Validation(
                WireIdentityError::PluginNameEmpty | WireIdentityError::PluginNameInvalid,
            ) => "name",
            IdentityReadError::Validation(
                WireIdentityError::PluginVersionEmpty | WireIdentityError::PluginVersionTooLong,
            ) => "version",
        };

        HandshakeError::InvalidIdentity {
            field,
            reason: error.to_string(),
        }
    }
}

fn map_wire_handshake_error(error: WireHandshakeError, reason: String) -> HandshakeError {
    match error {
        WireHandshakeError::FunctionCountExceeded { .. }
        | WireHandshakeError::FunctionVersionCountExceeded { .. }
        | WireHandshakeError::VersionOutOfRange { .. } => HandshakeError::InvalidIdentity {
            field: "function_versions",
            reason,
        },
        WireHandshakeError::CapabilityCountExceeded { .. } => HandshakeError::InvalidIdentity {
            field: "host_capabilities",
            reason,
        },
        WireHandshakeError::ChosenVersionNotOffered { .. }
        | WireHandshakeError::ChosenForUnknownFunction { .. } => HandshakeError::InvalidIdentity {
            field: "chosen_versions",
            reason,
        },
        WireHandshakeError::DowngradeAttempt {
            function,
            chosen,
            max_intersection,
        } => HandshakeError::DowngradeAttempt {
            function,
            host_offered_max: max_intersection,
            plugin_chose: chosen,
        },
        WireHandshakeError::RequiredCapabilityUnavailable { capability } => {
            HandshakeError::MissingCapability { name: capability }
        }
        WireHandshakeError::HandshakeSchemaVersionMismatch { .. } => {
            HandshakeError::InvalidIdentity {
                field: "handshake_schema_version",
                reason,
            }
        }
        WireHandshakeError::Canonical(_) => HandshakeError::Serialization { reason },
    }
}
