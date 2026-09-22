use serde::{Deserialize, Serialize};

/// Stage where an internal error occurred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InternalErrorStage {
    /// Ingress stage: request-body transport and pre-parse failures.
    Ingress,
    /// Authentication stage.
    Authn,
    /// Routing stage.
    #[default]
    Router,
    /// Router filter stage.
    RouterFilter,
    /// Request shaping stage.
    Shape,
    /// Request signing stage.
    Signer,
    /// Request relay stage.
    Relay,
    /// Storage stage (upstream-affinity reads/binds, durable lookups).
    Storage,
}

/// Kind of internal error that occurred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InternalErrorKind {
    /// Plugin crashed or returned an error.
    #[default]
    PluginError,
    /// Plugin returned invalid output.
    InvalidOutput,
    /// Client-supplied input was invalid (malformed body, bad key, oversize).
    InvalidInput,
    /// Plugin trapped during execution.
    Trap,
    /// Configuration error.
    ConfigError,
    /// Timeout error.
    Timeout,
    /// Resource unavailable.
    Unavailable,
}

/// Internal error information with stage and kind details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InternalError {
    /// Stage where the error occurred.
    pub stage: InternalErrorStage,
    /// Kind of error.
    pub kind: InternalErrorKind,
    /// Optional error message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
