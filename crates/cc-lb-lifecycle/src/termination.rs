use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Stage payloads: RequestTerminated
// ---------------------------------------------------------------------------

/// Why the request terminated.
///
/// String `error_code` values come from the engine's
/// `terminal_observer::error_codes` catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminationReason {
    /// Successful 2xx response.
    Success,
    /// Request was rejected or upstream returned an error. The string
    /// matches an entry in the `error_codes` catalog.
    ErrorCode(String),
    /// `LifecycleContext` was dropped without an explicit termination
    /// signal (usually an in-flight cancellation).
    Dropped,
}

impl TerminationReason {
    /// Static-label form for metrics without payload cardinality.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ErrorCode(_) => "error_code",
            Self::Dropped => "dropped",
        }
    }
}
