use serde::{Deserialize, Serialize};

/// Runtime chain slot a plugin can provide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginSlotKind {
    /// Router filter slot for selecting or filtering upstream candidates.
    Router,
    /// Observability hook slot for receiving request lifecycle events.
    ObservabilityHook,
    /// Request/response shaping slot for upstream-specific requests and response hooks.
    Shape,
}

impl PluginSlotKind {
    /// Returns the stable snake_case wire name for this slot.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Router => "router",
            Self::ObservabilityHook => "observability_hook",
            Self::Shape => "shape",
        }
    }

    /// Parses a stable snake_case wire name into a plugin slot.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "router" => Some(Self::Router),
            "observability_hook" => Some(Self::ObservabilityHook),
            "shape" => Some(Self::Shape),
            _ => None,
        }
    }
}
