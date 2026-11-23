use serde::{Deserialize, Serialize};

/// Maximum length of an error message in internal errors.
pub const MAX_ERROR_MESSAGE_LEN: usize = 1024;

/// Strategy for selecting a terminal upstream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TerminalStrategy {
    /// Select first available upstream.
    #[default]
    FirstPick,
    /// Select a router plugin at random.
    Random,
}
