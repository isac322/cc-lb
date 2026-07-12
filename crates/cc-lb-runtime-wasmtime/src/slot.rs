//! Runtime-owned identity keys for loaded plugin slots.

use serde::{Deserialize, Serialize};

/// Sentinel principal id used for proxy-wide global plugin slots.
///
/// Kept as a runtime-local copy that must stay byte-identical to
/// `cc_lb_domain::GLOBAL_PRINCIPAL`, so this published crate does not take a
/// normal dependency on the unpublished `cc-lb-domain` crate.
pub const GLOBAL_PRINCIPAL: &str = "__global__";

/// Composite key identifying a loaded plugin slot per principal × plugin name.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSlotKey {
    /// Principal id this slot is bound to, or [`GLOBAL_PRINCIPAL`] for
    /// proxy-wide globals.
    pub principal: String,
    /// Stable plugin name.
    pub plugin: String,
}

impl RuntimeSlotKey {
    /// Build a per-principal runtime slot key.
    pub fn new(principal: impl Into<String>, plugin: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            plugin: plugin.into(),
        }
    }

    /// Build a proxy-wide global runtime slot key.
    pub fn global(plugin: impl Into<String>) -> Self {
        Self::new(GLOBAL_PRINCIPAL, plugin)
    }
}
