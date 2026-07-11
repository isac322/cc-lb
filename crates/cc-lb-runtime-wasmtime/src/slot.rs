//! Runtime-owned identity keys for loaded plugin slots.

use cc_lb_domain::GLOBAL_PRINCIPAL;
use serde::{Deserialize, Serialize};

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
