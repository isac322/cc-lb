//! Runtime-owned identity keys for loaded plugin slots.

use serde::{Deserialize, Serialize};

/// Sentinel principal id used for proxy-wide global plugin slots.
///
/// Kept as a runtime-local copy that must stay byte-identical to
/// `cc_lb_domain::GLOBAL_PRINCIPAL`, so this published crate does not take a
/// normal dependency on the unpublished `cc-lb-domain` crate.
pub const GLOBAL_PRINCIPAL: &str = "__global__";

/// Independently registered runtime roles. All transform hooks belong to Shape.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum RuntimeSlotRole {
    Filter,
    Shape,
}

/// Composite key identifying a loaded slot per principal × plugin name × role.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSlotKey {
    /// Principal id this slot is bound to, or [`GLOBAL_PRINCIPAL`] for
    /// proxy-wide globals.
    pub principal: String,
    /// Stable plugin name.
    pub plugin: String,
    /// Registration role, independent of the module's exported ABI hooks.
    pub role: RuntimeSlotRole,
}

impl RuntimeSlotKey {
    /// Build a per-principal runtime slot key.
    pub fn new(
        principal: impl Into<String>,
        plugin: impl Into<String>,
        role: RuntimeSlotRole,
    ) -> Self {
        Self {
            principal: principal.into(),
            plugin: plugin.into(),
            role,
        }
    }

    /// Build a proxy-wide global runtime slot key.
    pub fn global(plugin: impl Into<String>, role: RuntimeSlotRole) -> Self {
        Self::new(GLOBAL_PRINCIPAL, plugin, role)
    }
}
