use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Plugin manifest passed to runtime adapters when instantiating plugins.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PluginManifest {
    /// Plugin name from configuration.
    pub(crate) name: String,
    /// Filesystem path or runtime-specific locator for the plugin artifact.
    pub(crate) artifact: String,
    /// Preferred plugin wire envelope version. Omitted manifests default to v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) wire_version: Option<u8>,
    /// Runtime configuration provided to the plugin.
    pub(crate) config: serde_json::Value,
    /// Runtime-specific metadata not interpreted by the core API contract.
    #[serde(default)]
    pub(crate) metadata: BTreeMap<String, serde_json::Value>,
    /// Pure-mode dispatch: every hook call builds a fresh wasm `Store`
    /// (no thread_local cache, no version-compare). Default `true` —
    /// stateless plugins (the common case) benefit from full isolation
    /// per call. Opt out only for plugins that genuinely need to keep
    /// mutable state across calls in the same worker.
    #[serde(default = "default_pure")]
    pub(crate) pure: bool,
}

/// `serde` default for [`PluginManifest::pure`]. Omitted manifests are
/// treated as pure to match the cc-lb-server-side default expectation.
const fn default_pure() -> bool {
    true
}
