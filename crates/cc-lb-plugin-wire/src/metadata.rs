//! Plugin metadata JSON schema, shared by `cc_lb.plugin.v1` (legacy) and
//! `cc_lb.plugin.v2` (provider-qualified) custom sections.
//! The guest-side emitter is in `cc-lb-pdk-wasmtime-macros`.

use std::collections::BTreeMap;
use std::string::String;

use serde::{Deserialize, Serialize};

/// Top-level plugin metadata. Written by the `#[cc_lb_plugin(...)]`
/// proc-macro into one metadata custom section. V1 hook keys are bare
/// (e.g. `shape`); V2 also accepts qualified keys (e.g. `shape_openai`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub description: String,
    pub usage: String,
    #[serde(default)]
    pub hooks: BTreeMap<String, HookMetadata>,
}

/// Whether a declared hook should be executed by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookMode {
    /// Host calls the wasm export for this hook.
    Active,
    /// Host treats this hook as an explicit no-op and skips the wasm call.
    Noop,
}

impl HookMode {
    pub const fn is_noop(self) -> bool {
        matches!(self, Self::Noop)
    }
}

/// Per-hook metadata declared by the plugin author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookMetadata {
    pub wire_version: u8,
    pub description: String,
    pub usage: String,
    pub mode: HookMode,
}

/// Errors returned by [`PluginMetadata::parse`].
#[derive(Debug)]
pub enum MetadataError {
    Json(serde_json::Error),
    MissingName,
    MissingVersion,
    MissingDescription,
    MissingUsage,
    UnknownHook(String),
    HookMissingDescription(String),
    HookMissingUsage(String),
    NoHooksDeclared,
}

impl core::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "invalid JSON: {e}"),
            Self::MissingName => write!(f, "missing plugin name"),
            Self::MissingVersion => write!(f, "missing plugin version"),
            Self::MissingDescription => write!(f, "missing plugin description"),
            Self::MissingUsage => write!(f, "missing plugin usage"),
            Self::UnknownHook(h) => write!(f, "unknown hook name: {h}"),
            Self::HookMissingDescription(h) => write!(f, "hook '{h}' missing description"),
            Self::HookMissingUsage(h) => write!(f, "hook '{h}' missing usage"),
            Self::NoHooksDeclared => write!(f, "plugin declares no hooks"),
        }
    }
}

impl std::error::Error for MetadataError {}

impl From<serde_json::Error> for MetadataError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

impl PluginMetadata {
    /// Parse V1 or V2 metadata custom section bytes as JSON and validate
    /// required fields. The host validates the section's version separately.
    pub fn parse(bytes: &[u8]) -> Result<Self, MetadataError> {
        let raw: Self = serde_json::from_slice(bytes)?;
        raw.validate()?;
        Ok(raw)
    }

    /// Validate the metadata satisfies the host contract.
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.name.trim().is_empty() {
            return Err(MetadataError::MissingName);
        }
        if self.version.trim().is_empty() {
            return Err(MetadataError::MissingVersion);
        }
        if self.description.trim().is_empty() {
            return Err(MetadataError::MissingDescription);
        }
        if self.usage.trim().is_empty() {
            return Err(MetadataError::MissingUsage);
        }
        if self.hooks.is_empty() {
            return Err(MetadataError::NoHooksDeclared);
        }

        for (name, hook) in &self.hooks {
            if crate::schema::HookKind::parse_entrypoint(name).is_none() {
                return Err(MetadataError::UnknownHook(name.clone()));
            }
            if hook.description.trim().is_empty() {
                return Err(MetadataError::HookMissingDescription(name.clone()));
            }
            if hook.usage.trim().is_empty() {
                return Err(MetadataError::HookMissingUsage(name.clone()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_mode_parses_explicit_noop() {
        let metadata = PluginMetadata::parse(
            br#"{"name":"x","version":"0.0.1","description":"test plugin","usage":"test usage","hooks":{"transform_response":{"wire_version":1,"description":"response hook","usage":"call transform_response","mode":"noop"}}}"#,
        )
        .expect("metadata parses");

        assert_eq!(metadata.hooks["transform_response"].mode, HookMode::Noop);
    }
}
