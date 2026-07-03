//! Plugin metadata JSON schema — parsed from wasm's `cc_lb.plugin.v1`
//! custom section at admission time. This is the host-side parser;
//! the guest-side emitter is in `cc-lb-pdk-wasmtime-macros`.

use std::collections::BTreeMap;
use std::string::String;

use serde::{Deserialize, Serialize};

/// Top-level plugin metadata. Written by the `#[cc_lb_plugin(...)]`
/// proc-macro into the wasm's `cc_lb.plugin.v1` custom section as
/// canonical JSON (sorted keys, no extra whitespace).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub description: String,
    pub usage: String,
    #[serde(default)]
    pub hooks: BTreeMap<String, HookMetadata>,
}

/// Per-hook metadata declared by the plugin author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookMetadata {
    pub wire_version: u8,
    pub description: String,
    pub usage: String,
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
    /// Parse `cc_lb.plugin.v1` custom section bytes as JSON and
    /// validate required fields.
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
            if crate::schema::HookKind::parse(name).is_none() {
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
