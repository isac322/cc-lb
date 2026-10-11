//! Per-principal prompt-cache policy.
//!
//! One enum carries the combined decision the admin UI exposes as a single
//! switch, so the wire contract cannot express a half-applied state (one-hour
//! TTLs with keep-alives still firing, or the reverse).
//!
//! - [`PromptCacheMode::PreserveClient`] honors the caller's own
//!   `cache_control` markers. The stored [`crate::CacheKeepaliveConfig`]
//!   operates normally.
//! - [`PromptCacheMode::OneHourNoKeepalive`] promotes supported existing
//!   ephemeral cache controls to `ttl: "1h"` and prohibits synthetic
//!   keep-alives. The stored keep-alive configuration stays persisted but
//!   dormant.
//!
//! Precedence lives in one place: `PrincipalSpecCached::cache_keepalive`
//! returns the effective configuration (absent in one-hour mode) and
//! `PrincipalSpecCached::prompt_cache_mode` returns the mode request
//! normalization needs. Callers must not re-derive the combination.

use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageResult};

/// Effective prompt-cache policy for a principal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheMode {
    /// Honor the caller's cache controls; keep-alive configuration applies.
    #[default]
    PreserveClient,
    /// Promote supported ephemeral cache controls to one hour and prohibit
    /// synthetic keep-alives.
    OneHourNoKeepalive,
}

impl PromptCacheMode {
    /// `prompt_cache_mode` column value for the default mode.
    pub const PRESERVE_CLIENT: &'static str = "preserve_client";
    /// `prompt_cache_mode` column value for one-hour mode.
    pub const ONE_HOUR_NO_KEEPALIVE: &'static str = "one_hour_no_keepalive";
    /// Every value the storage `CHECK` constraint accepts.
    pub const ALLOWED_DB_VALUES: [&'static str; 2] =
        [Self::PRESERVE_CLIENT, Self::ONE_HOUR_NO_KEEPALIVE];

    /// The stored representation.
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::PreserveClient => Self::PRESERVE_CLIENT,
            Self::OneHourNoKeepalive => Self::ONE_HOUR_NO_KEEPALIVE,
        }
    }

    /// Parse the stored representation, rejecting anything the storage
    /// `CHECK` constraint would have refused to insert.
    pub fn from_db_value(value: &str) -> StorageResult<Self> {
        match value {
            Self::PRESERVE_CLIENT => Ok(Self::PreserveClient),
            Self::ONE_HOUR_NO_KEEPALIVE => Ok(Self::OneHourNoKeepalive),
            value => Err(StorageError::Corrupted {
                message: format!("invalid principal prompt_cache_mode {value}"),
            }),
        }
    }

    /// True when synthetic keep-alives are prohibited for this principal,
    /// leaving any stored keep-alive configuration dormant.
    pub const fn prohibits_keepalive(self) -> bool {
        matches!(self, Self::OneHourNoKeepalive)
    }

    /// True when recognized ephemeral cache controls must be promoted to
    /// `ttl: "1h"`.
    pub const fn promotes_cache_control_ttl_to_one_hour(self) -> bool {
        matches!(self, Self::OneHourNoKeepalive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_preserve_client() {
        assert_eq!(PromptCacheMode::default(), PromptCacheMode::PreserveClient);
        assert!(!PromptCacheMode::default().prohibits_keepalive());
        assert!(!PromptCacheMode::default().promotes_cache_control_ttl_to_one_hour());
    }

    #[test]
    fn wire_values_round_trip_via_serde() {
        for (mode, wire) in [
            (PromptCacheMode::PreserveClient, "preserve_client"),
            (PromptCacheMode::OneHourNoKeepalive, "one_hour_no_keepalive"),
        ] {
            assert_eq!(mode.as_db_value(), wire);
            assert_eq!(PromptCacheMode::from_db_value(wire).unwrap(), mode);
            let json = serde_json::to_string(&mode).unwrap();
            assert_eq!(json, format!("\"{wire}\""));
            assert_eq!(
                serde_json::from_str::<PromptCacheMode>(&json).unwrap(),
                mode
            );
        }
    }

    #[test]
    fn deserialization_rejects_unknown_and_null_values() {
        assert!(serde_json::from_str::<PromptCacheMode>("\"one_hour\"").is_err());
        assert!(serde_json::from_str::<PromptCacheMode>("null").is_err());
        assert!(PromptCacheMode::from_db_value("preserve").is_err());
    }

    #[test]
    fn one_hour_mode_prohibits_keepalive_and_promotes_ttl() {
        let mode = PromptCacheMode::OneHourNoKeepalive;
        assert!(mode.prohibits_keepalive());
        assert!(mode.promotes_cache_control_ttl_to_one_hour());
    }

    #[test]
    fn allowed_db_values_cover_every_variant() {
        for mode in [
            PromptCacheMode::PreserveClient,
            PromptCacheMode::OneHourNoKeepalive,
        ] {
            assert!(PromptCacheMode::ALLOWED_DB_VALUES.contains(&mode.as_db_value()));
        }
    }
}
