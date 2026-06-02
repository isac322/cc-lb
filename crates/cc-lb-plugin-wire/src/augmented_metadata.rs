extern crate alloc;

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::identity::PluginIdentity;

/// Augmented metadata combining plugin identity with negotiated capabilities.
///
/// This is the single source of truth for negotiated plugin capabilities and versions.
/// It encapsulates the result of a successful handshake and self-check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AugmentedMetadata {
    /// The plugin's identity metadata (magic, abi_envelope, name, version).
    pub identity: PluginIdentity,

    /// Negotiated wire function versions: function_name -> negotiated_version.
    /// Ordered via BTreeMap for deterministic serialization.
    pub negotiated_functions: BTreeMap<String, u32>,

    /// Negotiated capabilities offered by the plugin.
    /// Ordered via BTreeSet for deterministic serialization.
    pub negotiated_capabilities: BTreeSet<String>,

    /// Unix timestamp (seconds) when handshake was completed.
    pub handshake_completed_at: i64,

    /// Whether the self-check passed successfully.
    pub self_check_passed: bool,

    /// Unix timestamp (seconds) when self-check was completed.
    pub self_check_completed_at: i64,

    /// Unix timestamp (seconds) when this metadata expires and re-handshake is needed.
    pub expires_at: i64,
}

/// Record in the plugin registry, tracking a deployed plugin instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginRegistryRecord {
    /// Unique plugin instance identifier (UUID or similar).
    pub plugin_id: String,

    /// Augmented metadata for this plugin instance.
    pub augmented_metadata: AugmentedMetadata,

    /// Unix timestamp (seconds) when the plugin was registered.
    pub registered_at: i64,
}

/// Error type for augmented metadata operations.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AugmentedMetadataError {
    #[error("identity validation failed: {0}")]
    IdentityValidationFailed(String),

    #[error(
        "augmented metadata serialization exceeds max size of {} bytes",
        crate::limits::AUGMENTED_METADATA_MAX_BYTES
    )]
    SizeLimitExceeded,

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("expires_at must be after handshake_completed_at")]
    ExpirationBeforeHandshake,

    #[error("no negotiated functions")]
    NoNegotiatedFunctions,

    #[error("serialization failed: {0}")]
    SerializationFailed(String),
}

impl AugmentedMetadata {
    /// Validate this augmented metadata.
    pub fn validate(&self) -> Result<(), AugmentedMetadataError> {
        // Validate identity.
        self.identity
            .validate()
            .map_err(|e| AugmentedMetadataError::IdentityValidationFailed(e.to_string()))?;

        // Validate timestamps.
        if self.handshake_completed_at <= 0 {
            return Err(AugmentedMetadataError::InvalidTimestamp(
                "handshake_completed_at must be positive".to_string(),
            ));
        }

        if self.self_check_completed_at <= 0 {
            return Err(AugmentedMetadataError::InvalidTimestamp(
                "self_check_completed_at must be positive".to_string(),
            ));
        }

        if self.expires_at <= 0 {
            return Err(AugmentedMetadataError::InvalidTimestamp(
                "expires_at must be positive".to_string(),
            ));
        }

        if self.expires_at <= self.handshake_completed_at {
            return Err(AugmentedMetadataError::ExpirationBeforeHandshake);
        }

        // Validate negotiated functions.
        if self.negotiated_functions.is_empty() {
            return Err(AugmentedMetadataError::NoNegotiatedFunctions);
        }

        // Validate size limit.
        self.check_size_limit()?;

        Ok(())
    }

    /// Check that serialized size does not exceed AUGMENTED_METADATA_MAX_BYTES.
    pub fn check_size_limit(&self) -> Result<(), AugmentedMetadataError> {
        let serialized = serde_json::to_vec(self)
            .map_err(|e| AugmentedMetadataError::SerializationFailed(e.to_string()))?;

        if serialized.len() > crate::limits::AUGMENTED_METADATA_MAX_BYTES {
            return Err(AugmentedMetadataError::SizeLimitExceeded);
        }

        Ok(())
    }

    /// Create AugmentedMetadata from handshake and self-check responses.
    ///
    /// Combines:
    /// - Plugin identity from handshake
    /// - Negotiated functions and capabilities from handshake
    /// - Self-check result
    ///
    /// # Arguments
    /// * `identity` - The plugin's identity metadata
    /// * `negotiated_functions` - BTreeMap of function_name -> negotiated_version
    /// * `negotiated_capabilities` - BTreeSet of negotiated capability names
    /// * `handshake_completed_at` - Timestamp when handshake completed
    /// * `self_check_passed` - Whether self-check passed
    /// * `self_check_completed_at` - Timestamp when self-check completed
    /// * `ttl_seconds` - TTL for this metadata in seconds
    pub fn from_handshake_and_self_check(
        identity: PluginIdentity,
        negotiated_functions: BTreeMap<String, u32>,
        negotiated_capabilities: BTreeSet<String>,
        handshake_completed_at: i64,
        self_check_passed: bool,
        self_check_completed_at: i64,
        ttl_seconds: u64,
    ) -> Result<Self, AugmentedMetadataError> {
        let expires_at = handshake_completed_at + ttl_seconds as i64;

        let metadata = AugmentedMetadata {
            identity,
            negotiated_functions,
            negotiated_capabilities,
            handshake_completed_at,
            self_check_passed,
            self_check_completed_at,
            expires_at,
        };

        metadata.validate()?;
        Ok(metadata)
    }

    /// Check if this metadata has expired.
    pub fn is_expired(&self, now_timestamp: i64) -> bool {
        now_timestamp >= self.expires_at
    }

    /// Get the TTL remaining in seconds.
    pub fn ttl_seconds(&self, now_timestamp: i64) -> i64 {
        (self.expires_at - now_timestamp).max(0)
    }
}

impl PluginRegistryRecord {
    /// Validate this registry record.
    pub fn validate(&self) -> Result<(), AugmentedMetadataError> {
        if self.plugin_id.is_empty() {
            return Err(AugmentedMetadataError::InvalidTimestamp(
                "plugin_id must not be empty".to_string(),
            ));
        }

        if self.registered_at <= 0 {
            return Err(AugmentedMetadataError::InvalidTimestamp(
                "registered_at must be positive".to_string(),
            ));
        }

        self.augmented_metadata.validate()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_identity() -> PluginIdentity {
        PluginIdentity {
            magic: crate::identity::CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "test-plugin".to_string(),
            plugin_version: "1.0.0".to_string(),
        }
    }

    #[test]
    fn test_augmented_metadata_valid() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let mut capabilities = BTreeSet::new();
        capabilities.insert("async".to_string());

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: capabilities,
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn test_augmented_metadata_no_functions() {
        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: BTreeMap::new(),
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        assert_eq!(
            metadata.validate(),
            Err(AugmentedMetadataError::NoNegotiatedFunctions)
        );
    }

    #[test]
    fn test_augmented_metadata_invalid_handshake_timestamp() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 0,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        assert!(metadata.validate().is_err());
    }

    #[test]
    fn test_augmented_metadata_expiration_before_handshake() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 2000,
            self_check_passed: true,
            self_check_completed_at: 2100,
            expires_at: 1000,
        };

        assert_eq!(
            metadata.validate(),
            Err(AugmentedMetadataError::ExpirationBeforeHandshake)
        );
    }

    #[test]
    fn test_augmented_metadata_serde() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);
        functions.insert("observe".to_string(), 2);

        let mut capabilities = BTreeSet::new();
        capabilities.insert("async".to_string());
        capabilities.insert("streaming".to_string());

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: capabilities,
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: AugmentedMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_augmented_metadata_deny_unknown_fields() {
        let json = r#"{"identity":{"magic":[204,27,112,16,0,1,0,0],"abi_envelope":1,"plugin_name":"test","plugin_version":"1.0"},"negotiated_functions":{"route":1},"negotiated_capabilities":[],"handshake_completed_at":1000,"self_check_passed":true,"self_check_completed_at":1100,"expires_at":2000,"unknown":"field"}"#;
        let result: Result<AugmentedMetadata, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_augmented_metadata_size_limit() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let mut capabilities = BTreeSet::new();
        // Add a huge capability name to exceed size limit
        capabilities.insert("x".repeat(crate::limits::AUGMENTED_METADATA_MAX_BYTES + 1));

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: capabilities,
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        assert_eq!(
            metadata.validate(),
            Err(AugmentedMetadataError::SizeLimitExceeded)
        );
    }

    #[test]
    fn test_from_handshake_and_self_check() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let mut capabilities = BTreeSet::new();
        capabilities.insert("async".to_string());

        let metadata = AugmentedMetadata::from_handshake_and_self_check(
            make_test_identity(),
            functions,
            capabilities,
            1000,
            true,
            1100,
            3600,
        );

        assert!(metadata.is_ok());
        let m = metadata.unwrap();
        assert_eq!(m.handshake_completed_at, 1000);
        assert_eq!(m.self_check_passed, true);
        assert_eq!(m.expires_at, 1000 + 3600);
    }

    #[test]
    fn test_from_handshake_and_self_check_validates() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        // Try with invalid timestamp
        let result = AugmentedMetadata::from_handshake_and_self_check(
            make_test_identity(),
            functions,
            BTreeSet::new(),
            0,
            true,
            1100,
            3600,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_is_expired() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        assert!(!metadata.is_expired(1999));
        assert!(metadata.is_expired(2000));
        assert!(metadata.is_expired(2001));
    }

    #[test]
    fn test_ttl_seconds() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        assert_eq!(metadata.ttl_seconds(1500), 500);
        assert_eq!(metadata.ttl_seconds(2000), 0);
        assert_eq!(metadata.ttl_seconds(2100), 0);
    }

    #[test]
    fn test_plugin_registry_record_valid() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        let record = PluginRegistryRecord {
            plugin_id: "plugin-uuid-123".to_string(),
            augmented_metadata: metadata,
            registered_at: 900,
        };

        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_plugin_registry_record_empty_id() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        let record = PluginRegistryRecord {
            plugin_id: String::new(),
            augmented_metadata: metadata,
            registered_at: 900,
        };

        assert!(record.validate().is_err());
    }

    #[test]
    fn test_plugin_registry_record_serde() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), 1);

        let metadata = AugmentedMetadata {
            identity: make_test_identity(),
            negotiated_functions: functions,
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1000,
            self_check_passed: true,
            self_check_completed_at: 1100,
            expires_at: 2000,
        };

        let record = PluginRegistryRecord {
            plugin_id: "plugin-123".to_string(),
            augmented_metadata: metadata,
            registered_at: 900,
        };

        let json = serde_json::to_string(&record).unwrap();
        let deserialized: PluginRegistryRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(record, deserialized);
    }

    #[test]
    fn test_plugin_registry_record_deny_unknown_fields() {
        let json = r#"{"plugin_id":"test","augmented_metadata":{"identity":{"magic":[204,27,112,16,0,1,0,0],"abi_envelope":1,"plugin_name":"test","plugin_version":"1.0"},"negotiated_functions":{"route":1},"negotiated_capabilities":[],"handshake_completed_at":1000,"self_check_passed":true,"self_check_completed_at":1100,"expires_at":2000},"registered_at":900,"unknown":"field"}"#;
        let result: Result<PluginRegistryRecord, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}
