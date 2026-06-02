extern crate alloc;

use alloc::string::String;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 8-byte magic number identifying cc-lb plugin wire format v1.
pub const CC_LB_PLUGIN_MAGIC: [u8; 8] = [0xCC, 0x1B, 0x70, 0x10, 0x00, 0x01, 0x00, 0x00];

/// Custom section name for plugin identity metadata in WASM modules.
pub const CC_LB_PLUGIN_SECTION_NAME: &str = "cc_lb.plugin.v1";

/// Identity metadata for a cc-lb plugin.
///
/// This struct is serialized into a WASM custom section and used to validate
/// plugin compatibility and metadata before instantiation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginIdentity {
    /// 8-byte magic number that must match CC_LB_PLUGIN_MAGIC.
    pub magic: [u8; 8],

    /// ABI envelope version or compatibility identifier.
    pub abi_envelope: u32,

    /// Plugin name: lowercase alphanumeric, underscore, and hyphen; max 64 bytes.
    pub plugin_name: String,

    /// Plugin semantic version string; max 32 bytes.
    pub plugin_version: String,
}

/// Error type for identity validation failures.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum IdentityError {
    #[error("magic number mismatch")]
    MagicMismatch,

    #[error("plugin name is empty")]
    PluginNameEmpty,

    #[error("plugin name does not match pattern ^[a-z][a-z0-9_-]*$ (max 64 bytes)")]
    PluginNameInvalid,

    #[error("plugin version is empty")]
    PluginVersionEmpty,

    #[error("plugin version exceeds max length of 32 bytes")]
    PluginVersionTooLong,
}

impl PluginIdentity {
    /// Validate this identity against required constraints.
    ///
    /// Checks:
    /// - magic matches CC_LB_PLUGIN_MAGIC
    /// - plugin_name matches ^[a-z][a-z0-9_-]*$ and is at most 64 bytes
    /// - plugin_version is at most 32 bytes and not empty
    pub fn validate(&self) -> Result<(), IdentityError> {
        // Check magic.
        if self.magic != CC_LB_PLUGIN_MAGIC {
            return Err(IdentityError::MagicMismatch);
        }

        // Check plugin_name.
        if self.plugin_name.is_empty() {
            return Err(IdentityError::PluginNameEmpty);
        }

        let name_bytes = self.plugin_name.as_bytes();
        if name_bytes.len() > 64 {
            return Err(IdentityError::PluginNameInvalid);
        }

        let is_valid_name = name_bytes[0] >= b'a'
            && name_bytes[0] <= b'z'
            && name_bytes.iter().all(|&b| {
                (b >= b'a' && b <= b'z') || (b >= b'0' && b <= b'9') || b == b'_' || b == b'-'
            });

        if !is_valid_name {
            return Err(IdentityError::PluginNameInvalid);
        }

        // Check plugin_version.
        if self.plugin_version.is_empty() {
            return Err(IdentityError::PluginVersionEmpty);
        }

        if self.plugin_version.as_bytes().len() > 32 {
            return Err(IdentityError::PluginVersionTooLong);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn test_valid_identity() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my-plugin".to_string(),
            plugin_version: "1.0.0".to_string(),
        };
        assert!(identity.validate().is_ok());
    }

    #[test]
    fn test_magic_mismatch() {
        let identity = PluginIdentity {
            magic: [0; 8],
            abi_envelope: 1,
            plugin_name: "my-plugin".to_string(),
            plugin_version: "1.0.0".to_string(),
        };
        assert_eq!(identity.validate(), Err(IdentityError::MagicMismatch));
    }

    #[test]
    fn test_plugin_name_empty() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: String::new(),
            plugin_version: "1.0.0".to_string(),
        };
        assert_eq!(identity.validate(), Err(IdentityError::PluginNameEmpty));
    }

    #[test]
    fn test_plugin_name_starts_with_uppercase() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "MyPlugin".to_string(),
            plugin_version: "1.0.0".to_string(),
        };
        assert_eq!(identity.validate(), Err(IdentityError::PluginNameInvalid));
    }

    #[test]
    fn test_plugin_name_with_invalid_char() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my.plugin".to_string(),
            plugin_version: "1.0.0".to_string(),
        };
        assert_eq!(identity.validate(), Err(IdentityError::PluginNameInvalid));
    }

    #[test]
    fn test_plugin_name_too_long() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "a".repeat(65),
            plugin_version: "1.0.0".to_string(),
        };
        assert_eq!(identity.validate(), Err(IdentityError::PluginNameInvalid));
    }

    #[test]
    fn test_plugin_name_max_length() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "a".repeat(64),
            plugin_version: "1.0.0".to_string(),
        };
        assert!(identity.validate().is_ok());
    }

    #[test]
    fn test_plugin_name_with_underscore_and_hyphen() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my_plugin-v2".to_string(),
            plugin_version: "1.0.0".to_string(),
        };
        assert!(identity.validate().is_ok());
    }

    #[test]
    fn test_plugin_version_empty() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my-plugin".to_string(),
            plugin_version: String::new(),
        };
        assert_eq!(identity.validate(), Err(IdentityError::PluginVersionEmpty));
    }

    #[test]
    fn test_plugin_version_too_long() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my-plugin".to_string(),
            plugin_version: "v".repeat(33),
        };
        assert_eq!(
            identity.validate(),
            Err(IdentityError::PluginVersionTooLong)
        );
    }

    #[test]
    fn test_plugin_version_max_length() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my-plugin".to_string(),
            plugin_version: "v".repeat(32),
        };
        assert!(identity.validate().is_ok());
    }

    #[test]
    fn test_serde_serialize_deserialize() {
        let identity = PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "my-plugin".to_string(),
            plugin_version: "1.0.0".to_string(),
        };

        let json = serde_json::to_string(&identity).unwrap();
        let deserialized: PluginIdentity = serde_json::from_str(&json).unwrap();

        assert_eq!(identity, deserialized);
    }

    #[test]
    fn test_serde_deny_unknown_fields() {
        let json = r#"{"magic":[204,27,112,16,0,1,0,0],"abi_envelope":1,"plugin_name":"my-plugin","plugin_version":"1.0.0","unknown_field":"value"}"#;
        let result: Result<PluginIdentity, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}
