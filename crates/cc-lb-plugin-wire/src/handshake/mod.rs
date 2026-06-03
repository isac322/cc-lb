extern crate alloc;

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::string::String;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod canonical;

use crate::limits::{
    CAPABILITIES_MAX_COUNT, FUNCTION_VERSIONS_KEYS_MAX, FUNCTION_VERSIONS_PER_FN_MAX, VERSION_MAX,
    VERSION_MIN,
};
pub use canonical::{CanonicalError, CanonicalOffer, canonicalize, host_offer_hash};

pub const HANDSHAKE_SCHEMA_VERSION_V1: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandshakeOfferRaw {
    pub handshake_schema_version: u32,
    pub envelope_version: u32,
    pub function_versions: Vec<FunctionVersionOfferRaw>,
    pub host_capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionVersionOfferRaw {
    pub function: String,
    pub versions: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandshakeOffer {
    pub handshake_schema_version: u32,
    pub envelope_version: u32,
    pub function_versions: BTreeMap<String, Vec<u32>>,
    pub host_capabilities: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandshakeAccept {
    pub handshake_schema_version: u32,
    pub envelope_version: u32,
    pub chosen_versions: BTreeMap<String, u32>,
    pub plugin_supported: BTreeMap<String, Vec<u32>>,
    pub implemented_functions: BTreeSet<String>,
    pub required_capabilities: BTreeSet<String>,
}

#[derive(Debug, Clone, Error)]
pub enum HandshakeError {
    #[error("function count {count} exceeds maximum {max}")]
    FunctionCountExceeded { count: usize, max: usize },

    #[error("function '{function}' has {count} versions, exceeds maximum {max}")]
    FunctionVersionCountExceeded {
        function: String,
        count: usize,
        max: usize,
    },

    #[error("version {version} outside valid range [{min}, {max}]")]
    VersionOutOfRange { version: u32, min: u32, max: u32 },

    #[error("capability count {count} exceeds maximum {max}")]
    CapabilityCountExceeded { count: usize, max: usize },

    #[error("chosen version not offered: function '{function}' version {version} not in offer")]
    ChosenVersionNotOffered { function: String, version: u32 },

    #[error("chosen for unknown function: '{function}' not in host offer")]
    ChosenForUnknownFunction { function: String },

    #[error(
        "downgrade attempt detected: function '{function}' chosen version {chosen} but max intersection is {max_intersection}"
    )]
    DowngradeAttempt {
        function: String,
        chosen: u32,
        max_intersection: u32,
    },

    #[error("required capability '{capability}' not available in host capabilities")]
    RequiredCapabilityUnavailable { capability: String },

    #[error("handshake schema version mismatch: got {got}, expected {expected}")]
    HandshakeSchemaVersionMismatch { got: u32, expected: u32 },

    #[error("canonical offer hash error: {0}")]
    Canonical(#[from] CanonicalError),
}

impl HandshakeOffer {
    pub fn validate(&self) -> Result<(), HandshakeError> {
        if self.function_versions.len() > FUNCTION_VERSIONS_KEYS_MAX {
            return Err(HandshakeError::FunctionCountExceeded {
                count: self.function_versions.len(),
                max: FUNCTION_VERSIONS_KEYS_MAX,
            });
        }

        for (function, versions) in &self.function_versions {
            if versions.len() > FUNCTION_VERSIONS_PER_FN_MAX {
                return Err(HandshakeError::FunctionVersionCountExceeded {
                    function: function.clone(),
                    count: versions.len(),
                    max: FUNCTION_VERSIONS_PER_FN_MAX,
                });
            }

            for &version in versions {
                if !(VERSION_MIN..=VERSION_MAX).contains(&version) {
                    return Err(HandshakeError::VersionOutOfRange {
                        version,
                        min: VERSION_MIN,
                        max: VERSION_MAX,
                    });
                }
            }
        }

        if self.host_capabilities.len() > CAPABILITIES_MAX_COUNT {
            return Err(HandshakeError::CapabilityCountExceeded {
                count: self.host_capabilities.len(),
                max: CAPABILITIES_MAX_COUNT,
            });
        }

        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<[u8; 32], CanonicalError> {
        let raw = HandshakeOfferRaw {
            handshake_schema_version: self.handshake_schema_version,
            envelope_version: self.envelope_version,
            function_versions: self
                .function_versions
                .iter()
                .map(|(fn_name, versions)| FunctionVersionOfferRaw {
                    function: fn_name.clone(),
                    versions: versions.clone(),
                })
                .collect(),
            host_capabilities: self.host_capabilities.iter().cloned().collect(),
        };
        host_offer_hash(&raw)
    }
}

impl HandshakeAccept {
    #[allow(clippy::collapsible_if)]
    pub fn validate_against_offer(&self, offer: &HandshakeOffer) -> Result<(), HandshakeError> {
        if self.handshake_schema_version != offer.handshake_schema_version {
            return Err(HandshakeError::HandshakeSchemaVersionMismatch {
                got: self.handshake_schema_version,
                expected: offer.handshake_schema_version,
            });
        }

        for (function, &chosen) in &self.chosen_versions {
            if !self.plugin_supported.contains_key(function) {
                return Err(HandshakeError::ChosenForUnknownFunction {
                    function: function.clone(),
                });
            }

            if !offer.function_versions.contains_key(function) {
                return Err(HandshakeError::ChosenForUnknownFunction {
                    function: function.clone(),
                });
            }

            let plugin_versions = &self.plugin_supported[function];
            let offer_versions = &offer.function_versions[function];

            if !offer_versions.contains(&chosen) {
                return Err(HandshakeError::ChosenVersionNotOffered {
                    function: function.clone(),
                    version: chosen,
                });
            }

            let intersection_max = offer_versions
                .iter()
                .filter(|v| plugin_versions.contains(v))
                .max()
                .copied();

            if let Some(max_intersection) = intersection_max {
                if chosen < max_intersection {
                    return Err(HandshakeError::DowngradeAttempt {
                        function: function.clone(),
                        chosen,
                        max_intersection,
                    });
                }
            }
        }

        for capability in &self.required_capabilities {
            if !offer.host_capabilities.contains(capability) {
                return Err(HandshakeError::RequiredCapabilityUnavailable {
                    capability: capability.clone(),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    #[test]
    fn handshake_schema_version_locked() {
        assert_eq!(HANDSHAKE_SCHEMA_VERSION_V1, 1);
    }

    #[test]
    fn empty_offer_valid() {
        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: BTreeMap::new(),
            host_capabilities: BTreeSet::new(),
        };
        assert!(offer.validate().is_ok());
    }

    #[test]
    fn function_count_exceeds_limit() {
        let mut functions = BTreeMap::new();
        for i in 0..=FUNCTION_VERSIONS_KEYS_MAX {
            functions.insert(alloc::format!("fn{}", i), vec![1]);
        }

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: functions,
            host_capabilities: BTreeSet::new(),
        };

        match offer.validate() {
            Err(HandshakeError::FunctionCountExceeded { .. }) => {}
            other => panic!("expected FunctionCountExceeded, got {:?}", other),
        }
    }

    #[test]
    fn version_count_per_function_exceeds_limit() {
        let mut functions = BTreeMap::new();
        functions.insert(
            "route".to_string(),
            (1..=FUNCTION_VERSIONS_PER_FN_MAX + 1)
                .map(|v| v as u32)
                .collect::<Vec<_>>(),
        );

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: functions,
            host_capabilities: BTreeSet::new(),
        };

        match offer.validate() {
            Err(HandshakeError::FunctionVersionCountExceeded { .. }) => {}
            other => panic!("expected FunctionVersionCountExceeded, got {:?}", other),
        }
    }

    #[test]
    fn invalid_version_too_low() {
        let mut functions = BTreeMap::new();
        functions.insert("route".to_string(), vec![0]);

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: functions,
            host_capabilities: BTreeSet::new(),
        };

        match offer.validate() {
            Err(HandshakeError::VersionOutOfRange { .. }) => {}
            other => panic!("expected VersionOutOfRange, got {:?}", other),
        }
    }

    #[test]
    fn capability_count_exceeds_limit() {
        let mut capabilities = BTreeSet::new();
        for i in 0..=CAPABILITIES_MAX_COUNT {
            capabilities.insert(alloc::format!("cap{}", i));
        }

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: BTreeMap::new(),
            host_capabilities: capabilities,
        };

        match offer.validate() {
            Err(HandshakeError::CapabilityCountExceeded { .. }) => {}
            other => panic!("expected CapabilityCountExceeded, got {:?}", other),
        }
    }

    #[test]
    fn downgrade_attack_rejected() {
        let mut offer_fns = BTreeMap::new();
        offer_fns.insert("route".to_string(), vec![1, 2, 3]);

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: offer_fns,
            host_capabilities: BTreeSet::new(),
        };

        let mut plugin_supported = BTreeMap::new();
        plugin_supported.insert("route".to_string(), vec![1, 2, 3]);

        let mut chosen = BTreeMap::new();
        chosen.insert("route".to_string(), 1);

        let accept = HandshakeAccept {
            handshake_schema_version: 1,
            envelope_version: 1,
            chosen_versions: chosen,
            plugin_supported,
            implemented_functions: BTreeSet::new(),
            required_capabilities: BTreeSet::new(),
        };

        match accept.validate_against_offer(&offer) {
            Err(HandshakeError::DowngradeAttempt {
                function,
                chosen,
                max_intersection,
            }) => {
                assert_eq!(function, "route");
                assert_eq!(chosen, 1);
                assert_eq!(max_intersection, 3);
            }
            other => panic!("expected DowngradeAttempt, got {:?}", other),
        }
    }

    #[test]
    fn chosen_not_in_offer_rejected() {
        let mut offer_fns = BTreeMap::new();
        offer_fns.insert("route".to_string(), vec![1, 2]);

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: offer_fns,
            host_capabilities: BTreeSet::new(),
        };

        let mut plugin_supported = BTreeMap::new();
        plugin_supported.insert("route".to_string(), vec![1, 2, 3]);

        let mut chosen = BTreeMap::new();
        chosen.insert("route".to_string(), 99);

        let accept = HandshakeAccept {
            handshake_schema_version: 1,
            envelope_version: 1,
            chosen_versions: chosen,
            plugin_supported,
            implemented_functions: BTreeSet::new(),
            required_capabilities: BTreeSet::new(),
        };

        match accept.validate_against_offer(&offer) {
            Err(HandshakeError::ChosenVersionNotOffered { .. }) => {}
            other => panic!("expected ChosenVersionNotOffered, got {:?}", other),
        }
    }

    #[test]
    fn missing_required_capability() {
        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: BTreeMap::new(),
            host_capabilities: {
                let mut caps = BTreeSet::new();
                caps.insert("log".to_string());
                caps
            },
        };

        let accept = HandshakeAccept {
            handshake_schema_version: 1,
            envelope_version: 1,
            chosen_versions: BTreeMap::new(),
            plugin_supported: BTreeMap::new(),
            implemented_functions: BTreeSet::new(),
            required_capabilities: {
                let mut caps = BTreeSet::new();
                caps.insert("trace".to_string());
                caps
            },
        };

        match accept.validate_against_offer(&offer) {
            Err(HandshakeError::RequiredCapabilityUnavailable { capability }) => {
                assert_eq!(capability, "trace");
            }
            other => panic!("expected RequiredCapabilityUnavailable, got {:?}", other),
        }
    }

    #[test]
    fn schema_version_mismatch() {
        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: BTreeMap::new(),
            host_capabilities: BTreeSet::new(),
        };

        let accept = HandshakeAccept {
            handshake_schema_version: 2,
            envelope_version: 1,
            chosen_versions: BTreeMap::new(),
            plugin_supported: BTreeMap::new(),
            implemented_functions: BTreeSet::new(),
            required_capabilities: BTreeSet::new(),
        };

        match accept.validate_against_offer(&offer) {
            Err(HandshakeError::HandshakeSchemaVersionMismatch { .. }) => {}
            other => panic!("expected HandshakeSchemaVersionMismatch, got {:?}", other),
        }
    }

    #[test]
    fn valid_handshake_roundtrip() {
        let mut offer_fns = BTreeMap::new();
        offer_fns.insert("route".to_string(), vec![1, 2]);
        offer_fns.insert("shape".to_string(), vec![1]);

        let mut offer_caps = BTreeSet::new();
        offer_caps.insert("streaming".to_string());

        let offer = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: offer_fns,
            host_capabilities: offer_caps,
        };

        assert!(offer.validate().is_ok());

        let mut plugin_supported = BTreeMap::new();
        plugin_supported.insert("route".to_string(), vec![1, 2]);
        plugin_supported.insert("shape".to_string(), vec![1]);

        let mut plugin_caps = BTreeSet::new();
        plugin_caps.insert("streaming".to_string());

        let mut chosen = BTreeMap::new();
        chosen.insert("route".to_string(), 2);
        chosen.insert("shape".to_string(), 1);

        let accept = HandshakeAccept {
            handshake_schema_version: 1,
            envelope_version: 1,
            chosen_versions: chosen,
            plugin_supported,
            implemented_functions: {
                let mut fns = BTreeSet::new();
                fns.insert("route".to_string());
                fns.insert("shape".to_string());
                fns
            },
            required_capabilities: plugin_caps,
        };

        assert!(accept.validate_against_offer(&offer).is_ok());
    }

    #[test]
    fn serde_roundtrip_offer() {
        let mut offer_fns = BTreeMap::new();
        offer_fns.insert("route".to_string(), vec![1, 2]);

        let original = HandshakeOffer {
            handshake_schema_version: 1,
            envelope_version: 1,
            function_versions: offer_fns,
            host_capabilities: {
                let mut caps = BTreeSet::new();
                caps.insert("streaming".to_string());
                caps
            },
        };

        let json = serde_json::to_vec(&original).expect("serialize");
        let deserialized: HandshakeOffer = serde_json::from_slice(&json).expect("deserialize");

        assert_eq!(original, deserialized);
    }

    #[test]
    fn serde_roundtrip_accept() {
        let mut plugin_supported = BTreeMap::new();
        plugin_supported.insert("route".to_string(), vec![1, 2]);

        let mut chosen = BTreeMap::new();
        chosen.insert("route".to_string(), 2);

        let original = HandshakeAccept {
            handshake_schema_version: 1,
            envelope_version: 1,
            chosen_versions: chosen,
            plugin_supported,
            implemented_functions: {
                let mut fns = BTreeSet::new();
                fns.insert("route".to_string());
                fns
            },
            required_capabilities: {
                let mut caps = BTreeSet::new();
                caps.insert("streaming".to_string());
                caps
            },
        };

        let json = serde_json::to_vec(&original).expect("serialize");
        let deserialized: HandshakeAccept = serde_json::from_slice(&json).expect("deserialize");

        assert_eq!(original, deserialized);
    }
}
