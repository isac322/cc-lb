extern crate alloc;

use alloc::{collections::BTreeMap, collections::BTreeSet, string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::HandshakeOfferRaw;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalOffer {
    pub handshake_schema_version: u32,
    pub envelope_version: u32,
    pub function_versions: BTreeMap<String, Vec<u32>>,
    pub host_capabilities: BTreeSet<String>,
}

#[derive(Debug, Error)]
pub enum CanonicalError {
    #[error("failed to serialize canonical handshake offer: {0}")]
    Serialize(#[from] serde_json::Error),
}

pub fn canonicalize(offer: &HandshakeOfferRaw) -> CanonicalOffer {
    let mut function_versions = BTreeMap::new();

    for offered_function in &offer.function_versions {
        let versions = function_versions
            .entry(offered_function.function.clone())
            .or_insert_with(Vec::new);
        versions.extend(offered_function.versions.iter().copied());
    }

    for versions in function_versions.values_mut() {
        versions.sort_unstable();
        versions.dedup();
    }

    let host_capabilities = offer
        .host_capabilities
        .iter()
        .map(|capability| capability.to_ascii_lowercase())
        .collect();

    CanonicalOffer {
        handshake_schema_version: offer.handshake_schema_version,
        envelope_version: offer.envelope_version,
        function_versions,
        host_capabilities,
    }
}

pub fn host_offer_hash(offer: &HandshakeOfferRaw) -> Result<[u8; 32], CanonicalError> {
    let canonical = canonicalize(offer);
    let bytes = serde_json::to_vec(&canonical)?;
    Ok(Sha256::digest(bytes).into())
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use alloc::{string::ToString, vec};
    use proptest::prelude::*;

    use crate::handshake::FunctionVersionOfferRaw;

    fn function_name_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("route".to_string()),
            Just("shape".to_string()),
            Just("normalize_error".to_string()),
            Just("build_signer".to_string()),
            Just("sign".to_string()),
            Just("on_unauthorized".to_string()),
            Just("observe".to_string()),
        ]
    }

    fn capability_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("Streaming".to_string()),
            Just("streaming".to_string()),
            Just("BATCHING".to_string()),
            Just("batching".to_string()),
            Just("Trace-Context".to_string()),
            Just("trace-context".to_string()),
            Just("CANARY".to_string()),
        ]
    }

    fn function_offer_strategy() -> impl Strategy<Value = FunctionVersionOfferRaw> {
        (
            function_name_strategy(),
            prop::collection::vec(0u32..16, 0..8),
        )
            .prop_map(|(function, versions)| FunctionVersionOfferRaw { function, versions })
    }

    fn offer_strategy() -> impl Strategy<Value = HandshakeOfferRaw> {
        (
            0u32..4,
            0u32..4,
            prop::collection::vec(function_offer_strategy(), 0..24),
            prop::collection::vec(capability_strategy(), 0..24),
        )
            .prop_map(
                |(
                    handshake_schema_version,
                    envelope_version,
                    function_versions,
                    host_capabilities,
                )| {
                    HandshakeOfferRaw {
                        handshake_schema_version,
                        envelope_version,
                        function_versions,
                        host_capabilities,
                    }
                },
            )
    }

    fn keyed_permutation<Item: Clone>(values: &[Item], keys: &[u64]) -> Vec<Item> {
        let mut keyed_values: Vec<_> = values
            .iter()
            .cloned()
            .enumerate()
            .map(|(value_index, value)| {
                (
                    keys.get(value_index).copied().unwrap_or(value_index as u64),
                    value_index,
                    value,
                )
            })
            .collect();
        keyed_values.sort_by_key(|(key, value_index, _)| (*key, *value_index));
        keyed_values
            .into_iter()
            .map(|(_, _, value)| value)
            .collect()
    }

    fn shuffled(
        offer: &HandshakeOfferRaw,
        function_keys: &[u64],
        capability_keys: &[u64],
        version_keys: &[Vec<u64>],
    ) -> HandshakeOfferRaw {
        let mut function_versions = keyed_permutation(&offer.function_versions, function_keys);
        for (function_index, function) in function_versions.iter_mut().enumerate() {
            let keys = version_keys
                .get(function_index)
                .map(Vec::as_slice)
                .unwrap_or_default();
            function.versions = keyed_permutation(&function.versions, keys);
        }

        HandshakeOfferRaw {
            handshake_schema_version: offer.handshake_schema_version,
            envelope_version: offer.envelope_version,
            function_versions,
            host_capabilities: keyed_permutation(&offer.host_capabilities, capability_keys),
        }
    }

    proptest! {
        #[test]
        fn canonicalize_is_shuffle_invariant(
            offer in offer_strategy(),
            function_keys in prop::collection::vec(any::<u64>(), 0..32),
            capability_keys in prop::collection::vec(any::<u64>(), 0..32),
            version_keys in prop::collection::vec(prop::collection::vec(any::<u64>(), 0..16), 0..32),
        ) {
            let shuffled = shuffled(&offer, &function_keys, &capability_keys, &version_keys);

            prop_assert_eq!(canonicalize(&offer), canonicalize(&shuffled));
        }

        #[test]
        fn host_offer_hash_is_shuffle_invariant(
            offer in offer_strategy(),
            function_keys in prop::collection::vec(any::<u64>(), 0..32),
            capability_keys in prop::collection::vec(any::<u64>(), 0..32),
            version_keys in prop::collection::vec(prop::collection::vec(any::<u64>(), 0..16), 0..32),
        ) {
            let shuffled = shuffled(&offer, &function_keys, &capability_keys, &version_keys);

            prop_assert_eq!(
                host_offer_hash(&offer).unwrap(),
                host_offer_hash(&shuffled).unwrap(),
            );
        }
    }

    #[test]
    fn canonicalize_sorts_dedups_and_lowercases() {
        let offer = HandshakeOfferRaw {
            handshake_schema_version: 1,
            envelope_version: 2,
            function_versions: vec![
                FunctionVersionOfferRaw {
                    function: "route".to_string(),
                    versions: vec![3, 1, 3],
                },
                FunctionVersionOfferRaw {
                    function: "route".to_string(),
                    versions: vec![2, 1],
                },
            ],
            host_capabilities: vec!["Streaming".to_string(), "streaming".to_string()],
        };

        let canonical = canonicalize(&offer);

        assert_eq!(canonical.function_versions["route"], vec![1, 2, 3]);
        assert_eq!(canonical.host_capabilities.len(), 1);
        assert!(canonical.host_capabilities.contains("streaming"));
    }
}
