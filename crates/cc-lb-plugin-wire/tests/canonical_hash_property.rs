use cc_lb_plugin_wire::handshake::{
    FunctionVersionOfferRaw, HandshakeOfferRaw, canonicalize, host_offer_hash,
};
use proptest::prelude::*;

const CASES: u32 = 1_000;

fn function_name_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("route".to_owned()),
        Just("shape".to_owned()),
        Just("normalize_error".to_owned()),
        Just("build_signer".to_owned()),
        Just("sign".to_owned()),
        Just("on_unauthorized".to_owned()),
        Just("observe".to_owned()),
    ]
}

fn capability_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Streaming".to_owned()),
        Just("streaming".to_owned()),
        Just("BATCHING".to_owned()),
        Just("batching".to_owned()),
        Just("Trace-Context".to_owned()),
        Just("trace-context".to_owned()),
        Just("CANARY".to_owned()),
        Just("audit-log".to_owned()),
    ]
}

fn function_offer_strategy() -> impl Strategy<Value = FunctionVersionOfferRaw> {
    (
        function_name_strategy(),
        prop::collection::vec(0_u32..16, 0..8),
    )
        .prop_map(|(function, versions)| FunctionVersionOfferRaw { function, versions })
}

fn offer_strategy() -> impl Strategy<Value = HandshakeOfferRaw> {
    (
        0_u32..8,
        0_u32..8,
        prop::collection::vec(function_offer_strategy(), 0..24),
        prop::collection::vec(capability_strategy(), 0..24),
    )
        .prop_map(
            |(handshake_schema_version, envelope_version, function_versions, host_capabilities)| {
                HandshakeOfferRaw {
                    handshake_schema_version,
                    envelope_version,
                    function_versions,
                    host_capabilities,
                }
            },
        )
}

#[derive(Clone, Copy, Debug)]
enum CanonicalMutation {
    HandshakeSchemaVersion,
    EnvelopeVersion,
    AddFunction,
    AddCapability,
}

fn mutation_strategy() -> impl Strategy<Value = CanonicalMutation> {
    prop_oneof![
        Just(CanonicalMutation::HandshakeSchemaVersion),
        Just(CanonicalMutation::EnvelopeVersion),
        Just(CanonicalMutation::AddFunction),
        Just(CanonicalMutation::AddCapability),
    ]
}

fn canonically_different_offer(
    offer: &HandshakeOfferRaw,
    mutation: CanonicalMutation,
) -> HandshakeOfferRaw {
    let mut changed_offer = offer.clone();

    match mutation {
        CanonicalMutation::HandshakeSchemaVersion => {
            changed_offer.handshake_schema_version += 1;
        }
        CanonicalMutation::EnvelopeVersion => {
            changed_offer.envelope_version += 1;
        }
        CanonicalMutation::AddFunction => {
            changed_offer
                .function_versions
                .push(FunctionVersionOfferRaw {
                    function: "collision_check_function".to_owned(),
                    versions: vec![1],
                });
        }
        CanonicalMutation::AddCapability => {
            changed_offer
                .host_capabilities
                .push("collision-check-capability".to_owned());
        }
    }

    changed_offer
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

fn shuffle(
    offer: &HandshakeOfferRaw,
    function_keys: &[u64],
    capability_keys: &[u64],
    version_keys: &[Vec<u64>],
) -> HandshakeOfferRaw {
    let mut function_versions = keyed_permutation(&offer.function_versions, function_keys);
    for (function_index, function_offer) in function_versions.iter_mut().enumerate() {
        let keys = version_keys
            .get(function_index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        function_offer.versions = keyed_permutation(&function_offer.versions, keys);
    }

    HandshakeOfferRaw {
        handshake_schema_version: offer.handshake_schema_version,
        envelope_version: offer.envelope_version,
        function_versions,
        host_capabilities: keyed_permutation(&offer.host_capabilities, capability_keys),
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: CASES,
        .. ProptestConfig::default()
    })]

    #[test]
    fn host_offer_hash_is_shuffle_invariant(
        offer in offer_strategy(),
        function_keys in prop::collection::vec(any::<u64>(), 0..32),
        capability_keys in prop::collection::vec(any::<u64>(), 0..32),
        version_keys in prop::collection::vec(prop::collection::vec(any::<u64>(), 0..16), 0..32),
    ) {
        let shuffled_offer = shuffle(&offer, &function_keys, &capability_keys, &version_keys);

        prop_assert_eq!(
            host_offer_hash(&shuffled_offer).unwrap(),
            host_offer_hash(&offer).unwrap(),
        );
    }

    #[test]
    fn canonically_different_offers_have_different_hashes(
        offer in offer_strategy(),
        mutation in mutation_strategy(),
    ) {
        let changed_offer = canonically_different_offer(&offer, mutation);

        prop_assert_ne!(canonicalize(&changed_offer), canonicalize(&offer));
        prop_assert_ne!(
            host_offer_hash(&changed_offer).unwrap(),
            host_offer_hash(&offer).unwrap(),
        );
    }
}

#[test]
fn known_hash_vectors_match_expected() {
    let vectors = vec![
        (
            offer(1, 1, vec![], vec![]),
            "13097368d9099cff63c6bbd7ebee48436f2fe29d1a3d4012cebfa2e73a83b168",
        ),
        (
            offer(
                1,
                1,
                vec![
                    function_offer("route", vec![2, 1, 2]),
                    function_offer("shape", vec![1]),
                ],
                vec!["Streaming", "streaming"],
            ),
            "96daf92ee1b32bb1acf0845e4036f6fdd663ff637f1f1ee2f8f2c728bb8e073e",
        ),
        (
            offer(
                1,
                2,
                vec![
                    function_offer("sign", vec![3, 1]),
                    function_offer("build_signer", vec![1, 1, 2]),
                    function_offer("sign", vec![2]),
                ],
                vec!["TRACE-CONTEXT", "batching", "trace-context"],
            ),
            "67b050f22577dd9d72f890d25e55b7551ea62c9c4ca4d9f18126d8d20b2980b9",
        ),
        (
            offer(
                2,
                1,
                vec![
                    function_offer("observe", vec![5, 4, 5, 1]),
                    function_offer("normalize_error", vec![1]),
                    function_offer("on_unauthorized", vec![2, 1]),
                ],
                vec!["Canary", "CANARY", "audit-log"],
            ),
            "4ce77c14cf3b212290ccb6e3d46882289c43f4db280b7a0dcd28d4e59c573617",
        ),
        (
            offer(
                1,
                3,
                vec![
                    function_offer("route", vec![1]),
                    function_offer("shape", vec![1]),
                    function_offer("normalize_error", vec![1]),
                    function_offer("build_signer", vec![1]),
                    function_offer("sign", vec![1]),
                    function_offer("on_unauthorized", vec![1]),
                    function_offer("observe", vec![1]),
                ],
                vec!["streaming", "trace-context", "batching"],
            ),
            "769a843b892afa269db9facd0620b670f50fead7146c85bc8fbff7e7dc88a41c",
        ),
    ];

    for (offer, expected_hash) in vectors {
        assert_eq!(hash_hex(&host_offer_hash(&offer).unwrap()), expected_hash);
    }
}

fn offer(
    handshake_schema_version: u32,
    envelope_version: u32,
    function_versions: Vec<FunctionVersionOfferRaw>,
    host_capabilities: Vec<&str>,
) -> HandshakeOfferRaw {
    HandshakeOfferRaw {
        handshake_schema_version,
        envelope_version,
        function_versions,
        host_capabilities: host_capabilities.into_iter().map(str::to_owned).collect(),
    }
}

fn function_offer(function: &str, versions: Vec<u32>) -> FunctionVersionOfferRaw {
    FunctionVersionOfferRaw {
        function: function.to_owned(),
        versions,
    }
}

fn hash_hex(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(64);
    for byte in hash {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
