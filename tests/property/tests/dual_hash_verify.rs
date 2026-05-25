#![forbid(unsafe_code)]

use cc_lb_core::api_keys::secret::{compute_index_hash, compute_verify_hash, verify_secret};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn dual_hash_verify_succeeds_for_correct_secret(
        secret in proptest::collection::vec(any::<u8>(), 43..=43),
        salt in proptest::array::uniform16(any::<u8>()),
    ) {
        let verify_hash = compute_verify_hash(&secret, &salt);

        prop_assert!(verify_secret(&secret, &verify_hash, &salt));
    }

    #[test]
    fn dual_hash_verify_fails_for_mutated_secret(
        secret in proptest::collection::vec(any::<u8>(), 43..=43),
        salt in proptest::array::uniform16(any::<u8>()),
        index in 0usize..43,
    ) {
        let verify_hash = compute_verify_hash(&secret, &salt);
        let mut mutated = secret.clone();

        mutated[index] ^= 0x01;

        prop_assert!(!verify_secret(&mutated, &verify_hash, &salt));
    }

    #[test]
    fn dual_hash_verify_fails_for_mutated_salt(
        secret in proptest::collection::vec(any::<u8>(), 43..=43),
        salt in proptest::array::uniform16(any::<u8>()),
        index in 0usize..16,
    ) {
        let verify_hash = compute_verify_hash(&secret, &salt);
        let mut mutated = salt;

        mutated[index] ^= 0x01;

        prop_assert!(!verify_secret(&secret, &verify_hash, &mutated));
    }

    #[test]
    fn dual_hash_verify_index_hash_deterministic(
        secret in proptest::collection::vec(any::<u8>(), 43..=43),
    ) {
        let first_hash = compute_index_hash(&secret);
        let second_hash = compute_index_hash(&secret);

        prop_assert_eq!(first_hash, second_hash);
    }
}
