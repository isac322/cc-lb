#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, Ordering};

use cc_lb_engine::api_keys::secret::{compute_verify_hash, generate_new, parse, verify_secret};
use proptest::prelude::*;

static ROUNDTRIP_CASE_COUNT_LOGGED: AtomicBool = AtomicBool::new(false);

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn sk_cclb_roundtrip_parses_generated_key(_seed in any::<u32>()) {
        if !ROUNDTRIP_CASE_COUNT_LOGGED.swap(true, Ordering::Relaxed) {
            println!("sk_cclb_roundtrip_parses_generated_key: 256 successful cases configured");
        }

        let output = generate_new();
        let (key_id, secret_b64_bytes) = parse(output.plaintext.expose()).expect("parse generated key");

        prop_assert_eq!(key_id, output.key_id);
        prop_assert_eq!(secret_b64_bytes.len(), 43);
    }

    #[test]
    fn sk_cclb_roundtrip_mutate_one_byte_fails_verify(mutation_index in 0usize..43) {
        let output = generate_new();
        let (_key_id, secret_b64_bytes) = parse(output.plaintext.expose()).expect("parse generated key");
        let verify_hash = compute_verify_hash(&secret_b64_bytes, &output.secret_salt);
        let mut mutated = secret_b64_bytes;

        mutated[mutation_index] ^= 0x01;

        prop_assert!(!verify_secret(&mutated, &verify_hash, &output.secret_salt));
    }
}
