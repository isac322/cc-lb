mod common;

use std::collections::HashSet;
use std::sync::Arc;

use proptest::prelude::*;

fn identifier() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-z0-9_]{1,16}").expect("identifier regex is valid")
}

fn filter_module() -> &'static str {
    r#"(module (func (export "filter") (result i32) (i32.const 0)))"#
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]

    #[test]
    fn committed_filter_slots_have_one_key_per_unique_principal_plugin_pair(
        pairs in proptest::collection::vec((identifier(), identifier()), 1..=256),
    ) {
        let fixture = common::fixture("filter", filter_module(), common::metadata(&[]));
        let runtime = common::runtime();
        let unique_pairs = pairs.iter().cloned().collect::<HashSet<_>>();
        let mut staged = Vec::with_capacity(pairs.len());

        for (principal_id, plugin_name) in &pairs {
            let (_handle, slot) = runtime
                .instantiate_filter_for(principal_id, uuid::Uuid::new_v4(), plugin_name, &fixture.manifest)
                .expect("filter staging succeeds");
            staged.push(slot);
        }

        runtime.commit_staged(staged).expect("staged slots commit");

        let registered_pairs = runtime
            .registered_slot_keys()
            .into_iter()
            .collect::<HashSet<_>>();
        prop_assert_eq!(registered_pairs.len(), unique_pairs.len());
        prop_assert_eq!(registered_pairs, unique_pairs);
    }
}

#[test]
fn staged_filter_handles_are_distinct_for_different_principals_with_same_plugin_name() {
    let fixture = common::fixture("filter", filter_module(), common::metadata(&[]));
    let runtime = common::runtime();

    let (alice_handle, alice_slot) = runtime
        .instantiate_filter_for("alice", uuid::Uuid::new_v4(), "shared", &fixture.manifest)
        .expect("alice filter stages");
    let (bob_handle, bob_slot) = runtime
        .instantiate_filter_for("bob", uuid::Uuid::new_v4(), "shared", &fixture.manifest)
        .expect("bob filter stages");

    assert!(!Arc::ptr_eq(&alice_handle, &bob_handle));

    runtime
        .commit_staged(vec![alice_slot, bob_slot])
        .expect("staged filters commit");
    assert_eq!(runtime.registered_slot_keys().len(), 2);
}
