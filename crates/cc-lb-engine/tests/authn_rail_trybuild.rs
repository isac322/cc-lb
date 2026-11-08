use super::attempt_rail_trybuild::{register_attempt_rail_cases, trybuild_support};

#[test]
fn authn_rail_typestate_contracts() {
    let _lock = trybuild_support::lock();
    let test_cases = trybuild::TestCases::new();
    test_cases.pass("tests/trybuild/authn_rail/pass_valid_flow.rs");
    test_cases.compile_fail("tests/trybuild/authn_rail/fail_authenticated_direct_construction.rs");
    test_cases.compile_fail("tests/trybuild/authn_rail/fail_handle_without_authenticated.rs");
    register_attempt_rail_cases(&test_cases);
}
