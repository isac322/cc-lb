#[test]
fn authn_rail_typestate_contracts() {
    let test_cases = trybuild::TestCases::new();
    test_cases.pass("tests/trybuild/authn_rail/pass_valid_flow.rs");
    test_cases.compile_fail("tests/trybuild/authn_rail/fail_authenticated_direct_construction.rs");
    test_cases.compile_fail("tests/trybuild/authn_rail/fail_handle_without_authenticated.rs");
}
