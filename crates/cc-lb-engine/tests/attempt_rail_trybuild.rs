#[test]
fn attempt_rail_typestate_contracts() {
    let test_cases = trybuild::TestCases::new();
    test_cases.pass("tests/trybuild/attempt_rail/pass_valid_flow.rs");
    test_cases.compile_fail("tests/trybuild/attempt_rail/fail_signed_without_scoped.rs");
    test_cases.compile_fail("tests/trybuild/attempt_rail/fail_dispatch_without_signed.rs");
    test_cases.compile_fail("tests/trybuild/attempt_rail/fail_reserved_direct_construction.rs");
}
