use super::attempt_rail_trybuild::{register_attempt_rail_cases, trybuild_support};

#[allow(dead_code)]
#[path = "trybuild/authn_rail/pass_valid_flow.rs"]
mod authn_pass_valid_flow;

#[allow(dead_code)]
#[path = "trybuild/attempt_rail/pass_valid_flow.rs"]
mod attempt_pass_valid_flow;

#[test]
fn authn_rail_typestate_contracts() {
    trybuild_support::run_with_unified_rustflags(
        "authn_rail_trybuild::authn_rail_typestate_contracts",
        || {
            let test_cases = trybuild::TestCases::new();
            test_cases.compile_fail(
                "tests/trybuild/authn_rail/fail_authenticated_direct_construction.rs",
            );
            test_cases
                .compile_fail("tests/trybuild/authn_rail/fail_handle_without_authenticated.rs");
            register_attempt_rail_cases(&test_cases);
        },
    );
}
