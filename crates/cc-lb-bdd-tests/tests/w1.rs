//! Writer stream W1 — day-to-day operator traffic.
//!
//! Hosts the W1 feature scenarios (F1, F2, F3, F4, F6, F19, F26;
//! 98 scenarios total in the v5.2 corpus). Each feature lives in its
//! own `mod w1_f<n>` submodule; scenarios inside are emitted by the
//! `bdd_scenario!` macro.
//!
//! See `docs/cc-lb-bdd-test-conversion-map.md` for the full list of
//! pending W1 scenarios and `docs/cc-lb-bdd-fast-subset.md` for the
//! `fast_` prefix budget.

use cc_lb_bdd_tests::{PrincipalCreateResult, bdd_scenario};

mod w1_f1 {
    use super::*;

    bdd_scenario! {
        id: "F1.1a",
        fn_name: fast_f1_1a,
        persona: Alice,
        title: "Alice registers a new principal and sees it become active",
        description:
            "Alice creates a machine principal named 'team-x'. The store \
             returns a record whose enabled flag is true (the user-facing \
             'active' signal). Name and revision must round-trip unchanged.",
        given: |ctx| {
            ctx.alice().await
        },
        when: |alice| {
            alice.create_principal("team-x").await?
        },
        then: |result, ctx| {
            let result: PrincipalCreateResult = result;
            ctx.assert(
                result.is_active,
                "active flag missing on freshly created principal: \
                 expected=true, actual=false",
            );
            ctx.assert(
                result.name == "team-x",
                format!(
                    "principal name mismatch: expected='team-x', actual='{}'",
                    result.name
                ),
            );
            ctx.assert(
                result.revision == 0 || result.revision == 1,
                format!(
                    "freshly created principal revision out of range: \
                     expected=0 or 1, actual={}",
                    result.revision
                ),
            );
        },
    }
}
