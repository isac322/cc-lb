//! Writer stream W2 — credentials and incident response.
//!
//! Hosts the W2 feature scenarios (F5, F7, F8, F10, F11A, F11B, F11C;
//! 66 scenarios total in the v5.2 corpus). Each feature lives in its
//! own `mod w2_f<n>` submodule. See `docs/cc-lb-bdd-fast-subset.md`
//! for the W2 fast-subset budget (9 scenarios).

use cc_lb_bdd_tests::{KillswitchState, bdd_scenario};

mod w2_f7 {
    use super::*;

    bdd_scenario! {
        id: "F7.1",
        fn_name: fast_f7_1,
        persona: Charlie,
        title: "Charlie engaging the killswitch flips the global signal on",
        description:
            "Charlie flips the emergency killswitch. The global flag in \
             the storage meta table reports enabled=true so the request \
             pipeline can reject every subsequent call. The storage-side \
             invariant — the flag actually flips — is what F7.1 owns; the \
             downstream response shape lives in F7.5 and the integration \
             flow lives in F3 scenarios.",
        given: |ctx| {
            ctx.charlie().await
        },
        when: |charlie| {
            charlie.enable_killswitch().await?
        },
        then: |state, ctx| {
            let state: KillswitchState = state;
            ctx.assert(
                state.enabled,
                format!(
                    "killswitch did not engage: expected enabled=true, actual={}",
                    state.enabled
                ),
            );
        },
    }

    bdd_scenario! {
        id: "F7.2",
        fn_name: fast_f7_2,
        persona: Charlie,
        title: "Charlie disengaging the killswitch returns the system to normal",
        description:
            "Charlie first engages the killswitch, then disengages it. \
             The global flag returns to enabled=false so the request \
             pipeline accepts traffic again. F7.2 is the symmetric \
             counterpart to F7.1: an incident playbook MUST be able to \
             reverse itself without bouncing the process.",
        given: |ctx| {
            ctx.charlie().await
        },
        when: |charlie| {
            charlie.enable_killswitch().await?;
            charlie.disable_killswitch().await?
        },
        then: |state, ctx| {
            let state: KillswitchState = state;
            ctx.assert(
                !state.enabled,
                format!(
                    "killswitch did not disengage: expected enabled=false, actual={}",
                    state.enabled
                ),
            );
        },
    }
}
