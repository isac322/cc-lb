//! The `bdd_scenario!` macro.
//!
//! Expands a scenario declaration into one `#[tokio::test]` per
//! backend (sqlite always, postgres behind the `postgres` cargo
//! feature). The generated tests sit inside a per-scenario module
//! named after `fn_name`, so the full nextest test path becomes
//! `<binary>::<fn_name>::sqlite` (and `<binary>::<fn_name>::postgres`).
//! That layout keeps the `fast_` prefix usable by the nextest filter
//! expression documented in `docs/cc-lb-bdd-test-conversion-plan.md`
//! §10.3 (`test(/fast_/)`).
//!
//! ### Required attributes
//!
//! - `id`           — scenario id (string literal) used as the panic
//!                    prefix and as the doc-comment first line.
//! - `fn_name`      — Rust identifier used as the module name.
//!                    Prefix with `fast_` to include in the PR-gate
//!                    fast subset.
//! - `persona`      — `Alice` / `Bob` / `Charlie` / `Dana`.
//! - `title`        — short, English, one-line scenario title.
//! - `description`  — multi-sentence English description; required
//!                    by the v3.2 plan §3.5 to guarantee static
//!                    extractability by the lint described in §12.1.
//! - `given`        — block `|ctx| { ... }`; returns the subject the
//!                    `when` block consumes.
//! - `when`         — block `|subject| { ... }`; returns the action
//!                    result the `then` block consumes.
//! - `then`         — block `|result, ctx| { ... }`; performs
//!                    assertions. `result` must be type-annotated by
//!                    the caller via `let result: T = result;`.

#[macro_export]
macro_rules! bdd_scenario {
    (
        id: $id:literal,
        fn_name: $fn_name:ident,
        persona: $persona:ident,
        title: $title:literal,
        description: $desc:literal,
        given: | $g_ctx:ident | $g_body:block ,
        when:  | $w_in:ident |  $w_body:block ,
        then:  | $t_result:ident , $t_ctx:ident | $t_body:block $(,)?
    ) => {
        #[doc = $title]
        #[doc = ""]
        #[doc = $desc]
        pub mod $fn_name {
            #[allow(unused_imports)]
            use super::*;

            pub const SCENARIO_ID: &str = $id;
            pub const PERSONA: $crate::Persona = $crate::Persona::$persona;

            #[tokio::test]
            async fn sqlite() -> ::anyhow::Result<()> {
                let ctx =
                    $crate::BddCtx::new_sqlite(SCENARIO_ID, PERSONA).await?;
                let $w_in = {
                    let $g_ctx = &ctx;
                    $g_body
                };
                let $t_result = $w_body;
                {
                    let $t_ctx = &ctx;
                    $t_body
                }
                Ok(())
            }
        }
    };
}
