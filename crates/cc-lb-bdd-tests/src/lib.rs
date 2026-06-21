//! BDD scenario test harness for the cc-lb v5.2 BDD corpus.
//!
//! - 27 features / 321 scenarios across four writer streams (W1..W4).
//! - Each scenario is realised as a `bdd_scenario!` macro invocation that
//!   expands to a `_sqlite()` and (when the `postgres` feature is enabled)
//!   `_postgres()` `#[tokio::test]` pair.
//! - See `docs/cc-lb-bdd-test-conversion-plan.md` for the authoritative
//!   v3.2 conversion plan, including the macro contract (§3.5), the
//!   forbidden Rust jargon list (§12.1), and the no-real-API gate (§13).

pub mod backend;
pub mod ctx;
pub mod persona;
pub mod results;
#[macro_use]
pub mod scenario;

pub use ctx::BddCtx;
pub use persona::{Alice, Bob, Charlie, Dana};
pub use results::{
    AuditEntrySummary, HealthSnapshot, KillswitchState, PrincipalCreateResult,
    PrincipalDisableResult, PrincipalModelAclResult, PrincipalSoftDeleteResult,
};

/// Marker enum used by the macro `persona = ...` attribute. The variant
/// names match the v5.2 persona vocabulary exactly: Alice (operator),
/// Bob (developer / plugin author), Charlie (SRE), Dana (auditor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Persona {
    Alice,
    Bob,
    Charlie,
    Dana,
}

impl Persona {
    pub fn label(self) -> &'static str {
        match self {
            Persona::Alice => "Alice",
            Persona::Bob => "Bob",
            Persona::Charlie => "Charlie",
            Persona::Dana => "Dana",
        }
    }
}
