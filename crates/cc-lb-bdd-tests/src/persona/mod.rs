//! Persona-typed client helpers used inside scenario closures.
//!
//! Each persona carries its own privilege envelope so that scenarios
//! fail loudly when the wrong actor performs an action. The full role
//! boundaries table lives in `docs/cc-lb-bdd-persona-helper-spec.md`.

pub mod alice;
pub mod bob;
pub mod charlie;
pub mod dana;

pub use alice::Alice;
pub use bob::Bob;
pub use charlie::Charlie;
pub use dana::Dana;
