//! Result types returned by persona client methods. These are the
//! types referenced verbatim in the `then` closures of `bdd_scenario!`
//! invocations and therefore form part of the test surface.

use uuid::Uuid;

/// Outcome of `Alice::create_principal`. The shape matches the
/// expectations of W1 registration scenarios (F1.1a etc.): the
/// scenario verifies that the new principal is active and that a
/// first managed key was issued in the same flow.
#[derive(Debug, Clone)]
pub struct PrincipalCreateResult {
    pub id: Uuid,
    pub name: String,
    /// `true` when the principal record carries `enabled = true`
    /// immediately after creation — the user-visible "active" signal
    /// in the v5.2 BDD source.
    pub is_active: bool,
    pub revision: u64,
    /// First managed key value emitted alongside the principal, if
    /// the registration flow issued one. `None` until the managed-key
    /// helper is wired into `Alice::create_principal` (M2 follow-up).
    pub first_key: Option<String>,
}
