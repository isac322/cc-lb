//! Result types returned by persona client methods. These are the
//! types referenced verbatim in the `then` closures of `bdd_scenario!`
//! invocations and therefore form part of the test surface.

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PrincipalCreateResult {
    pub id: Uuid,
    pub name: String,
    pub is_active: bool,
    pub revision: u64,
    pub first_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PrincipalSoftDeleteResult {
    pub id: Uuid,
    pub deleted_at_unix_secs: Option<u64>,
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub struct AuditEntrySummary {
    pub kind: String,
    pub actor: String,
    pub principal_id: String,
    pub ts: u64,
}
