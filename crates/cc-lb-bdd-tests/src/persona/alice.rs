//! Alice — the operator persona. Holds admin privileges and exercises
//! the registration / configuration paths for principals, keys, and
//! upstreams (Writer stream W1 plus parts of W2).

use anyhow::Result;
use cc_lb_storage_api::PrincipalStore;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};

use crate::backend::StorageHandle;
use crate::results::PrincipalCreateResult;

/// Operator client. Currently exposes the subset of admin operations
/// required by the first wave of W1 scenarios (`F1.1a` onwards). The
/// full surface area is documented in
/// `docs/cc-lb-bdd-persona-helper-spec.md` §3 (`OperatorClient`).
pub struct Alice {
    storage: StorageHandle,
}

impl Alice {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    /// Register a new machine principal under the given name. The
    /// returned `PrincipalCreateResult` carries the fields exercised
    /// by the `then` clauses in the W1 registration scenarios.
    ///
    /// First-key issuance is not yet implemented; the returned
    /// `first_key` field is therefore always `None` and any scenario
    /// that asserts on it stays marked `pending` in
    /// `docs/cc-lb-bdd-test-conversion-map.md` until the managed-key
    /// helper is wired in (tracked by M2 follow-up).
    pub async fn create_principal(&self, name: &str) -> Result<PrincipalCreateResult> {
        let now = unix_now_secs();
        let record = PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            now,
        )
        .await?;

        Ok(PrincipalCreateResult {
            id: record.id,
            name: record.name,
            is_active: record.enabled,
            revision: record.revision,
            first_key: None,
        })
    }
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
