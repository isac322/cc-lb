//! Durable account-owner binding storage vocabulary.
//!
//! A binding revision fences permission changes. `owner_generation` changes only
//! when the provider/owner/upstream/vendor-workspace identity changes; tombstones are terminal.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageResult, UpstreamRecordId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountOwnerBinding {
    pub binding_id: String,
    pub principal_id: String,
    pub provider: String,
    pub owner_id: String,
    pub upstream_id: UpstreamRecordId,
    pub owner_generation: u64,
    pub local_user_id: String,
    pub allowed_workspaces: Vec<String>,
    pub provider_workspace_id: String,
    pub outbound_region: Option<String>,
    pub revision: u64,
    pub revoked_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerBindingExpectation {
    pub binding_id: String,
    pub principal_id: String,
    pub provider: String,
    pub owner_generation: u64,
    pub binding_revision: u64,
    pub workspace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountOwnerWorkspaceTarget {
    pub principal_id: String,
    pub provider: String,
    pub local_user_id: String,
    pub workspace_id: String,
}

#[async_trait]
pub trait AccountOwnerStore: Send + Sync {
    async fn create_owner_binding(&self, binding: AccountOwnerBinding) -> StorageResult<()>;
    async fn get_owner_binding(
        &self,
        binding_id: &str,
    ) -> StorageResult<Option<AccountOwnerBinding>>;
    /// Locates a snapshot, not an authorization decision. Revalidate it with
    /// the binding expectation in the same transaction as the protected write.
    async fn find_owner_binding_for_workspace(
        &self,
        target: &AccountOwnerWorkspaceTarget,
    ) -> StorageResult<AccountOwnerBinding>;
    async fn compare_and_replace_owner_binding(
        &self,
        binding: AccountOwnerBinding,
        expected_revision: u64,
    ) -> StorageResult<()>;
    async fn resolve_owner_binding(
        &self,
        expected: &OwnerBindingExpectation,
    ) -> StorageResult<AccountOwnerBinding>;
}

fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}

fn validate_common(binding: &AccountOwnerBinding) -> StorageResult<()> {
    for (name, value) in [
        ("binding_id", binding.binding_id.as_str()),
        ("principal_id", binding.principal_id.as_str()),
        ("provider", binding.provider.as_str()),
        ("owner_id", binding.owner_id.as_str()),
        ("local_user_id", binding.local_user_id.as_str()),
        (
            "provider_workspace_id",
            binding.provider_workspace_id.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(conflict(format!("{name} must not be empty")));
        }
    }
    if binding.allowed_workspaces.is_empty()
        || binding
            .allowed_workspaces
            .iter()
            .any(|workspace| workspace.trim().is_empty())
    {
        return Err(conflict(
            "allowed_workspaces must contain only non-empty aliases",
        ));
    }
    let mut unique = std::collections::HashSet::with_capacity(binding.allowed_workspaces.len());
    if binding
        .allowed_workspaces
        .iter()
        .any(|workspace| !unique.insert(workspace))
    {
        return Err(conflict("allowed_workspaces must not contain duplicates"));
    }
    Ok(())
}

/// Validates an initial active binding. Initial revisions and generations are one.
pub fn validate_owner_binding_for_create(binding: &AccountOwnerBinding) -> StorageResult<()> {
    validate_common(binding)?;
    if binding.revision != 1
        || binding.owner_generation != 1
        || binding.revoked_at_unix_secs.is_some()
    {
        return Err(conflict(
            "new owner binding must be active at revision and generation one",
        ));
    }
    Ok(())
}

/// Validates a replacement against the currently stored binding.
/// Identity changes advance generation; same-identity binding updates preserve it.
pub fn validate_owner_binding_replacement(
    existing: &AccountOwnerBinding,
    replacement: &AccountOwnerBinding,
    expected_revision: u64,
) -> StorageResult<()> {
    validate_common(replacement)?;
    if existing.binding_id != replacement.binding_id
        || existing.principal_id != replacement.principal_id
        || existing.local_user_id != replacement.local_user_id
    {
        return Err(conflict(
            "binding_id, principal_id, and local_user_id are immutable",
        ));
    }
    if existing.revoked_at_unix_secs.is_some() {
        return Err(conflict("revoked owner binding is terminal"));
    }
    if existing.revision != expected_revision {
        return Err(conflict("owner binding revision is stale"));
    }
    let next_revision = expected_revision
        .checked_add(1)
        .ok_or_else(|| conflict("owner binding revision overflow"))?;
    if replacement.revision != next_revision {
        return Err(conflict("replacement revision must advance by one"));
    }
    let identity_changed = existing.provider != replacement.provider
        || existing.owner_id != replacement.owner_id
        || existing.upstream_id != replacement.upstream_id
        || existing.provider_workspace_id != replacement.provider_workspace_id;
    let expected_generation = if identity_changed {
        existing
            .owner_generation
            .checked_add(1)
            .ok_or_else(|| conflict("owner generation overflow"))?
    } else {
        existing.owner_generation
    };
    if replacement.owner_generation != expected_generation {
        return Err(conflict(
            "owner generation does not match identity transition",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> AccountOwnerBinding {
        AccountOwnerBinding {
            binding_id: "b".into(),
            principal_id: "p".into(),
            provider: "openai".into(),
            owner_id: "o".into(),
            upstream_id: uuid::Uuid::nil(),
            owner_generation: 1,
            provider_workspace_id: "vendor-workspace".into(),
            local_user_id: "u".into(),
            allowed_workspaces: vec!["w".into()],
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        }
    }

    #[test]
    fn vendor_workspace_change_requires_generation_advance() {
        let initial = binding();
        let mut next = initial.clone();
        next.revision = 2;
        next.provider_workspace_id = "other-vendor-workspace".into();
        assert!(validate_owner_binding_replacement(&initial, &next, 1).is_err());
        next.owner_generation = 2;
        validate_owner_binding_replacement(&initial, &next, 1).unwrap();
        next.provider_workspace_id.clear();
        assert!(validate_owner_binding_replacement(&initial, &next, 1).is_err());
        let mut malformed = initial;
        malformed.provider_workspace_id = " ".into();
        assert!(validate_owner_binding_for_create(&malformed).is_err());
    }

    #[test]
    fn same_identity_binding_update() {
        let initial = binding();
        validate_owner_binding_for_create(&initial).unwrap();
        let mut next = initial.clone();
        next.revision = 2;
        validate_owner_binding_replacement(&initial, &next, 1).unwrap();
    }

    #[test]
    fn replacement_contract_rejects_stale_skip_and_overflow() {
        let initial = binding();
        for expected in [0, 2] {
            let mut next = initial.clone();
            next.revision = expected + 1;
            assert!(validate_owner_binding_replacement(&initial, &next, expected).is_err());
        }
        let mut next = initial.clone();
        next.revision = 3;
        assert!(validate_owner_binding_replacement(&initial, &next, 1).is_err());
        let mut maxed = initial.clone();
        maxed.revision = u64::MAX;
        let mut replacement = maxed.clone();
        replacement.revision = u64::MAX;
        assert!(validate_owner_binding_replacement(&maxed, &replacement, u64::MAX).is_err());
    }

    #[test]
    fn identity_and_generation_transitions_are_fenced() {
        let initial = binding();
        let mut changed = initial.clone();
        changed.revision = 2;
        changed.owner_id = "other".into();
        assert!(validate_owner_binding_replacement(&initial, &changed, 1).is_err());
        changed.owner_generation = 2;
        validate_owner_binding_replacement(&initial, &changed, 1).unwrap();
        let mut unchanged = changed.clone();
        unchanged.revision = 3;
        unchanged.owner_generation = 3;
        assert!(validate_owner_binding_replacement(&changed, &unchanged, 2).is_err());
    }

    #[test]
    fn immutable_identity_and_workspace_rules_are_enforced() {
        let initial = binding();
        let mut principal_changed = initial.clone();
        principal_changed.revision = 2;
        principal_changed.principal_id = "other".into();
        assert!(validate_owner_binding_replacement(&initial, &principal_changed, 1).is_err());
        let mut local_changed = initial.clone();
        local_changed.revision = 2;
        local_changed.local_user_id = "other".into();
        assert!(validate_owner_binding_replacement(&initial, &local_changed, 1).is_err());
        for workspaces in [vec!["w".into(), "w".into()], vec!["".into()]] {
            let mut next = initial.clone();
            next.allowed_workspaces = workspaces;
            assert!(validate_owner_binding_for_create(&next).is_err());
        }
    }

    #[test]
    fn permission_withdrawal_is_revision_fenced_and_revoked_create_denied() {
        let mut initial = binding();
        initial.allowed_workspaces.push("x".into());
        let mut withdrawn = initial.clone();
        withdrawn.revision = 2;
        withdrawn.allowed_workspaces = vec!["w".into()];
        validate_owner_binding_replacement(&initial, &withdrawn, 1).unwrap();
        assert!(validate_owner_binding_replacement(&initial, &withdrawn, 2).is_err());
        let mut revoked = initial.clone();
        revoked.revoked_at_unix_secs = Some(1);
        assert!(validate_owner_binding_for_create(&revoked).is_err());
        assert!(validate_owner_binding_replacement(&revoked, &initial, 1).is_err());
    }
}
