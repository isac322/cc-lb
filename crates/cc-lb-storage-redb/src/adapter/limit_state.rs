use async_trait::async_trait;
use cc_lb_storage_api::{
    LimitStateStore, StorageError as ApiStorageError, StorageResult,
    types::{PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState},
};

use crate::RedbStorage;

fn limit_state_removed() -> ApiStorageError {
    ApiStorageError::Fatal {
        message: "legacy redb limit state store removed; use limit engine".to_owned(),
    }
}

#[async_trait]
impl LimitStateStore for RedbStorage {
    async fn put_principal_limit_state(&self, _state: &PrincipalLimitState) -> StorageResult<()> {
        Err(limit_state_removed())
    }

    async fn get_principal_limit_state(
        &self,
        _principal_id: &str,
        _identity_kind: PrincipalLimitIdentityKind,
        _identity_value: Option<&str>,
        _window: &str,
        _kind: PrincipalLimitKind,
    ) -> StorageResult<Option<PrincipalLimitState>> {
        Err(limit_state_removed())
    }

    async fn list_principal_limit_states(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<PrincipalLimitState>> {
        Err(limit_state_removed())
    }
}
