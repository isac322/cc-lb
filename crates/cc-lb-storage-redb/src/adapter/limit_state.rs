use async_trait::async_trait;
use cc_lb_storage_api::{
    LimitStateStore, StorageResult,
    types::{
        PrincipalLimitIdentityKind as ApiPrincipalLimitIdentityKind,
        PrincipalLimitKind as ApiPrincipalLimitKind, PrincipalLimitState as ApiPrincipalLimitState,
    },
};

use crate::{
    PrincipalLimitIdentityKind as RedbPrincipalLimitIdentityKind,
    PrincipalLimitKind as RedbPrincipalLimitKind, PrincipalLimitState as RedbPrincipalLimitState,
    RedbStorage,
};

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl LimitStateStore for RedbStorage {
    async fn put_principal_limit_state(&self, state: &ApiPrincipalLimitState) -> StorageResult<()> {
        let storage = self.clone();
        let state = to_redb_principal_limit_state(state);

        tokio::task::spawn_blocking(move || {
            RedbStorage::put_principal_limit_state(&storage, &state)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_principal_limit_state(
        &self,
        principal_id: &str,
        identity_kind: ApiPrincipalLimitIdentityKind,
        identity_value: Option<&str>,
        window: &str,
        kind: ApiPrincipalLimitKind,
    ) -> StorageResult<Option<ApiPrincipalLimitState>> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let identity_kind = to_redb_principal_limit_identity_kind(identity_kind);
        let identity_value = identity_value.map(str::to_owned);
        let window = window.to_owned();
        let kind = to_redb_principal_limit_kind(kind);

        tokio::task::spawn_blocking(move || {
            RedbStorage::get_principal_limit_state(
                &storage,
                &principal_id,
                identity_kind,
                identity_value.as_deref(),
                &window,
                kind,
            )
        })
        .await
        .map_err(map_join_err)?
        .map(|state| state.map(to_api_principal_limit_state))
        .map_err(map_redb_err)
    }

    async fn list_principal_limit_states(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<ApiPrincipalLimitState>> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();

        tokio::task::spawn_blocking(move || {
            RedbStorage::list_principal_limit_states(&storage, &principal_id)
        })
        .await
        .map_err(map_join_err)?
        .map(|states| {
            states
                .into_iter()
                .map(to_api_principal_limit_state)
                .collect()
        })
        .map_err(map_redb_err)
    }
}

fn to_redb_principal_limit_state(state: &ApiPrincipalLimitState) -> RedbPrincipalLimitState {
    RedbPrincipalLimitState {
        principal_id: state.principal_id.clone(),
        identity_kind: to_redb_principal_limit_identity_kind(state.identity_kind),
        identity_value: state.identity_value.clone(),
        account_observed: state.account_observed,
        window: state.window.clone(),
        kind: to_redb_principal_limit_kind(state.kind),
        limit: state.limit,
        remaining: state.remaining,
        reset: state.reset.clone(),
        observed_at_unix_secs: state.observed_at_unix_secs,
        stored_at_unix_secs: state.stored_at_unix_secs,
    }
}

fn to_api_principal_limit_state(state: RedbPrincipalLimitState) -> ApiPrincipalLimitState {
    ApiPrincipalLimitState {
        principal_id: state.principal_id,
        identity_kind: to_api_principal_limit_identity_kind(state.identity_kind),
        identity_value: state.identity_value,
        account_observed: state.account_observed,
        window: state.window,
        kind: to_api_principal_limit_kind(state.kind),
        limit: state.limit,
        remaining: state.remaining,
        reset: state.reset,
        observed_at_unix_secs: state.observed_at_unix_secs,
        stored_at_unix_secs: state.stored_at_unix_secs,
    }
}

fn to_redb_principal_limit_kind(kind: ApiPrincipalLimitKind) -> RedbPrincipalLimitKind {
    match kind {
        ApiPrincipalLimitKind::Requests => RedbPrincipalLimitKind::Requests,
        ApiPrincipalLimitKind::Tokens => RedbPrincipalLimitKind::Tokens,
        ApiPrincipalLimitKind::InputTokens => RedbPrincipalLimitKind::InputTokens,
        ApiPrincipalLimitKind::OutputTokens => RedbPrincipalLimitKind::OutputTokens,
    }
}

fn to_api_principal_limit_kind(kind: RedbPrincipalLimitKind) -> ApiPrincipalLimitKind {
    match kind {
        RedbPrincipalLimitKind::Requests => ApiPrincipalLimitKind::Requests,
        RedbPrincipalLimitKind::Tokens => ApiPrincipalLimitKind::Tokens,
        RedbPrincipalLimitKind::InputTokens => ApiPrincipalLimitKind::InputTokens,
        RedbPrincipalLimitKind::OutputTokens => ApiPrincipalLimitKind::OutputTokens,
    }
}

fn to_redb_principal_limit_identity_kind(
    kind: ApiPrincipalLimitIdentityKind,
) -> RedbPrincipalLimitIdentityKind {
    match kind {
        ApiPrincipalLimitIdentityKind::Account => RedbPrincipalLimitIdentityKind::Account,
        ApiPrincipalLimitIdentityKind::Credential => RedbPrincipalLimitIdentityKind::Credential,
        ApiPrincipalLimitIdentityKind::Unobserved => RedbPrincipalLimitIdentityKind::Unobserved,
    }
}

fn to_api_principal_limit_identity_kind(
    kind: RedbPrincipalLimitIdentityKind,
) -> ApiPrincipalLimitIdentityKind {
    match kind {
        RedbPrincipalLimitIdentityKind::Account => ApiPrincipalLimitIdentityKind::Account,
        RedbPrincipalLimitIdentityKind::Credential => ApiPrincipalLimitIdentityKind::Credential,
        RedbPrincipalLimitIdentityKind::Unobserved => ApiPrincipalLimitIdentityKind::Unobserved,
    }
}
