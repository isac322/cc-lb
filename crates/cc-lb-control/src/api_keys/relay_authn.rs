//! Header-only verification of local remote-control capabilities.

use std::sync::Arc;

use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_storage_api::codex_sessions::{
    CredentialPurpose, RelayTargetSnapshot, SessionStoreError,
};
use cc_lb_storage_api::{KeyStatus, RelayEnrollmentStore};

use super::{
    builtin_authn::{AuthnSuccess, BuiltinAuthError, RelayCredentialVerifier},
    principal_view::{PrincipalStatus, PrincipalView},
    secret,
    session_token::{self, Purpose},
};

pub struct StoredRelayVerifier {
    store: Arc<dyn RelayEnrollmentStore>,
    clock: ClockHandle,
}

impl StoredRelayVerifier {
    pub fn new(store: Arc<dyn RelayEnrollmentStore>, clock: ClockHandle) -> Self {
        Self { store, clock }
    }
}

#[async_trait::async_trait]
impl RelayCredentialVerifier for StoredRelayVerifier {
    async fn authenticate_relay(
        &self,
        input: &str,
        view: &PrincipalView,
    ) -> Result<AuthnSuccess, BuiltinAuthError> {
        if !session_token::validate(input, Purpose::Relay) {
            return Err(BuiltinAuthError::InvalidFormat);
        }
        let record = self
            .store
            .lookup_relay_credential(&secret::compute_index_hash(input.as_bytes()))
            .await
            .map_err(|error| match error {
                SessionStoreError::PrincipalDisabled => BuiltinAuthError::PrincipalDisabled,
                SessionStoreError::Storage(_) => BuiltinAuthError::Unavailable,
                _ => BuiltinAuthError::KeyRevoked,
            })?
            .ok_or(BuiltinAuthError::NotFound)?;
        let credential = record.credential;
        if !session_token::verify(
            input,
            Purpose::Relay,
            &credential.verify_hash,
            &credential.salt,
        ) {
            return Err(BuiltinAuthError::SignatureMismatch);
        }
        let family = record.family;
        let snapshot = record.snapshot;
        let now = unix_secs(self.clock.now());
        if snapshot.purpose != CredentialPurpose::RemoteRelay
            || snapshot.credential_id != credential.credential_id
            || snapshot.family_id != credential.identity.family_id
            || snapshot.family_id != family.family_id
            || snapshot.parent_key_id != family.parent_key_id
            || snapshot.owner_binding_id != family.owner_binding_id
            || snapshot.provider != family.provider
            || snapshot.owner_generation != family.owner_generation
            || snapshot.binding_revision != family.binding_revision
            || snapshot.workspace_id != family.workspace_id
            || snapshot.workspace_generation != family.workspace_generation
            || snapshot.origin != family.origin
            || snapshot.access_generation == 0
            || snapshot.access_generation > family.access_generation
            || now < credential.issued_at_unix_secs
            || now < family.created_at
            || now >= credential.expires_at_unix_secs
            || now >= family.expires_at
            || credential.expires_at_unix_secs > family.expires_at
            || family.revoked_at.is_some()
            || record.parent_key.status != KeyStatus::Active
            || record.parent_key.revoked_at_unix_secs.is_some()
            || record
                .parent_key
                .expires_at_unix_secs
                .is_some_and(|expiry| now > expiry)
        {
            return Err(BuiltinAuthError::KeyRevoked);
        }
        view.get(&family.principal_id)
            .ok_or(BuiltinAuthError::PrincipalMissing)?;
        if !record.principal_enabled
            || view.principal_status(&family.principal_id) != PrincipalStatus::Active
        {
            return Err(BuiltinAuthError::PrincipalDisabled);
        }
        Ok(AuthnSuccess {
            principal_id: family.principal_id,
            key_id: family.parent_key_id,
            last_4: record.parent_key.last_4.clone(),
            record: record.parent_key,
            api_key: None,
            session: Some(snapshot),
            relay: Some(RelayTargetSnapshot {
                enrollment_id: credential.enrollment_id,
                server_id: credential.identity.server_id,
                environment_id: credential.identity.environment_id,
                installation_id: credential.identity.installation_id,
            }),
        })
    }
}

#[cfg(test)]
#[path = "relay_authn_tests.rs"]
mod tests;
