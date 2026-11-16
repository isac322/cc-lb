use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_lb_storage_api::codex_sessions::{
    CodexSessionStore, CredentialPurpose, IssueSession, ReplaceSessionAccess, SessionAuthRecord,
    SessionCredential, SessionFamily, SessionSnapshot, SessionStoreError, SessionStoreResult,
};
use cc_lb_storage_api::{KeyStatus, StoredApiKeyRecord};
use http::{HeaderMap, HeaderName, HeaderValue};

use super::*;
struct FixtureStore {
    record: Mutex<Option<SessionAuthRecord>>,
    unavailable: Mutex<bool>,
}

impl FixtureStore {
    fn new(record: SessionAuthRecord) -> Self {
        Self { record: Mutex::new(Some(record)), unavailable: Mutex::new(false) }
    }
}

#[async_trait]
impl CodexSessionStore for FixtureStore {
    async fn lookup_session_credential(&self, index_hash: &[u8; 32]) -> SessionStoreResult<Option<SessionAuthRecord>> {
        if *self.unavailable.lock().unwrap() {
            return Err(SessionStoreError::Storage(cc_lb_storage_api::StorageError::Unavailable { message: "test unavailable".into() }));
        }
        let record = self.record.lock().unwrap();
        Ok(record.as_ref().filter(|r| &r.credential.index_hash == index_hash).map(|r| SessionAuthRecord {
            credential: r.credential.clone(),
            family: r.family.clone(),
            parent_key: r.parent_key.clone(),
            principal_enabled: r.principal_enabled,
        }))
    }

    async fn issue_session(&self, command: IssueSession) -> SessionStoreResult<()> {
        let mut record = self.record.lock().unwrap();
        let parent_key = record.as_ref().map(|r| r.parent_key.clone()).unwrap_or_default();
        let principal_enabled = record.as_ref().map(|r| r.principal_enabled).unwrap_or(true);
        *record = Some(SessionAuthRecord {
            credential: command.access,
            family: command.family,
            parent_key,
            principal_enabled,
        });
        Ok(())
    }

    async fn replace_session_access(&self, command: ReplaceSessionAccess, _now: u64) -> SessionStoreResult<SessionFamily> {
        let mut record = self.record.lock().unwrap();
        let current = record.as_mut().ok_or(SessionStoreError::CredentialRejected)?;
        if current.family != command.expected || current.credential.credential_id != command.authorizing_credential_id {
            return Err(SessionStoreError::OwnerConflict);
        }
        current.credential = command.access;
        Ok(current.family.clone())
    }

    async fn get_session_family(&self, family_id: &str) -> SessionStoreResult<Option<SessionFamily>> {
        Ok(self.record.lock().unwrap().as_ref().map(|r| r.family.clone()).filter(|f| f.family_id == family_id))
    }

    async fn revoke_session_family(&self, family_id: &str, _renewal_credential_id: &str, now: u64) -> SessionStoreResult<()> {
        let mut record = self.record.lock().unwrap();
        let current = record.as_mut().ok_or(SessionStoreError::CredentialRejected)?;
        if current.family.family_id != family_id {
            return Err(SessionStoreError::CredentialRejected);
        }
        current.family.revoked_at = Some(now);
        current.credential.revoked_at = Some(now);
        Ok(())
    }

    async fn fence_session_dispatch(&self, principal_id: &str, snapshot: &SessionSnapshot, _now: u64) -> SessionStoreResult<()> {
        let record = self.record.lock().unwrap();
        let current = record.as_ref().ok_or(SessionStoreError::CredentialRejected)?;
        if current.family.principal_id != principal_id
            || current.family.family_id != snapshot.family_id
            || current.family.parent_key_id != snapshot.parent_key_id
            || current.credential.credential_id != snapshot.credential_id
        {
            return Err(SessionStoreError::CredentialRejected);
        }
        Ok(())
    }
}

fn session_fixture(token: &session_token::MintedToken, purpose: CredentialPurpose) -> SessionAuthRecord {
    let family = SessionFamily {
        family_id: "family-1".into(),
        principal_id: "principal-1".into(),
        parent_key_id: "parent-1".into(),
        owner_binding_id: "binding-1".into(),
        provider: "openai".into(),
        owner_generation: 1,
        binding_revision: 1,
        workspace_id: "workspace-1".into(),
        workspace_generation: 1,
        access_generation: 1,
        local_user_id: "user-1".into(),
        plan_type: "plus".into(),
        origin: "https://chatgpt.com".into(),
        access_ttl_secs: 200,
        created_at: 100,
        expires_at: 500,
        revoked_at: None,
    };
    let credential = SessionCredential {
        credential_id: "access-1".into(),
        family_id: family.family_id.clone(),
        purpose,
        index_hash: token.index_hash,
        verify_hash: token.verify_hash,
        salt: token.secret_salt,
        workspace_generation: 1,
        access_generation: 1,
        issued_at: 100,
        expires_at: 300,
        revoked_at: None,
    };
    SessionAuthRecord {
        credential,
        family,
        parent_key: StoredApiKeyRecord {
            key_hash_b64: "parent-1".into(),
            status: KeyStatus::Active,
            expires_at_unix_secs: Some(200),
            last_4: "1234".into(),
            ..StoredApiKeyRecord::default()
        },
        principal_enabled: true,
    }
}
fn authn_with_session(token: &session_token::MintedToken, purpose: CredentialPurpose) -> (BuiltinAuthn, Arc<FixtureStore>) {
    authn_with_session_at(token, purpose, 100)
}

fn authn_with_session_at(token: &session_token::MintedToken, purpose: CredentialPurpose, now: u64) -> (BuiltinAuthn, Arc<FixtureStore>) {
    let store = Arc::new(FixtureStore::new(session_fixture(token, purpose)));
    let (authn, _) = api_key_authn(LookupAction::Return(Box::new(None)));
    let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(now));
    let authn = BuiltinAuthn::new(authn.key_store.clone(), clock).with_session_store(store.clone());
    (authn, store)
}

fn header(name: &str, value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_bytes(name.as_bytes()).unwrap(), HeaderValue::from_str(value).unwrap());
    headers
}

#[tokio::test]
async fn access_session_authenticates_without_returning_raw_bearer() {
    let token = session_token::mint_access(&session_token::Claims {
        issuer: "https://chatgpt.com".into(), local_user_id: "user-1".into(), workspace_id: "workspace-1".into(),
        family_id: "family-1".into(), access_id: "access-1".into(), issued_at: 100, expires_at: 300, plan: "plus".into(),
    }).unwrap();
    let bearer = token.plaintext.expose().to_owned();
    let (authn, _) = authn_with_session(&token, CredentialPurpose::SessionAccess);
    let success = authn.authenticate(&header("authorization", &format!("Bearer {bearer}")), &principal_view(true)).await.unwrap();
    assert_eq!(success.principal_id, "principal-1");
    assert_eq!(success.key_id, "parent-1");
    assert_eq!(success.record.status, KeyStatus::Active);
    assert_eq!(success.record.expires_at_unix_secs, Some(200));
    assert_eq!(success.last_4, "1234");
    assert_eq!(success.api_key, None);
    assert_eq!(success.session.as_ref().unwrap().purpose, CredentialPurpose::SessionAccess);
    assert_eq!(success.session.as_ref().unwrap().workspace_id, "workspace-1");
    let (authn, _) = authn_with_session_at(&token, CredentialPurpose::SessionAccess, 200);
    assert!(authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(true)).await.is_ok());
    let (authn, _) = authn_with_session_at(&token, CredentialPurpose::SessionAccess, 201);
    assert_eq!(authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(true)).await.unwrap_err(), BuiltinAuthError::KeyRevoked);
}

#[tokio::test]
async fn invalid_payload_and_wrong_purpose_are_rejected() {
    let token = session_token::mint_access(&session_token::Claims {
        issuer: "https://chatgpt.com".into(), local_user_id: "user-1".into(), workspace_id: "workspace-1".into(),
        family_id: "family-1".into(), access_id: "access-1".into(), issued_at: 100, expires_at: 300, plan: "plus".into(),
    }).unwrap();
    let mut bytes = token.plaintext.expose().as_bytes().to_vec();
    let pivot = bytes.iter().position(|b| *b == b'.').unwrap() + 2;
    bytes[pivot] = if bytes[pivot] == b'A' { b'B' } else { b'A' };
    let mutated = String::from_utf8(bytes).unwrap();
    let (authn, _) = authn_with_session(&token, CredentialPurpose::SessionRenewal);
    assert_eq!(authn.authenticate(&header("x-api-key", &mutated), &principal_view(true)).await.unwrap_err(), BuiltinAuthError::NotFound);
    assert_eq!(authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(true)).await.unwrap_err(), BuiltinAuthError::SignatureMismatch);
}

#[tokio::test]
async fn family_revocation_disabled_principal_and_expiry_are_enforced() {
    let token = session_token::mint_access(&session_token::Claims {
        issuer: "https://chatgpt.com".into(), local_user_id: "user-1".into(), workspace_id: "workspace-1".into(),
        family_id: "family-1".into(), access_id: "access-1".into(), issued_at: 100, expires_at: 300, plan: "plus".into(),
    }).unwrap();
    let (authn, store) = authn_with_session(&token, CredentialPurpose::SessionAccess);
    store.revoke_session_family("family-1", "renewal-1", 101).await.unwrap();
    assert_eq!(authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(true)).await.unwrap_err(), BuiltinAuthError::KeyRevoked);
    let (authn, _) = authn_with_session(&token, CredentialPurpose::SessionAccess);
    assert_eq!(authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(false)).await.unwrap_err().http_status(), 403);
}

#[tokio::test]
async fn relay_requires_remote_relay_snapshot_and_parent_identity() {
    let relay = session_token::mint_relay();
    let mut success = AuthnSuccess {
        principal_id: "principal-1".into(), key_id: "parent-1".into(), record: StoredApiKeyRecord::default(),
        last_4: "1234".into(), api_key: None, session: Some(SessionSnapshot {
            family_id: "family-1".into(), parent_key_id: "parent-1".into(), credential_id: "relay-1".into(),
            purpose: CredentialPurpose::RemoteRelay, owner_binding_id: "binding-1".into(), provider: "openai".into(),
            owner_generation: 1, binding_revision: 1, workspace_id: "workspace-1".into(), workspace_generation: 1,
            access_generation: 1, origin: "https://chatgpt.com".into(),
        }),
    };
    struct Verifier(Mutex<Option<AuthnSuccess>>);
    #[async_trait]
    impl RelayCredentialVerifier for Verifier {
        async fn authenticate_relay(&self, _credential: &str, _view: &PrincipalView) -> Result<AuthnSuccess, BuiltinAuthError> {
            Ok(self.0.lock().unwrap().take().unwrap())
        }
    }
    let (base, _) = api_key_authn(LookupAction::Return(Box::new(None)));
    let authn = base.clone().with_relay_verifier(Arc::new(Verifier(Mutex::new(Some(success.clone())))));
    assert!(authn.authenticate(&header("x-api-key", relay.plaintext.expose()), &principal_view(true)).await.is_ok());
    success.session.as_mut().unwrap().purpose = CredentialPurpose::SessionAccess;
    let authn = base.with_relay_verifier(Arc::new(Verifier(Mutex::new(Some(success)))));
    assert_eq!(authn.authenticate(&header("x-api-key", relay.plaintext.expose()), &principal_view(true)).await.unwrap_err(), BuiltinAuthError::InvalidFormat);
}

#[tokio::test]
async fn header_selection_and_expiry_boundaries_are_preserved() {
    let token = session_token::mint_access(&session_token::Claims {
        issuer: "https://chatgpt.com".into(), local_user_id: "user-1".into(), workspace_id: "workspace-1".into(),
        family_id: "family-1".into(), access_id: "access-1".into(), issued_at: 100, expires_at: 300, plan: "plus".into(),
    }).unwrap();
    let (authn, _) = authn_with_session_at(&token, CredentialPurpose::SessionAccess, 100);
    let mut headers = header("x-api-key", "not-a-session-token");
    headers.insert("authorization", HeaderValue::from_str(&format!("Bearer {}", token.plaintext.expose())).unwrap());
    assert_eq!(authn.authenticate(&headers, &principal_view(true)).await.unwrap_err(), BuiltinAuthError::InvalidFormat);
    let (authn, store) = authn_with_session_at(&token, CredentialPurpose::SessionAccess, 300);
    store.record.lock().unwrap().as_mut().unwrap().parent_key.expires_at_unix_secs = Some(500);
    assert_eq!(authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(true)).await.unwrap_err(), BuiltinAuthError::SessionAccessExpired);
}

#[tokio::test]
async fn session_storage_unavailable_maps_to_503() {
    let token = session_token::mint_access(&session_token::Claims {
        issuer: "https://chatgpt.com".into(), local_user_id: "user-1".into(), workspace_id: "workspace-1".into(),
        family_id: "family-1".into(), access_id: "access-1".into(), issued_at: 100, expires_at: 300, plan: "plus".into(),
    }).unwrap();
    let (authn, store) = authn_with_session(&token, CredentialPurpose::SessionAccess);
    *store.unavailable.lock().unwrap() = true;
    let error = authn.authenticate(&header("x-api-key", token.plaintext.expose()), &principal_view(true)).await.unwrap_err();
    assert_eq!(error, BuiltinAuthError::Unavailable);
    assert_eq!(error.http_status(), 503);
}
