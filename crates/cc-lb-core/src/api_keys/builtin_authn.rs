use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_storage_api::{
    StorageError,
    types::{KeyStatus, StoredApiKeyRecord, UpstreamKind},
};

use crate::api_keys::{
    key_store::{KeyStore, KeyStoreError},
    principal_view::{PrincipalStatus, PrincipalView},
    secret,
};

#[derive(Clone)]
pub struct BuiltinAuthn {
    mode: DownstreamAuthMode,
    none_mode: Option<NoneModeConfig>,
    key_store: Option<Arc<KeyStore>>,
}

#[derive(Debug, Clone)]
pub struct AuthnSuccess {
    pub principal_id: String,
    pub key_id: String,
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub record: StoredApiKeyRecord,
    pub last_4: String,
    pub api_key: Option<String>,
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum BuiltinAuthError {
    #[error("missing x-api-key header")]
    MissingHeader,
    #[error("invalid api key format")]
    InvalidFormat,
    #[error("api key not found")]
    NotFound,
    #[error("api key signature mismatch")]
    SignatureMismatch,
    #[error("api key disabled")]
    KeyDisabled,
    #[error("api key revoked")]
    KeyRevoked,
    #[error("api key expired")]
    Expired,
    #[error("principal not found")]
    PrincipalMissing,
    #[error("principal disabled")]
    PrincipalDisabled,
    #[error("api key storage unavailable")]
    Unavailable,
}

impl BuiltinAuthError {
    pub fn http_status(&self) -> u16 {
        match self {
            Self::Unavailable => 503,
            Self::KeyDisabled | Self::PrincipalDisabled => 403,
            Self::MissingHeader
            | Self::InvalidFormat
            | Self::NotFound
            | Self::SignatureMismatch
            | Self::KeyRevoked
            | Self::Expired
            | Self::PrincipalMissing => 401,
        }
    }
}

impl BuiltinAuthn {
    pub fn new(
        mode: DownstreamAuthMode,
        none_mode: Option<NoneModeConfig>,
        key_store: Option<Arc<KeyStore>>,
    ) -> Self {
        Self {
            mode,
            none_mode,
            key_store,
        }
    }

    pub async fn authenticate(
        &self,
        headers: &http::HeaderMap,
        view: &PrincipalView,
    ) -> Result<AuthnSuccess, BuiltinAuthError> {
        let input = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .ok_or(BuiltinAuthError::MissingHeader)?;
        let (parsed_key_id, secret_bytes) =
            secret::parse(input).map_err(|_| BuiltinAuthError::InvalidFormat)?;
        let index_hash = secret::compute_index_hash(&secret_bytes);
        let Some(key_store) = &self.key_store else {
            return Err(BuiltinAuthError::NotFound);
        };
        let (principal_id, key_id_storage, record) = key_store
            .lookup_by_index_hash(&index_hash)
            .await
            .map_err(map_lookup_error)?
            .ok_or(BuiltinAuthError::NotFound)?;

        if parsed_key_id != key_id_storage {
            return Err(BuiltinAuthError::NotFound);
        }
        if !secret::verify_secret(&secret_bytes, &record.verify_hash, &record.secret_salt) {
            return Err(BuiltinAuthError::SignatureMismatch);
        }
        match record.status {
            KeyStatus::Active => {}
            KeyStatus::Disabled => return Err(BuiltinAuthError::KeyDisabled),
            KeyStatus::Revoked => return Err(BuiltinAuthError::KeyRevoked),
        }
        if let Some(expires_at_unix_secs) = record.expires_at_unix_secs {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now > expires_at_unix_secs {
                return Err(BuiltinAuthError::Expired);
            }
        }

        view.get(&principal_id)
            .ok_or(BuiltinAuthError::PrincipalMissing)?;
        if view.principal_status(&principal_id) != PrincipalStatus::Active {
            return Err(BuiltinAuthError::PrincipalDisabled);
        }

        Ok(AuthnSuccess {
            principal_id,
            key_id: key_id_storage,
            upstream_kind: record.upstream_kind,
            upstream_credential_ref: record.upstream_credential_ref.clone(),
            record: record.clone(),
            last_4: record.last_4.clone(),
            api_key: Some(input.to_owned()),
        })
    }

    pub async fn authenticate_none_mode(&self, headers: &http::HeaderMap) -> Option<AuthnSuccess> {
        if self.mode != DownstreamAuthMode::None {
            return None;
        }

        let none_mode = self.none_mode.as_ref()?;
        let record = StoredApiKeyRecord {
            status: KeyStatus::Active,
            upstream_kind: map_none_mode_upstream_kind(none_mode.upstream_kind.clone()),
            upstream_credential_ref: none_mode.upstream_credential_ref.clone(),
            verify_hash: [0; 32],
            secret_salt: [0; 16],
            last_4: String::new(),
            ..Default::default()
        };

        let api_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_owned());

        Some(AuthnSuccess {
            principal_id: none_mode.principal_id.clone(),
            key_id: "none-mode".to_owned(),
            upstream_kind: record.upstream_kind,
            upstream_credential_ref: record.upstream_credential_ref.clone(),
            record,
            last_4: String::new(),
            api_key,
        })
    }
}

fn map_lookup_error(error: KeyStoreError) -> BuiltinAuthError {
    match error {
        KeyStoreError::Storage(
            StorageError::Unavailable { .. } | StorageError::Transient { .. },
        ) => BuiltinAuthError::Unavailable,
        KeyStoreError::Storage(_) | KeyStoreError::KeyAlreadyRevoked { .. } => {
            BuiltinAuthError::NotFound
        }
    }
}

pub fn map_none_mode_upstream_kind(kind: NoneModeUpstreamKind) -> UpstreamKind {
    match kind {
        NoneModeUpstreamKind::AnthropicKey => UpstreamKind::AnthropicKey,
        NoneModeUpstreamKind::AnthropicOAuth => UpstreamKind::AnthropicOAuth,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use cc_lb_storage_api::{
        ManagedKeyStore, StorageResult,
        types::{ApiKeyMutation, IssueParams, PrincipalKindLite},
    };
    use http::{HeaderMap, HeaderValue};

    use super::*;

    #[tokio::test]
    async fn authenticates_valid_key() {
        let generated = secret::generate_new();
        let record = active_record(&generated);
        let (authn, store) = api_key_authn(
            LookupAction::Return(Box::new(Some((
                "principal-1".to_owned(),
                generated.key_id.clone(),
                record.clone(),
            )))),
            true,
        );

        let success = authn
            .authenticate(
                &headers(generated.plaintext.expose()),
                &principal_view(true),
            )
            .await
            .expect("generated key authenticates");

        assert_eq!(success.principal_id, "principal-1");
        assert_eq!(success.key_id, generated.key_id);
        assert_eq!(success.upstream_kind, UpstreamKind::AnthropicKey);
        assert_eq!(success.upstream_credential_ref, "anthropic-prod");
        assert_eq!(success.last_4, generated.last_4);
        assert_eq!(
            success.api_key.as_deref(),
            Some(generated.plaintext.expose())
        );
        assert_eq!(
            *store.seen_index_hash.lock().unwrap(),
            Some(record.index_hash)
        );
    }

    #[tokio::test]
    async fn storage_unavailable_returns_503() {
        let generated = secret::generate_new();
        let (authn, _store) = api_key_authn(LookupAction::Unavailable, true);

        let error = authn_error(
            authn
                .authenticate(
                    &headers(generated.plaintext.expose()),
                    &principal_view(true),
                )
                .await,
        );

        assert_eq!(error, BuiltinAuthError::Unavailable);
        assert_eq!(error.http_status(), 503);
    }

    #[tokio::test]
    async fn storage_transient_returns_503() {
        let generated = secret::generate_new();
        let (authn, _store) = api_key_authn(LookupAction::Transient, true);

        let error = authn_error(
            authn
                .authenticate(
                    &headers(generated.plaintext.expose()),
                    &principal_view(true),
                )
                .await,
        );

        assert_eq!(error, BuiltinAuthError::Unavailable);
        assert_eq!(error.http_status(), 503);
    }

    #[tokio::test]
    async fn missing_lookup_stays_401_not_found() {
        let generated = secret::generate_new();
        let (authn, _store) = api_key_authn(LookupAction::Return(Box::new(None)), true);

        let error = authn_error(
            authn
                .authenticate(
                    &headers(generated.plaintext.expose()),
                    &principal_view(true),
                )
                .await,
        );

        assert_eq!(error, BuiltinAuthError::NotFound);
        assert_eq!(error.http_status(), 401);
    }

    #[tokio::test]
    async fn authenticate_none_mode_is_async() {
        let store = Arc::new(StubManagedKeyStore::new(LookupAction::Return(Box::new(
            None,
        ))));
        let authn = BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "principal-none".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicOAuth,
                upstream_credential_ref: "oauth-ref".to_owned(),
            }),
            Some(Arc::new(KeyStore::new(store))),
        );

        let success = authn
            .authenticate_none_mode(&headers("passthrough-key"))
            .await
            .expect("none mode authenticates without storage lookup");

        assert_eq!(success.principal_id, "principal-none");
        assert_eq!(success.key_id, "none-mode");
        assert_eq!(success.upstream_kind, UpstreamKind::AnthropicOAuth);
        assert_eq!(success.upstream_credential_ref, "oauth-ref");
        assert_eq!(success.api_key.as_deref(), Some("passthrough-key"));
    }

    fn authn_error(result: Result<AuthnSuccess, BuiltinAuthError>) -> BuiltinAuthError {
        match result {
            Ok(_) => panic!("authentication unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    fn api_key_authn(
        lookup: LookupAction,
        _principal_enabled: bool,
    ) -> (BuiltinAuthn, Arc<StubManagedKeyStore>) {
        let store = Arc::new(StubManagedKeyStore::new(lookup));
        let authn = BuiltinAuthn::new(
            DownstreamAuthMode::ApiKey,
            None,
            Some(Arc::new(KeyStore::new(store.clone()))),
        );
        (authn, store)
    }

    fn principal_view(enabled: bool) -> PrincipalView {
        PrincipalView::for_tests(
            "principal-1",
            enabled,
            Vec::new(),
            Vec::new(),
            HashMap::new(),
        )
    }

    fn headers(api_key: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(api_key).expect("test api key is a valid header value"),
        );
        headers
    }

    fn active_record(generated: &secret::NewKeyOutput) -> StoredApiKeyRecord {
        StoredApiKeyRecord {
            key_hash_b64: generated.key_id.clone(),
            verify_hash: generated.verify_hash,
            secret_salt: generated.secret_salt,
            upstream_kind: UpstreamKind::AnthropicKey,
            upstream_credential_ref: "anthropic-prod".to_owned(),
            status: KeyStatus::Active,
            expires_at_unix_secs: Some(4_102_444_800),
            last_4: generated.last_4.clone(),
            principal_kind: PrincipalKindLite::Machine,
            index_hash: generated.index_hash,
            ..StoredApiKeyRecord::default()
        }
    }

    #[allow(clippy::large_enum_variant)]
    enum LookupAction {
        Return(Box<Option<(String, String, StoredApiKeyRecord)>>),
        Unavailable,
        Transient,
    }

    struct StubManagedKeyStore {
        lookup: Mutex<Option<LookupAction>>,
        seen_index_hash: Mutex<Option<[u8; 32]>>,
    }

    impl StubManagedKeyStore {
        fn new(lookup: LookupAction) -> Self {
            Self {
                lookup: Mutex::new(Some(lookup)),
                seen_index_hash: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl ManagedKeyStore for StubManagedKeyStore {
        async fn issue(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _params: IssueParams,
        ) -> StorageResult<StoredApiKeyRecord> {
            Err(unused_store_method("issue"))
        }

        async fn get(
            &self,
            _principal_id: &str,
            _key_id: &str,
        ) -> StorageResult<Option<StoredApiKeyRecord>> {
            Err(unused_store_method("get"))
        }

        async fn lookup_by_index_hash(
            &self,
            index_hash: &[u8; 32],
        ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
            *self.seen_index_hash.lock().unwrap() = Some(*index_hash);
            match self
                .lookup
                .lock()
                .unwrap()
                .take()
                .expect("lookup action is configured")
            {
                LookupAction::Return(result) => Ok(*result),
                LookupAction::Unavailable => Err(StorageError::Unavailable {
                    message: "database temporarily unavailable".to_owned(),
                }),
                LookupAction::Transient => Err(StorageError::Transient {
                    retryable: false,
                    source: Box::new(io::Error::new(io::ErrorKind::TimedOut, "timeout")),
                }),
            }
        }

        async fn list_by_principal(
            &self,
            _principal_id: &str,
        ) -> StorageResult<Vec<StoredApiKeyRecord>> {
            Err(unused_store_method("list_by_principal"))
        }

        async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
            Err(unused_store_method("list_all"))
        }

        async fn update(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _mutation: ApiKeyMutation,
        ) -> StorageResult<()> {
            Err(unused_store_method("update"))
        }

        async fn revoke_zero_secrets(
            &self,
            _principal_id: &str,
            _key_id: &str,
        ) -> StorageResult<()> {
            Err(unused_store_method("revoke_zero_secrets"))
        }
    }

    fn unused_store_method(method: &str) -> StorageError {
        StorageError::Fatal {
            message: format!("{method} is not used by builtin authn tests"),
        }
    }
}
