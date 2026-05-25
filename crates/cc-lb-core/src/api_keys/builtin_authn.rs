use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_storage_redb::{KeyStatus, StoredApiKeyRecord};

use crate::api_keys::{
    key_store::KeyStore,
    principal_view::{PrincipalStatus, PrincipalView},
    secret,
};

#[derive(Clone)]
pub struct BuiltinAuthn {
    mode: DownstreamAuthMode,
    none_mode: Option<NoneModeConfig>,
    key_store: Arc<KeyStore>,
    principal_view: Arc<arc_swap::ArcSwap<PrincipalView>>,
}

#[derive(Debug, Clone)]
pub struct AuthnSuccess {
    pub principal_id: String,
    pub key_id: String,
    pub upstream_kind: cc_lb_storage_redb::UpstreamKind,
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
}

impl BuiltinAuthError {
    pub fn http_status(&self) -> u16 {
        match self {
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
        key_store: Arc<KeyStore>,
        principal_view: Arc<arc_swap::ArcSwap<PrincipalView>>,
    ) -> Self {
        Self {
            mode,
            none_mode,
            key_store,
            principal_view,
        }
    }

    pub fn authenticate(
        &self,
        headers: &http::HeaderMap,
    ) -> Result<AuthnSuccess, BuiltinAuthError> {
        let input = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .ok_or(BuiltinAuthError::MissingHeader)?;
        let (parsed_key_id, secret_bytes) =
            secret::parse(input).map_err(|_| BuiltinAuthError::InvalidFormat)?;
        let index_hash = secret::compute_index_hash(&secret_bytes);
        let (principal_id, key_id_storage, record) = self
            .key_store
            .lookup_by_index_hash(&index_hash)
            .map_err(|_| BuiltinAuthError::NotFound)?
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

        let view = self.principal_view.load();
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

    pub fn authenticate_none_mode(&self) -> Option<AuthnSuccess> {
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

        Some(AuthnSuccess {
            principal_id: none_mode.principal_id.clone(),
            key_id: "none-mode".to_owned(),
            upstream_kind: record.upstream_kind,
            upstream_credential_ref: record.upstream_credential_ref.clone(),
            record,
            last_4: String::new(),
            api_key: None,
        })
    }
}

pub fn map_none_mode_upstream_kind(kind: NoneModeUpstreamKind) -> cc_lb_storage_redb::UpstreamKind {
    match kind {
        NoneModeUpstreamKind::AnthropicKey => cc_lb_storage_redb::UpstreamKind::AnthropicKey,
        NoneModeUpstreamKind::AnthropicOAuth => cc_lb_storage_redb::UpstreamKind::AnthropicOAuth,
        NoneModeUpstreamKind::AwsSigV4 => cc_lb_storage_redb::UpstreamKind::AwsSigV4,
        NoneModeUpstreamKind::GcpOAuth => cc_lb_storage_redb::UpstreamKind::GcpOAuth,
    }
}
