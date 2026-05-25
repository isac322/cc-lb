use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use cc_lb_config::{
    Config, DownstreamAuthMode, Limit, LimitKind, NoneModeConfig, NoneModeUpstreamKind,
    PrincipalSpec, PrincipalType,
};
use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::api_keys::secret;
use cc_lb_server::builtins::{AuthnError, BuiltinAuthn};
use cc_lb_storage_redb::{
    IssueParams, KeyStatus, Limit as StorageLimit, LimitKind as StorageLimitKind,
    PrincipalKindLite, Storage, UpstreamKind,
};
use http::{HeaderMap, HeaderValue};

struct Harness {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    key_store: Arc<KeyStore>,
    principal_view: Arc<ArcSwap<PrincipalView>>,
}

impl Harness {
    fn new(principal_enabled: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(Storage::open(
            &dir.path().join("builtin_authn.redb"),
            [31; 32],
        )?);
        let key_store = Arc::new(KeyStore::new(storage.clone()));
        let principal_view = Arc::new(ArcSwap::from(PrincipalView::from_config(&config(
            principal_enabled,
        ))));

        Ok(Self {
            _dir: dir,
            storage,
            key_store,
            principal_view,
        })
    }

    fn authn(&self) -> BuiltinAuthn {
        BuiltinAuthn::new(
            DownstreamAuthMode::ApiKey,
            None,
            self.key_store.clone(),
            self.principal_view.clone(),
        )
    }
}

#[test]
fn valid_key_returns_authn_success() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;
    let (record, plaintext) = harness.key_store.create("u1", create_params(None))?;
    let headers = headers_with_key(plaintext.expose());

    let success = harness.authn().authenticate(&headers)?;

    assert_eq!(success.principal_id, "u1");
    assert_eq!(success.record, record);
    assert_eq!(success.upstream_kind, UpstreamKind::AnthropicKey);
    assert_eq!(success.upstream_credential_ref, "anthropic-prod");
    assert_eq!(success.last_4, record.last_4);
    Ok(())
}

#[test]
fn missing_header_returns_missing_header() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;

    let error = harness.authn().authenticate(&HeaderMap::new()).unwrap_err();

    assert_eq!(error, AuthnError::MissingHeader);
    assert_eq!(error.http_status(), 401);
    Ok(())
}

#[test]
fn bad_format_returns_invalid_format() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;

    let error = harness
        .authn()
        .authenticate(&headers_with_key("not-a-key"))
        .unwrap_err();

    assert_eq!(error, AuthnError::InvalidFormat);
    assert_eq!(error.http_status(), 401);
    Ok(())
}

#[test]
fn unknown_key_returns_not_found() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;
    let generated = secret::generate_new();

    let error = harness
        .authn()
        .authenticate(&headers_with_key(generated.plaintext.expose()))
        .unwrap_err();

    assert_eq!(error, AuthnError::NotFound);
    assert_eq!(error.http_status(), 401);
    Ok(())
}

#[test]
fn tampered_secret_returns_signature_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;
    let generated = secret::generate_new();
    let (key_id, _) = secret::parse(generated.plaintext.expose())?;
    let tampered = tamper_after_separator(generated.plaintext.expose());
    let (_, tampered_secret_bytes) = secret::parse(&tampered)?;

    harness.storage.issue_api_key_record_with_index(
        "u1",
        &key_id,
        IssueParams {
            label: "tampered index".to_owned(),
            description: None,
            upstream_kind: UpstreamKind::AnthropicKey,
            upstream_credential_ref: "anthropic-prod".to_owned(),
            expires_at_unix_secs: Some(future_expiry()),
            limit_overrides: vec![storage_limit()],
            secret_salt: generated.secret_salt,
            verify_hash: generated.verify_hash,
            last_4: generated.last_4,
            principal_kind: PrincipalKindLite::Machine,
            index_hash: secret::compute_index_hash(&tampered_secret_bytes),
        },
    )?;

    let error = harness
        .authn()
        .authenticate(&headers_with_key(&tampered))
        .unwrap_err();

    assert_eq!(error, AuthnError::SignatureMismatch);
    assert_eq!(error.http_status(), 401);
    Ok(())
}

#[test]
fn disabled_key_returns_key_disabled() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;
    let (_record, plaintext) = harness.key_store.create("u1", create_params(None))?;
    let (key_id, _) = secret::parse(plaintext.expose())?;
    harness.key_store.disable("u1", &key_id)?;

    let error = harness
        .authn()
        .authenticate(&headers_with_key(plaintext.expose()))
        .unwrap_err();

    assert_eq!(error, AuthnError::KeyDisabled);
    assert_eq!(error.http_status(), 403);
    Ok(())
}

#[test]
fn expired_key_returns_expired() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;
    let (_record, plaintext) = harness.key_store.create("u1", create_params(Some(0)))?;

    let error = harness
        .authn()
        .authenticate(&headers_with_key(plaintext.expose()))
        .unwrap_err();

    assert_eq!(error, AuthnError::Expired);
    assert_eq!(error.http_status(), 401);
    Ok(())
}

#[test]
fn disabled_principal_returns_principal_disabled() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(false)?;
    let (_record, plaintext) = harness.key_store.create("u1", create_params(None))?;

    let error = harness
        .authn()
        .authenticate(&headers_with_key(plaintext.expose()))
        .unwrap_err();

    assert_eq!(error, AuthnError::PrincipalDisabled);
    assert_eq!(error.http_status(), 403);
    Ok(())
}

#[test]
fn mode_none_returns_authn_success_from_none_mode() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(true)?;
    let authn = BuiltinAuthn::new(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: "anon".to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicOAuth,
            upstream_credential_ref: "oauth-prod".to_owned(),
        }),
        harness.key_store.clone(),
        harness.principal_view.clone(),
    );

    let success = authn
        .authenticate_none_mode()
        .expect("none mode should authenticate");

    assert_eq!(success.principal_id, "anon");
    assert_eq!(success.key_id, "none-mode");
    assert_eq!(success.upstream_kind, UpstreamKind::AnthropicOAuth);
    assert_eq!(success.upstream_credential_ref, "oauth-prod");
    assert_eq!(success.record.status, KeyStatus::Active);
    assert_eq!(success.record.verify_hash, [0; 32]);
    assert_eq!(success.record.secret_salt, [0; 16]);
    assert_eq!(success.last_4, "");
    Ok(())
}

fn config(principal_enabled: bool) -> Config {
    let mut principals = HashMap::new();
    principals.insert(
        "u1".to_owned(),
        PrincipalSpec {
            principal_type: PrincipalType::Machine,
            default_limits: vec![Limit {
                kind: LimitKind::Requests,
                window: Duration::from_secs(60),
                cap_micros: 1_000,
            }],
            enabled: principal_enabled,
            allowed_models: vec!["*".to_owned()],
            credentials_ref: None,
        },
    );

    Config {
        principals,
        ..Config::default()
    }
}

fn create_params(expires_at_unix_secs: Option<u64>) -> CreateParams {
    CreateParams {
        upstream_kind: UpstreamKind::AnthropicKey,
        upstream_credential_ref: "anthropic-prod".to_owned(),
        label: "managed key".to_owned(),
        description: Some("test key".to_owned()),
        expires_at_unix_secs: Some(expires_at_unix_secs.unwrap_or_else(future_expiry)),
        limit_overrides: vec![storage_limit()],
        principal_kind: PrincipalKindLite::Machine,
    }
}

fn storage_limit() -> StorageLimit {
    StorageLimit {
        kind: StorageLimitKind::Requests,
        window_secs: 60,
        cap_micros: 100,
    }
}

fn headers_with_key(api_key: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(api_key).expect("valid header"),
    );
    headers
}

fn future_expiry() -> u64 {
    4_102_444_800
}

fn tamper_after_separator(input: &str) -> String {
    let mut bytes = input.as_bytes().to_vec();
    let index = input.find('_').expect("secret separator") + 1;
    bytes[index] = if bytes[index] == b'A' { b'B' } else { b'A' };
    String::from_utf8(bytes).expect("valid utf-8")
}
