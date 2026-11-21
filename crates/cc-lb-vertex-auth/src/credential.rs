use base64::{Engine as _, engine::general_purpose::STANDARD};
use cc_lb_domain::vertex::{VertexCredentialType, validate_project_id};
use ring::signature::RsaKeyPair;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::Value;
use std::fmt;
use zeroize::Zeroizing;

pub const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;
pub const GOOGLE_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CredentialError {
    TooLarge,
    InvalidJson,
    MissingType,
    UnsupportedType,
    ForbiddenField,
    InvalidUniverseDomain,
    InvalidTokenUri,
    InvalidServiceAccountEmail,
    InvalidPrivateKey,
    InvalidPrivateKeyId,
    InvalidProjectId,
    InvalidSecretField,
    InvalidImpersonationUrl,
    NestedImpersonation,
    InvalidSourceCredentials,
    InvalidDelegate,
    TooManyDelegates,
}

impl CredentialError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::TooLarge => "credential_too_large",
            Self::InvalidJson => "credential_invalid_json",
            Self::MissingType => "credential_missing_type",
            Self::UnsupportedType => "credential_unsupported_type",
            Self::ForbiddenField => "credential_forbidden_field",
            Self::InvalidUniverseDomain => "credential_invalid_universe_domain",
            Self::InvalidTokenUri => "credential_invalid_token_uri",
            Self::InvalidServiceAccountEmail => "credential_invalid_service_account_email",
            Self::InvalidPrivateKey => "credential_invalid_private_key",
            Self::InvalidPrivateKeyId => "credential_invalid_private_key_id",
            Self::InvalidProjectId => "credential_invalid_project_id",
            Self::InvalidSecretField => "credential_invalid_secret",
            Self::InvalidImpersonationUrl => "credential_invalid_impersonation_url",
            Self::NestedImpersonation => "credential_nested_impersonation",
            Self::InvalidSourceCredentials => "credential_invalid_source",
            Self::InvalidDelegate => "credential_invalid_delegate",
            Self::TooManyDelegates => "credential_too_many_delegates",
        }
    }
}
impl fmt::Debug for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for CredentialError {}

#[derive(Clone)]
pub struct ServiceAccountCredential {
    pub client_email: String,
    pub private_key_id: Option<String>,
    private_key: Zeroizing<Vec<u8>>,
    pub project_id: Option<String>,
}
#[derive(Clone)]
pub struct AuthorizedUserCredential {
    pub client_id: String,
    pub(crate) client_secret: Zeroizing<String>,
    pub(crate) refresh_token: Zeroizing<String>,
    pub quota_project_id: Option<String>,
}
#[derive(Clone)]
pub struct ImpersonatedServiceAccountCredential {
    pub target_email: String,
    pub source_credentials: Box<CanonicalCredential>,
    pub delegates: Vec<String>,
    pub quota_project_id: Option<String>,
}

#[derive(Clone)]
pub enum CanonicalCredential {
    ServiceAccount(ServiceAccountCredential),
    AuthorizedUser(AuthorizedUserCredential),
    ImpersonatedServiceAccount(ImpersonatedServiceAccountCredential),
}

impl fmt::Debug for ServiceAccountCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAccountCredential")
            .field("client_email", &self.client_email)
            .field("private_key_id", &self.private_key_id)
            .field("private_key", &"[REDACTED]")
            .field("project_id", &self.project_id)
            .finish()
    }
}
impl fmt::Debug for AuthorizedUserCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizedUserCredential")
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .field("quota_project_id", &self.quota_project_id)
            .finish()
    }
}
impl fmt::Debug for ImpersonatedServiceAccountCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImpersonatedServiceAccountCredential")
            .field("target_email", &self.target_email)
            .field("source_credentials", &self.source_credentials)
            .field("delegates", &self.delegates)
            .field("quota_project_id", &self.quota_project_id)
            .finish()
    }
}
impl fmt::Debug for CanonicalCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceAccount(v) => v.fmt(f),
            Self::AuthorizedUser(v) => v.fmt(f),
            Self::ImpersonatedServiceAccount(v) => v.fmt(f),
        }
    }
}

impl CanonicalCredential {
    pub fn credential_type(&self) -> VertexCredentialType {
        match self {
            Self::ServiceAccount(_) => VertexCredentialType::ServiceAccount,
            Self::AuthorizedUser(_) => VertexCredentialType::AuthorizedUser,
            Self::ImpersonatedServiceAccount(_) => VertexCredentialType::ImpersonatedServiceAccount,
        }
    }
    pub fn principal_hint(&self) -> &str {
        match self {
            Self::ServiceAccount(v) => &v.client_email,
            Self::AuthorizedUser(v) => &v.client_id,
            Self::ImpersonatedServiceAccount(v) => &v.target_email,
        }
    }
    pub fn quota_project_id(&self) -> Option<&str> {
        match self {
            Self::ServiceAccount(_) => None,
            Self::AuthorizedUser(v) => v.quota_project_id.as_deref(),
            Self::ImpersonatedServiceAccount(v) => v.quota_project_id.as_deref(),
        }
    }
    pub fn project_id(&self) -> Option<&str> {
        match self {
            Self::ServiceAccount(v) => v.project_id.as_deref(),
            Self::AuthorizedUser(_) | Self::ImpersonatedServiceAccount(_) => None,
        }
    }
    pub(crate) fn service_account(&self) -> Option<&ServiceAccountCredential> {
        match self {
            Self::ServiceAccount(v) => Some(v),
            _ => None,
        }
    }
    pub(crate) fn private_key(&self) -> Option<&[u8]> {
        self.service_account().map(|v| v.private_key.as_slice())
    }
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum StoredCredential<'a> {
    #[serde(rename = "service_account")]
    ServiceAccount {
        v: u8,
        #[serde(rename = "private_key")]
        private_key: &'a str,
        client_email: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        private_key_id: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        project_id: Option<&'a str>,
    },
    #[serde(rename = "authorized_user")]
    AuthorizedUser {
        v: u8,
        client_id: &'a str,
        client_secret: &'a str,
        refresh_token: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        quota_project_id: Option<&'a str>,
    },
    #[serde(rename = "impersonated_service_account")]
    ImpersonatedServiceAccount {
        v: u8,
        target_email: &'a str,
        service_account_impersonation_url: String,
        source_credentials: &'a CanonicalCredential,
        delegates: &'a [String],
        #[serde(skip_serializing_if = "Option::is_none")]
        quota_project_id: Option<&'a str>,
    },
}

impl Serialize for CanonicalCredential {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::ServiceAccount(v) => {
                let encoded = STANDARD.encode(v.private_key.as_slice());
                StoredCredential::ServiceAccount {
                    v: 1,
                    private_key: &encoded,
                    client_email: &v.client_email,
                    private_key_id: v.private_key_id.as_deref(),
                    project_id: v.project_id.as_deref(),
                }
                .serialize(serializer)
            }
            Self::AuthorizedUser(v) => StoredCredential::AuthorizedUser {
                v: 1,
                client_id: &v.client_id,
                client_secret: v.client_secret.as_str(),
                refresh_token: v.refresh_token.as_str(),
                quota_project_id: v.quota_project_id.as_deref(),
            }
            .serialize(serializer),
            Self::ImpersonatedServiceAccount(v) => StoredCredential::ImpersonatedServiceAccount {
                v: 1,
                target_email: &v.target_email,
                service_account_impersonation_url: format!(
                    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
                    v.target_email
                ),
                source_credentials: &v.source_credentials,
                delegates: &v.delegates,
                quota_project_id: v.quota_project_id.as_deref(),
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for CanonicalCredential {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        from_value(&value).map_err(|e| D::Error::custom(e.code()))
    }
}

pub fn parse_credential(bytes: &[u8]) -> Result<CanonicalCredential, CredentialError> {
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::TooLarge);
    }
    let mut input = bytes;
    if input.starts_with(&[0xef, 0xbb, 0xbf]) {
        input = &input[3..];
    }
    input = trim_ascii(input);
    if input.is_empty() {
        return Err(CredentialError::InvalidJson);
    }
    let value: Value = serde_json::from_slice(input).map_err(|_| CredentialError::InvalidJson)?;
    from_value(&value)
}

fn trim_ascii(mut input: &[u8]) -> &[u8] {
    while input.first().is_some_and(u8::is_ascii_whitespace) {
        input = &input[1..];
    }
    while input.last().is_some_and(u8::is_ascii_whitespace) {
        input = &input[..input.len() - 1];
    }
    input
}
fn string<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}
fn optional_string(v: &Value, key: &str) -> Result<Option<String>, CredentialError> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        _ => Err(CredentialError::InvalidSecretField),
    }
}
fn required_string(v: &Value, key: &str) -> Result<String, CredentialError> {
    string(v, key)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(CredentialError::InvalidSecretField)
}
fn validate_email(email: &str) -> bool {
    let Some((local, domain)) = email.rsplit_once('@') else {
        return false;
    };
    !local.is_empty()
        && domain.ends_with(".gserviceaccount.com")
        && local.len() <= 128
        && local
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
}
fn validate_secret(s: &str) -> bool {
    !s.is_empty() && s.len() <= 2048 && !s.bytes().any(|b| b.is_ascii_control())
}
fn validate_key_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn validate_project(s: &str) -> bool {
    validate_project_id(s).is_ok()
}

fn reject_forbidden(v: &Value) -> Result<(), CredentialError> {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                if matches!(
                    k.as_str(),
                    "credential_source"
                        | "subject_token_type"
                        | "workforce_pool_user_project"
                        | "token_info_url"
                ) {
                    return Err(CredentialError::ForbiddenField);
                }
                reject_forbidden(child)?;
            }
        }
        Value::Array(xs) => {
            for x in xs {
                reject_forbidden(x)?
            }
        }
        _ => {}
    }
    Ok(())
}
fn validate_common(v: &Value) -> Result<(), CredentialError> {
    if let Some(value) = v.get("universe_domain")
        && value.as_str() != Some("googleapis.com")
    {
        return Err(CredentialError::InvalidUniverseDomain);
    }
    if let Some(value) = v.get("token_uri")
        && value.as_str() != Some(GOOGLE_TOKEN_URI)
    {
        return Err(CredentialError::InvalidTokenUri);
    }
    Ok(())
}
/// Returns the key as PKCS#8 DER. Google issues PKCS#8 (`BEGIN PRIVATE KEY`),
/// but Google's own client libraries also accept PKCS#1 (`BEGIN RSA PRIVATE
/// KEY`), and re-exported files from other tools carry it. A PKCS#1 key is
/// wrapped into PKCS#8 so storage and signing see a single format.
fn decode_pkcs8(pem: &str) -> Result<Vec<u8>, CredentialError> {
    let der = if pem.contains("BEGIN RSA PRIVATE KEY") {
        let pkcs1 = Zeroizing::new(pem_body(
            pem,
            "-----BEGIN RSA PRIVATE KEY-----",
            "-----END RSA PRIVATE KEY-----",
        )?);
        // `from_pkcs8` below rejects a malformed inner PKCS#1 key.
        wrap_pkcs1(&pkcs1)
    } else if pem.contains("BEGIN PRIVATE KEY") {
        pem_body(
            pem,
            "-----BEGIN PRIVATE KEY-----",
            "-----END PRIVATE KEY-----",
        )?
    } else {
        STANDARD
            .decode(pem.as_bytes())
            .map_err(|_| CredentialError::InvalidPrivateKey)?
    };
    let pair = RsaKeyPair::from_pkcs8(&der).map_err(|_| CredentialError::InvalidPrivateKey)?;
    if pair.public().modulus_len() < 256 || pair.public().modulus_len() > 1024 {
        return Err(CredentialError::InvalidPrivateKey);
    }
    Ok(der)
}

fn pem_body(pem: &str, begin: &str, end: &str) -> Result<Vec<u8>, CredentialError> {
    let Some(start) = pem.find(begin) else {
        return Err(CredentialError::InvalidPrivateKey);
    };
    let Some(stop) = pem.find(end) else {
        return Err(CredentialError::InvalidPrivateKey);
    };
    if stop <= start + begin.len() {
        return Err(CredentialError::InvalidPrivateKey);
    }
    let body: String = pem[start + begin.len()..stop]
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    STANDARD
        .decode(body.as_bytes())
        .map_err(|_| CredentialError::InvalidPrivateKey)
}

/// `PrivateKeyInfo { version 0, rsaEncryption with NULL parameters, OCTET STRING pkcs1 }`
/// (RFC 5208 §5, RFC 8017 Appendix C).
fn wrap_pkcs1(pkcs1: &[u8]) -> Vec<u8> {
    const VERSION_AND_ALGORITHM: [u8; 18] = [
        0x02, 0x01, 0x00, // INTEGER 0
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05,
        0x00, // SEQUENCE { OID 1.2.840.113549.1.1.1, NULL }
    ];
    let mut octets = vec![0x04];
    der_length(pkcs1.len(), &mut octets);
    let inner_len = VERSION_AND_ALGORITHM.len() + octets.len() + pkcs1.len();
    let mut der = Vec::with_capacity(inner_len + 6);
    der.push(0x30);
    der_length(inner_len, &mut der);
    der.extend_from_slice(&VERSION_AND_ALGORITHM);
    der.extend_from_slice(&octets);
    der.extend_from_slice(pkcs1);
    der
}

fn der_length(len: usize, out: &mut Vec<u8>) {
    if len < 0x80 {
        out.push(len as u8);
        return;
    }
    let bytes = len.to_be_bytes();
    let skip = bytes.iter().take_while(|byte| **byte == 0).count();
    out.push(0x80 | (bytes.len() - skip) as u8);
    out.extend_from_slice(&bytes[skip..]);
}
fn parse_service(v: &Value) -> Result<CanonicalCredential, CredentialError> {
    validate_common(v)?;
    let email = required_string(v, "client_email")?;
    if !validate_email(&email) {
        return Err(CredentialError::InvalidServiceAccountEmail);
    }
    let key = required_string(v, "private_key")?;
    let der = decode_pkcs8(&key)?;
    let key_id = optional_string(v, "private_key_id")?;
    if let Some(id) = &key_id
        && !validate_key_id(id)
    {
        return Err(CredentialError::InvalidPrivateKeyId);
    }
    let project = optional_string(v, "project_id")?;
    if let Some(p) = &project
        && !validate_project(p)
    {
        return Err(CredentialError::InvalidProjectId);
    }
    Ok(CanonicalCredential::ServiceAccount(
        ServiceAccountCredential {
            client_email: email,
            private_key_id: key_id,
            private_key: Zeroizing::new(der),
            project_id: project,
        },
    ))
}
fn parse_authorized(v: &Value) -> Result<CanonicalCredential, CredentialError> {
    validate_common(v)?;
    let client_id = required_string(v, "client_id")?;
    let client_secret = required_string(v, "client_secret")?;
    let refresh_token = required_string(v, "refresh_token")?;
    if !validate_secret(&client_id)
        || !validate_secret(&client_secret)
        || !validate_secret(&refresh_token)
    {
        return Err(CredentialError::InvalidSecretField);
    }
    let quota = optional_string(v, "quota_project_id")?;
    if let Some(q) = &quota
        && !validate_project(q)
    {
        return Err(CredentialError::InvalidProjectId);
    }
    Ok(CanonicalCredential::AuthorizedUser(
        AuthorizedUserCredential {
            client_id,
            client_secret: Zeroizing::new(client_secret),
            refresh_token: Zeroizing::new(refresh_token),
            quota_project_id: quota,
        },
    ))
}
fn parse_impersonated(v: &Value) -> Result<CanonicalCredential, CredentialError> {
    validate_common(v)?;
    let url = required_string(v, "service_account_impersonation_url")?;
    let prefix = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";
    let Some(rest) = url
        .strip_prefix(prefix)
        .and_then(|s| s.strip_suffix(":generateAccessToken"))
    else {
        return Err(CredentialError::InvalidImpersonationUrl);
    };
    if rest.contains('/')
        || rest.contains('?')
        || rest.contains('#')
        || rest.contains('%')
        || !validate_email(rest)
    {
        return Err(CredentialError::InvalidImpersonationUrl);
    }
    let source = v
        .get("source_credentials")
        .ok_or(CredentialError::InvalidSourceCredentials)?;
    let Some(map) = source.as_object() else {
        return Err(CredentialError::InvalidSourceCredentials);
    };
    if map.get("type").and_then(Value::as_str) == Some("impersonated_service_account") {
        return Err(CredentialError::NestedImpersonation);
    }
    let source_cred = parse_object(source)?;
    if !matches!(
        source_cred,
        CanonicalCredential::ServiceAccount(_) | CanonicalCredential::AuthorizedUser(_)
    ) {
        return Err(CredentialError::InvalidSourceCredentials);
    }
    let delegates_v = v.get("delegates");
    let mut delegates = Vec::new();
    if let Some(d) = delegates_v {
        let Some(xs) = d.as_array() else {
            return Err(CredentialError::InvalidDelegate);
        };
        if xs.len() > 8 {
            return Err(CredentialError::TooManyDelegates);
        }
        for x in xs {
            let Some(raw) = x.as_str() else {
                return Err(CredentialError::InvalidDelegate);
            };
            let email = raw
                .strip_prefix("projects/-/serviceAccounts/")
                .unwrap_or(raw);
            if email.contains('/') || !validate_email(email) {
                return Err(CredentialError::InvalidDelegate);
            }
            delegates.push(format!("projects/-/serviceAccounts/{email}"));
        }
    }
    let quota = optional_string(v, "quota_project_id")?;
    if let Some(q) = &quota
        && !validate_project(q)
    {
        return Err(CredentialError::InvalidProjectId);
    }
    Ok(CanonicalCredential::ImpersonatedServiceAccount(
        ImpersonatedServiceAccountCredential {
            target_email: rest.to_owned(),
            source_credentials: Box::new(source_cred),
            delegates,
            quota_project_id: quota,
        },
    ))
}
fn parse_object(v: &Value) -> Result<CanonicalCredential, CredentialError> {
    let Some(map) = v.as_object() else {
        return Err(CredentialError::InvalidJson);
    };
    match map.get("type").and_then(Value::as_str) {
        Some("service_account") => parse_service(v),
        Some("authorized_user") => parse_authorized(v),
        Some("impersonated_service_account") => parse_impersonated(v),
        Some(_) => Err(CredentialError::UnsupportedType),
        None => Err(CredentialError::MissingType),
    }
}
fn from_value(v: &Value) -> Result<CanonicalCredential, CredentialError> {
    reject_forbidden(v)?;
    parse_object(v)
}

pub(crate) fn credential_type_label(v: &CanonicalCredential) -> &'static str {
    v.credential_type().as_str()
}
