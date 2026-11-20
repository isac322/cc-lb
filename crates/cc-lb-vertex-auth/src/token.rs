use crate::credential::{
    CLOUD_PLATFORM_SCOPE, CanonicalCredential, GOOGLE_TOKEN_URI, MAX_RESPONSE_BYTES,
    credential_type_label,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_domain::vertex::{VERTEX_CREDENTIAL_INVALID_CLIENT, VERTEX_CREDENTIAL_INVALID_GRANT};
use http::HeaderValue;
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenEndpoint {
    OAuth2Token,
    IamGenerateAccessToken { email: String },
}
#[derive(Clone)]
pub struct TokenRequest {
    pub body: Vec<u8>,
    pub content_type: &'static str,
    pub headers: Vec<(String, String)>,
}
impl fmt::Debug for TokenRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenRequest")
            .field("content_type", &self.content_type)
            .field(
                "headers",
                &self.headers.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("body", &"[REDACTED]")
            .finish()
    }
}
#[derive(Clone)]
pub struct TokenResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}
impl fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenResponse")
            .field("status", &self.status)
            .field(
                "headers",
                &self.headers.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("body", &"[REDACTED]")
            .finish()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportError {
    Timeout,
    Network,
    Redirect,
    InvalidEndpoint,
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Timeout => "token_transport_timeout",
            Self::Network => "token_transport_network",
            Self::Redirect => "token_transport_redirect",
            Self::InvalidEndpoint => "token_transport_invalid_endpoint",
        })
    }
}
impl std::error::Error for TransportError {}
#[async_trait]
pub trait TokenTransport: Send + Sync {
    async fn post(
        &self,
        endpoint: TokenEndpoint,
        request: TokenRequest,
    ) -> Result<TokenResponse, TransportError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MintError {
    Transport,
    InvalidGrant,
    /// An invalid grant accompanied by a response clock more than 30 seconds
    /// away from the local clock. This is transient and must not count toward
    /// the terminal invalid-grant strike latch.
    InvalidGrantClockSkew,
    InvalidGrantTerminal,
    InvalidClient,
    UnauthorizedClient,
    InvalidScope,
    Unauthorized,
    PermissionDenied,
    ResponseTooLarge,
    MalformedResponse,
    InvalidTokenType,
    InvalidAccessToken,
    InvalidExpiry,
    IamUnauthorized,
    Backoff,
}
impl MintError {
    pub const fn terminal_reason(self) -> Option<&'static str> {
        match self {
            Self::InvalidGrantTerminal => Some(VERTEX_CREDENTIAL_INVALID_GRANT),
            Self::InvalidClient
            | Self::UnauthorizedClient
            | Self::InvalidScope
            | Self::Unauthorized => Some(VERTEX_CREDENTIAL_INVALID_CLIENT),
            _ => None,
        }
    }
    fn outcome(self) -> &'static str {
        if self.terminal_reason().is_some() {
            "terminal"
        } else {
            "transient"
        }
    }
}
impl fmt::Display for MintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Transport => "token_transport_failure",
            Self::InvalidGrant => "token_invalid_grant",
            Self::InvalidGrantClockSkew => "token_invalid_grant_clock_skew",
            Self::InvalidGrantTerminal => "token_invalid_grant_terminal",
            Self::InvalidClient => "token_invalid_client",
            Self::UnauthorizedClient => "token_unauthorized_client",
            Self::InvalidScope => "token_invalid_scope",
            Self::Unauthorized => "token_unauthorized",
            Self::PermissionDenied => "token_permission_denied",
            Self::ResponseTooLarge => "token_response_too_large",
            Self::MalformedResponse => "token_malformed_response",
            Self::InvalidTokenType => "token_invalid_type",
            Self::InvalidAccessToken => "token_invalid_access_token",
            Self::InvalidExpiry => "token_invalid_expiry",
            Self::IamUnauthorized => "iam_unauthorized",
            Self::Backoff => "token_backoff",
        })
    }
}
impl std::error::Error for MintError {}

#[derive(Clone)]
pub struct BearerToken {
    pub header: HeaderValue,
    pub id: u64,
    pub expires_at_unix: u64,
}
impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BearerToken")
            .field("id", &self.id)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("header", &"[REDACTED]")
            .finish()
    }
}
#[async_trait]
pub trait BearerSource: Send + Sync {
    async fn bearer(&self) -> Result<BearerToken, MintError>;
    fn invalidate_if_current(&self, token_id: u64) -> bool;
    fn quota_project(&self) -> Option<&str>;
}

struct Cached {
    token: BearerToken,
}
struct State {
    cached: Option<Cached>,
    minting: bool,
    notify: Arc<Notify>,
    next_retry: u64,
    failures: u32,
    invalid_grants: u32,
    last_invalid_grant: u64,
    last_forced: u64,
    terminal: Option<&'static str>,
    next_id: u64,
}
pub struct TokenSource {
    credential: Arc<CanonicalCredential>,
    transport: Arc<dyn TokenTransport>,
    clock: ClockHandle,
    state: Mutex<State>,
}
impl fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSource")
            .field("credential_type", &credential_type_label(&self.credential))
            .field("principal_hint", &"[REDACTED]")
            .finish()
    }
}
impl TokenSource {
    pub fn new(
        credential: CanonicalCredential,
        transport: Arc<dyn TokenTransport>,
        clock: ClockHandle,
    ) -> Arc<Self> {
        Arc::new(Self {
            credential: Arc::new(credential),
            transport,
            clock,
            state: Mutex::new(State {
                cached: None,
                minting: false,
                notify: Arc::new(Notify::new()),
                next_retry: 0,
                failures: 0,
                invalid_grants: 0,
                last_invalid_grant: 0,
                last_forced: 0,
                terminal: None,
                next_id: 0,
            }),
        })
    }
    pub fn credential(&self) -> &CanonicalCredential {
        &self.credential
    }
    pub fn is_terminal(&self) -> bool {
        self.state
            .lock()
            .expect("token state poisoned")
            .terminal
            .is_some()
    }
    pub fn terminal_reason(&self) -> Option<&'static str> {
        self.state.lock().expect("token state poisoned").terminal
    }
    pub fn quota_project(&self) -> Option<&str> {
        self.credential.quota_project_id()
    }
    pub fn invalidate_if_current(&self, token_id: u64) -> bool {
        let now = unix_secs(self.clock.now());
        let mut state = self.state.lock().expect("token state poisoned");
        if state.cached.as_ref().map(|v| v.token.id) != Some(token_id)
            || (state.last_forced != 0 && now.saturating_sub(state.last_forced) < 30)
        {
            return false;
        }
        state.cached = None;
        state.last_forced = now;
        true
    }
    fn use_cached(state: &State, now: u64) -> bool {
        state
            .cached
            .as_ref()
            .is_some_and(|v| v.token.expires_at_unix > now + 60)
    }
    fn soft_refresh(state: &State, now: u64) -> bool {
        state.cached.as_ref().is_some_and(|v| {
            v.token.expires_at_unix <= now + 240 && v.token.expires_at_unix > now + 60
        })
    }
    fn spawn_mint(self: &Arc<Self>) -> Arc<Notify> {
        let notify = {
            let mut state = self.state.lock().expect("token state poisoned");
            if state.minting {
                return Arc::clone(&state.notify);
            }
            state.minting = true;
            // Keep one notification generation for the source. Waiters are
            // registered while holding the state lock before they await it.
            Arc::clone(&state.notify)
        };
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let result = this.mint().await;
            this.finish_mint(result);
        });
        notify
    }
    fn finish_mint(&self, result: Result<BearerToken, MintError>) {
        let now = unix_secs(self.clock.now());
        let mut state = self.state.lock().expect("token state poisoned");
        match result {
            Ok(mut token) => {
                state.next_id = state.next_id.wrapping_add(1);
                token.id = state.next_id;
                state.cached = Some(Cached { token });
                state.failures = 0;
                state.next_retry = 0;
                state.invalid_grants = 0;
                metrics::counter!("cclb_vertex_token_mint_total", "credential_type" => credential_type_label(&self.credential), "outcome" => "success").increment(1);
            }
            Err(error) => {
                state.failures = state.failures.saturating_add(1);
                let delay = (1u64 << state.failures.min(6)).min(60);
                let jitter = (delay / 5).max(1);
                state.next_retry = now + delay.saturating_sub(jitter) + (now % (jitter * 2 + 1));
                let mut outcome = error.outcome();
                if matches!(error, MintError::InvalidGrant) {
                    state.invalid_grants = if now.saturating_sub(state.last_invalid_grant) >= 30 {
                        state.invalid_grants.saturating_add(1)
                    } else {
                        1
                    };
                    state.last_invalid_grant = now;
                    if state.invalid_grants >= 2 {
                        state.terminal = Some(VERTEX_CREDENTIAL_INVALID_GRANT);
                        outcome = "terminal";
                    }
                }
                if let Some(reason) = error.terminal_reason() {
                    state.terminal = Some(reason);
                }
                metrics::counter!("cclb_vertex_token_mint_total", "credential_type" => credential_type_label(&self.credential), "outcome" => outcome).increment(1);
            }
        }
        state.minting = false;
        state.notify.notify_waiters();
    }
    async fn mint(&self) -> Result<BearerToken, MintError> {
        mint_credential(&self.credential, &self.transport, &self.clock).await
    }
    pub async fn bearer(self: &Arc<Self>) -> Result<BearerToken, MintError> {
        loop {
            let now = unix_secs(self.clock.now());
            let (cached, minting, backoff, terminal, wait) = {
                let state = self.state.lock().expect("token state poisoned");
                // `notify_waiters` does not retain a permit. Register and
                // enable this waiter before releasing the state lock so a
                // completion cannot be lost between observing `minting` and
                // awaiting.
                let mut wait = Box::pin(state.notify.clone().notified_owned());
                wait.as_mut().enable();
                (
                    state.cached.as_ref().map(|v| v.token.clone()),
                    state.minting,
                    state.next_retry > now,
                    state.terminal,
                    wait,
                )
            };
            if let Some(reason) = terminal {
                return Err(if reason == VERTEX_CREDENTIAL_INVALID_GRANT {
                    MintError::InvalidGrantTerminal
                } else {
                    MintError::InvalidClient
                });
            }
            if let Some(token) = cached {
                let state = self.state.lock().expect("token state poisoned");
                if Self::use_cached(&state, now) {
                    if Self::soft_refresh(&state, now) && !minting && !backoff {
                        drop(state);
                        self.spawn_mint();
                    }
                    return Ok(token);
                }
            }
            if backoff {
                return Err(MintError::Backoff);
            }
            if !minting {
                self.spawn_mint();
            }
            wait.await;
        }
    }
}
#[async_trait]
impl BearerSource for Arc<TokenSource> {
    async fn bearer(&self) -> Result<BearerToken, MintError> {
        self.clone().bearer().await
    }
    fn invalidate_if_current(&self, token_id: u64) -> bool {
        self.as_ref().invalidate_if_current(token_id)
    }
    fn quota_project(&self) -> Option<&str> {
        self.as_ref().quota_project()
    }
}

async fn mint_credential(
    credential: &CanonicalCredential,
    transport: &Arc<dyn TokenTransport>,
    clock: &ClockHandle,
) -> Result<BearerToken, MintError> {
    match credential {
        CanonicalCredential::ServiceAccount(_) => {
            let now = unix_secs(clock.now());
            let assertion = make_jwt(credential, now)?;
            let body = form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &assertion),
            ]);
            let response = transport
                .post(
                    TokenEndpoint::OAuth2Token,
                    TokenRequest {
                        body,
                        content_type: "application/x-www-form-urlencoded",
                        headers: vec![],
                    },
                )
                .await
                .map_err(|_| MintError::Transport)?;
            parse_token_response(response, now, false)
        }
        CanonicalCredential::AuthorizedUser(user) => {
            let body = form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &user.client_id),
                ("client_secret", user.client_secret.as_str()),
                ("refresh_token", user.refresh_token.as_str()),
            ]);
            let now = unix_secs(clock.now());
            let response = transport
                .post(
                    TokenEndpoint::OAuth2Token,
                    TokenRequest {
                        body,
                        content_type: "application/x-www-form-urlencoded",
                        headers: vec![],
                    },
                )
                .await
                .map_err(|_| MintError::Transport)?;
            parse_token_response(response, now, false)
        }
        CanonicalCredential::ImpersonatedServiceAccount(imp) => {
            // Mint the source credential directly so source failures are observed by
            // the outer TokenSource. In particular, invalid_grant must count toward
            // the outer two-strike terminal latch instead of being hidden behind a
            // freshly-created inner source's backoff state.
            let mut source_token =
                Box::pin(mint_credential(&imp.source_credentials, transport, clock)).await?;
            let body = serde_json::to_vec(&json!({ "delegates": imp.delegates, "scope": [CLOUD_PLATFORM_SCOPE], "lifetime": "3600s" })).map_err(|_| MintError::MalformedResponse)?;
            // `BearerToken.header` already carries the `Bearer ` scheme.
            let mut headers = vec![(
                "authorization".to_owned(),
                source_token.header.to_str().unwrap_or_default().to_owned(),
            )];
            if let Some(quota) = imp.source_credentials.quota_project_id() {
                headers.push(("x-goog-user-project".to_owned(), quota.to_owned()));
            }
            let endpoint = TokenEndpoint::IamGenerateAccessToken {
                email: imp.target_email.clone(),
            };
            let first = transport
                .post(
                    endpoint.clone(),
                    TokenRequest {
                        body: body.clone(),
                        content_type: "application/json",
                        headers: headers.clone(),
                    },
                )
                .await
                .map_err(|_| MintError::Transport)?;
            let response = if first.status == 401 {
                source_token =
                    Box::pin(mint_credential(&imp.source_credentials, transport, clock)).await?;
                headers[0] = (
                    "authorization".to_owned(),
                    source_token.header.to_str().unwrap_or_default().to_owned(),
                );
                transport
                    .post(
                        endpoint,
                        TokenRequest {
                            body,
                            content_type: "application/json",
                            headers,
                        },
                    )
                    .await
                    .map_err(|_| MintError::Transport)?
            } else {
                first
            };
            if response.status == 401 {
                return Err(MintError::IamUnauthorized);
            }
            parse_token_response(response, unix_secs(clock.now()), true)
        }
    }
}
pub async fn mint_once(
    credential: &CanonicalCredential,
    transport: Arc<dyn TokenTransport>,
    clock: ClockHandle,
) -> Result<BearerToken, MintError> {
    mint_credential(credential, &transport, &clock).await
}
fn form(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in fields {
        serializer.append_pair(key, value);
    }
    serializer.finish().into_bytes()
}
fn make_jwt(credential: &CanonicalCredential, now: u64) -> Result<String, MintError> {
    let sa = credential
        .service_account()
        .ok_or(MintError::MalformedResponse)?;
    let header = if let Some(kid) = &sa.private_key_id {
        json!({"alg":"RS256","typ":"JWT","kid":kid})
    } else {
        json!({"alg":"RS256","typ":"JWT"})
    };
    let payload = json!({"iss":sa.client_email,"scope":CLOUD_PLATFORM_SCOPE,"aud":GOOGLE_TOKEN_URI,"iat":now.saturating_sub(30),"exp":now.saturating_add(3300)});
    let encoded = |value: Value| -> Result<String, MintError> {
        Ok(URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&value).map_err(|_| MintError::MalformedResponse)?))
    };
    let signing = format!("{}.{}", encoded(header)?, encoded(payload)?);
    let der = credential
        .private_key()
        .ok_or(MintError::MalformedResponse)?;
    let pair = RsaKeyPair::from_pkcs8(der).map_err(|_| MintError::MalformedResponse)?;
    let mut signature = vec![0; pair.public().modulus_len()];
    pair.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        signing.as_bytes(),
        &mut signature,
    )
    .map_err(|_| MintError::MalformedResponse)?;
    Ok(format!("{signing}.{}", URL_SAFE_NO_PAD.encode(signature)))
}
fn parse_token_response(
    response: TokenResponse,
    now: u64,
    impersonation: bool,
) -> Result<BearerToken, MintError> {
    if response.body.len() > MAX_RESPONSE_BYTES {
        return Err(MintError::ResponseTooLarge);
    }
    let value: Value =
        serde_json::from_slice(&response.body).map_err(|_| MintError::MalformedResponse)?;
    if response.status == 400 {
        return Err(match value.get("error").and_then(Value::as_str) {
            Some("invalid_grant") => {
                let response_time = response
                    .headers
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("date"))
                    .and_then(|(_, value)| chrono::DateTime::parse_from_rfc2822(value).ok())
                    .map(|value| value.timestamp());
                if response_time.is_some_and(|value| {
                    (value - i64::try_from(now).unwrap_or(i64::MAX)).unsigned_abs() > 30
                }) {
                    MintError::InvalidGrantClockSkew
                } else {
                    MintError::InvalidGrant
                }
            }
            Some("invalid_client") => MintError::InvalidClient,
            Some("unauthorized_client") => MintError::UnauthorizedClient,
            Some("invalid_scope") => MintError::InvalidScope,
            _ => MintError::Transport,
        });
    }
    if response.status == 401 {
        return Err(if impersonation {
            MintError::IamUnauthorized
        } else {
            MintError::Unauthorized
        });
    }
    if response.status == 403 {
        return Err(MintError::PermissionDenied);
    }
    if !(200..300).contains(&response.status) {
        return Err(MintError::Transport);
    }
    let token = value
        .get(if impersonation {
            "accessToken"
        } else {
            "access_token"
        })
        .and_then(Value::as_str)
        .ok_or(MintError::InvalidAccessToken)?;
    let header = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| MintError::InvalidAccessToken)?;
    let expires_at = if impersonation {
        let expire = value
            .get("expireTime")
            .and_then(Value::as_str)
            .ok_or(MintError::InvalidExpiry)?;
        let expire = chrono::DateTime::parse_from_rfc3339(expire)
            .map_err(|_| MintError::InvalidExpiry)?
            .timestamp();
        let provider_now = response
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("date"))
            .and_then(|(_, v)| chrono::DateTime::parse_from_rfc2822(v).ok())
            .map(|v| v.timestamp())
            .unwrap_or(now as i64);
        if expire <= provider_now {
            return Err(MintError::InvalidExpiry);
        }
        now.saturating_add((expire - provider_now) as u64)
    } else {
        let token_type = value
            .get("token_type")
            .and_then(Value::as_str)
            .ok_or(MintError::InvalidTokenType)?;
        if !token_type.eq_ignore_ascii_case("bearer") {
            return Err(MintError::InvalidTokenType);
        }
        let expires = value
            .get("expires_in")
            .and_then(Value::as_i64)
            .ok_or(MintError::InvalidExpiry)?;
        if expires <= 0 {
            return Err(MintError::InvalidExpiry);
        }
        now.saturating_add(expires as u64)
    };
    Ok(BearerToken {
        header,
        id: 0,
        expires_at_unix: expires_at,
    })
}

type TokenRegistryEntries = HashMap<(String, [u8; 32]), Weak<TokenSource>>;

pub struct TokenRegistry {
    entries: Mutex<TokenRegistryEntries>,
}
impl Default for TokenRegistry {
    fn default() -> Self {
        Self::new()
    }
}
impl TokenRegistry {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
    pub fn get(&self, upstream_id: &str, ciphertext: &[u8]) -> Option<Arc<TokenSource>> {
        let mut hash = [0; 32];
        hash.copy_from_slice(Sha256::digest(ciphertext).as_slice());
        self.entries
            .lock()
            .expect("token registry poisoned")
            .get(&(upstream_id.to_owned(), hash))
            .and_then(Weak::upgrade)
    }
    pub fn source_for(
        &self,
        upstream_id: impl Into<String>,
        ciphertext: &[u8],
        credential: CanonicalCredential,
        transport: Arc<dyn TokenTransport>,
        clock: ClockHandle,
    ) -> Arc<TokenSource> {
        let mut hash = [0; 32];
        hash.copy_from_slice(Sha256::digest(ciphertext).as_slice());
        let key = (upstream_id.into(), hash);
        let mut entries = self.entries.lock().expect("token registry poisoned");
        if let Some(source) = entries.get(&key).and_then(Weak::upgrade) {
            return source;
        }
        let source = TokenSource::new(credential, transport, clock);
        entries.insert(key, Arc::downgrade(&source));
        source
    }
    pub fn retain_live(&self, live: &std::collections::HashSet<String>) {
        self.entries
            .lock()
            .expect("token registry poisoned")
            .retain(|(id, _), weak| live.contains(id) && weak.strong_count() > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cc_lb_clock::TestClock;
    use std::sync::{Arc, Mutex};

    struct CaptureTransport {
        calls: Mutex<Vec<(TokenEndpoint, TokenRequest)>>,
        responses: Mutex<Vec<TokenResponse>>,
    }

    #[async_trait]
    impl TokenTransport for CaptureTransport {
        async fn post(
            &self,
            endpoint: TokenEndpoint,
            request: TokenRequest,
        ) -> Result<TokenResponse, TransportError> {
            self.calls.lock().expect("calls").push((endpoint, request));
            self.responses
                .lock()
                .expect("responses")
                .pop()
                .ok_or(TransportError::Network)
        }
    }

    #[tokio::test]
    async fn impersonation_reuses_source_bearer_header_without_duplication() {
        let source_key = include_str!("../../cc-lb-server/tests/fixtures/tls/key-a.pem");
        let raw = format!(
            r#"{{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{{"type":"service_account","client_email":"source@project.iam.gserviceaccount.com","private_key":{source_key:?},"private_key_id":"kid-1"}}}}"#
        );
        let credential = crate::parse_credential(raw.as_bytes()).expect("credential");
        let transport = Arc::new(CaptureTransport {
            calls: Mutex::new(Vec::new()),
            responses: Mutex::new(vec![
                TokenResponse {
                    status: 200,
                    headers: vec![],
                    body: br#"{"accessToken":"impersonated-token","expireTime":"2030-01-01T00:00:00Z"}"#
                        .to_vec(),
                },
                TokenResponse {
                    status: 200,
                    headers: vec![],
                    body: br#"{"access_token":"source-token","token_type":"Bearer","expires_in":3600}"#
                        .to_vec(),
                },
            ]),
        });
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));

        mint_once(&credential, transport.clone(), clock)
            .await
            .expect("mint");

        let calls = transport.calls.lock().expect("calls");
        assert_eq!(calls.len(), 2);
        assert!(matches!(
            calls[1].0,
            TokenEndpoint::IamGenerateAccessToken { .. }
        ));
        let iam_request = &calls[1].1;
        assert_eq!(
            iam_request
                .headers
                .iter()
                .find(|(name, _)| name == "authorization")
                .map(|(_, value)| value.as_str()),
            Some("Bearer source-token")
        );
    }

    #[tokio::test]
    async fn qa_tok_impersonated_revoked_source_reaches_terminal_invalid_grant() {
        let credential = crate::parse_credential(
            br#"{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{"type":"authorized_user","client_id":"client-id","client_secret":"CLIENT-SECRET","refresh_token":"REVOKED"}}"#,
        )
        .expect("credential");
        let transport = Arc::new(CaptureTransport {
            calls: Mutex::new(Vec::new()),
            responses: Mutex::new(vec![
                TokenResponse {
                    status: 400,
                    headers: vec![],
                    body: br#"{"error":"invalid_grant"}"#.to_vec(),
                },
                TokenResponse {
                    status: 400,
                    headers: vec![],
                    body: br#"{"error":"invalid_grant"}"#.to_vec(),
                },
            ]),
        });
        let test_clock: Arc<TestClock> = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let source = TokenSource::new(credential, transport.clone(), test_clock.clone());

        assert!(source.bearer().await.is_err());
        test_clock.advance_secs(31);
        assert!(source.bearer().await.is_err());
        assert!(matches!(
            source.bearer().await,
            Err(MintError::InvalidGrantTerminal)
        ));
        assert_eq!(transport.calls.lock().expect("calls").len(), 2);
    }

    #[tokio::test]
    async fn qa_tok_impersonated_source_401_is_terminal_invalid_client() {
        let credential = crate::parse_credential(
            br#"{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{"type":"authorized_user","client_id":"client-id","client_secret":"CLIENT-SECRET","refresh_token":"REVOKED"}}"#,
        )
        .expect("credential");
        let transport = Arc::new(CaptureTransport {
            calls: Mutex::new(Vec::new()),
            responses: Mutex::new(vec![TokenResponse {
                status: 401,
                headers: vec![],
                body: br#"{"error":"invalid_client"}"#.to_vec(),
            }]),
        });
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));

        assert!(matches!(
            mint_once(&credential, transport, clock).await,
            Err(MintError::Unauthorized)
        ));
    }
}
