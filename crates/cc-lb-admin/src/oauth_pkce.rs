use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::Clock;
use cc_lb_oauth_protocol::{
    ExistingTokenParts, parse_token_endpoint_response, refreshed_token_parts,
};
use cc_lb_storage_api::OAuthCredentials;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use oauth2::{AuthUrl, ClientId, PkceCodeChallenge, TokenUrl};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OAuthTokenMode {
    /// 365-day access token, never refreshed. Requested on every connect.
    #[default]
    #[serde(rename = "long_lived_365d")]
    LongLived365d,
    /// Classic 8-hour access token with background/lazy refresh.
    #[serde(rename = "refreshing")]
    Refreshing,
}

impl OAuthTokenMode {
    pub(crate) fn never_refresh(self) -> bool {
        matches!(self, Self::LongLived365d)
    }

    pub(crate) fn from_never_refresh(never_refresh: bool) -> Self {
        if never_refresh {
            Self::LongLived365d
        } else {
            Self::Refreshing
        }
    }
}

/// Why a long-lived request ended up as a refreshing credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LongLivedFallbackReason {
    /// Anthropic returned 400 invalid_request for the custom `expires_in`.
    #[serde(rename = "rejected")]
    Rejected,
    /// Anthropic returned 400 invalid_request for the custom `expires_in`
    /// while the configured scope set asked for a scope Anthropic will not
    /// grant a year-long token for (for example `org:create_api_key`). The
    /// operator's own `oauth.anthropic.scopes` is the likely cause.
    #[serde(rename = "scope_rejected")]
    ScopeRejected,
    /// Anthropic accepted the exchange but granted a materially shorter
    /// lifetime than requested.
    #[serde(rename = "clamped")]
    Clamped,
}

#[derive(Clone)]
pub(crate) struct PkceHandshake {
    pub(crate) authorize_url: Url,
    client_id: ClientId,
    token_endpoint: TokenUrl,
    scopes: Vec<String>,
    redirect_uri: Url,
    verifier: SecretString,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PkceHandshakeState {
    pub(crate) authorize_url: Url,
    client_id: String,
    token_endpoint: Url,
    scopes: Vec<String>,
    redirect_uri: Url,
    verifier: String,
}

impl PkceHandshake {
    pub(crate) fn into_state(self) -> PkceHandshakeState {
        PkceHandshakeState {
            authorize_url: self.authorize_url,
            client_id: self.client_id.to_string(),
            token_endpoint: self.token_endpoint.url().clone(),
            scopes: self.scopes,
            redirect_uri: self.redirect_uri,
            verifier: self.verifier.expose_secret().to_owned(),
        }
    }
}

impl PkceHandshakeState {
    pub(crate) fn into_handshake(self) -> Result<PkceHandshake, url::ParseError> {
        Ok(PkceHandshake {
            authorize_url: self.authorize_url,
            client_id: ClientId::new(self.client_id),
            token_endpoint: TokenUrl::new(self.token_endpoint.to_string())?,
            scopes: self.scopes,
            redirect_uri: self.redirect_uri,
            verifier: SecretString::new(self.verifier.into_boxed_str()),
        })
    }
}

impl fmt::Debug for PkceHandshake {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PkceHandshake")
            .field("authorize_url", &self.authorize_url)
            .field("client_id", &self.client_id)
            .field("token_endpoint", &self.token_endpoint)
            .field("scopes", &self.scopes)
            .field("redirect_uri", &self.redirect_uri)
            .field("verifier", &"[REDACTED]")
            .finish()
    }
}

pub(crate) fn start_pkce_flow(
    client_id: ClientId,
    authorize_endpoint: AuthUrl,
    token_endpoint: TokenUrl,
    scopes: Vec<String>,
    redirect_uri: Url,
) -> PkceHandshake {
    let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
    let authorize_url = authorize_url(
        authorize_endpoint.url().clone(),
        &client_id,
        &scopes,
        &redirect_uri,
        &challenge,
    );
    tracing::info!(
        verifier_len = verifier.secret().len(),
        verifier_first6 = %verifier.secret().chars().take(6).collect::<String>(),
        challenge_in_url = %challenge.as_str(),
        "oauth start: generated PKCE verifier+challenge"
    );

    PkceHandshake {
        authorize_url,
        client_id,
        token_endpoint,
        scopes,
        redirect_uri,
        verifier: SecretString::new(verifier.into_secret().into_boxed_str()),
    }
}

pub(crate) struct PkceExchangeOutcome {
    pub(crate) credentials: OAuthCredentials,
    /// Mode actually negotiated. `Refreshing` when a long-lived request fell back.
    pub(crate) mode: OAuthTokenMode,
    /// `true` when long-lived was requested but the grant could not stay
    /// long-lived; always mirrors `fallback_reason.is_some()`.
    pub(crate) long_lived_fallback: bool,
    /// Which fallback path produced a refreshing credential, if any.
    pub(crate) fallback_reason: Option<LongLivedFallbackReason>,
    /// Access-token lifetime the server actually granted, in seconds.
    pub(crate) granted_expires_in_secs: Option<u64>,
}

pub(crate) async fn complete_pkce_flow(
    handshake: PkceHandshake,
    auth_code: String,
    state_token: String,
    http: Arc<dyn OAuthHttpClient>,
    clock: &dyn Clock,
) -> Result<PkceExchangeOutcome, OAuthTokenError> {
    let now_epoch_secs = cc_lb_clock::unix_secs(clock.now());
    // Every connect requests the 365-day grant; Anthropic may reject the
    // custom `expires_in` outright or silently clamp the granted lifetime.
    let first = exchange_pkce_code(
        http.as_ref(),
        handshake.token_endpoint.url(),
        handshake.client_id.as_str(),
        &auth_code,
        &state_token,
        &handshake.verifier,
        &handshake.redirect_uri,
        now_epoch_secs,
        Some(cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS),
    )
    .await;

    match first {
        Ok(credentials) => {
            // The granted lifetime comes from the token response, never from
            // the value we requested: Anthropic may silently clamp it.
            let granted_secs = credentials.expires_at.saturating_sub(now_epoch_secs);
            if long_lived_grant_is_usable(granted_secs) {
                Ok(PkceExchangeOutcome {
                    credentials,
                    mode: OAuthTokenMode::LongLived365d,
                    long_lived_fallback: false,
                    fallback_reason: None,
                    granted_expires_in_secs: Some(granted_secs),
                })
            } else {
                // The server silently clamped below the point where
                // never-refreshing is safe. Keep the issued tokens as-is —
                // the refresh token is now the renewal path, so the
                // credential must not be marked non-refreshable.
                Ok(PkceExchangeOutcome {
                    credentials,
                    mode: OAuthTokenMode::Refreshing,
                    long_lived_fallback: true,
                    fallback_reason: Some(LongLivedFallbackReason::Clamped),
                    granted_expires_in_secs: Some(granted_secs),
                })
            }
        }
        Err(error) => {
            if !is_long_lived_rejection(&error) {
                return Err(error);
            }
            // Anthropic does not consume the authorization code on a failed
            // exchange, so the same code can be re-exchanged without the
            // custom `expires_in` as a classic refreshing credential.
            let retry_now_epoch_secs = cc_lb_clock::unix_secs(clock.now());
            let credentials = exchange_pkce_code(
                http.as_ref(),
                handshake.token_endpoint.url(),
                handshake.client_id.as_str(),
                &auth_code,
                &state_token,
                &handshake.verifier,
                &handshake.redirect_uri,
                retry_now_epoch_secs,
                None,
            )
            .await?;
            Ok(PkceExchangeOutcome {
                granted_expires_in_secs: Some(
                    credentials.expires_at.saturating_sub(retry_now_epoch_secs),
                ),
                credentials,
                mode: OAuthTokenMode::Refreshing,
                long_lived_fallback: true,
                fallback_reason: Some(if requests_non_inference_scope(&handshake.scopes) {
                    LongLivedFallbackReason::ScopeRejected
                } else {
                    LongLivedFallbackReason::Rejected
                }),
            })
        }
    }
}

/// `true` when the granted access-token lifetime is long enough to justify
/// never-refreshing mode. Anthropic anchors refresh tokens to a 30-day
/// window, so a grant at or below [`cc_lb_config::LONG_LIVED_MIN_GRANT_SECS`]
/// buys nothing over the refreshing flow while giving up renewal.
fn long_lived_grant_is_usable(granted_secs: u64) -> bool {
    granted_secs > cc_lb_config::LONG_LIVED_MIN_GRANT_SECS
}

/// Scope set Anthropic grants a year-long token for: the inference-only
/// default `oauth.anthropic.scopes`. Any configured scope outside this set
/// (for example `org:create_api_key`, `user:sessions:claude_code`, or
/// `user:mcp_servers`) makes a 365-day grant impossible, so a rejection under
/// such a configuration is reported as the operator's own scope choice.
const LONG_LIVED_ELIGIBLE_SCOPES: [&str; 2] = ["user:profile", "user:inference"];

/// `true` when the requested scope set contains a scope outside the
/// inference-only set Anthropic grants year-long tokens for.
fn requests_non_inference_scope(scopes: &[String]) -> bool {
    scopes
        .iter()
        .any(|scope| !LONG_LIVED_ELIGIBLE_SCOPES.contains(&scope.as_str()))
}

/// `true` when Anthropic rejected the request purely because of the custom
/// `expires_in`, meaning the same authorization code can be re-exchanged
/// without it. Anthropic does not consume the code on a failed exchange.
pub(crate) fn is_long_lived_rejection(error: &OAuthTokenError) -> bool {
    let OAuthTokenError::TokenEndpoint {
        status,
        code,
        description,
    } = error
    else {
        return false;
    };
    if *status != StatusCode::BAD_REQUEST || code.as_deref() != Some("invalid_request") {
        return false;
    }
    let Some(description) = description else {
        return false;
    };
    let description = description.to_lowercase();
    description.contains("expires_in") || description.contains("expiry")
}

fn authorize_url(
    mut endpoint: Url,
    client_id: &ClientId,
    scopes: &[String],
    redirect_uri: &Url,
    challenge: &PkceCodeChallenge,
) -> Url {
    {
        let mut query = endpoint.query_pairs_mut();
        query.append_pair("code", "true");
        query.append_pair("client_id", client_id.as_str());
        query.append_pair("response_type", "code");
        query.append_pair("redirect_uri", redirect_uri.as_str());
        query.append_pair("code_challenge", challenge.as_str());
        query.append_pair("code_challenge_method", challenge.method().as_str());
        if !scopes.is_empty() {
            query.append_pair("scope", &scopes.join(" "));
        }
    }
    endpoint
}

#[derive(Clone)]
pub(crate) struct OAuthTokenRequest {
    endpoint: Url,
    form_body: SecretString,
}

impl fmt::Debug for OAuthTokenRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthTokenRequest")
            .field("endpoint", &self.endpoint)
            .field("form_body", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OAuthTokenResponse {
    status: StatusCode,
    body: Bytes,
}

#[derive(Debug, Error)]
pub(crate) enum OAuthTokenError {
    #[error(
        "oauth token endpoint returned status {status}{}{}",
        .code.as_deref().map(|c| format!(" ({c})")).unwrap_or_default(),
        .description.as_deref().map(|d| format!(": {d}")).unwrap_or_default()
    )]
    TokenEndpoint {
        status: StatusCode,
        code: Option<String>,
        description: Option<String>,
    },
    #[error("oauth token endpoint request failed: {reason}")]
    Http { reason: String },
    #[error("oauth token response json failed: {reason}")]
    Json { reason: String },
}

#[derive(Debug, Error)]
pub(crate) enum OAuthHttpError {
    #[error("oauth token request build failed: {reason}")]
    RequestBuild { reason: String },
    #[error("oauth token request failed: {reason}")]
    Request { reason: String },
    #[error("oauth token response body failed: {reason}")]
    ResponseBody { reason: String },
}

#[async_trait]
pub(crate) trait OAuthHttpClient: Send + Sync + fmt::Debug {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError>;
}

#[derive(Clone)]
pub(crate) struct HyperOAuthHttpClient {
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Full<Bytes>,
    >,
}

impl HyperOAuthHttpClient {
    pub(crate) fn new() -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }
}

impl fmt::Debug for HyperOAuthHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("HyperOAuthHttpClient").finish()
    }
}

#[async_trait]
impl OAuthHttpClient for HyperOAuthHttpClient {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError> {
        let body = request.form_body.expose_secret().to_owned();
        let content_length = HeaderValue::from_str(&body.len().to_string()).map_err(|source| {
            OAuthHttpError::RequestBuild {
                reason: source.to_string(),
            }
        })?;
        let http_request = Request::post(request.endpoint.as_str())
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, content_length)
            .body(Full::new(Bytes::from(body)))
            .map_err(|source| OAuthHttpError::RequestBuild {
                reason: source.to_string(),
            })?;

        let response =
            self.client
                .request(http_request)
                .await
                .map_err(|source| OAuthHttpError::Request {
                    reason: source.to_string(),
                })?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|source| OAuthHttpError::ResponseBody {
                reason: source.to_string(),
            })?
            .to_bytes();

        Ok(OAuthTokenResponse { status, body })
    }
}

#[allow(clippy::too_many_arguments)]
async fn exchange_pkce_code(
    http: &dyn OAuthHttpClient,
    token_url: &Url,
    client_id: &str,
    auth_code: &str,
    state_token: &str,
    code_verifier: &SecretString,
    redirect_uri: &Url,
    now_epoch_secs: u64,
    expires_in: Option<u64>,
) -> Result<OAuthCredentials, OAuthTokenError> {
    let body = form_body(
        client_id,
        code_verifier.expose_secret(),
        auth_code,
        state_token,
        redirect_uri,
        expires_in,
    );
    let verifier_str = code_verifier.expose_secret();
    let verifier_challenge = {
        use base64::Engine;
        use sha2::{Digest, Sha256};
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(verifier_str.as_bytes()))
    };
    tracing::info!(
        token_url = %token_url,
        client_id = %client_id,
        redirect_uri = %redirect_uri,
        code_len = auth_code.len(),
        code_first8 = %auth_code.chars().take(8).collect::<String>(),
        code_has_hash = auth_code.contains('#'),
        state_len = state_token.len(),
        verifier_len = verifier_str.len(),
        verifier_first6 = %verifier_str.chars().take(6).collect::<String>(),
        verifier_sha256_challenge = %verifier_challenge,
        "oauth complete: posting token exchange to anthropic"
    );
    let response = http
        .post_token(OAuthTokenRequest {
            endpoint: token_url.clone(),
            form_body: body,
        })
        .await
        .map_err(|source| OAuthTokenError::Http {
            reason: source.to_string(),
        })?;

    if !response.status.is_success() {
        tracing::warn!(
            status = %response.status,
            body = %String::from_utf8_lossy(&response.body),
            "anthropic token endpoint rejected oauth exchange"
        );
        let (code, description) = parse_oauth_error_body(&response.body);
        return Err(OAuthTokenError::TokenEndpoint {
            status: response.status,
            code,
            description,
        });
    }

    parse_token_response(response.body, now_epoch_secs)
}

fn parse_oauth_error_body(body: &[u8]) -> (Option<String>, Option<String>) {
    #[derive(Deserialize)]
    struct ErrorBody {
        error: Option<String>,
        error_description: Option<String>,
        message: Option<String>,
    }
    let Ok(parsed) = serde_json::from_slice::<ErrorBody>(body) else {
        return (None, None);
    };
    let description = parsed.error_description.or(parsed.message);
    (parsed.error, description)
}

/// Parses the token endpoint body into credentials. `expires_at` is always
/// derived from the `expires_in` the server actually returned (via
/// [`refreshed_token_parts`], which computes `now + response.expires_in`);
/// the lifetime cc-lb requested must never be substituted for it, because
/// Anthropic may silently grant a shorter lifetime than asked for.
fn parse_token_response(
    body: Bytes,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, OAuthTokenError> {
    let parsed = parse_token_endpoint_response(&body).map_err(|source| OAuthTokenError::Json {
        reason: source.to_string(),
    })?;
    let refreshed = refreshed_token_parts(
        ExistingTokenParts {
            refresh_token: String::new(),
            refresh_token_expires_at_unix_secs: None,
            scopes: Vec::new(),
        },
        parsed,
        now_epoch_secs,
    );

    Ok(OAuthCredentials {
        access_token: refreshed.access_token,
        refresh_token: refreshed.refresh_token,
        expires_at: refreshed.expires_at_unix_secs,
        refresh_token_expires_at_unix_secs: refreshed.refresh_token_expires_at_unix_secs,
        scopes: refreshed.scopes,
    })
}

fn form_body(
    client_id: &str,
    code_verifier: &str,
    auth_code: &str,
    state_token: &str,
    redirect_uri: &Url,
    expires_in: Option<u64>,
) -> SecretString {
    let mut payload = serde_json::json!({
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri.as_str(),
        "client_id": client_id,
        "code_verifier": code_verifier,
        "state": state_token,
    });
    if let Some(expires_in) = expires_in {
        payload["expires_in"] = serde_json::json!(expires_in);
    }
    SecretString::new(payload.to_string().into_boxed_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token_endpoint_error(
        status: StatusCode,
        code: Option<&str>,
        description: Option<&str>,
    ) -> OAuthTokenError {
        OAuthTokenError::TokenEndpoint {
            status,
            code: code.map(str::to_owned),
            description: description.map(str::to_owned),
        }
    }

    #[test]
    fn long_lived_rejection_matches_expiry_invalid_request() {
        for description in [
            "Invalid expiry for scope",
            "Custom expires_in not allowed for scope 'user:mcp_servers'",
        ] {
            let error = token_endpoint_error(
                StatusCode::BAD_REQUEST,
                Some("invalid_request"),
                Some(description),
            );
            assert!(is_long_lived_rejection(&error), "{description}");
        }
    }

    #[test]
    fn long_lived_rejection_ignores_other_failures() {
        for error in [
            token_endpoint_error(
                StatusCode::BAD_REQUEST,
                Some("invalid_grant"),
                Some("The authorization code has expired"),
            ),
            token_endpoint_error(
                StatusCode::UNAUTHORIZED,
                Some("invalid_request"),
                Some("Invalid expiry for scope"),
            ),
            token_endpoint_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                Some("invalid_request"),
                Some("Invalid expiry for scope"),
            ),
            token_endpoint_error(StatusCode::BAD_REQUEST, Some("invalid_request"), None),
        ] {
            assert!(!is_long_lived_rejection(&error), "{error}");
        }
    }

    #[test]
    fn non_inference_scope_detection() {
        assert!(!requests_non_inference_scope(&[
            "user:profile".to_owned(),
            "user:inference".to_owned(),
        ]));
        assert!(!requests_non_inference_scope(&[]));
        for scope in [
            "org:create_api_key",
            "user:sessions:claude_code",
            "user:mcp_servers",
        ] {
            assert!(
                requests_non_inference_scope(&[
                    "user:profile".to_owned(),
                    "user:inference".to_owned(),
                    scope.to_owned(),
                ]),
                "{scope}"
            );
        }
    }

    #[test]
    fn long_lived_grant_boundary() {
        assert!(!long_lived_grant_is_usable(
            cc_lb_config::LONG_LIVED_MIN_GRANT_SECS
        ));
        assert!(long_lived_grant_is_usable(
            cc_lb_config::LONG_LIVED_MIN_GRANT_SECS + 1
        ));
        // An 8-hour grant is far below the threshold and must demote.
        assert!(!long_lived_grant_is_usable(8 * 60 * 60));
    }
}
