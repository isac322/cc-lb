use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
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

    PkceHandshake {
        authorize_url,
        client_id,
        token_endpoint,
        scopes,
        redirect_uri,
        verifier: SecretString::new(verifier.into_secret().into_boxed_str()),
    }
}

pub(crate) async fn complete_pkce_flow(
    handshake: PkceHandshake,
    auth_code: String,
    state_token: String,
    http: Arc<dyn OAuthHttpClient>,
) -> Result<OAuthCredentials, OAuthTokenError> {
    exchange_pkce_code(
        http.as_ref(),
        handshake.token_endpoint.url(),
        handshake.client_id.as_str(),
        &auth_code,
        &state_token,
        &handshake.verifier,
        &handshake.redirect_uri,
        now_epoch_secs(),
    )
    .await
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
        query.append_pair("response_type", "code");
        query.append_pair("client_id", client_id.as_str());
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
    #[error("oauth token endpoint returned status {status}")]
    TokenEndpoint { status: StatusCode },
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
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
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
) -> Result<OAuthCredentials, OAuthTokenError> {
    let body = form_body(
        client_id,
        code_verifier.expose_secret(),
        auth_code,
        state_token,
        redirect_uri,
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
        return Err(OAuthTokenError::TokenEndpoint {
            status: response.status,
        });
    }

    parse_token_response(response.body, now_epoch_secs)
}

fn parse_token_response(
    body: Bytes,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, OAuthTokenError> {
    #[derive(Debug, Deserialize)]
    struct TokenEndpointJson {
        access_token: String,
        refresh_token: Option<String>,
        expires_in: u64,
        scope: Option<String>,
    }

    let parsed: TokenEndpointJson =
        serde_json::from_slice(&body).map_err(|source| OAuthTokenError::Json {
            reason: source.to_string(),
        })?;
    let scopes = parsed
        .scope
        .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_default();

    Ok(OAuthCredentials {
        access_token: parsed.access_token,
        refresh_token: parsed.refresh_token.unwrap_or_default(),
        expires_at: now_epoch_secs.saturating_add(parsed.expires_in),
        scopes,
    })
}

fn form_body(
    client_id: &str,
    code_verifier: &str,
    auth_code: &str,
    state_token: &str,
    redirect_uri: &Url,
) -> SecretString {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "authorization_code");
    serializer.append_pair("code", auth_code);
    serializer.append_pair("state", state_token);
    serializer.append_pair("code_verifier", code_verifier);
    serializer.append_pair("redirect_uri", redirect_uri.as_str());
    serializer.append_pair("client_id", client_id);
    SecretString::new(serializer.finish().into_boxed_str())
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
