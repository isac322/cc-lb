use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_storage_redb::OAuthCredentials;
use oauth2::{AuthUrl, ClientId, PkceCodeChallenge, TokenUrl};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::http_client::OAuthHttpClient;
use crate::refresh::{RefreshError, exchange_pkce_code};

#[derive(Clone)]
pub struct PkceHandshake {
    pub authorize_url: Url,
    pub client_id: ClientId,
    pub token_endpoint: TokenUrl,
    pub scopes: Vec<String>,
    pub redirect_uri: Url,
    verifier: SecretString,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PkceHandshakeState {
    pub authorize_url: Url,
    pub client_id: String,
    pub token_endpoint: Url,
    pub scopes: Vec<String>,
    pub redirect_uri: Url,
    pub verifier: String,
}

impl PkceHandshake {
    pub fn into_state(self) -> PkceHandshakeState {
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
    pub fn into_handshake(self) -> Result<PkceHandshake, url::ParseError> {
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

pub fn start_pkce_flow(
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

pub async fn complete_pkce_flow(
    handshake: PkceHandshake,
    auth_code: String,
    http: Arc<dyn OAuthHttpClient>,
) -> Result<OAuthCredentials, RefreshError> {
    exchange_pkce_code(
        http.as_ref(),
        handshake.token_endpoint.url(),
        handshake.client_id.as_str(),
        &auth_code,
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

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
