use bytes::Bytes;
use cc_lb_oauth_protocol::{
    ExistingTokenParts, parse_token_endpoint_response, refreshed_token_parts,
};
use cc_lb_storage_api::OAuthCredentials;
use http::StatusCode;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;
use url::Url;

use crate::http_client::{APPLICATION_JSON, OAuthHttpClient, OAuthTokenRequest};

pub const REFRESH_SOFT_BUFFER_SECS: u64 = 300;

#[derive(Debug, Error)]
pub enum RefreshError {
    #[error("oauth token endpoint returned status {status}")]
    TokenEndpoint { status: StatusCode },
    #[error("oauth token endpoint request failed: {reason}")]
    Http { reason: String },
    #[error("oauth token response json failed: {reason}")]
    Json { reason: String },
}

pub async fn exchange_pkce_code(
    http: &dyn OAuthHttpClient,
    token_url: &Url,
    client_id: &str,
    auth_code: &str,
    code_verifier: &SecretString,
    redirect_uri: &Url,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, RefreshError> {
    let body = pkce_token_body(
        client_id,
        code_verifier.expose_secret(),
        auth_code,
        redirect_uri,
    );
    let response = http
        .post_token(OAuthTokenRequest {
            endpoint: token_url.clone(),
            form_body: body,
            content_type: APPLICATION_JSON,
        })
        .await
        .map_err(|source| RefreshError::Http {
            reason: source.to_string(),
        })?;

    if !response.status.is_success() {
        return Err(RefreshError::TokenEndpoint {
            status: response.status,
        });
    }

    let placeholder = OAuthCredentials {
        access_token: String::new(),
        refresh_token: String::new(),
        expires_at: now_epoch_secs,
        refresh_token_expires_at_unix_secs: None,
        scopes: Vec::new(),
    };
    parse_token_response(response.body, &placeholder, now_epoch_secs)
}

fn parse_token_response(
    body: Bytes,
    existing: &OAuthCredentials,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, RefreshError> {
    let parsed = parse_token_endpoint_response(&body).map_err(|source| RefreshError::Json {
        reason: source.to_string(),
    })?;
    let refreshed = refreshed_token_parts(
        ExistingTokenParts {
            refresh_token: existing.refresh_token.clone(),
            refresh_token_expires_at_unix_secs: existing.refresh_token_expires_at_unix_secs,
            scopes: existing.scopes.clone(),
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

fn pkce_token_body(
    client_id: &str,
    code_verifier: &str,
    auth_code: &str,
    redirect_uri: &Url,
) -> SecretString {
    let payload = serde_json::json!({
        "grant_type": "authorization_code",
        "code": auth_code,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri.as_str(),
        "client_id": client_id,
    });
    SecretString::new(payload.to_string().into_boxed_str())
}
