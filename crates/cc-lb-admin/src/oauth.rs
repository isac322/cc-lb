use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_storage_redb::{OAuthCredentials as RedbOAuthCredentials, StorageError};
use oauth2::{AuthUrl, ClientId, TokenUrl};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use url::Url;

use crate::AdminState;
use crate::oauth_pkce::{
    HyperOAuthHttpClient, PkceHandshakeState, complete_pkce_flow, start_pkce_flow,
};

#[derive(Deserialize)]
pub struct OauthStartRequest {
    principal_id: String,
    provider: String,
}

pub async fn start_oauth(
    State(state): State<AdminState>,
    Json(payload): Json<OauthStartRequest>,
) -> Result<Json<Value>, StatusCode> {
    if payload.provider != "anthropic_oauth" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let config = state.config.current_config();
    let client_id = oauth_client_id(&config).ok_or(StatusCode::BAD_REQUEST)?;
    let issuer_base = config
        .signers
        .anthropic_oauth
        .issuer_base_url
        .trim_end_matches('/');
    let authorize_endpoint = AuthUrl::new(format!("{issuer_base}/oauth/authorize"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let token_endpoint = TokenUrl::new(format!("{issuer_base}/v1/oauth/token"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let redirect_uri = Url::parse(&config.signers.anthropic_oauth.redirect_uri)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let scopes = config.signers.anthropic_oauth.scopes.clone();
    let handshake = start_pkce_flow(
        ClientId::new(client_id),
        authorize_endpoint,
        token_endpoint,
        scopes,
        redirect_uri,
    );
    let mut handshake_state = handshake.into_state();
    let state_token = encode_state(&OauthStateToken {
        principal_id: payload.principal_id,
        provider: payload.provider,
        handshake: handshake_state.clone(),
    })?;
    handshake_state
        .authorize_url
        .query_pairs_mut()
        .append_pair("state", &state_token);

    Ok(Json(json!({
        "authorize_url": handshake_state.authorize_url.to_string(),
        "state_token": state_token,
    })))
}

#[derive(Deserialize)]
pub struct OauthCompleteRequest {
    state_token: String,
    code: String,
}

pub async fn complete_oauth(
    State(state): State<AdminState>,
    Json(payload): Json<OauthCompleteRequest>,
) -> Response {
    let state_token = match decode_state(&payload.state_token) {
        Ok(state_token) => state_token,
        Err(status) => return status.into_response(),
    };
    if state_token.provider != "anthropic_oauth" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let handshake = match state_token.handshake.into_handshake() {
        Ok(handshake) => handshake,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let creds = match complete_pkce_flow(
        handshake,
        payload.code,
        Arc::new(HyperOAuthHttpClient::new()),
    )
    .await
    {
        Ok(creds) => creds,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };

    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let creds = RedbOAuthCredentials {
        access_token: creds.access_token,
        refresh_token: creds.refresh_token,
        expires_at: creds.expires_at,
        scopes: creds.scopes,
    };
    match storage.put_oauth(&state_token.principal_id, &state_token.provider, &creds) {
        Ok(()) => Json(json!({ "status": "ok" })).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "admin oauth storage operation failed");
            storage_error_response(&error)
        }
    }
}

pub async fn oauth_status(
    State(state): State<AdminState>,
    axum::extract::Path(oauth_credential_id): axum::extract::Path<String>,
) -> Response {
    let config = state.config.current_config();
    for (principal_id, principal) in &config.principals {
        let matches_provider = principal
            .credentials_ref
            .as_deref()
            .map(str::trim)
            .is_some_and(|reference| {
                reference == oauth_credential_id
                    || reference
                        .strip_prefix("oauth:")
                        .map(str::trim)
                        .is_some_and(|provider| provider == oauth_credential_id)
            });
        if !matches_provider {
            continue;
        }
        let Some(storage) = state.storage.as_ref() else {
            return StatusCode::NOT_IMPLEMENTED.into_response();
        };
        match storage.get_oauth(principal_id, &oauth_credential_id) {
            Ok(Some(_)) => return Json(json!({ "enrolled": true })).into_response(),
            Ok(None) => {}
            Err(error) => {
                tracing::error!(error = %error, "admin oauth status storage operation failed");
                return storage_error_response(&error);
            }
        }
    }

    if let Some(storage) = state.storage.as_ref() {
        let records = match storage.list_api_keys_all() {
            Ok(records) => records,
            Err(error) => {
                tracing::error!(error = %error, "admin oauth status managed-key scan failed");
                return storage_error_response(&error);
            }
        };
        for (principal_id, _key_id, record) in records {
            if record.upstream_credential_ref != oauth_credential_id {
                continue;
            }
            match storage.get_oauth(&principal_id, &oauth_credential_id) {
                Ok(Some(_)) => return Json(json!({ "enrolled": true })).into_response(),
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(error = %error, "admin oauth status managed-key oauth lookup failed");
                    return storage_error_response(&error);
                }
            }
        }
    }

    Json(json!({ "enrolled": false })).into_response()
}

fn storage_error_response(error: &StorageError) -> Response {
    match error {
        StorageError::RedbStorage(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            Json(json!({ "error": "storage_error" })),
        )
            .into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OauthStateToken {
    principal_id: String,
    provider: String,
    handshake: PkceHandshakeState,
}

fn encode_state(state: &OauthStateToken) -> Result<String, StatusCode> {
    let json = serde_json::to_vec(state).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_state(value: &str) -> Result<OauthStateToken, StatusCode> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_REQUEST)
}

fn oauth_client_id(config: &cc_lb_config::Config) -> Option<String> {
    if !config.signers.anthropic_oauth.client_id.trim().is_empty() {
        return Some(config.signers.anthropic_oauth.client_id.clone());
    }
    std::env::var("CC_LB_OAUTH_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
}
