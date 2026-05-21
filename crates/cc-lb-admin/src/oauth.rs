use axum::{extract::State, http::StatusCode, response::Json};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use cc_lb_signer_anthropic_oauth::{
    complete_pkce_flow, start_pkce_flow, HyperOAuthHttpClient, PkceHandshakeState,
};
use oauth2::{AuthUrl, ClientId, TokenUrl};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use url::Url;

use crate::AdminState;

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
) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };
    let state_token = decode_state(&payload.state_token)?;
    if state_token.provider != "anthropic_oauth" {
        return Err(StatusCode::BAD_REQUEST);
    }
    let handshake = state_token
        .handshake
        .into_handshake()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let creds = complete_pkce_flow(
        handshake,
        payload.code,
        Arc::new(HyperOAuthHttpClient::new()),
    )
    .await
    .map_err(|_| StatusCode::BAD_GATEWAY)?;

    storage
        .put_oauth(&state_token.principal_id, &state_token.provider, &creds)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({ "status": "ok" })))
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
    config
        .plugins
        .authn_plugin
        .as_ref()
        .and_then(|plugin| plugin.config.get("client_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| std::env::var("CC_LB_OAUTH_CLIENT_ID").ok())
        .filter(|value| !value.trim().is_empty())
}
