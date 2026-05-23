#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::digest;
use serde::Deserialize;
use serde_json::json;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct AppState {
    codes: Arc<Mutex<HashMap<String, CodeRecord>>>,
}

#[derive(Clone, Debug)]
struct CodeRecord {
    code_challenge: String,
    redirect_uri: String,
    state: Option<String>,
}

pub fn app() -> Router {
    Router::new()
        .route("/oauth/authorize", get(authorize))
        .route("/v1/oauth/token", post(token))
        .route("/oauth/token", post(token))
        .with_state(AppState::default())
}

#[derive(Debug, Deserialize)]
struct AuthorizeQuery {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    state: Option<String>,
    scope: Option<String>,
}

async fn authorize(State(state): State<AppState>, Query(query): Query<AuthorizeQuery>) -> Response {
    let _client_and_scope = (&query.client_id, &query.scope);
    if query.response_type != "code" || query.code_challenge_method != "S256" {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    }

    let code = auth_code(query.state.as_deref(), &query.code_challenge);
    let record = CodeRecord {
        code_challenge: query.code_challenge,
        redirect_uri: query.redirect_uri.clone(),
        state: query.state.clone(),
    };
    match state.codes.lock() {
        Ok(mut codes) => {
            codes.insert(code.clone(), record);
        }
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }

    let mut redirect = match Url::parse(&query.redirect_uri) {
        Ok(url) => url,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "invalid_redirect_uri"),
    };
    {
        let mut pairs = redirect.query_pairs_mut();
        pairs.append_pair("code", &code);
        if let Some(state_token) = query.state {
            pairs.append_pair("state", &state_token);
        }
    }

    redirect_response(redirect.as_str())
}

#[derive(Debug, Deserialize)]
struct TokenForm {
    grant_type: String,
    client_id: String,
    code: Option<String>,
    code_verifier: Option<String>,
    redirect_uri: Option<String>,
    refresh_token: Option<String>,
}

async fn token(State(state): State<AppState>, Form(form): Form<TokenForm>) -> Response {
    let _client_id = &form.client_id;
    match form.grant_type.as_str() {
        "authorization_code" => exchange_code(state, form),
        "refresh_token" => refresh_token(form),
        _ => json_error(StatusCode::BAD_REQUEST, "unsupported_grant_type"),
    }
}

fn exchange_code(state: AppState, form: TokenForm) -> Response {
    let Some(code) = form.code else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(verifier) = form.code_verifier else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(redirect_uri) = form.redirect_uri else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };

    let record = match state.codes.lock() {
        Ok(mut codes) => codes.remove(&code),
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    };
    let Some(record) = record else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };

    if redirect_uri != record.redirect_uri || pkce_s256(&verifier) != record.code_challenge {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }

    let token_seed = record.state.as_deref().unwrap_or(&code);
    token_response(
        &format!("sk-ant-oat01-MOCK-{code}"),
        &format!("sk-ant-ort01-MOCK-{token_seed}"),
    )
}

fn refresh_token(form: TokenForm) -> Response {
    let Some(refresh_token) = form.refresh_token else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if !refresh_token.starts_with("sk-ant-ort01-MOCK-") {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }
    let suffix = refresh_token.trim_start_matches("sk-ant-ort01-MOCK-");
    token_response(
        &format!("sk-ant-oat01-MOCK-refresh-{suffix}"),
        &format!("sk-ant-ort01-MOCK-refresh-{suffix}"),
    )
}

fn token_response(access_token: &str, refresh_token: &str) -> Response {
    Json(json!({
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": 3600,
        "scope": "messages files"
    }))
    .into_response()
}

fn redirect_response(location: &str) -> Response {
    let mut headers = HeaderMap::new();
    match HeaderValue::from_str(location) {
        Ok(value) => {
            headers.insert(http::header::LOCATION, value);
            (StatusCode::FOUND, headers).into_response()
        }
        Err(_) => json_error(StatusCode::INTERNAL_SERVER_ERROR, "redirect_build_failed"),
    }
}

fn json_error(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

fn auth_code(state: Option<&str>, challenge: &str) -> String {
    let seed = state.unwrap_or(challenge);
    let digest = digest::digest(&digest::SHA256, seed.as_bytes());
    let encoded = URL_SAFE_NO_PAD.encode(digest.as_ref());
    let short = encoded.get(..16).unwrap_or(encoded.as_str());
    format!("alice-{short}")
}

fn pkce_s256(verifier: &str) -> String {
    let digest = digest::digest(&digest::SHA256, verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest.as_ref())
}
