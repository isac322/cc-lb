#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::digest;
use serde::Deserialize;
use serde_json::json;
use url::Url;

/// Token lifetime the mock reports when a request does not ask for a custom
/// `expires_in`.
const DEFAULT_TOKEN_EXPIRES_IN_SECS: u64 = 3600;

#[derive(Clone, Debug, Default)]
pub struct AppState {
    codes: Arc<Mutex<HashMap<String, CodeRecord>>>,
    /// `Some` rejects authorization-code exchanges that carry `expires_in`.
    expires_in_rejection: Option<(ExpiresInRejection, RejectionShape)>,
    /// `Some` caps the granted `expires_in`: the exchange succeeds but the
    /// response reports a shorter lifetime than requested.
    expires_in_cap: Option<u64>,
    /// `scope` parameter of every accepted `/oauth/authorize` request.
    requested_scopes: Arc<Mutex<Vec<Option<String>>>>,
    /// `expires_in` parameter of every `/oauth/token` request.
    requested_expires_in: Arc<Mutex<Vec<Option<u64>>>>,
}

impl AppState {
    /// Configures the token endpoint to reject authorization-code exchanges
    /// that carry a custom `expires_in`, mirroring Anthropic's refusal of
    /// long-lived grants. The authorization code is not consumed, so a retry
    /// without `expires_in` still succeeds.
    pub fn with_expires_in_rejection(
        mut self,
        rejection: ExpiresInRejection,
        shape: RejectionShape,
    ) -> Self {
        self.expires_in_rejection = Some((rejection, shape));
        self
    }

    /// Configures the token endpoint to silently clamp the granted
    /// `expires_in` to at most `cap` seconds, mirroring a provider that
    /// accepts the exchange but grants a shorter lifetime than requested.
    /// The exchange still succeeds; only the reported lifetime shrinks.
    pub fn with_expires_in_cap(mut self, cap: u64) -> Self {
        self.expires_in_cap = Some(cap);
        self
    }

    /// `expires_in` parameter of every `/oauth/token` request, in request
    /// order; `None` entries are requests that did not send `expires_in`.
    pub fn requested_expires_in(&self) -> Vec<Option<u64>> {
        self.requested_expires_in
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }

    /// `scope` parameter of every accepted `/oauth/authorize` request, in
    /// request order.
    pub fn requested_scopes(&self) -> Vec<Option<String>> {
        self.requested_scopes
            .lock()
            .map(|scopes| scopes.clone())
            .unwrap_or_default()
    }
}

/// When the mock rejects an authorization-code exchange carrying
/// `expires_in`.
#[derive(Clone, Debug)]
pub enum ExpiresInRejection {
    /// Reject every exchange that carries `expires_in`.
    AnyExpiresIn,
    /// Reject only when the requested scope set contains this scope (for
    /// example `user:mcp_servers` or `org:create_api_key`).
    Scope(String),
}

/// Error body the mock emits when it rejects `expires_in`.
#[derive(Clone, Debug)]
pub enum RejectionShape {
    /// `{"error":"invalid_request","error_description":"Invalid expiry for scope"}`
    InvalidExpiryForScope,
    /// `{"error":"invalid_request","error_description":"Custom expires_in not
    /// allowed for scope '<scope>'"}` with the given scope name.
    CustomExpiresInNotAllowed(String),
}

#[derive(Clone, Debug)]
struct CodeRecord {
    code_challenge: String,
    redirect_uri: String,
    state: Option<String>,
    scope: Option<String>,
}

pub fn app() -> Router {
    app_with_state(AppState::default())
}

pub fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/oauth/authorize", get(authorize))
        .route("/v1/oauth/token", post(token))
        .route("/oauth/token", post(token))
        .with_state(state)
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
    let _client_id = &query.client_id;
    if query.response_type != "code" || query.code_challenge_method != "S256" {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    }

    let code = auth_code(query.state.as_deref(), &query.code_challenge);
    let record = CodeRecord {
        code_challenge: query.code_challenge,
        redirect_uri: query.redirect_uri.clone(),
        state: query.state.clone(),
        scope: query.scope.clone(),
    };
    match state.codes.lock() {
        Ok(mut codes) => {
            codes.insert(code.clone(), record);
        }
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
    if let Ok(mut scopes) = state.requested_scopes.lock() {
        scopes.push(query.scope.clone());
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
    /// Custom access-token lifetime requested by the client (long-lived
    /// grants); echoed back in the token response unless rejected.
    expires_in: Option<u64>,
    /// Optional scope set on the token request itself.
    scope: Option<String>,
}

async fn token(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let is_json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.trim()
                .to_ascii_lowercase()
                .starts_with("application/json")
        })
        .unwrap_or(false);
    let form: TokenForm = if is_json {
        match serde_json::from_slice(&body) {
            Ok(f) => f,
            Err(_) => return json_error(StatusCode::BAD_REQUEST, "invalid_request"),
        }
    } else {
        match serde_urlencoded::from_bytes(&body) {
            Ok(f) => f,
            Err(_) => return json_error(StatusCode::BAD_REQUEST, "invalid_request"),
        }
    };
    let _client_id = &form.client_id;
    if let Ok(mut requests) = state.requested_expires_in.lock() {
        requests.push(form.expires_in);
    }
    match form.grant_type.as_str() {
        "authorization_code" => exchange_code(state, form),
        "refresh_token" => refresh_token(&state, form),
        _ => json_error(StatusCode::BAD_REQUEST, "unsupported_grant_type"),
    }
}

fn exchange_code(state: AppState, form: TokenForm) -> Response {
    let Some(code) = form.code.as_deref() else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(verifier) = form.code_verifier.as_deref() else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(redirect_uri) = form.redirect_uri.as_deref() else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };

    // The authorization code is consumed only on a successful exchange: a
    // rejected or invalid attempt must leave it redeemable so the caller can
    // retry without `expires_in`, matching Anthropic's behaviour.
    let record = match state.codes.lock() {
        Ok(codes) => codes.get(code).cloned(),
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    };
    let Some(record) = record else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };

    if redirect_uri != record.redirect_uri || pkce_s256(verifier) != record.code_challenge {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }

    if let Some(rejection) = expires_in_rejection_response(&state, &form, &record) {
        return rejection;
    }

    match state.codes.lock() {
        Ok(mut codes) => {
            codes.remove(code);
        }
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }

    let token_seed = record.state.as_deref().unwrap_or(code);
    token_response(
        &format!("sk-ant-oat01-MOCK-{code}"),
        &format!("sk-ant-ort01-MOCK-{token_seed}"),
        granted_expires_in(&state, form.expires_in),
    )
}

/// `Some` response when the configured policy rejects this exchange's
/// `expires_in`; `None` when the exchange may proceed.
fn expires_in_rejection_response(
    state: &AppState,
    form: &TokenForm,
    record: &CodeRecord,
) -> Option<Response> {
    let (rejection, shape) = state.expires_in_rejection.as_ref()?;
    form.expires_in?;
    let requested_scope = form.scope.as_deref().or(record.scope.as_deref());
    let rejected = match rejection {
        ExpiresInRejection::AnyExpiresIn => true,
        ExpiresInRejection::Scope(scope) => requested_scope
            .map(|scopes| scopes.split_whitespace().any(|entry| entry == scope))
            .unwrap_or(false),
    };
    if !rejected {
        return None;
    }
    let description = match shape {
        RejectionShape::InvalidExpiryForScope => "Invalid expiry for scope".to_owned(),
        RejectionShape::CustomExpiresInNotAllowed(scope) => {
            format!("Custom expires_in not allowed for scope '{scope}'")
        }
    };
    Some(
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_request",
                "error_description": description,
            })),
        )
            .into_response(),
    )
}

fn refresh_token(state: &AppState, form: TokenForm) -> Response {
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
        granted_expires_in(state, form.expires_in),
    )
}

/// Lifetime the mock grants for a token request: the requested `expires_in`
/// (or the default) capped by the configured clamp policy, if any.
fn granted_expires_in(state: &AppState, requested: Option<u64>) -> u64 {
    let granted = requested.unwrap_or(DEFAULT_TOKEN_EXPIRES_IN_SECS);
    match state.expires_in_cap {
        Some(cap) => granted.min(cap),
        None => granted,
    }
}

fn token_response(access_token: &str, refresh_token: &str, expires_in: u64) -> Response {
    Json(json!({
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
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
