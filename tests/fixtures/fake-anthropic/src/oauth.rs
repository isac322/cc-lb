use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use crate::routes::{AppState, with_fixture_headers};

#[derive(Debug, Default)]
pub struct OAuthState {
    codes: Mutex<HashMap<String, CodeRecord>>,
    refresh_tokens: Mutex<HashMap<String, RefreshTokenRecord>>,
    refresh_history: Mutex<Vec<RefreshHistoryEntry>>,
}

#[derive(Clone, Debug)]
struct CodeRecord {
    code_challenge: String,
    redirect_uri: String,
    scope: Option<String>,
}

#[derive(Clone, Debug)]
struct RefreshTokenRecord {
    scope: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RefreshHistoryEntry {
    old_refresh_token_fingerprint: String,
    new_refresh_token_fingerprint: String,
    access_token_fingerprint: String,
    issued_at_unix_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct AuthorizeQuery {
    response_type: String,
    client_id: Option<String>,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    state: Option<String>,
    scope: Option<String>,
}

pub async fn authorize(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuthorizeQuery>,
) -> Response {
    let _client_id = &query.client_id;
    if query.response_type != "code" || query.code_challenge_method != "S256" {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    }

    let mut redirect = match Url::parse(&query.redirect_uri) {
        Ok(url) => url,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "invalid_redirect_uri"),
    };

    let code = random_token("code_");
    let record = CodeRecord {
        code_challenge: query.code_challenge,
        redirect_uri: query.redirect_uri.clone(),
        scope: query.scope,
    };
    match state.oauth.codes.lock() {
        Ok(mut codes) => {
            codes.insert(code.clone(), record);
        }
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }

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
pub struct TokenForm {
    grant_type: String,
    client_id: Option<String>,
    code: Option<String>,
    code_verifier: Option<String>,
    redirect_uri: Option<String>,
    refresh_token: Option<String>,
}

pub async fn token(State(state): State<Arc<AppState>>, Form(form): Form<TokenForm>) -> Response {
    let _client_id = &form.client_id;
    match form.grant_type.as_str() {
        "authorization_code" => exchange_code(state, form),
        "refresh_token" => rotate_refresh_token(state, form),
        _ => json_error(StatusCode::BAD_REQUEST, "unsupported_grant_type"),
    }
}

pub async fn refresh_history(State(state): State<Arc<AppState>>) -> Response {
    let history = match state.oauth.refresh_history.lock() {
        Ok(history) => history.clone(),
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    };

    json_response(
        StatusCode::OK,
        json!({
            "refreshes": history,
        }),
    )
}

fn exchange_code(state: Arc<AppState>, form: TokenForm) -> Response {
    let Some(code) = form.code else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(verifier) = form.code_verifier else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(redirect_uri) = form.redirect_uri else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };

    let record = match state.oauth.codes.lock() {
        Ok(mut codes) => codes.remove(&code),
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    };
    let Some(record) = record else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };

    if redirect_uri != record.redirect_uri || pkce_s256(&verifier) != record.code_challenge {
        return json_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }

    let access_token = random_token("sk-ant-oat01-");
    let refresh_token = random_token("sk-ant-ort01-");
    match state.oauth.refresh_tokens.lock() {
        Ok(mut refresh_tokens) => {
            refresh_tokens.insert(
                refresh_token.clone(),
                RefreshTokenRecord {
                    scope: record.scope.clone(),
                },
            );
        }
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }

    token_response(
        &access_token,
        &refresh_token,
        record.scope.as_deref(),
        state.config.tokens_expire_in,
    )
}

fn rotate_refresh_token(state: Arc<AppState>, form: TokenForm) -> Response {
    let Some(old_refresh_token) = form.refresh_token else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_request");
    };

    let record = match state.oauth.refresh_tokens.lock() {
        Ok(mut refresh_tokens) => match refresh_tokens.remove(&old_refresh_token) {
            Some(record) => record,
            None => return json_error(StatusCode::BAD_REQUEST, "invalid_grant"),
        },
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    };

    let access_token = random_token("sk-ant-oat01-");
    let new_refresh_token = random_token("sk-ant-ort01-");
    match state.oauth.refresh_tokens.lock() {
        Ok(mut refresh_tokens) => {
            refresh_tokens.insert(new_refresh_token.clone(), record.clone());
        }
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }

    let history_entry = RefreshHistoryEntry {
        old_refresh_token_fingerprint: token_fingerprint(&old_refresh_token),
        new_refresh_token_fingerprint: token_fingerprint(&new_refresh_token),
        access_token_fingerprint: token_fingerprint(&access_token),
        issued_at_unix_secs: now_unix_secs(),
    };
    match state.oauth.refresh_history.lock() {
        Ok(mut history) => history.push(history_entry),
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }

    token_response(
        &access_token,
        &new_refresh_token,
        record.scope.as_deref(),
        state.config.tokens_expire_in,
    )
}

fn token_response(
    access_token: &str,
    refresh_token: &str,
    scope: Option<&str>,
    expires_in: u64,
) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": expires_in,
            "scope": scope.unwrap_or("messages files"),
            "token_type": "Bearer",
        }),
    )
}

fn redirect_response(location: &str) -> Response {
    let mut headers = HeaderMap::new();
    match HeaderValue::from_str(location) {
        Ok(value) => {
            headers.insert(header::LOCATION, value);
            with_fixture_headers((StatusCode::FOUND, headers).into_response())
        }
        Err(_) => json_error(StatusCode::INTERNAL_SERVER_ERROR, "redirect_build_failed"),
    }
}

fn json_response(status: StatusCode, body: Value) -> Response {
    let mut response = Json(body).into_response();
    *response.status_mut() = status;
    with_fixture_headers(response)
}

fn json_error(status: StatusCode, error: &str) -> Response {
    json_response(status, json!({ "error": error }))
}

fn random_token(prefix: &str) -> String {
    format!("{prefix}{}", Uuid::new_v4().simple())
}

fn pkce_s256(verifier: &str) -> String {
    let digest = digest::digest(&digest::SHA256, verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest.as_ref())
}

fn token_fingerprint(token: &str) -> String {
    let digest = digest::digest(&digest::SHA256, token.as_bytes());
    let encoded = URL_SAFE_NO_PAD.encode(digest.as_ref());
    encoded.get(..12).unwrap_or(encoded.as_str()).to_owned()
}

fn now_unix_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use http::{Request, StatusCode, header};
    use serde_json::Value;
    use tower::ServiceExt;
    use url::Url;

    use crate::routes::{AppConfig, app};

    use super::pkce_s256;

    const REDIRECT_URI: &str = "http://localhost/callback";

    #[tokio::test]
    async fn authorize_redirects_with_code() {
        let app = app(AppConfig::default());
        let response = authorize_request(app, "test-verifier", Some("state-123")).await;

        assert_eq!(response.status(), StatusCode::FOUND);
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .expect("location header");
        let redirect = Url::parse(location).expect("valid redirect location");
        assert_eq!(redirect.as_str().split('?').next(), Some(REDIRECT_URI));

        let params = redirect
            .query_pairs()
            .into_owned()
            .collect::<std::collections::HashMap<_, _>>();
        assert!(
            params
                .get("code")
                .is_some_and(|code| code.starts_with("code_"))
        );
        assert_eq!(params.get("state").map(String::as_str), Some("state-123"));
    }

    #[tokio::test]
    async fn token_code_exchange() {
        let app = app(AppConfig::default());
        let verifier = "exchange-verifier";
        let code = authorize_code(app.clone(), verifier).await;
        let tokens = exchange_code(app, &code, verifier).await;

        assert_token_response(&tokens);
    }

    #[tokio::test]
    async fn token_pkce_verifier_mismatch_rejected() {
        let app = app(AppConfig::default());
        let code = authorize_code(app.clone(), "correct-verifier").await;
        let response = token_form(
            app,
            &[
                ("grant_type", "authorization_code"),
                ("client_id", "test-client"),
                ("redirect_uri", REDIRECT_URI),
                ("code", &code),
                ("code_verifier", "wrong-verifier"),
            ],
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert_eq!(body["error"], "invalid_grant");
    }

    #[tokio::test]
    async fn token_refresh_rotates_both() {
        let app = app(AppConfig::default());
        let code = authorize_code(app.clone(), "refresh-verifier").await;
        let tokens = exchange_code(app.clone(), &code, "refresh-verifier").await;

        let refresh = refresh(
            app.clone(),
            tokens["refresh_token"].as_str().expect("refresh token"),
        )
        .await;
        assert_ne!(refresh["access_token"], tokens["access_token"]);
        assert_ne!(refresh["refresh_token"], tokens["refresh_token"]);

        let history = refresh_history(app).await;
        assert_eq!(history["refreshes"].as_array().expect("refreshes").len(), 1);
    }

    #[tokio::test]
    async fn token_refresh_invalidates_old_refresh() {
        let app = app(AppConfig::default());
        let code = authorize_code(app.clone(), "invalidate-verifier").await;
        let tokens = exchange_code(app.clone(), &code, "invalidate-verifier").await;
        let old_refresh = tokens["refresh_token"].as_str().expect("refresh token");

        let rotated = refresh(app.clone(), old_refresh).await;
        assert_token_response(&rotated);

        let response = token_form(
            app,
            &[
                ("grant_type", "refresh_token"),
                ("client_id", "test-client"),
                ("refresh_token", old_refresh),
            ],
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert_eq!(body["error"], "invalid_grant");
    }

    async fn authorize_code(app: axum::Router, verifier: &str) -> String {
        let response = authorize_request(app, verifier, None).await;
        assert_eq!(response.status(), StatusCode::FOUND);

        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .expect("location header");
        let redirect = Url::parse(location).expect("valid redirect location");
        redirect
            .query_pairs()
            .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
            .expect("authorization code")
    }

    async fn authorize_request(
        app: axum::Router,
        verifier: &str,
        state: Option<&str>,
    ) -> http::Response<Body> {
        let challenge = pkce_s256(verifier);
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("response_type", "code");
        serializer.append_pair("client_id", "test-client");
        serializer.append_pair("redirect_uri", REDIRECT_URI);
        serializer.append_pair("code_challenge", &challenge);
        serializer.append_pair("code_challenge_method", "S256");
        serializer.append_pair("scope", "messages files");
        if let Some(state) = state {
            serializer.append_pair("state", state);
        }
        let query = serializer.finish();

        app.oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/oauth/authorize?{query}"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("response returned")
    }

    async fn exchange_code(app: axum::Router, code: &str, verifier: &str) -> Value {
        let response = token_form(
            app,
            &[
                ("grant_type", "authorization_code"),
                ("client_id", "test-client"),
                ("redirect_uri", REDIRECT_URI),
                ("code", code),
                ("code_verifier", verifier),
            ],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        json_body(response).await
    }

    async fn refresh(app: axum::Router, refresh_token: &str) -> Value {
        let response = token_form(
            app,
            &[
                ("grant_type", "refresh_token"),
                ("client_id", "test-client"),
                ("refresh_token", refresh_token),
            ],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        json_body(response).await
    }

    async fn token_form(app: axum::Router, fields: &[(&str, &str)]) -> http::Response<Body> {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (name, value) in fields {
            serializer.append_pair(name, value);
        }

        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(serializer.finish()))
                .expect("request builds"),
        )
        .await
        .expect("response returned")
    }

    async fn refresh_history(app: axum::Router) -> Value {
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__refresh_history")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("response returned");
        assert_eq!(response.status(), StatusCode::OK);
        json_body(response).await
    }

    async fn json_body(response: http::Response<Body>) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        serde_json::from_slice(&body).expect("json body")
    }

    fn assert_token_response(tokens: &Value) {
        assert!(
            tokens["access_token"]
                .as_str()
                .is_some_and(|token| token.starts_with("sk-ant-oat01-"))
        );
        assert!(
            tokens["refresh_token"]
                .as_str()
                .is_some_and(|token| token.starts_with("sk-ant-ort01-"))
        );
        assert_eq!(tokens["expires_in"], 3600);
        assert_eq!(tokens["token_type"], "Bearer");
    }
}
