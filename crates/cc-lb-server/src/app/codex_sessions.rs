use axum::body::Body;
use cc_lb_engine::{
    LifecycleContext, TerminalClassification,
    authn_rail::Authenticated,
    codex_sessions::{CodexSessionService, SessionError},
};
use http::{Response, StatusCode, header::CACHE_CONTROL};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenewalRequest {
    grant_type: String,
    expected_workspace_generation: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeRequest {}

/// Called only after header authentication, registered-purpose authorization,
/// and the bounded body reader. The proof cannot be constructed by this module.
pub(super) async fn handle(
    sessions: &CodexSessionService,
    auth: &Authenticated,
    path: &str,
    body: &[u8],
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    let result = match path {
        "/oauth/token" => match sonic_rs::from_slice::<RenewalRequest>(body) {
            Ok(request) if request.grant_type == "refresh_token" => {
                sessions.renew_session(auth, request.expected_workspace_generation).await.map(|grant| {
                    let payload = json!({
                        "session_id": grant.session_id,
                        "access_token": grant.access_token.expose(),
                        "token_type": "Bearer",
                        "expires_at": grant.expires_at,
                        "session_expires_at": grant.session_expires_at,
                        "origin": grant.origin,
                        "account_id": grant.account_id,
                        "user_id": grant.user_id,
                        "workspace_generation": grant.workspace_generation,
                    });
                    json_response(StatusCode::OK, &payload)
                })
            }
            _ => Err(SessionError::InvalidRequest),
        },
        "/oauth/revoke" => match sonic_rs::from_slice::<RevokeRequest>(body) {
            Ok(_) => sessions.revoke_session(auth).await.map(|()| {
                let mut response = Response::new(Body::empty());
                *response.status_mut() = StatusCode::NO_CONTENT;
                response
            }),
            Err(_) => Err(SessionError::InvalidRequest),
        },
        _ => Err(SessionError::InvalidRequest),
    };
    let mut response = match result {
        Ok(response) => response,
        Err(error) => {
            let (status, origin, code) = match error {
                SessionError::Unauthenticated(error) => (
                    StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    "renewal", "local_session_rejected",
                ),
                SessionError::OwnerConflict => (StatusCode::BAD_REQUEST, "proxy", "owner_conflict"),
                SessionError::InvalidRequest => (StatusCode::BAD_REQUEST, "proxy", "invalid_session_request"),
                SessionError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "proxy", "session_unavailable"),
            };
            let mut response = json_response(status, &json!({"error": code}));
            response.headers_mut().insert("cc-lb-error-origin", http::HeaderValue::from_static(origin));
            response
        }
    };
    response.headers_mut().insert(CACHE_CONTROL, http::HeaderValue::from_static("no-store"));
    let classification = TerminalClassification::local(response.status());
    response.extensions_mut().insert(classification);
    if let Some(observer) = observer {
        observer.terminate_local(classification);
    }
    response
}

fn json_response(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Body::from(serde_json::to_vec(value).expect("JSON value serializes")));
    *response.status_mut() = status;
    response.headers_mut().insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    response
}
