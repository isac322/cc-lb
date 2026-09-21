use axum::{
    Json, Router,
    extract::{Extension, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

use crate::AdminState;
use crate::auth::{AdminActorKind, AdminAuthMode, AdminIdentity};
use crate::ports::{RoutePreviewError, RoutePreviewInput};

const DEFAULT_MODEL: &str = "claude-sonnet-4-5-20250929";

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/auth/session", get(auth_session))
        .route("/admin/v1/router/preview", post(preview_route))
}

#[derive(Serialize)]
struct AuthSessionResponse {
    authority: String,
    subject: String,
    kind: AdminActorKind,
    provider_id: String,
    email: Option<String>,
    display_name: Option<String>,
    expires_at_unix_secs: Option<u64>,
    auth_mode: AdminAuthMode,
}

async fn auth_session(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
) -> Json<AuthSessionResponse> {
    Json(AuthSessionResponse {
        authority: identity.authority,
        subject: identity.subject,
        kind: identity.kind,
        provider_id: identity.provider_id,
        email: identity.email,
        display_name: identity.display_name,
        expires_at_unix_secs: identity.expires_at_unix_secs,
        auth_mode: state.admin_auth.mode(),
    })
}

#[derive(Debug, Deserialize)]
struct PreviewRequest {
    principal_id: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: Option<Value>,
}

async fn preview_route(
    State(state): State<AdminState>,
    Json(req): Json<PreviewRequest>,
) -> axum::response::Response {
    let Some(route_preview) = state
        .lifecycle
        .as_ref()
        .and_then(|ports| ports.route_preview.as_deref())
    else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "lifecycle_unavailable",
            "admin instance built without lifecycle",
        );
    };

    let headers = match build_headers(&req.headers) {
        Ok(headers) => headers,
        Err(header_err) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_headers", &header_err);
        }
    };
    let body_value = req.body.unwrap_or_else(default_body);
    let body_bytes = match serde_json::to_vec(&body_value) {
        Ok(bytes) => Bytes::from(bytes),
        Err(error) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_body", &error.to_string());
        }
    };

    match route_preview
        .preview_route(RoutePreviewInput {
            principal_id: req.principal_id,
            request_id: req.request_id,
            headers,
            body_bytes,
        })
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(RoutePreviewError::PrincipalNotFound(id)) => error_response(
            StatusCode::NOT_FOUND,
            "principal_not_found",
            &format!("principal {id} not found"),
        ),
        Err(RoutePreviewError::PipelineInstantiationError(detail)) => error_response(
            StatusCode::BAD_GATEWAY,
            "router_pipeline_instantiation_error",
            &detail,
        ),
    }
}

fn build_headers(headers_in: &HashMap<String, String>) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    for (name, value) in headers_in {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| format!("bad header name {name:?}: {error}"))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|error| format!("bad header value for {name:?}: {error}"))?;
        headers.append(header_name, header_value);
    }
    Ok(headers)
}

fn default_body() -> Value {
    json!({
        "model": DEFAULT_MODEL,
        "messages": []
    })
}

fn error_response(
    status: StatusCode,
    code: &'static str,
    detail: &str,
) -> axum::response::Response {
    (status, Json(json!({ "error": code, "detail": detail }))).into_response()
}
