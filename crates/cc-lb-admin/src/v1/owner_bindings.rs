use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cc_lb_storage_api::{AccountOwnerBinding, AccountOwnerStore, StorageError};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/account-owner-bindings",
            post(create_owner_binding),
        )
        .route(
            "/admin/v1/account-owner-bindings/{id}",
            get(get_owner_binding),
        )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateOwnerBindingBody {
    binding_id: String,
    principal_id: String,
    provider: String,
    owner_id: String,
    upstream_id: String,
    local_user_id: String,
    allowed_workspaces: Vec<String>,
    provider_workspace_id: String,
    #[serde(default)]
    outbound_region: Option<String>,
}

async fn create_owner_binding(
    State(state): State<AdminState>,
    Json(body): Json<CreateOwnerBindingBody>,
) -> Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    if !is_safe_binding_id(&body.binding_id) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_owner_binding_id");
    }
    if !is_canonical_uuid(&body.principal_id) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    }
    let Ok(upstream_id) = body.upstream_id.parse::<Uuid>() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_upstream_id");
    };
    if upstream_id.to_string() != body.upstream_id {
        return error_response(StatusCode::BAD_REQUEST, "invalid_upstream_id");
    }

    let binding = AccountOwnerBinding {
        binding_id: body.binding_id,
        principal_id: body.principal_id,
        provider: body.provider,
        owner_id: body.owner_id,
        upstream_id,
        owner_generation: 1,
        local_user_id: body.local_user_id,
        allowed_workspaces: body.allowed_workspaces,
        provider_workspace_id: body.provider_workspace_id,
        outbound_region: body.outbound_region,
        revision: 1,
        revoked_at_unix_secs: None,
    };

    match AccountOwnerStore::create_owner_binding(storage, binding.clone()).await {
        Ok(()) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::LOCATION,
                header_value(&format!(
                    "/admin/v1/account-owner-bindings/{}",
                    binding.binding_id
                )),
            );
            headers.insert(
                header::ETAG,
                header_value(&format!("W/\"{}\"", binding.revision)),
            );
            (StatusCode::CREATED, headers, Json(binding)).into_response()
        }
        Err(error) => storage_error(error),
    }
}

async fn get_owner_binding(State(state): State<AdminState>, Path(id): Path<String>) -> Response {
    if !is_safe_binding_id(&id) {
        return error_response(StatusCode::NOT_FOUND, "unknown_owner_binding");
    }
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };

    match AccountOwnerStore::get_owner_binding(storage, &id).await {
        Ok(Some(binding)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::ETAG,
                header_value(&format!("W/\"{}\"", binding.revision)),
            );
            (headers, Json(binding)).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "unknown_owner_binding"),
        Err(error) => storage_error(error),
    }
}

fn is_safe_binding_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn is_canonical_uuid(value: &str) -> bool {
    value
        .parse::<Uuid>()
        .is_ok_and(|uuid| uuid.to_string() == value)
}
fn header_value(value: &str) -> HeaderValue {
    HeaderValue::from_str(value).expect("owner binding response header is valid")
}

fn storage_error(error: StorageError) -> Response {
    match error {
        StorageError::Conflict { message } => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "storage_conflict", "message": message })),
        )
            .into_response(),
        StorageError::Unavailable { .. } => {
            let mut response =
                error_response(StatusCode::SERVICE_UNAVAILABLE, "upstream_unavailable");
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        StorageError::Transient {
            retryable: true, ..
        } => {
            let mut response =
                error_response(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable");
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        StorageError::Transient {
            retryable: false, ..
        } => error_response(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
        StorageError::InvalidInput { .. } => {
            error_response(StatusCode::BAD_REQUEST, "invalid_input")
        }
        _ => error_response(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    }
}

fn storage_unavailable() -> Response {
    let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable");
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

fn error_response(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}
