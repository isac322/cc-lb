use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use cc_lb_core::api_keys::key_store::CreateParams;
use cc_lb_core::api_keys::secret;
use cc_lb_storage_api::types::{PrincipalKindLite, UpstreamKind};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::AdminState;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/principals/{id}/keys",
            get(list_keys).post(issue_key),
        )
        .route(
            "/admin/v1/principals/{id}/keys/{key_id}/revoke",
            post(revoke_key),
        )
}

#[derive(Debug, Deserialize)]
struct IssueKeyRequest {
    label: Option<String>,
}

#[derive(Debug, Serialize)]
struct IssueKeyResponse {
    principal_id: String,
    key_id: String,
    plaintext_key: String,
    issued_at_unix_secs: u64,
}

#[derive(Debug, Serialize)]
struct ApiKeyRecord {
    key_id: String,
    label: Option<String>,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct KeyListResponse {
    keys: Vec<ApiKeyRecord>,
}

#[derive(Debug, Serialize)]
struct RevokeKeyResponse {
    key_id: String,
    revoked_at_unix_secs: u64,
}

async fn issue_key(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(body): Json<IssueKeyRequest>,
) -> axum::response::Response {
    let Some(key_store) = state.key_store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "key_store_unavailable" })),
        )
            .into_response();
    };

    let params = CreateParams {
        upstream_kind: UpstreamKind::AnthropicKey,
        label: body.label.unwrap_or_default(),
        description: None,
        expires_at_unix_secs: None,
        limit_overrides: vec![],
        principal_kind: PrincipalKindLite::Machine,
    };

    match key_store.create(&id, params).await {
        Ok((record, plaintext)) => {
            let (key_id, _) = secret::parse(plaintext.expose())
                .expect("plaintext key format is guaranteed by KeyStore::create");
            let response = IssueKeyResponse {
                principal_id: id,
                key_id,
                plaintext_key: plaintext.expose().to_string(),
                issued_at_unix_secs: record.issued_at_unix_secs,
            };
            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to issue key: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal_error" })),
            )
                .into_response()
        }
    }
}

async fn list_keys(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(key_store) = state.key_store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "key_store_unavailable" })),
        )
            .into_response();
    };

    match key_store.list_by_principal(&id).await {
        Ok(records) => {
            let mut keys = Vec::new();
            for r in records {
                let key_id = if r.index_hash == [0; 32] {
                    String::new()
                } else {
                    key_store
                        .lookup_by_index_hash(&r.index_hash)
                        .await
                        .unwrap_or(None)
                        .map(|(_, key_id, _)| key_id)
                        .unwrap_or_default()
                };
                keys.push(ApiKeyRecord {
                    key_id,
                    label: if r.label.is_empty() {
                        None
                    } else {
                        Some(r.label)
                    },
                    issued_at_unix_secs: r.issued_at_unix_secs,
                    revoked_at_unix_secs: r.revoked_at_unix_secs,
                });
            }
            Json(KeyListResponse { keys }).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list keys: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal_error" })),
            )
                .into_response()
        }
    }
}

async fn revoke_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    let Some(key_store) = state.key_store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "key_store_unavailable" })),
        )
            .into_response();
    };

    match key_store.revoke(&id, &key_id).await {
        Ok(_) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time is always after UNIX_EPOCH")
                .as_secs();
            let response = RevokeKeyResponse {
                key_id,
                revoked_at_unix_secs: now,
            };
            Json(response).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to revoke key: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal_error" })),
            )
                .into_response()
        }
    }
}
