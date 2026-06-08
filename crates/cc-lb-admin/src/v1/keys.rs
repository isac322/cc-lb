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

use super::add_dynamic_rebind_headers;
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
    last_4: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_used_at_unix_secs: Option<u64>,
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
            let mut http_response = (StatusCode::CREATED, Json(response)).into_response();
            add_dynamic_rebind_headers(&mut http_response, &state).await;
            http_response
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
            // Build key_id -> max(ts) map from recent request events.
            // Window: last 30 days, capped to 5000 events. Best-effort.
            let last_used_map: std::collections::HashMap<String, u64> = if let Some(storage) =
                state.storage.as_ref()
            {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let since_ms = now_ms.saturating_sub(30 * 24 * 60 * 60 * 1000);
                match storage.query_request_events(since_ms, now_ms, 5000).await {
                    Ok(events) => {
                        let mut map: std::collections::HashMap<String, u64> =
                            std::collections::HashMap::new();
                        for ev in events {
                            if let Some(kid) = ev.key_id.as_ref() {
                                if kid.is_empty() {
                                    continue;
                                }
                                let entry = map.entry(kid.clone()).or_insert(0);
                                if ev.ts > *entry {
                                    *entry = ev.ts;
                                }
                            }
                        }
                        map
                    }
                    Err(err) => {
                        tracing::warn!(%err, "list_keys: query_request_events failed; last_used unavailable");
                        std::collections::HashMap::new()
                    }
                }
            } else {
                std::collections::HashMap::new()
            };

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
                let last_used_at_unix_secs = if key_id.is_empty() {
                    None
                } else {
                    last_used_map.get(&key_id).copied()
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
                    last_4: r.last_4,
                    last_used_at_unix_secs,
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
            let mut http_response = Json(response).into_response();
            add_dynamic_rebind_headers(&mut http_response, &state).await;
            http_response
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
