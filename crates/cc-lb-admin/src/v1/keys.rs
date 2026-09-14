use std::future::Future;

use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use cc_lb_control::api_keys::key_store::CreateParams;
use cc_lb_control::api_keys::secret;
use cc_lb_storage_api::types::{KeyStatus, PrincipalKindLite, UpstreamKind};
use cc_lb_storage_api::{RequestEventKeyLastUsedQuery, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::add_dynamic_rebind_headers;
use crate::audit::{AdminAuditEvent, record_admin_audit};
use crate::{
    AdminState,
    auth::{AdminAction, AdminIdentity, authorize},
};

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

#[derive(Debug, Deserialize)]
struct ListKeysQuery {
    status: Option<ListKeysStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ListKeysStatus {
    Active,
    Disabled,
    Revoked,
    All,
}

impl ListKeysStatus {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Revoked => "revoked",
            Self::All => "all",
        }
    }
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
    Extension(identity): Extension<AdminIdentity>,
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
            let route = format!("/admin/v1/principals/{id}/keys");
            if record_issue_audit_or_revoke(
                key_store.as_ref(),
                &id,
                &key_id,
                record_admin_audit(
                    &state,
                    AdminAuditEvent {
                        identity: Some(&identity),
                        system_component: None,
                        action: "principal_key_issue",
                        route: &route,
                        target_principal_id: Some(&id),
                        target_upstream: None,
                        api_key_id: Some(&key_id),
                        status: StatusCode::CREATED.as_u16(),
                        payload: Some(json!({ "plaintext_exposed": true })),
                    },
                ),
            )
            .await
            .is_err()
            {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "audit_write_failed" })),
                )
                    .into_response();
            }
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

async fn record_issue_audit_or_revoke(
    key_store: &cc_lb_control::api_keys::key_store::KeyStore,
    principal_id: &str,
    key_id: &str,
    audit_write: impl Future<Output = Result<(), StorageError>>,
) -> Result<(), StorageError> {
    let Err(audit_error) = audit_write.await else {
        return Ok(());
    };

    match key_store.revoke(principal_id, key_id).await {
        Ok(()) => tracing::error!(
            error = %audit_error,
            action = "principal_key_issue",
            "admin audit write failed; issued key revoked"
        ),
        Err(rollback_error) => tracing::error!(
            audit_error = %audit_error,
            rollback_error = %rollback_error,
            action = "principal_key_issue",
            "admin audit write and issued key rollback failed"
        ),
    }

    Err(audit_error)
}

async fn list_keys(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path(id): Path<String>,
    Query(query): Query<ListKeysQuery>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response();
    }
    let Some(key_store) = state.key_store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "key_store_unavailable" })),
        )
            .into_response();
    };

    match key_store.list_all().await {
        Ok(records) => {
            let last_used_map: std::collections::HashMap<String, u64> = if let Some(storage) =
                state.storage.as_ref()
            {
                let now_ms =
                    cc_lb_clock::unix_millis(state.clock.now()).min(u128::from(u64::MAX)) as u64;
                let since_ms = now_ms.saturating_sub(30 * 24 * 60 * 60 * 1000);
                match storage
                    .request_event_key_last_used(&RequestEventKeyLastUsedQuery {
                        principal_id: id.clone(),
                        since_unix_secs: since_ms / 1000,
                        until_unix_secs: now_ms / 1000,
                    })
                    .await
                {
                    Ok(rows) => rows
                        .into_iter()
                        .map(|row| (row.key_id, row.last_used_at_unix_secs))
                        .collect(),
                    Err(err) => {
                        tracing::warn!(%err, "list_keys: request_event_key_last_used failed; last_used unavailable");
                        std::collections::HashMap::new()
                    }
                }
            } else {
                std::collections::HashMap::new()
            };

            let mut keys = Vec::new();
            for (principal_id, key_id, r) in records {
                if principal_id != id {
                    continue;
                }
                if !matches_status(r.status, query.status.as_ref()) {
                    continue;
                }
                let last_used_at_unix_secs = last_used_map.get(&key_id).copied();
                keys.push(ApiKeyRecord {
                    last_4: r.last_4,
                    key_id,
                    label: if r.label.is_empty() {
                        None
                    } else {
                        Some(r.label)
                    },
                    issued_at_unix_secs: r.issued_at_unix_secs,
                    revoked_at_unix_secs: r.revoked_at_unix_secs,
                    last_used_at_unix_secs,
                });
            }
            let route = format!("/admin/v1/principals/{id}/keys");
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action: "principal_keys_list",
                    route: &route,
                    target_principal_id: Some(&id),
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: Some(json!({
                        "principal_id": &id,
                        "status": query
                            .status
                            .as_ref()
                            .map_or("active", ListKeysStatus::as_str),
                    })),
                },
            )
            .await
            {
                tracing::error!(
                    error = %error,
                    action = "principal_keys_list",
                    "admin audit write failed"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "audit_write_failed" })),
                )
                    .into_response();
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

fn matches_status(status: KeyStatus, requested: Option<&ListKeysStatus>) -> bool {
    match requested.unwrap_or(&ListKeysStatus::Active) {
        ListKeysStatus::Active => status == KeyStatus::Active,
        ListKeysStatus::Disabled => status == KeyStatus::Disabled,
        ListKeysStatus::Revoked => status == KeyStatus::Revoked,
        ListKeysStatus::All => true,
    }
}

async fn revoke_key(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
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
            let now = cc_lb_clock::unix_secs(state.clock.now());
            let route = format!("/admin/v1/principals/{id}/keys/{key_id}/revoke");
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action: "principal_key_revoke",
                    route: &route,
                    target_principal_id: Some(&id),
                    target_upstream: None,
                    api_key_id: Some(&key_id),
                    status: StatusCode::OK.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                tracing::error!(
                    error = %error,
                    action = "principal_key_revoke",
                    "admin audit write failed"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "audit_write_failed" })),
                )
                    .into_response();
            }
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
