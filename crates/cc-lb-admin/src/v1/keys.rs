use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use cc_lb_engine::AuditEntry;
use cc_lb_engine::api_keys::key_store::CreateParams;
use cc_lb_engine::api_keys::secret;
use cc_lb_storage_api::types::{KeyStatus, PrincipalKindLite, UpstreamKind};
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
            emit_key_audit(&state, &id, &key_id, "principal_key_issue", 201);
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
    Query(query): Query<ListKeysQuery>,
) -> axum::response::Response {
    let Some(key_store) = state.key_store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "key_store_unavailable" })),
        )
            .into_response();
    };

    match key_store.list_all().await {
        Ok(records) => {
            // Build key_id -> max(ts) map from recent request events.
            // Window: last 30 days, capped to 5000 events. Best-effort.
            let last_used_map: std::collections::HashMap<String, u64> = if let Some(storage) =
                state.storage.as_ref()
            {
                let now_ms = cc_lb_engine::clock::unix_millis(state.clock.now())
                    .min(u128::from(u64::MAX)) as u64;
                let since_ms = now_ms.saturating_sub(30 * 24 * 60 * 60 * 1000);
                match storage
                    .query_request_events(since_ms / 1000, now_ms / 1000, 5000)
                    .await
                {
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
            let now = cc_lb_engine::clock::unix_secs(state.clock.now());
            emit_key_audit(&state, &id, &key_id, "principal_key_revoke", 200);
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

fn emit_key_audit(state: &AdminState, principal_id: &str, key_id: &str, action: &str, status: u16) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = cc_lb_engine::clock::unix_secs(state.clock.now());
    let _ = audit_sink.try_enqueue(AuditEntry {
        ts,
        request_id: format!("admin-v1-key-{key_id}-{ts}"),
        principal_id: principal_id.to_owned(),
        route: "admin_v1_principal_keys".to_owned(),
        upstream: "admin".to_owned(),
        status,
        input_tokens: Some(0),
        output_tokens: Some(0),
        duration_ms: 0,
        api_key_id: Some(key_id.to_owned()),
        admin_action: Some(format!(
            "{action}(principal_id={principal_id}, key_id={key_id})"
        )),
        actor: Some("admin".to_owned()),
        ..AuditEntry::default()
    });
}
