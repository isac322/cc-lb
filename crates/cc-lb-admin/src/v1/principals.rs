use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post, put},
};
use cc_lb_core::{AuditEntry, AuditPayload};
use cc_lb_plugin_api::TerminalStrategy;
use cc_lb_storage_api::principal::Limit;
use cc_lb_storage_api::{
    PluginSlot, PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalStore, PrincipalUpdate,
    StorageError,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use crate::AdminState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/principals",
            post(create_principal).get(list_principals),
        )
        .route(
            "/admin/v1/principals/{id}",
            get(get_principal)
                .put(update_principal)
                .patch(update_principal)
                .delete(delete_principal),
        )
        .route("/admin/v1/principals/{id}/enable", post(enable_principal))
        .route("/admin/v1/principals/{id}/disable", post(disable_principal))
        .route(
            "/admin/v1/principals/{id}/router-terminal",
            get(get_router_terminal).put(update_router_terminal),
        )
        .route(
            "/admin/v1/principals/{id}/allowed_models",
            put(update_allowed_models),
        )
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    after: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CreatePrincipalBody {
    name: String,
    kind: PrincipalKind,
    #[serde(default)]
    allowed_models: Vec<String>,
    #[serde(default)]
    allowed_upstreams: Vec<Uuid>,
    #[serde(default)]
    default_limits: Vec<Limit>,
}

#[derive(Debug, Deserialize)]
struct UpdatePrincipalBody {
    name: Option<String>,
    allowed_models: Option<Vec<String>>,
    allowed_upstreams: Option<Vec<Uuid>>,
    default_limits: Option<Vec<Limit>>,
}

#[derive(Debug, Deserialize)]
struct AllowedModelsBody {
    models: Vec<String>,
    expected_revision: u64,
}

#[derive(Debug, Deserialize)]
struct RouterTerminalBody {
    strategy: String,
}

#[derive(Debug, Serialize)]
struct PrincipalResponse {
    id: String,
    name: String,
    kind: PrincipalKind,
    enabled: bool,
    revision: u64,
    allowed_models: Vec<String>,
    allowed_upstreams: Vec<Uuid>,
    default_limits: Vec<Limit>,
}

#[derive(Debug, Serialize)]
struct PrincipalSummaryResponse {
    id: String,
    name: String,
    kind: PrincipalKind,
    enabled: bool,
    revision: u64,
}

#[derive(Debug, Serialize)]
struct RouterTerminalResponse {
    strategy: TerminalStrategy,
    revision: u64,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    principals: Vec<PrincipalResponse>,
}

#[derive(Debug, Serialize)]
struct ReferenceResponse {
    kind: &'static str,
    id: String,
}

async fn create_principal(
    State(state): State<AdminState>,
    Json(body): Json<CreatePrincipalBody>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };

    let input = PrincipalCreate {
        name: body.name,
        kind: body.kind,
        allowed_models: body.allowed_models,
        allowed_upstreams: body.allowed_upstreams,
        default_limits: body.default_limits,
    };

    match PrincipalStore::create(storage, input, unix_now_secs()).await {
        Ok(record) => {
            emit_audit(
                &state,
                AuditPayload::PrincipalCreate {
                    principal_id: record.id.to_string(),
                    principal_kind: principal_kind_name(record.kind).to_owned(),
                },
            );
            let location = format!("/admin/v1/principals/{}", record.id);
            let mut headers = HeaderMap::new();
            insert_header(&mut headers, header::LOCATION, &location);
            insert_header(&mut headers, header::ETAG, &etag(record.revision));
            let mut response =
                (StatusCode::CREATED, headers, Json(summary_response(record))).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(error) => storage_error(error),
    }
}

async fn list_principals(
    State(state): State<AdminState>,
    Query(query): Query<ListQuery>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let offset = query.after.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);

    let total = match PrincipalStore::list(storage, 0, MAX_LIMIT, false).await {
        Ok(records) => records.len(),
        Err(error) => return storage_error(error),
    };
    match PrincipalStore::list(storage, offset, limit, false).await {
        Ok(records) => {
            let mut headers = HeaderMap::new();
            insert_header(&mut headers, "x-total-count", &total.to_string());
            (
                headers,
                Json(ListResponse {
                    principals: records.into_iter().map(principal_response).collect(),
                }),
            )
                .into_response()
        }
        Err(error) => storage_error(error),
    }
}

async fn get_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Ok(id) = id.parse() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    };

    match PrincipalStore::get_by_id(storage, id).await {
        Ok(Some(record)) if record.deleted_at_unix_secs.is_none() => {
            let mut headers = HeaderMap::new();
            insert_header(&mut headers, header::ETAG, &etag(record.revision));
            (headers, Json(principal_response(record))).into_response()
        }
        Ok(_) => error_response(StatusCode::NOT_FOUND, "unknown_principal"),
        Err(error) => storage_error(error),
    }
}

async fn update_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UpdatePrincipalBody>,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error_response(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let fields_changed = update_fields_changed(&body);
    update_principal_record(
        state,
        id,
        expected_revision,
        PrincipalUpdate {
            name: body.name,
            allowed_models: body.allowed_models,
            allowed_upstreams: body.allowed_upstreams,
            default_limits: body.default_limits,
            router_terminal_strategy: None,
        },
        fields_changed,
    )
    .await
}

async fn enable_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> axum::response::Response {
    set_enabled(state, id, headers, true).await
}

async fn disable_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> axum::response::Response {
    set_enabled(state, id, headers, false).await
}

async fn set_enabled(
    state: AdminState,
    id: String,
    headers: HeaderMap,
    enabled: bool,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error_response(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Ok(id) = id.parse() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    };

    match PrincipalStore::set_enabled(storage, id, expected_revision, enabled, unix_now_secs())
        .await
    {
        Ok(Some(record)) => {
            emit_audit(
                &state,
                AuditPayload::PrincipalUpdate {
                    principal_id: record.id.to_string(),
                    fields_changed: vec!["enabled"],
                },
            );
            let mut response = respond_with_etag(record);
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "unknown_principal"),
        Err(error) => storage_mutation_error(error),
    }
}

async fn delete_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error_response(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Ok(id) = id.parse() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    };

    let mut references = Vec::new();
    for slot in [
        PluginSlot::Router,
        PluginSlot::ObservabilityHook,
        PluginSlot::Shape,
    ] {
        match storage.list_chain_for_principal(id, slot).await {
            Ok(entries) => references.extend(entries.into_iter().map(|entry| ReferenceResponse {
                kind: "plugin_chain",
                id: entry.id.to_string(),
            })),
            Err(error) => return storage_error(error),
        }
    }
    if !references.is_empty() {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "referenced_by",
                "references": references,
            })),
        )
            .into_response();
    }

    match PrincipalStore::soft_delete(storage, id, expected_revision, unix_now_secs()).await {
        Ok(Some(record)) => {
            emit_audit(
                &state,
                AuditPayload::PrincipalDelete {
                    principal_id: record.id.to_string(),
                },
            );
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "unknown_principal"),
        Err(error) => storage_mutation_error(error),
    }
}

async fn update_allowed_models(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(body): Json<AllowedModelsBody>,
) -> axum::response::Response {
    update_principal_record(
        state,
        id,
        body.expected_revision,
        PrincipalUpdate {
            allowed_models: Some(body.models),
            ..PrincipalUpdate::default()
        },
        vec!["allowed_models"],
    )
    .await
}

async fn get_router_terminal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Ok(id) = id.parse() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    };

    match PrincipalStore::get_by_id(storage, id).await {
        Ok(Some(record)) if record.deleted_at_unix_secs.is_none() => {
            router_terminal_with_etag(record)
        }
        Ok(_) => error_response(StatusCode::NOT_FOUND, "unknown_principal"),
        Err(error) => storage_error(error),
    }
}

async fn update_router_terminal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<RouterTerminalBody>,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error_response(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let strategy = match parse_router_terminal_strategy(&body.strategy) {
        Ok(strategy) => strategy,
        Err(response) => return response,
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Ok(id) = id.parse() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    };

    match PrincipalStore::update(
        storage,
        id,
        expected_revision,
        PrincipalUpdate {
            router_terminal_strategy: Some(strategy),
            ..PrincipalUpdate::default()
        },
        unix_now_secs(),
    )
    .await
    {
        Ok(Some(record)) => {
            emit_audit(
                &state,
                AuditPayload::PrincipalUpdate {
                    principal_id: record.id.to_string(),
                    fields_changed: vec!["router_terminal_strategy"],
                },
            );
            let mut response = router_terminal_with_etag(record);
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "unknown_principal"),
        Err(error) => storage_mutation_error(error),
    }
}

async fn update_principal_record(
    state: AdminState,
    id: String,
    expected_revision: u64,
    update: PrincipalUpdate,
    fields_changed: Vec<&'static str>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Ok(id) = id.parse() else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_principal_id");
    };

    match PrincipalStore::update(storage, id, expected_revision, update, unix_now_secs()).await {
        Ok(Some(record)) => {
            emit_audit(
                &state,
                AuditPayload::PrincipalUpdate {
                    principal_id: record.id.to_string(),
                    fields_changed,
                },
            );
            let mut response = respond_with_etag(record);
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "unknown_principal"),
        Err(error) => storage_mutation_error(error),
    }
}

fn respond_with_etag(record: PrincipalRecord) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, header::ETAG, &etag(record.revision));
    (headers, Json(principal_response(record))).into_response()
}

fn router_terminal_with_etag(record: PrincipalRecord) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, header::ETAG, &etag(record.revision));
    let response = RouterTerminalResponse {
        strategy: record.router_terminal_strategy,
        revision: record.revision,
    };
    (headers, Json(response)).into_response()
}

fn principal_response(record: PrincipalRecord) -> PrincipalResponse {
    PrincipalResponse {
        id: record.id.to_string(),
        name: record.name,
        kind: record.kind,
        enabled: record.enabled,
        revision: record.revision,
        allowed_models: record.allowed_models,
        allowed_upstreams: record.allowed_upstreams,
        default_limits: record.default_limits,
    }
}

fn summary_response(record: PrincipalRecord) -> PrincipalSummaryResponse {
    PrincipalSummaryResponse {
        id: record.id.to_string(),
        name: record.name,
        kind: record.kind,
        enabled: record.enabled,
        revision: record.revision,
    }
}

fn update_fields_changed(body: &UpdatePrincipalBody) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if body.name.is_some() {
        fields.push("name");
    }
    if body.allowed_models.is_some() {
        fields.push("allowed_models");
    }
    if body.allowed_upstreams.is_some() {
        fields.push("allowed_upstreams");
    }
    if body.default_limits.is_some() {
        fields.push("default_limits");
    }
    fields
}

#[allow(clippy::result_large_err)]
fn parse_router_terminal_strategy(
    value: &str,
) -> Result<TerminalStrategy, axum::response::Response> {
    match value {
        "first-pick" => Ok(TerminalStrategy::FirstPick),
        "random" => Ok(TerminalStrategy::Random),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_router_terminal_strategy",
                "allowed": ["first-pick", "random"]
            })),
        )
            .into_response()),
    }
}

fn if_match_revision(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(header::IF_MATCH)?.to_str().ok()?.trim();
    parse_revision(value)
}

fn parse_revision(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if let Some(inner) = trimmed
        .strip_prefix("W/\"")
        .and_then(|v| v.strip_suffix('"'))
    {
        return inner.parse().ok();
    }
    if let Some(inner) = trimmed.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
        return inner.parse().ok();
    }
    trimmed.parse().ok()
}

fn etag(revision: u64) -> String {
    format!("W/\"{revision}\"")
}

fn insert_header(
    headers: &mut HeaderMap,
    name: impl axum::http::header::IntoHeaderName,
    value: &str,
) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn storage_mutation_error(error: StorageError) -> axum::response::Response {
    if let StorageError::Conflict { message } = &error
        && let Some(current_revision) = current_revision_from_message(message)
    {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "stale_revision", "current_revision": current_revision })),
        )
            .into_response();
    }
    storage_error(error)
}

fn current_revision_from_message(message: &str) -> Option<u64> {
    let digits = message
        .chars()
        .rev()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.chars().rev().collect::<String>().parse().ok()
}

fn storage_error(storage_error: StorageError) -> axum::response::Response {
    match storage_error {
        StorageError::Conflict { message } => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "storage_conflict", "message": message })),
        )
            .into_response(),
        StorageError::InvalidInput { field, reason } => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_input", "field": field, "reason": reason })),
        )
            .into_response(),
        source => {
            tracing::error!(error = %source, "admin v1 principal storage operation failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "storage_error")
        }
    }
}

fn storage_unavailable() -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        Json(json!({ "error": "storage_unavailable" })),
    )
        .into_response()
}

fn error_response(status: StatusCode, code: &str) -> axum::response::Response {
    (status, Json(json!({ "error": code }))).into_response()
}

fn emit_audit(state: &AdminState, payload: AuditPayload) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let principal_id = principal_id_for_audit(&payload);
    let action = payload.to_string();
    let ts = unix_now_secs();
    let mut entry: AuditEntry = payload.into();
    entry.ts = ts;
    entry.request_id = format!("admin-v1-principal-{principal_id}-{ts}");
    entry.principal_id = principal_id;
    entry.route = "admin_v1_principals".to_owned();
    entry.status = 200;
    entry.actor = Some("admin".to_owned());
    entry.admin_action = Some(action);
    let _ = audit_sink.try_enqueue(entry);
}

fn principal_id_for_audit(payload: &AuditPayload) -> String {
    match payload {
        AuditPayload::PrincipalCreate { principal_id, .. }
        | AuditPayload::PrincipalUpdate { principal_id, .. }
        | AuditPayload::PrincipalDelete { principal_id } => principal_id.clone(),
        _ => String::new(),
    }
}

fn principal_kind_name(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::Machine => "machine",
        PrincipalKind::Human => "human",
        PrincipalKind::Admin => "admin",
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
