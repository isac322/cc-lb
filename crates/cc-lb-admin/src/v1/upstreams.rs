use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ETAG, IF_MATCH},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cc_lb_aead::AeadEncryptedField;
use cc_lb_core::{AuditEntry, AuditPayload};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamShapePluginRef};
use cc_lb_storage_api::{
    PluginSlot, PrincipalStore, Storage, StorageError, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamUpdate,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use super::add_dynamic_rebind_headers;
use crate::AdminState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const STORE_PAGE_LIMIT: usize = 1000;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/upstreams",
            post(create_upstream).get(list_upstreams),
        )
        .route(
            "/admin/v1/upstreams/{id}",
            get(get_upstream)
                .put(update_upstream)
                .delete(delete_upstream),
        )
        .route("/admin/v1/upstreams/{id}/enable", post(enable_upstream))
        .route("/admin/v1/upstreams/{id}/disable", post(disable_upstream))
}

#[derive(Debug, Deserialize)]
struct UpstreamCreateBody {
    name: String,
    kind: UpstreamKind,
    #[serde(default)]
    base_url: Option<Url>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    shape_plugin: Option<UpstreamShapePluginRef>,
}

#[derive(Debug, Deserialize)]
struct UpstreamUpdateBody {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    base_url: Option<Url>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_shape_plugin")]
    shape_plugin: Option<Option<UpstreamShapePluginRef>>,
}

fn deserialize_optional_shape_plugin<'de, D>(
    deserializer: D,
) -> Result<Option<Option<UpstreamShapePluginRef>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<UpstreamShapePluginRef>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct UpstreamResponse {
    id: String,
    name: String,
    kind: UpstreamKind,
    enabled: bool,
    revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    shape_plugin: Option<UpstreamShapePluginRef>,
}

#[derive(Debug, Serialize)]
struct ReferenceResponse {
    kind: &'static str,
    id: String,
    name: String,
}

enum UpstreamError {
    StorageUnavailable,
    NotFound,
    BadRequest { error: &'static str, detail: String },
    MissingIfMatch,
    StaleRevision { current_revision: u64 },
    ReferencedBy { references: Vec<ReferenceResponse> },
    Conflict { detail: String },
    Storage(StorageError),
}

impl IntoResponse for UpstreamError {
    fn into_response(self) -> Response {
        match self {
            Self::StorageUnavailable => (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({ "error": "storage_unavailable" })),
            )
                .into_response(),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "upstream_not_found" })),
            )
                .into_response(),
            Self::BadRequest { error, detail } => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": error, "detail": detail })),
            )
                .into_response(),
            Self::MissingIfMatch => (
                StatusCode::PRECONDITION_REQUIRED,
                Json(json!({ "error": "if_match_required" })),
            )
                .into_response(),
            Self::StaleRevision { current_revision } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "stale_revision", "current_revision": current_revision })),
            )
                .into_response(),
            Self::ReferencedBy { references } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "referenced_by", "references": references })),
            )
                .into_response(),
            Self::Conflict { detail } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "conflict", "detail": detail })),
            )
                .into_response(),
            Self::Storage(error) => {
                tracing::error!(error = %error, "admin v1 upstream storage operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

impl From<StorageError> for UpstreamError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Conflict { message } => Self::Conflict { detail: message },
            other => Self::Storage(other),
        }
    }
}

async fn create_upstream(
    State(state): State<AdminState>,
    Json(body): Json<UpstreamCreateBody>,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let api_key_ciphertext = api_key_ciphertext_for_create(&state, &body)?;
    let kind = body.kind;
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: body.name,
            kind,
            base_url: body.base_url,
            api_key_ciphertext,
            shape_plugin: body.shape_plugin,
        },
    )
    .await?;
    enqueue_upstream_audit(
        &state,
        &created,
        AuditPayload::UpstreamCreate {
            upstream_id: created.id.to_string(),
            kind: created.kind.as_str().to_owned(),
        },
    );

    let mut response = (StatusCode::CREATED, Json(upstream_response(&created))).into_response();
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", created.id)).map_err(|error| {
            UpstreamError::BadRequest {
                error: "invalid_location",
                detail: error.to_string(),
            }
        })?,
    );
    add_dynamic_rebind_headers(&mut response, &state).await;
    Ok(response)
}

async fn list_upstreams(
    State(state): State<AdminState>,
    Query(query): Query<ListQuery>,
) -> Result<Response, UpstreamError> {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let all = list_all_upstreams(&state).await?;
    let total = all.len();
    let page = all
        .into_iter()
        .filter(|record| {
            query
                .after
                .as_ref()
                .is_none_or(|after| record.id.to_string() > *after)
        })
        .take(limit)
        .map(|record| upstream_response(&record))
        .collect::<Vec<_>>();

    let mut response = Json(json!({ "upstreams": page })).into_response();
    response.headers_mut().insert(
        "X-Total-Count",
        HeaderValue::from_str(&total.to_string()).map_err(|error| UpstreamError::BadRequest {
            error: "invalid_total_count",
            detail: error.to_string(),
        })?,
    );
    Ok(response)
}

async fn get_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, UpstreamError> {
    let record = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let mut response = Json(upstream_response(&record)).into_response();
    response
        .headers_mut()
        .insert(axum::http::header::ETAG, etag_value(record.revision)?);
    Ok(response)
}

async fn update_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UpstreamUpdateBody>,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let current = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        return Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        });
    }

    let fields_changed = changed_fields(&body);
    let api_key_ciphertext = api_key_ciphertext_for_update(&state, &current, &body)?;
    let storage = storage(&state)?;
    match UpstreamStore::update(
        storage,
        current.id,
        expected_revision,
        UpstreamUpdate {
            name: body.name,
            base_url: body.base_url,
            api_key_ciphertext,
            shape_plugin: body.shape_plugin,
        },
    )
    .await
    {
        Ok(updated) => {
            enqueue_upstream_audit(
                &state,
                &updated,
                AuditPayload::UpstreamUpdate {
                    upstream_id: updated.id.to_string(),
                    fields_changed,
                },
            );
            let mut response = Json(upstream_response(&updated)).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
        }
        Err(StorageError::Conflict { message }) => {
            stale_or_conflict(&state, &id, expected_revision, message).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn enable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, true).await
}

async fn disable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, false).await
}

async fn delete_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let current = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        return Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        });
    }
    let references = upstream_references(&state, &current).await?;
    if !references.is_empty() {
        return Err(UpstreamError::ReferencedBy { references });
    }

    let storage = storage(&state)?;
    match UpstreamStore::soft_delete(storage, current.id, expected_revision).await {
        Ok(()) => {
            enqueue_upstream_audit(
                &state,
                &current,
                AuditPayload::UpstreamDelete {
                    upstream_id: current.id.to_string(),
                },
            );
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
        }
        Err(StorageError::Conflict { message }) => {
            stale_or_conflict(&state, &id, expected_revision, message).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn set_enabled(
    state: AdminState,
    id: String,
    headers: HeaderMap,
    enabled: bool,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let expected_revision = parse_if_match(&headers)?;
    let Ok(id) = id.parse() else {
        return Err(UpstreamError::BadRequest {
            error: "invalid_upstream_id",
            detail: "invalid uuid".to_owned(),
        });
    };

    let updated = match UpstreamStore::set_enabled(storage, id, expected_revision, enabled).await {
        Ok(updated) => updated,
        Err(StorageError::Conflict { message }) => {
            return stale_or_conflict(&state, &id.to_string(), expected_revision, message).await;
        }
        Err(error) => return Err(error.into()),
    };

    enqueue_upstream_audit(
        &state,
        &updated,
        if enabled {
            AuditPayload::UpstreamEnable {
                upstream_id: updated.id.to_string(),
            }
        } else {
            AuditPayload::UpstreamDisable {
                upstream_id: updated.id.to_string(),
            }
        },
    );

    let mut response = respond_with_etag(updated);
    crate::v1::add_dynamic_rebind_headers(&mut response, &state).await;
    Ok(response)
}

async fn stale_or_conflict(
    state: &AdminState,
    id: &str,
    expected_revision: u64,
    detail: String,
) -> Result<Response, UpstreamError> {
    let current = find_upstream(state, id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        })
    } else {
        Err(UpstreamError::Conflict { detail })
    }
}

fn respond_with_etag(record: UpstreamRecord) -> Response {
    let mut response = Json(upstream_response(&record)).into_response();
    if let Ok(etag) = etag_value(record.revision) {
        response.headers_mut().insert(ETAG, etag);
    }
    response
}

fn storage(state: &AdminState) -> Result<&dyn Storage, UpstreamError> {
    state
        .storage
        .as_deref()
        .ok_or(UpstreamError::StorageUnavailable)
}

fn upstream_response(record: &UpstreamRecord) -> UpstreamResponse {
    UpstreamResponse {
        id: record.id.to_string(),
        name: record.name.clone(),
        kind: record.kind,
        enabled: record.enabled,
        revision: record.revision,
        shape_plugin: record.shape_plugin.clone(),
    }
}

fn parse_if_match(headers: &HeaderMap) -> Result<u64, UpstreamError> {
    let value = headers.get(IF_MATCH).ok_or(UpstreamError::MissingIfMatch)?;
    let raw = value.to_str().map_err(|error| UpstreamError::BadRequest {
        error: "invalid_if_match",
        detail: error.to_string(),
    })?;
    let trimmed = raw.trim();
    let revision = trimmed
        .strip_prefix("W/\"")
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
        .unwrap_or(trimmed);
    revision
        .parse::<u64>()
        .map_err(|error| UpstreamError::BadRequest {
            error: "invalid_if_match",
            detail: error.to_string(),
        })
}

fn etag_value(revision: u64) -> Result<HeaderValue, UpstreamError> {
    HeaderValue::from_str(&format!("W/\"{revision}\"")).map_err(|error| UpstreamError::BadRequest {
        error: "invalid_etag",
        detail: error.to_string(),
    })
}

fn api_key_ciphertext_for_create(
    state: &AdminState,
    body: &UpstreamCreateBody,
) -> Result<Option<Vec<u8>>, UpstreamError> {
    match body.kind {
        UpstreamKind::AnthropicApiKey => {
            let env_name =
                body.api_key_env
                    .as_deref()
                    .ok_or_else(|| UpstreamError::BadRequest {
                        error: "missing_api_key_env",
                        detail: "anthropic_api_key upstreams require api_key_env".to_owned(),
                    })?;
            encrypt_env_value(state, env_name, body.name.as_bytes()).map(Some)
        }
        UpstreamKind::AnthropicOauth => {
            if body.api_key_env.is_some() {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key_env",
                    detail: "anthropic_oauth upstreams do not accept credentials at create time"
                        .to_owned(),
                });
            }
            Ok(None)
        }
        UpstreamKind::Custom => Ok(None),
    }
}

fn api_key_ciphertext_for_update(
    state: &AdminState,
    current: &UpstreamRecord,
    body: &UpstreamUpdateBody,
) -> Result<Option<Vec<u8>>, UpstreamError> {
    body.api_key_env
        .as_deref()
        .map(|env_name| encrypt_env_value(state, env_name, current.id.as_bytes()))
        .transpose()
}

fn encrypt_env_value(
    state: &AdminState,
    env_name: &str,
    aad: &[u8],
) -> Result<Vec<u8>, UpstreamError> {
    let value = std::env::var(env_name).map_err(|error| UpstreamError::BadRequest {
        error: "missing_api_key_env_value",
        detail: format!("{env_name}: {error}"),
    })?;
    let encrypted = AeadEncryptedField::<String>::encrypt(state.aead.as_ref(), &value, aad)
        .map_err(|error| UpstreamError::BadRequest {
            error: "credential_encryption_failed",
            detail: error.to_string(),
        })?;
    Ok(encrypted.ciphertext().to_vec())
}

fn changed_fields(body: &UpstreamUpdateBody) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if body.name.is_some() {
        fields.push("name");
    }
    if body.base_url.is_some() {
        fields.push("base_url");
    }
    if body.api_key_env.is_some() {
        fields.push("api_key_ciphertext");
    }
    fields
}

async fn find_upstream(
    state: &AdminState,
    id: &str,
) -> Result<Option<UpstreamRecord>, UpstreamError> {
    Ok(list_all_upstreams(state)
        .await?
        .into_iter()
        .find(|record| record.id.to_string() == id))
}

async fn list_all_upstreams(state: &AdminState) -> Result<Vec<UpstreamRecord>, UpstreamError> {
    let storage = storage(state)?;
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = UpstreamStore::list(storage, after, STORE_PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    Ok(all)
}

async fn upstream_references(
    state: &AdminState,
    upstream: &UpstreamRecord,
) -> Result<Vec<ReferenceResponse>, UpstreamError> {
    let storage = storage(state)?;
    let mut references = Vec::new();
    let mut offset = 0;
    loop {
        let principals = PrincipalStore::list(storage, offset, STORE_PAGE_LIMIT, false).await?;
        if principals.is_empty() {
            break;
        }
        for principal in &principals {
            let router_entries = storage
                .list_chain_for_principal(principal.id, PluginSlot::Router)
                .await?;
            let hook_entries = storage
                .list_chain_for_principal(principal.id, PluginSlot::ObservabilityHook)
                .await?;
            let shape_entries = storage
                .list_chain_for_principal(principal.id, PluginSlot::Shape)
                .await?;
            if router_entries
                .iter()
                .chain(hook_entries.iter())
                .chain(shape_entries.iter())
                .any(|entry| config_references_upstream(&entry.config, &upstream.name))
            {
                references.push(ReferenceResponse {
                    kind: "principal",
                    id: principal.id.to_string(),
                    name: principal.name.clone(),
                });
            }
        }
        let page_len = principals.len();
        offset += page_len;
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    Ok(references)
}

fn config_references_upstream(config: &Value, upstream_name: &str) -> bool {
    match config {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "upstream_name" && value.as_str() == Some(upstream_name))
                || config_references_upstream(value, upstream_name)
        }),
        Value::Array(values) => values
            .iter()
            .any(|value| config_references_upstream(value, upstream_name)),
        _ => false,
    }
}

fn enqueue_upstream_audit(state: &AdminState, upstream: &UpstreamRecord, payload: AuditPayload) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = unix_now_secs();
    let action = payload.to_string();
    let mut entry: AuditEntry = payload.into();
    entry.ts = ts;
    entry.request_id = format!("admin-upstream-{}-{ts}", upstream.id);
    entry.principal_id = "admin".to_owned();
    entry.route = "admin_v1_upstreams".to_owned();
    entry.upstream = upstream.name.clone();
    entry.status = 200;
    entry.duration_ms = 0;
    entry.actor = Some("admin".to_owned());
    entry.admin_action = Some(action);
    let _ = audit_sink.try_enqueue(entry);
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
