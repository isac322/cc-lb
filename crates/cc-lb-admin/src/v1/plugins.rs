use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post, put},
};
use cc_lb_core::{AuditEntry, AuditPayload};
use cc_lb_storage_api::{
    PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate, PluginSlot, PrincipalStore,
    Storage, StorageError, WasmRegistryEntry, sparse_order,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use crate::AdminState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/plugins/registry", get(list_registry))
        .route(
            "/admin/v1/plugins/registry/{id}",
            get(get_registry)
                .patch(patch_registry)
                .delete(delete_registry),
        )
        .route(
            "/admin/v1/principals/{principal_id}/plugin-chain",
            get(list_chain).post(insert_chain),
        )
        .route(
            "/admin/v1/principals/{principal_id}/plugin-chain/reorder",
            post(reorder_chain),
        )
        .route(
            "/admin/v1/principals/{principal_id}/plugin-chain/rebalance",
            post(rebalance_chain),
        )
        .route(
            "/admin/v1/plugin-chain-entries/{id}",
            put(update_chain).delete(delete_chain),
        )
}

#[derive(Debug, Deserialize)]
struct RegistryQuery {
    after: Option<Uuid>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SlotQuery {
    slot: SlotParam,
}

#[derive(Debug, Deserialize)]
struct PatchRegistryBody {
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InsertChainBody {
    slot: SlotParam,
    wasm_registry_id: Uuid,
    config: Option<Value>,
    sse_per_event: Option<bool>,
    batched_events_per_flush: Option<u32>,
    batched_flush_ms: Option<u64>,
    position: Option<Position>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Position {
    Named(String),
    Before { before: Uuid },
    After { after: Uuid },
}

#[derive(Debug, Deserialize)]
struct ReorderBody {
    entries: Vec<ReorderEntry>,
}

#[derive(Debug, Deserialize)]
struct ReorderEntry {
    id: Uuid,
    order: i64,
    expected_revision: u64,
}

#[derive(Debug, Serialize)]
struct RegistryListResponse {
    entries: Vec<RegistryEntryResponse>,
}

#[derive(Debug, Serialize)]
struct RegistryEntryResponse {
    id: Uuid,
    sha256_hex: String,
    name: String,
    original_filename: String,
    label: Option<String>,
    size_bytes: u64,
    refcount: i64,
    revision: u64,
    uploaded_at_unix_secs: u64,
}

#[derive(Debug, Serialize)]
struct ChainListResponse {
    entries: Vec<PluginChainEntry>,
}

#[derive(Debug, Serialize)]
struct ReferenceResponse {
    kind: &'static str,
    id: String,
    principal_id: String,
}

#[derive(Debug, Clone, Copy)]
struct SlotParam(PluginSlot);

impl<'de> Deserialize<'de> for SlotParam {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_slot(&value)
            .map(SlotParam)
            .ok_or_else(|| serde::de::Error::custom("invalid plugin slot"))
    }
}

async fn list_registry(
    State(state): State<AdminState>,
    Query(query): Query<RegistryQuery>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let all = match all_registry_entries(storage).await {
        Ok(entries) => entries,
        Err(error) => return storage_error(error),
    };
    let total = all.len();
    let start = query
        .after
        .and_then(|id| {
            all.iter()
                .position(|entry| entry.id == id)
                .map(|idx| idx + 1)
        })
        .unwrap_or(0);
    let page = all.into_iter().skip(start).take(limit).collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(page.len());
    for entry in page {
        let size_bytes = match registry_size_bytes(storage, entry.sha256).await {
            Ok(size_bytes) => size_bytes,
            Err(error) => return storage_error(error),
        };
        entries.push(registry_response(entry, size_bytes));
    }
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, "x-total-count", &total.to_string());
    (headers, Json(RegistryListResponse { entries })).into_response()
}

async fn get_registry(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match storage.get_registry_entry_by_id(id).await {
        Ok(Some(entry)) => registry_with_etag(storage, entry).await,
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_registry_entry"),
        Err(error) => storage_error(error),
    }
}

async fn patch_registry(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<PatchRegistryBody>,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match storage
        .update_registry_label(id, expected_revision, body.label)
        .await
    {
        Ok(entry) => {
            let mut response = registry_with_etag(storage, entry).await;
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(error) => storage_mutation_error(error),
    }
}

async fn delete_registry(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let entry = match storage.get_registry_entry_by_id(id).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return error(StatusCode::NOT_FOUND, "unknown_registry_entry"),
        Err(error) => return storage_error(error),
    };
    if entry.revision != expected_revision {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "stale_revision", "current_revision": entry.revision })),
        )
            .into_response();
    }
    match registry_references(storage, id).await {
        Ok(references) if !references.is_empty() => {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "referenced_by",
                    "references": references,
                })),
            )
                .into_response();
        }
        Ok(_) => {}
        Err(error) => return storage_error(error),
    }
    match storage.delete_registry_entry(id, expected_revision).await {
        Ok(Some(deleted)) => {
            if let Err(error) = storage
                .decrement_blob_refcount_or_delete(deleted.sha256)
                .await
            {
                return storage_error(error);
            }
            emit_audit(
                &state,
                AuditPayload::PluginRegistryDelete {
                    sha256: hex_sha256(deleted.sha256),
                },
            );
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_registry_entry"),
        Err(error) => storage_mutation_error(error),
    }
}

async fn list_chain(
    State(state): State<AdminState>,
    Path(principal_id): Path<Uuid>,
    Query(query): Query<SlotQuery>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match storage
        .list_chain_for_principal(principal_id, query.slot.0)
        .await
    {
        Ok(entries) => Json(ChainListResponse { entries }).into_response(),
        Err(error) => storage_error(error),
    }
}

async fn insert_chain(
    State(state): State<AdminState>,
    Path(principal_id): Path<Uuid>,
    Json(body): Json<InsertChainBody>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let slot = body.slot.0;
    let existing = match storage.list_chain_for_principal(principal_id, slot).await {
        Ok(entries) => entries,
        Err(error) => return storage_error(error),
    };
    let order = match compute_order(&existing, body.position.as_ref()) {
        Ok(order) => order,
        Err(response) => return *response,
    };
    let input = PluginChainEntryInput {
        principal_id,
        slot,
        order,
        wasm_registry_id: body.wasm_registry_id,
        config: body.config.unwrap_or_else(|| json!({})),
        sse_per_event: body.sse_per_event.unwrap_or(false),
        batched_events_per_flush: body.batched_events_per_flush.unwrap_or(1),
        batched_flush_ms: body.batched_flush_ms.unwrap_or(100),
    };
    match storage.insert_chain_entry(input).await {
        Ok(entry) => {
            emit_chain_audit(&state, principal_id, slot);
            let mut headers = HeaderMap::new();
            insert_header(
                &mut headers,
                header::LOCATION,
                &format!("/admin/v1/plugin-chain-entries/{}", entry.id),
            );
            insert_header(&mut headers, header::ETAG, &etag(entry.revision));
            let mut response = (StatusCode::CREATED, headers, Json(entry)).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(error) => storage_error(error),
    }
}

async fn update_chain(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    for field in ["slot", "wasm_registry_id", "order", "principal_id"] {
        if body.get(field).is_some() {
            return error(StatusCode::BAD_REQUEST, "immutable_field");
        }
    }
    let update = PluginChainEntryUpdate {
        config: body.get("config").cloned(),
        sse_per_event: body.get("sse_per_event").and_then(Value::as_bool),
        batched_events_per_flush: body
            .get("batched_events_per_flush")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok()),
        batched_flush_ms: body.get("batched_flush_ms").and_then(Value::as_u64),
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match storage
        .update_chain_entry(id, expected_revision, update)
        .await
    {
        Ok(Some(entry)) => {
            emit_chain_audit(&state, entry.principal_id, entry.slot);
            let mut response = chain_with_etag(entry);
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry"),
        Err(error) => storage_mutation_error(error),
    }
}

async fn reorder_chain(
    State(state): State<AdminState>,
    Path(principal_id): Path<Uuid>,
    Json(body): Json<ReorderBody>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let orders = body
        .entries
        .iter()
        .map(|entry| entry.order)
        .collect::<Vec<_>>();
    if sparse_order::needs_rebalance(&orders) {
        return needs_rebalance();
    }
    let slot = match infer_reorder_slot(storage, principal_id, &body.entries).await {
        Ok(slot) => slot,
        Err(response) => return response,
    };
    let new_orders = body
        .entries
        .into_iter()
        .map(|entry| (entry.id, entry.order, entry.expected_revision))
        .collect();
    match storage.reorder_chain(principal_id, slot, new_orders).await {
        Ok(entries) => {
            emit_chain_audit(&state, principal_id, slot);
            let mut response = Json(ChainListResponse { entries }).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(error) => storage_mutation_error(error),
    }
}

async fn rebalance_chain(
    State(state): State<AdminState>,
    Path(principal_id): Path<Uuid>,
    Query(query): Query<SlotQuery>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match storage.rebalance_chain(principal_id, query.slot.0).await {
        Ok(entries) => {
            let mut response = Json(ChainListResponse { entries }).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(error) => storage_error(error),
    }
}

async fn delete_chain(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let Some(entry) = find_chain_entry(storage, id).await else {
        return error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry");
    };
    if entry.revision != expected_revision {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "stale_revision", "current_revision": entry.revision })),
        )
            .into_response();
    }
    match storage.delete_chain_entry(id, expected_revision).await {
        Ok(Some(_)) => {
            emit_chain_audit(&state, entry.principal_id, entry.slot);
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry"),
        Err(error) => storage_error(error),
    }
}

async fn all_registry_entries(
    storage: &dyn Storage,
) -> Result<Vec<WasmRegistryEntry>, StorageError> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = storage.list_registry(after, DEFAULT_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|entry| entry.id);
        all.extend(page);
    }
    Ok(all)
}

async fn registry_references(
    storage: &dyn Storage,
    registry_id: Uuid,
) -> Result<Vec<ReferenceResponse>, StorageError> {
    let mut references = Vec::new();
    let mut offset = 0;
    loop {
        let principals = PrincipalStore::list(storage, offset, DEFAULT_LIMIT, false).await?;
        if principals.is_empty() {
            break;
        }
        offset += principals.len();
        for principal in principals {
            for slot in [
                PluginSlot::Router,
                PluginSlot::ObservabilityHook,
                PluginSlot::Shape,
            ] {
                let entries = storage.list_chain_for_principal(principal.id, slot).await?;
                references.extend(entries.into_iter().filter_map(|entry| {
                    (entry.wasm_registry_id == registry_id).then(|| ReferenceResponse {
                        kind: "plugin_chain",
                        id: entry.id.to_string(),
                        principal_id: principal.id.to_string(),
                    })
                }));
            }
        }
    }
    Ok(references)
}

async fn infer_reorder_slot(
    storage: &dyn Storage,
    principal_id: Uuid,
    entries: &[ReorderEntry],
) -> Result<PluginSlot, axum::response::Response> {
    for slot in [
        PluginSlot::Router,
        PluginSlot::ObservabilityHook,
        PluginSlot::Shape,
    ] {
        let chain = storage
            .list_chain_for_principal(principal_id, slot)
            .await
            .map_err(storage_error)?;
        if entries
            .iter()
            .all(|entry| chain.iter().any(|candidate| candidate.id == entry.id))
        {
            return Ok(slot);
        }
    }
    Err(error(StatusCode::BAD_REQUEST, "entry_not_in_chain"))
}

async fn find_chain_entry(storage: &dyn Storage, id: Uuid) -> Option<PluginChainEntry> {
    let mut offset = 0;
    loop {
        let principals = PrincipalStore::list(storage, offset, DEFAULT_LIMIT, false)
            .await
            .ok()?;
        if principals.is_empty() {
            return None;
        }
        offset += principals.len();
        for principal in principals {
            for slot in [
                PluginSlot::Router,
                PluginSlot::ObservabilityHook,
                PluginSlot::Shape,
            ] {
                let entries = storage
                    .list_chain_for_principal(principal.id, slot)
                    .await
                    .ok()?;
                if let Some(entry) = entries.into_iter().find(|entry| entry.id == id) {
                    return Some(entry);
                }
            }
        }
    }
}

fn compute_order(
    entries: &[PluginChainEntry],
    position: Option<&Position>,
) -> Result<i64, Box<axum::response::Response>> {
    match position.unwrap_or(&Position::Named("last".to_owned())) {
        Position::Named(value) if value == "last" => Ok(sparse_order::next_after(
            &entries.iter().map(|entry| entry.order).collect::<Vec<_>>(),
        )),
        Position::Named(value) if value == "first" => Ok(entries
            .iter()
            .map(|entry| entry.order)
            .min()
            .map(|order| order - sparse_order::STEP)
            .unwrap_or(sparse_order::STEP)),
        Position::Before { before } => {
            let Some(index) = entries.iter().position(|entry| entry.id == *before) else {
                return Err(Box::new(error(
                    StatusCode::BAD_REQUEST,
                    "unknown_before_entry",
                )));
            };
            let lower = index
                .checked_sub(1)
                .map(|idx| entries[idx].order)
                .unwrap_or(0);
            let upper = entries[index].order;
            between_or_rebalance(lower, upper)
        }
        Position::After { after } => {
            let Some(index) = entries.iter().position(|entry| entry.id == *after) else {
                return Err(Box::new(error(
                    StatusCode::BAD_REQUEST,
                    "unknown_after_entry",
                )));
            };
            let lower = entries[index].order;
            let upper = entries
                .get(index + 1)
                .map(|entry| entry.order)
                .unwrap_or(lower + sparse_order::STEP * 2);
            between_or_rebalance(lower, upper)
        }
        Position::Named(_) => Err(Box::new(error(StatusCode::BAD_REQUEST, "invalid_position"))),
    }
}

fn between_or_rebalance(lower: i64, upper: i64) -> Result<i64, Box<axum::response::Response>> {
    if upper - lower < 2 {
        return Err(Box::new(needs_rebalance()));
    }
    Ok(sparse_order::between(lower, upper))
}

async fn registry_with_etag(
    storage: &dyn Storage,
    entry: WasmRegistryEntry,
) -> axum::response::Response {
    let size_bytes = match registry_size_bytes(storage, entry.sha256).await {
        Ok(size_bytes) => size_bytes,
        Err(error) => return storage_error(error),
    };
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, header::ETAG, &etag(entry.revision));
    (headers, Json(registry_response(entry, size_bytes))).into_response()
}

fn chain_with_etag(entry: PluginChainEntry) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, header::ETAG, &etag(entry.revision));
    (headers, Json(entry)).into_response()
}

async fn registry_size_bytes(storage: &dyn Storage, sha256: [u8; 32]) -> Result<u64, StorageError> {
    Ok(storage
        .get_blob_bytes(sha256)
        .await?
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0))
}

fn registry_response(entry: WasmRegistryEntry, size_bytes: u64) -> RegistryEntryResponse {
    RegistryEntryResponse {
        id: entry.id,
        sha256_hex: hex_sha256(entry.sha256),
        name: entry.name,
        original_filename: entry.original_filename,
        label: entry.label,
        size_bytes,
        refcount: entry.refcount,
        revision: entry.revision,
        uploaded_at_unix_secs: entry.uploaded_at_unix_secs,
    }
}

fn parse_slot(value: &str) -> Option<PluginSlot> {
    match value {
        "Router" | "router" => Some(PluginSlot::Router),
        "ObservabilityHook" | "observability_hook" => Some(PluginSlot::ObservabilityHook),
        "Shape" | "shape" => Some(PluginSlot::Shape),
        _ => None,
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

fn needs_rebalance() -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": "needs_rebalance", "hint": "call /rebalance first" })),
    )
        .into_response()
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
    if let StorageError::Conflict { message } = &error
        && message.contains("rebalance")
    {
        return needs_rebalance();
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
            tracing::error!(error = %source, "admin v1 plugin storage operation failed");
            error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error")
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

fn error(status: StatusCode, code: &str) -> axum::response::Response {
    (status, Json(json!({ "error": code }))).into_response()
}

fn emit_chain_audit(state: &AdminState, principal_id: Uuid, slot: PluginSlot) {
    emit_audit(
        state,
        AuditPayload::PluginChainUpdate {
            principal_id: principal_id.to_string(),
            slots_changed: vec![slot.as_str()],
        },
    );
}

fn emit_audit(state: &AdminState, payload: AuditPayload) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let action = payload.to_string();
    let ts = unix_now_secs();
    let mut entry: AuditEntry = payload.into();
    entry.ts = ts;
    entry.request_id = format!("admin-v1-plugin-{ts}");
    entry.principal_id = String::new();
    entry.route = "admin_v1_plugins".to_owned();
    entry.status = 200;
    entry.actor = Some("admin".to_owned());
    entry.admin_action = Some(action);
    let _ = audit_sink.try_enqueue(entry);
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
