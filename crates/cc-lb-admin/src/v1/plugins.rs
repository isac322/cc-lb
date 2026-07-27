use std::io;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use cc_lb_control::{AuditEntry, AuditPayload};
use cc_lb_plugin_wire::metadata::HookMetadata;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, PluginChainConflictReason, PluginChainEntry,
    PluginChainEntryInput, PluginChainEntryUpdate, PluginMetadata, PluginSlotKind, PrincipalStore,
    Storage, StorageError, WasmRegistryEntry, WasmRegistryReference,
    WasmRegistryReferenceFingerprint, sparse_order,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use super::wasm_cache::wasm_cache_path;
use crate::AdminState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/plugins/registry", get(list_registry))
        .route(
            "/admin/v1/plugins/registry/{id}/references",
            get(get_registry_references),
        )
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
            get(get_chain).put(update_chain).delete(delete_chain),
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
struct DeleteRegistryQuery {
    cascade: Option<String>,
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

enum IfMatchError {
    Missing,
    Malformed,
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
    version: Option<String>,
    label: Option<String>,
    size_bytes: u64,
    refcount: i64,
    revision: u64,
    uploaded_at_unix_secs: u64,
    kind: String,
    description: String,
    usage: String,
    hook_metadata: std::collections::BTreeMap<String, HookMetadata>,
    is_builtin: bool,
    metadata: Option<PluginMetadata>,
    supported_slots: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RegistryReferencesResponse {
    registry: RegistryEntryResponse,
    refcount: i64,
    reference_fingerprint: String,
    references: Vec<WasmRegistryReference>,
}

#[derive(Debug, Serialize)]
struct RegistryCascadeDeleteResponse {
    deleted: RegistryEntryResponse,
    reference_fingerprint: String,
    removed_references: Vec<WasmRegistryReference>,
}

#[derive(Debug, Serialize)]
struct ChainListResponse {
    entries: Vec<PluginChainEntry>,
}

#[derive(Debug, Clone, Copy)]
enum SlotParam {
    Stored(PluginSlotKind),
    RuntimeOnly,
}

impl SlotParam {
    fn stored(self) -> Option<PluginSlotKind> {
        match self {
            Self::Stored(slot) => Some(slot),
            Self::RuntimeOnly => None,
        }
    }
}

struct PluginChainAuditMetadata {
    wasm_registry_id: String,
    sha256_hex: String,
    supported_slots: Vec<String>,
}

impl<'de> Deserialize<'de> for SlotParam {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_slot(&value).ok_or_else(|| serde::de::Error::custom("invalid plugin slot"))
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
    let storage_entries = match all_registry_entries(storage).await {
        Ok(entries) => entries,
        Err(error) => return storage_error(error),
    };
    let mut all = storage_entries;
    if !all
        .iter()
        .any(|entry| entry.id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
    {
        all.push(WasmRegistryEntry::builtin_subscription_preference(0));
    }
    all.sort_by_key(|entry| entry.id);
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
    if id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
        return builtin_plugin_immutable();
    }
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

async fn get_registry_references(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let entry = match storage.get_registry_entry_by_id(id).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return error(StatusCode::NOT_FOUND, "unknown_registry_entry"),
        Err(error) => return storage_error(error),
    };
    let size_bytes = match registry_size_bytes(storage, entry.sha256).await {
        Ok(size_bytes) => size_bytes,
        Err(error) => return storage_error(error),
    };
    let references = match storage.list_registry_references(id).await {
        Ok(references) => references,
        Err(error) => return storage_error(error),
    };
    let reference_fingerprint = fingerprint_hex(&references.fingerprint);
    Json(RegistryReferencesResponse {
        refcount: entry.refcount,
        registry: registry_response(entry, size_bytes),
        reference_fingerprint,
        references: references.references,
    })
    .into_response()
}

async fn delete_registry(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    Query(query): Query<DeleteRegistryQuery>,
    headers: HeaderMap,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    if id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
        return builtin_plugin_immutable();
    }
    if query.cascade.as_deref() == Some("references") {
        let Some(fingerprint) = reference_fingerprint_header(&headers) else {
            return error(
                StatusCode::PRECONDITION_REQUIRED,
                "reference_fingerprint_required",
            );
        };
        return match storage
            .cascade_delete_registry_entry(id, expected_revision, fingerprint)
            .await
        {
            Ok(Some(deleted)) => {
                let size_bytes = match registry_size_bytes(storage, deleted.entry.sha256).await {
                    Ok(size_bytes) => size_bytes,
                    Err(error) => return storage_error(error),
                };
                remove_wasm_cache_file(&state, deleted.entry.sha256).await;
                emit_audit(
                    &state,
                    AuditPayload::PluginRegistryDelete {
                        sha256: hex_sha256(deleted.entry.sha256),
                    },
                );
                let reference_fingerprint = fingerprint_hex(
                    &WasmRegistryReferenceFingerprint::from_references(&deleted.references),
                );
                let mut response = Json(RegistryCascadeDeleteResponse {
                    deleted: registry_response(deleted.entry, size_bytes),
                    reference_fingerprint,
                    removed_references: deleted.references,
                })
                .into_response();
                add_dynamic_rebind_headers(&mut response, &state).await;
                response
            }
            Ok(None) => error(StatusCode::NOT_FOUND, "unknown_registry_entry"),
            Err(StorageError::StalePluginRegistryRevision { current }) => stale_revision(current),
            Err(StorageError::StalePluginRegistryReferences) => references_changed(),
            Err(error) => storage_mutation_error(error),
        };
    }
    match storage.delete_registry_entry(id, expected_revision).await {
        Ok(Some(deleted)) => {
            remove_wasm_cache_file(&state, deleted.sha256).await;
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
        Err(StorageError::PluginRegistryReferenced { id }) => plugin_registry_referenced(id),
        Err(StorageError::StalePluginRegistryRevision { current }) => stale_revision(current),
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
    let Some(slot) = query.slot.stored() else {
        return Json(ChainListResponse {
            entries: Vec::new(),
        })
        .into_response();
    };
    match storage.list_chain_for_principal(principal_id, slot).await {
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
    let Some(slot) = body.slot.stored() else {
        return error(StatusCode::BAD_REQUEST, "unsupported_plugin_slot");
    };
    let audit_metadata = match storage
        .get_registry_entry_by_id(body.wasm_registry_id)
        .await
    {
        Ok(Some(entry)) => {
            if entry.supported_slots.is_empty() && !entry.is_builtin {
                return slot_metadata_unknown(&entry.name);
            }
            if !entry.supported_slots.is_empty() && !entry.supported_slots.contains(&slot) {
                return unsupported_slot(&entry.name, slot);
            }
            Some(plugin_chain_audit_metadata(&entry))
        }
        Ok(None) => None,
        Err(error) => return storage_error(error),
    };
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
            let audit_metadata = audit_metadata
                .unwrap_or_else(|| empty_plugin_chain_audit_metadata(entry.wasm_registry_id));
            emit_chain_audit(
                &state,
                principal_id,
                slot,
                audit_metadata.wasm_registry_id,
                audit_metadata.sha256_hex,
                audit_metadata.supported_slots,
            );
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
        Err(StorageError::PrincipalNotFound { id }) => unknown_principal(id),
        Err(StorageError::PluginChainConflict {
            reason: PluginChainConflictReason::InvalidOrderGap,
        }) => invalid_order(),
        Err(StorageError::PluginChainConflict {
            reason: PluginChainConflictReason::SlotIsSingleton { existing_entry_id },
        }) if slot == PluginSlotKind::Shape => slot_singleton(existing_entry_id),
        Err(error) => storage_error(error),
    }
}

async fn update_chain(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(update): Json<PluginChainEntryUpdate>,
) -> axum::response::Response {
    let Some(expected_revision) = if_match_revision(&headers) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    if plugin_chain_update_empty(&update) {
        return error(StatusCode::BAD_REQUEST, "empty_update");
    }
    let current_entry = match find_chain_entry(storage, id).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry"),
        Err(error) => return storage_error(error),
    };
    if current_entry.revision != expected_revision {
        return stale_revision(current_entry.revision);
    }
    match storage
        .get_registry_entry_by_id(current_entry.wasm_registry_id)
        .await
    {
        Ok(Some(registry_entry)) => {
            if registry_entry.supported_slots.is_empty() && !registry_entry.is_builtin {
                return slot_metadata_unknown(&registry_entry.name);
            }
            if registry_entry_unsupported_slot(&registry_entry, current_entry.slot) {
                return unsupported_slot(&registry_entry.name, current_entry.slot);
            }
        }
        Ok(None) => {}
        Err(error) => return storage_error(error),
    }
    match storage
        .update_chain_entry(id, expected_revision, update)
        .await
    {
        Ok(Some(entry)) => {
            let audit_metadata =
                match fetch_plugin_chain_audit_metadata(storage, entry.wasm_registry_id).await {
                    Ok(metadata) => metadata,
                    Err(error) => return storage_error(error),
                };
            emit_chain_audit(
                &state,
                entry.principal_id,
                entry.slot,
                audit_metadata.wasm_registry_id,
                audit_metadata.sha256_hex,
                audit_metadata.supported_slots,
            );
            let mut response = chain_with_etag(entry);
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry"),
        Err(StorageError::InvalidInput { reason, .. }) if reason.contains("empty_update") => {
            error(StatusCode::BAD_REQUEST, "empty_update")
        }
        Err(StorageError::StalePluginChainRevision { current }) => stale_revision(current),
        Err(error) => storage_mutation_error(error),
    }
}

async fn get_chain(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match find_chain_entry(storage, id).await {
        Ok(Some(entry)) => chain_with_etag(entry),
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry"),
        Err(error) => storage_error(error),
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
    let (slot, existing) = match infer_reorder_chain(storage, principal_id, &body.entries).await {
        Ok(result) => result,
        Err(response) => return *response,
    };
    if let Err(response) = revalidate_chain_registry_slots(storage, principal_id, &existing).await {
        return *response;
    }
    let new_orders = body
        .entries
        .into_iter()
        .map(|entry| (entry.id, entry.order, entry.expected_revision))
        .collect();
    match storage.reorder_chain(principal_id, slot, new_orders).await {
        Ok(entries) => {
            let audit_metadata = match first_plugin_chain_audit_metadata(storage, &entries).await {
                Ok(metadata) => metadata,
                Err(error) => return storage_error(error),
            };
            emit_chain_audit(
                &state,
                principal_id,
                slot,
                audit_metadata.wasm_registry_id,
                audit_metadata.sha256_hex,
                audit_metadata.supported_slots,
            );
            let mut response = Json(ChainListResponse { entries }).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(StorageError::PluginChainConflict {
            reason: PluginChainConflictReason::InvalidOrderGap,
        }) => invalid_order(),
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
    let Some(slot) = query.slot.stored() else {
        return Json(ChainListResponse {
            entries: Vec::new(),
        })
        .into_response();
    };
    let existing = match storage.list_chain_for_principal(principal_id, slot).await {
        Ok(entries) => entries,
        Err(error) => return storage_error(error),
    };
    if let Err(response) = revalidate_chain_registry_slots(storage, principal_id, &existing).await {
        return *response;
    }
    match storage.rebalance_chain(principal_id, slot).await {
        Ok(entries) => {
            let audit_metadata = match first_plugin_chain_audit_metadata(storage, &entries).await {
                Ok(metadata) => metadata,
                Err(error) => return storage_error(error),
            };
            emit_chain_audit(
                &state,
                principal_id,
                slot,
                audit_metadata.wasm_registry_id,
                audit_metadata.sha256_hex,
                audit_metadata.supported_slots,
            );
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
    let expected_revision = match required_if_match_revision(&headers) {
        Ok(revision) => revision,
        Err(IfMatchError::Missing) => {
            return error(StatusCode::PRECONDITION_REQUIRED, "if_match_required");
        }
        Err(IfMatchError::Malformed) => return error(StatusCode::BAD_REQUEST, "invalid_if_match"),
    };
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    match storage.delete_chain_entry(id, expected_revision).await {
        Ok(Some(entry)) => {
            let audit_metadata =
                match fetch_plugin_chain_audit_metadata(storage, entry.wasm_registry_id).await {
                    Ok(metadata) => metadata,
                    Err(error) => return storage_error(error),
                };
            emit_chain_audit(
                &state,
                entry.principal_id,
                entry.slot,
                audit_metadata.wasm_registry_id,
                audit_metadata.sha256_hex,
                audit_metadata.supported_slots,
            );
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "unknown_plugin_chain_entry"),
        Err(StorageError::StalePluginChainRevision { current }) => stale_revision(current),
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

async fn find_chain_entry(
    storage: &dyn Storage,
    id: Uuid,
) -> Result<Option<PluginChainEntry>, StorageError> {
    let mut offset = 0;
    loop {
        let principals = PrincipalStore::list(storage, offset, DEFAULT_LIMIT, true).await?;
        if principals.is_empty() {
            return Ok(None);
        }
        offset += principals.len();
        for principal in principals {
            for slot in [
                PluginSlotKind::Router,
                PluginSlotKind::ObservabilityHook,
                PluginSlotKind::Shape,
            ] {
                let entries = storage.list_chain_for_principal(principal.id, slot).await?;
                if let Some(entry) = entries.into_iter().find(|entry| entry.id == id) {
                    return Ok(Some(entry));
                }
            }
        }
    }
}

fn plugin_chain_update_empty(update: &PluginChainEntryUpdate) -> bool {
    update.config.is_none()
        && update.sse_per_event.is_none()
        && update.batched_events_per_flush.is_none()
        && update.batched_flush_ms.is_none()
}

fn registry_entry_unsupported_slot(
    registry_entry: &WasmRegistryEntry,
    slot: PluginSlotKind,
) -> bool {
    !registry_entry.is_builtin
        && !registry_entry.supported_slots.is_empty()
        && !registry_entry.supported_slots.contains(&slot)
}

async fn infer_reorder_chain(
    storage: &dyn Storage,
    principal_id: Uuid,
    entries: &[ReorderEntry],
) -> Result<(PluginSlotKind, Vec<PluginChainEntry>), Box<axum::response::Response>> {
    for slot in [
        PluginSlotKind::Router,
        PluginSlotKind::ObservabilityHook,
        PluginSlotKind::Shape,
    ] {
        let chain = storage
            .list_chain_for_principal(principal_id, slot)
            .await
            .map_err(|error| Box::new(storage_error(error)))?;
        if entries
            .iter()
            .all(|entry| chain.iter().any(|candidate| candidate.id == entry.id))
        {
            return Ok((slot, chain));
        }
    }
    Err(Box::new(error(
        StatusCode::BAD_REQUEST,
        "entry_not_in_chain",
    )))
}

async fn revalidate_chain_registry_slots(
    storage: &dyn Storage,
    principal_id: Uuid,
    entries: &[PluginChainEntry],
) -> Result<(), Box<axum::response::Response>> {
    for entry in entries {
        match storage
            .get_registry_entry_by_id(entry.wasm_registry_id)
            .await
        {
            Ok(Some(registry_entry)) => {
                if registry_entry_unsupported_slot(&registry_entry, entry.slot) {
                    return Err(Box::new(chain_drift_detected(
                        principal_id,
                        entry,
                        &registry_entry,
                    )));
                }
            }
            Ok(None) => {}
            Err(error) => return Err(Box::new(storage_error(error))),
        }
    }
    Ok(())
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
            let upper = entries[index].order;
            let lower = index
                .checked_sub(1)
                .map(|idx| entries[idx].order)
                .unwrap_or(upper - sparse_order::STEP * 2);
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
    let supported_slots = supported_slot_strings(&entry.supported_slots);
    RegistryEntryResponse {
        id: entry.id,
        sha256_hex: hex_sha256(entry.sha256),
        name: entry.name,
        original_filename: entry.original_filename,
        version: entry.version,
        label: entry.label,
        size_bytes,
        refcount: entry.refcount,
        revision: entry.revision,
        uploaded_at_unix_secs: entry.uploaded_at_unix_secs,
        kind: entry.kind,
        description: entry.description,
        usage: entry.usage,
        hook_metadata: entry.hook_metadata,
        is_builtin: entry.is_builtin,
        metadata: entry.metadata,
        supported_slots,
    }
}

async fn fetch_plugin_chain_audit_metadata(
    storage: &dyn Storage,
    wasm_registry_id: Uuid,
) -> Result<PluginChainAuditMetadata, StorageError> {
    let Some(entry) = storage.get_registry_entry_by_id(wasm_registry_id).await? else {
        return Ok(empty_plugin_chain_audit_metadata(wasm_registry_id));
    };
    Ok(plugin_chain_audit_metadata(&entry))
}

async fn first_plugin_chain_audit_metadata(
    storage: &dyn Storage,
    entries: &[PluginChainEntry],
) -> Result<PluginChainAuditMetadata, StorageError> {
    let Some(entry) = entries.first() else {
        return Ok(empty_plugin_chain_audit_metadata(Uuid::nil()));
    };
    fetch_plugin_chain_audit_metadata(storage, entry.wasm_registry_id).await
}

fn plugin_chain_audit_metadata(entry: &WasmRegistryEntry) -> PluginChainAuditMetadata {
    PluginChainAuditMetadata {
        wasm_registry_id: entry.id.to_string(),
        sha256_hex: hex_sha256(entry.sha256),
        supported_slots: supported_slot_strings(&entry.supported_slots),
    }
}

fn empty_plugin_chain_audit_metadata(wasm_registry_id: Uuid) -> PluginChainAuditMetadata {
    PluginChainAuditMetadata {
        wasm_registry_id: wasm_registry_id.to_string(),
        sha256_hex: String::new(),
        supported_slots: Vec::new(),
    }
}

fn supported_slot_strings(slots: &[PluginSlotKind]) -> Vec<String> {
    slots.iter().map(|slot| slot.as_str().to_owned()).collect()
}

fn parse_slot(value: &str) -> Option<SlotParam> {
    match value {
        "Router" | "router" | "filter" => Some(SlotParam::Stored(PluginSlotKind::Router)),
        "ObservabilityHook" | "observability_hook" | "observe" => {
            Some(SlotParam::Stored(PluginSlotKind::ObservabilityHook))
        }
        "Shape" | "shape" => Some(SlotParam::Stored(PluginSlotKind::Shape)),
        "build_signer" | "sign" | "on_unauthorized" => Some(SlotParam::RuntimeOnly),
        _ => None,
    }
}

fn if_match_revision(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(header::IF_MATCH)?.to_str().ok()?.trim();
    parse_revision(value)
}

fn reference_fingerprint_header(headers: &HeaderMap) -> Option<WasmRegistryReferenceFingerprint> {
    let value = headers
        .get("x-reference-fingerprint")?
        .to_str()
        .ok()?
        .trim();
    let bytes = hex::decode(value).ok()?;
    let bytes: [u8; 32] = bytes.try_into().ok()?;
    Some(WasmRegistryReferenceFingerprint::from_bytes(bytes))
}

fn fingerprint_hex(fingerprint: &WasmRegistryReferenceFingerprint) -> String {
    hex::encode(fingerprint.as_bytes())
}

fn required_if_match_revision(headers: &HeaderMap) -> Result<u64, IfMatchError> {
    let Some(value) = headers.get(header::IF_MATCH) else {
        return Err(IfMatchError::Missing);
    };
    value
        .to_str()
        .ok()
        .and_then(parse_revision)
        .ok_or(IfMatchError::Malformed)
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
    match error {
        StorageError::StalePluginRegistryRevision { current }
        | StorageError::StalePluginChainRevision { current } => stale_revision(current),
        StorageError::PluginRegistryReferenced { id } => plugin_registry_referenced(id),
        StorageError::PluginChainConflict {
            reason: PluginChainConflictReason::InvalidOrderGap,
        } => invalid_order(),
        StorageError::PluginChainConflict {
            reason: PluginChainConflictReason::SlotIsSingleton { existing_entry_id },
        } => slot_singleton(existing_entry_id),
        error => storage_error(error),
    }
}

async fn remove_wasm_cache_file(state: &AdminState, sha256: [u8; 32]) {
    let sha256_hex = hex_sha256(sha256);
    let cache_path = wasm_cache_path(state, &sha256_hex);
    match tokio::fs::remove_file(&cache_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(%error, path = %cache_path.display(), sha256 = %sha256_hex, "failed to remove wasm cache file after registry delete")
        }
    }
}

fn builtin_plugin_immutable() -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": "builtin_plugin_immutable" })),
    )
        .into_response()
}

fn plugin_registry_referenced(id: String) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": "plugin_registry_referenced", "id": id })),
    )
        .into_response()
}

fn stale_revision(current: u64) -> axum::response::Response {
    (
        StatusCode::PRECONDITION_FAILED,
        Json(json!({ "error": "stale_revision", "current": current })),
    )
        .into_response()
}

fn references_changed() -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": "references_changed" })),
    )
        .into_response()
}

fn unknown_principal(id: String) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "unknown_principal", "id": id })),
    )
        .into_response()
}

fn invalid_order() -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": "invalid_order" })),
    )
        .into_response()
}

fn slot_singleton(existing_entry_id: Uuid) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "slot_singleton",
            "existing_entry_id": existing_entry_id.to_string()
        })),
    )
        .into_response()
}

fn chain_drift_detected(
    principal_id: Uuid,
    entry: &PluginChainEntry,
    registry_entry: &WasmRegistryEntry,
) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "chain_drift_detected",
            "principal_id": principal_id,
            "wasm_registry_id": entry.wasm_registry_id,
            "chain_slot": entry.slot.as_str(),
            "supported_slots": supported_slot_strings(&registry_entry.supported_slots),
        })),
    )
        .into_response()
}

fn unsupported_slot(plugin_name: &str, slot: PluginSlotKind) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": "unsupported_slot",
            "plugin_name": plugin_name,
            "slot": slot.as_str(),
        })),
    )
        .into_response()
}

fn slot_metadata_unknown(plugin_name: &str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": "slot_metadata_unknown",
            "plugin_name": plugin_name,
            "hint": "re-upload the plugin or restart the server so supported_slots can be backfilled",
        })),
    )
        .into_response()
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

fn emit_chain_audit(
    state: &AdminState,
    principal_id: Uuid,
    slot: PluginSlotKind,
    wasm_registry_id: String,
    sha256_hex: String,
    supported_slots: Vec<String>,
) {
    emit_audit(
        state,
        AuditPayload::PluginChainUpdate {
            principal_id: principal_id.to_string(),
            slots_changed: vec![slot.as_str()],
            wasm_registry_id,
            sha256_hex,
            supported_slots,
        },
    );
}

fn emit_audit(state: &AdminState, payload: AuditPayload) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let action = payload.to_string();
    let ts = cc_lb_clock::unix_secs(state.clock.now());
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
