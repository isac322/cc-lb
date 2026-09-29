use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Extension, Multipart, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use cc_lb_control::AuditPayload;
use cc_lb_plugin_wire::schema::HookKind;
use cc_lb_runtime_wasmtime::{ModuleInspection, WasmtimeRuntime, WasmtimeRuntimeError};
use cc_lb_storage_api::{
    MAX_WASM_BLOB_BYTES, PluginSlotKind, StorageError, WasmBlob, WasmRegistryEntry,
    WasmRegistryEntryInput,
};
use semver::Version;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use super::wasm_cache::{data_dir, wasm_cache_path};
use crate::{
    AdminState,
    audit::{AdminAuditEvent, record_admin_audit},
    auth::AdminIdentity,
};

const WASM_MAGIC: &[u8; 4] = b"\0asm";
const MIN_WASM_BYTES: usize = 8;
const UPLOAD_ROUTE: &str = "/admin/v1/plugins/wasm";
const UPLOAD_WINDOW: Duration = Duration::from_secs(60);
const UPLOAD_LIMIT: usize = 10;

#[derive(Clone, Default)]
struct UploadRateLimitState {
    buckets: Arc<Mutex<HashMap<Uuid, VecDeque<Instant>>>>,
}

#[derive(Serialize)]
struct UploadResponse {
    sha256_hex: String,
    id: Uuid,
    size_bytes: u64,
    original_filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    revision: u64,
    idempotent: bool,
    action: UploadAction,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum UploadAction {
    Created,
    Noop,
    Replaced,
}

#[derive(Default)]
struct UploadParts {
    bytes: Option<Vec<u8>>,
    original_filename: Option<String>,
    confirm_replacement: bool,
    replace_registry_id: Option<Uuid>,
    expected_revision: Option<u64>,
}

pub fn router() -> Router<AdminState> {
    let limiter = UploadRateLimitState::default();
    let upload_layers =
        ServiceBuilder::new().layer(middleware::from_fn_with_state(limiter, upload_rate_limit));

    Router::new()
        .route(UPLOAD_ROUTE, post(upload_wasm).route_layer(upload_layers))
        .route("/admin/v1/plugins/wasm/gc", post(gc_wasm))
        .layer(DefaultBodyLimit::max(
            (MAX_WASM_BLOB_BYTES as usize) + 4 * 1024 * 1024,
        ))
}

async fn upload_rate_limit(
    State(limiter): State<UploadRateLimitState>,
    Extension(identity): Extension<AdminIdentity>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let admin_id = admin_id(&identity);
    let now = Instant::now();
    let mut buckets = limiter.buckets.lock().await;
    let bucket = buckets.entry(admin_id).or_default();
    while bucket
        .front()
        .is_some_and(|seen| now.duration_since(*seen) >= UPLOAD_WINDOW)
    {
        bucket.pop_front();
    }
    if bucket.len() >= UPLOAD_LIMIT {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "60")],
            Json(json!({ "error": "rate_limited" })),
        )
            .into_response();
    }
    bucket.push_back(now);
    drop(buckets);
    next.run(request).await
}

async fn upload_wasm(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    multipart: Multipart,
) -> Response {
    if let Some(storage) = &state.storage {
        // With storage configured, the shared audit log is the source of truth
        // so replicas cannot exceed the cap in combination. Fetch the actor's
        // window unbounded (i64::MAX keeps the limit bind in range) and count
        // only this route's rows.
        let now = cc_lb_clock::unix_secs(state.clock.now());
        let since = now.saturating_sub(UPLOAD_WINDOW.as_secs());
        match storage
            .query_audit_by_actor(
                &identity.authority,
                &identity.subject,
                since,
                now,
                i64::MAX as usize,
            )
            .await
        {
            Ok(entries) => {
                let uploads = entries
                    .iter()
                    .filter(|entry| entry.route == UPLOAD_ROUTE)
                    .count();
                if uploads >= UPLOAD_LIMIT {
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        [(header::RETRY_AFTER, "60")],
                        Json(json!({ "error": "rate_limited" })),
                    )
                        .into_response();
                }
            }
            Err(error) => return storage_response(error),
        }
    }
    match upload_wasm_inner(&state, &identity, multipart).await {
        Ok((status, response)) => {
            let payload = AuditPayload::PluginRegistryUpload {
                sha256: response.sha256_hex.clone(),
                size_bytes: response.size_bytes,
                original_filename: response.original_filename.clone(),
            };
            let action = payload.to_string();
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action: &action,
                    route: UPLOAD_ROUTE,
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: status.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed(error, &action);
            }
            let mut builder = Response::builder().status(status);
            if status == StatusCode::CREATED {
                builder = builder.header(
                    header::LOCATION,
                    format!("/admin/v1/plugins/registry/{}", response.id),
                );
            }
            let mut response = builder
                .header("X-Idempotent", response.idempotent.to_string())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&response).unwrap_or_default(),
                ))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
            add_dynamic_rebind_headers(&mut response, &state).await;
            response
        }
        Err(error) => {
            let status = error.status().as_u16();
            let action = "plugin_registry_upload_attempt";
            if let Err(audit_error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: UPLOAD_ROUTE,
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status,
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed(audit_error, action);
            }
            (*error).into_response()
        }
    }
}

async fn upload_wasm_inner(
    state: &AdminState,
    identity: &AdminIdentity,
    multipart: Multipart,
) -> Result<(StatusCode, UploadResponse), Box<Response>> {
    let storage = state.storage.as_deref().ok_or_else(|| {
        Box::new(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "storage required",
        ))
    })?;
    let parts = read_upload_parts(multipart).await?;
    let bytes = parts.bytes.ok_or_else(|| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: bytes",
        ))
    })?;
    validate_wasm_bytes(&bytes).map_err(Box::new)?;
    let inspection = inspect_with_wasmtime(state, &bytes).await?;
    let supported_slots = supported_slots_from_inspection(&inspection);
    let registry_name = inspection.metadata.name.clone();
    let original_filename = parts
        .original_filename
        .as_deref()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{registry_name}.wasm"));
    validate_original_filename(&original_filename).map_err(Box::new)?;

    let sha256 = tokio::task::spawn_blocking({
        let bytes = bytes.clone();
        move || Sha256::digest(&bytes).into()
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "wasm sha256 worker failed");
        Box::new(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "hash_failed",
            "sha256 computation failed",
        ))
    })?;
    let sha256_hex = hex_sha256(sha256);
    let existing_by_name = storage
        .get_registry_entry_by_name(&registry_name)
        .await
        .map_err(|error| Box::new(storage_response(error)))?;
    let admin_id = admin_id(identity);
    let uploaded_at_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    let blob = WasmBlob {
        sha256,
        size_bytes: bytes.len() as u64,
        bytes: bytes.clone(),
    };
    let entry_input = WasmRegistryEntryInput {
        name: registry_name.clone(),
        version: Some(inspection.metadata.version.clone()),
        original_filename: original_filename.clone(),
        label: None,
        uploaded_at_unix_secs,
        uploaded_by_admin_id: admin_id,
        description: inspection.metadata.description.clone(),
        usage: inspection.metadata.usage.clone(),
        hook_metadata: inspection.metadata.hooks.clone(),
        supported_slots,
    };

    let (entry, status, action, idempotent, old_sha256_hex) = match existing_by_name {
        Some(existing) if existing.sha256 == sha256 => {
            materialize_cache(state, &sha256_hex, &bytes)
                .await
                .map_err(cache_materialization_response(&sha256_hex))?;
            (existing, StatusCode::OK, UploadAction::Noop, true, None)
        }
        Some(existing) => {
            let replacement = decide_replacement(&existing.version, &inspection.metadata.version);
            if !parts.confirm_replacement && !replacement.is_auto_allowed {
                return Err(Box::new(replacement_confirmation_required(
                    &existing,
                    &sha256_hex,
                    &inspection.metadata.version,
                )));
            }
            if parts.confirm_replacement {
                validate_replacement_confirmation(
                    parts.replace_registry_id,
                    parts.expected_revision,
                    &existing,
                )?;
            }
            let old_sha256_hex = hex_sha256(existing.sha256);
            let expected_revision = parts.expected_revision.unwrap_or(existing.revision);
            materialize_cache(state, &sha256_hex, &bytes)
                .await
                .map_err(cache_materialization_response(&sha256_hex))?;
            let entry = storage
                .replace_wasm_entry(blob, entry_input, expected_revision)
                .await
                .map_err(storage_response)?;
            (
                entry,
                StatusCode::OK,
                UploadAction::Replaced,
                false,
                Some(old_sha256_hex),
            )
        }
        None => {
            let same_sha_entry = storage
                .get_registry_entry_by_sha(sha256)
                .await
                .map_err(|error| Box::new(storage_response(error)))?;
            if let Some(entry) = same_sha_entry {
                return Err(Box::new(json_error(
                    StatusCode::CONFLICT,
                    "wasm_upload_conflict",
                    format!(
                        "wasm sha already belongs to registry entry `{}` ({})",
                        entry.name, entry.id
                    ),
                )));
            }
            materialize_cache(state, &sha256_hex, &bytes)
                .await
                .map_err(cache_materialization_response(&sha256_hex))?;
            let (entry, existed) = storage
                .persist_wasm_upload(blob, entry_input)
                .await
                .map_err(storage_response)?;
            let status = if existed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            let action = if existed {
                UploadAction::Noop
            } else {
                UploadAction::Created
            };
            (entry, status, action, existed, None)
        }
    };

    if let Some(old_sha256_hex) = old_sha256_hex {
        remove_cache_file(state, &old_sha256_hex).await;
    }

    Ok((
        status,
        UploadResponse {
            sha256_hex,
            id: entry.id,
            size_bytes: bytes.len() as u64,
            original_filename: entry.original_filename,
            version: entry.version,
            revision: entry.revision,
            idempotent,
            action,
        },
    ))
}

async fn read_upload_parts(mut multipart: Multipart) -> Result<UploadParts, Box<Response>> {
    let mut parts = UploadParts::default();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| Box::new(multipart_error_response(error)))?
    {
        let Some(name) = field.name().map(ToOwned::to_owned) else {
            continue;
        };
        match name.as_str() {
            "bytes" => {
                let data = field
                    .bytes()
                    .await
                    .map_err(|error| Box::new(multipart_error_response(error)))?;
                if data.len() as u64 > MAX_WASM_BLOB_BYTES {
                    return Err(Box::new(json_error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "wasm_too_large",
                        "bytes part exceeds 32 MiB",
                    )));
                }
                parts.bytes = Some(data.to_vec());
            }
            "original_filename" => {
                parts.original_filename = Some(field.text().await.map_err(|error| {
                    Box::new(json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    ))
                })?);
            }
            "confirm_replacement" => {
                let value = field.text().await.map_err(|error| {
                    Box::new(json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    ))
                })?;
                parts.confirm_replacement = parse_bool_field("confirm_replacement", &value)?;
            }
            "replace_registry_id" => {
                let value = field.text().await.map_err(|error| {
                    Box::new(json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    ))
                })?;
                parts.replace_registry_id = Some(parse_uuid_field("replace_registry_id", &value)?);
            }
            "expected_revision" => {
                let value = field.text().await.map_err(|error| {
                    Box::new(json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    ))
                })?;
                parts.expected_revision = Some(parse_u64_field("expected_revision", &value)?);
            }
            _ => {}
        }
    }
    Ok(parts)
}

fn parse_bool_field(name: &str, value: &str) -> Result<bool, Box<Response>> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_multipart",
            format!("{name} must be true or false"),
        ))),
    }
}

fn parse_uuid_field(name: &str, value: &str) -> Result<Uuid, Box<Response>> {
    Uuid::parse_str(value).map_err(|error| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_multipart",
            format!("{name} must be a UUID: {error}"),
        ))
    })
}

fn parse_u64_field(name: &str, value: &str) -> Result<u64, Box<Response>> {
    value.parse::<u64>().map_err(|error| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_multipart",
            format!("{name} must be an unsigned integer: {error}"),
        ))
    })
}

fn multipart_error_response(error: axum::extract::multipart::MultipartError) -> Response {
    let reason = error.to_string();
    let status = if reason.contains("field")
        || reason.contains("limit")
        || reason.contains("length")
        || reason.contains("request body")
    {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::BAD_REQUEST
    };
    json_error(status, "invalid_multipart", reason)
}

struct ReplacementDecision {
    is_auto_allowed: bool,
}

fn decide_replacement(current: &Option<String>, incoming: &str) -> ReplacementDecision {
    let is_auto_allowed = current
        .as_deref()
        .and_then(|value| Version::parse(value).ok())
        .zip(Version::parse(incoming).ok())
        .is_some_and(|(current, incoming)| incoming > current);
    ReplacementDecision { is_auto_allowed }
}

fn validate_replacement_confirmation(
    replace_registry_id: Option<Uuid>,
    expected_revision: Option<u64>,
    existing: &WasmRegistryEntry,
) -> Result<(), Box<Response>> {
    let Some(replace_registry_id) = replace_registry_id else {
        return Err(Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: replace_registry_id",
        )));
    };
    if replace_registry_id != existing.id {
        return Err(Box::new(json_error(
            StatusCode::CONFLICT,
            "replacement_confirmation_mismatch",
            "replace_registry_id does not match the current registry entry",
        )));
    }
    if expected_revision.is_none() {
        return Err(Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: expected_revision",
        )));
    }
    Ok(())
}

fn replacement_confirmation_required(
    existing: &WasmRegistryEntry,
    incoming_sha256_hex: &str,
    incoming_version: &str,
) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "replacement_confirmation_required",
            "name": existing.name,
            "replace_registry_id": existing.id,
            "expected_revision": existing.revision,
            "current_version": existing.version,
            "incoming_version": incoming_version,
            "current_sha256_hex": hex_sha256(existing.sha256),
            "incoming_sha256_hex": incoming_sha256_hex,
        })),
    )
        .into_response()
}

async fn gc_wasm(State(state): State<AdminState>) -> Response {
    let Some(storage) = state.storage.as_deref() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "storage required",
        );
    };
    let orphaned = match storage.list_orphan_blobs().await {
        Ok(orphaned) => orphaned,
        Err(error) => return storage_response(error),
    };
    let mut removed = Vec::new();
    for sha in orphaned {
        let sha_hex = hex_sha256(sha);
        match storage.decrement_blob_refcount_or_delete(sha).await {
            Ok(true) => {
                let cache_path = wasm_cache_path(&state, &sha_hex);
                match tokio::fs::remove_file(&cache_path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => {
                        tracing::warn!(%error, path = %cache_path.display(), "failed to remove wasm cache file")
                    }
                }
                tracing::info!(sha256 = %sha_hex, "removed orphan wasm blob");
                removed.push(sha_hex);
            }
            Ok(false) => {}
            Err(error) => return storage_response(error),
        }
    }
    let count = removed.len();
    Json(json!({ "removed": removed, "count": count })).into_response()
}

#[allow(clippy::result_large_err)]
fn validate_original_filename(filename: &str) -> Result<(), Response> {
    let reason = if filename.contains('/') {
        Some("filename cannot contain slash")
    } else if filename.contains('\\') {
        Some("filename cannot contain backslash")
    } else if filename.contains("..") {
        Some("filename cannot contain traversal segment")
    } else if filename.contains('\0') {
        Some("filename cannot contain NUL")
    } else if filename.len() > 255 {
        Some("filename cannot exceed 255 bytes")
    } else {
        None
    };
    if let Some(reason) = reason {
        Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_filename", "reason": reason })),
        )
            .into_response())
    } else {
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
fn validate_wasm_bytes(bytes: &[u8]) -> Result<(), Response> {
    if bytes.len() as u64 > MAX_WASM_BLOB_BYTES {
        return Err(json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "wasm_too_large",
            "bytes part exceeds 32 MiB",
        ));
    }
    if bytes.len() < MIN_WASM_BYTES {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_wasm_length",
            "wasm bytes must be at least 8 bytes",
        ));
    }
    if &bytes[..4] != WASM_MAGIC {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_wasm_magic",
            "wasm bytes must start with \\0asm",
        ));
    }
    Ok(())
}

async fn inspect_with_wasmtime(
    state: &AdminState,
    bytes: &[u8],
) -> Result<ModuleInspection, Box<Response>> {
    let bytes = bytes.to_vec();
    let runtime = state.runtime.clone();
    let inspection = tokio::task::spawn_blocking(move || match runtime {
        Some(runtime) => runtime.admit_wasm_agnostic(&bytes),
        None => WasmtimeRuntime::with_defaults()?.admit_wasm_agnostic(&bytes),
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "wasm inspect worker failed");
        Box::new(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "inspect_worker_failed",
            "wasm inspect worker panicked",
        ))
    })?;
    inspection.map_err(|error| {
        let (status, code) = match &error {
            WasmtimeRuntimeError::ModuleRejected { .. } => {
                (StatusCode::BAD_REQUEST, "invalid_wasm")
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "inspect_failed"),
        };
        let mut message = error.to_string();
        if message.len() > 500 {
            message.truncate(500);
        }
        Box::new(json_error(status, code, message))
    })
}

fn supported_slots_from_inspection(inspection: &ModuleInspection) -> Vec<PluginSlotKind> {
    let mut slots = BTreeSet::new();
    for hook in inspection.hook_versions.keys() {
        match hook {
            HookKind::Filter => {
                slots.insert(PluginSlotKind::Router);
            }
            HookKind::Shape | HookKind::TransformResponse | HookKind::TransformSseEvent => {
                slots.insert(PluginSlotKind::Shape);
            }
        }
    }
    slots.into_iter().collect()
}

async fn materialize_cache(state: &AdminState, sha256_hex: &str, bytes: &[u8]) -> io::Result<()> {
    let data_dir = data_dir(state);
    let cache_dir = data_dir.join("plugins").join("wasm").join("cache");
    let tmp_dir = cache_dir.join(".tmp");
    tokio::fs::create_dir_all(&tmp_dir).await?;
    set_dir_mode(&cache_dir).await?;
    set_dir_mode(&tmp_dir).await?;
    let target = cache_dir.join(format!("{sha256_hex}.wasm"));
    if target.exists() {
        return Ok(());
    }
    let tmp = tmp_dir.join(format!("{}.wasm", Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await?;
    set_file_mode(&tmp).await?;
    tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
    file.sync_all().await?;
    drop(file);
    match tokio::fs::rename(&tmp, &target).await {
        Ok(()) => Ok(()),
        Err(error) if target.exists() => {
            let _ = tokio::fs::remove_file(&tmp).await;
            let _ = error;
            Ok(())
        }
        Err(error) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            Err(error)
        }
    }
}

fn cache_materialization_response(
    sha256_hex: &str,
) -> impl FnOnce(io::Error) -> Box<Response> + '_ {
    move |error| {
        tracing::error!(%error, sha256 = %sha256_hex, "wasm cache materialization failed");
        Box::new(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "cache_materialization_failed",
            "failed to write wasm cache file",
        ))
    }
}

async fn remove_cache_file(state: &AdminState, sha256_hex: &str) {
    let cache_path = wasm_cache_path(state, sha256_hex);
    match tokio::fs::remove_file(&cache_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(%error, path = %cache_path.display(), "failed to remove wasm cache file")
        }
    }
}

async fn set_dir_mode(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = tokio::fs::metadata(path).await?.permissions();
        permissions.set_mode(0o700);
        tokio::fs::set_permissions(path, permissions).await?;
    }
    Ok(())
}

async fn set_file_mode(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = tokio::fs::metadata(path).await?.permissions();
        permissions.set_mode(0o600);
        tokio::fs::set_permissions(path, permissions).await?;
    }
    Ok(())
}

fn storage_response(error: StorageError) -> Response {
    match error {
        StorageError::Conflict { message } => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "wasm_upload_conflict", "reason": message })),
        )
            .into_response(),
        StorageError::InvalidInput { reason, .. } => {
            json_error(StatusCode::BAD_REQUEST, "invalid_input", reason)
        }
        error => {
            tracing::error!(%error, "wasm upload storage operation failed");
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                "storage operation failed",
            )
        }
    }
}

fn json_error(status: StatusCode, error: &str, reason: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": error, "reason": reason.into() })),
    )
        .into_response()
}

fn audit_write_failed(error: StorageError, action: &str) -> Response {
    tracing::error!(%error, action, "admin audit write failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "audit_write_failed" })),
    )
        .into_response()
}

fn admin_id(identity: &AdminIdentity) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("{}/{}", identity.authority, identity.subject).as_bytes(),
    )
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
