use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use cc_lb_core::{AuditEntry, AuditPayload};
use cc_lb_runtime_wasmtime::{ModuleInspection, SlotKind, WasmtimeRuntimeError, inspect_wasm};
use cc_lb_storage_api::{
    MAX_WASM_BLOB_BYTES, PluginSlot, StorageError, WasmBlob, WasmRegistryEntryInput,
    default_wire_version,
};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use super::wasm_cache::{data_dir, wasm_cache_path};
use crate::AdminState;

const WASM_MAGIC: &[u8; 4] = b"\0asm";
const MIN_WASM_BYTES: usize = 8;
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
    revision: u64,
    idempotent: bool,
}

#[derive(Default)]
struct UploadParts {
    bytes: Option<Vec<u8>>,
    name: Option<String>,
    original_filename: Option<String>,
    /// Required: which slot kind the plugin targets — `filter`,
    /// `shape`, or `observe`. Maps to [`SlotKind`] for wasmtime
    /// load-time inspection and to [`PluginSlot`] for the registry.
    slot_kind: Option<String>,
}

pub fn router() -> Router<AdminState> {
    let limiter = UploadRateLimitState::default();
    let upload_layers =
        ServiceBuilder::new().layer(middleware::from_fn_with_state(limiter, upload_rate_limit));

    Router::new()
        .route(
            "/admin/v1/plugins/wasm",
            post(upload_wasm).route_layer(upload_layers),
        )
        .route("/admin/v1/plugins/wasm/gc", post(gc_wasm))
        .layer(DefaultBodyLimit::max(
            (MAX_WASM_BLOB_BYTES as usize) + 4 * 1024 * 1024,
        ))
}

async fn upload_rate_limit(
    State(limiter): State<UploadRateLimitState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let admin_id = admin_id_from_headers(request.headers());
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
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    match upload_wasm_inner(&state, &headers, multipart).await {
        Ok((status, response)) => {
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
            enqueue_upload_attempt_audit(&state, status);
            (*error).into_response()
        }
    }
}

async fn upload_wasm_inner(
    state: &AdminState,
    headers: &HeaderMap,
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
    let name = parts.name.ok_or_else(|| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: name",
        ))
    })?;
    let original_filename = parts.original_filename.ok_or_else(|| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: original_filename",
        ))
    })?;
    validate_original_filename(&original_filename).map_err(Box::new)?;
    let bytes = parts.bytes.ok_or_else(|| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: bytes",
        ))
    })?;
    validate_wasm_bytes(&bytes).map_err(Box::new)?;
    let slot_kind_str = parts.slot_kind.ok_or_else(|| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "missing_part",
            "missing multipart part: slot_kind (must be one of filter, shape, observe)",
        ))
    })?;
    let (slot_kind, plugin_slot) = parse_slot_kind(&slot_kind_str).map_err(Box::new)?;
    let inspection = inspect_with_wasmtime(&bytes, slot_kind).await?;
    let inspected_schema_hash = inspection.primary_schema_hash();
    if let Some(embedded_name) = embedded_plugin_name(inspection.plugin_metadata.as_deref())
        && embedded_name != name
    {
        return Err(Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "identity_mismatch",
            format!(
                "multipart name `{name}` does not match `cc_lb.plugin.v1` metadata name `{embedded_name}`"
            ),
        )));
    }

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
    let existing = storage
        .get_registry_entry_by_sha(sha256)
        .await
        .map_err(|error| Box::new(storage_response(error)))?;
    let (supported_slots, fresh_wire_version) = match &existing {
        Some(entry) if !entry.supported_slots.is_empty() => {
            (entry.supported_slots.clone(), entry.wire_version)
        }
        _ => (vec![plugin_slot], default_wire_version()),
    };
    let admin_id = admin_id_from_headers(headers);
    let uploaded_at_unix_secs = cc_lb_core::clock::unix_secs(state.clock.now());
    let blob = WasmBlob {
        sha256,
        size_bytes: bytes.len() as u64,
        bytes: bytes.clone(),
        parse_validated_at_unix_secs: uploaded_at_unix_secs,
    };
    let entry_input = WasmRegistryEntryInput {
        schema_hash: Some(inspected_schema_hash),
        name,
        original_filename: original_filename.clone(),
        label: None,
        uploaded_at_unix_secs,
        uploaded_by_admin_id: admin_id,
        wire_version: fresh_wire_version,
        supported_slots: supported_slots.clone(),
    };
    let (mut entry, existed) = storage
        .persist_wasm_upload(blob, entry_input)
        .await
        .map_err(storage_response)?;
    if existed && entry.supported_slots.is_empty() && !supported_slots.is_empty() {
        storage
            .update_supported_slots(entry.id, supported_slots.clone())
            .await
            .map_err(|error| Box::new(storage_response(error)))?;
        entry.supported_slots = supported_slots;
    }
    if existed && entry.wire_version != fresh_wire_version {
        storage
            .update_wire_version(entry.id, fresh_wire_version)
            .await
            .map_err(|error| Box::new(storage_response(error)))?;
        entry.wire_version = fresh_wire_version;
    }
    materialize_cache(state, &sha256_hex, &bytes)
        .await
        .map_err(|error| {
            tracing::error!(%error, sha256 = %sha256_hex, "wasm cache materialization failed");
            Box::new(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cache_materialization_failed",
                "failed to write wasm cache file",
            ))
        })?;
    enqueue_upload_audit(state, &sha256_hex, bytes.len() as u64, &original_filename);
    let idempotent = existed;
    let status = if idempotent {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((
        status,
        UploadResponse {
            sha256_hex,
            id: entry.id,
            size_bytes: bytes.len() as u64,
            original_filename: entry.original_filename,
            revision: entry.revision,
            idempotent,
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
            "name" => {
                parts.name = Some(field.text().await.map_err(|error| {
                    Box::new(json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    ))
                })?);
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
            "slot_kind" => {
                parts.slot_kind = Some(field.text().await.map_err(|error| {
                    Box::new(json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    ))
                })?);
            }
            _ => {}
        }
    }
    Ok(parts)
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

#[allow(clippy::result_large_err)]
fn parse_slot_kind(value: &str) -> Result<(SlotKind, PluginSlot), Response> {
    match value {
        "filter" => Ok((SlotKind::Filter, PluginSlot::Router)),
        "shape" => Ok((SlotKind::Shape, PluginSlot::Shape)),
        "observe" => Ok((SlotKind::Observe, PluginSlot::ObservabilityHook)),
        other => Err(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_slot_kind",
            format!("slot_kind must be one of filter|shape|observe, got `{other}`"),
        )),
    }
}

async fn inspect_with_wasmtime(
    bytes: &[u8],
    slot_kind: SlotKind,
) -> Result<ModuleInspection, Box<Response>> {
    let bytes = bytes.to_vec();
    let inspection = tokio::task::spawn_blocking(move || inspect_wasm(slot_kind, &bytes))
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

// Parse the JSON custom section `cc_lb.plugin.v1` emitted by the
// wasmtime PDK macro and return the `name` field if present. Returns
// `None` on missing metadata, non-UTF8 bytes, invalid JSON, or an
// absent/non-string `name` — the section is diagnostics-only per
// runtime contract, so silent failure keeps upload flow permissive
// for plugins that pre-date this cross-check.
fn embedded_plugin_name(metadata: Option<&[u8]>) -> Option<String> {
    let bytes = metadata?;
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value.get("name")?.as_str().map(str::to_owned)
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

fn enqueue_upload_audit(
    state: &AdminState,
    sha256: &str,
    size_bytes: u64,
    original_filename: &str,
) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let payload = AuditPayload::PluginRegistryUpload {
        sha256: sha256.to_owned(),
        size_bytes,
        original_filename: original_filename.to_owned(),
    };
    let mut entry: AuditEntry = payload.into();
    entry.ts = cc_lb_core::clock::unix_secs(state.clock.now());
    entry.request_id = format!("admin-plugin-registry-upload-{sha256}-{}", entry.ts);
    entry.principal_id = "admin".to_owned();
    entry.route = "/admin/v1/plugins/wasm".to_owned();
    entry.status = 201;
    entry.actor = Some("admin".to_owned());
    let _ = audit_sink.try_enqueue(entry);
}

fn enqueue_upload_attempt_audit(state: &AdminState, status: u16) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = cc_lb_core::clock::unix_secs(state.clock.now());
    let _ = audit_sink.try_enqueue(AuditEntry {
        ts,
        request_id: format!("admin-plugin-registry-upload-attempt-{ts}"),
        principal_id: "admin".to_owned(),
        route: "/admin/v1/plugins/wasm".to_owned(),
        upstream: "admin".to_owned(),
        status,
        input_tokens: Some(0),
        output_tokens: Some(0),
        duration_ms: 0,
        admin_action: Some("plugin_registry_upload_attempt".to_owned()),
        actor: Some("admin".to_owned()),
        ..AuditEntry::default()
    });
}

fn admin_id_from_headers(headers: &HeaderMap) -> Uuid {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("anonymous-admin");
    // The current admin auth surface is a bearer-token singleton rather than a
    // request extension carrying a DB principal, so the per-admin rate bucket is
    // keyed by a stable UUID derived from the authenticated bearer credential.
    let digest = Sha256::digest(token.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
