#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Multipart, Path, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use cc_lb_runtime_extism::identity::read_identity;
use cc_lb_runtime_extism::registry::{PluginRegistry, RegistryError};
use cc_lb_storage_api::MAX_WASM_BLOB_BYTES;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

#[derive(Clone)]
struct AdminPluginsState {
    registry: PluginRegistry,
    admin_token: Option<String>,
    registration_lock: Arc<Mutex<()>>,
}

#[derive(Default)]
struct RegisterParts {
    bytes: Option<Vec<u8>>,
    name: Option<String>,
    original_filename: Option<String>,
}

pub(crate) fn router(admin_token: Option<String>, registry: PluginRegistry) -> Router {
    let state = AdminPluginsState {
        registry,
        admin_token,
        registration_lock: Arc::new(Mutex::new(())),
    };

    Router::new()
        .route("/admin/plugins", post(register_plugin).get(list_plugins))
        .route(
            "/admin/plugins/{sha256}",
            get(get_plugin).delete(delete_plugin),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_auth,
        ))
        .layer(DefaultBodyLimit::max(
            (MAX_WASM_BLOB_BYTES as usize) + 4 * 1024 * 1024,
        ))
        .with_state(state)
}

async fn require_admin_auth(
    State(state): State<AdminPluginsState>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected_token) = &state.admin_token else {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    };
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok());
    let Some(auth_header) = auth_header else {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    };
    let Some(token) = auth_header.strip_prefix("Bearer ") else {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    };
    if token != expected_token {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

async fn register_plugin(State(state): State<AdminPluginsState>, multipart: Multipart) -> Response {
    match register_plugin_inner(&state, multipart).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn register_plugin_inner(
    state: &AdminPluginsState,
    multipart: Multipart,
) -> Result<Response, Response> {
    let parts = read_register_parts(multipart).await?;
    let wasm_bytes = parts.bytes.ok_or_else(|| missing_part_response("bytes"))?;
    let plugin_name = parts.name.ok_or_else(|| missing_part_response("name"))?;
    let _original_filename = parts
        .original_filename
        .ok_or_else(|| missing_part_response("original_filename"))?;
    let sha256: [u8; 32] = Sha256::digest(&wasm_bytes).into();
    let _guard = state.registration_lock.lock().await;
    if let Some(existing) = state
        .registry
        .get_by_sha256(&sha256)
        .await
        .map_err(registry_error_response)?
    {
        if existing.plugin_name != plugin_name {
            return Err(name_mismatch_response(
                &sha256,
                &existing.plugin_name,
                &plugin_name,
            ));
        }
        state.registry.load_record_into_cache(&existing);
        return Ok((StatusCode::OK, Json(existing)).into_response());
    }

    let identity = read_identity(&wasm_bytes).map_err(|error| {
        json_error(
            StatusCode::BAD_REQUEST,
            "invalid_plugin_identity",
            error.to_string(),
        )
    })?;
    if identity.plugin_name != plugin_name {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "plugin_name_mismatch",
            format!(
                "multipart name '{}' does not match wasm identity '{}'",
                plugin_name, identity.plugin_name
            ),
        ));
    }

    let record = state
        .registry
        .register_plugin(&wasm_bytes)
        .await
        .map_err(registry_error_response)?;
    Ok((StatusCode::CREATED, Json(record)).into_response())
}

async fn list_plugins(State(state): State<AdminPluginsState>) -> Response {
    match state.registry.list_active().await {
        Ok(records) => Json(records).into_response(),
        Err(error) => registry_error_response(error),
    }
}

async fn get_plugin(
    State(state): State<AdminPluginsState>,
    Path(sha256_hex): Path<String>,
) -> Response {
    let sha256 = match parse_sha256_hex(&sha256_hex) {
        Ok(sha256) => sha256,
        Err(response) => return response,
    };
    match state.registry.get_by_sha256(&sha256).await {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => json_error(StatusCode::NOT_FOUND, "not_found", "plugin not found"),
        Err(error) => registry_error_response(error),
    }
}

async fn delete_plugin(
    State(state): State<AdminPluginsState>,
    Path(sha256_hex): Path<String>,
) -> Response {
    let sha256 = match parse_sha256_hex(&sha256_hex) {
        Ok(sha256) => sha256,
        Err(response) => return response,
    };
    match state.registry.delete_plugin(&sha256).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => registry_error_response(error),
    }
}

async fn read_register_parts(mut multipart: Multipart) -> Result<RegisterParts, Response> {
    let mut parts = RegisterParts::default();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(multipart_error_response)?
    {
        let Some(name) = field.name().map(ToOwned::to_owned) else {
            continue;
        };
        match name.as_str() {
            "bytes" => {
                let data = field.bytes().await.map_err(multipart_error_response)?;
                if data.len() as u64 > MAX_WASM_BLOB_BYTES {
                    return Err(json_error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "wasm_too_large",
                        "bytes part exceeds 32 MiB",
                    ));
                }
                parts.bytes = Some(data.to_vec());
            }
            "name" => {
                parts.name = Some(field.text().await.map_err(|error| {
                    json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    )
                })?);
            }
            "original_filename" => {
                parts.original_filename = Some(field.text().await.map_err(|error| {
                    json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_multipart",
                        error.to_string(),
                    )
                })?);
            }
            _ => {}
        }
    }
    Ok(parts)
}

fn parse_sha256_hex(value: &str) -> Result<[u8; 32], Response> {
    if value.len() != 64 {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_sha256",
            "sha256 must be 64 hex characters",
        ));
    }
    let mut output = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

fn hex_nibble(byte: u8) -> Result<u8, Response> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_sha256",
            "sha256 must contain only hex characters",
        )),
    }
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

fn missing_part_response(part: &str) -> Response {
    json_error(
        StatusCode::BAD_REQUEST,
        "missing_part",
        format!("missing multipart part: {part}"),
    )
}

fn registry_error_response(error: RegistryError) -> Response {
    let status = match &error {
        RegistryError::PluginNameMismatch { .. } => StatusCode::CONFLICT,
        RegistryError::Identity(_)
        | RegistryError::Handshake(_)
        | RegistryError::SelfCheck(_)
        | RegistryError::Metadata(_)
        | RegistryError::BlobSha256Mismatch { .. } => StatusCode::BAD_REQUEST,
        RegistryError::BlobMissing { .. } => StatusCode::NOT_FOUND,
        RegistryError::HostOfferValidation(_)
        | RegistryError::HostOfferHash(_)
        | RegistryError::RegistryRepo { .. }
        | RegistryError::BlobRepo { .. }
        | RegistryError::Clock { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let code = if status == StatusCode::CONFLICT {
        "name_mismatch"
    } else if status == StatusCode::BAD_REQUEST {
        "invalid_plugin"
    } else if status == StatusCode::NOT_FOUND {
        "not_found"
    } else {
        "registry_error"
    };
    json_error(status, code, error.to_string())
}

fn name_mismatch_response(sha256: &[u8; 32], existing: &str, actual: &str) -> Response {
    json_error(
        StatusCode::CONFLICT,
        "name_mismatch",
        format!(
            "sha256 {} is already registered as plugin '{}', not '{}'",
            hex_sha256(sha256),
            existing,
            actual
        ),
    )
}

fn json_error(status: StatusCode, error: &str, reason: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": error, "reason": reason.into() })),
    )
        .into_response()
}

fn hex_sha256(sha256: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, header};
    use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
    use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
    use cc_lb_runtime_extism::handshake::build_offer;
    use cc_lb_storage_api::{
        PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, RepoError,
    };
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    const TOKEN: &str = "test-token";

    #[tokio::test]
    async fn register_happy() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone());
        let app = router(Some(TOKEN.to_owned()), registry.clone());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        let sha256 = sha256(&wasm);
        let sha256_hex = hex_sha256(&sha256);

        let (status, body) = post_register(app.clone(), "test-plugin", &wasm).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["plugin_name"], "test-plugin");
        assert!(registry.get_metadata(&sha256).is_some());

        let (status, body) = request_json(app.clone(), "GET", "/admin/plugins").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body.as_array().expect("list response").len(), 1);

        let (status, body) =
            request_json(app.clone(), "GET", &format!("/admin/plugins/{sha256_hex}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["plugin_name"], "test-plugin");

        let (status, _) = request_json(
            app.clone(),
            "DELETE",
            &format!("/admin/plugins/{sha256_hex}"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(registry.get_metadata(&sha256).is_none());
        assert!(
            registry
                .get_by_sha256(&sha256)
                .await
                .expect("get after delete")
                .is_none()
        );
        assert!(repos.blobs.get(&sha256).await.is_none());
    }

    #[tokio::test]
    async fn register_idempotent() {
        let repos = Repos::default();
        let app = router(
            Some(TOKEN.to_owned()),
            registry(repos.registry.clone(), repos.blobs.clone()),
        );
        let wasm = plugin_wasm("test-plugin", "1.0.0");

        let (first_status, first_body) = post_register(app.clone(), "test-plugin", &wasm).await;
        let (second_status, second_body) = post_register(app, "test-plugin", &wasm).await;

        assert_eq!(first_status, StatusCode::CREATED, "{first_body}");
        assert_eq!(second_status, StatusCode::OK, "{second_body}");
        assert_eq!(repos.registry.upserts.load(Ordering::SeqCst), 1);
        assert_eq!(repos.blobs.puts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn name_mismatch_rejected() {
        let repos = Repos::default();
        let app = router(
            Some(TOKEN.to_owned()),
            registry(repos.registry.clone(), repos.blobs.clone()),
        );
        let wasm = plugin_wasm("test-plugin", "1.0.0");

        let (first_status, first_body) = post_register(app.clone(), "test-plugin", &wasm).await;
        let (second_status, second_body) = post_register(app, "other-plugin", &wasm).await;

        assert_eq!(first_status, StatusCode::CREATED, "{first_body}");
        assert_eq!(second_status, StatusCode::CONFLICT, "{second_body}");
        assert_eq!(second_body["error"], "name_mismatch");
    }

    #[tokio::test]
    async fn concurrent_register_same_sha256() {
        let repos = Repos::default();
        let app = router(
            Some(TOKEN.to_owned()),
            registry(repos.registry.clone(), repos.blobs.clone()),
        );
        let wasm = plugin_wasm("test-plugin", "1.0.0");

        let first = post_register(app.clone(), "test-plugin", &wasm);
        let second = post_register(app, "test-plugin", &wasm);
        let ((first_status, first_body), (second_status, second_body)) =
            tokio::join!(first, second);
        let mut statuses = [first_status.as_u16(), second_status.as_u16()];
        statuses.sort();

        assert_eq!(
            statuses,
            [StatusCode::OK.as_u16(), StatusCode::CREATED.as_u16()]
        );
        assert_eq!(repos.registry.upserts.load(Ordering::SeqCst), 1);
        assert_eq!(repos.blobs.puts.load(Ordering::SeqCst), 1);
        assert_eq!(first_body["plugin_name"], "test-plugin");
        assert_eq!(second_body["plugin_name"], "test-plugin");
    }

    async fn post_register(app: Router, name: &str, wasm: &[u8]) -> (StatusCode, Value) {
        let boundary = "plugin-boundary";
        let body = multipart_body(boundary, name, "plugin.wasm", wasm);
        response_json(
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/plugins")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds"),
        )
        .await
    }

    async fn request_json(app: Router, method: &str, uri: &str) -> (StatusCode, Value) {
        response_json(
            app.oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds"),
        )
        .await
    }

    async fn response_json(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, value)
    }

    fn multipart_body(
        boundary: &str,
        name: &str,
        original_filename: &str,
        bytes: &[u8],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        push_text_part(&mut body, boundary, "name", name.as_bytes());
        push_text_part(
            &mut body,
            boundary,
            "original_filename",
            original_filename.as_bytes(),
        );
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"bytes\"; filename=\"plugin.wasm\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/wasm\r\n\r\n");
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    fn push_text_part(body: &mut Vec<u8>, boundary: &str, name: &str, value: &[u8]) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value);
        body.extend_from_slice(b"\r\n");
    }

    fn registry(
        registry_repo: Arc<MemoryRegistryRepo>,
        blob_repo: Arc<MemoryBlobRepo>,
    ) -> PluginRegistry {
        PluginRegistry::new(registry_repo, blob_repo, build_offer(&BTreeSet::new()))
            .expect("registry builds")
    }

    #[derive(Default)]
    struct Repos {
        registry: Arc<MemoryRegistryRepo>,
        blobs: Arc<MemoryBlobRepo>,
    }

    #[derive(Default)]
    struct MemoryRegistryRepo {
        records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
        upserts: AtomicUsize,
    }

    #[async_trait]
    impl PluginRegistryRepo for MemoryRegistryRepo {
        async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
            self.upserts.fetch_add(1, Ordering::SeqCst);
            self.records
                .lock()
                .await
                .insert(record.sha256, record.clone());
            Ok(())
        }

        async fn get_by_sha256(
            &self,
            sha256: &[u8; 32],
        ) -> Result<Option<PluginRegistryRecord>, RepoError> {
            Ok(self.records.lock().await.get(sha256).cloned())
        }

        async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
            Ok(self
                .records
                .lock()
                .await
                .values()
                .filter(|record| record.status == PluginRegistryStatus::Active)
                .cloned()
                .collect())
        }

        async fn set_status(
            &self,
            sha256: &[u8; 32],
            status: PluginRegistryStatus,
        ) -> Result<(), RepoError> {
            if let Some(record) = self.records.lock().await.get_mut(sha256) {
                record.status = status;
            }
            Ok(())
        }

        async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
            self.records.lock().await.remove(sha256);
            Ok(())
        }

        async fn count(&self) -> Result<usize, RepoError> {
            Ok(self.records.lock().await.len())
        }

        async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
            Ok(None)
        }

        async fn set_shutdown_marker(&self, _unix_secs: i64) -> Result<(), RepoError> {
            Ok(())
        }

        async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryBlobRepo {
        blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
        puts: AtomicUsize,
    }

    impl MemoryBlobRepo {
        async fn get(&self, sha256: &[u8; 32]) -> Option<Vec<u8>> {
            self.blobs.lock().await.get(sha256).cloned()
        }
    }

    #[async_trait]
    impl PluginBlobRepo for MemoryBlobRepo {
        async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.blobs.lock().await.insert(*sha256, bytes.to_vec());
            Ok(())
        }

        async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
            Ok(self.blobs.lock().await.get(sha256).cloned())
        }

        async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
            self.blobs.lock().await.remove(sha256);
            Ok(())
        }

        async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
            Ok(self.blobs.lock().await.keys().copied().collect())
        }
    }

    fn plugin_wasm(plugin_name: &str, plugin_version: &str) -> Vec<u8> {
        let accept = HandshakeAccept {
            handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
            envelope_version: 1,
            chosen_versions: BTreeMap::from([("route".to_owned(), 1)]),
            plugin_supported: BTreeMap::from([("route".to_owned(), vec![1])]),
            implemented_functions: BTreeSet::from(["route".to_owned()]),
            required_capabilities: BTreeSet::new(),
        };
        let handshake_output = serde_json::to_string(&accept).expect("accept serializes");
        let self_check_output = json!({
            "status": "success",
            "failures": [],
            "completed_at": 1,
        })
        .to_string();
        let wat = format!(
            r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {handshake_helper}
  {self_check_helper}
  (func (export "cc_lb_handshake") (result i32)
    (call $output_set (call $handshake_out) (i64.const {handshake_len}))
    (i32.const 0))
  (func (export "cc_lb_self_check") (result i32)
    (call $output_set (call $self_check_out) (i64.const {self_check_len}))
    (i32.const 0))
  (func (export "route") (result i32)
    (i32.const 0))
)
"#,
            handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
            self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
            handshake_len = handshake_output.len(),
            self_check_len = self_check_output.len(),
        );
        let mut wasm = wat::parse_str(&wat).expect("wat parses");
        append_identity_section(&mut wasm, plugin_name, plugin_version);
        wasm
    }

    fn bytes_helper(name: &str, bytes: &[u8]) -> String {
        let mut stores = String::new();
        for (index, byte) in bytes.iter().enumerate() {
            stores.push_str(&format!(
                "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
            ));
        }
        format!(
            r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
            len = bytes.len()
        )
    }

    fn append_identity_section(wasm: &mut Vec<u8>, plugin_name: &str, plugin_version: &str) {
        let payload = json!({
            "magic": CC_LB_PLUGIN_MAGIC,
            "abi_envelope": 1,
            "plugin_name": plugin_name,
            "plugin_version": plugin_version,
        })
        .to_string();
        wasm.push(0);
        let mut section = Vec::new();
        encode_u32(CC_LB_PLUGIN_SECTION_NAME.len() as u32, &mut section);
        section.extend_from_slice(CC_LB_PLUGIN_SECTION_NAME.as_bytes());
        section.extend_from_slice(payload.as_bytes());
        encode_u32(section.len() as u32, wasm);
        wasm.extend_from_slice(&section);
    }

    fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }
}
