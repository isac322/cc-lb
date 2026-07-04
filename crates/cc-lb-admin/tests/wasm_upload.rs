//! Stage 4 — admin plugin upload UI on wasmtime ABI.
//!
//! Verifies `POST /admin/v1/plugins/wasm` against the new
//! slot-kind-aware validator: happy path + the three rejection
//! cases the wasmtime `inspect_wasm` gate enforces (missing
//! required exports, schema_hash mismatch, host import).

#[path = "admin_test_common.rs"]
mod admin_test_common;

use admin_test_common::spawn_admin_server;
use axum::http::StatusCode;
use cc_lb_plugin_wire::schema::{HookKind, WireSchema, WireVersion};
use cc_lb_plugin_wire::{FilterRequest, FilterResponse};
use http_body_util::BodyExt;
use tower::ServiceExt;

const BOUNDARY: &str = "ccLbWasmUploadBoundary";

fn multipart_body(parts: &[(&str, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in parts {
        body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn schema_section_bytes() -> Vec<u8> {
    <FilterRequest as WireSchema>::FINGERPRINT.to_vec()
}

fn schema_section_name() -> String {
    format!(
        "{}.{}",
        HookKind::Filter.section_prefix(),
        WireVersion::V1.as_str()
    )
}

fn append_custom_section(module: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    encode_leb128(&mut payload, name.len() as u64);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    module.push(0);
    encode_leb128(module, payload.len() as u64);
    module.extend_from_slice(&payload);
}

fn encode_leb128(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn wat_with_sections(wat: &str, sections: &[(&str, &[u8])]) -> Vec<u8> {
    let mut module = wat::parse_str(wat).expect("valid wat");
    for (name, data) in sections {
        append_custom_section(&mut module, name, data);
    }
    module
}

fn minimal_filter_wat() -> String {
    let response = rkyv::to_bytes::<rkyv::rancor::Error>(&FilterResponse {
        results: Box::new([]),
    })
    .expect("encode response");
    let data = wat_data_bytes(&response);
    let len = response.len();
    let packed = ((4096u64) << 32) | (len as u64);
    format!(
        r#"
    (module
        (memory (export "memory") 1)
        (data (i32.const 4096) "{data}")
        (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 1024)
        (func (export "cc_lb_free") (param i32 i32 i32))
        (func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const {packed})
    )
    "#
    )
}

fn wat_data_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:02x}")).collect()
}

fn metadata(name: &str) -> String {
    format!(
        r#"{{"name":"{name}","version":"0.0.1","description":"test plugin","usage":"test usage","hooks":{{"filter":{{"wire_version":1,"description":"filter hook","usage":"called by router"}}}}}}"#
    )
}

fn filter_wasm_valid() -> Vec<u8> {
    wat_with_sections(
        &minimal_filter_wat(),
        &[
            (&schema_section_name(), &schema_section_bytes()),
            ("cc_lb.plugin.v1", metadata("cache-aware-test").as_bytes()),
        ],
    )
}

fn filter_wasm_wrong_schema() -> Vec<u8> {
    wat_with_sections(
        &minimal_filter_wat(),
        &[
            (&schema_section_name(), &[0u8; 32]),
            ("cc_lb.plugin.v1", metadata("wrong-hash").as_bytes()),
        ],
    )
}

fn filter_wasm_missing_export() -> Vec<u8> {
    wat_with_sections(
        r#"
        (module
            (memory (export "memory") 1)
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
            (func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const 0)
        )
        "#,
        &[
            (&schema_section_name(), &schema_section_bytes()),
            ("cc_lb.plugin.v1", metadata("missing-free").as_bytes()),
        ],
    )
}

fn filter_wasm_with_import() -> Vec<u8> {
    wat_with_sections(
        r#"
        (module
            (import "env" "host_log" (func (param i32 i32)))
            (memory (export "memory") 1)
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const 0)
        )
        "#,
        &[
            (&schema_section_name(), &schema_section_bytes()),
            ("cc_lb.plugin.v1", metadata("with-import").as_bytes()),
        ],
    )
}

#[tokio::test]
async fn happy_path_accepts_valid_filter_plugin() {
    use cc_lb_storage_api::PluginRegistryStore;
    use sha2::{Digest, Sha256};

    let server = spawn_admin_server().await;
    let wasm = filter_wasm_valid();
    let body = multipart_body(&[
        ("name", b"cache-aware-test"),
        ("original_filename", b"cache-aware-test.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "expected 201 for valid filter upload; got {status} body={value}"
    );
    assert!(
        value["sha256_hex"].is_string(),
        "response missing sha256_hex"
    );
    assert_eq!(value["idempotent"], serde_json::json!(false));

    // Schema hash round-trip: inspect_wasm derives BLAKE3 of the
    // wire-schema tag, the upload persists it, and the storage row
    // round-trips with the expected bytes.
    let sha256: [u8; 32] = Sha256::digest(&wasm).into();
    let entry = server
        .storage
        .get_registry_entry_by_sha(sha256)
        .await
        .expect("storage lookup OK")
        .expect("entry persisted");
    assert_eq!(
        entry.schema_hash,
        Some(<FilterRequest as WireSchema>::FINGERPRINT),
        "schema_hash must round-trip via storage",
    );
}

#[tokio::test]
async fn rejects_missing_required_export() {
    let server = spawn_admin_server().await;
    let wasm = filter_wasm_missing_export();
    let body = multipart_body(&[
        ("name", b"missing-free"),
        ("original_filename", b"missing-free.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let reason = value["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("cc_lb_free") || value["error"] == "invalid_wasm",
        "unexpected rejection payload: {value}"
    );
}

#[tokio::test]
async fn rejects_schema_hash_mismatch() {
    let server = spawn_admin_server().await;
    let wasm = filter_wasm_wrong_schema();
    let body = multipart_body(&[
        ("name", b"wrong-hash"),
        ("original_filename", b"wrong-hash.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let reason = value["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("hash mismatch"),
        "unexpected rejection payload: {value}"
    );
}

#[tokio::test]
async fn rejects_host_import_violation() {
    let server = spawn_admin_server().await;
    let wasm = filter_wasm_with_import();
    let body = multipart_body(&[
        ("name", b"with-import"),
        ("original_filename", b"with-import.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let reason = value["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("import"),
        "unexpected rejection payload: {value}"
    );
}

fn filter_wasm_with_embedded_name(embedded_name: &str) -> Vec<u8> {
    wat_with_sections(
        &minimal_filter_wat(),
        &[
            (&schema_section_name(), &schema_section_bytes()),
            ("cc_lb.plugin.v1", metadata(embedded_name).as_bytes()),
        ],
    )
}

#[tokio::test]
async fn rejects_identity_mismatch_between_multipart_and_embedded_name() {
    let server = spawn_admin_server().await;
    let wasm = filter_wasm_with_embedded_name("embedded-name");
    let body = multipart_body(&[
        ("name", b"different-multipart-name"),
        ("original_filename", b"identity-mismatch.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "identity_mismatch");
    let reason = value["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("different-multipart-name") && reason.contains("embedded-name"),
        "reason must name both sides: {value}"
    );
}

#[tokio::test]
async fn accepts_when_embedded_name_matches_multipart() {
    let server = spawn_admin_server().await;
    let wasm = filter_wasm_with_embedded_name("aligned-name");
    let body = multipart_body(&[
        ("name", b"aligned-name"),
        ("original_filename", b"aligned.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::CREATED, "body={value}");
}

#[tokio::test]
async fn rejects_when_metadata_section_absent() {
    let server = spawn_admin_server().await;
    let wasm = wat_with_sections(
        &minimal_filter_wat(),
        &[(&schema_section_name(), &schema_section_bytes())],
    );
    let body = multipart_body(&[
        ("name", b"no-metadata-section"),
        ("original_filename", b"no-metadata.wasm"),
        ("slot_kind", b"filter"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body={value}");
}

#[tokio::test]
async fn rejects_missing_slot_kind_part() {
    let server = spawn_admin_server().await;
    let wasm = filter_wasm_valid();
    let body = multipart_body(&[
        ("name", b"no-slot"),
        ("original_filename", b"no-slot.wasm"),
        ("bytes", &wasm),
    ]);
    let (status, value) = upload(&server, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "missing_part");
}

// === server hookup that bypasses the private AdminClient internals ===

async fn upload(
    server: &admin_test_common::SpawnedAdminServer,
    body: Vec<u8>,
) -> (StatusCode, serde_json::Value) {
    use axum::body::Body;
    use axum::http::{Method, Request, header};

    let state = build_admin_state(server).await;
    let router = cc_lb_admin::router(state);
    let request = Request::builder()
        .method(Method::POST)
        .uri("/admin/v1/plugins/wasm")
        .header(header::AUTHORIZATION, "Bearer test-token")
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .body(Body::from(body))
        .expect("request builds");
    let response = router
        .oneshot(request)
        .await
        .expect("admin request succeeds");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is json")
    };
    (status, value)
}

async fn build_admin_state(
    server: &admin_test_common::SpawnedAdminServer,
) -> cc_lb_admin::AdminState {
    use std::sync::Arc;
    cc_lb_admin::AdminState {
        storage: Some(server.storage.clone()),
        key_store: Some(admin_test_common::key_store(server.storage.clone())),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        subscription_metadata_hook: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: Some(server._dir.path().to_path_buf()),
        warmup_dialect_dispatcher: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&cc_lb_config::Config::default()),
        config: Arc::new(cc_lb_config::Config::default()),
        scheduler: None,
        admin_token: Some("test-token".to_owned()),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_engine::SystemClock),
    }
}
