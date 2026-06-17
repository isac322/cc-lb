mod config_admin_common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::router;
use cc_lb_config::Config;
use cc_lb_storage_api::PluginRegistryStore;
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use config_admin_common::{TOKEN, temp_storage, test_state};

#[tokio::test]
async fn upload_derives_wire_version_from_handshake_chosen_versions() {
    let harness = Harness::new().await;
    let wasm = handshake_wasm_with_filter_v3_shape_v2();

    let upload = harness
        .upload("versioned-plugin", "versioned-plugin.wasm", &wasm)
        .await;

    assert_eq!(upload.status, StatusCode::CREATED);
    let id: Uuid = upload.json["id"].as_str().unwrap().parse().unwrap();
    let entry = harness
        .storage
        .get_registry_entry_by_id(id)
        .await
        .unwrap()
        .expect("uploaded entry is stored");
    assert_eq!(entry.wire_version, 3);
}

#[tokio::test]
async fn idempotent_reupload_skips_handshake_and_preserves_stored_wire_version() {
    let harness = Harness::new().await;
    let wasm = handshake_wasm_with_filter_v3_shape_v2();

    let first = harness
        .upload("versioned-heal", "versioned-heal.wasm", &wasm)
        .await;
    assert_eq!(first.status, StatusCode::CREATED);
    let id: Uuid = first.json["id"].as_str().unwrap().parse().unwrap();

    // Simulate disk-level wire_version drift. Idempotent re-upload must take the
    // existing supported_slots/wire_version fast path and skip the (slow) handshake;
    // drift recovery is the startup re-handshake's job, not the upload handler's.
    harness.storage.update_wire_version(id, 1).await.unwrap();
    let drifted = harness
        .storage
        .get_registry_entry_by_id(id)
        .await
        .unwrap()
        .expect("drifted entry is stored");
    assert_eq!(drifted.wire_version, 1);

    let second = harness
        .upload("versioned-heal", "versioned-heal.wasm", &wasm)
        .await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(second.json["idempotent"], true);

    let after = harness
        .storage
        .get_registry_entry_by_id(id)
        .await
        .unwrap()
        .expect("entry still stored after re-upload");
    assert_eq!(
        after.wire_version, 1,
        "idempotent re-upload must not re-handshake; drift recovery is startup's job",
    );
}

struct Harness {
    app: axum::Router,
    _dir: tempfile::TempDir,
    storage: Arc<SqliteStorage>,
}

impl Harness {
    async fn new() -> Self {
        let (dir, storage) = temp_storage().await;
        let mut config = Config::default();
        config.runtime.data_dir = Some(dir.path().join("data"));
        let app = router(test_state(config, Some(storage.clone())));
        Self {
            app,
            _dir: dir,
            storage,
        }
    }

    async fn upload(&self, name: &str, original_filename: &str, bytes: &[u8]) -> TestResponse {
        let boundary = format!("boundary-{}", Uuid::new_v4());
        let body = multipart_body(&boundary, name, original_filename, bytes);
        let request = Request::builder()
            .method("POST")
            .uri("/admin/v1/plugins/wasm")
            .header("Authorization", format!("Bearer {TOKEN}"))
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();
        send(self.app.clone(), request).await
    }
}

struct TestResponse {
    status: StatusCode,
    json: Value,
}

async fn send(app: axum::Router, request: Request<Body>) -> TestResponse {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if !status.is_success() {
        eprintln!("status={status} body={}", String::from_utf8_lossy(&bytes));
    }
    TestResponse { status, json }
}

fn multipart_body(boundary: &str, name: &str, original_filename: &str, bytes: &[u8]) -> Vec<u8> {
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

fn handshake_wasm_with_filter_v3_shape_v2() -> Vec<u8> {
    let accept = json!({
        "handshake_schema_version": 1,
        "envelope_version": 1,
        "chosen_versions": {
            "filter": 3,
            "shape": 2,
        },
        "plugin_supported": {
            "filter": [1, 2, 3],
            "shape": [1, 2],
        },
        "implemented_functions": ["filter", "shape"],
        "required_capabilities": [],
    })
    .to_string();
    extism_string_handshake_wasm(accept.as_bytes())
}

fn extism_string_handshake_wasm(output: &[u8]) -> Vec<u8> {
    let mut module = Vec::from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
    push_section(&mut module, 1, type_section());
    push_section(&mut module, 2, import_section());
    push_section(&mut module, 3, function_section());
    push_section(&mut module, 7, export_section());
    push_section(&mut module, 10, code_section(output));
    module
}

fn type_section() -> Vec<u8> {
    let mut payload = Vec::new();
    push_vec_len(&mut payload, 5);
    push_function_type(&mut payload, &[0x7e], &[0x7e]);
    push_function_type(&mut payload, &[0x7e, 0x7f], &[]);
    push_function_type(&mut payload, &[0x7e, 0x7e], &[]);
    push_function_type(&mut payload, &[], &[0x7f]);
    push_function_type(&mut payload, &[], &[0x7e]);
    payload
}

fn import_section() -> Vec<u8> {
    let mut payload = Vec::new();
    push_vec_len(&mut payload, 3);
    push_function_import(&mut payload, "extism:host/env", "alloc", 0);
    push_function_import(&mut payload, "extism:host/env", "store_u8", 1);
    push_function_import(&mut payload, "extism:host/env", "output_set", 2);
    payload
}

fn function_section() -> Vec<u8> {
    let mut payload = Vec::new();
    push_vec_len(&mut payload, 4);
    push_u32_leb(&mut payload, 4);
    push_u32_leb(&mut payload, 3);
    push_u32_leb(&mut payload, 3);
    push_u32_leb(&mut payload, 3);
    payload
}

fn export_section() -> Vec<u8> {
    let mut payload = Vec::new();
    push_vec_len(&mut payload, 3);
    push_function_export(&mut payload, "cc_lb_handshake", 4);
    push_function_export(&mut payload, "filter", 5);
    push_function_export(&mut payload, "shape", 6);
    payload
}

fn code_section(output: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    push_vec_len(&mut payload, 4);
    push_function_body(&mut payload, handshake_output_helper_body(output));
    push_function_body(&mut payload, handshake_body(output.len()));
    push_function_body(&mut payload, constant_zero_body());
    push_function_body(&mut payload, constant_zero_body());
    payload
}

fn handshake_output_helper_body(output: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    push_vec_len(&mut body, 1);
    push_u32_leb(&mut body, 1);
    body.push(0x7e);
    body.push(0x42);
    push_i64_leb(&mut body, i64::try_from(output.len()).unwrap());
    body.push(0x10);
    push_u32_leb(&mut body, 0);
    body.push(0x21);
    push_u32_leb(&mut body, 0);
    for (byte_index, byte) in output.iter().enumerate() {
        body.push(0x20);
        push_u32_leb(&mut body, 0);
        body.push(0x42);
        push_i64_leb(&mut body, i64::try_from(byte_index).unwrap());
        body.push(0x7c);
        body.push(0x41);
        push_i32_leb(&mut body, i32::from(*byte));
        body.push(0x10);
        push_u32_leb(&mut body, 1);
    }
    body.push(0x20);
    push_u32_leb(&mut body, 0);
    body.push(0x0b);
    body
}

fn handshake_body(output_len: usize) -> Vec<u8> {
    let mut body = Vec::new();
    push_vec_len(&mut body, 0);
    body.push(0x10);
    push_u32_leb(&mut body, 3);
    body.push(0x42);
    push_i64_leb(&mut body, i64::try_from(output_len).unwrap());
    body.push(0x10);
    push_u32_leb(&mut body, 2);
    body.push(0x41);
    push_i32_leb(&mut body, 0);
    body.push(0x0b);
    body
}

fn constant_zero_body() -> Vec<u8> {
    let mut body = Vec::new();
    push_vec_len(&mut body, 0);
    body.push(0x41);
    push_i32_leb(&mut body, 0);
    body.push(0x0b);
    body
}

fn push_function_type(payload: &mut Vec<u8>, params: &[u8], results: &[u8]) {
    payload.push(0x60);
    push_vec_len(payload, params.len());
    payload.extend_from_slice(params);
    push_vec_len(payload, results.len());
    payload.extend_from_slice(results);
}

fn push_function_import(payload: &mut Vec<u8>, module: &str, name: &str, type_index: u32) {
    push_name(payload, module);
    push_name(payload, name);
    payload.push(0x00);
    push_u32_leb(payload, type_index);
}

fn push_function_export(payload: &mut Vec<u8>, name: &str, function_index: u32) {
    push_name(payload, name);
    payload.push(0x00);
    push_u32_leb(payload, function_index);
}

fn push_function_body(payload: &mut Vec<u8>, body: Vec<u8>) {
    push_vec_len(payload, body.len());
    payload.extend(body);
}

fn push_section(module: &mut Vec<u8>, section_id: u8, payload: Vec<u8>) {
    module.push(section_id);
    push_vec_len(module, payload.len());
    module.extend(payload);
}

fn push_name(buffer: &mut Vec<u8>, name: &str) {
    push_vec_len(buffer, name.len());
    buffer.extend_from_slice(name.as_bytes());
}

fn push_vec_len(buffer: &mut Vec<u8>, len: usize) {
    push_u32_leb(buffer, u32::try_from(len).unwrap());
}

fn push_u32_leb(buffer: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buffer.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn push_i32_leb(buffer: &mut Vec<u8>, value: i32) {
    push_i64_leb(buffer, i64::from(value));
}

fn push_i64_leb(buffer: &mut Vec<u8>, mut value: i64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        let done = (value == 0 && (byte & 0x40) == 0) || (value == -1 && (byte & 0x40) != 0);
        buffer.push(if done { byte } else { byte | 0x80 });
        if done {
            break;
        }
    }
}
