mod config_admin_common;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::router;
use cc_lb_config::Config;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use config_admin_common::{TOKEN, temp_storage, test_state};

#[tokio::test]
async fn router_wire_v1_v2_upload_is_rejected() {
    let harness = Harness::new();
    let response = harness
        .upload(
            "legacy-router",
            "legacy-router.wasm",
            &minimal_wasm_export("route"),
        )
        .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "unsupported_wire_version");
}

#[tokio::test]
async fn shape_plugin_without_handshake_export_is_rejected() {
    let harness = Harness::new();
    let response = harness
        .upload("shape", "shape.wasm", &minimal_wasm_export("shape"))
        .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "handshake_failed");
}

#[tokio::test]
async fn observability_plugin_without_handshake_export_is_rejected() {
    let harness = Harness::new();
    let response = harness
        .upload("observe", "observe.wasm", &minimal_wasm_export("observe"))
        .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "handshake_failed");
}

struct Harness {
    app: axum::Router,
    _dir: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let (dir, storage) = temp_storage();
        let mut config = Config::default();
        config.runtime.data_dir = Some(dir.path().join("data"));
        let app = router(test_state(config, Some(storage)));
        Self { app, _dir: dir }
    }

    async fn upload(&self, name: &str, original_filename: &str, bytes: &[u8]) -> TestResponse {
        let boundary = format!("boundary-{}", uuid::Uuid::new_v4());
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

fn minimal_wasm_export(name: &str) -> Vec<u8> {
    assert!(name.len() < 128);
    let mut wasm = Vec::from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
    wasm.extend_from_slice(&[0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f]);
    wasm.extend_from_slice(&[0x03, 0x02, 0x01, 0x00]);

    let mut export_section = Vec::new();
    export_section.push(0x01);
    export_section.push(name.len() as u8);
    export_section.extend_from_slice(name.as_bytes());
    export_section.push(0x00);
    export_section.push(0x00);
    wasm.push(0x07);
    wasm.push(export_section.len() as u8);
    wasm.extend_from_slice(&export_section);

    wasm.extend_from_slice(&[0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x00, 0x0b]);
    wasm
}
