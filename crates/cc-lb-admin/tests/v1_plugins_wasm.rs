mod config_admin_common;

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::router;
use cc_lb_config::Config;
use http_body_util::BodyExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use config_admin_common::{TOKEN, temp_storage, test_state};

static FIXTURE_WASM: OnceLock<PathBuf> = OnceLock::new();

#[tokio::test]
async fn happy_upload_returns_201_with_sha_and_cache_file_exists() {
    let harness = Harness::new();
    let wasm = fixture_wasm();
    let response = harness.upload("echo", "echo.wasm", wasm).await;
    assert_eq!(response.status, StatusCode::CREATED);
    assert_eq!(response.json["idempotent"], false);
    assert_eq!(response.headers.get("x-idempotent").unwrap(), "false");
    assert_eq!(response.json["size_bytes"], wasm.len() as u64);
    let sha = response.json["sha256_hex"].as_str().unwrap();
    assert_eq!(sha, hex_sha(wasm));
    assert!(response.headers.get(header::LOCATION).is_some());
    assert!(
        harness
            .data_dir
            .join("plugins/wasm/cache")
            .join(format!("{sha}.wasm"))
            .exists()
    );
}

#[tokio::test]
async fn upload_persists_supported_slots_for_filter_exporting_plugin() {
    let harness = Harness::new();
    let wasm = fixture_wasm();
    let upload = harness.upload("echo-slots", "echo-slots.wasm", wasm).await;
    assert_eq!(upload.status, StatusCode::CREATED);
    let id = upload.json["id"].as_str().unwrap().to_owned();

    let registry = harness.list_registry().await;
    let entry = registry.json["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"].as_str() == Some(&id))
        .expect("uploaded entry visible in registry");
    let slots: Vec<String> = entry["supported_slots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(slots, vec!["router".to_owned()]);
}

#[tokio::test]
async fn reupload_heals_empty_supported_slots_on_existing_entry() {
    use cc_lb_storage_api::PluginRegistryStore;

    let harness = Harness::new();
    let wasm = fixture_wasm();
    let first = harness.upload("echo-heal", "echo-heal.wasm", wasm).await;
    assert_eq!(first.status, StatusCode::CREATED);
    let id: uuid::Uuid = first.json["id"].as_str().unwrap().parse().unwrap();

    harness
        .storage
        .update_supported_slots(id, Vec::new())
        .await
        .unwrap();
    let drift = harness.list_registry().await;
    let drift_entry = drift.json["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"].as_str() == Some(&id.to_string()))
        .expect("entry visible after slot drift");
    assert!(
        drift_entry["supported_slots"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let second = harness.upload("echo-heal", "echo-heal.wasm", wasm).await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(second.json["idempotent"], true);

    let healed = harness.list_registry().await;
    let healed_entry = healed.json["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"].as_str() == Some(&id.to_string()))
        .expect("entry visible after heal");
    let slots: Vec<String> = healed_entry["supported_slots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(slots, vec!["router".to_owned()]);
}

#[tokio::test]
async fn idempotent_duplicate_upload_returns_200_same_sha() {
    let harness = Harness::new();
    let wasm = fixture_wasm();
    let first = harness.upload("echo", "echo.wasm", wasm).await;
    let second = harness.upload("echo", "echo.wasm", wasm).await;
    assert_eq!(first.status, StatusCode::CREATED);
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(first.headers.get("x-idempotent").unwrap(), "false");
    assert_eq!(second.headers.get("x-idempotent").unwrap(), "true");
    assert_eq!(first.json["sha256_hex"], second.json["sha256_hex"]);
    assert_eq!(first.json["id"], second.json["id"]);
    assert_eq!(second.json["idempotent"], true);
}

#[allow(non_snake_case)]
#[tokio::test]
async fn oversize_33MiB_returns_413() {
    let harness = Harness::new();
    let bytes = vec![0_u8; 33 * 1024 * 1024];
    let response = harness.upload("big", "big.wasm", &bytes).await;
    assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn bad_magic_bytes_returns_400_with_invalid_wasm_magic() {
    let harness = Harness::new();
    let response = harness.upload("bad", "bad.wasm", b"NOT_WASM").await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "invalid_wasm_magic");
}

#[tokio::test]
async fn extism_parse_failure_returns_400_with_truncated_message() {
    let harness = Harness::new();
    let response = harness
        .upload("parsefail", "parsefail.wasm", b"\0asm\x01\0\0\0garbage")
        .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "invalid_wasm");
    assert!(response.json["reason"].as_str().unwrap().len() <= 500);
}

#[tokio::test]
async fn ratelimit_11th_upload_in_60s_returns_429_with_retry_after() {
    let harness = Harness::new();
    let wasm = fixture_wasm();
    let mut last = None;
    for index in 0..11 {
        last = Some(
            harness
                .upload(
                    &format!("rate-{index}"),
                    &format!("rate-{index}.wasm"),
                    wasm,
                )
                .await,
        );
    }
    let response = last.unwrap();
    assert_eq!(response.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers.get(header::RETRY_AFTER).unwrap(), "60");
}

#[tokio::test]
async fn gc_keeps_referenced_uploads_and_cache_files() {
    let harness = Harness::new();
    let wasm = fixture_wasm();
    let upload = harness.upload("echo", "echo.wasm", wasm).await;
    assert_eq!(upload.status, StatusCode::CREATED);
    let sha = upload.json["sha256_hex"].as_str().unwrap().to_owned();
    let cache = harness
        .data_dir
        .join("plugins/wasm/cache")
        .join(format!("{sha}.wasm"));
    assert!(cache.exists());
    let gc = harness.post_gc().await;
    assert_eq!(gc.status, StatusCode::OK);
    assert_eq!(gc.json["count"], 0);
    assert_eq!(gc.json["removed"].as_array().unwrap().len(), 0);
    assert!(cache.exists());
}

#[tokio::test]
async fn filename_with_traversal_dot_dot_slash_rejected_400() {
    let harness = Harness::new();
    let response = harness
        .upload("badname", "../bad.wasm", fixture_wasm())
        .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "invalid_filename");
}

#[tokio::test]
async fn filename_with_embedded_nul_rejected_400() {
    let harness = Harness::new();
    let response = harness
        .upload("badnul", "bad\0name.wasm", fixture_wasm())
        .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.json["error"], "invalid_filename");
}

struct Harness {
    app: axum::Router,
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    storage: std::sync::Arc<cc_lb_storage_redb::RedbStorage>,
}

impl Harness {
    fn new() -> Self {
        let (dir, storage) = temp_storage();
        let data_dir = dir.path().join("data");
        let mut config = Config::default();
        config.runtime.data_dir = Some(data_dir.clone());
        let app = router(test_state(config, Some(storage.clone())));
        Self {
            app,
            _dir: dir,
            data_dir,
            storage,
        }
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

    async fn post_gc(&self) -> TestResponse {
        let request = Request::builder()
            .method("POST")
            .uri("/admin/v1/plugins/wasm/gc")
            .header("Authorization", format!("Bearer {TOKEN}"))
            .body(Body::empty())
            .unwrap();
        send(self.app.clone(), request).await
    }

    async fn list_registry(&self) -> TestResponse {
        let request = Request::builder()
            .method("GET")
            .uri("/admin/v1/plugins/registry")
            .header("Authorization", format!("Bearer {TOKEN}"))
            .body(Body::empty())
            .unwrap();
        send(self.app.clone(), request).await
    }
}

struct TestResponse {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    json: Value,
}

async fn send(app: axum::Router, request: Request<Body>) -> TestResponse {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if !status.is_success() {
        eprintln!("status={status} body={}", String::from_utf8_lossy(&bytes));
    }
    TestResponse {
        status,
        headers,
        json,
    }
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

fn fixture_wasm() -> &'static [u8] {
    let path = FIXTURE_WASM.get_or_init(|| {
        let workspace_target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target");
        let fixture_target = workspace_target.join("fixture-wasm");
        let path = fixture_target.join("wasm32-unknown-unknown/release/extism_echo_plugin.wasm");
        if !path.exists() {
            let status = Command::new("cargo")
                .env_remove("RUSTFLAGS")
                .env_remove("CARGO_ENCODED_RUSTFLAGS")
                .env_remove("RUSTC_WORKSPACE_WRAPPER")
                .env_remove("RUSTC_WRAPPER")
                .env_remove("LLVM_PROFILE_FILE")
                .env_remove("CARGO_BUILD_RUSTFLAGS")
                .env("CARGO_TARGET_DIR", &fixture_target)
                .args([
                    "build",
                    "-p",
                    "extism-echo-plugin",
                    "--target",
                    "wasm32-unknown-unknown",
                    "--release",
                ])
                .status()
                .expect("cargo build extism fixture starts");
            assert!(status.success(), "fixture wasm build failed");
        }
        path
    });
    Box::leak(
        std::fs::read(path)
            .expect("fixture wasm exists")
            .into_boxed_slice(),
    )
}

fn hex_sha(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
