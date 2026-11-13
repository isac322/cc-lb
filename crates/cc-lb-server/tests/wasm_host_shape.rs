use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
use cc_lb_server::wasm_host::WasmtimeUpstreamDialect;
use cc_lb_upstream::{DialectShapeContext, shape_request};
use http::{HeaderMap, HeaderName, HeaderValue, Method};

fn wasm_path() -> PathBuf {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    workspace_root.join("target/wasm32-unknown-unknown/release/wasmtime_shape_passthrough.wasm")
}

fn load_wasm_or_skip() -> Option<Vec<u8>> {
    let path = wasm_path();
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(error) => {
            eprintln!(
                "skipping wasmtime-shape-passthrough e2e: wasm artifact missing at {} ({error})",
                path.display(),
            );
            None
        }
    }
}

#[test]
fn shape_adapter_when_wasm_passthrough_then_shapes_and_strips_auth_header() {
    // Given: a real PDK-built shape plugin and a request that carries credentials.
    let Some(wasm_bytes) = load_wasm_or_skip() else {
        return;
    };
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("test-shape"),
            "test-shape",
            &wasm_bytes,
        )
        .expect("register_shape OK");
    let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    ));
    let dialect = WasmtimeUpstreamDialect::new(dispatch);
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        HeaderName::from_static("x-trace-id"),
        HeaderValue::from_static("abc-123"),
    );
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret"),
    );
    let context = DialectShapeContext {
        request_id: "req-shape".to_owned(),
        downstream_headers: headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: Some("stream=true".to_owned()),
        body_bytes: Bytes::from_static(b"{\"prompt\":\"hi\"}"),
    };
    let principal = Principal {
        id: "tenant-shape".to_owned(),
        kind: PrincipalKind::ApiKey,
    };
    let upstream = Upstream::AnthropicDirect {
        base_url: Some(url::Url::parse("https://example.test").expect("fixture URL")),
    };

    // When: the server-owned adapter shapes the request through wasm.
    let shaped = shape_request(&dialect, &context, &upstream, &principal).expect("shape OK");

    // Then: the guest shape is retained while host credential policy is enforced.
    assert_eq!(
        shaped.url().as_str(),
        "https://example.test/v1/messages?stream=true"
    );
    assert_eq!(shaped.method(), &Method::POST);
    assert_eq!(shaped.body().as_ref(), b"{\"prompt\":\"hi\"}");
    assert_eq!(
        shaped.headers().get(http::header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/json")),
    );
    assert_eq!(
        shaped.headers().get("x-trace-id"),
        Some(&HeaderValue::from_static("abc-123")),
    );
    assert!(shaped.headers().get(http::header::AUTHORIZATION).is_none());
}

fn openai_response_header_plugin() -> Vec<u8> {
    use cc_lb_plugin_wire::schema::WireSchema;

    let output = rkyv::to_bytes::<rkyv::rancor::Error>(
        &cc_lb_plugin_wire::TransformResponseResult::Replace {
            status: None,
            headers: Some(
                vec![cc_lb_plugin_wire::Header {
                    name: "x-wasm-transformed".into(),
                    value: Box::from(*b"1"),
                }]
                .into_boxed_slice(),
            ),
            body: None,
        },
    )
    .expect("serialize response header transform");
    let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
    let packed = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32);
    let mut wasm = wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 16) "{data}")
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 8192)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_transform_response_openai") (param i32 i32) (result i64)
                i64.const {}))"#,
        packed as i64,
    ))
    .expect("compile in-memory response transform plugin");

    fn leb(bytes: &mut Vec<u8>, mut value: usize) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            bytes.push(if value == 0 { byte } else { byte | 0x80 });
            if value == 0 {
                break;
            }
        }
    }
    fn section(bytes: &mut Vec<u8>, name: &str, data: &[u8]) {
        let mut payload = Vec::new();
        leb(&mut payload, name.len());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(data);
        bytes.push(0);
        leb(bytes, payload.len());
        bytes.extend_from_slice(&payload);
    }
    section(
        &mut wasm,
        "cc_lb.schema.transform_response.v1",
        &<cc_lb_plugin_wire::TransformResponseRequest as WireSchema>::FINGERPRINT,
    );
    section(
        &mut wasm,
        "cc_lb.plugin.v2",
        br#"{
            "name":"openai-response-header","version":"1.0.0",
            "description":"OpenAI response header transform","usage":"test",
            "hooks":{"transform_response_openai":{
                "wire_version":1,"description":"response","usage":"test","mode":"active"
            }}
        }"#,
    );
    wasm
}

struct LoopbackServer {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for LoopbackServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_loopback(router: axum::Router) -> LoopbackServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback server");
    let addr = listener.local_addr().expect("loopback server address");
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    LoopbackServer { addr, task }
}

#[tokio::test]
async fn openai_transform_response_wasm_plugin_relays_chunked_binary_stream_without_buffering_and_transforms_json()
 {
    use std::convert::Infallible;
    use std::time::Duration;

    use axum::{Json, Router, body::Body, routing::post};
    use cc_lb_aead::AeadService;
    use cc_lb_config::{Config, StorageConfig};
    use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
    use cc_lb_storage_api::{
        MetaStore, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
        PrincipalKind as StoragePrincipalKind, PrincipalStore, UpstreamCreate, UpstreamStore,
        WasmBlob, WasmRegistryEntryInput, upstream::UpstreamKind,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::sync::Notify;
    use tokio::time::timeout;
    use uuid::Uuid;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let flushed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let upstream = serve_loopback(
        Router::new()
            .route(
                "/v1/audio/speech",
                post({
                    let flushed = flushed.clone();
                    let release = release.clone();
                    move || {
                        let flushed = flushed.clone();
                        let release = release.clone();
                        async move {
                            let chunks = futures_util::stream::unfold(
                                (0, flushed, release),
                                |(stage, flushed, release)| async move {
                                    match stage {
                                        0 => {
                                            flushed.notify_one();
                                            Some((
                                                Ok::<_, Infallible>(Bytes::from_static(
                                                    &[b'A'; 64],
                                                )),
                                                (1, flushed, release),
                                            ))
                                        }
                                        1 => {
                                            release.notified().await;
                                            Some((
                                                Ok::<_, Infallible>(Bytes::from_static(
                                                    &[b'B'; 64],
                                                )),
                                                (2, flushed, release),
                                            ))
                                        }
                                        _ => None,
                                    }
                                },
                            );
                            http::Response::builder()
                                .status(200)
                                .header(http::header::CONTENT_TYPE, "application/octet-stream")
                                .body(Body::from_stream(chunks))
                                .expect("binary upstream response")
                        }
                    }
                }),
            )
            .route(
                "/v1/responses",
                post(|| async {
                    Json(json!({
                        "id": "resp_json", "object": "response", "status": "completed",
                        "output": [],
                        "usage": {"input_tokens": 4, "output_tokens": 2, "total_tokens": 6}
                    }))
                }),
            ),
    )
    .await;

    let dir = tempfile::tempdir().expect("proxy storage directory");
    let path = dir.path().join("storage.sqlite");
    let clock = Arc::new(cc_lb_clock::SystemClock);
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&format!("sqlite://{}", path.display()), clock.clone())
            .await
            .expect("open proxy storage"),
    );
    storage
        .initialize()
        .await
        .expect("initialize proxy storage");
    let aead = Arc::new(AeadService::from_master_key([0; 32]));
    let upstream_id = Uuid::new_v4();
    UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: upstream_id,
            name: "binary-openai".to_owned(),
            kind: UpstreamKind::OpenaiApiKey,
            base_url: Some(url::Url::parse(&format!("http://{}/", upstream.addr)).unwrap()),
            api_key_ciphertext: Some(
                aead.encrypt(b"sk-openai-binary-test", upstream_id.as_bytes())
                    .expect("encrypt upstream API key"),
            ),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create OpenAI upstream");
    let principal = PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "binary-wasm-proxy".to_owned(),
            kind: StoragePrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: vec![upstream_id],
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("create proxy principal");
    let (_, key) = KeyStore::new(storage.clone())
        .create(
            &principal.name,
            CreateParams {
                label: "binary-wasm-test".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("create managed API key");
    let wasm = openai_response_header_plugin();
    let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
        .expect("inspect real response transform plugin")
        .metadata;
    let (registry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: Sha256::digest(&wasm).into(),
                size_bytes: wasm.len() as u64,
                bytes: wasm,
            },
            WasmRegistryEntryInput {
                name: metadata.name,
                version: Some(metadata.version),
                original_filename: "openai-response-header.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1,
                uploaded_by_admin_id: Uuid::nil(),
                description: metadata.description,
                usage: metadata.usage,
                hook_metadata: metadata.hooks,
                supported_slots: vec![PluginSlotKind::Shape],
            },
        )
        .await
        .expect("persist response transform plugin");
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id: principal.id,
            slot: PluginSlotKind::Shape,
            order: 1000,
            wasm_registry_id: registry.id,
            config: json!({}),
        })
        .await
        .expect("attach response transform plugin");
    let mut config = Config {
        storage: StorageConfig::Sqlite { path },
        ..Default::default()
    };
    config.runtime.data_dir = Some(dir.path().to_path_buf());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    let app = cc_lb_server::app::build_app_with_storage(
        config,
        None,
        storage.clone(),
        storage.clone(),
        aead,
        clock,
    )
    .await
    .expect("build real proxy app");
    let proxy = serve_loopback(app.router.clone()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("HTTP client");
    let json_response = client
        .post(format!("http://{}/openai/v1/responses", proxy.addr))
        .bearer_auth(key.expose())
        .json(&json!({
            "model": "gpt-4o-mini", "input": "hi", "stream": false, "max_output_tokens": 8
        }))
        .send()
        .await
        .expect("JSON control response");
    assert_eq!(json_response.status(), reqwest::StatusCode::OK);
    assert_eq!(json_response.headers()["x-wasm-transformed"], "1");
    assert_eq!(
        json_response.json::<serde_json::Value>().await.unwrap()["id"],
        "resp_json"
    );

    let mut binary_response = timeout(
        Duration::from_secs(2),
        client
            .post(format!("http://{}/openai/v1/audio/speech", proxy.addr))
            .bearer_auth(key.expose())
            .json(&json!({
                "model": "gpt-4o-mini-tts", "input": "hi", "voice": "alloy"
            }))
            .send(),
    )
    .await
    .expect("binary headers must arrive before releasing the upstream")
    .expect("binary response");
    assert_eq!(binary_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        binary_response.headers()[http::header::CONTENT_TYPE],
        "application/octet-stream"
    );
    assert!(
        binary_response
            .headers()
            .get("x-wasm-transformed")
            .is_none()
    );
    assert!(
        binary_response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .is_none()
    );
    timeout(Duration::from_secs(2), flushed.notified())
        .await
        .expect("upstream yielded its initial chunk");
    let first = timeout(Duration::from_secs(2), binary_response.chunk())
        .await
        .expect("first binary chunk must arrive before releasing the upstream")
        .expect("read initial binary chunk")
        .expect("initial binary chunk");
    assert_eq!(first.as_ref(), &[b'A'; 64]);

    release.notify_one();
    let remaining = timeout(Duration::from_secs(2), binary_response.bytes())
        .await
        .expect("remaining binary body must finish after release")
        .expect("read remaining binary body");
    assert_eq!(remaining.as_ref(), &[b'B'; 64]);
}
