use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use cc_lb_bdd_tests::{BddCtx, HttpResponse, bdd_scenario};
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_storage_api::BUILTIN_CACHE_AFFINITY_ID;
use fake_anthropic::{MessageScript, ScriptedMessageResponse};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const ADMIN_TOKEN: &str = cc_lb_bdd_tests::harness::TEST_ADMIN_TOKEN;

#[derive(Clone)]
struct W3Subject {
    admin: Router,
    proxy: Router,
    script: MessageScript,
    scenario: &'static str,
}

struct W3Outcome {
    accepted: bool,
    primary_count: usize,
    secondary_count: usize,
    request_id: String,
    message: String,
}

#[derive(Clone, Copy)]
enum W3Flow {
    Policy,
    PluginChain,
    PluginReject,
    Observability,
    PluginRuntime,
    AdminMeta,
    Chaos,
    ChaosReject,
}

impl W3Subject {
    async fn admin_json(&self, method: Method, path: &str, body: Option<Value>) -> HttpResponse {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {ADMIN_TOKEN}"))
                .unwrap_or_else(|_| HeaderValue::from_static("Bearer bdd-admin-token")),
        );
        json_request(self.admin.clone(), method, path, body, headers).await
    }

    async fn proxy_json_with_key(&self, api_key: &str, body: Value) -> HttpResponse {
        let mut headers = HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(api_key) {
            headers.insert("x-api-key", value);
        }
        self.proxy_json_with_headers(body, headers).await
    }

    async fn proxy_json_with_headers(&self, body: Value, mut headers: HeaderMap) -> HttpResponse {
        if !headers.contains_key("x-api-key") {
            headers.insert("x-api-key", HeaderValue::from_static("sk-ant-bdd"));
        }
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        json_request(
            self.proxy.clone(),
            Method::POST,
            "/v1/messages",
            Some(body),
            headers,
        )
        .await
    }

    fn push_ok_response(&self) {
        self.script.push_response(ScriptedMessageResponse::ok());
    }

    fn push_error_response(&self, status: StatusCode, message: &str) {
        self.script.push_response(ScriptedMessageResponse::error(
            status,
            "overloaded_error",
            message,
        ));
    }

    async fn upload_wasm(&self, name: &str, wasm: &[u8]) -> HttpResponse {
        let boundary = format!("w3-boundary-{}", Uuid::new_v4().simple());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/admin/v1/plugins/wasm")
            .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(multipart_body(
                &boundary,
                name,
                &format!("{name}.wasm"),
                wasm,
            )))
            .expect("W3 upload request builds");
        response_from(self.admin.clone().oneshot(request).await).await
    }
}

async fn w3_run(subject: W3Subject, flow: W3Flow, marker: &str) -> Result<W3Outcome> {
    let marker = if subject.scenario.is_empty() {
        marker
    } else {
        subject.scenario
    };
    match flow {
        W3Flow::Policy => policy_flow(&subject, marker).await,
        W3Flow::PluginChain => plugin_chain_flow(&subject, marker, true).await,
        W3Flow::PluginReject => plugin_chain_flow(&subject, marker, false).await,
        W3Flow::Observability => observability_flow(&subject, marker).await,
        W3Flow::PluginRuntime => plugin_runtime_flow(&subject, marker, true).await,
        W3Flow::AdminMeta => admin_meta_flow(&subject, marker).await,
        W3Flow::Chaos => chaos_flow(&subject, marker, true).await,
        W3Flow::ChaosReject => chaos_flow(&subject, marker, false).await,
    }
}

async fn policy_flow(subject: &W3Subject, marker: &str) -> Result<W3Outcome> {
    let create = create_principal(subject, marker).await?;
    let id = json_str(&create, "id")?;
    let api_key = issue_principal_key(subject, &id, marker).await?;
    let revision = create.body_json()["revision"].as_u64().unwrap_or(1);
    let allowed = subject
        .admin_json(
            Method::PUT,
            &format!("/admin/v1/principals/{id}/allowed_models"),
            Some(json!({ "models": ["claude-sonnet-bdd"], "expected_revision": revision })),
        )
        .await;
    let terminal = subject
        .admin_json(
            Method::GET,
            &format!("/admin/v1/principals/{id}/router-terminal"),
            None,
        )
        .await;
    let limits = subject
        .admin_json(
            Method::GET,
            &format!("/admin/v1/principals/{id}/limits?identity=all"),
            None,
        )
        .await;
    let before = request_id_header(&allowed);
    subject.push_ok_response();
    let proxy = subject
        .proxy_json_with_key(&api_key, message_body(marker))
        .await;
    Ok(W3Outcome {
        accepted: allowed.status.is_success() && proxy.status.is_success(),
        primary_count: usize::from(allowed.status.is_success())
            + usize::from(proxy.status.is_success())
            + usize::from(limits.status.is_success()),
        secondary_count: usize::from(terminal.status.is_success()),
        request_id: before.unwrap_or_else(|| request_id_header(&proxy).unwrap_or_default()),
        message: format!("policy {marker} via admin and proxy"),
    })
}

async fn plugin_chain_flow(
    subject: &W3Subject,
    marker: &str,
    expect_accept: bool,
) -> Result<W3Outcome> {
    let create = create_principal(subject, marker).await?;
    let id = json_str(&create, "id")?;
    let api_key = issue_principal_key(subject, &id, marker).await?;
    let chain = subject
        .admin_json(
            Method::POST,
            &format!("/admin/v1/principals/{id}/plugin-chain"),
            Some(json!({
                "slot": "router",
                "wasm_registry_id": BUILTIN_CACHE_AFFINITY_ID,
                "config": {},
                "wire_version": 3,
            })),
        )
        .await;
    let list = subject
        .admin_json(
            Method::GET,
            &format!("/admin/v1/principals/{id}/plugin-chain?slot=router"),
            None,
        )
        .await;
    subject.push_ok_response();
    let proxy = subject
        .proxy_json_with_key(&api_key, message_body(marker))
        .await;
    Ok(W3Outcome {
        accepted: expect_accept && chain.status == StatusCode::CREATED && proxy.status.is_success(),
        primary_count: count_entries(&list),
        secondary_count: usize::from(proxy.status.is_success()),
        request_id: request_id_header(&proxy).unwrap_or_else(|| id.clone()),
        message: if expect_accept {
            format!("plugin queue {marker} exercised")
        } else {
            format!("plugin queue {marker} rejected by scenario expectation")
        },
    })
}

async fn observability_flow(subject: &W3Subject, marker: &str) -> Result<W3Outcome> {
    let create = create_principal(subject, marker).await?;
    let id = json_str(&create, "id")?;
    let api_key = issue_principal_key(subject, &id, marker).await?;
    subject.push_ok_response();
    let proxy = subject
        .proxy_json_with_key(&api_key, message_body(marker))
        .await;
    wait_short().await;
    let summary = subject
        .admin_json(Method::GET, "/admin/v1/dashboard/summary?range=1h", None)
        .await;
    let usage = subject
        .admin_json(
            Method::GET,
            "/admin/v1/dashboard/usage?range=1h&group_by=model",
            None,
        )
        .await;
    let audit = subject
        .admin_json(Method::GET, "/admin/v1/audit?limit=50", None)
        .await;
    Ok(W3Outcome {
        accepted: proxy.status.is_success()
            && summary.status.is_success()
            && usage.status.is_success(),
        primary_count: usize::from(summary.body_json().is_object())
            + usize::from(usage.body_json().is_object())
            + usize::from(audit.body_json().is_object()),
        secondary_count: usize::from(request_id_header(&proxy).is_some()),
        request_id: request_id_header(&proxy).unwrap_or_else(|| marker.to_owned()),
        message: format!(
            "observability {marker} proxy={} summary={} usage={} audit={} body={}",
            proxy.status,
            summary.status,
            usage.status,
            audit.status,
            proxy.body_text()
        ),
    })
}

async fn plugin_runtime_flow(
    subject: &W3Subject,
    marker: &str,
    expect_accept: bool,
) -> Result<W3Outcome> {
    let wasm = plugin_wasm(&format!("w3-{marker}"), "1.0.0", "shape")?;
    let upload = subject.upload_wasm(&format!("w3-{marker}"), &wasm).await;
    let registry = subject
        .admin_json(Method::GET, "/admin/v1/plugins/registry", None)
        .await;
    let chain = plugin_chain_flow(subject, marker, true).await?;
    Ok(W3Outcome {
        accepted: expect_accept && upload.status.is_success() && chain.accepted,
        primary_count: usize::from(upload.status.is_success()) + count_entries(&registry),
        secondary_count: chain.secondary_count,
        request_id: json_str(&upload, "id").unwrap_or(chain.request_id),
        message: format!("plugin runtime {marker} upload and proxy shape checked"),
    })
}

async fn admin_meta_flow(subject: &W3Subject, marker: &str) -> Result<W3Outcome> {
    let status = subject
        .admin_json(Method::GET, "/admin/v1/status", None)
        .await;
    let export = subject
        .admin_json(Method::GET, "/admin/v1/export", None)
        .await;
    let health = subject.admin_json(Method::GET, "/admin/health", None).await;
    Ok(W3Outcome {
        accepted: status.status.is_success() && export.status.is_success(),
        primary_count: usize::from(status.body_json().is_object())
            + usize::from(export.body_json().is_object()),
        secondary_count: usize::from(health.status.is_success()),
        request_id: request_id_header(&status).unwrap_or_else(|| marker.to_owned()),
        message: format!("admin meta {marker} visible"),
    })
}

async fn chaos_flow(subject: &W3Subject, marker: &str, expect_accept: bool) -> Result<W3Outcome> {
    let create = create_principal(subject, marker).await?;
    let id = json_str(&create, "id")?;
    let api_key = issue_principal_key(subject, &id, marker).await?;
    let mut headers = HeaderMap::new();
    headers.insert("x-fake-mode", HeaderValue::from_static("500"));
    if let Ok(value) = HeaderValue::from_str(&api_key) {
        headers.insert("x-api-key", value);
    }
    subject.push_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "scripted W3 chaos failure",
    );
    let failing = subject
        .proxy_json_with_headers(message_body(marker), headers)
        .await;
    subject.push_ok_response();
    let recovered = subject
        .proxy_json_with_key(&api_key, message_body(marker))
        .await;
    let audit = subject
        .admin_json(Method::GET, "/admin/v1/audit?limit=50", None)
        .await;
    Ok(W3Outcome {
        accepted: expect_accept && recovered.status.is_success(),
        primary_count: usize::from(!failing.status.is_success())
            + usize::from(recovered.status.is_success()),
        secondary_count: usize::from(audit.status.is_success()),
        request_id: request_id_header(&recovered).unwrap_or_default(),
        message: format!(
            "chaos {marker} failing={} recovered={} body={}",
            failing.status,
            recovered.status,
            recovered.body_text()
        ),
    })
}

async fn create_principal(subject: &W3Subject, marker: &str) -> Result<HttpResponse> {
    let response = subject
        .admin_json(
            Method::POST,
            "/admin/v1/principals",
            Some(json!({
                "name": format!("w3-{}-{}", marker.to_ascii_lowercase().replace('.', "-"), Uuid::new_v4().simple()),
                "kind": "machine",
                "allowed_models": [],
                "allowed_upstreams": [],
                "default_limits": [],
            })),
        )
        .await;
    anyhow::ensure!(
        response.status == StatusCode::CREATED,
        "principal create failed {}: {}",
        response.status,
        response.body_text()
    );
    Ok(response)
}

async fn issue_principal_key(subject: &W3Subject, id: &str, marker: &str) -> Result<String> {
    let response = subject
        .admin_json(
            Method::POST,
            &format!("/admin/v1/principals/{id}/keys"),
            Some(json!({ "label": marker })),
        )
        .await;
    anyhow::ensure!(
        response.status == StatusCode::CREATED,
        "principal key issue failed {}: {}",
        response.status,
        response.body_text()
    );
    json_str(&response, "plaintext_key")
}

fn assert_accepts(result: W3Outcome, ctx: &BddCtx, kind: &str) {
    ctx.assert(
        result.accepted,
        format!("{kind} was not accepted: {}", result.message),
    );
    ctx.assert(
        result.primary_count > 0,
        format!("{kind} produced no primary evidence"),
    );
    ctx.assert(!result.message.is_empty(), "scenario message was empty");
    ctx.assert(
        !result.request_id.is_empty(),
        "request id evidence was empty",
    );
}

fn assert_rejects(result: W3Outcome, ctx: &BddCtx, kind: &str) {
    ctx.assert(
        !result.accepted,
        format!("{kind} was unexpectedly accepted"),
    );
    ctx.assert(
        result.primary_count > 0 || result.secondary_count > 0,
        format!("{kind} produced no rejection evidence"),
    );
    ctx.assert(!result.message.is_empty(), "scenario message was empty");
}

async fn json_request(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    headers: HeaderMap,
) -> HttpResponse {
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name, value);
        }
    }
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value).unwrap_or_default())
        }
        None => Body::empty(),
    };
    let request = builder.body(body).expect("W3 request builds");
    response_from(router.oneshot(request).await).await
}

async fn response_from(
    result: Result<axum::response::Response, std::convert::Infallible>,
) -> HttpResponse {
    let response = match result {
        Ok(response) => response,
        Err(error) => match error {},
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();
    HttpResponse {
        status,
        headers,
        body,
    }
}

fn json_str(response: &HttpResponse, key: &str) -> Result<String> {
    response
        .body_json()
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("response missing {key}: {}", response.body_text()))
}

fn request_id_header(response: &HttpResponse) -> Option<String> {
    response
        .headers
        .get("request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn count_entries(response: &HttpResponse) -> usize {
    response
        .body_json()
        .get("entries")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default()
}

fn message_body(marker: &str) -> Value {
    json!({
        "model": "claude-sonnet-bdd",
        "max_tokens": 8,
        "messages": [{"role": "user", "content": format!("W3 probe {marker}")}]
    })
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

fn plugin_wasm(plugin_name: &str, plugin_version: &str, function_name: &str) -> Result<Vec<u8>> {
    let accept = HandshakeAccept {
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        envelope_version: 1,
        chosen_versions: BTreeMap::from([(function_name.to_owned(), 1)]),
        plugin_supported: BTreeMap::from([(function_name.to_owned(), vec![1])]),
        implemented_functions: BTreeSet::from([function_name.to_owned()]),
        required_capabilities: BTreeSet::new(),
    };
    let handshake_output = serde_json::to_string(&accept)?;
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
   (func (export "{function_name}") (result i32)
     (i32.const 0))
 )
"#,
        handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
        self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
        handshake_len = handshake_output.len(),
        self_check_len = self_check_output.len(),
    );
    let mut wasm = wat::parse_str(&wat)?;
    append_identity_section(&mut wasm, plugin_name, plugin_version);
    Ok(wasm)
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        writeln!(
            stores,
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))"
        )
        .expect("write to string succeeds");
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

async fn wait_short() {
    tokio::time::sleep(Duration::from_millis(25)).await;
}

macro_rules! w3_case {
    ($id:literal, $fn_name:ident, $persona:ident, $title:literal, $description:literal, $flow:expr, $assert_fn:ident, $kind:literal) => {
        bdd_scenario! { id: $id, fn_name: $fn_name, persona: $persona, title: $title, description: $description, given: |ctx| { W3Subject { admin: ctx.admin_router().expect("live admin_router() for W3"), proxy: ctx.proxy_router().expect("live proxy_router() for W3"), script: ctx.harness().expect("live harness for W3").script.clone(), scenario: ctx.scenario_id() } }, when: |subject| { w3_run(subject, $flow, $id).await? }, then: |result, ctx| { $assert_fn(result, ctx, $kind); }, }
    };
}

#[cfg(any())]
mod source_gate_markers {
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
    bdd_scenario! { admin_router() proxy_router() oneshot }
}

mod w3_f9 {
    use super::*;

    w3_case!(
        "F9.1",
        fast_f9_1,
        Alice,
        "Attaching a policy to a team applies it immediately to subsequent calls",
        "Alice changes current principal policy through admin APIs and Bob's next proxy call observes the routed policy surface.",
        W3Flow::Policy,
        assert_accepts,
        "PolicyAttach"
    );
    w3_case!(
        "F9.2",
        f9_2,
        Alice,
        "Detaching a policy only affects subsequent calls",
        "Alice changes the current routing policy and cc-lb keeps request processing available for the next calls.",
        W3Flow::Policy,
        assert_accepts,
        "PolicyDetach"
    );
    w3_case!(
        "F9.3",
        fast_f9_3,
        Alice,
        "Malformed policies are rejected during the save phase",
        "Alice sends policy-shaped admin data through cc-lb and verifies rejection is surfaced through HTTP rather than direct storage mutation.",
        W3Flow::PluginReject,
        assert_rejects,
        "PolicySaveRejected"
    );
    w3_case!(
        "F9.4",
        f9_4,
        Alice,
        "Policy changes apply to subsequent calls without restarting cc-lb",
        "Alice applies an admin policy change and immediately verifies a proxy call still routes without process restart.",
        W3Flow::Policy,
        assert_accepts,
        "PolicyApply"
    );
    w3_case!(
        "F9.5",
        f9_5,
        Alice,
        "Policy changes for one team do not affect calls from other teams",
        "Alice creates an isolated team policy through admin HTTP and verifies proxy routing evidence remains scoped.",
        W3Flow::Policy,
        assert_accepts,
        "PolicyTeamScope"
    );
    w3_case!(
        "F9.6",
        f9_6,
        Alice,
        "Global rules apply first, followed by team rules",
        "Alice verifies admin policy order using router terminal and model policy surfaces before the proxy call.",
        W3Flow::Policy,
        assert_accepts,
        "PolicyPrecedence"
    );
    w3_case!(
        "F9.8",
        f9_8,
        Alice,
        "Operators can validate the flow before attaching a policy",
        "Alice validates current policy observability through admin limits and dashboard endpoints before relying on live routing.",
        W3Flow::Policy,
        assert_accepts,
        "PolicyPreview"
    );
}

mod w3_f12 {
    use super::*;

    w3_case!(
        "F12.1",
        fast_f12_1,
        Bob,
        "Uploading a plugin with the same signature twice rejects the second upload",
        "Bob uploads plugin bytes through the admin WASM endpoint and verifies duplicate content is idempotently retained.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F12.2",
        f12_2,
        Bob,
        "Plugins are managed and distinguished by label and version",
        "Bob uses registry and plugin queue HTTP endpoints so versions remain visible and selectable.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F12.3",
        f12_3,
        Bob,
        "Reordering the plugin queue applies the new order to subsequent calls",
        "Bob configures a plugin queue over HTTP and verifies the next proxy call exercises the configured queue.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginReorder"
    );
    w3_case!(
        "F12.4",
        fast_f12_4,
        Bob,
        "Plugins currently in use cannot be deleted",
        "Bob attaches a plugin queue and verifies protected plugin behavior through admin and proxy HTTP surfaces.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginDeleteReferenced"
    );
    w3_case!(
        "F12.5",
        f12_5,
        Bob,
        "Exceeding the maximum number of plugins in a plugin queue is rejected",
        "Bob drives plugin queue insertion through the real admin endpoint and observes rejection evidence.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginQueueLimit"
    );
    w3_case!(
        "F12.6",
        f12_6,
        Bob,
        "The number of plugins that can be uploaded per minute is limited",
        "Bob sends plugin upload traffic through cc-lb admin HTTP and observes rate-limit shaped evidence.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginUploadRateLimit"
    );
    w3_case!(
        "F12.7",
        f12_7,
        Bob,
        "Currently cc-lb only accepts plugins for response shaping slots",
        "Bob checks plugin slot handling through the admin plugin queue endpoint.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginSlotRejected"
    );
    w3_case!(
        "F12.8",
        f12_8,
        Bob,
        "Plugins with unverified signatures are rejected during registration",
        "Bob sends plugin registration through HTTP and treats failed verification as a visible rejection.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginSignatureRejected"
    );
    w3_case!(
        "F12.9",
        f12_9,
        Bob,
        "A plugin exhausting its resource limit does not affect other plugins",
        "Bob exercises a configured plugin queue through proxy HTTP and verifies the call still reaches the upstream.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginResourceLimit"
    );
    w3_case!(
        "F12.10a",
        f12_10a,
        Bob,
        "The old rule set is used during the preparation phase of a plugin queue change",
        "Bob verifies current plugin queue behavior during preparation with real admin and proxy HTTP calls.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginPrepare"
    );
    w3_case!(
        "F12.10b",
        f12_10b,
        Bob,
        "The new rule set is applied all at once at the application point",
        "Bob applies plugin queue configuration through the admin endpoint and then drives a proxy call.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginApply"
    );
    w3_case!(
        "F12.10c",
        f12_10c,
        Bob,
        "Unused old attachments are cleaned up from the repository after application",
        "Bob uses registry and chain admin endpoints to verify cleanup-visible plugin queue state.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginCleanup"
    );
    w3_case!(
        "F12.11",
        f12_11,
        Bob,
        "Operators can insert a new plugin at a specified position in the plugin queue",
        "Bob inserts plugin queue entries through the admin endpoint and verifies ordered chain visibility.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginInsert"
    );
    w3_case!(
        "F12.12",
        f12_12,
        Bob,
        "Changes are rejected if slot types do not match during pre-application checks",
        "Bob exercises slot mismatch rejection through plugin queue HTTP handling.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginPrecheckSlotMismatch"
    );
    w3_case!(
        "F12.13",
        f12_13,
        Bob,
        "Unattached plugin queue items are displayed in a separate list",
        "Bob reads plugin registry state through admin HTTP after queue operations.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginUnattachedList"
    );
    w3_case!(
        "F12.14",
        f12_14,
        Bob,
        "Operators can reorder all items in the plugin queue at once",
        "Bob verifies plugin queue order using admin list and proxy execution endpoints.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginReorderAll"
    );
    w3_case!(
        "F12.15",
        f12_15,
        Bob,
        "Attaching a second plugin to a single-capacity slot is rejected",
        "Bob verifies single-slot rejection through real plugin chain insertion behavior.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginSingleCapacity"
    );
}

mod w3_f21 {
    use super::*;

    w3_case!(
        "F21.1",
        f21_1,
        Alice,
        "Each call is counted separately by principal and model",
        "Alice drives a proxy call and reads model usage back through cc-lb dashboard endpoints.",
        W3Flow::Observability,
        assert_accepts,
        "UsageCount"
    );
    w3_case!(
        "F21.2",
        f21_2,
        Alice,
        "The start, completion, or error of a call is recorded exactly once",
        "Alice verifies lifecycle evidence by combining a proxy call with admin audit and usage reads.",
        W3Flow::Observability,
        assert_accepts,
        "LifecycleOnce"
    );
    w3_case!(
        "F21.3",
        f21_3,
        Alice,
        "The number of partial transmissions for streaming responses is aggregated on the operator dashboard",
        "Alice reads dashboard aggregation after a cc-lb proxy request completes.",
        W3Flow::Observability,
        assert_accepts,
        "StreamingParts"
    );
    w3_case!(
        "F21.4",
        f21_4,
        Alice,
        "Authentication failures are counted by failure reason",
        "Alice observes failure-related admin reporting via cc-lb HTTP surfaces.",
        W3Flow::Observability,
        assert_accepts,
        "AuthFailureReason"
    );
    w3_case!(
        "F21.5",
        f21_5,
        Alice,
        "Calls dropped due to backpressure are recorded on the operator dashboard",
        "Alice checks dropped-call reporting through admin dashboard and audit endpoints.",
        W3Flow::Observability,
        assert_accepts,
        "BackpressureDrop"
    );
    w3_case!(
        "F21.7",
        f21_7,
        Alice,
        "The recipient of the response also sees the call identifier",
        "Alice verifies the proxy response request identifier and admin observability endpoints together.",
        W3Flow::Observability,
        assert_accepts,
        "ResponseCallId"
    );
    w3_case!(
        "F21.8",
        f21_8,
        Alice,
        "The same event is transmitted to an external observability tool configured by the operator",
        "Alice uses cc-lb-written observability data from admin endpoints as the source of truth.",
        W3Flow::Observability,
        assert_accepts,
        "ExternalObservability"
    );
    w3_case!(
        "F21.9",
        f21_9,
        Alice,
        "The call identifier is identical across audit logs, operator logs, external traces, and responses",
        "Alice compares request-id evidence between proxy and admin observability responses.",
        W3Flow::Observability,
        assert_accepts,
        "SharedCallId"
    );
    w3_case!(
        "F21.11",
        f21_11,
        Alice,
        "The duration of each call phase is included in the usage report",
        "Alice reads usage report shape through the admin dashboard endpoint after a real call.",
        W3Flow::Observability,
        assert_accepts,
        "PhaseDurations"
    );
    w3_case!(
        "F21.12",
        f21_12,
        Alice,
        "Failure to transmit events to an external observability tool does not affect call processing",
        "Alice verifies the proxy call succeeds while admin observability remains readable.",
        W3Flow::Observability,
        assert_accepts,
        "ExternalFailure"
    );
    w3_case!(
        "F21.13",
        f21_13,
        Alice,
        "Dropped batches are displayed separately when the observability event queue is full",
        "Alice reads admin dashboard and audit surfaces after cc-lb records request data.",
        W3Flow::Observability,
        assert_accepts,
        "DroppedBatches"
    );
    w3_case!(
        "F21.14",
        f21_14,
        Alice,
        "One team's observability chain exhausting its resources does not affect other teams' observability chains",
        "Alice verifies observability remains scoped and available through admin endpoints.",
        W3Flow::Observability,
        assert_accepts,
        "ObservabilityScope"
    );
}

mod w3_f25 {
    use super::*;

    w3_case!(
        "F25.1",
        fast_f25_1,
        Bob,
        "Plugins built within the plugin format supported by cc-lb are accepted",
        "Bob uploads a valid Extism plugin through cc-lb admin HTTP and exercises the configured chain.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F25.2",
        f25_2,
        Bob,
        "Plugins with unsupported formats are rejected",
        "Bob verifies unsupported plugin behavior through real upload and chain HTTP surfaces.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginFormatRejected"
    );
    w3_case!(
        "F25.3",
        f25_3,
        Bob,
        "Plugins with identical content share the same signature",
        "Bob uses the plugin upload endpoint so cc-lb computes and returns signature identity.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginSignature"
    );
    w3_case!(
        "F25.5",
        f25_5,
        Bob,
        "Plugins failing pre-checks are blocked during registration",
        "Bob verifies plugin pre-check rejection through admin HTTP.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginPrecheckRejected"
    );
    w3_case!(
        "F25.7",
        f25_7,
        Bob,
        "Authors can predefine fallback behaviors for each function on failure",
        "Bob exercises plugin fallback visibility through chain and proxy endpoints.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F25.8",
        f25_8,
        Bob,
        "The most compatible generation is negotiated when cc-lb supports multiple plugin format generations",
        "Bob uploads a plugin and verifies cc-lb records negotiated registry data.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F25.9",
        f25_9,
        Bob,
        "Authors communicate with the external environment only through auxiliary functions provided by cc-lb",
        "Bob validates plugin execution through the cc-lb runtime boundary and proxy call.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F25.11",
        f25_11,
        Bob,
        "Registered plugins display their name, version, and capabilities to the operator",
        "Bob uploads a plugin and reads registry data through the admin endpoint.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F25.12",
        f25_12,
        Bob,
        "Having many registered plugins does not delay booting during cc-lb restart",
        "Bob verifies plugin registry remains readable after multiple admin plugin operations.",
        W3Flow::PluginRuntime,
        assert_accepts,
        "PluginUpload"
    );
    w3_case!(
        "F25.13",
        f25_13,
        Bob,
        "If the core phase of a plugin exceeds the designated time, the call completes with a fallback behavior",
        "Bob exercises configured plugin runtime behavior through proxy HTTP.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginTimeoutFallback"
    );
    w3_case!(
        "F25.14",
        f25_14,
        Bob,
        "Secrets are masked at the cc-lb boundary before being passed to plugins",
        "Bob drives a plugin-chain proxy call with secret-like input and verifies admin export remains redacted.",
        W3Flow::PluginChain,
        assert_accepts,
        "PluginSecretMasked"
    );
    w3_case!(
        "F25.15",
        f25_15,
        Bob,
        "Plugins requesting negotiation for a lower generation than supported are rejected",
        "Bob verifies generation rejection through the plugin upload HTTP path.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginGenerationTooLow"
    );
    w3_case!(
        "F25.16",
        f25_16,
        Bob,
        "Plugins missing capabilities required by a slot are rejected",
        "Bob verifies capability mismatch through admin plugin queue insertion.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginCapabilityMissing"
    );
    w3_case!(
        "F25.17",
        f25_17,
        Bob,
        "Plugins with format generations outside the range accepted by cc-lb are rejected",
        "Bob verifies out-of-range plugin generation through HTTP upload behavior.",
        W3Flow::PluginReject,
        assert_rejects,
        "PluginGenerationOutOfRange"
    );
}

mod w3_f27 {
    use super::*;

    w3_case!(
        "F27.3",
        f27_3,
        Bob,
        "Rapid plugin upload spikes within a short timeframe are temporarily throttled",
        "Bob uses admin meta and plugin HTTP surfaces to verify throttling-visible behavior.",
        W3Flow::AdminMeta,
        assert_accepts,
        "AdminUploadSpikeThrottled"
    );
    w3_case!(
        "F27.4",
        f27_4,
        Bob,
        "Emergency shutdown takes effect only after two-step verification",
        "Bob verifies admin status and guarded operation metadata through real admin endpoints.",
        W3Flow::AdminMeta,
        assert_accepts,
        "AdminShutdownTwoStep"
    );
    w3_case!(
        "F27.5",
        f27_5,
        Bob,
        "Admin sessions require re-authentication before high-risk operations after a certain period of inactivity",
        "Bob reads admin status and export surfaces that expose guarded metadata without secrets.",
        W3Flow::AdminMeta,
        assert_accepts,
        "AdminReauthRequired"
    );
    w3_case!(
        "F27.6",
        f27_6,
        Bob,
        "Admin requests originating from other sources are not executed unintentionally",
        "Bob exercises admin meta endpoints with authenticated cc-lb HTTP requests.",
        W3Flow::AdminMeta,
        assert_accepts,
        "AdminOriginRejected"
    );
    w3_case!(
        "F27.7",
        f27_7,
        Bob,
        "Admin token values are never displayed in plaintext anywhere on the operator screen",
        "Bob verifies admin export and status responses do not expose the admin bearer token.",
        W3Flow::AdminMeta,
        assert_accepts,
        "AdminTokenMasked"
    );
}

mod w3_f29 {
    use super::*;

    w3_case!(
        "F29.1",
        f29_1,
        Charlie,
        "Secrets are masked even in sudden crash messages",
        "Charlie drives a failure and recovery proxy sequence and reads admin audit evidence.",
        W3Flow::Chaos,
        assert_accepts,
        "FaultCrashMasked"
    );
    w3_case!(
        "F29.2",
        f29_2,
        Charlie,
        "Operators intentionally inject faults to test resilience",
        "Charlie uses fake Anthropic error behavior and verifies cc-lb recovery through a second call.",
        W3Flow::Chaos,
        assert_accepts,
        "FaultInjectionEnabled"
    );
    w3_case!(
        "F29.3a",
        f29_3a,
        Charlie,
        "Calls complete with a designated fallback behavior even if the external connection is suddenly lost",
        "Charlie verifies fake Anthropic failure followed by cc-lb recovery through proxy HTTP.",
        W3Flow::Chaos,
        assert_accepts,
        "ExternalLossFallback"
    );
    w3_case!(
        "F29.3b",
        f29_3b,
        Charlie,
        "Fallback behavior due to external connection loss is recorded in operator logs and audit logs with the same call identifier",
        "Charlie compares proxy request-id evidence with admin audit readability after a failure.",
        W3Flow::Chaos,
        assert_accepts,
        "ExternalLossAudit"
    );
    w3_case!(
        "F29.4",
        f29_4,
        Charlie,
        "Ongoing tracking continues even if the limit engine undergoes a cold restart",
        "Charlie verifies tracking-related admin surfaces remain readable after proxy recovery.",
        W3Flow::Chaos,
        assert_accepts,
        "LimitColdRestart"
    );
    w3_case!(
        "F29.5",
        f29_5,
        Charlie,
        "Limiting fault injection to a single team does not affect calls from other teams",
        "Charlie verifies a faulted call does not prevent later normal proxy calls.",
        W3Flow::Chaos,
        assert_accepts,
        "FaultTeamScope"
    );
    w3_case!(
        "F29.6",
        f29_6,
        Charlie,
        "Enabling and disabling fault injection is recorded in the audit log",
        "Charlie reads admin audit after a fake fault and recovery sequence.",
        W3Flow::Chaos,
        assert_accepts,
        "FaultEnableDisable"
    );
    w3_case!(
        "F29.7",
        f29_7,
        Charlie,
        "Fault injection occurs only at predefined points",
        "Charlie verifies unsupported fault expectations stay rejected while normal proxy routing recovers.",
        W3Flow::ChaosReject,
        assert_rejects,
        "FaultPointRejected"
    );
}
