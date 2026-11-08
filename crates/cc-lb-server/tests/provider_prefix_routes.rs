use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use cc_lb_storage_api::principal::{Limit, LimitKind};
use reqwest::Method;
use serde_json::{Value, json};
use sqlx::{Row, sqlite::SqlitePoolOptions};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

type Recorded = (String, String, Vec<u8>);

#[derive(Clone, Default)]
struct FakeState(Arc<Mutex<Vec<Recorded>>>);

impl FakeState {
    fn snapshot(&self) -> Vec<Recorded> {
        self.0.lock().expect("fake request lock").clone()
    }

    fn count_path(&self, path: &str) -> usize {
        self.snapshot()
            .iter()
            .filter(|(_, request_path, _)| request_path == path)
            .count()
    }
}

struct FakeUpstream {
    base_url: String,
    state: FakeState,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake_handler(State(state): State<FakeState>, request: Request) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let body = to_bytes(request.into_body(), 1_048_576)
        .await
        .expect("fake request body");
    state
        .0
        .lock()
        .expect("fake request lock")
        .push((method, path.clone(), body.to_vec()));

    let request_json = serde_json::from_slice::<Value>(&body).unwrap_or_default();
    if (path == "/v1/responses"
        && request_json
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false))
        || (path.starts_with("/v1beta/models/") && path.ends_with(":streamGenerateContent"))
    {
        let (content_type, body) = if path == "/v1/responses" {
            (
                "text/event-stream",
                concat!(
                    "event: response.created\n",
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_provider_prefix\",\"status\":\"in_progress\",\"usage\":null}}\n\n",
                    "event: response.output_text.delta\n",
                    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"openai-ok\"}\n\n",
                    "event: response.completed\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_provider_prefix\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
                ),
            )
        } else {
            (
                "text/event-stream",
                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"gemini-ok\"}],\"role\":\"model\"},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1,\"totalTokenCount\":2}}\n\n",
            )
        };
        return (
            axum::http::StatusCode::OK,
            [(header::CONTENT_TYPE, content_type)],
            Body::from(body),
        )
            .into_response();
    }

    let (status, response) = match path.as_str() {
        "/v1/messages" => (
            200,
            json!({
                "id": "msg_provider_prefix",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-5-sonnet-20241022",
                "content": [{"type": "text", "text": "anthropic-ok"}],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {"input_tokens": 3, "output_tokens": 4}
            }),
        ),
        "/v1/chat/completions" => (
            200,
            json!({
                "id": "chatcmpl_provider_prefix",
                "object": "chat.completion",
                "created": 1,
                "model": "gpt-4o-mini",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "openai-ok"},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18
                }
            }),
        ),
        "/v1/responses" => (
            200,
            json!({
                "id": "resp_provider_prefix",
                "object": "response",
                "status": "completed",
                "model": "gpt-5",
                "output": [{
                    "type": "message",
                    "id": "msg_provider_prefix",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "openai-ok", "annotations": []}]
                }],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
            }),
        ),
        path if path.starts_with("/v1beta/models/") && path.ends_with(":generateContent") => (
            200,
            json!({
                "candidates": [{
                    "content": {"parts": [{"text": "gemini-ok"}], "role": "model"},
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": 1,
                    "candidatesTokenCount": 1,
                    "totalTokenCount": 2
                },
                "modelVersion": "gemini-2.5-flash"
            }),
        ),
        _ => (404, json!({"error": "unexpected fake upstream path"})),
    };
    (
        axum::http::StatusCode::from_u16(status).expect("fake status"),
        Json(response),
    )
        .into_response()
}

async fn spawn_fake_upstream() -> FakeUpstream {
    let state = FakeState::default();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind provider fake upstream");
    let addr = listener.local_addr().expect("provider fake address");
    let router = Router::new()
        .fallback(fake_handler)
        .with_state(state.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    FakeUpstream {
        base_url: format!("http://{addr}/"),
        state,
        task,
    }
}

async fn admin_request(
    client: &reqwest::Client,
    server: &common::TestServer,
    method: Method,
    path: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> (reqwest::StatusCode, Value, Option<String>) {
    let url = format!("http://{}{}", server.admin_addr, path);
    let mut request = client
        .request(method, url)
        .bearer_auth("admin-token")
        .header("accept", "application/json");
    if let Some(if_match) = if_match {
        request = request.header("if-match", if_match);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.expect("admin request");
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.json::<Value>().await.expect("admin JSON response");
    (status, body, etag)
}

async fn disable_seeded_anthropic(client: &reqwest::Client, server: &common::TestServer) {
    let (status, body, _) = admin_request(
        client,
        server,
        Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "list upstreams: {body}");
    let ids = body["upstreams"]
        .as_array()
        .expect("upstreams list")
        .iter()
        .map(|entry| entry["id"].as_str().expect("upstream id").to_owned())
        .collect::<Vec<_>>();
    for id in ids {
        let (status, detail, etag) = admin_request(
            client,
            server,
            Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "upstream detail: {detail}");
        let etag = etag.expect("upstream detail etag");
        let (status, body, _) = admin_request(
            client,
            server,
            Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
            Some(&etag),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "disable upstream: {body}");
    }
}

async fn create_upstream(
    client: &reqwest::Client,
    server: &common::TestServer,
    body: Value,
) -> String {
    let (status, response, _) = admin_request(
        client,
        server,
        Method::POST,
        "/admin/v1/upstreams",
        Some(body),
        None,
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "create upstream: {response}"
    );
    response["id"]
        .as_str()
        .expect("created upstream id")
        .to_owned()
}

async fn unauthenticated_post(
    server: &common::TestServer,
    path: &str,
    body: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{}{}", server.proxy_addr, path))
        .header("content-type", "application/json")
        .body(body.to_owned())
        .send()
        .await
        .expect("unauthenticated proxy request")
}

async fn wait_for_openai_event(server: &common::TestServer) -> sqlx::sqlite::SqliteRow {
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .expect("open request event database");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(event) = sqlx::query(
            "SELECT upstream_name, input_tokens, output_tokens, list_status, event_kind
             FROM request_events_v1
             WHERE upstream_name = 'provider-openai' AND list_status = 200
             ORDER BY id DESC LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .expect("query OpenAI request event")
        {
            return event;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for OpenAI request event"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn provider_prefixed_routes_dispatch_and_filter_by_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server().await;
    let fake = spawn_fake_upstream().await;
    let admin = reqwest::Client::new();

    disable_seeded_anthropic(&admin, &server).await;
    create_upstream(
        &admin,
        &server,
        json!({
            "name": "provider-anthropic",
            "kind": "anthropic_api_key",
            "base_url": fake.base_url.clone(),
            "api_key_value": "sk-ant-api03-provider-prefix-test",
            "skip_preflight": true
        }),
    )
    .await;
    create_upstream(
        &admin,
        &server,
        json!({
            "name": "provider-openai",
            "kind": "openai_api_key",
            "base_url": fake.base_url.clone(),
            "api_key_value": "sk-openai-provider-prefix-test",
            "skip_preflight": true
        }),
    )
    .await;
    create_upstream(
        &admin,
        &server,
        json!({
            "name": "provider-gemini",
            "kind": "gemini_api_key",
            "base_url": fake.base_url.clone(),
            "api_key_value": "AIza-provider-prefix-test",
            "skip_preflight": true
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(350)).await;

    let anthropic = common::http_post(
        server.proxy_addr,
        "/anthropic/v1/messages",
        &server.managed_key.plaintext,
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":8}"#,
        &[],
    )
    .await
    .expect("Anthropic provider-prefixed request");
    assert_eq!(anthropic.status, 200, "{}", anthropic.body);
    assert_eq!(fake.state.count_path("/v1/messages"), 1);

    let openai = common::http_post(
        server.proxy_addr,
        "/openai/v1/chat/completions",
        &server.managed_key.plaintext,
        r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}"#,
        &[],
    )
    .await
    .expect("OpenAI provider-prefixed request");
    assert_eq!(openai.status, 200, "{}", openai.body);
    assert_eq!(fake.state.count_path("/v1/chat/completions"), 1);
    let event = wait_for_openai_event(&server).await;
    assert_eq!(event.get::<String, _>("upstream_name"), "provider-openai");
    assert_eq!(event.get::<i64, _>("input_tokens"), 11);
    assert_eq!(event.get::<i64, _>("output_tokens"), 7);
    assert_eq!(event.get::<i64, _>("list_status"), 200);
    assert_eq!(event.get::<String, _>("event_kind"), "chat_completions");

    let openai_messages_before = fake.state.count_path("/v1/messages");
    let openai_messages = common::http_post(
        server.proxy_addr,
        "/openai/v1/messages",
        &server.managed_key.plaintext,
        "{}",
        &[],
    )
    .await
    .expect("OpenAI unsupported request");
    assert_eq!(openai_messages.status, 404);
    let openai_messages_body: Value =
        serde_json::from_str(&openai_messages.body).expect("OpenAI unsupported JSON");
    assert_eq!(
        openai_messages_body["error"]["code"],
        "operation_not_supported"
    );
    assert_eq!(
        openai_messages_body["error"]["type"],
        "operation_not_supported"
    );
    assert_eq!(
        fake.state.count_path("/v1/messages"),
        openai_messages_before,
        "unsupported OpenAI operation called an upstream"
    );

    let gemini = common::http_post(
        server.proxy_addr,
        "/gemini/v1beta/models/gemini-2.5-flash:generateContent",
        &server.managed_key.plaintext,
        r#"{"contents":[{"parts":[{"text":"hi"}]}]}"#,
        &[],
    )
    .await
    .expect("Gemini provider-prefixed request");
    assert_eq!(gemini.status, 200, "{}", gemini.body);
    assert_eq!(
        fake.state
            .count_path("/v1beta/models/gemini-2.5-flash:generateContent"),
        1
    );

    let gemini_files_before = fake.state.snapshot().len();
    let gemini_files = common::http_post(
        server.proxy_addr,
        "/gemini/v1beta/files",
        &server.managed_key.plaintext,
        "{}",
        &[],
    )
    .await
    .expect("Gemini unsupported request");
    assert_eq!(gemini_files.status, 404);
    let gemini_files_body: Value =
        serde_json::from_str(&gemini_files.body).expect("Gemini unsupported JSON");
    assert_eq!(gemini_files_body["error"]["code"], 404);
    assert_eq!(
        gemini_files_body["error"]["type"],
        "operation_not_supported"
    );
    assert_eq!(
        fake.state.snapshot().len(),
        gemini_files_before,
        "unsupported Gemini operation called an upstream"
    );

    let legacy_before = fake.state.snapshot().len();
    let legacy_openai_chat = common::http_post(
        server.proxy_addr,
        "/v1/chat/completions",
        &server.managed_key.plaintext,
        "{}",
        &[],
    )
    .await
    .expect("unprefixed OpenAI chat request");
    assert_eq!(legacy_openai_chat.status, 404);
    let legacy_body: Value =
        serde_json::from_str(&legacy_openai_chat.body).expect("legacy unsupported JSON");
    assert_eq!(legacy_body["type"], "error");
    assert_eq!(legacy_body["error"]["type"], "operation_not_supported");
    assert_eq!(fake.state.snapshot().len(), legacy_before);

    let unauthenticated_before = fake.state.snapshot().len();
    let unauthenticated = unauthenticated_post(
        &server,
        "/openai/v1/chat/completions",
        r#"{"model":"gpt-4o-mini","messages":[]}"#,
    )
    .await;
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(fake.state.snapshot().len(), unauthenticated_before);

    let openai_id = {
        let (status, body, _) = admin_request(
            &admin,
            &server,
            Method::GET,
            "/admin/v1/upstreams",
            None,
            None,
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK);
        body["upstreams"]
            .as_array()
            .expect("upstream list")
            .iter()
            .find(|entry| entry["name"] == "provider-openai")
            .and_then(|entry| entry["id"].as_str())
            .expect("OpenAI API-key upstream id")
            .to_owned()
    };
    let (status, detail, etag) = admin_request(
        &admin,
        &server,
        Method::GET,
        &format!("/admin/v1/upstreams/{openai_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "OpenAI detail: {detail}");
    let (status, disabled, _) = admin_request(
        &admin,
        &server,
        Method::POST,
        &format!("/admin/v1/upstreams/{openai_id}/disable"),
        None,
        Some(&etag.expect("OpenAI upstream etag")),
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "disable OpenAI: {disabled}"
    );
    create_upstream(
        &admin,
        &server,
        json!({
            "name": "provider-openai-oauth",
            "kind": "openai_oauth",
            "base_url": fake.base_url.clone(),
            "oauth_tokens": {
                "access_token": "provider-prefix-oauth-access",
                "refresh_token": "",
                "expires_at_unix_secs": 1_900_000_000u64,
                "refresh_token_expires_at_unix_secs": null,
                "account_id": "provider-prefix-account",
                "project_id": null,
                "scopes": ["openid", "profile"],
                "never_refresh": true
            },
            "skip_preflight": true
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(350)).await;

    let embeddings_before = fake.state.snapshot().len();
    let embeddings = common::http_post(
        server.proxy_addr,
        "/openai/v1/embeddings",
        &server.managed_key.plaintext,
        r#"{"model":"text-embedding-3-small","input":"hello"}"#,
        &[],
    )
    .await
    .expect("OpenAI OAuth-only embeddings request");
    assert_eq!(embeddings.status, 503, "{}", embeddings.body);
    let embeddings_body: Value =
        serde_json::from_str(&embeddings.body).expect("embeddings error JSON");
    assert_eq!(
        embeddings_body["error"]["code"],
        "route_no_upstream_after_filter"
    );
    assert_eq!(fake.state.snapshot().len(), embeddings_before);
}

#[tokio::test]
async fn output_token_cap_rejects_uncapped_responses_and_gemini_generation() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_principal_limits(
        "",
        vec![Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: 1000,
        }],
        fake_anthropic::AppConfig::default(),
    )
    .await;
    let fake = spawn_fake_upstream().await;
    let admin = reqwest::Client::new();

    disable_seeded_anthropic(&admin, &server).await;
    create_upstream(
        &admin,
        &server,
        json!({
            "name": "provider-openai-output-cap",
            "kind": "openai_api_key",
            "base_url": fake.base_url.clone(),
            "api_key_value": "sk-openai-output-cap-test",
            "skip_preflight": true
        }),
    )
    .await;
    create_upstream(
        &admin,
        &server,
        json!({
            "name": "provider-gemini-output-cap",
            "kind": "gemini_api_key",
            "base_url": fake.base_url.clone(),
            "api_key_value": "AIza-output-cap-test",
            "skip_preflight": true
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(350)).await;

    let before = fake.state.snapshot().len();
    let responses_uncapped = common::http_post(
        server.proxy_addr,
        "/openai/v1/responses",
        &server.managed_key.plaintext,
        r#"{"model":"gpt-5","input":"hello"}"#,
        &[],
    )
    .await
    .expect("uncapped Responses request");
    assert_eq!(
        responses_uncapped.status, 400,
        "{}",
        responses_uncapped.body
    );
    assert_eq!(fake.state.snapshot().len(), before);

    let responses_capped = common::http_post(
        server.proxy_addr,
        "/openai/v1/responses",
        &server.managed_key.plaintext,
        r#"{"model":"gpt-5","input":"hello","max_output_tokens":1}"#,
        &[],
    )
    .await
    .expect("capped Responses request");
    assert_eq!(responses_capped.status, 200, "{}", responses_capped.body);
    assert_eq!(fake.state.count_path("/v1/responses"), 1);

    let before = fake.state.snapshot().len();
    let responses_stream_uncapped = common::http_post(
        server.proxy_addr,
        "/openai/v1/responses",
        &server.managed_key.plaintext,
        r#"{"model":"gpt-5","input":"hello","stream":true}"#,
        &[],
    )
    .await
    .expect("uncapped Responses stream request");
    assert_eq!(
        responses_stream_uncapped.status, 400,
        "{}",
        responses_stream_uncapped.body
    );
    assert_eq!(fake.state.snapshot().len(), before);

    let before = fake.state.snapshot().len();
    let gemini_uncapped = common::http_post(
        server.proxy_addr,
        "/gemini/v1beta/models/gemini-2.5-flash:generateContent",
        &server.managed_key.plaintext,
        r#"{"contents":[{"parts":[{"text":"hello"}]}]}"#,
        &[],
    )
    .await
    .expect("uncapped Gemini request");
    assert_eq!(gemini_uncapped.status, 400, "{}", gemini_uncapped.body);
    assert_eq!(fake.state.snapshot().len(), before);

    let gemini_capped = common::http_post(
        server.proxy_addr,
        "/gemini/v1beta/models/gemini-2.5-flash:generateContent",
        &server.managed_key.plaintext,
        r#"{"contents":[{"parts":[{"text":"hello"}]}],"generationConfig":{"maxOutputTokens":1}}"#,
        &[],
    )
    .await
    .expect("capped Gemini request");
    assert_eq!(gemini_capped.status, 200, "{}", gemini_capped.body);
    assert_eq!(
        fake.state
            .count_path("/v1beta/models/gemini-2.5-flash:generateContent"),
        1
    );

    let before = fake.state.snapshot().len();
    let gemini_stream_uncapped = common::http_post(
        server.proxy_addr,
        "/gemini/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
        &server.managed_key.plaintext,
        r#"{"contents":[{"parts":[{"text":"hello"}]}]}"#,
        &[],
    )
    .await
    .expect("uncapped Gemini stream request");
    assert_eq!(
        gemini_stream_uncapped.status, 400,
        "{}",
        gemini_stream_uncapped.body
    );
    assert_eq!(fake.state.snapshot().len(), before);
}
