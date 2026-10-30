//! Deterministic end-to-end simulations for the real prompt-cache sessions.
//!
//! The upstream is always a loopback Axum service.  The same `TestClock` is
//! injected into storage, lifecycle, and scheduler construction, so advancing
//! the clock never sleeps for the fifteen-minute gaps represented by these
//! scenarios.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_clock::{Clock, TestClock, unix_secs};
use cc_lb_config::{Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::cache_keepalive::SessionKey;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::upstream::{OAuthTokensCreate, UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{CacheKeepaliveConfig, MetaStore, PrincipalStore, UpstreamStore};
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use url::Url;
use uuid::Uuid;

const NOW: u64 = 1_800_000_000;
const CACHE_TTL: u64 = 300;
// Builtin API-key authentication carries the configured principal name into the
// engine; lifecycle glue hashes non-UUID principal IDs before building SessionKey.
const PRINCIPAL_NAME: &str = "real-session-principal";

fn runtime_principal_uuid(principal_name: &str) -> Uuid {
    let digest = Sha256::digest(principal_name.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn runtime_session_key_hash(
    principal_name: &str,
    thread_id: &str,
    agent_id: Option<&str>,
) -> String {
    SessionKey::from_thread_id(runtime_principal_uuid(principal_name), thread_id)
        .with_agent_scope(
            agent_id
                .and_then(|raw| SessionKey::normalize_agent_id(Some(raw)))
                .map(ToOwned::to_owned),
        )
        .to_string()
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MockEvent {
    kind: &'static str,
    credential: String,
    model: String,
    prefix_digest: String,
    cached_tokens: u64,
    hit: bool,
    synthetic: bool,
}

type CacheKey = (String, String, String);

#[derive(Clone)]
struct MockAnthropic {
    clock: Arc<TestClock>,
    cache: Arc<Mutex<HashMap<CacheKey, CacheEntry>>>,
    events: Arc<Mutex<Vec<MockEvent>>>,
}

#[derive(Clone, Copy)]
struct CacheEntry {
    last_access_ts: u64,
}

impl MockAnthropic {
    fn new(clock: Arc<TestClock>) -> Self {
        Self {
            clock,
            cache: Arc::new(Mutex::new(HashMap::new())),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn events(&self) -> Vec<MockEvent> {
        lock(&self.events).clone()
    }

    fn last(&self, kind: &'static str) -> MockEvent {
        lock(&self.events)
            .iter()
            .rev()
            .find(|event| event.kind == kind)
            .cloned()
            .expect("mock event exists")
    }
}

async fn mock_messages(State(mock): State<MockAnthropic>, request: Request) -> Response<Body> {
    assert_eq!(request.uri().path(), "/v1/messages");
    let credential = request
        .headers()
        .get("authorization")
        .or_else(|| request.headers().get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = request
        .into_body()
        .collect()
        .await
        .expect("mock body")
        .to_bytes();
    let value: Value = serde_json::from_slice(&body).expect("mock receives JSON");
    validate_messages_request(&value);

    let model = value.get("model").and_then(Value::as_str).expect("model");
    let system_text = value
        .get("system")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first())
        .and_then(|block| block.get("text"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let prefix_tokens = parse_tag(system_text, "tokens=")
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or_default();
    let synthetic = value.get("max_tokens").and_then(Value::as_u64) == Some(0);
    if synthetic {
        assert!(value.get("stream").is_none(), "keepalive removed stream");
        assert_eq!(value.get("max_tokens"), Some(&json!(0)));
        if let Some(thinking) = value.get("thinking") {
            assert_eq!(
                thinking.get("type").and_then(Value::as_str),
                Some("disabled")
            );
        }
    }

    let prefix_digest = prefix_digest(&value);
    let cache_key = (credential.clone(), model.to_owned(), prefix_digest.clone());
    let now = unix_secs(mock.clock.now());
    let mut cache = lock(&mock.cache);
    let (hit, cached_read, cached_create) = if let Some(entry) = cache.get_mut(&cache_key) {
        if now < entry.last_access_ts.saturating_add(CACHE_TTL) {
            entry.last_access_ts = now;
            (true, prefix_tokens, 0)
        } else {
            *entry = CacheEntry {
                last_access_ts: now,
            };
            (false, 0, prefix_tokens)
        }
    } else {
        cache.insert(
            cache_key,
            CacheEntry {
                last_access_ts: now,
            },
        );
        (false, 0, prefix_tokens)
    };
    drop(cache);

    lock(&mock.events).push(MockEvent {
        kind: if synthetic { "renewal" } else { "proxy" },
        credential,
        model: model.to_owned(),
        prefix_digest,
        cached_tokens: cached_read.max(cached_create),
        hit,
        synthetic,
    });

    let tail_text = value
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|arr| arr.last())
        .and_then(|msg| msg.get("content"))
        .and_then(Value::as_array)
        .and_then(|arr| arr.first())
        .and_then(|block| block.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stop_reason = parse_tag(tail_text, "stop=").unwrap_or("tool_use");
    let tool = parse_tag(tail_text, "tool=").unwrap_or("bash");
    let content = if stop_reason == "end_turn" {
        Vec::new()
    } else {
        vec![json!({
            "type": "tool_use",
            "id": format!("tool-{now}"),
            "name": tool,
            "input": {}
        })]
    };
    let response = json!({
        "id": format!("msg-{now}"),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": 1,
            "output_tokens": 1,
            "cache_read_input_tokens": cached_read,
            "cache_creation_input_tokens": cached_create
        }
    });
    (StatusCode::OK, Json(response)).into_response()
}

fn validate_messages_request(value: &Value) {
    let messages = value
        .get("messages")
        .and_then(Value::as_array)
        .expect("messages array");
    assert!(!messages.is_empty(), "messages must not be empty");
    assert_eq!(
        messages
            .last()
            .and_then(|message| message.get("role"))
            .and_then(Value::as_str),
        Some("user")
    );
    for (index, message) in messages.iter().enumerate() {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let uses = blocks(message)
            .into_iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
            .collect::<Vec<_>>();
        if uses.is_empty() {
            continue;
        }
        let next = messages
            .get(index + 1)
            .expect("tool_use has following user");
        assert_eq!(next.get("role").and_then(Value::as_str), Some("user"));
        let results = blocks(next)
            .into_iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
            .collect::<Vec<_>>();
        for use_block in uses {
            let id = use_block.get("id").and_then(Value::as_str);
            assert!(
                results
                    .iter()
                    .any(|result| result.get("tool_use_id").and_then(Value::as_str) == id)
            );
        }
    }
}

fn blocks(message: &Value) -> Vec<&Value> {
    match message.get("content") {
        Some(Value::Array(blocks)) => blocks.iter().collect(),
        _ => Vec::new(),
    }
}

fn parse_tag<'a>(input: &'a str, key: &str) -> Option<&'a str> {
    let start = input.find(key)? + key.len();
    let rest = &input[start..];
    let end = rest.find(';').unwrap_or(rest.len());
    Some(&rest[..end])
}

fn prefix_digest(value: &Value) -> String {
    let mut prefix = json!({
        "tools": value.get("tools").cloned().unwrap_or(Value::Null),
        "system": value.get("system").cloned().unwrap_or(Value::Null),
        "messages": []
    });
    let Some(messages) = value.get("messages").and_then(Value::as_array) else {
        return digest(&prefix);
    };
    let mut selected = Vec::new();
    for message in messages {
        let mut selected_message = message.clone();
        let explicit_message_breakpoint = message.get("cache_control").is_some();
        let block_breakpoint =
            message
                .get("content")
                .and_then(Value::as_array)
                .and_then(|content| {
                    content
                        .iter()
                        .rposition(|block| block.get("cache_control").is_some())
                });
        if let Some(last_block) = block_breakpoint
            && let Some(content) = selected_message
                .get_mut("content")
                .and_then(Value::as_array_mut)
        {
            content.truncate(last_block + 1);
        }
        selected.push(selected_message);
        if explicit_message_breakpoint || block_breakpoint.is_some() {
            break;
        }
    }
    prefix["messages"] = Value::Array(selected);
    digest(&prefix)
}

fn digest(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).expect("digest JSON");
    hex::encode(Sha256::digest(bytes))
}

struct Harness {
    principal_name: String,
    _dir: TempDir,
    _app: cc_lb_server::App,
    _proxy_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    proxy_addr: std::net::SocketAddr,
    storage: Arc<SqliteStorage>,
    scheduler_pool: sqlx::SqlitePool,
    clock: Arc<TestClock>,
    mock: MockAnthropic,
    client: Client,
    key: String,
}

impl Harness {
    async fn new(snapshot_max_bytes: u32) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let scheduler_sqlite_path = sqlite_path.with_extension("scheduler.sqlite");
        let clock = Arc::new(TestClock::new_at_secs(NOW));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(
                &format!("sqlite://{}", sqlite_path.display()),
                clock.clone(),
            )
            .await
            .expect("open sqlite"),
        );
        storage.initialize().await.expect("initialize sqlite");
        let mock = MockAnthropic::new(clock.clone());
        let mock_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock listener");
        let mock_addr = mock_listener.local_addr().expect("mock address");
        let mock_router = Router::new()
            .route("/v1/messages", post(mock_messages))
            .with_state(mock.clone());
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_router)
                .await
                .expect("mock server")
        });

        let aead = Arc::new(AeadService::from_master_key([0; 32]));
        let upstream_id = Uuid::new_v4();
        let tokens = OAuthTokenBundle {
            access_token: "sk-ant-oat-test".to_owned(),
            refresh_token: "sk-ant-ort-test".to_owned(),
            expires_at_unix_secs: NOW + 86_400,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: vec!["messages".to_owned()],
            never_refresh: true,
        };
        let encrypted =
            cc_lb_aead::EncryptedOAuthTokens::encrypt(&aead, &tokens, upstream_id.as_bytes())
                .expect("encrypt oauth tokens");
        let upstream = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: upstream_id,
                name: "real-session-upstream".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse(&format!("http://{mock_addr}/")).expect("mock URL")),
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens: encrypted,
                    never_refresh: true,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("create OAuth upstream");
        let principal_name = PRINCIPAL_NAME.to_owned();
        let principal = PrincipalCreate {
            name: principal_name.clone(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: vec![upstream.id],
            default_limits: Vec::new(),
            cache_keepalive: Some(CacheKeepaliveConfig {
                enabled: true,
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 3,
                max_total_duration_secs: 1_800,
                snapshot_max_bytes,
                min_cache_tokens: 10_000,
                classifier: Default::default(),
            }),
        };
        PrincipalStore::create(storage.as_ref(), principal, NOW)
            .await
            .expect("create principal");
        let key_store = KeyStore::new(storage.clone());
        let (_record, secret) = key_store
            .create(
                &principal_name,
                CreateParams {
                    label: "real-session".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("create managed key");

        let mut config = Config {
            storage: StorageConfig::Sqlite { path: sqlite_path },
            ..Default::default()
        };
        config.price_catalog.url = "http://127.0.0.1:9/price-catalog.json".to_owned();
        config.body.messages_cap_bytes = 12 * 1024 * 1024;
        config.listener.proxy_addr = "127.0.0.1:0".parse().expect("proxy address");
        config.listener.admin_addr = "127.0.0.1:0".parse().expect("admin address");
        let app = build_app_with_storage(
            config,
            None,
            storage.clone(),
            storage.clone(),
            aead,
            clock.clone(),
        )
        .await
        .expect("build app");
        let scheduler_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "sqlite://{}?mode=rwc",
                scheduler_sqlite_path.display()
            ))
            .await
            .expect("open scheduler sqlite");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let proxy_addr = listener.local_addr().expect("proxy address");
        let router = app.router.clone();
        let proxy_task = tokio::spawn(async move { axum::serve(listener, router).await });
        sleep(Duration::from_millis(20)).await;
        Self {
            principal_name,
            _dir: dir,
            _app: app,
            _proxy_task: proxy_task,
            proxy_addr,
            storage,
            scheduler_pool,
            clock,
            mock,
            client: Client::new(),
            key: secret.expose().to_owned(),
        }
    }

    async fn post(&self, body: Vec<u8>, session: &str, agent: Option<&str>) -> Value {
        let mut request = self
            .client
            .post(format!("http://{}/v1/messages", self.proxy_addr))
            .header("x-api-key", &self.key)
            .header("anthropic-version", "2023-06-01")
            .header("x-claude-code-session-id", session)
            .body(body);
        if let Some(agent) = agent {
            request = request.header("x-claude-code-agent-id", agent);
        }
        let response = request.send().await.expect("proxy request");
        assert_eq!(response.status(), StatusCode::OK);
        response.json().await.expect("proxy JSON")
    }

    fn session_key_hash(&self, thread_id: &str, agent_id: Option<&str>) -> String {
        runtime_session_key_hash(&self.principal_name, thread_id, agent_id)
    }

    async fn wait_for_session_count(&self, status: &str, min_count: i64) {
        for _ in 0..200 {
            let query = if status == "cancelled" {
                "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE status = 'terminal' AND terminal_reason = 'cancelled'"
            } else {
                "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE status = 'active' AND enqueue_state = 'enqueued'"
            };
            let count = sqlx::query_scalar::<_, i64>(query)
                .fetch_one(self.storage.pool())
                .await
                .expect("query session count");
            if count >= min_count {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {status} session count >= {min_count}");
    }

    async fn wait_for_enqueued_generation(&self, session_key_hash: &str, generation: i64) {
        for _ in 0..200 {
            let present = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE session_key_hash = ? AND generation = ? AND status = 'active' AND enqueue_state = 'enqueued'",
            )
            .bind(session_key_hash)
            .bind(generation)
            .fetch_one(self.storage.pool())
            .await
            .expect("query session generation");
            if present > 0 {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for session {session_key_hash} generation = {generation}");
    }

    async fn advance_and_dispatch_due(&self, delta: Duration) {
        self.clock.advance(delta);
        let now = i64::try_from(unix_secs(self.clock.now())).expect("now fits i64");
        sqlx::query(
            "UPDATE Jobs SET run_at = 0 WHERE job_type = 'cache_keepalive' AND status = 'Pending' AND run_at <= ?",
        )
        .bind(now)
        .execute(&self.scheduler_pool)
        .await
        .expect("release due cache_keepalive jobs");
        for _ in 0..200 {
            let due_remaining = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs WHERE job_type = 'cache_keepalive' AND status IN ('Pending', 'Running') AND run_at <= 0",
            )
            .fetch_one(&self.scheduler_pool)
            .await
            .expect("count due cache_keepalive jobs");
            if due_remaining == 0 {
                settle().await;
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for due cache_keepalive jobs to finish");
    }
}

fn request_body(
    target_size: usize,
    prefix_label: &str,
    prefix_tokens: u64,
    tool: &str,
    stop_reason: &str,
    oversized_prefix: bool,
) -> Vec<u8> {
    let prefix = if oversized_prefix {
        format!(
            "tokens={prefix_tokens};{}",
            "p".repeat(target_size.saturating_sub(2_000))
        )
    } else {
        format!("tokens={prefix_tokens};stable cached prefix {prefix_label}")
    };
    let tail_directive = format!("tool={tool};stop={stop_reason};");
    let mut value = json!({
        "model": "claude-opus-5-5",
        "max_tokens": 256,
        "stream": false,
        "thinking": {"type": "enabled", "budget_tokens": 1024},
        "tools": [{"name": "bash", "description": "test tool", "input_schema": {"type": "object"}}],
        "system": [{"type": "text", "text": prefix, "cache_control": {"type": "ephemeral", "ttl": "5m"}}],
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "cached breakpoint", "cache_control": {"type": "ephemeral", "ttl": "5m"}}, {"type": "text", "text": ""}]},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "prior-tool", "name": "bash", "input": {}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "prior-tool", "content": tail_directive}]}
        ]
    });
    let base = serde_json::to_vec(&value).expect("request JSON").len();
    if !oversized_prefix {
        let padding = target_size.saturating_sub(base);
        value["messages"][0]["content"][1]["text"] = Value::String("x".repeat(padding));
    }
    serde_json::to_vec(&value).expect("request body")
}

async fn settle() {
    for _ in 0..20 {
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn sim_principal_a_cap_crossing_long_gap_and_ask_tool() {
    let harness = Harness::new(8_388_608).await;
    let session = "00000000-0000-7000-8000-000000000001";
    let session_key_hash = harness.session_key_hash(session, None);
    let t1 = harness
        .post(
            request_body(355_453, "principal-a-t1", 83_900, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(t1["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;
    let t2 = harness
        .post(
            request_body(583_888, "principal-a-t2", 91_366, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(t2["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 2)
        .await;
    let t3 = harness
        .post(
            request_body(
                3_368_863,
                "principal-a-gap",
                364_072,
                "wait",
                "tool_use",
                false,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(t3["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 3)
        .await;
    for _ in 0..3 {
        harness
            .advance_and_dispatch_due(Duration::from_secs(270))
            .await;
    }
    harness
        .advance_and_dispatch_due(Duration::from_secs(128))
        .await;
    let t4 = harness
        .post(
            request_body(
                3_370_380,
                "principal-a-gap",
                364_072,
                "bash",
                "tool_use",
                false,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(t4["usage"]["cache_read_input_tokens"], 364_072);
    settle().await;
    let ask = harness
        .post(
            request_body(
                2_902_959,
                "principal-a-ask",
                215_118,
                "ask",
                "tool_use",
                false,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(ask["stop_reason"], "tool_use");
    harness.wait_for_session_count("cancelled", 1).await;

    let events = harness.mock.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.synthetic && event.hit)
            .count(),
        3
    );
    assert!(!harness.mock.last("proxy").hit);
    assert_eq!(harness.mock.last("proxy").cached_tokens, 215_118);
    let renewal_events = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM request_events_v1 WHERE source_kind = 'renewal'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("renewal request events");
    assert_eq!(renewal_events, 3);
    let renewal_turns = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cache_keepalive_turns WHERE hit_miss = 'hit'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("renewal turn projections");
    assert_eq!(renewal_turns, 3);
    let sessions = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE status = 'terminal' AND terminal_reason = 'cancelled'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("cancelled session row");
    assert!(sessions >= 1);
}

#[tokio::test]
async fn sim_principal_b_multi_agent_interleaving_and_4mb_gap() {
    let harness = Harness::new(8_388_608).await;
    let session = "00000000-0000-4000-8000-000000000002";
    let parent = harness
        .post(
            request_body(689_605, "parent", 267_130, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(parent["stop_reason"], "tool_use");
    harness.wait_for_session_count("active", 1).await;
    let small = harness
        .post(
            request_body(947, "agent-small", 0, "bash", "tool_use", false),
            session,
            Some("subagent-worker-01"),
        )
        .await;
    assert_eq!(small["stop_reason"], "tool_use");
    settle().await;
    let agent = harness
        .post(
            request_body(298_803, "agent", 95_637, "bash", "tool_use", false),
            session,
            Some("subagent-worker-01"),
        )
        .await;
    assert_eq!(agent["stop_reason"], "tool_use");
    harness.wait_for_session_count("active", 2).await;
    let done = harness
        .post(
            request_body(299_403, "agent-end", 95_637, "bash", "end_turn", false),
            session,
            Some("subagent-worker-01"),
        )
        .await;
    assert_eq!(done["stop_reason"], "end_turn");
    harness.wait_for_session_count("cancelled", 1).await;
    let active_after_agent = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE status = 'active'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("active parent session");
    assert!(active_after_agent >= 1);
    let parent_gap = harness
        .post(
            request_body(4_182_148, "parent-gap", 598_453, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(parent_gap["stop_reason"], "tool_use");
    let parent_session_key_hash = harness.session_key_hash(session, None);
    harness
        .wait_for_enqueued_generation(&parent_session_key_hash, 2)
        .await;
    for _ in 0..3 {
        harness
            .advance_and_dispatch_due(Duration::from_secs(270))
            .await;
    }
    harness
        .advance_and_dispatch_due(Duration::from_secs(80))
        .await;
    let resumed = harness
        .post(
            request_body(4_182_731, "parent-gap", 598_453, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(resumed["usage"]["cache_read_input_tokens"], 598_453);
    let haiku = harness
        .post(
            request_body(100_000, "haiku", 3_547, "bash", "tool_use", false),
            session,
            Some("haiku-agent"),
        )
        .await;
    assert_eq!(haiku["stop_reason"], "tool_use");
    settle().await;
    let oversized = harness
        .post(
            request_body(
                9 * 1024 * 1024,
                "oversized",
                700_000,
                "bash",
                "tool_use",
                true,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(oversized["stop_reason"], "tool_use");
    harness.wait_for_session_count("cancelled", 2).await;
    settle().await;

    let events = harness.mock.events();
    assert!(
        events
            .iter()
            .any(|event| event.cached_tokens == 598_453 && !event.hit)
    );
    assert!(
        events
            .iter()
            .filter(|event| event.synthetic && event.hit)
            .count()
            >= 3
    );
    assert!(
        events
            .iter()
            .any(|event| event.cached_tokens == 598_453 && event.hit)
    );
    let cancelled = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE status = 'terminal' AND terminal_reason = 'cancelled'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("cancelled rows");
    assert!(cancelled >= 2);
    let small_prefix_decisions = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cache_keepalive_decisions WHERE reason LIKE 'cached prefix too small%'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("small-prefix decision");
    assert!(small_prefix_decisions >= 1);
    let oversized_decisions = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cache_keepalive_decisions WHERE reason = 'snapshot too large'",
    )
    .fetch_one(harness.storage.pool())
    .await
    .expect("oversized decision");
    assert!(oversized_decisions >= 1);
}

#[test]
fn session_key_agent_normalization_matches_real_headers() {
    let principal = Uuid::from_u128(7);
    assert_eq!(SessionKey::normalize_agent_id(Some(" main ")), None);
    assert_eq!(
        SessionKey::normalize_agent_id(Some(" subagent-worker-01 ")),
        Some("subagent-worker-01")
    );
    assert_ne!(
        SessionKey::from_thread_id(principal, "00000000-0000-4000-8000-000000000002"),
        SessionKey::from_thread_and_agent(
            principal,
            "00000000-0000-4000-8000-000000000002",
            "subagent-worker-01"
        )
    );
}
