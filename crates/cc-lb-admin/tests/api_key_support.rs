//! Harness for API-key upstream admin tests: the in-process admin router
//! with its org API origin injected as a loopback fake-anthropic (D-FXGAP1),
//! a SQLite store, and a pinned clock. Nothing dials a non-loopback host:
//! upstreams created through the harness point `base_url` at the fake.
#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, header};
use cc_lb_admin::{AdminState, router};
use cc_lb_clock::TestClock;
use cc_lb_config::Config;
use cc_lb_control::anthropic_api_key::AnthropicOrgApi;
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyUsagePollOutcome, AuditEntry, AuditQueryScope, AuditStore,
    UpstreamApiKeyDetailsRecord, UpstreamApiKeyStore, UpstreamCreate, UpstreamKind, UpstreamRecord,
    UpstreamStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use fake_anthropic::{AppConfig, KeyPolicies, KeyPolicy, RecordedRequest, RequestRecorder, app};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::net::TcpListener;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

use crate::admin_test_common;

/// 2026-10-15T12:00:00Z.
pub const HARNESS_NOW_UNIX_SECS: u64 = 1_792_065_600;
pub const ADMIN_TOKEN: &str = "test-token";
pub const UPSTREAMS: &str = "/admin/v1/upstreams";
pub const PREFLIGHT: &str = "/admin/v1/upstreams/preflight";

pub struct ApiKeyHarness {
    pub app: axum::Router,
    pub state: AdminState,
    pub storage: Arc<SqliteStorage>,
    /// `http://127.0.0.1:<port>/`: both the org API origin and the default
    /// `base_url` of upstreams created through the harness.
    pub fake_url: Url,
    pub policies: KeyPolicies,
    pub recorder: RequestRecorder,
    pub clock: Arc<TestClock>,
    _dir: tempfile::TempDir,
    _server: tokio::task::JoinHandle<()>,
}

pub struct HarnessResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// `Null` when the body is empty or not JSON.
    pub body: Value,
}

impl HarnessResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    /// `ETag` value for a follow-up `If-Match`.
    pub fn etag(&self) -> String {
        self.header("etag").unwrap_or_default().to_owned()
    }

    pub fn id(&self) -> Uuid {
        self.body["id"]
            .as_str()
            .and_then(|id| id.parse().ok())
            .unwrap_or_default()
    }
}

/// Fake-anthropic on a loopback port plus an admin app wired to it.
pub async fn spawn_api_key_harness() -> ApiKeyHarness {
    let config = AppConfig::default();
    let policies = config.key_policies.clone();
    let recorder = config.recorder.clone();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback fake-anthropic");
    let addr = listener.local_addr().expect("fake-anthropic addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app(config)).await;
    });
    let fake_url = Url::parse(&format!("http://{addr}/")).expect("loopback url parses");

    let clock = Arc::new(TestClock::new_at_secs(HARNESS_NOW_UNIX_SECS));
    let dir = tempfile::tempdir().expect("storage dir");
    let storage =
        admin_test_common::sqlite_storage_with_clock(dir.path(), "api_key.sqlite", clock.clone())
            .await;
    let config = Config::default();
    let state = AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage.clone())),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([7; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        dynamic_view_rebinder: None,
        config: Arc::new(config),
        scheduler: None,
        admin_auth: admin_test_common::static_token_auth(ADMIN_TOKEN),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: clock.clone(),
        anthropic_org_api: AnthropicOrgApi::with_origin(fake_url.clone()),
    };
    ApiKeyHarness {
        app: router(state.clone()),
        state,
        storage,
        fake_url,
        policies,
        recorder,
        clock,
        _dir: dir,
        _server: server,
    }
}

impl ApiKeyHarness {
    /// Authenticated request (`Bearer test-token`).
    pub async fn request(
        &self,
        method: &str,
        uri: &str,
        body: Option<Value>,
        if_match: Option<&str>,
    ) -> HarnessResponse {
        self.request_with_token(method, uri, body, if_match, Some(ADMIN_TOKEN))
            .await
    }

    /// Request with an arbitrary bearer token, or none.
    pub async fn request_with_token(
        &self,
        method: &str,
        uri: &str,
        body: Option<Value>,
        if_match: Option<&str>,
        token: Option<&str>,
    ) -> HarnessResponse {
        let body = body.map(|body| body.to_string());
        self.raw_request(method, uri, body, if_match, token).await
    }

    /// Request with a raw body string (non-JSON bodies, size limits).
    pub async fn raw_request(
        &self,
        method: &str,
        uri: &str,
        body: Option<String>,
        if_match: Option<&str>,
        token: Option<&str>,
    ) -> HarnessResponse {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        if body.is_some() {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
        }
        if let Some(if_match) = if_match {
            builder = builder.header(header::IF_MATCH, if_match);
        }
        let request = builder
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .expect("request builds");
        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("router answers");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        HarnessResponse {
            status,
            headers,
            body,
        }
    }

    /// `POST /admin/v1/upstreams` for an API key. `extra` (an object) is
    /// merged into the body; `base_url` defaults to the fake.
    pub async fn create_api_key_upstream(
        &self,
        name: &str,
        key: &str,
        extra: Value,
    ) -> HarnessResponse {
        let mut body = serde_json::json!({
            "name": name,
            "kind": "anthropic_api_key",
            "api_key_value": key,
            "base_url": self.fake_url.as_str(),
        });
        if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            for (field, value) in extra {
                body.insert(field.clone(), value.clone());
            }
        }
        self.request("POST", UPSTREAMS, Some(body), None).await
    }

    /// `POST /admin/v1/upstreams/preflight` with `base_url` at the fake
    /// unless `extra` sets it.
    pub async fn preflight(&self, key: &str, extra: Value) -> HarnessResponse {
        let mut body = serde_json::json!({
            "api_key_value": key,
            "base_url": self.fake_url.as_str(),
        });
        if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            for (field, value) in extra {
                body.insert(field.clone(), value.clone());
            }
        }
        self.request("POST", PREFLIGHT, Some(body), None).await
    }

    /// `PUT /admin/v1/upstreams/{id}` with `If-Match`.
    pub async fn put(&self, id: Uuid, if_match: &str, body: Value) -> HarnessResponse {
        self.request(
            "PUT",
            &format!("{UPSTREAMS}/{id}"),
            Some(body),
            Some(if_match),
        )
        .await
    }

    pub async fn get_upstream(&self, id: Uuid) -> HarnessResponse {
        self.request("GET", &format!("{UPSTREAMS}/{id}"), None, None)
            .await
    }

    /// A pre-migration API-key upstream: the key sealed with AAD = id,
    /// `base_url` at the fake, and no details row.
    pub async fn seed_legacy_api_key_upstream(&self, name: &str, key: &str) -> UpstreamRecord {
        let created = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: Some(self.fake_url.clone()),
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("seed upstream");
        let ciphertext = self
            .state
            .aead
            .encrypt(key.as_bytes(), created.id.as_bytes())
            .expect("seal key");
        self.storage
            .update_api_key_secret(created.id, Some(ciphertext))
            .await
            .expect("store key")
    }

    /// Writes a capability snapshot at the upstream's current revision.
    pub async fn seed_details(&self, upstream_id: Uuid, access: ApiKeyAccessWrite) {
        let record = self.record(upstream_id).await;
        UpstreamApiKeyStore::write_api_key_access(
            self.storage.as_ref(),
            upstream_id,
            record.revision,
            &access,
        )
        .await
        .expect("seed details");
    }

    /// Writes a poll outcome (and its buckets) at the current revision.
    pub async fn seed_usage_buckets(&self, upstream_id: Uuid, outcome: ApiKeyUsagePollOutcome) {
        let record = self.record(upstream_id).await;
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            self.storage.as_ref(),
            upstream_id,
            record.revision,
            &outcome,
        )
        .await
        .expect("seed usage buckets");
    }

    pub fn set_policy(&self, key: &str, policy: KeyPolicy) {
        self.policies.set(key, policy);
    }

    /// The stored spec, including soft-deleted rows.
    pub async fn record(&self, upstream_id: Uuid) -> UpstreamRecord {
        UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .expect("read upstream")
            .expect("upstream exists")
    }

    pub async fn details(&self, upstream_id: Uuid) -> Option<UpstreamApiKeyDetailsRecord> {
        UpstreamApiKeyStore::get_api_key_details(self.storage.as_ref(), upstream_id)
            .await
            .expect("read details")
    }

    /// Live upstream specs.
    pub async fn upstream_count(&self) -> usize {
        UpstreamStore::list(self.storage.as_ref(), None, 1000)
            .await
            .expect("list upstreams")
            .len()
    }

    /// Recorded requests whose path starts with `prefix`.
    pub fn requests(&self, prefix: &str) -> Vec<RecordedRequest> {
        self.recorder
            .snapshot()
            .into_iter()
            .filter(|request| request.path.starts_with(prefix))
            .collect()
    }

    /// Every request the fake received.
    pub fn outbound_count(&self) -> usize {
        self.recorder.snapshot().len()
    }

    /// Directory holding the SQLite database files.
    pub fn storage_dir(&self) -> &std::path::Path {
        self._dir.path()
    }

    /// Audit rows, newest first.
    pub async fn audit_rows(&self) -> Vec<AuditEntry> {
        self.storage
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 1000, true)
            .await
            .expect("list audit")
    }
}
