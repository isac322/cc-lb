#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Bytes;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::SubscriptionQuotaSink;
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_server::upstream_warmup_loop::UpstreamWarmupLoop;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamLeaseKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalStore, StorageResult,
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamSubscriptionQuotaStore, UpstreamUpdate,
};
use cc_lb_storage_redb::Storage;
use chrono::{DateTime, TimeZone, Utc};
use fake_anthropic::{
    AppConfig, MessageScript, RecordedMessageRequest, ScriptedMessageResponse,
    app as fake_anthropic_app,
};
use http::StatusCode;
use http::header::LOCATION;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::MakeWriter;
use url::Url;
use uuid::Uuid;

const MASTER_KEY: [u8; 32] = [31; 32];

pub struct WarmupFixture {
    _dir: tempfile::TempDir,
    pub storage: Arc<Storage>,
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub fake: RunningFakeAnthropic,
    pub replica_id: Uuid,
    clock: Arc<AtomicI64>,
    subscription_quota_sink: SubscriptionQuotaSink,
    _quota_receiver: Receiver<SubscriptionQuotaObservationRecord>,
}

impl WarmupFixture {
    pub async fn new() -> Self {
        let fake = RunningFakeAnthropic::spawn().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(
            Storage::open(&dir.path().join("upstream-warmup.redb"), MASTER_KEY)
                .expect("redb storage opens"),
        );
        let clock = Arc::new(AtomicI64::new(real_now_secs()));
        let upstreams = Arc::new(TestClockUpstreamStore::new(storage.clone(), clock.clone()));
        let stores = Arc::new(Stores {
            upstreams,
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage.clone()),
            plugin_registry_repo: None,
        });
        let aead = Arc::new(AeadService::from_master_key(MASTER_KEY));
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse(&format!("{}/oauth/authorize", fake.base_url)).expect("auth url"),
            token_url: Url::parse(&format!("{}/oauth/token", fake.base_url)).expect("token url"),
            redirect_uri: Url::parse("http://localhost/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        let (subscription_quota_sink, quota_receiver) = SubscriptionQuotaSink::with_capacity(256);

        Self {
            _dir: dir,
            storage,
            stores,
            aead,
            oauth_cfg,
            fake,
            replica_id: Uuid::new_v4(),
            clock,
            subscription_quota_sink,
            _quota_receiver: quota_receiver,
        }
    }

    pub fn now_unix_secs(&self) -> i64 {
        self.clock.load(Ordering::SeqCst)
    }

    pub fn set_now_unix_secs(&self, now: i64) {
        self.clock.store(now, Ordering::SeqCst);
    }

    pub async fn create_due_oauth_upstream(&self, name: &str) -> Uuid {
        self.create_oauth_upstream(name, Some(self.now_unix_secs()))
            .await
    }

    pub async fn create_oauth_upstream(&self, name: &str, next_warmup_at: Option<i64>) -> Uuid {
        let tokens = initial_tokens(&self.fake.base_url).await;
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse(&self.fake.base_url).expect("fake base url")),
                api_key_ciphertext: None,
                warmup_enabled: true,
                next_warmup_at: next_warmup_at.map(unix_datetime),
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created");
        let encrypted = encrypted(
            &self.aead,
            record.id,
            &OAuthTokenBundle {
                access_token: tokens.access_token,
                refresh_token: tokens.refresh_token,
                expires_at_unix_secs: u64::try_from(self.now_unix_secs().saturating_add(10_000))
                    .expect("positive test time"),
                scopes: vec!["messages".to_owned()],
            },
        );
        self.storage
            .store_oauth_tokens(record.id, record.revision, encrypted)
            .await
            .expect("tokens stored");
        record.id
    }

    pub async fn create_principal(&self, name: &str) {
        PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            u64::try_from(self.now_unix_secs()).expect("positive test time"),
        )
        .await
        .expect("principal created");
    }

    pub async fn put_five_hour_observation(&self, upstream_id: Uuid, resets_at_unix_secs: i64) {
        let observed_at_unix_millis = u64::try_from(self.now_unix_secs())
            .expect("positive test time")
            .saturating_mul(1_000);
        let record = SubscriptionQuotaObservationRecord {
            upstream_id,
            window: SubscriptionQuotaWindow::FiveHour,
            source: SubscriptionQuotaSource::Api,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis,
            sample_id: Uuid::new_v4(),
            utilization: Some(0.0),
            status: Some(SubscriptionQuotaStatus::Allowed),
            resets_at_unix_secs: Some(
                u64::try_from(resets_at_unix_secs).expect("positive reset timestamp"),
            ),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        };
        self.storage
            .put_subscription_quota(&record)
            .await
            .expect("quota observation inserted");
    }

    pub async fn upstream_record(&self, upstream_id: Uuid) -> UpstreamRecord {
        UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .expect("get upstream")
            .expect("upstream exists")
    }

    pub async fn set_next_warmup_at(&self, upstream_id: Uuid, next_warmup_at: i64) {
        let record = self.upstream_record(upstream_id).await;
        UpstreamStore::update(
            self.storage.as_ref(),
            upstream_id,
            record.revision,
            UpstreamUpdate {
                next_warmup_at: Some(unix_datetime(next_warmup_at)),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("next_warmup_at updated");
    }

    pub async fn set_last_warmup_cycle_key(&self, upstream_id: Uuid, cycle_key: i64) {
        let record = self.upstream_record(upstream_id).await;
        UpstreamStore::update(
            self.storage.as_ref(),
            upstream_id,
            record.revision,
            UpstreamUpdate {
                last_warmup_cycle_key: Some(cycle_key),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("last_warmup_cycle_key updated");
    }

    pub async fn scan_once(&self) {
        self.scan_once_with_replica(self.replica_id).await;
    }

    pub async fn scan_once_with_replica(&self, replica_id: Uuid) {
        let cancel = CancellationToken::new();
        let loop_handle = self.warmup_loop(replica_id, cancel.clone());
        let task = tokio::spawn(async move { loop_handle.scan_and_fire_once(&cancel).await });
        task.await
            .expect("warmup scan task")
            .expect("warmup scan succeeds");
    }

    pub fn warmup_loop(
        &self,
        replica_id: Uuid,
        refresh_cancel: CancellationToken,
    ) -> Arc<UpstreamWarmupLoop> {
        let lazy_refresher = Arc::new(LazyRefresher::new(
            self.stores.clone(),
            self.aead.clone(),
            self.oauth_cfg.clone(),
            replica_id,
            None,
            refresh_cancel,
        ));
        let mut warmup_loop = UpstreamWarmupLoop::new(
            self.stores.clone(),
            self.aead.clone(),
            lazy_refresher,
            self.subscription_quota_sink.clone(),
            replica_id,
            None,
            self._dir.path().to_path_buf(),
        );
        warmup_loop.set_jitter_enabled(false);
        Arc::new(warmup_loop)
    }

    pub async fn refresh_history_len(&self) -> usize {
        refresh_history_len(&self.fake.base_url).await
    }
}

pub async fn wait_for_message_count(script: &MessageScript, expected: usize) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if script.request_count() >= expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    script.request_count() >= expected
}

pub fn unix_datetime(unix_secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(unix_secs, 0)
        .single()
        .expect("valid unix timestamp")
}

pub fn ok_response() -> ScriptedMessageResponse {
    ScriptedMessageResponse::ok()
}

pub fn delayed_ok_response(delay: Duration) -> ScriptedMessageResponse {
    ScriptedMessageResponse::ok().with_delay(delay)
}

pub fn error_response(status: StatusCode) -> ScriptedMessageResponse {
    let (error_type, message) = match status {
        StatusCode::UNAUTHORIZED => ("authentication_error", "forced warmup unauthorized"),
        StatusCode::FORBIDDEN => ("permission_error", "forced warmup forbidden"),
        StatusCode::BAD_REQUEST => ("invalid_request_error", "forced warmup bad request"),
        StatusCode::NOT_FOUND => ("not_found_error", "forced warmup not found"),
        StatusCode::TOO_MANY_REQUESTS => ("rate_limit_error", "forced warmup rate limit"),
        _ => ("api_error", "forced warmup error"),
    };
    ScriptedMessageResponse::error(status, error_type, message)
}

pub fn active_rate_limit_response(resets_at_unix_secs: i64) -> ScriptedMessageResponse {
    error_response(StatusCode::TOO_MANY_REQUESTS).with_header(
        "anthropic-ratelimit-unified-5h-reset",
        resets_at_unix_secs.to_string(),
    )
}

pub fn assert_locked_warmup_request(request: &RecordedMessageRequest) {
    assert_eq!(request.body_json["model"], "claude-haiku-4-5-20251001");
    assert_eq!(request.body_json["max_tokens"], 1);
    assert_eq!(
        request.body_json["messages"],
        serde_json::json!([{ "role": "user", "content": "." }])
    );
    for forbidden in ["system", "metadata", "tools", "stream", "thinking"] {
        assert!(
            request.body_json.get(forbidden).is_none(),
            "warmup body unexpectedly contained {forbidden}: {:?}",
            request.body_json
        );
    }
    assert!(
        request
            .headers
            .get("authorization")
            .is_some_and(|value| value.starts_with("Bearer sk-ant-oat01-")),
        "authorization header was not an Anthropic OAuth bearer token: {:?}",
        request.headers
    );
    assert_eq!(
        request.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(
        request.headers.get("anthropic-version").map(String::as_str),
        Some("2023-06-01")
    );
    assert_eq!(
        request.headers.get("anthropic-beta").map(String::as_str),
        Some("oauth-2025-04-20")
    );
    for forbidden in ["x-app", "user-agent", "idempotency-key"] {
        assert!(
            !request.headers.contains_key(forbidden),
            "warmup request must not include {forbidden}: {:?}",
            request.headers
        );
    }
}

pub fn assert_next_warmup_after_cycle(record: &UpstreamRecord, cycle_key: i64) {
    let next = record
        .next_warmup_at
        .expect("next warmup timestamp should be set")
        .timestamp();
    assert!(
        next >= cycle_key + 5 * 60 * 60 + 30,
        "next_warmup_at={next}, cycle_key={cycle_key}"
    );
    assert!(
        next < cycle_key + 5 * 60 * 60 + 61,
        "next_warmup_at={next}, cycle_key={cycle_key}"
    );
}

pub struct RunningFakeAnthropic {
    pub addr: SocketAddr,
    pub base_url: String,
    pub messages: MessageScript,
    task: JoinHandle<()>,
}

impl RunningFakeAnthropic {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake anthropic");
        let addr = listener.local_addr().expect("fake anthropic addr");
        let messages = MessageScript::new();
        let app = fake_anthropic_app(AppConfig {
            message_script: Some(messages.clone()),
            ..AppConfig::default()
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("fake anthropic server")
        });
        Self {
            addr,
            base_url: format!("http://{addr}"),
            messages,
            task,
        }
    }
}

impl Drop for RunningFakeAnthropic {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct TestClockUpstreamStore {
    inner: Arc<Storage>,
    now: Arc<AtomicI64>,
}

impl TestClockUpstreamStore {
    fn new(inner: Arc<Storage>, now: Arc<AtomicI64>) -> Self {
        Self { inner, now }
    }
}

#[async_trait]
impl UpstreamStore for TestClockUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        UpstreamStore::create(self.inner.as_ref(), create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_name(self.inner.as_ref(), name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(self.inner.as_ref(), id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        UpstreamStore::list(self.inner.as_ref(), after, limit).await
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update(self.inner.as_ref(), id, expected_revision, update).await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::set_enabled(self.inner.as_ref(), id, expected_revision, enabled).await
    }


    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_spec(self.inner.as_ref(), id, expected_revision, update).await
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_api_key_secret(self.inner.as_ref(), id, api_key_ciphertext).await
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_oauth_token(self.inner.as_ref(), id, tokens).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        UpstreamStore::set_status(self.inner.as_ref(), id, status).await
    }

    async fn claim_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_lease(self.inner.as_ref(), id, lease_kind, holder, ttl_secs).await
    }

    async fn renew_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        UpstreamStore::renew_lease(self.inner.as_ref(), id, lease_kind, holder, ttl_secs).await
    }

    async fn release_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
    ) -> StorageResult<bool> {
        UpstreamStore::release_lease(self.inner.as_ref(), id, lease_kind, holder).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::store_oauth_tokens(self.inner.as_ref(), id, expected_revision, tokens).await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_refresh_lease(self.inner.as_ref(), id, holder, ttl_secs).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(self.inner.as_ref(), id, holder, tokens).await
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        UpstreamStore::release_lease_on_failure(self.inner.as_ref(), id, holder, reason).await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        UpstreamStore::set_last_apply_error(self.inner.as_ref(), id, error).await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        UpstreamStore::soft_delete(self.inner.as_ref(), id, expected_revision).await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        UpstreamStore::hard_delete(self.inner.as_ref(), id).await
    }

    async fn claim_warmup_lease(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_warmup_lease(self.inner.as_ref(), upstream_id, holder, ttl_secs).await
    }

    async fn write_warmup_cycle_key(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool> {
        UpstreamStore::write_warmup_cycle_key(
            self.inner.as_ref(),
            upstream_id,
            holder,
            new_cycle_key,
            next_warmup_at,
        )
        .await
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        UpstreamStore::release_warmup_lease(self.inner.as_ref(), id, holder).await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::clear_warmup_dialect_plugin(self.inner.as_ref(), id, expected_revision).await
    }

    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        Ok(self.now.load(Ordering::SeqCst))
    }

    async fn write_warmup_next_at(
        &self,
        upstream_id: Uuid,
        holder: &str,
        next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        UpstreamStore::write_warmup_next_at(
            self.inner.as_ref(),
            upstream_id,
            holder,
            next_warmup_at,
        )
        .await
    }
}

#[derive(Deserialize)]
struct InitialTokens {
    access_token: String,
    refresh_token: String,
}

async fn initial_tokens(base: &str) -> InitialTokens {
    let verifier = "verifier";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut authorize_url = Url::parse(&format!("{base}/oauth/authorize")).expect("authorize url");
    authorize_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", "test-client")
        .append_pair("redirect_uri", "http://localhost/callback")
        .append_pair("code_challenge", challenge.as_str())
        .append_pair("code_challenge_method", "S256");
    let authorize = raw_http("GET", authorize_url.as_str(), &[], &[])
        .await
        .expect("authorize");
    assert!(
        authorize.status.is_redirection(),
        "authorize status {}",
        authorize.status
    );
    let location = authorize
        .headers
        .get(LOCATION)
        .expect("location")
        .to_str()
        .expect("location str");
    let code = Url::parse(location)
        .expect("location url")
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .expect("code");
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "authorization_code");
    serializer.append_pair("client_id", "test-client");
    serializer.append_pair("redirect_uri", "http://localhost/callback");
    serializer.append_pair("code", &code);
    serializer.append_pair("code_verifier", verifier);
    let body = serializer.finish();
    let token = raw_http(
        "POST",
        &format!("{base}/oauth/token"),
        &[("content-type", "application/x-www-form-urlencoded")],
        body.as_bytes(),
    )
    .await
    .expect("token");
    assert!(token.status.is_success(), "token status {}", token.status);
    serde_json::from_slice(&token.body).expect("token json")
}

async fn refresh_history_len(base: &str) -> usize {
    let response = raw_http("GET", &format!("{base}/__refresh_history"), &[], &[])
        .await
        .expect("history");
    let body: Value = serde_json::from_slice(&response.body).expect("history json");
    body["refreshes"].as_array().expect("refreshes").len()
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

struct RawHttpResponse {
    status: StatusCode,
    headers: http::HeaderMap,
    body: Bytes,
}

async fn raw_http(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<RawHttpResponse> {
    let url = Url::parse(url).expect("test url");
    let host = url.host_str().expect("test url host");
    let port = url.port_or_known_default().expect("test url port");
    let mut target = url.path().to_owned();
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    let authority = if url.port().is_some() {
        format!("{host}:{port}")
    } else {
        host.to_owned()
    };
    let mut request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let mut stream = TcpStream::connect((host, port)).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    parse_raw_response(&bytes)
}

fn parse_raw_response(bytes: &[u8]) -> std::io::Result<RawHttpResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing headers"))?;
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid status"))?;
    let mut headers = http::HeaderMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = http::header::HeaderName::from_bytes(name.trim().as_bytes())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            let value = http::HeaderValue::from_str(value.trim())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            headers.insert(name, value);
        }
    }
    let body = &bytes[header_end + 4..];
    let body = if headers
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(body)?
    } else {
        body.to_vec()
    };
    Ok(RawHttpResponse {
        status,
        headers,
        body: Bytes::from(body),
    })
}

fn decode_chunked(mut bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk size"))?;
        let size_text = std::str::from_utf8(&bytes[..line_end])
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        bytes = &bytes[line_end + 2..];
        if size == 0 {
            return Ok(decoded);
        }
        if bytes.len() < size + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk body",
            ));
        }
        decoded.extend_from_slice(&bytes[..size]);
        bytes = &bytes[size + 2..];
    }
}

fn real_now_secs() -> i64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    i64::try_from(secs).unwrap_or(i64::MAX)
}

#[derive(Clone, Default)]
pub struct CapturedLogs {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    pub fn contents(&self) -> String {
        let bytes = self.inner.lock().expect("captured logs lock");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

pub struct LogCapture {
    logs: CapturedLogs,
    _guard: tracing::dispatcher::DefaultGuard,
}

impl LogCapture {
    pub fn contents(&self) -> String {
        self.logs.contents()
    }
}

pub fn capture_logs() -> LogCapture {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    LogCapture {
        logs,
        _guard: guard,
    }
}

pub struct CapturedWriter {
    logs: CapturedLogs,
}

impl Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.logs
            .inner
            .lock()
            .expect("captured logs lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter { logs: self.clone() }
    }
}

pub fn request_headers(request: &RecordedMessageRequest) -> &BTreeMap<String, String> {
    &request.headers
}
