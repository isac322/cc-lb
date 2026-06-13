#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_plugin_api::{
    Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect, shape_request,
};
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSigner, OAuthHttpClient, OAuthHttpError, OAuthTokenRequest, OAuthTokenResponse,
};
use cc_lb_storage_api::{
    ApiKeyStore, AuditEntry, AuditStore, BackendKind, ConfigDraftState, ConfigStore, HistoryEntry,
    HistorySummary, LimitStateStore, MetaStore, OAuthCredentialStore, OAuthCredentials,
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, QuotaStore, RequestEvent,
    RequestEventStore, StorageError, StorageResult, UsageRollup, UsageRollupResolution,
    UsageRollupRun, UsageRollupStore,
};
use http::header::USER_AGENT;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use oauth2::ClientId;
use secrecy::ExposeSecret;
use url::Url;

#[derive(Debug)]
pub struct FakeOAuthClient {
    pub calls: AtomicU32,
    responses: Mutex<VecDeque<OAuthTokenResponse>>,
    bodies: Mutex<Vec<String>>,
    response_delay: Option<Duration>,
}

impl FakeOAuthClient {
    pub fn new(responses: Vec<OAuthTokenResponse>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicU32::new(0),
            responses: Mutex::new(VecDeque::from(responses)),
            bodies: Mutex::new(Vec::new()),
            response_delay: None,
        })
    }

    pub fn with_response_delay(
        responses: Vec<OAuthTokenResponse>,
        response_delay: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicU32::new(0),
            responses: Mutex::new(VecDeque::from(responses)),
            bodies: Mutex::new(Vec::new()),
            response_delay: Some(response_delay),
        })
    }

    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::Relaxed)
    }

    pub fn bodies(&self) -> Vec<String> {
        self.bodies.lock().expect("bodies lock").clone()
    }
}

#[async_trait]
impl OAuthHttpClient for FakeOAuthClient {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bodies
            .lock()
            .expect("bodies lock")
            .push(request.form_body.expose_secret().to_owned());
        if let Some(response_delay) = self.response_delay {
            tokio::time::sleep(response_delay).await;
        }
        self.responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .ok_or_else(|| OAuthHttpError::Request {
                reason: "fake response queue empty".to_owned(),
            })
    }
}

#[derive(Default)]
pub struct MemoryStorage {
    oauth: Mutex<HashMap<(String, String), Vec<u8>>>,
    anthropic_api_keys: Mutex<HashMap<String, Vec<u8>>>,
}

pub struct TestStorage {
    pub storage: Arc<dyn OAuthCredentialStore>,
    pub aead: Arc<AeadService>,
}

impl TestStorage {
    pub async fn put_oauth(
        &self,
        principal_id: &str,
        provider: &str,
        creds: &OAuthCredentials,
    ) -> StorageResult<()> {
        let plaintext = serde_json::to_vec(creds)?;
        let ciphertext = self
            .aead
            .encrypt(&plaintext, &oauth_aad(principal_id, provider))
            .map_err(|source| StorageError::Aead(source.to_string()))?;
        self.storage
            .put_oauth_ciphertext(principal_id, provider, &ciphertext)
            .await
    }

    pub async fn get_oauth(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<OAuthCredentials>> {
        let Some(ciphertext) = self
            .storage
            .get_oauth_ciphertext(principal_id, provider)
            .await?
        else {
            return Ok(None);
        };
        let plaintext = self
            .aead
            .decrypt(&ciphertext, &oauth_aad(principal_id, provider))
            .map_err(|source| StorageError::Aead(source.to_string()))?;
        Ok(Some(serde_json::from_slice(&plaintext)?))
    }
}

pub fn storage() -> TestStorage {
    let storage = Arc::new(MemoryStorage::default());
    let storage: Arc<dyn OAuthCredentialStore> = storage;
    let aead = Arc::new(AeadService::from_master_key([0x42; 32]));
    TestStorage { storage, aead }
}

pub fn signer(
    storage: Arc<dyn OAuthCredentialStore>,
    aead: Arc<AeadService>,
    http: Arc<dyn OAuthHttpClient>,
) -> AnthropicOAuthSigner {
    AnthropicOAuthSigner::with_http(
        "alice",
        "anthropic_oauth",
        storage,
        aead,
        Url::parse("https://platform.claude.com/v1/oauth/token").expect("token url"),
        ClientId::new("client-test".to_owned()),
        http,
    )
}

pub fn creds(access_token: &str, refresh_token: &str, expires_at: u64) -> OAuthCredentials {
    OAuthCredentials {
        access_token: access_token.to_owned(),
        refresh_token: refresh_token.to_owned(),
        expires_at,
        scopes: vec!["messages".to_owned()],
    }
}

pub fn success_response(
    access_token: &str,
    refresh_token: Option<&str>,
    expires_in: u64,
) -> OAuthTokenResponse {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "access_token".to_owned(),
        serde_json::Value::String(access_token.to_owned()),
    );
    if let Some(refresh_token) = refresh_token {
        fields.insert(
            "refresh_token".to_owned(),
            serde_json::Value::String(refresh_token.to_owned()),
        );
    }
    fields.insert(
        "expires_in".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(expires_in)),
    );
    fields.insert(
        "scope".to_owned(),
        serde_json::Value::String("messages files".to_owned()),
    );
    OAuthTokenResponse {
        status: StatusCode::OK,
        body: Bytes::from(serde_json::Value::Object(fields).to_string()),
    }
}

pub fn failure_response() -> OAuthTokenResponse {
    OAuthTokenResponse {
        status: StatusCode::BAD_REQUEST,
        body: Bytes::from_static(br#"{"error":"invalid_grant"}"#),
    }
}

pub fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs()
}

pub fn shaped_request() -> ShapedRequest {
    let ctx = RequestContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::OAuthSubject,
        claims: serde_json::Map::new(),
    };
    shape_request(&DirectDialect, &ctx, &Upstream::AnthropicDirect { base_url: None }, &principal)
        .expect("shape request")
}

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("test-agent"));
        Ok(builder.shaped_request(
            Url::parse("https://api.anthropic.com/v1/messages").expect("request url"),
            Method::POST,
            headers,
            Bytes::from_static(br#"{"model":"claude-test"}"#),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

pub fn unauthorized_error() -> cc_lb_plugin_api::UpstreamError {
    cc_lb_plugin_api::UpstreamError::Unauthorized {
        status: StatusCode::UNAUTHORIZED,
        body: None,
    }
}

fn oauth_aad(principal_id: &str, provider: &str) -> Vec<u8> {
    format!("oauth:{principal_id}:{provider}").into_bytes()
}

fn unsupported<T>() -> StorageResult<T> {
    Err(StorageError::Fatal {
        message: "unsupported test storage method".to_owned(),
    })
}

#[async_trait]
impl OAuthCredentialStore for MemoryStorage {
    async fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        self.oauth.lock().expect("oauth storage lock").insert(
            (principal_id.to_owned(), provider.to_owned()),
            ciphertext.to_vec(),
        );
        Ok(())
    }

    async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .oauth
            .lock()
            .expect("oauth storage lock")
            .get(&(principal_id.to_owned(), provider.to_owned()))
            .cloned())
    }

    async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool> {
        Ok(self
            .oauth
            .lock()
            .expect("oauth storage lock")
            .remove(&(principal_id.to_owned(), provider.to_owned()))
            .is_some())
    }

    async fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        self.anthropic_api_keys
            .lock()
            .expect("anthropic api key storage lock")
            .insert(storage_key.to_owned(), ciphertext.to_vec());
        Ok(())
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .anthropic_api_keys
            .lock()
            .expect("anthropic api key storage lock")
            .get(storage_key)
            .cloned())
    }
}

#[async_trait]
impl AuditStore for MemoryStorage {
    async fn append_audit(&self, _entry: &AuditEntry) -> StorageResult<()> {
        unsupported()
    }

    async fn query_audit(
        &self,
        _principal_id: Option<&str>,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        unsupported()
    }

    async fn prune_audit(&self, _older_than: u64) -> StorageResult<u64> {
        unsupported()
    }
}

#[async_trait]
impl RequestEventStore for MemoryStorage {
    async fn append_request_event(&self, _event: &RequestEvent) -> StorageResult<()> {
        unsupported()
    }

    async fn query_request_events(
        &self,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        unsupported()
    }
}

#[async_trait]
impl QuotaStore for MemoryStorage {
    async fn incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
        _amount: u64,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn try_incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
        _amount: u64,
        _capacity: u64,
    ) -> StorageResult<Option<u64>> {
        unsupported()
    }

    async fn get_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn adjust_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
        _delta: i64,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn sweep_old_quotas(&self, _older_than_window_start: u64) -> StorageResult<u64> {
        unsupported()
    }
}

#[async_trait]
impl LimitStateStore for MemoryStorage {
    async fn put_principal_limit_state(&self, _state: &PrincipalLimitState) -> StorageResult<()> {
        unsupported()
    }

    async fn get_principal_limit_state(
        &self,
        _principal_id: &str,
        _identity_kind: PrincipalLimitIdentityKind,
        _identity_value: Option<&str>,
        _window: &str,
        _kind: PrincipalLimitKind,
    ) -> StorageResult<Option<PrincipalLimitState>> {
        unsupported()
    }

    async fn list_principal_limit_states(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<PrincipalLimitState>> {
        unsupported()
    }
}

#[async_trait]
impl UsageRollupStore for MemoryStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        unsupported()
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        unsupported()
    }

    async fn query_usage_rollups_in_range(
        &self,
        _resolution: UsageRollupResolution,
        _window_start_unix_secs: u64,
        _window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>> {
        unsupported()
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        unsupported()
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        _run: &UsageRollupRun,
    ) -> StorageResult<()> {
        unsupported()
    }
}

#[async_trait]
impl ApiKeyStore for MemoryStorage {
    async fn put_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _ciphertext: &[u8],
    ) -> StorageResult<()> {
        unsupported()
    }

    async fn get_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        unsupported()
    }

    async fn list_api_key_ciphertexts(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        unsupported()
    }

    async fn revoke_api_key(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _revoked_ciphertext: &[u8],
    ) -> StorageResult<bool> {
        unsupported()
    }
}

#[async_trait]
impl ConfigStore for MemoryStorage {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState> {
        unsupported()
    }

    async fn put_config_draft(
        &self,
        _new: ConfigDraftState,
        _expected_revision: u64,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn set_last_validated_revision(
        &self,
        _revision: u64,
        _error: Option<String>,
    ) -> StorageResult<()> {
        unsupported()
    }

    async fn append_config_history(
        &self,
        _revision: u64,
        _config_toml: String,
        _applied_at_unix_secs: u64,
        _summary: HistorySummary,
    ) -> StorageResult<()> {
        unsupported()
    }

    async fn list_config_history(&self, _limit: usize) -> StorageResult<Vec<HistoryEntry>> {
        unsupported()
    }

    async fn get_config_history(&self, _revision: u64) -> StorageResult<Option<HistoryEntry>> {
        unsupported()
    }
}

#[async_trait]
impl MetaStore for MemoryStorage {
    async fn initialize(&self, _requested: BackendKind) -> StorageResult<()> {
        unsupported()
    }

    async fn contract_version(&self) -> StorageResult<u32> {
        unsupported()
    }

    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        unsupported()
    }

    async fn killswitch_enabled(&self) -> StorageResult<bool> {
        unsupported()
    }

    async fn set_killswitch_enabled(&self, _enabled: bool) -> StorageResult<()> {
        unsupported()
    }
}
