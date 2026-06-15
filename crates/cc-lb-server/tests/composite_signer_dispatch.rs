use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::{HeaderMap, Method, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::{DynamicViewHolder, Lifecycle, LifecycleConfig};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalStore, StorageResult, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamUpdate,
};

use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamLeaseKind, UpstreamStatusUpdate};
use cc_lb_storage_redb::Storage;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::Request;
use http::header::LOCATION;
use http_body_util::BodyExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn router_choice_dispatches_to_matching_oauth_upstream_not_first_anthropic_direct() {
    let fixture = Fixture::new().await;
    let target_id = fixture
        .create_oauth_upstream("oauth-target", now_secs() + 3600, true)
        .await;
    fixture
        .create_missing_oauth_upstream_before(target_id)
        .await;
    fixture
        .create_principal("oauth-principal", vec![target_id])
        .await;
    let runtime = ExtismRuntime::new();
    let view = build_dynamic_view(
        fixture.stores.as_ref(),
        fixture.oauth_cfg.as_ref(),
        fixture.aead.clone(),
        None,
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
    )
    .await
    .expect("dynamic view builds");
    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "oauth-principal".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicOAuth,
            }),
            None,
        )),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    );

    let response = lifecycle
        .handle(message_request())
        .await
        .expect("lifecycle response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
}

struct Fixture {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    fake_base: String,
}

impl Fixture {
    async fn new() -> Self {
        let fake_addr = spawn_fake_anthropic().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(
            Storage::open(&dir.path().join("composite-dispatch.redb"), [33; 32]).expect("storage"),
        );
        let stores = Arc::new(Stores {
            upstreams: Arc::new(OrderedUpstreamStore {
                inner: storage.clone(),
            }),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage.clone()),
            plugin_registry_repo: None,
        });
        let aead = Arc::new(AeadService::from_master_key([33; 32]));
        let fake_base = format!("http://{fake_addr}");
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse(&format!("{fake_base}/oauth/authorize")).expect("auth url"),
            token_url: Url::parse(&format!("{fake_base}/oauth/token")).expect("token url"),
            redirect_uri: Url::parse("http://localhost/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        Self {
            _dir: dir,
            storage,
            stores,
            aead,
            oauth_cfg,
            fake_base,
        }
    }

    async fn create_principal(&self, name: &str, allowed_upstreams: Vec<Uuid>) {
        PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams,
                default_limits: Vec::new(),
            },
            now_secs(),
        )
        .await
        .expect("principal created");
    }

    async fn create_oauth_upstream(&self, name: &str, expires_at: u64, store_tokens: bool) -> Uuid {
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse(&self.fake_base).expect("fake url")),
                api_key_ciphertext: None,
                warmup_enabled: false,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created");
        if store_tokens {
            let tokens = initial_tokens(&self.fake_base).await;
            UpstreamStore::store_oauth_tokens(
                self.storage.as_ref(),
                record.id,
                record.revision,
                encrypted(
                    &self.aead,
                    record.id,
                    &OAuthTokenBundle {
                        access_token: tokens.access_token,
                        refresh_token: tokens.refresh_token,
                        expires_at_unix_secs: expires_at,
                        scopes: vec!["messages".to_owned()],
                    },
                ),
            )
            .await
            .expect("tokens stored");
        }
        record.id
    }

    async fn create_missing_oauth_upstream_before(&self, before: Uuid) {
        let id = self
            .create_oauth_upstream("missing-before-target", now_secs() + 3600, false)
            .await;
        assert_ne!(id, before);
    }
}

struct OrderedUpstreamStore {
    inner: Arc<Storage>,
}

#[async_trait]
impl UpstreamStore for OrderedUpstreamStore {
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
        let mut records = UpstreamStore::list(self.inner.as_ref(), after, limit).await?;
        records.sort_by_key(|record| record.name != "missing-before-target");
        Ok(records)
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
        next_warmup_at: Option<chrono::DateTime<chrono::Utc>>,
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

struct RawHttpResponse {
    status: StatusCode,
    headers: HeaderMap,
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
    let mut headers = HeaderMap::new();
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

async fn spawn_fake_anthropic() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake");
    let addr = listener.local_addr().expect("fake addr");
    tokio::spawn(async move {
        axum::serve(listener, fake_anthropic_app(AppConfig::default()))
            .await
            .expect("fake server")
    });
    addr
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

fn message_request() -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .expect("request builds")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
