use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::refresh::OAuthRefresher;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_redb::Storage;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

struct MockUpstreamStore {
    inner: Arc<Storage>,
    claim_refresh_lease_calls: Arc<AtomicUsize>,
    inject_error: Arc<AtomicBool>,
}

impl MockUpstreamStore {
    fn new(
        inner: Arc<Storage>,
        claim_refresh_lease_calls: Arc<AtomicUsize>,
        inject_error: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            claim_refresh_lease_calls,
            inject_error,
        }
    }
}

#[async_trait]
impl UpstreamStore for MockUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        self.inner.create(create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        self.inner.get_by_name(name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        self.inner.get_by_id(id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        if self.inject_error.load(Ordering::SeqCst) {
            return Err(cc_lb_storage_api::StorageError::Unavailable {
                message: "injected list error".to_owned(),
            });
        }
        self.inner.list(after, limit).await
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.update(id, expected_revision, update).await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.set_enabled(id, expected_revision, enabled).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        self.inner
            .store_oauth_tokens(id, expected_revision, tokens)
            .await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        self.claim_refresh_lease_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.claim_refresh_lease(id, holder, ttl_secs).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        self.inner.complete_refresh(id, holder, tokens).await
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        self.inner
            .release_lease_on_failure(id, holder, reason)
            .await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.inner.set_last_apply_error(id, error).await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        self.inner.soft_delete(id, expected_revision).await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        self.inner.hard_delete(id).await
    }
}

#[derive(Clone, Default)]
struct CapturedLogs {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.inner.lock().expect("captured logs lock");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

struct CapturedWriter {
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

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter { logs: self.clone() }
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage =
            Arc::new(Storage::open(&dir.path().join("cadence.redb"), [31; 32]).expect("storage"));
        let aead = Arc::new(AeadService::from_master_key([31; 32]));
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse("http://localhost/oauth/authorize").expect("auth url"),
            token_url: Url::parse("http://localhost/oauth/token").expect("token url"),
            redirect_uri: Url::parse("http://localhost/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        Self {
            _dir: dir,
            storage,
            aead,
            oauth_cfg,
        }
    }

    async fn with_token_url(token_url: Url) -> Self {
        let mut fixture = Self::new().await;
        let mut oauth_cfg = (*fixture.oauth_cfg).clone();
        oauth_cfg.token_url = token_url;
        fixture.oauth_cfg = Arc::new(oauth_cfg);
        fixture
    }

    async fn create_oauth_upstream(&self, name: &str, expires_at: u64) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
            })
            .await
            .expect("upstream created");
        let encrypted = encrypted(
            &self.aead,
            record.id,
            &OAuthTokenBundle {
                access_token: "sk-ant-oat01-test".to_owned(),
                refresh_token: "sk-ant-ort01-test".to_owned(),
                expires_at_unix_secs: expires_at,
                scopes: vec!["messages".to_owned()],
            },
        );
        self.storage
            .store_oauth_tokens(record.id, record.revision, encrypted)
            .await
            .expect("tokens stored");
        record.id
    }

    fn refresher(
        &self,
        upstreams: Arc<dyn UpstreamStore>,
        replica_id: Uuid,
        cancel: CancellationToken,
    ) -> Arc<OAuthRefresher> {
        Arc::new(OAuthRefresher::new(
            Arc::new(Stores {
                upstreams,
                principals: self.storage.clone(),
                plugin_registry: self.storage.clone(),
                upstream_rate_limits: self.storage.clone(),
                audit: Some(self.storage.clone()),
            }),
            self.aead.clone(),
            self.oauth_cfg.clone(),
            replica_id,
            cancel,
        ))
    }
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn spawn_mock_token_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    let app = axum::Router::new().route(
        "/oauth/token",
        axum::routing::post(|| async {
            axum::Json(serde_json::json!({
                "access_token": "sk-ant-oat01-new-token",
                "refresh_token": "sk-ant-ort01-new-refresh",
                "expires_in": 28800,
                "scope": "messages"
            }))
        }),
    );
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock server");
    });
    (addr, handle)
}

fn mock_upstream_store(
    fixture: &Fixture,
) -> (Arc<MockUpstreamStore>, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let claim_calls = Arc::new(AtomicUsize::new(0));
    let inject_error = Arc::new(AtomicBool::new(false));
    let upstreams = Arc::new(MockUpstreamStore::new(
        fixture.storage.clone(),
        claim_calls.clone(),
        inject_error.clone(),
    ));
    (upstreams, claim_calls, inject_error)
}

async fn spawn_refresher(refresher: Arc<OAuthRefresher>) -> JoinHandle<()> {
    let task = tokio::spawn(async move { refresher.run().await });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    task
}

async fn shutdown_refresher(cancel: CancellationToken, task: JoinHandle<()>) {
    cancel.cancel();
    task.await.expect("refresher task");
}

async fn shutdown_mock_server(handle: JoinHandle<()>) {
    handle.abort();
    let _ = handle.await;
}

async fn upstream_record(fixture: &Fixture, upstream_id: Uuid) -> UpstreamRecord {
    fixture
        .storage
        .get_by_id(upstream_id)
        .await
        .expect("get upstream")
        .expect("upstream exists")
}

async fn oauth_ciphertext(fixture: &Fixture, upstream_id: Uuid) -> EncryptedOAuthTokens {
    upstream_record(fixture, upstream_id)
        .await
        .oauth_credentials
        .expect("oauth credentials")
}

async fn advance_to_first_sweep_after_jitter() {
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;
}

async fn advance_remaining_to_eleven_minutes() {
    tokio::time::advance(Duration::from_secs(49)).await;
    tokio::task::yield_now().await;
}

async fn advance_eleven_minutes() {
    advance_to_first_sweep_after_jitter().await;
    advance_remaining_to_eleven_minutes().await;
}

async fn wait_for_claim(claim_calls: &AtomicUsize) {
    // NOTE [Priority-3 footgun]: under #[tokio::test(start_paused = true)] the only
    // way to give the spawned refresher task progress is `yield_now`. 1000 yields
    // was enough locally but flaked on ARC self-hosted CI where the runtime gets
    // 2 worker threads competing with other parallel test binaries; the refresher
    // sometimes had not yet reached `claim_refresh_lease` by the deadline. 100k
    // yields cap real-time at ~tens-of-ms (each yield is sub-microsecond) so the
    // poll still bounds aggressively while tolerating CI scheduling jitter.
    for _ in 0..100_000 {
        if claim_calls.load(Ordering::SeqCst) > 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("claim_refresh_lease was not called");
}

async fn wait_for_ciphertext_change(
    fixture: &Fixture,
    upstream_id: Uuid,
    before: &EncryptedOAuthTokens,
) -> UpstreamRecord {
    let mut last_record = None;
    for _ in 0..1000 {
        let record = upstream_record(fixture, upstream_id).await;
        if record
            .oauth_credentials
            .as_ref()
            .is_some_and(|tokens| tokens != before)
        {
            return record;
        }
        last_record = Some(record);
        tokio::task::yield_now().await;
    }
    let Some(record) = last_record else {
        panic!("oauth ciphertext did not change and upstream was never read");
    };
    panic!(
        "oauth ciphertext did not change; lease_holder={:?}; lease_until={:?}; last_apply_error={:?}",
        record.refresh_lease_holder, record.refresh_lease_until_unix_secs, record.last_apply_error
    );
}

#[tokio::test(start_paused = true)]
async fn gate_skips_sweep_when_no_oauth_credential_registered() {
    let fixture = Fixture::new().await;
    let (upstreams, claim_calls, _inject_error) = mock_upstream_store(&fixture);
    let cancel = CancellationToken::new();
    let refresher = fixture.refresher(upstreams, Uuid::new_v4(), cancel.clone());
    let task = spawn_refresher(refresher).await;

    advance_eleven_minutes().await;

    assert_eq!(claim_calls.load(Ordering::SeqCst), 0);
    shutdown_refresher(cancel, task).await;
}

#[tokio::test(start_paused = true)]
async fn gate_proceeds_when_at_least_one_oauth_credential_exists() {
    let (addr, server) = spawn_mock_token_server().await;
    let fixture = Fixture::with_token_url(
        Url::parse(&format!("http://{addr}/oauth/token")).expect("mock token url"),
    )
    .await;
    let upstream_id = fixture
        .create_oauth_upstream("within-window", now_secs() + 600)
        .await;
    let before = oauth_ciphertext(&fixture, upstream_id).await;
    let (upstreams, claim_calls, _inject_error) = mock_upstream_store(&fixture);
    let cancel = CancellationToken::new();
    let refresher = fixture.refresher(upstreams, Uuid::new_v4(), cancel.clone());
    let task = spawn_refresher(refresher).await;

    advance_to_first_sweep_after_jitter().await;
    wait_for_claim(&claim_calls).await;
    let refreshed = wait_for_ciphertext_change(&fixture, upstream_id, &before).await;
    advance_remaining_to_eleven_minutes().await;

    assert!(claim_calls.load(Ordering::SeqCst) > 0);
    assert_ne!(refreshed.oauth_credentials.as_ref(), Some(&before));
    assert_eq!(refreshed.refresh_lease_holder, None);
    assert_eq!(refreshed.refresh_lease_until_unix_secs, None);
    let bundle = refreshed
        .oauth_credentials
        .expect("refreshed credentials")
        .decrypt(&fixture.aead, upstream_id.as_bytes())
        .expect("decrypt refreshed credentials");
    assert_eq!(bundle.access_token, "sk-ant-oat01-new-token");
    assert_eq!(bundle.refresh_token, "sk-ant-ort01-new-refresh");

    shutdown_refresher(cancel, task).await;
    shutdown_mock_server(server).await;
}

#[tokio::test(start_paused = true)]
async fn refresh_does_not_fire_before_ten_minute_tick() {
    let (addr, server) = spawn_mock_token_server().await;
    let fixture = Fixture::with_token_url(
        Url::parse(&format!("http://{addr}/oauth/token")).expect("mock token url"),
    )
    .await;
    fixture
        .create_oauth_upstream("before-tick", now_secs() + 600)
        .await;
    let (upstreams, claim_calls, _inject_error) = mock_upstream_store(&fixture);
    let cancel = CancellationToken::new();
    let refresher = fixture.refresher(upstreams, Uuid::new_v4(), cancel.clone());
    let task = spawn_refresher(refresher).await;

    tokio::time::advance(Duration::from_secs(540)).await;
    tokio::task::yield_now().await;

    assert_eq!(claim_calls.load(Ordering::SeqCst), 0);
    shutdown_refresher(cancel, task).await;
    shutdown_mock_server(server).await;
}

#[tokio::test(start_paused = true)]
async fn refresh_fires_for_token_within_twenty_minute_lookahead_and_not_outside() {
    let (addr, server) = spawn_mock_token_server().await;
    let fixture = Fixture::with_token_url(
        Url::parse(&format!("http://{addr}/oauth/token")).expect("mock token url"),
    )
    .await;
    let now = now_secs();
    let inside_id = fixture
        .create_oauth_upstream("inside-window", now + 1140)
        .await;
    let outside_id = fixture
        .create_oauth_upstream("outside-window", now + 1260)
        .await;
    let inside_before = oauth_ciphertext(&fixture, inside_id).await;
    let outside_before = oauth_ciphertext(&fixture, outside_id).await;
    let (upstreams, claim_calls, _inject_error) = mock_upstream_store(&fixture);
    let cancel = CancellationToken::new();
    let refresher = fixture.refresher(upstreams, Uuid::new_v4(), cancel.clone());
    let task = spawn_refresher(refresher).await;

    advance_to_first_sweep_after_jitter().await;
    wait_for_claim(&claim_calls).await;
    let inside_after = wait_for_ciphertext_change(&fixture, inside_id, &inside_before).await;
    advance_remaining_to_eleven_minutes().await;
    let outside_after = upstream_record(&fixture, outside_id).await;

    assert!(claim_calls.load(Ordering::SeqCst) > 0);
    assert_ne!(
        inside_after.oauth_credentials.as_ref(),
        Some(&inside_before)
    );
    assert_eq!(inside_after.refresh_lease_holder, None);
    assert_eq!(inside_after.refresh_lease_until_unix_secs, None);
    assert_eq!(
        outside_after.oauth_credentials.as_ref(),
        Some(&outside_before)
    );
    assert_eq!(outside_after.refresh_lease_holder, None);
    assert_eq!(outside_after.refresh_lease_until_unix_secs, None);

    shutdown_refresher(cancel, task).await;
    shutdown_mock_server(server).await;
}

#[tokio::test(start_paused = true)]
async fn candidates_error_does_not_trigger_empty_gate_early_return() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let fixture = Fixture::new().await;
    let (upstreams, _claim_calls, inject_error) = mock_upstream_store(&fixture);
    inject_error.store(true, Ordering::SeqCst);
    let cancel = CancellationToken::new();
    let refresher = fixture.refresher(upstreams, Uuid::new_v4(), cancel.clone());
    let task = spawn_refresher(refresher).await;

    advance_eleven_minutes().await;
    shutdown_refresher(cancel, task).await;

    let output = logs.contents();
    assert!(
        output
            .lines()
            .any(|line| line.contains("WARN") && line.contains("oauth refresh sweep failed")),
        "expected WARN oauth refresh sweep failure log, captured logs:\n{output}"
    );
    assert!(
        !output.contains("oauth refresh sweep skipped: no oauth credentials registered"),
        "did not expect empty-gate debug log, captured logs:\n{output}"
    );
}
