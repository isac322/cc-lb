use std::collections::VecDeque;
use std::sync::Mutex;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use cc_lb_server::refresh::{LazyRefreshClaim, LazyRefreshClaimGuard, LazyRefreshTaskState};
use cc_lb_storage_api::{StorageResult, UpstreamStatusUpdate};
use tokio::sync::{Notify, watch};

use super::*;

#[tokio::test]
async fn terminal_refresh_failure_blocks_lazy_attempts_until_tokens_are_replaced() {
    for (status, body, reason) in [
        (
            StatusCode::BAD_REQUEST,
            serde_json::json!({"error": "invalid_grant", "error_description": "private provider details"}),
            "status_400",
        ),
        (
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"error": "unauthorized", "error_description": "private provider details"}),
            "status_401",
        ),
    ] {
        let fixture =
            LazyFixture::new(vec![TokenReply::new(status, body), successful_reply()]).await;
        let upstream_id = fixture.create_upstream().await;
        let refresher = fixture.refresher(fixture.claims.clone());

        refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("terminal refresh failure");
        let failed = fixture.record(upstream_id).await;
        assert_eq!(failed.last_apply_error.as_deref(), Some(reason));
        assert_eq!(fixture.endpoint_calls(), 1);
        assert_eq!(fixture.claims.calls.load(Ordering::SeqCst), 1);

        for _ in 0..2 {
            refresher
                .refresh_one(upstream_id)
                .await
                .expect_err("reconnect required");
            refresher
                .enqueue_only(upstream_id)
                .await
                .expect_err("reconnect required");
        }
        assert_eq!(fixture.endpoint_calls(), 1);
        assert_eq!(fixture.claims.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.record(upstream_id).await.oauth_token_generation,
            failed.oauth_token_generation
        );

        fixture.replace_tokens(upstream_id, None).await;
        let reconnected = fixture.record(upstream_id).await;
        assert!(reconnected.oauth_token_generation > failed.oauth_token_generation);
        assert_eq!(reconnected.last_apply_error, None);
        refresher
            .refresh_one(upstream_id)
            .await
            .expect("refresh resumes after actual replacement");
        let refreshed = fixture.record(upstream_id).await;
        assert!(refreshed.oauth_token_generation > reconnected.oauth_token_generation);
        assert_eq!(fixture.endpoint_calls(), 2);
        assert_eq!(refreshed.last_apply_error, None);
        let bundle = refreshed
            .oauth_credentials
            .expect("credentials")
            .decrypt(&fixture.inner.aead, upstream_id.as_bytes())
            .expect("decrypt");
        assert_eq!(bundle.access_token, "access-refreshed");
    }
}

#[tokio::test]
async fn unknown_bad_request_and_transient_server_failures_leave_lazy_refresh_retryable() {
    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let fixture = LazyFixture::new(vec![
            TokenReply::new(
                status,
                serde_json::json!({"error": "temporarily_unavailable"}),
            ),
            successful_reply(),
        ])
        .await;
        let upstream_id = fixture.create_upstream().await;
        let refresher = fixture.refresher(fixture.claims.clone());

        refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("retryable provider failure");
        let failed = fixture.record(upstream_id).await;
        assert!(!cc_lb_oauth_protocol::refresh_requires_reconnect(
            failed.last_apply_error.as_deref()
        ));
        refresher
            .refresh_one(upstream_id)
            .await
            .expect("retry can succeed without reconnect");
        let refreshed = fixture.record(upstream_id).await;
        assert!(refreshed.oauth_token_generation > failed.oauth_token_generation);
        assert_eq!(refreshed.last_apply_error, None);
        assert_eq!(fixture.endpoint_calls(), 2);
    }
}

#[tokio::test]
async fn expired_refresh_deadline_blocks_both_lazy_modes_before_claim_or_provider() {
    let fixture = LazyFixture::new(vec![successful_reply()]).await;
    let upstream_id = fixture.create_upstream().await;
    fixture
        .replace_tokens(upstream_id, Some(now_secs(fixture.inner.clock.as_ref())))
        .await;
    let before = fixture.record(upstream_id).await;
    let refresher = fixture.refresher(fixture.claims.clone());

    refresher
        .enqueue_only(upstream_id)
        .await
        .expect_err("expired refresh token cannot enqueue");
    refresher
        .refresh_one(upstream_id)
        .await
        .expect_err("expired refresh token cannot refresh");
    assert_eq!(fixture.endpoint_calls(), 0);
    assert_eq!(fixture.claims.calls.load(Ordering::SeqCst), 0);
    let expired = fixture.record(upstream_id).await;
    assert_eq!(
        expired.oauth_token_generation,
        before.oauth_token_generation
    );
    assert_eq!(
        expired.last_apply_error.as_deref(),
        Some("refresh_token_expired")
    );

    fixture.replace_tokens(upstream_id, None).await;
    refresher
        .refresh_one(upstream_id)
        .await
        .expect("replacement removes expired refresh deadline");
    assert_eq!(fixture.endpoint_calls(), 1);
    assert_eq!(fixture.record(upstream_id).await.last_apply_error, None);
}

#[tokio::test]
async fn fallback_does_not_contact_provider_after_worker_persists_terminal_failure() {
    let fixture = LazyFixture::new(vec![]).await;
    let upstream_id = fixture.create_upstream().await;
    let before = fixture.record(upstream_id).await;
    let claims = Arc::new(TerminalOnWait {
        storage: fixture.inner.storage.clone(),
        upstream_id,
        generation: before.oauth_token_generation,
        calls: AtomicUsize::new(0),
    });
    let refresher = fixture.refresher(claims.clone());

    refresher
        .refresh_one(upstream_id)
        .await
        .expect_err("worker terminal failure remains terminal in fallback");
    refresher
        .refresh_one(upstream_id)
        .await
        .expect_err("repeated proxy refresh remains terminal");
    refresher
        .enqueue_only(upstream_id)
        .await
        .expect_err("soft refresh remains terminal");
    assert_eq!(fixture.endpoint_calls(), 0);
    assert_eq!(claims.calls.load(Ordering::SeqCst), 1);
    let failed = fixture.record(upstream_id).await;
    assert_eq!(failed.oauth_token_generation, before.oauth_token_generation);
    assert_eq!(failed.last_apply_error.as_deref(), Some("status_400"));
}

#[tokio::test]
async fn transient_fallback_failure_cannot_clear_concurrent_same_generation_terminal_latch() {
    let release = Arc::new(Notify::new());
    let mut delayed_failure = TokenReply::new(
        StatusCode::SERVICE_UNAVAILABLE,
        serde_json::json!({"error": "temporarily_unavailable"}),
    );
    delayed_failure.release = Some(release.clone());
    let fixture = LazyFixture::new(vec![delayed_failure, successful_reply()]).await;
    let upstream_id = fixture.create_upstream().await;
    let before = fixture.record(upstream_id).await;
    let claims = Arc::new(DoneOnWait::default());
    let refresher = fixture.refresher(claims.clone());
    let inflight = tokio::spawn({
        let refresher = refresher.clone();
        async move { refresher.refresh_one(upstream_id).await }
    });
    fixture.wait_for_request().await;

    fixture
        .inner
        .storage
        .set_status(
            upstream_id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some("status_400".to_owned())),
                expected_oauth_token_generation: Some(before.oauth_token_generation),
                ..Default::default()
            },
        )
        .await
        .expect("concurrent worker commits terminal failure");
    release.notify_one();
    inflight
        .await
        .expect("fallback joins")
        .expect_err("transient fallback request fails");

    let after_failure = fixture.record(upstream_id).await;
    assert_eq!(
        after_failure.oauth_token_generation,
        before.oauth_token_generation
    );
    assert_eq!(
        after_failure.last_apply_error.as_deref(),
        Some("status_400")
    );
    refresher
        .refresh_one(upstream_id)
        .await
        .expect_err("terminal latch still blocks another refresh");
    refresher
        .enqueue_only(upstream_id)
        .await
        .expect_err("terminal latch still blocks soft refresh");
    assert_eq!(fixture.endpoint_calls(), 1);
    assert_eq!(claims.calls.load(Ordering::SeqCst), 1);

    fixture.replace_tokens(upstream_id, None).await;
    refresher
        .refresh_one(upstream_id)
        .await
        .expect("actual reconnect releases terminal latch");
    assert_eq!(fixture.endpoint_calls(), 2);
    assert_eq!(fixture.record(upstream_id).await.last_apply_error, None);
}

#[tokio::test]
async fn terminal_failure_from_old_generation_cannot_poison_reconnected_tokens() {
    let release = Arc::new(Notify::new());
    let mut delayed_failure = TokenReply::new(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error": "invalid_grant"}),
    );
    delayed_failure.release = Some(release.clone());
    let fixture = LazyFixture::new(vec![delayed_failure, successful_reply()]).await;
    let upstream_id = fixture.create_upstream().await;
    let refresher = fixture.refresher(fixture.claims.clone());
    let inflight = tokio::spawn({
        let refresher = refresher.clone();
        async move { refresher.refresh_one(upstream_id).await }
    });
    fixture.wait_for_request().await;
    let old = fixture.record(upstream_id).await;
    fixture.replace_tokens(upstream_id, None).await;
    let reconnected = fixture.record(upstream_id).await;
    assert!(reconnected.oauth_token_generation > old.oauth_token_generation);
    release.notify_one();
    inflight
        .await
        .expect("refresh joins")
        .expect_err("stale generation failure cannot be persisted");

    let after_failure = fixture.record(upstream_id).await;
    assert_eq!(
        after_failure.oauth_token_generation,
        reconnected.oauth_token_generation
    );
    assert_eq!(after_failure.last_apply_error, None);
    let bundle = after_failure
        .oauth_credentials
        .expect("credentials")
        .decrypt(&fixture.inner.aead, upstream_id.as_bytes())
        .expect("decrypt");
    assert_eq!(bundle.refresh_token, "refresh-reconnected");
    refresher
        .refresh_one(upstream_id)
        .await
        .expect("new generation remains refreshable");
    assert_eq!(fixture.endpoint_calls(), 2);
    assert_eq!(fixture.record(upstream_id).await.last_apply_error, None);
}

pub(super) struct LazyFixture {
    pub(super) inner: Fixture,
    endpoint: TokenEndpoint,
    pub(super) claims: Arc<ImmediateClaims>,
}

impl LazyFixture {
    pub(super) async fn new(replies: Vec<TokenReply>) -> Self {
        let mut inner = Fixture::new_without_scheduler(AppConfig::default()).await;
        let endpoint = TokenEndpoint::new(replies).await;
        let mut oauth_cfg = inner.oauth_cfg.as_ref().clone();
        oauth_cfg.token_url = endpoint.url.clone();
        inner.oauth_cfg = Arc::new(oauth_cfg);
        Self {
            inner,
            endpoint,
            claims: Arc::new(ImmediateClaims::default()),
        }
    }

    pub(super) async fn create_upstream(&self) -> Uuid {
        self.inner
            .create_oauth_upstream_with_tokens(
                "lazy-terminal",
                now_secs(self.inner.clock.as_ref()).saturating_sub(1),
                InitialTokens {
                    access_token: "access-old".to_owned(),
                    refresh_token: "refresh-old".to_owned(),
                },
                None,
                false,
            )
            .await
    }

    pub(super) fn refresher(&self, claims: Arc<dyn LazyRefreshClaimGuard>) -> Arc<LazyRefresher> {
        Arc::new(LazyRefresher::new_with_claim_guard(
            LazyRefresherDeps {
                stores: self.inner.stores.clone(),
                aead: self.inner.aead.clone(),
                oauth_cfg: self.inner.oauth_cfg.clone(),
                clock: self.inner.clock.clone(),
            },
            Uuid::new_v4(),
            None,
            CancellationToken::new(),
            claims,
            self.inner.scheduler_backend.clone(),
        ))
    }

    pub(super) async fn record(&self, upstream_id: Uuid) -> UpstreamRecord {
        cc_lb_storage_api::UpstreamStore::get_by_id(self.inner.storage.as_ref(), upstream_id)
            .await
            .expect("read upstream")
            .expect("upstream")
    }

    pub(super) async fn replace_tokens(&self, upstream_id: Uuid, refresh_deadline: Option<u64>) {
        let current = self.record(upstream_id).await;
        let tokens = encrypted(
            &self.inner.aead,
            upstream_id,
            &OAuthTokenBundle {
                access_token: "access-reconnected".to_owned(),
                refresh_token: "refresh-reconnected".to_owned(),
                expires_at_unix_secs: now_secs(self.inner.clock.as_ref()).saturating_sub(1),
                refresh_token_expires_at_unix_secs: refresh_deadline,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
        );
        self.inner
            .storage
            .store_oauth_tokens(upstream_id, current.revision, tokens, false)
            .await
            .expect("actual token replacement");
    }

    pub(super) fn endpoint_calls(&self) -> usize {
        self.endpoint.calls.load(Ordering::SeqCst)
    }

    async fn wait_for_request(&self) {
        let mut started = self.endpoint.started.clone();
        timeout(Duration::from_secs(5), async {
            while *started.borrow_and_update() == 0 {
                started.changed().await.expect("endpoint stays alive");
            }
        })
        .await
        .expect("provider receives request");
    }
}

#[derive(Default)]
pub(super) struct ImmediateClaims {
    calls: AtomicUsize,
}

#[async_trait]
impl LazyRefreshClaimGuard for ImmediateClaims {
    async fn begin_refresh(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _expires_at_unix_secs: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(LazyRefreshClaim::Acquired)
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(true)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(true)
    }
}

struct TerminalOnWait {
    storage: Arc<Storage>,
    upstream_id: Uuid,
    generation: u64,
    calls: AtomicUsize,
}

#[async_trait]
impl LazyRefreshClaimGuard for TerminalOnWait {
    async fn begin_refresh(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _expires_at_unix_secs: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(LazyRefreshClaim::Enqueued {
            idempotency_key: "terminal-worker".to_owned(),
        })
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(false)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        self.storage
            .set_status(
                self.upstream_id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some("status_400".to_owned())),
                    expected_oauth_token_generation: Some(self.generation),
                    ..Default::default()
                },
            )
            .await?;
        Ok(LazyRefreshTaskState::TerminalFailure {
            reason: "worker requires reconnect".to_owned(),
        })
    }
}

pub(super) struct TokenReply {
    status: StatusCode,
    body: Value,
    release: Option<Arc<Notify>>,
}

impl TokenReply {
    pub(super) fn new(status: StatusCode, body: Value) -> Self {
        Self {
            status,
            body,
            release: None,
        }
    }
}

pub(super) fn successful_reply() -> TokenReply {
    TokenReply::new(
        StatusCode::OK,
        serde_json::json!({
            "access_token": "access-refreshed",
            "refresh_token": "refresh-refreshed",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "messages"
        }),
    )
}

#[derive(Clone)]
struct EndpointState {
    replies: Arc<Mutex<VecDeque<TokenReply>>>,
    calls: Arc<AtomicUsize>,
    started: watch::Sender<usize>,
}

struct TokenEndpoint {
    url: Url,
    calls: Arc<AtomicUsize>,
    started: watch::Receiver<usize>,
    task: JoinHandle<()>,
}

impl Drop for TokenEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TokenEndpoint {
    async fn new(replies: Vec<TokenReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind token endpoint");
        let url = Url::parse(&format!(
            "http://{}/oauth/token",
            listener.local_addr().expect("endpoint address")
        ))
        .expect("token URL");
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started) = watch::channel(0);
        let state = EndpointState {
            replies: Arc::new(Mutex::new(replies.into())),
            calls: calls.clone(),
            started: started_tx,
        };
        let app = axum::Router::new()
            .route("/oauth/token", axum::routing::post(scripted_reply))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("token endpoint");
        });
        Self {
            url,
            calls,
            started,
            task,
        }
    }
}

async fn scripted_reply(State(state): State<EndpointState>) -> Response {
    let calls = state.calls.fetch_add(1, Ordering::SeqCst) + 1;
    let reply = state
        .replies
        .lock()
        .expect("reply queue")
        .pop_front()
        .unwrap_or_else(|| {
            TokenReply::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"error": "unexpected_refresh"}),
            )
        });
    state.started.send_replace(calls);
    if let Some(release) = reply.release {
        release.notified().await;
    }
    (reply.status, axum::Json(reply.body)).into_response()
}

#[derive(Default)]
struct DoneOnWait {
    calls: AtomicUsize,
}

#[async_trait]
impl LazyRefreshClaimGuard for DoneOnWait {
    async fn begin_refresh(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _expires_at_unix_secs: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(LazyRefreshClaim::Enqueued {
            idempotency_key: "completed-without-token-change".to_owned(),
        })
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(false)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        Ok(LazyRefreshTaskState::Done)
    }
}
