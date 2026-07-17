use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{AnthropicKeepaliveDispatcher, RequestSnapshot};
use cc_lb_engine::clock::{Clock, TestClock, unix_secs};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    UpstreamDispatch,
};
use cc_lb_plugin_api::{
    Principal, RequestContext, RetryDecision, RouteDecision, RouteError, RouterPlugin,
    ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, Upstream,
    UpstreamCandidate,
};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord,
    CacheKeepaliveSessionStore, CacheTtl, MetaStore, UpstreamStore,
};
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

pub(super) struct RenewalFixture {
    _temp_dir: TempDir,
    pub(super) clock: Arc<TestClock>,
    pub(super) dispatcher: AnthropicKeepaliveDispatcher,
    pub(super) http: Arc<RecordingRenewalHttp>,
    pub(super) snapshot: RequestSnapshot,
    pub(super) storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    pub(super) upstream_id: Uuid,
}

impl RenewalFixture {
    pub(super) async fn new(cache_read_input_tokens: u64) -> Self {
        let temp_dir = tempfile::tempdir().expect("temporary directory");
        let clock = Arc::new(TestClock::new_at_secs(1_000));
        let database_url = format!(
            "sqlite://{}",
            temp_dir.path().join("renewal.sqlite").display()
        );
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite storage"),
        );
        storage
            .initialize(BackendKind::Sqlite)
            .await
            .expect("initialize sqlite storage");
        let upstream = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "renewal-upstream".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse("http://renewal.local/").expect("valid renewal URL")),
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("create renewal upstream");
        let principal_view = Arc::new(PrincipalView::from_db(
            &[],
            std::collections::HashMap::new(),
        ));
        let view = Arc::new(DynamicViewHolder::new(
            DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(RenewalSignerFactory))
                .global_router(Arc::new(NoRouteRouter))
                .global_observability_hooks(vec![])
                .principal_view(principal_view)
                .upstream_records(vec![upstream.clone()])
                .build(),
        ));
        let http = Arc::new(RecordingRenewalHttp::new(cache_read_input_tokens));
        let upstream_store: Arc<dyn UpstreamStore> = storage.clone();
        let dispatcher = AnthropicKeepaliveDispatcher::new(view, upstream_store, http.clone());
        let mut headers = HeaderMap::new();
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert("x-api-key", HeaderValue::from_static("sk-downstream"));
        let snapshot = RequestSnapshot::capture(
            Url::parse("https://api.anthropic.com/v1/messages").expect("valid Anthropic URL"),
            Method::POST,
            headers,
            Bytes::from_static(
                br#"{"model":"claude-test","max_tokens":32,"stream":true,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}"#,
            ),
            upstream.id,
            CacheTtl::Ttl5m,
            524_288,
        )
        .expect("capture renewal snapshot");

        Self {
            _temp_dir: temp_dir,
            clock,
            dispatcher,
            http,
            snapshot,
            storage,
            upstream_id: upstream.id,
        }
    }

    pub(super) async fn schedule_session(&self) -> CacheKeepaliveSessionRecord {
        let now = unix_secs(self.clock.now());
        self.storage
            .replace_from_real_request(&CacheKeepaliveReplaceRequest {
                session_key_hash: "renewal-session".to_owned(),
                principal_id: "renewal-principal".to_owned(),
                upstream_id: self.upstream_id,
                cache_anchor_at_unix_secs: now,
                ttl: CacheTtl::Ttl5m,
                run_at_unix_secs: now + 10,
                expires_at_unix_secs: now + 300,
                encrypted_payload: vec![1],
                now_unix_secs: now,
            })
            .await
            .expect("schedule renewal session")
    }

    pub(super) async fn request_event_count(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
            .fetch_one(self.storage.pool())
            .await
            .expect("count request event rows")
    }
}

struct RenewalSignerFactory;

impl ApiKeyAwareSignerFactory for RenewalSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(RenewalSigner)
    }
}

struct RenewalSigner;

#[async_trait]
impl SignerFactory for RenewalSigner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(Self))
    }
}

#[async_trait]
impl Signer for RenewalSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("sk-renewal"));
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct NoRouteRouter;

impl RouterPlugin for NoRouteRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "renewal dispatch uses its persisted upstream".to_owned(),
        })
    }
}

pub(super) struct RecordingRenewalHttp {
    cache_read_input_tokens: u64,
    calls: Mutex<u64>,
}

impl RecordingRenewalHttp {
    fn new(cache_read_input_tokens: u64) -> Self {
        Self {
            cache_read_input_tokens,
            calls: Mutex::new(0),
        }
    }

    pub(super) fn calls(&self) -> u64 {
        *self.calls.lock().expect("renewal HTTP call lock")
    }
}

#[async_trait]
impl UpstreamDispatch for RecordingRenewalHttp {
    async fn dispatch(
        &self,
        _request: cc_lb_plugin_api::SignedRequest,
    ) -> Result<Response<Body>, DispatchError> {
        *self.calls.lock().expect("renewal HTTP call lock") += 1;
        let body = serde_json::json!({
            "content": [],
            "stop_reason": "max_tokens",
            "usage": {"cache_read_input_tokens": self.cache_read_input_tokens}
        });
        let mut response = Response::new(Body::from(body.to_string()));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}
