#![allow(dead_code, deprecated)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::{Router, body::Body};
use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_config::Config;
use cc_lb_control::api_keys::{
    concurrent_guard::KeyConcurrencyManager, principal_view::PrincipalView,
};
use cc_lb_control::{
    DynamicViewBuilder, DynamicViewHolder, RouteDecision, RouteError, RouterPlugin, RoutingContext,
    UpstreamStatusSnapshot, api_keys::limit_engine::LimitEngine,
};
use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
use cc_lb_observability::ObservabilityHook;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use http_body_util::BodyExt;
use tower::ServiceExt;

pub fn limit_engine() -> Arc<LimitEngine> {
    limit_engine_with_clock(system_clock())
}

pub fn limit_engine_with_clock(clock: ClockHandle) -> Arc<LimitEngine> {
    LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock)
}

fn system_clock() -> ClockHandle {
    Arc::new(SystemClock)
}

pub fn key_store(storage: Arc<SqliteStorage>) -> Arc<cc_lb_control::api_keys::key_store::KeyStore> {
    Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(storage))
}

pub fn dynamic_view_holder(_config: &Config) -> Arc<DynamicViewHolder> {
    let principal_view = Arc::new(PrincipalView::from_db(
        &[],
        std::collections::HashMap::new(),
    ));
    Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopSignerFactory))
            .global_router(Arc::new(NoopRouter))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build(),
    ))
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(
        &self,
        _upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
        Err(cc_lb_upstream::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "noop test router".to_owned(),
        })
    }
}

pub struct SpawnedAdminServer {
    pub _dir: tempfile::TempDir,
    pub storage: Arc<SqliteStorage>,
    pub dynamic_view: Arc<DynamicViewHolder>,
    pub client: AdminClient,
}

#[derive(Clone)]
pub struct AdminClient {
    app: Router,
    token: String,
}

pub fn static_token_provider(token: &str) -> Arc<dyn cc_lb_admin::auth::AdminAuthProvider> {
    Arc::new(TestStaticTokenProvider {
        token: token.to_owned(),
    })
}

pub fn static_token_auth(token: &str) -> Arc<cc_lb_admin::auth::AdminAuthenticator> {
    Arc::new(cc_lb_admin::auth::AdminAuthenticator::new(vec![
        static_token_provider(token),
    ]))
}

struct TestStaticTokenProvider {
    token: String,
}

#[async_trait]
impl cc_lb_admin::auth::AdminAuthProvider for TestStaticTokenProvider {
    fn id(&self) -> &str {
        "test-static-token"
    }

    fn is_static_token(&self) -> bool {
        true
    }

    async fn authenticate(&self, headers: &HeaderMap) -> cc_lb_admin::auth::ProviderOutcome {
        let Some(token) = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return cc_lb_admin::auth::ProviderOutcome::NotPresent;
        };
        if token != self.token {
            return cc_lb_admin::auth::ProviderOutcome::Rejected("static_token_mismatch");
        }
        cc_lb_admin::auth::ProviderOutcome::Verified(cc_lb_admin::auth::AdminIdentity {
            authority: "static-token".to_owned(),
            subject: self.id().to_owned(),
            kind: cc_lb_admin::auth::AdminActorKind::BreakGlass,
            provider_id: self.id().to_owned(),
            email: None,
            display_name: Some("Test admin token".to_owned()),
            groups: Vec::new(),
            expires_at_unix_secs: None,
        })
    }
}

pub async fn spawn_admin_server() -> SpawnedAdminServer {
    spawn_admin_server_with_clock(system_clock()).await
}

pub async fn spawn_admin_server_with_auth(
    admin_auth: Arc<cc_lb_admin::auth::AdminAuthenticator>,
) -> SpawnedAdminServer {
    spawn_admin_server_with_clock_and_auth(system_clock(), admin_auth).await
}

pub async fn spawn_admin_server_with_clock(clock: ClockHandle) -> SpawnedAdminServer {
    spawn_admin_server_with_clock_and_auth(clock, static_token_auth("test-token")).await
}

pub async fn spawn_admin_server_with_clock_and_auth(
    clock: ClockHandle,
    admin_auth: Arc<cc_lb_admin::auth::AdminAuthenticator>,
) -> SpawnedAdminServer {
    let dir = tempfile::tempdir().expect("temp admin server dir");
    let storage = sqlite_storage_with_clock(dir.path(), "admin.sqlite", clock.clone()).await;
    let config = Config::default();
    let dynamic_view = dynamic_view_holder(&config);
    let state = cc_lb_admin::AdminState {
        storage: Some(storage.clone()),
        key_store: Some(key_store(storage.clone())),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: limit_engine_with_clock(clock.clone()),
        lifecycle: None,
        subscription_metadata_hook: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        dynamic_view: dynamic_view.clone(),
        config: Arc::new(config),
        scheduler: None,
        admin_auth,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
    };
    SpawnedAdminServer {
        _dir: dir,
        storage,
        dynamic_view,
        client: AdminClient {
            app: cc_lb_admin::router(state),
            token: "test-token".to_owned(),
        },
    }
}

pub fn set_dynamic_principal(dynamic_view: &DynamicViewHolder, principal_id: &str) {
    let principal_view = Arc::new(PrincipalView::for_tests(
        principal_id,
        true,
        Vec::new(),
        Vec::new(),
        std::collections::HashMap::new(),
    ));
    let view = DynamicViewBuilder::new(dynamic_view.generation().saturating_add(1))
        .signer_factory(Arc::new(NoopSignerFactory))
        .global_router(Arc::new(NoopRouter))
        .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
        .principal_view(principal_view)
        .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
        .build();
    dynamic_view.store(view);
}

pub async fn sqlite_storage(dir: &std::path::Path, filename: &str) -> Arc<SqliteStorage> {
    sqlite_storage_with_clock(dir, filename, system_clock()).await
}

pub async fn sqlite_storage_with_clock(
    dir: &std::path::Path,
    filename: &str,
    clock: ClockHandle,
) -> Arc<SqliteStorage> {
    let database_url = format!("sqlite://{}", dir.join(filename).display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .expect("admin sqlite opens");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("admin sqlite initializes");
    Arc::new(storage)
}

impl AdminClient {
    pub async fn json(
        &self,
        method: &str,
        uri: &str,
        body: Option<serde_json::Value>,
        headers: &[(&str, &str)],
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        let mut builder = Request::builder()
            .method(Method::from_bytes(method.as_bytes()).expect("valid method"))
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {}", self.token));
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request_body = match body {
            Some(value) => {
                builder = builder.header(header::CONTENT_TYPE, "application/json");
                Body::from(serde_json::to_vec(&value).expect("json body serializes"))
            }
            None => Body::empty(),
        };
        let response = self
            .app
            .clone()
            .oneshot(builder.body(request_body).expect("request builds"))
            .await
            .expect("admin request succeeds");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("response is json")
        };
        (status, headers, value)
    }

    pub async fn get(&self, uri: &str) -> (StatusCode, HeaderMap, serde_json::Value) {
        self.json("GET", uri, None, &[]).await
    }

    pub async fn get_without_auth(&self, uri: &str) -> (StatusCode, HeaderMap, serde_json::Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("admin request succeeds");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("response is json")
        };
        (status, headers, value)
    }

    pub async fn delete(
        &self,
        uri: &str,
        if_match: &str,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        self.json(
            "DELETE",
            uri,
            None,
            &[(header::IF_MATCH.as_str(), if_match)],
        )
        .await
    }

    pub async fn post_json(
        &self,
        uri: &str,
        body: serde_json::Value,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        self.json("POST", uri, Some(body), &[]).await
    }

    pub async fn post_raw(
        &self,
        uri: &str,
        content_type: &str,
        body: &'static str,
    ) -> (StatusCode, HeaderMap, bytes::Bytes) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
                    .header(header::CONTENT_TYPE, content_type)
                    .body(Body::from(body))
                    .expect("request builds"),
            )
            .await
            .expect("admin request succeeds");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        (status, headers, body)
    }

    pub async fn post_with_if_match(
        &self,
        uri: &str,
        if_match: &str,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        self.json("POST", uri, None, &[(header::IF_MATCH.as_str(), if_match)])
            .await
    }

    pub async fn put_json(
        &self,
        uri: &str,
        body: serde_json::Value,
        if_match: Option<&str>,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        let headers = if_match
            .map(|value| vec![(header::IF_MATCH.as_str(), value)])
            .unwrap_or_default();
        self.json("PUT", uri, Some(body), &headers).await
    }

    pub fn header_str<'a>(&self, headers: &'a HeaderMap, name: &'static str) -> &'a str {
        headers
            .get(name)
            .and_then(|value: &HeaderValue| value.to_str().ok())
            .expect("response header exists")
    }
}
