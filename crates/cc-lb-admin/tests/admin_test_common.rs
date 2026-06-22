#![allow(dead_code, deprecated)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::{Router, body::Body};
use cc_lb_config::Config;
use cc_lb_core::api_keys::{
    concurrent_guard::KeyConcurrencyManager, principal_view::PrincipalView,
};
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, UpstreamDispatch, UpstreamStatusSnapshot, api_keys::limit_engine::LimitEngine,
    spawn_audit_writer,
};
use cc_lb_plugin_api::{
    ObservabilityHook, Principal, RequestContext, RouteDecision, RouteError, RouterPlugin,
    SignedRequest, SignerFactory, Upstream, UpstreamCandidate,
};
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use tower::ServiceExt;

pub fn limit_engine() -> Arc<LimitEngine> {
    LimitEngine::new(Arc::new(KeyConcurrencyManager::new()))
}

pub fn key_store(storage: Arc<SqliteStorage>) -> Arc<cc_lb_core::api_keys::key_store::KeyStore> {
    Arc::new(cc_lb_core::api_keys::key_store::KeyStore::new(storage))
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
            .dispatcher(Arc::new(NoopDispatch))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
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
    ) -> Result<Arc<dyn cc_lb_plugin_api::Signer>, cc_lb_plugin_api::SignerError> {
        Err(cc_lb_plugin_api::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "noop test router".to_owned(),
        })
    }
}

struct NoopDispatch;

#[async_trait]
impl UpstreamDispatch for NoopDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        Ok(http::Response::new(Body::empty()))
    }
}

pub struct SpawnedAdminServer {
    pub _dir: tempfile::TempDir,
    pub storage: Arc<SqliteStorage>,
    pub client: AdminClient,
    pub _audit_task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub struct AdminClient {
    app: Router,
    token: String,
}

pub async fn spawn_admin_server() -> SpawnedAdminServer {
    let dir = tempfile::tempdir().expect("temp admin server dir");
    let storage = sqlite_storage(dir.path(), "admin.sqlite").await;
    let (audit_sink, audit_task) = spawn_audit_writer(storage.clone(), 128);
    let config = Config::default();
    let state = cc_lb_admin::AdminState {
        storage: Some(storage.clone()),
        key_store: Some(key_store(storage.clone())),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: limit_engine(),
        lifecycle: None,
        subscription_metadata_hook: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        audit_sink: Some(Arc::new(audit_sink)),
        dynamic_view: dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: None,
        admin_token: Some("test-token".to_owned()),
        start_time: std::time::Instant::now(),
    };
    SpawnedAdminServer {
        _dir: dir,
        storage,
        client: AdminClient {
            app: cc_lb_admin::router(state),
            token: "test-token".to_owned(),
        },
        _audit_task: audit_task,
    }
}

pub async fn sqlite_storage(dir: &std::path::Path, filename: &str) -> Arc<SqliteStorage> {
    let database_url = format!("sqlite://{}", dir.join(filename).display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url)
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
