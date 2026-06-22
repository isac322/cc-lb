#![allow(deprecated)]

mod admin_test_common;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::{AdminState, CurrentConfig, DynamicViewRebinder, router};
use cc_lb_config::Config;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, ApplyStatus, DispatchError, DynamicView, DynamicViewBuilder,
    UpstreamDispatch, UpstreamStatusEntry, UpstreamStatusSnapshot,
};
use cc_lb_plugin_api::{
    Principal, RequestContext, RouteDecision, RouteError, RouterPlugin, SignedRequest,
    SignerFactory, Upstream, UpstreamCandidate,
};
use cc_lb_storage_api::UpstreamStore;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

struct TestCurrentConfig {
    config: Config,
    rebinder: Arc<dyn DynamicViewRebinder>,
}

impl CurrentConfig for TestCurrentConfig {
    fn current_config(&self) -> Arc<Config> {
        Arc::new(self.config.clone())
    }

    fn dynamic_view_rebinder(&self) -> Option<Arc<dyn DynamicViewRebinder>> {
        Some(self.rebinder.clone())
    }
}

struct SnapshotRebinder {
    storage: Arc<Storage>,
}

#[async_trait]
impl DynamicViewRebinder for SnapshotRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let mut entries = HashMap::new();
        let mut after = None;
        loop {
            let page = UpstreamStore::list(&*self.storage, after, 100).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|record| record.id);
            for upstream in page {
                if upstream.deleted_at_unix_secs.is_none() {
                    entries.insert(
                        upstream.name,
                        UpstreamStatusEntry {
                            status: ApplyStatus::Active,
                            last_apply_error: None,
                            last_apply_at_unix_secs: 1,
                        },
                    );
                }
            }
        }

        let principal_view = Arc::new(PrincipalView::from_db(&[], HashMap::new()));
        Ok(DynamicViewBuilder::new(current_generation)
            .signer_factory(Arc::new(NoopSignerFactory))
            .global_router(Arc::new(NoopRouter))
            .dispatcher(Arc::new(NoopDispatch))
            .global_observability_hooks(Vec::new())
            .error_normalizer(Arc::new(cc_lb_core::ErrorNormalizer::new()))
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot {
                entries,
                applied_at_unix_secs: 1,
                revision_hash: 1,
            }))
            .build())
    }
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

#[tokio::test]
async fn create_upstream_rebinds_dynamic_view_before_response_returns() {
    let dir = tempfile::tempdir().expect("temp admin dir");
    let storage = admin_test_common::sqlite_storage(dir.path(), "admin.sqlite").await;
    let config = Config::default();
    let holder = admin_test_common::dynamic_view_holder(&config);
    let rebinder = Arc::new(SnapshotRebinder {
        storage: storage.clone(),
    });
    let state = AdminState {
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage.clone())),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: holder.clone(),
        config: Arc::new(TestCurrentConfig { config, rebinder }),
        scheduler: None,
        admin_token: Some("test-token".to_owned()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
    };
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/v1/upstreams")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "sync-primary",
                        "kind": "anthropic_api_key",
                        "base_url": "https://example.com",
                        "api_key_value": "sk-ant-test"
                    })
                    .to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("admin request succeeds");

    assert_eq!(response.status(), StatusCode::CREATED);
    let generation = response
        .headers()
        .get("x-cc-lb-generation")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .expect("generation header is present");
    let _body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();

    let view = holder.load();
    assert_eq!(view.generation, generation);
    assert!(
        view.upstream_status_snapshot
            .entries
            .contains_key("sync-primary")
    );
}
