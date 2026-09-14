use crate::admin_test_common;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::{CurrentConfig, DynamicViewRebinder, router};
use cc_lb_config::Config;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{
    ApplyStatus, DynamicView, DynamicViewBuilder, RouteDecision, RouteError, RouterPlugin,
    RoutingContext, UpstreamStatusEntry, UpstreamStatusSnapshot,
};
use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
use cc_lb_storage_api::UpstreamStore;
use cc_lb_testkit::{InMemoryStorage, fixed_clock};
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
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
    storage: Arc<InMemoryStorage>,
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
            .global_observability_hooks(Vec::new())
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

#[tokio::test]
async fn t2__create_upstream_rebinds_dynamic_view_before_response_returns() {
    let storage = Arc::new(InMemoryStorage::with_clock(fixed_clock(1_700_000_000)));
    let config = Config::default();
    let holder = admin_test_common::dynamic_view_holder(&config);
    let rebinder = Arc::new(SnapshotRebinder {
        storage: storage.clone(),
    });
    let mut state = crate::config_admin_common::test_state(config.clone(), Some(storage.clone()));
    state.dynamic_view = holder.clone();
    state.config = Arc::new(TestCurrentConfig { config, rebinder });
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
