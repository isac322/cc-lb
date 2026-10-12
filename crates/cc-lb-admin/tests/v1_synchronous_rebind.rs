use crate::admin_test_common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Waker};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::{AdminState, DynamicViewRebinder, router};
use cc_lb_config::Config;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{
    ApplyStatus, DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusEntry,
    UpstreamStatusSnapshot,
};
use cc_lb_domain::Upstream;
use cc_lb_storage_api::UpstreamStore;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

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
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot {
                entries,
                applied_at_unix_secs: 1,
                revision_hash: 1,
            }))
            .build())
    }
}

/// Probes, from inside the admin route's build, whether another publisher
/// could acquire the holder's rebuild lock. A rebuild that read storage
/// before the mutation committed (a notify or reconcile rebuild) could only
/// install first if the admin route released the lock between its
/// generation read and its install.
struct LockProbeRebinder {
    fresh: SnapshotRebinder,
    holder: Arc<DynamicViewHolder>,
    lock_held_during_build: AtomicBool,
}

#[async_trait]
impl DynamicViewRebinder for LockProbeRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let lock_held = {
            let mut competitor = std::pin::pin!(self.holder.lock_rebuild());
            let mut cx = Context::from_waker(Waker::noop());
            competitor.as_mut().poll(&mut cx).is_pending()
        };
        self.lock_held_during_build
            .store(lock_held, Ordering::SeqCst);
        self.fresh.rebuild_dynamic_view(current_generation).await
    }
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
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

fn admin_state(
    storage: Arc<Storage>,
    holder: Arc<DynamicViewHolder>,
    rebinder: Arc<dyn DynamicViewRebinder>,
    config: Config,
) -> AdminState {
    AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage)),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: holder,
        config: Arc::new(config),
        dynamic_view_rebinder: Some(rebinder),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
            .expect("production Anthropic origin parses"),
    }
}

/// Creates `sync-primary` and asserts the response's generation names a
/// resident view that already contains it.
async fn assert_create_is_visible(app: axum::Router, holder: &DynamicViewHolder) {
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
                        "api_key_value": "sk-ant-test",
                        "skip_preflight": true
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

#[tokio::test]
async fn create_upstream_rebinds_dynamic_view_before_response_returns() {
    let dir = tempfile::tempdir().expect("temp admin dir");
    let storage = admin_test_common::sqlite_storage(dir.path(), "admin.sqlite").await;
    let config = Config::default();
    let holder = admin_test_common::dynamic_view_holder(&config);
    let rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(SnapshotRebinder {
        storage: storage.clone(),
    });
    let app = router(admin_state(storage, holder.clone(), rebinder, config));
    assert_create_is_visible(app, &holder).await;
}

#[tokio::test]
async fn create_upstream_holds_rebuild_lock_so_no_competing_view_installs_mid_build() {
    let dir = tempfile::tempdir().expect("temp admin dir");
    let storage = admin_test_common::sqlite_storage(dir.path(), "admin.sqlite").await;
    let config = Config::default();
    let holder = admin_test_common::dynamic_view_holder(&config);
    let rebinder = Arc::new(LockProbeRebinder {
        fresh: SnapshotRebinder {
            storage: storage.clone(),
        },
        holder: holder.clone(),
        lock_held_during_build: AtomicBool::new(false),
    });
    let app = router(admin_state(
        storage,
        holder.clone(),
        rebinder.clone() as Arc<dyn DynamicViewRebinder>,
        config,
    ));
    assert_create_is_visible(app, &holder).await;
    assert!(
        rebinder.lock_held_during_build.load(Ordering::SeqCst),
        "a competing publisher must not acquire the rebuild lock while the admin route builds"
    );
}
