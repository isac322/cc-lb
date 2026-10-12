//! OAuth onboarding and reconnection rebuild the serving view after their
//! credential change has committed, even when the audit write then fails.
//! The client still receives the accepted `500 audit_write_failed` body.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode, header};
use cc_lb_admin::{AdminState, DynamicViewRebinder, router};
use cc_lb_aead::AeadService;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot};
use cc_lb_domain::Upstream;
use cc_lb_storage_api::UpstreamStore;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamCreate, UpstreamRecord};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use http_body_util::BodyExt;
use mock_anthropic_oauth_server::AppState as MockOAuthState;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use crate::admin_test_common;
use crate::v1_oauth::{
    MASTER_KEY, TEST_NOW_UNIX_SECS, authorize_code, spawn_mock_anthropic, test_config,
};

const AUDIT_WRITE_FAILED_BODY: &[u8] = br#"{"error":"audit_write_failed"}"#;

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

/// Rebuilds the view's upstream records from storage.
struct StorageRebinder {
    storage: Arc<SqliteStorage>,
}

#[async_trait]
impl DynamicViewRebinder for StorageRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let upstreams = UpstreamStore::list(&*self.storage, None, 1000).await?;
        Ok(DynamicViewBuilder::new(current_generation)
            .signer_factory(Arc::new(NoopSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .upstream_records(upstreams)
            .build())
    }
}

struct Harness {
    _dir: tempfile::TempDir,
    storage: Arc<SqliteStorage>,
    app: axum::Router,
    holder: Arc<DynamicViewHolder>,
}

async fn harness() -> Harness {
    harness_with_scheduler(None).await
}

async fn harness_with_scheduler(
    scheduler: Option<cc_lb_scheduler::admin::SchedulerAdminHandle>,
) -> Harness {
    let (oauth_addr, _oauth_state) = spawn_mock_anthropic(MockOAuthState::default()).await;
    let dir = tempfile::tempdir().expect("temp admin dir");
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
    let storage = admin_test_common::sqlite_storage_with_clock(
        dir.path(),
        "oauth_post_commit.sqlite",
        clock.clone(),
    )
    .await;
    let config = test_config(oauth_addr, None);
    let holder = admin_test_common::dynamic_view_holder(&config);
    let rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(StorageRebinder {
        storage: storage.clone(),
    });
    let state = AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage.clone())),
        aead: Arc::new(AeadService::from_master_key(MASTER_KEY)),
        limit_engine: admin_test_common::limit_engine_with_clock(clock.clone()),
        lifecycle: None,
        dynamic_view: holder.clone(),
        config: Arc::new(config),
        dynamic_view_rebinder: Some(rebinder),
        scheduler,
        admin_auth: admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
    };
    Harness {
        _dir: dir,
        storage,
        app: router(state),
        holder,
    }
}

async fn post(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, HeaderMap, Bytes) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    (status, headers, bytes)
}

async fn post_json(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let (status, _, bytes) = post(app, uri, body).await;
    let value = serde_json::from_slice(&bytes).expect("response body is JSON");
    (status, value)
}

async fn fail_audit_inserts(storage: &SqliteStorage) {
    sqlx::query(
        "CREATE TRIGGER fail_admin_audit BEFORE INSERT ON audit_log_v1 \
         BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END",
    )
    .execute(storage.pool())
    .await
    .expect("audit failure trigger");
}

fn view_contains_upstream(holder: &DynamicViewHolder, id: Uuid) -> bool {
    holder
        .load()
        .upstreams_snapshot()
        .iter()
        .any(|upstream| upstream.id == id)
}

/// The accepted audit-failure response: exact body, and none of the success
/// headers (generation, rebind status, Location).
fn assert_audit_write_failed(status: StatusCode, headers: &HeaderMap, bytes: &Bytes) {
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(&bytes[..], AUDIT_WRITE_FAILED_BODY);
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());
    assert!(headers.get(header::LOCATION).is_none());
}

#[tokio::test]
async fn create_from_oauth_draft_audit_failure_still_rebuilds_view() {
    let harness = harness().await;
    let (status, start) = post_json(&harness.app, "/admin/v1/oauth/draft/start", json!({})).await;
    assert_eq!(status, StatusCode::OK, "draft start: {start}");
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let (status, complete) = post_json(
        &harness.app,
        "/admin/v1/oauth/draft/complete",
        json!({ "state_token": state_token, "code": code }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "draft complete: {complete}");

    fail_audit_inserts(&harness.storage).await;
    let generation_before = harness.holder.generation();
    let name = "oauth-draft-audit-failure";
    let (status, headers, bytes) = post(
        &harness.app,
        "/admin/v1/upstreams/from-oauth-draft",
        json!({ "state_token": state_token, "name": name }),
    )
    .await;

    assert_audit_write_failed(status, &headers, &bytes);
    let created = UpstreamStore::get_by_name(&*harness.storage, name)
        .await
        .expect("stored upstream read")
        .expect("the onboarding is committed despite the 500");
    assert!(harness.holder.generation() > generation_before);
    assert!(
        view_contains_upstream(&harness.holder, created.id),
        "the serving view must offer the committed upstream"
    );
}

#[tokio::test]
async fn oauth_complete_audit_failure_still_rebuilds_view() {
    let harness = harness().await;
    let upstream: UpstreamRecord = UpstreamStore::create(
        &*harness.storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "oauth-complete-audit-failure".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("create upstream");
    assert!(!view_contains_upstream(&harness.holder, upstream.id));

    let (status, start) = post_json(
        &harness.app,
        &format!("/admin/v1/upstreams/{}/oauth/start", upstream.id),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "oauth start: {start}");
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;

    fail_audit_inserts(&harness.storage).await;
    let generation_before = harness.holder.generation();
    let (status, headers, bytes) = post(
        &harness.app,
        &format!("/admin/v1/upstreams/{}/oauth/complete", upstream.id),
        json!({ "state_token": state_token, "code": code }),
    )
    .await;

    assert_audit_write_failed(status, &headers, &bytes);
    let stored = UpstreamStore::get_by_id(&*harness.storage, upstream.id)
        .await
        .expect("stored upstream read")
        .expect("upstream exists");
    // Token storage lives in the credential row and does not bump the spec
    // revision, so the committed change is the credential appearing.
    assert!(upstream.oauth_credentials.is_none());
    assert!(
        stored.oauth_credentials.is_some(),
        "the credential exchange is committed despite the 500"
    );
    assert!(harness.holder.generation() > generation_before);
    assert!(
        view_contains_upstream(&harness.holder, upstream.id),
        "the serving view must offer the reconnected upstream"
    );
}

/// Runs draft start and draft complete against the mock provider and returns
/// the completed draft's state token.
async fn completed_draft(harness: &Harness) -> String {
    let (status, start) = post_json(&harness.app, "/admin/v1/oauth/draft/start", json!({})).await;
    assert_eq!(status, StatusCode::OK, "draft start: {start}");
    let state_token = start["state_token"]
        .as_str()
        .expect("state token")
        .to_owned();
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let (status, complete) = post_json(
        &harness.app,
        "/admin/v1/oauth/draft/complete",
        json!({ "state_token": state_token, "code": code }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "draft complete: {complete}");
    state_token
}

/// Metadata storage fails after the upstream create committed, and the
/// compensating hard delete fails too: the upstream stays committed, so the
/// serving view must be rebuilt while the original rollback error is returned.
#[tokio::test]
async fn create_from_oauth_draft_failed_rollback_still_rebuilds_view() {
    let harness = harness().await;
    let state_token = completed_draft(&harness).await;
    for trigger in [
        "CREATE TRIGGER fail_subscription_metadata_insert BEFORE INSERT ON \
         upstream_subscription_metadata_v1 \
         BEGIN SELECT RAISE(ABORT, 'injected subscription metadata failure'); END",
        "CREATE TRIGGER fail_upstream_hard_delete BEFORE DELETE ON upstream_spec_v1 \
         BEGIN SELECT RAISE(ABORT, 'injected upstream delete failure'); END",
    ] {
        sqlx::query(trigger)
            .execute(harness.storage.pool())
            .await
            .expect("install failure trigger");
    }
    let generation_before = harness.holder.generation();
    let name = "oauth-draft-failed-rollback";

    let (status, headers, bytes) = post(
        &harness.app,
        "/admin/v1/upstreams/from-oauth-draft",
        json!({ "state_token": state_token, "name": name }),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());
    assert!(headers.get(header::LOCATION).is_none());
    let body: Value = serde_json::from_slice(&bytes).expect("error body is JSON");
    assert_eq!(body["error"], "oauth_draft_create_failed");
    let detail = body["detail"].as_str().expect("error detail");
    assert!(
        detail.starts_with("subscription metadata storage failed: "),
        "{detail}"
    );
    assert!(detail.contains("; rollback delete failed: "), "{detail}");
    assert_eq!(
        body.as_object().map(|object| object.len()),
        Some(2),
        "the error body keeps exactly its error and detail fields"
    );

    let stranded = UpstreamStore::get_by_name(&*harness.storage, name)
        .await
        .expect("stored upstream read")
        .expect("the failed rollback leaves the upstream committed");
    assert!(harness.holder.generation() > generation_before);
    assert!(
        view_contains_upstream(&harness.holder, stranded.id),
        "the serving view must reflect the upstream left in storage"
    );
}

/// Metadata storage fails after the upstream create committed and the
/// compensating hard delete succeeds: the original rollback error is returned
/// and the serving view is rebuilt from the compensated state, so a view that
/// published the short-lived upstream meanwhile cannot keep serving it.
#[tokio::test]
async fn create_from_oauth_draft_rollback_rebuilds_view_from_compensated_state() {
    let harness = harness().await;
    let state_token = completed_draft(&harness).await;
    sqlx::query(
        "CREATE TRIGGER fail_subscription_metadata_insert BEFORE INSERT ON \
         upstream_subscription_metadata_v1 \
         BEGIN SELECT RAISE(ABORT, 'injected subscription metadata failure'); END",
    )
    .execute(harness.storage.pool())
    .await
    .expect("install failure trigger");
    let generation_before = harness.holder.generation();
    let name = "oauth-draft-compensated";

    let (status, headers, bytes) = post(
        &harness.app,
        "/admin/v1/upstreams/from-oauth-draft",
        json!({ "state_token": state_token, "name": name }),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());
    assert!(headers.get(header::LOCATION).is_none());
    let body: Value = serde_json::from_slice(&bytes).expect("error body is JSON");
    assert_eq!(body["error"], "oauth_draft_create_failed");
    let detail = body["detail"].as_str().expect("error detail");
    assert!(
        detail.starts_with("subscription metadata storage failed: "),
        "{detail}"
    );
    assert!(!detail.contains("rollback delete failed"), "{detail}");
    assert!(
        UpstreamStore::get_by_name(&*harness.storage, name)
            .await
            .expect("stored upstream read")
            .is_none(),
        "the rollback removed the upstream"
    );
    assert!(
        harness.holder.generation() > generation_before,
        "the compensated state is rebuilt into the serving view"
    );
    assert!(
        !harness
            .holder
            .load()
            .upstreams_snapshot()
            .iter()
            .any(|upstream| upstream.name == name),
        "the serving view must not offer the rolled-back upstream"
    );
}

/// Scheduler whose backing pool is closed, so every task push fails with a
/// non-conflict error.
#[cfg(feature = "sqlite")]
async fn failing_scheduler() -> cc_lb_scheduler::admin::SchedulerAdminHandle {
    let pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("scheduler sqlite opens");
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .expect("apalis sqlite schema initializes");
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .expect("scheduler migrations initialize");
    pool.close().await;
    cc_lb_scheduler::admin::SchedulerAdminHandle::new(
        cc_lb_scheduler::worker::SchedulerBackend::Sqlite(
            cc_lb_scheduler::worker::SqliteSchedulerBackend::new(
                pool,
                Arc::new(cc_lb_clock::SystemClock),
            ),
        ),
    )
}

/// Reconnect seeding fails after the new credential committed: the existing
/// empty 500 is returned unchanged and the serving view is still rebuilt.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn oauth_complete_seed_failure_still_rebuilds_view() {
    let harness = harness_with_scheduler(Some(failing_scheduler().await)).await;
    // Warmup-enabled OAuth upstreams always get a bootstrap task after
    // reconnecting, whatever the granted credential mode.
    let upstream: UpstreamRecord = UpstreamStore::create(
        &*harness.storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "oauth-complete-seed-failure".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: true,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("create upstream");
    assert!(!view_contains_upstream(&harness.holder, upstream.id));

    let (status, start) = post_json(
        &harness.app,
        &format!("/admin/v1/upstreams/{}/oauth/start", upstream.id),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "oauth start: {start}");
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let generation_before = harness.holder.generation();

    let (status, headers, bytes) = post(
        &harness.app,
        &format!("/admin/v1/upstreams/{}/oauth/complete", upstream.id),
        json!({ "state_token": state_token, "code": code }),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(bytes.is_empty(), "the scheduler error keeps its empty body");
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());
    let stored = UpstreamStore::get_by_id(&*harness.storage, upstream.id)
        .await
        .expect("stored upstream read")
        .expect("upstream exists");
    assert!(
        stored.oauth_credentials.is_some(),
        "the credential exchange is committed despite the seed failure"
    );
    assert!(harness.holder.generation() > generation_before);
    assert!(
        view_contains_upstream(&harness.holder, upstream.id),
        "the serving view must offer the reconnected upstream"
    );
}
