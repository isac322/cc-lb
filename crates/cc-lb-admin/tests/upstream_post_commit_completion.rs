//! Upstream mutations finish their post-commit work (audit row, dynamic-view
//! rebuild) even when the audit write fails or the request future is dropped
//! after the domain change has committed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::ports::{
    SubscriptionQuotaIngestionPort, WarmupAttemptInput, WarmupAttemptOutcomeSnapshot, WarmupPort,
};
use cc_lb_admin::{AdminPorts, AdminState, DynamicViewRebinder, router};
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_clock::Clock;
use cc_lb_config::Config;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot};
use cc_lb_domain::Upstream;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditQueryScope, AuditStore, UpstreamCreate, UpstreamStore, WarmupAttemptOutcome,
    WarmupSuccessReason,
};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::admin_test_common;

/// Fails the test instead of hanging when an expected event never happens.
const HANG_GUARD: Duration = Duration::from_secs(30);

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

/// Rebuilds the view's upstream records from storage. It installs the view
/// itself before reporting the build so a test observing the report never
/// races the admin-side install of the same generation.
struct StorageRebinder {
    storage: Arc<SqliteStorage>,
    holder: Arc<DynamicViewHolder>,
    rebuilt: UnboundedSender<u64>,
}

#[async_trait]
impl DynamicViewRebinder for StorageRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let upstreams = UpstreamStore::list(&*self.storage, None, 1000).await?;
        let view = DynamicViewBuilder::new(current_generation)
            .signer_factory(Arc::new(NoopSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .upstream_records(upstreams)
            .build();
        self.holder.try_store_if_newer(view.clone());
        let _ = self.rebuilt.send(view.generation);
        Ok(view)
    }
}

/// Wall clock whose next read after `arm` blocks until the test releases it.
/// The admin audit writer reads the clock right before inserting the audit
/// row, so an armed clock holds the audit insert of the next mutation.
struct GateClock {
    armed: AtomicBool,
    reached: UnboundedSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl GateClock {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl Clock for GateClock {
    fn now(&self) -> SystemTime {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.send(()).expect("test awaits the gate");
            self.release
                .lock()
                .expect("gate receiver lock")
                .recv()
                .expect("test releases the gate");
        }
        SystemTime::now()
    }
}

struct Harness {
    _dir: tempfile::TempDir,
    storage: Arc<SqliteStorage>,
    app: axum::Router,
    holder: Arc<DynamicViewHolder>,
    rebuilt: UnboundedReceiver<u64>,
}

async fn harness(clock: Arc<dyn Clock>) -> Harness {
    harness_with(clock, None, None).await
}

/// Builds the admin app. With `audit_inserted`, the app's storage reports
/// every `audit_log_v1` insert through a SQLite update hook installed on each
/// pooled connection; the hook fires inside the inserting statement, while
/// that connection holds the write lock.
async fn harness_with(
    clock: Arc<dyn Clock>,
    lifecycle: Option<AdminPorts>,
    audit_inserted: Option<UnboundedSender<()>>,
) -> Harness {
    let dir = tempfile::tempdir().expect("temp admin dir");
    let initialized =
        admin_test_common::sqlite_storage(dir.path(), "upstream_post_commit.sqlite").await;
    let storage = match audit_inserted {
        None => initialized,
        Some(audit_inserted) => {
            hooked_sqlite_storage(
                &dir.path().join("upstream_post_commit.sqlite"),
                audit_inserted,
            )
            .await
        }
    };
    let config = Config::default();
    let holder = admin_test_common::dynamic_view_holder(&config);
    let (rebuilt_tx, rebuilt) = unbounded_channel();
    let rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(StorageRebinder {
        storage: storage.clone(),
        holder: holder.clone(),
        rebuilt: rebuilt_tx,
    });
    let state = AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage.clone())),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle,
        dynamic_view: holder.clone(),
        config: Arc::new(config),
        dynamic_view_rebinder: Some(rebinder),
        scheduler: None,
        admin_auth: admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
            .expect("production Anthropic origin parses"),
    };
    Harness {
        _dir: dir,
        storage,
        app: router(state),
        holder,
        rebuilt,
    }
}

/// Opens the already-migrated database with the production connection
/// settings plus an update hook that reports audit inserts.
async fn hooked_sqlite_storage(
    database: &std::path::Path,
    audit_inserted: UnboundedSender<()>,
) -> Arc<SqliteStorage> {
    let options = SqliteConnectOptions::new()
        .filename(database)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .after_connect(move |connection, _meta| {
            let audit_inserted = audit_inserted.clone();
            Box::pin(async move {
                connection
                    .lock_handle()
                    .await?
                    .set_update_hook(move |change| {
                        if change.table == "audit_log_v1" {
                            let _ = audit_inserted.send(());
                        }
                    });
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .expect("hooked admin sqlite opens");
    Arc::new(SqliteStorage::new(pool, Arc::new(cc_lb_clock::SystemClock)))
}

fn admin_request(
    method: &str,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer test-token");
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(if_match) = if_match {
        builder = builder.header(header::IF_MATCH, if_match);
    }
    builder
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .expect("request builds")
}

async fn create_upstream(harness: &mut Harness, name: &str) -> Uuid {
    let response = harness
        .app
        .clone()
        .oneshot(admin_request(
            "POST",
            "/admin/v1/upstreams",
            Some(json!({
                "name": name,
                "kind": "anthropic_api_key",
                "base_url": "https://example.com",
                "api_key_value": "sk-ant-post-commit",
                "skip_preflight": true
            })),
            None,
        ))
        .await
        .expect("create request");
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("create body")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).expect("create body is JSON");
    harness
        .rebuilt
        .recv()
        .await
        .expect("create rebuilds the view");
    body["id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("created upstream id")
}

fn view_upstream_enabled(holder: &DynamicViewHolder, id: Uuid) -> Option<bool> {
    holder
        .load()
        .upstreams_snapshot()
        .iter()
        .find(|upstream| upstream.id == id)
        .map(|upstream| upstream.enabled)
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

/// Sends a disabling mutation while every audit insert fails and checks the
/// accepted `500 audit_write_failed` response is unchanged while the serving
/// view still reflects the committed disable.
async fn assert_disable_with_failed_audit_rebuilds_view(
    method: &str,
    uri_suffix: &str,
    body: Option<Value>,
) {
    let mut harness = harness(Arc::new(cc_lb_clock::SystemClock)).await;
    let id = create_upstream(&mut harness, "post-commit-audit-failure").await;
    assert_eq!(view_upstream_enabled(&harness.holder, id), Some(true));
    fail_audit_inserts(&harness.storage).await;

    let response = harness
        .app
        .clone()
        .oneshot(admin_request(
            method,
            &format!("/admin/v1/upstreams/{id}{uri_suffix}"),
            body,
            Some("W/\"1\""),
        ))
        .await
        .expect("disable request");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get("x-cc-lb-generation").is_none());
    assert!(response.headers().get("x-cc-lb-rebind-status").is_none());
    assert!(response.headers().get(header::ETAG).is_none());
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("error body")
        .to_bytes();
    assert_eq!(&bytes[..], br#"{"error":"audit_write_failed"}"#);

    let stored = UpstreamStore::get_by_id(&*harness.storage, id)
        .await
        .expect("stored upstream read")
        .expect("upstream exists");
    assert!(!stored.enabled, "the disable is committed despite the 500");
    assert_eq!(
        view_upstream_enabled(&harness.holder, id),
        Some(false),
        "the serving view must stop offering the committed-disabled upstream"
    );
}

#[tokio::test]
async fn upstream_disable_audit_failure_still_rebuilds_view() {
    assert_disable_with_failed_audit_rebuilds_view("POST", "/disable", None).await;
}

#[tokio::test]
async fn upstream_patch_disable_audit_failure_still_rebuilds_view() {
    assert_disable_with_failed_audit_rebuilds_view("PATCH", "", Some(json!({ "enabled": false })))
        .await;
}

/// Hides the details table so every `get_api_key_details` read fails while
/// spec writes, audit inserts, and view rebuilds keep working.
async fn fail_details_reads(storage: &SqliteStorage) {
    sqlx::query(
        "ALTER TABLE upstream_api_key_details_v1 RENAME TO upstream_api_key_details_v1_hidden",
    )
    .execute(storage.pool())
    .await
    .expect("details table hidden");
}

/// Sends a disabling mutation while the API-key details projection cannot be
/// read and checks the mutation still answers 200, omits the display-only
/// `api_key_access` projection, and the serving view still reflects the
/// committed disable.
async fn assert_disable_with_failed_details_read_rebuilds_view(
    method: &str,
    uri_suffix: &str,
    body: Option<Value>,
) {
    let mut harness = harness(Arc::new(cc_lb_clock::SystemClock)).await;
    let id = create_upstream(&mut harness, "post-commit-details-failure").await;
    assert_eq!(view_upstream_enabled(&harness.holder, id), Some(true));
    fail_details_reads(&harness.storage).await;

    let response = harness
        .app
        .clone()
        .oneshot(admin_request(
            method,
            &format!("/admin/v1/upstreams/{id}{uri_suffix}"),
            body,
            Some("W/\"1\""),
        ))
        .await
        .expect("disable request");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("x-cc-lb-generation").is_some());
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).expect("response is JSON");
    assert_eq!(body["enabled"], false);
    assert_eq!(
        body["api_key_access"],
        Value::Null,
        "a failed details read drops the display-only projection"
    );

    let stored = UpstreamStore::get_by_id(&*harness.storage, id)
        .await
        .expect("stored upstream read")
        .expect("upstream exists");
    assert!(!stored.enabled, "the disable is committed");
    assert_eq!(
        view_upstream_enabled(&harness.holder, id),
        Some(false),
        "the serving view must stop offering the committed-disabled upstream"
    );
}

#[tokio::test]
async fn upstream_disable_details_read_failure_still_rebuilds_view() {
    assert_disable_with_failed_details_read_rebuilds_view("POST", "/disable", None).await;
}

#[tokio::test]
async fn upstream_patch_disable_details_read_failure_still_rebuilds_view() {
    assert_disable_with_failed_details_read_rebuilds_view(
        "PATCH",
        "",
        Some(json!({ "enabled": false })),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_disable_completes_audit_and_rebuild_after_request_is_dropped() {
    let (reached_tx, mut reached) = unbounded_channel();
    let (release, release_rx) = mpsc::channel();
    let clock = Arc::new(GateClock {
        armed: AtomicBool::new(false),
        reached: reached_tx,
        release: Mutex::new(release_rx),
    });
    let mut harness = harness(clock.clone()).await;
    let id = create_upstream(&mut harness, "post-commit-dropped-request").await;
    assert_eq!(view_upstream_enabled(&harness.holder, id), Some(true));

    clock.arm();
    let request = tokio::spawn(harness.app.clone().oneshot(admin_request(
        "POST",
        &format!("/admin/v1/upstreams/{id}/disable"),
        None,
        Some("W/\"1\""),
    )));
    tokio::time::timeout(HANG_GUARD, reached.recv())
        .await
        .expect("audit write reaches the gate")
        .expect("gate reports");

    // The disable is committed and its audit insert is held: the client
    // disconnects, so the request future is dropped.
    let stored = UpstreamStore::get_by_id(&*harness.storage, id)
        .await
        .expect("stored upstream read")
        .expect("upstream exists");
    assert!(
        !stored.enabled,
        "the disable committed before the audit gate"
    );
    request.abort();
    release.send(()).expect("gate is waiting");
    let joined = request.await;
    assert!(
        joined.is_err_and(|error| error.is_cancelled()),
        "the request future was dropped"
    );

    let generation = tokio::time::timeout(HANG_GUARD, harness.rebuilt.recv())
        .await
        .expect("the dropped mutation still rebuilds the view")
        .expect("rebinder reports the build");
    assert_eq!(harness.holder.generation(), generation);
    assert_eq!(
        view_upstream_enabled(&harness.holder, id),
        Some(false),
        "the serving view must stop offering the committed-disabled upstream"
    );

    let disable_route = format!("/admin/v1/upstreams/{id}/disable");
    let entries = harness
        .storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 10, false)
        .await
        .expect("audit query");
    assert!(
        entries.iter().any(|entry| {
            entry.route == disable_route
                && entry
                    .admin_action
                    .as_deref()
                    .is_some_and(|action| action.starts_with("upstream_disable"))
        }),
        "the dropped mutation still writes its audit row"
    );
}

/// Warmup port that reports a committed successful attempt.
struct CommittedWarmupPort;

#[async_trait]
impl WarmupPort for CommittedWarmupPort {
    async fn record_attempt(&self, _input: WarmupAttemptInput<'_>) -> WarmupAttemptOutcomeSnapshot {
        WarmupAttemptOutcomeSnapshot {
            outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
            cycle_key: Some(1),
            error_detail: None,
        }
    }
}

/// Quota ingestion that holds the first step after the attempt commit until
/// the test releases it.
struct GatedQuotaIngestion {
    reached: UnboundedSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl SubscriptionQuotaIngestionPort for GatedQuotaIngestion {
    fn ingest_subscription_quota_headers(
        &self,
        _headers: &axum::http::HeaderMap,
        _upstream_id: Uuid,
        _observed_at: SystemTime,
    ) {
        self.reached.send(()).expect("test awaits the gate");
        self.release
            .lock()
            .expect("gate receiver lock")
            .recv()
            .expect("test releases the gate");
    }
}

async fn seed_warmup_oauth_upstream(storage: &SqliteStorage, base_url: &str) -> Uuid {
    let upstream = UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "post-commit-fire-now".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse(base_url).expect("base url parses")),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: true,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create upstream");
    let bundle = OAuthTokenBundle {
        access_token: "sk-ant-oat01-post-commit".to_owned(),
        refresh_token: "sk-ant-ort01-post-commit".to_owned(),
        expires_at_unix_secs: 4_000_000_000,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["user:inference".to_owned()],
        never_refresh: true,
    };
    let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        &AeadService::from_master_key([0; 32]),
        &bundle,
        upstream.id.as_bytes(),
    )
    .expect("encrypt bundle");
    storage
        .store_oauth_tokens(upstream.id, upstream.revision, encrypted, true)
        .await
        .expect("store oauth tokens")
        .id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fire_now_completes_persistence_and_audit_after_request_is_dropped() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&provider)
        .await;
    let (reached_tx, mut reached) = unbounded_channel();
    let (release, release_rx) = mpsc::channel();
    let ports = AdminPorts {
        warmup: Some(Arc::new(CommittedWarmupPort)),
        subscription_quota_ingestion: Some(Arc::new(GatedQuotaIngestion {
            reached: reached_tx,
            release: Mutex::new(release_rx),
        })),
        ..AdminPorts::default()
    };
    let (audit_inserted_tx, mut audit_inserted) = unbounded_channel();
    let harness = harness_with(
        Arc::new(cc_lb_clock::SystemClock),
        Some(ports),
        Some(audit_inserted_tx),
    )
    .await;
    let id = seed_warmup_oauth_upstream(&harness.storage, &provider.uri()).await;

    let request = tokio::spawn(harness.app.clone().oneshot(admin_request(
        "POST",
        &format!("/admin/v1/upstreams/{id}/warmup/fire-now"),
        None,
        None,
    )));
    tokio::time::timeout(HANG_GUARD, reached.recv())
        .await
        .expect("post-commit quota ingestion reaches the gate")
        .expect("gate reports");

    // The warmup attempt is committed and the follow-up persistence is held:
    // the client disconnects, so the request future is dropped.
    request.abort();
    release.send(()).expect("gate is waiting");
    let joined = request.await;
    assert!(
        joined.is_err_and(|error| error.is_cancelled()),
        "the request future was dropped"
    );

    // The audit row is the continuation's last write. The update hook fires
    // while the inserting connection holds the SQLite write lock; taking the
    // write lock afterwards therefore waits for that insert to commit.
    tokio::time::timeout(HANG_GUARD, audit_inserted.recv())
        .await
        .expect("the dropped fire-now still writes its audit row")
        .expect("audit hook reports");
    drop(
        harness
            .storage
            .begin_immediate()
            .await
            .expect("write lock after the audit insert commits"),
    );
    let fire_now_route = format!("/admin/v1/upstreams/{id}/warmup/fire-now");
    let entries = harness
        .storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 10, false)
        .await
        .expect("audit query");
    let audited = entries
        .into_iter()
        .find(|entry| entry.route == fire_now_route)
        .expect("fire-now audit row is committed");
    assert_eq!(
        audited.admin_action.as_deref(),
        Some("upstream_warmup_fire_now")
    );
    assert_eq!(audited.status, StatusCode::OK.as_u16());

    let stored = UpstreamStore::get_by_id(&*harness.storage, id)
        .await
        .expect("stored upstream read")
        .expect("upstream exists");
    assert!(
        stored.last_warmup_at_unix_secs.is_some(),
        "the dropped fire-now still records its warmup status before the audit"
    );
}
