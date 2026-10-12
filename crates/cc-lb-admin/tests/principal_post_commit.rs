//! Regression coverage for admin principal and key mutations after their
//! domain change commits (D1):
//!
//! - an audit-write failure still returns the unchanged
//!   `500 {"error":"audit_write_failed"}` but rebuilds the serving replica's
//!   dynamic view from the committed state;
//! - dropping the request future (client disconnect) while the post-commit
//!   audit or rebuild waits does not abandon that work;
//! - a stale principal `If-Match` answers the documented
//!   `409 {"error":"stale_revision","current_revision":N}` on every backend.
//!
//! Faults are injected through the database (SQLite trigger, a Postgres
//! `CREATE INDEX` holding its SHARE lock on `audit_log_v1`) or a test-scoped
//! rebinder gate; handlers and storage run unmodified.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode, header};
use cc_lb_admin::{AdminState, DynamicViewRebinder, router};
use cc_lb_config::Config;
use cc_lb_control::api_keys::key_store::KeyStore;
use cc_lb_control::api_keys::principal_view::{PrincipalStatus, PrincipalView};
use cc_lb_control::{DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot};
use cc_lb_domain::Upstream;
use cc_lb_storage_api::{
    AuditQueryScope, KeyStatus, MetaStore, PrincipalStore, Storage as DynStorage,
};
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection};
use tokio::sync::{mpsc, oneshot};
use tower::ServiceExt;
use uuid::Uuid;

use crate::admin_test_common;

/// Failure-only deadline: waits below never retry or relax assertions, the
/// guard only keeps a missing completion from hanging the test run.
const HANG_GUARD: Duration = Duration::from_secs(30);

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
            reason: "noop".to_owned(),
        })
    }
}

/// One armed pause of the next rebuild: reports entry, then waits for release.
struct RebuildGate {
    entered: mpsc::UnboundedSender<()>,
    release: oneshot::Receiver<()>,
}

/// Rebuilds the principal view from storage like the production builder, and
/// can pause one rebuild so a test can drop the request future meanwhile.
struct StorageRebinder {
    storage: Arc<dyn DynStorage>,
    gate: Mutex<Option<RebuildGate>>,
}

#[async_trait]
impl DynamicViewRebinder for StorageRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let gate = self.gate.lock().expect("rebuild gate lock").take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.release.await;
        }
        let principals = PrincipalStore::list(&*self.storage, 0, 1000, false).await?;
        Ok(DynamicViewBuilder::new(current_generation)
            .signer_factory(Arc::new(NoopSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(
                &principals,
                HashMap::new(),
            )))
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build())
    }
}

struct Harness {
    app: axum::Router,
    storage: Arc<dyn DynStorage>,
    holder: Arc<DynamicViewHolder>,
    rebinder: Arc<StorageRebinder>,
    key_store: Option<Arc<KeyStore>>,
}

impl Harness {
    fn new(storage: Arc<dyn DynStorage>, key_store: Option<Arc<KeyStore>>) -> Self {
        let config = Config::default();
        let holder = admin_test_common::dynamic_view_holder(&config);
        let rebinder = Arc::new(StorageRebinder {
            storage: storage.clone(),
            gate: Mutex::new(None),
        });
        let state = AdminState {
            config_path: None,
            startup_config_overrides: Default::default(),
            storage: Some(storage.clone()),
            key_store: key_store.clone(),
            aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
            limit_engine: admin_test_common::limit_engine(),
            lifecycle: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            dynamic_view: holder.clone(),
            config: Arc::new(config),
            dynamic_view_rebinder: Some(rebinder.clone()),
            scheduler: None,
            admin_auth: admin_test_common::static_token_auth("test-token"),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: cc_lb_admin::events::storage_tail_channel(),
            clock: Arc::new(cc_lb_clock::SystemClock),
        };
        Self {
            app: router(state),
            storage,
            holder,
            rebinder,
            key_store,
        }
    }

    /// Pauses the next rebuild. Returns the entry signal and the release.
    fn arm_rebuild_gate(&self) -> (mpsc::UnboundedReceiver<()>, oneshot::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.rebinder.gate.lock().expect("rebuild gate lock") = Some(RebuildGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    fn principal_status(&self, principal_id: Uuid) -> PrincipalStatus {
        self.holder
            .load()
            .principal_view
            .principal_status(&principal_id.to_string())
    }

    async fn create_principal(&self, name: &str) -> (Uuid, String) {
        let (status, headers, body) = call(
            &self.app,
            "POST",
            "/admin/v1/principals",
            Some(json!({ "name": name, "kind": "machine", "allowed_models": [], "default_limits": [] })),
            None,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let body: Value = serde_json::from_slice(&body).expect("create body is JSON");
        let id = body["id"]
            .as_str()
            .expect("created id")
            .parse()
            .expect("created id is a UUID");
        (id, etag(&headers))
    }

    async fn audit_routes(&self, principal_id: Uuid) -> Vec<String> {
        let principal_id = principal_id.to_string();
        self.storage
            .query_recent_audit(
                AuditQueryScope::Principal(&principal_id),
                0,
                u64::MAX,
                100,
                true,
            )
            .await
            .expect("audit query")
            .into_iter()
            .map(|entry| entry.route)
            .collect()
    }
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> (StatusCode, HeaderMap, Bytes) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer test-token");
    if let Some(etag) = if_match {
        builder = builder.header(header::IF_MATCH, etag);
    }
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router responds");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    (status, headers, bytes)
}

fn etag(headers: &HeaderMap) -> String {
    headers
        .get(header::ETAG)
        .expect("ETag header")
        .to_str()
        .expect("ETag is ASCII")
        .to_owned()
}

fn json_body(body: &Bytes) -> Value {
    serde_json::from_slice(body).expect("JSON body")
}

/// The existing audit-failure response, byte for byte, with no ETag and no
/// generation/rebind headers added by the internal rebuild.
fn assert_audit_write_failed(status: StatusCode, headers: &HeaderMap, body: &Bytes) {
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body.as_ref(), AUDIT_WRITE_FAILED_BODY);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).map(|v| v.as_bytes()),
        Some(b"application/json".as_slice())
    );
    assert!(headers.get(header::ETAG).is_none());
    assert!(headers.get("x-cc-lb-generation").is_none());
    assert!(headers.get("x-cc-lb-rebind-status").is_none());
}

async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let waiting = async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(HANG_GUARD, waiting)
        .await
        .unwrap_or_else(|_| panic!("{what} never happened"));
}

async fn sqlite_backend() -> (tempfile::TempDir, Arc<SqliteStorage>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage =
        admin_test_common::sqlite_storage(dir.path(), "principal-post-commit.sqlite").await;
    (dir, storage)
}

async fn fail_sqlite_audit_inserts(storage: &SqliteStorage) {
    sqlx::query(
        "CREATE TRIGGER fail_audit_insert BEFORE INSERT ON audit_log_v1 \
         BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END",
    )
    .execute(storage.pool())
    .await
    .expect("install audit failure trigger");
}

#[tokio::test]
async fn principal_disable_audit_failure_still_rebuilds_view() {
    let (_dir, sqlite) = sqlite_backend().await;
    let harness = Harness::new(sqlite.clone(), None);
    let (principal_id, principal_etag) = harness.create_principal("audit-fail-disable").await;
    assert_eq!(
        harness.principal_status(principal_id),
        PrincipalStatus::Active
    );
    let audit_before = harness.audit_routes(principal_id).await;

    fail_sqlite_audit_inserts(&sqlite).await;
    let (status, headers, body) = call(
        &harness.app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/disable"),
        None,
        Some(&principal_etag),
    )
    .await;

    assert_audit_write_failed(status, &headers, &body);
    let stored = PrincipalStore::get_by_id(&*harness.storage, principal_id)
        .await
        .expect("principal lookup")
        .expect("principal exists");
    assert!(
        !stored.enabled,
        "the disable committed before the audit write"
    );
    assert_eq!(harness.audit_routes(principal_id).await, audit_before);
    assert_eq!(
        harness.principal_status(principal_id),
        PrincipalStatus::Disabled,
        "the serving view must reflect the committed disable"
    );
}

#[tokio::test]
async fn principal_disable_completes_after_request_dropped_during_rebuild() {
    let (_dir, sqlite) = sqlite_backend().await;
    let harness = Harness::new(sqlite, None);
    let (principal_id, principal_etag) = harness.create_principal("dropped-disable").await;
    assert_eq!(
        harness.principal_status(principal_id),
        PrincipalStatus::Active
    );

    let (mut entered, release) = harness.arm_rebuild_gate();
    let app = harness.app.clone();
    let uri = format!("/admin/v1/principals/{principal_id}/disable");
    let request =
        tokio::spawn(async move { call(&app, "POST", &uri, None, Some(&principal_etag)).await });
    tokio::time::timeout(HANG_GUARD, entered.recv())
        .await
        .expect("post-commit rebuild never started")
        .expect("rebuild gate signals entry");

    // The client goes away: axum drops the in-flight handler future.
    request.abort();
    let outcome = request.await;
    assert!(outcome.is_err_and(|error| error.is_cancelled()));
    let _ = release.send(());

    wait_until("post-commit rebuild after the dropped request", || {
        harness.principal_status(principal_id) == PrincipalStatus::Disabled
    })
    .await;
    let disable_route = format!("/admin/v1/principals/{principal_id}/disable");
    assert!(
        harness
            .audit_routes(principal_id)
            .await
            .contains(&disable_route),
        "the committed disable is audited"
    );
}

#[tokio::test]
async fn key_issue_audit_failure_revokes_key_and_rebuilds_view() {
    let (_dir, sqlite) = sqlite_backend().await;
    let key_store = admin_test_common::key_store(sqlite.clone());
    let harness = Harness::new(sqlite.clone(), Some(key_store));
    let (principal_id, _) = harness.create_principal("audit-fail-key").await;
    let generation_before = harness.holder.generation();

    fail_sqlite_audit_inserts(&sqlite).await;
    let (status, headers, body) = call(
        &harness.app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/keys"),
        Some(json!({ "label": "audit failure" })),
        None,
    )
    .await;

    assert_audit_write_failed(status, &headers, &body);
    let keys = harness
        .key_store
        .as_ref()
        .expect("key store configured")
        .list_by_principal(&principal_id.to_string())
        .await
        .expect("key listing");
    assert_eq!(keys.len(), 1, "exactly the issued key is stored");
    assert_eq!(
        keys[0].status,
        KeyStatus::Revoked,
        "audit failure revokes the issued key"
    );
    assert!(
        harness.holder.generation() > generation_before,
        "the view is rebuilt from the post-revocation state"
    );
}

async fn assert_stale_principal_revisions_report_current_revision(harness: &Harness) {
    let (principal_id, created_etag) = harness.create_principal("stale-revision").await;
    let uri = format!("/admin/v1/principals/{principal_id}");

    let (status, headers, body) = call(
        &harness.app,
        "PATCH",
        &uri,
        Some(json!({ "name": "stale-revision-renamed" })),
        Some(&created_etag),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let current_revision = json_body(&body)["revision"]
        .as_u64()
        .expect("updated revision");
    let current_etag = etag(&headers);
    let stale = json!({ "error": "stale_revision", "current_revision": current_revision });

    let (status, _, body) = call(
        &harness.app,
        "PATCH",
        &uri,
        Some(json!({ "name": "stale-revision-again" })),
        Some(&created_etag),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json_body(&body), stale);

    let (status, _, body) = call(
        &harness.app,
        "POST",
        &format!("{uri}/disable"),
        None,
        Some(&created_etag),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json_body(&body), stale);

    let (status, _, body) = call(&harness.app, "DELETE", &uri, None, Some(&created_etag)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json_body(&body), stale);

    // Deleted principals keep their existing non-revision conflict.
    let (status, _, _) = call(&harness.app, "DELETE", &uri, None, Some(&current_etag)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, body) = call(&harness.app, "DELETE", &uri, None, Some(&current_etag)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json_body(&body)["error"], "storage_conflict");

    // A missing principal stays 404.
    let missing = Uuid::new_v4();
    let (status, _, body) = call(
        &harness.app,
        "POST",
        &format!("/admin/v1/principals/{missing}/disable"),
        None,
        Some(&current_etag),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body), json!({ "error": "unknown_principal" }));
}

#[tokio::test]
async fn sqlite_stale_principal_if_match_returns_stale_revision() {
    let (_dir, sqlite) = sqlite_backend().await;
    let harness = Harness::new(sqlite, None);
    assert_stale_principal_revisions_report_current_revision(&harness).await;
}

struct Pg {
    url: String,
    schema: String,
    storage: Arc<PostgresStorage>,
}

async fn postgres() -> Option<Pg> {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return None;
    };
    let schema = format!("principal_post_commit_{}", Uuid::new_v4().simple());
    let mut admin = sqlx::PgConnection::connect(&url)
        .await
        .expect("postgres admin connection");
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut admin)
        .await
        .expect("create schema");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(schema_options(&url, &schema))
        .await
        .expect("postgres pool");
    let storage = PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));
    storage.initialize().await.expect("postgres migrations");
    Some(Pg {
        url,
        schema,
        storage: Arc::new(storage),
    })
}

fn schema_options(url: &str, schema: &str) -> PgConnectOptions {
    PgConnectOptions::from_str(url)
        .expect("postgres url")
        .options([("search_path", schema)])
}

impl Pg {
    async fn drop_schema(self) {
        self.storage.pool().close().await;
        let mut admin = sqlx::PgConnection::connect(&self.url)
            .await
            .expect("postgres admin connection");
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&mut admin)
        .await
        .expect("drop schema");
    }

    /// Holds the SHARE lock a `CREATE INDEX` on `audit_log_v1` takes, which
    /// blocks audit inserts while the principal tables stay writable.
    async fn block_audit_inserts(&self) -> sqlx::PgConnection {
        let mut conn = sqlx::PgConnection::connect_with(&schema_options(&self.url, &self.schema))
            .await
            .expect("postgres lock connection");
        sqlx::query("BEGIN")
            .execute(&mut conn)
            .await
            .expect("begin lock transaction");
        sqlx::query("CREATE INDEX principal_post_commit_probe ON audit_log_v1 (ts, seq)")
            .execute(&mut conn)
            .await
            .expect("take audit_log_v1 SHARE lock");
        conn
    }

    async fn wait_until_audit_insert_blocked(&self) {
        let relation = format!("{}.audit_log_v1", self.schema);
        let waiting = async {
            loop {
                let blocked: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND relation = to_regclass($1))",
                )
                .bind(&relation)
                .fetch_one(self.storage.pool())
                .await
                .expect("pg_locks query");
                if blocked {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(HANG_GUARD, waiting)
            .await
            .expect("audit insert never blocked");
    }
}

#[tokio::test]
async fn postgres_principal_disable_completes_after_request_dropped_during_audit() {
    let Some(pg) = postgres().await else {
        return;
    };
    let harness = Harness::new(pg.storage.clone(), None);
    let (principal_id, principal_etag) = harness.create_principal("pg-dropped-disable").await;
    assert_eq!(
        harness.principal_status(principal_id),
        PrincipalStatus::Active
    );

    let mut lock = pg.block_audit_inserts().await;
    let app = harness.app.clone();
    let uri = format!("/admin/v1/principals/{principal_id}/disable");
    let request =
        tokio::spawn(async move { call(&app, "POST", &uri, None, Some(&principal_etag)).await });
    pg.wait_until_audit_insert_blocked().await;

    // The client goes away while the audit insert waits.
    request.abort();
    let outcome = request.await;
    assert!(outcome.is_err_and(|error| error.is_cancelled()));
    sqlx::query("ROLLBACK")
        .execute(&mut lock)
        .await
        .expect("release audit_log_v1 lock");
    drop(lock);

    wait_until(
        "post-commit audit and rebuild after the dropped request",
        || harness.principal_status(principal_id) == PrincipalStatus::Disabled,
    )
    .await;
    let disable_route = format!("/admin/v1/principals/{principal_id}/disable");
    assert!(
        harness
            .audit_routes(principal_id)
            .await
            .contains(&disable_route),
        "the committed disable is audited"
    );
    drop(harness);
    pg.drop_schema().await;
}

#[tokio::test]
async fn postgres_stale_principal_if_match_returns_stale_revision() {
    let Some(pg) = postgres().await else {
        return;
    };
    let harness = Harness::new(pg.storage.clone(), None);
    assert_stale_principal_revisions_report_current_revision(&harness).await;
    drop(harness);
    pg.drop_schema().await;
}
