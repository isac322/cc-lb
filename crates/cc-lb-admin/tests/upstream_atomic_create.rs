//! Regression coverage for isac322/cc-lb#852: admin upstream creation must
//! persist the spec row and its credential atomically, so no reader, listener,
//! or failed request can ever observe a credential-less upstream.
//!
//! Faults are injected with database triggers and table locks on the real
//! adapters, so the production handlers and storage code run unmodified. The
//! tests only use HTTP endpoints, SQL, and storage reads.

use std::str::FromStr;
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_aead::AeadService;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_storage_api::{MetaStore, Storage as DynStorage, UpstreamRecord, UpstreamStore};
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_storage_sqlite::SqliteStorage;
use mock_anthropic_oauth_server::AppState as MockOAuthState;
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgListener, PgPool, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection};
use tower::ServiceExt;
use uuid::Uuid;

use crate::admin_test_common;
use crate::v1_oauth::{
    MASTER_KEY, TEST_NOW_UNIX_SECS, authorize_code, json_response, spawn_mock_anthropic,
    test_config,
};

const UPSTREAM_CHANNEL: &str = "cclb_upstream_changed";
const API_KEY_PLAINTEXT: &str = "sk-ant-api03-atomic-create";

/// Failure-only deadline for waits that must never retry or relax assertions.
/// Postgres lock operations in these tests complete in far less time; the
/// timeout only prevents a stuck create from hanging the test run.
const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Fault {
    ApiKeyCredential,
    OAuthCredential,
    SoftDeleteRollback,
    HardDeleteRollback,
}

enum Backend {
    Sqlite {
        _dir: tempfile::TempDir,
        storage: Arc<SqliteStorage>,
    },
    Postgres {
        url: String,
        schema: String,
        storage: Arc<PostgresStorage>,
    },
}

impl Backend {
    async fn sqlite() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = admin_test_common::sqlite_storage_with_clock(
            dir.path(),
            "atomic-create.sqlite",
            test_clock(),
        )
        .await;
        Self::Sqlite { _dir: dir, storage }
    }

    async fn postgres() -> Option<Self> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return None;
        };
        let schema = format!("upstream_atomic_create_{}", Uuid::new_v4().simple());
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
        let storage = PostgresStorage::new(pool, test_clock());
        storage.initialize().await.expect("postgres migrations");
        Some(Self::Postgres {
            url,
            schema,
            storage: Arc::new(storage),
        })
    }

    fn storage(&self) -> Arc<dyn DynStorage> {
        match self {
            Self::Sqlite { storage, .. } => storage.clone(),
            Self::Postgres { storage, .. } => storage.clone(),
        }
    }

    async fn inject(&self, faults: &[Fault]) {
        match self {
            Self::Sqlite { storage, .. } => {
                for fault in faults {
                    let sql = match fault {
                        Fault::ApiKeyCredential => {
                            "CREATE TRIGGER fail_api_key_insert BEFORE INSERT ON upstream_api_key_secret_v1 BEGIN SELECT RAISE(ABORT, 'injected credential failure'); END"
                        }
                        Fault::OAuthCredential => {
                            "CREATE TRIGGER fail_oauth_insert BEFORE INSERT ON upstream_oauth_token_v1 BEGIN SELECT RAISE(ABORT, 'injected credential failure'); END"
                        }
                        Fault::SoftDeleteRollback => {
                            "CREATE TRIGGER fail_soft_delete BEFORE UPDATE OF deleted_at ON upstream_spec_v1 WHEN NEW.deleted_at IS NOT NULL BEGIN SELECT RAISE(ABORT, 'injected rollback failure'); END"
                        }
                        Fault::HardDeleteRollback => {
                            "CREATE TRIGGER fail_hard_delete BEFORE DELETE ON upstream_spec_v1 BEGIN SELECT RAISE(ABORT, 'injected rollback failure'); END"
                        }
                    };
                    sqlx::query(sql)
                        .execute(storage.pool())
                        .await
                        .expect("install sqlite trigger");
                }
            }
            Self::Postgres { storage, .. } => {
                sqlx::query(
                    "CREATE FUNCTION fail_injected() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected %', TG_ARGV[0]; END $$",
                )
                .execute(storage.pool())
                .await
                .expect("install postgres trigger function");
                for fault in faults {
                    let sql = match fault {
                        Fault::ApiKeyCredential => {
                            "CREATE TRIGGER fail_api_key_insert BEFORE INSERT ON upstream_api_key_secret_v1 FOR EACH ROW EXECUTE FUNCTION fail_injected('credential failure')"
                        }
                        Fault::OAuthCredential => {
                            "CREATE TRIGGER fail_oauth_insert BEFORE INSERT ON upstream_oauth_token_v1 FOR EACH ROW EXECUTE FUNCTION fail_injected('credential failure')"
                        }
                        Fault::SoftDeleteRollback => {
                            "CREATE TRIGGER fail_soft_delete BEFORE UPDATE ON upstream_spec_v1 FOR EACH ROW WHEN (NEW.deleted_at IS NOT NULL AND OLD.deleted_at IS NULL) EXECUTE FUNCTION fail_injected('rollback failure')"
                        }
                        Fault::HardDeleteRollback => {
                            "CREATE TRIGGER fail_hard_delete BEFORE DELETE ON upstream_spec_v1 FOR EACH ROW EXECUTE FUNCTION fail_injected('rollback failure')"
                        }
                    };
                    sqlx::query(sql)
                        .execute(storage.pool())
                        .await
                        .expect("install postgres trigger");
                }
            }
        }
    }

    async fn drop_schema(self) {
        if let Self::Postgres {
            url,
            schema,
            storage,
        } = self
        {
            storage.pool().close().await;
            let mut admin = sqlx::PgConnection::connect(&url)
                .await
                .expect("postgres admin connection");
            sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
                .execute(&mut admin)
                .await
                .expect("drop schema");
        }
    }
}

fn schema_options(url: &str, schema: &str) -> PgConnectOptions {
    PgConnectOptions::from_str(url)
        .expect("postgres url")
        .options([("search_path", schema)])
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS))
}

async fn admin_app(storage: Arc<dyn DynStorage>) -> axum::Router {
    let clock = test_clock();
    let (oauth_addr, _oauth_state) = spawn_mock_anthropic(MockOAuthState::default()).await;
    let config = test_config(oauth_addr, None);
    router(AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage),
        key_store: None,
        aead: Arc::new(AeadService::from_master_key(MASTER_KEY)),
        limit_engine: admin_test_common::limit_engine_with_clock(clock.clone()),
        lifecycle: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        dynamic_view_rebinder: None,
        config: Arc::new(config),
        scheduler: None,
        admin_auth: admin_test_common::static_token_auth("test-token"),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
    })
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", "Bearer test-token");
    let body = match body {
        Some(body) => {
            builder = builder.header("Content-Type", "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("request succeeds");
    json_response(response).await
}

async fn completed_draft(app: &axum::Router) -> String {
    let (status, start) = call(app, "POST", "/admin/v1/oauth/draft/start", Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK, "draft start: {start}");
    let state_token = start["state_token"]
        .as_str()
        .expect("state token")
        .to_owned();
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let (status, complete) = call(
        app,
        "POST",
        "/admin/v1/oauth/draft/complete",
        Some(json!({ "state_token": state_token, "code": code })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "draft complete: {complete}");
    state_token
}

async fn listed_names(app: &axum::Router) -> Vec<String> {
    let (status, list) = call(app, "GET", "/admin/v1/upstreams", None).await;
    assert_eq!(status, StatusCode::OK, "list upstreams: {list}");
    list["upstreams"]
        .as_array()
        .expect("upstreams array")
        .iter()
        .filter_map(|upstream| upstream["name"].as_str().map(str::to_owned))
        .collect()
}

#[derive(Clone, Copy)]
enum Kind {
    ApiKey,
    OAuth,
}

impl Kind {
    fn has_credential(self, record: &UpstreamRecord) -> bool {
        match self {
            Self::ApiKey => record.api_key_ciphertext.is_some(),
            Self::OAuth => record.oauth_credentials.is_some(),
        }
    }

    fn credential_table(self) -> &'static str {
        match self {
            Self::ApiKey => "upstream_api_key_secret_v1",
            Self::OAuth => "upstream_oauth_token_v1",
        }
    }

    fn credential_fault(self) -> Fault {
        match self {
            Self::ApiKey => Fault::ApiKeyCredential,
            Self::OAuth => Fault::OAuthCredential,
        }
    }

    /// The cleanup the pre-#852 two-phase handlers fell back on; blocking it
    /// stands in for losing the process between the two phases.
    fn rollback_fault(self) -> Fault {
        match self {
            Self::ApiKey => Fault::SoftDeleteRollback,
            Self::OAuth => Fault::HardDeleteRollback,
        }
    }

    async fn create_request(self, app: &axum::Router, name: &str) -> (&'static str, Value) {
        match self {
            Self::ApiKey => (
                "/admin/v1/upstreams",
                json!({
                    "name": name,
                    "kind": "anthropic_api_key",
                    "api_key_value": API_KEY_PLAINTEXT,
                }),
            ),
            Self::OAuth => (
                "/admin/v1/upstreams/from-oauth-draft",
                json!({ "state_token": completed_draft(app).await, "name": name }),
            ),
        }
    }
}

/// A failing credential insert must abort the whole create: the request fails
/// and no upstream with the requested name is stored or listed, even when the
/// handler's cleanup path is unavailable.
async fn credential_insert_failure_leaves_no_row(backend: &Backend, kind: Kind) {
    let storage = backend.storage();
    let app = admin_app(storage.clone()).await;
    let name = format!("atomic-{}", Uuid::new_v4().simple());
    let (uri, body) = kind.create_request(&app, &name).await;
    backend
        .inject(&[kind.credential_fault(), kind.rollback_fault()])
        .await;

    let (status, response) = call(&app, "POST", uri, Some(body)).await;
    assert!(
        status.is_server_error(),
        "create with failing credential insert must be a server error: {status} {response}"
    );

    let stored = UpstreamStore::get_by_name(storage.as_ref(), &name)
        .await
        .expect("get_by_name");
    assert!(
        stored.is_none(),
        "failed create left an upstream row behind: {stored:?}"
    );
    assert!(
        !listed_names(&app).await.contains(&name),
        "failed create is visible through the admin list"
    );
}

/// Waits until some backend is queued on a lock for `relation`. Every
/// iteration is a database round trip, so the loop observes lock state rather
/// than racing a timer. The deadline only guards against a stuck create: the
/// poll must succeed far sooner in any healthy run.
async fn wait_until_lock_waiter(pool: &PgPool, relation: &str) {
    let waiting = async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND relation = to_regclass($1))",
            )
            .bind(relation)
            .fetch_one(pool)
            .await
            .expect("pg_locks query");
            if waiting {
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    match tokio::time::timeout(HANG_GUARD, waiting).await {
        Ok(()) => {}
        Err(_) => {
            panic!("no lock waiter for {relation} within {HANG_GUARD:?}; the create may be stuck")
        }
    }
}

/// Sends a sentinel on the upstream channel and returns every payload that
/// arrived before it. Postgres delivers notifications in commit order, so the
/// result holds exactly the notifications committed before the sentinel. The
/// deadline only guards against a lost connection or a create that never
/// commits; a healthy run drains far sooner.
async fn drain_until_sentinel(listener: &mut PgListener, pool: &PgPool) -> Vec<String> {
    let sentinel = format!("sentinel-{}", Uuid::new_v4().simple());
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(UPSTREAM_CHANNEL)
        .bind(&sentinel)
        .execute(pool)
        .await
        .expect("sentinel notify");
    let drain = async {
        let mut payloads = Vec::new();
        loop {
            match listener.try_recv().await {
                Ok(Some(notification)) => {
                    if notification.payload() == sentinel {
                        return payloads;
                    }
                    payloads.push(notification.payload().to_owned());
                }
                Ok(None) => panic!(
                    "{UPSTREAM_CHANNEL} listener connection lost before the sentinel arrived"
                ),
                Err(error) => panic!("{UPSTREAM_CHANNEL} listener recv failed: {error}"),
            }
        }
    };
    match tokio::time::timeout(HANG_GUARD, drain).await {
        Ok(payloads) => payloads,
        Err(_) => panic!("sentinel not received on {UPSTREAM_CHANNEL} within {HANG_GUARD:?}"),
    }
}

/// Holds the credential table lock while the create is in flight. Nothing may
/// become visible or be announced until the single transaction commits; after
/// it does, exactly one `cclb_upstream_changed` notification names the new
/// upstream, and the upstream it names already carries its credential.
async fn postgres_single_notify_with_credential_present(kind: Kind) {
    let Some(backend) = Backend::postgres().await else {
        return;
    };
    let Backend::Postgres {
        url,
        schema,
        storage,
    } = &backend
    else {
        unreachable!("Backend::postgres returns the Postgres variant");
    };
    let pool = storage.pool().clone();
    let app = admin_app(backend.storage()).await;
    let name = format!("atomic-{}", Uuid::new_v4().simple());
    let (uri, body) = kind.create_request(&app, &name).await;

    let mut listener = PgListener::connect(url).await.expect("listener");
    listener.listen(UPSTREAM_CHANNEL).await.expect("listen");

    let mut locker = sqlx::PgConnection::connect_with(&schema_options(url, schema))
        .await
        .expect("locker connection");
    let mut lock_tx = locker.begin().await.expect("lock transaction");
    sqlx::query(AssertSqlSafe(format!(
        "LOCK TABLE {} IN EXCLUSIVE MODE",
        kind.credential_table()
    )))
    .execute(&mut *lock_tx)
    .await
    .expect("lock credential table");

    let request_app = app.clone();
    let mut request =
        tokio::spawn(async move { call(&request_app, "POST", uri, Some(body)).await });
    let relation = format!("{schema}.{}", kind.credential_table());
    tokio::select! {
        () = wait_until_lock_waiter(&pool, &relation) => {}
        finished = &mut request => {
            panic!("create finished without waiting on {relation}: {finished:?}");
        }
    }

    assert!(
        UpstreamStore::get_by_name(storage.as_ref(), &name)
            .await
            .expect("get_by_name")
            .is_none(),
        "upstream is readable while its credential write is blocked"
    );
    assert!(
        !listed_names(&app).await.contains(&name),
        "upstream is listed while its credential write is blocked"
    );
    for payload in drain_until_sentinel(&mut listener, &pool).await {
        let Ok(id) = payload.parse::<Uuid>() else {
            continue;
        };
        let observed = UpstreamStore::get_by_id(storage.as_ref(), id)
            .await
            .expect("get_by_id");
        assert!(
            observed.is_none(),
            "{UPSTREAM_CHANNEL} announced upstream {id} while its credential write is blocked: {observed:?}"
        );
    }

    lock_tx
        .commit()
        .await
        .expect("release credential table lock");
    let (status, response) = request.await.expect("request task");
    assert_eq!(status, StatusCode::CREATED, "create response: {response}");
    let upstream_id: Uuid = response["id"]
        .as_str()
        .expect("created upstream id")
        .parse()
        .expect("created upstream id is a uuid");

    let mut announcements = 0_usize;
    for payload in drain_until_sentinel(&mut listener, &pool).await {
        if payload != upstream_id.to_string() {
            continue;
        }
        announcements += 1;
        let observed = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
            .await
            .expect("get_by_id")
            .expect("announced upstream exists");
        assert!(
            kind.has_credential(&observed),
            "announced upstream has no credential: {observed:?}"
        );
    }
    assert_eq!(
        announcements, 1,
        "atomic create must announce the upstream exactly once"
    );

    drop(listener);
    drop(locker);
    backend.drop_schema().await;
}

#[tokio::test]
async fn test_sqlite_api_key_credential_insert_failure_leaves_no_row() {
    let backend = Backend::sqlite().await;
    credential_insert_failure_leaves_no_row(&backend, Kind::ApiKey).await;
}

#[tokio::test]
async fn test_sqlite_oauth_credential_insert_failure_leaves_no_row() {
    let backend = Backend::sqlite().await;
    credential_insert_failure_leaves_no_row(&backend, Kind::OAuth).await;
}

#[tokio::test]
async fn test_postgres_api_key_credential_insert_failure_leaves_no_row() {
    let Some(backend) = Backend::postgres().await else {
        return;
    };
    credential_insert_failure_leaves_no_row(&backend, Kind::ApiKey).await;
    backend.drop_schema().await;
}

#[tokio::test]
async fn test_postgres_oauth_credential_insert_failure_leaves_no_row() {
    let Some(backend) = Backend::postgres().await else {
        return;
    };
    credential_insert_failure_leaves_no_row(&backend, Kind::OAuth).await;
    backend.drop_schema().await;
}

#[tokio::test]
async fn test_postgres_api_key_single_notify_with_credential_present() {
    postgres_single_notify_with_credential_present(Kind::ApiKey).await;
}

#[tokio::test]
async fn test_postgres_oauth_single_notify_with_credential_present() {
    postgres_single_notify_with_credential_present(Kind::OAuth).await;
}

#[tokio::test]
async fn api_key_ciphertext_bound_to_preallocated_id() {
    let backend = Backend::sqlite().await;
    let storage = backend.storage();
    let app = admin_app(storage.clone()).await;
    let name = format!("atomic-{}", Uuid::new_v4().simple());
    let (uri, body) = Kind::ApiKey.create_request(&app, &name).await;

    let (status, response) = call(&app, "POST", uri, Some(body)).await;
    assert_eq!(status, StatusCode::CREATED, "create response: {response}");
    let record = UpstreamStore::get_by_name(storage.as_ref(), &name)
        .await
        .expect("get_by_name")
        .expect("created upstream is stored");
    assert_eq!(response["id"], record.id.to_string());
    let ciphertext = record
        .api_key_ciphertext
        .as_deref()
        .expect("api key ciphertext is stored with the upstream");

    let aead = AeadService::from_master_key(MASTER_KEY);
    let plaintext = aead
        .decrypt(ciphertext, record.id.as_bytes())
        .expect("ciphertext opens with AAD = persisted upstream id");
    assert_eq!(plaintext, API_KEY_PLAINTEXT.as_bytes());
    assert!(
        aead.decrypt(ciphertext, Uuid::nil().as_bytes()).is_err(),
        "ciphertext must not open with the nil AAD"
    );
    assert!(
        aead.decrypt(ciphertext, Uuid::new_v4().as_bytes()).is_err(),
        "ciphertext must not open with another upstream id"
    );
}
