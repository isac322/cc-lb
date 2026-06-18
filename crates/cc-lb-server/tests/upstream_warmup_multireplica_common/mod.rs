#![allow(deprecated)]
#![allow(dead_code)]

use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::body::Body as AxumBody;
use axum::extract::State;
use axum::http::{Method, Request, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use cc_lb_admin::{AdminState, CurrentConfig};
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    Body as CoreBody, DispatchError, DynamicView, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, SubscriptionQuotaSink, UpstreamDispatch, UpstreamStatusSnapshot,
};
use cc_lb_plugin_api::{
    ApiKeyAwareSignerFactory, ObservabilityError, ObservabilityHook, ObserveEvent, Principal,
    RequestContext, RouteDecision, RouteError, RouterPlugin, ShapedRequest, SignedRequest, Signer,
    SignerError, SignerFactory, SigningCapability, Upstream, UpstreamCandidate,
};
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_server::upstream_warmup_loop::UpstreamWarmupLoop;
use cc_lb_server::warmup::stable_jitter_ms;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditStore, BackendKind, MetaStore, PluginRegistryStore, PrincipalStore,
    PromptCacheObservationStore, Storage as StorageTrait, SubscriptionQuotaObservationRecord,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamCreate, UpstreamRateLimitStateStore, UpstreamRecord,
    UpstreamStore, UpstreamSubscriptionQuotaStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use chrono::{DateTime, TimeZone, Utc};
use http_body_util::BodyExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use url::Url;
use uuid::Uuid;

pub const FIVE_HOURS_SECS: i64 = 5 * 60 * 60;

static TRACING_INIT: Once = Once::new();

pub type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

pub fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_test_writer()
            .try_init();
    });
}

pub struct PostgresWarmupFixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
    pub storage: Arc<PostgresStorage>,
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
}

impl PostgresWarmupFixture {
    pub async fn create(schema_prefix: &str) -> TestResult<Option<Self>> {
        init_tracing();
        let Some(url) = std::env::var("DATABASE_URL_TEST").ok() else {
            tracing::warn!(
                target: "warmup_multireplica_test",
                skip = true,
                env = "DATABASE_URL_TEST",
                "SKIP upstream warmup multi-replica test: DATABASE_URL_TEST unset"
            );
            return Ok(None);
        };

        let schema = format!("{schema_prefix}_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        let storage = Arc::new(PostgresStorage::new(pool.clone()));
        storage.initialize(BackendKind::Postgres).await?;
        install_cycle_key_write_trigger(&pool).await?;

        let stores = stores_from_postgres(storage.clone());
        let aead = Arc::new(AeadService::from_master_key([17; 32]));

        Ok(Some(Self {
            schema,
            admin_pool,
            pool,
            storage,
            stores,
            aead,
        }))
    }

    pub async fn drop_schema(self) -> TestResult {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }

    pub fn warmup_loop(&self, replica_id: Uuid) -> Arc<UpstreamWarmupLoop> {
        let refresher = Arc::new(LazyRefresher::new(
            self.stores.clone(),
            self.aead.clone(),
            oauth_config(),
            replica_id,
            None,
            CancellationToken::new(),
        ));
        let (subscription_quota_sink, _subscription_quota_rx) =
            SubscriptionQuotaSink::with_capacity(16);
        Arc::new(UpstreamWarmupLoop::new(
            self.stores.clone(),
            self.aead.clone(),
            refresher,
            subscription_quota_sink,
            replica_id,
            None,
            std::path::PathBuf::new(),
        ))
    }

    pub fn admin_state(&self) -> AdminState {
        AdminState {
            storage: Some(self.storage.clone() as Arc<dyn StorageTrait>),
            key_store: None,
            aead: self.aead.clone(),
            limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
            lifecycle: None,
            subscription_metadata_hook: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            audit_sink: None,
            dynamic_view: Arc::new(DynamicViewHolder::new(test_dynamic_view())),
            config: Arc::new(Config::default()) as Arc<dyn CurrentConfig>,
            admin_token: None,
            start_time: Instant::now(),
        }
    }

    pub async fn create_due_oauth_upstream(&self, base_url: Url) -> TestResult<UpstreamRecord> {
        let now = self.storage.warmup_now_unix_secs().await?;
        let created = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                name: format!("warmup_{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(base_url),
                api_key_ciphertext: None,
                warmup_enabled: true,
                next_warmup_at: Some(unix_datetime(now.saturating_sub(1))?),
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            },
        )
        .await?;
        let bundle = OAuthTokenBundle {
            access_token: "warmup-access-token".to_owned(),
            refresh_token: "warmup-refresh-token".to_owned(),
            expires_at_unix_secs: u64::MAX / 2,
            scopes: Vec::new(),
        };
        let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
            &self.aead,
            &bundle,
            created.id.as_bytes(),
        )?;
        Ok(self
            .storage
            .store_oauth_tokens(created.id, created.revision, encrypted)
            .await?)
    }

    pub async fn seed_latest_observation(
        &self,
        upstream_id: Uuid,
        cycle_key: i64,
        observed_offset_millis: u64,
    ) -> TestResult {
        let db_now = self.storage.warmup_now_unix_secs().await?;
        let base_millis = u64::try_from(db_now.saturating_mul(1_000))?;
        self.storage
            .put_subscription_quota(&subscription_observation(
                upstream_id,
                cycle_key,
                base_millis.saturating_add(observed_offset_millis),
            )?)
            .await?;
        Ok(())
    }

    pub async fn expire_warmup_lease(&self, upstream_id: Uuid) -> TestResult {
        sqlx::query(
            "UPDATE upstream_lease_v1
                SET until_unix_secs = extract(epoch from now())::bigint - 1
              WHERE upstream_id = $1
                AND lease_kind = 'warmup'",
        )
        .bind(upstream_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn warmup_lease_until(&self, upstream_id: Uuid) -> TestResult<Option<i64>> {
        Ok(sqlx::query_scalar::<_, Option<i64>>(
            "SELECT until_unix_secs FROM upstream_lease_v1 WHERE upstream_id = $1 AND lease_kind = 'warmup'",
        )
        .bind(upstream_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten())
    }

    pub async fn cycle_key_write_count(&self, upstream_id: Uuid) -> TestResult<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM warmup_cycle_key_writes WHERE upstream_id = $1",
        )
        .bind(upstream_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn cycle_key_writes(&self, upstream_id: Uuid) -> TestResult<Vec<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT new_cycle_key
               FROM warmup_cycle_key_writes
              WHERE upstream_id = $1
              ORDER BY write_index ASC",
        )
        .bind(upstream_id)
        .fetch_all(&self.pool)
        .await?)
    }
}

pub struct RunningWarmupServer {
    addr: SocketAddr,
    state: Arc<FakeWarmupState>,
    task: JoinHandle<Result<(), io::Error>>,
}

impl RunningWarmupServer {
    pub async fn spawn(response_delay: Duration) -> TestResult<Self> {
        let state = Arc::new(FakeWarmupState {
            calls: Mutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            response_delay,
        });
        let app = Router::new()
            .route("/v1/messages", post(record_warmup_call))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        Ok(Self { addr, state, task })
    }

    pub fn base_url(&self) -> TestResult<Url> {
        Ok(Url::parse(&format!("http://{}/", self.addr))?)
    }

    pub async fn calls(&self) -> Vec<WarmupCall> {
        self.state.calls.lock().await.clone()
    }

    pub async fn call_count(&self) -> usize {
        self.state.calls.lock().await.len()
    }

    pub async fn wait_for_call_count(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> TestResult<Vec<WarmupCall>> {
        let deadline = Instant::now() + timeout;
        loop {
            let calls = self.calls().await;
            if calls.len() >= expected {
                return Ok(calls);
            }
            if Instant::now() >= deadline {
                return Err(error(format!(
                    "timed out waiting for {expected} warmup calls; observed {}",
                    calls.len()
                )));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let _ = tokio::time::timeout(remaining, self.state.notify.notified()).await;
        }
    }
}

impl Drop for RunningWarmupServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Debug)]
pub struct WarmupCall {
    pub method: Method,
    pub path: String,
    pub received_at: Instant,
}

struct FakeWarmupState {
    calls: Mutex<Vec<WarmupCall>>,
    notify: tokio::sync::Notify,
    response_delay: Duration,
}

async fn record_warmup_call(
    State(state): State<Arc<FakeWarmupState>>,
    request: Request<AxumBody>,
) -> Response<AxumBody> {
    state.calls.lock().await.push(WarmupCall {
        method: request.method().clone(),
        path: request.uri().path().to_owned(),
        received_at: Instant::now(),
    });
    state.notify.notify_waiters();

    if !state.response_delay.is_zero() {
        tokio::time::sleep(state.response_delay).await;
    }

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        r#"{"id":"msg_warmup","type":"message","role":"assistant","model":"claude-haiku-4-5-20251001","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    )
        .into_response()
}

pub fn assert_warmup_posts(calls: &[WarmupCall]) {
    for call in calls {
        assert_eq!(call.method, Method::POST);
        assert_eq!(call.path, "/v1/messages");
    }
}

pub fn low_jitter_cycle_key(upstream_id: Uuid, now_unix_secs: i64) -> i64 {
    low_jitter_cycle_key_from_offsets(upstream_id, now_unix_secs, 60, 86_400)
}

pub fn low_jitter_old_cycle_key(upstream_id: Uuid, now_unix_secs: i64) -> i64 {
    low_jitter_cycle_key_from_offsets(
        upstream_id,
        now_unix_secs,
        FIVE_HOURS_SECS + 120,
        FIVE_HOURS_SECS + 86_400,
    )
}

fn low_jitter_cycle_key_from_offsets(
    upstream_id: Uuid,
    now_unix_secs: i64,
    start_offset_secs: i64,
    end_offset_secs: i64,
) -> i64 {
    for offset in start_offset_secs..=end_offset_secs {
        let cycle_key = now_unix_secs.saturating_sub(offset);
        if let Ok(cycle_key_u64) = u64::try_from(cycle_key)
            && stable_jitter_ms(upstream_id, cycle_key_u64) <= 100
        {
            return cycle_key;
        }
    }
    now_unix_secs.saturating_sub(end_offset_secs)
}

pub async fn response_json(
    response: Response<AxumBody>,
) -> TestResult<(StatusCode, serde_json::Value)> {
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    Ok((status, serde_json::from_slice(&body)?))
}

fn stores_from_postgres(storage: Arc<PostgresStorage>) -> Arc<Stores> {
    Arc::new(Stores {
        upstreams: storage.clone() as Arc<dyn UpstreamStore>,
        principals: storage.clone() as Arc<dyn PrincipalStore>,
        plugin_registry: storage.clone() as Arc<dyn PluginRegistryStore>,
        upstream_rate_limits: storage.clone() as Arc<dyn UpstreamRateLimitStateStore>,
        upstream_subscription_quotas: storage.clone() as Arc<dyn UpstreamSubscriptionQuotaStore>,
        prompt_cache_observations: storage.clone() as Arc<dyn PromptCacheObservationStore>,
        anthropic_compatibility_kv: storage.clone(),
        audit: Some(storage as Arc<dyn AuditStore>),
        plugin_registry_repo: None,
    })
}

async fn install_cycle_key_write_trigger(pool: &PgPool) -> TestResult {
    sqlx::query(
        "CREATE TABLE warmup_cycle_key_writes (
            write_index BIGSERIAL PRIMARY KEY,
            upstream_id UUID NOT NULL,
            holder TEXT,
            new_cycle_key BIGINT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE OR REPLACE FUNCTION record_warmup_cycle_key_write()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF TG_OP = 'INSERT' THEN
                 IF NEW.last_warmup_cycle_key IS NOT NULL THEN
                     INSERT INTO warmup_cycle_key_writes (upstream_id, holder, new_cycle_key)
                     VALUES (NEW.upstream_id, NULL, NEW.last_warmup_cycle_key);
                 END IF;
             ELSIF OLD.last_warmup_cycle_key IS DISTINCT FROM NEW.last_warmup_cycle_key
                   AND NEW.last_warmup_cycle_key IS NOT NULL THEN
                 INSERT INTO warmup_cycle_key_writes (upstream_id, holder, new_cycle_key)
                 VALUES (NEW.upstream_id, NULL, NEW.last_warmup_cycle_key);
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TRIGGER record_warmup_cycle_key_write_trigger
         AFTER INSERT OR UPDATE OF last_warmup_cycle_key ON upstream_status_v1
         FOR EACH ROW
         EXECUTE FUNCTION record_warmup_cycle_key_write()",
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn subscription_observation(
    upstream_id: Uuid,
    cycle_key: i64,
    observed_at_unix_millis: u64,
) -> TestResult<SubscriptionQuotaObservationRecord> {
    Ok(SubscriptionQuotaObservationRecord {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization: Some(0.5),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(u64::try_from(cycle_key)?),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    })
}

fn oauth_config() -> Arc<AnthropicOAuthConfig> {
    Arc::new(AnthropicOAuthConfig {
        client_id: "warmup-test-client".to_owned(),
        auth_url: Url::parse("http://127.0.0.1:9/oauth/authorize").expect("auth URL parses"),
        token_url: Url::parse("http://127.0.0.1:9/v1/oauth/token").expect("token URL parses"),
        redirect_uri: Url::parse("http://127.0.0.1/admin/oauth/callback")
            .expect("redirect URL parses"),
        scopes: Vec::new(),
    })
}

fn unix_datetime(unix_secs: i64) -> TestResult<DateTime<Utc>> {
    Utc.timestamp_opt(unix_secs, 0)
        .single()
        .ok_or_else(|| error(format!("invalid unix timestamp: {unix_secs}")))
}

fn test_dynamic_view() -> Arc<DynamicView> {
    let principal_view = Arc::new(PrincipalView::from_db(&[], HashMap::new()));
    DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .global_router(Arc::new(TestRouter))
        .dispatcher(Arc::new(TestDispatcher))
        .global_observability_hooks(Vec::new())
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
        .build()
}

struct TestSignerFactory;

impl ApiKeyAwareSignerFactory for TestSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(TestSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for TestSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(TestSigner))
    }
}

struct TestSigner;

#[async_trait]
impl Signer for TestSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(
        &self,
        _err: &cc_lb_plugin_api::UpstreamError,
    ) -> cc_lb_plugin_api::RetryDecision {
        cc_lb_plugin_api::RetryDecision::Fail
    }
}

struct TestRouter;

impl RouterPlugin for TestRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "test router has no route".to_owned(),
        })
    }
}

struct TestDispatcher;

#[async_trait]
impl UpstreamDispatch for TestDispatcher {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<CoreBody>, DispatchError> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(CoreBody::from(Vec::new()))
            .expect("test response builds"))
    }
}

struct TestHook;

impl ObservabilityHook for TestHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

pub fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
