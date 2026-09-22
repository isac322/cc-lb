//! Task 20 keeps the existing principal-keyed PKCE helper intact and wraps it with an
//! upstream-keyed in-process state map: the serialized state token contains the upstream UUID and
//! nonce, while the server-side entry binds that token to the upstream UUID, verifier, and start
//! revision. This preserves the shared token exchange path but makes completion upstream-scoped.

use crate::admin_test_common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{AuditEntry, AuditStore, UpstreamCreate, UpstreamStore};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::{BodyExt, Empty};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use mock_anthropic_oauth_server::{AppState as MockOAuthState, ExpiresInRejection, RejectionShape};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const MASTER_KEY: [u8; 32] = [7; 32];
const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;

struct Fixture {
    _temp_dir: tempfile::TempDir,
    app: axum::Router,
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
    /// Mock OAuth server state; records the requests the flow issued.
    oauth_state: MockOAuthState,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_oauth_state(MockOAuthState::default()).await
    }

    async fn with_oauth_state(oauth_state: MockOAuthState) -> Self {
        let (oauth_addr, oauth_state) = spawn_mock_anthropic(oauth_state).await;
        Self::build(oauth_addr, oauth_state, None, None).await
    }

    async fn with_oauth_state_and_scopes(oauth_state: MockOAuthState, scopes: Vec<String>) -> Self {
        let (oauth_addr, oauth_state) = spawn_mock_anthropic(oauth_state).await;
        Self::build(oauth_addr, oauth_state, None, Some(scopes)).await
    }

    #[cfg(feature = "sqlite")]
    async fn new_with_scheduler() -> (Self, cc_lb_scheduler::admin::SchedulerAdminHandle) {
        Self::with_oauth_state_and_scheduler(MockOAuthState::default()).await
    }

    #[cfg(feature = "sqlite")]
    async fn with_oauth_state_and_scheduler(
        oauth_state: MockOAuthState,
    ) -> (Self, cc_lb_scheduler::admin::SchedulerAdminHandle) {
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
        let scheduler = cc_lb_scheduler::admin::SchedulerAdminHandle::new(
            cc_lb_scheduler::worker::SchedulerBackend::Sqlite(
                cc_lb_scheduler::worker::SqliteSchedulerBackend::new(
                    pool,
                    Arc::new(cc_lb_clock::SystemClock),
                ),
            ),
        );
        let (oauth_addr, oauth_state) = spawn_mock_anthropic(oauth_state).await;
        let fixture = Self::build(oauth_addr, oauth_state, Some(scheduler.clone()), None).await;
        (fixture, scheduler)
    }

    async fn build(
        oauth_addr: SocketAddr,
        oauth_state: MockOAuthState,
        scheduler: Option<cc_lb_scheduler::admin::SchedulerAdminHandle>,
        scopes: Option<Vec<String>>,
    ) -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let test_clock = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
        let clock: ClockHandle = test_clock.clone();
        let storage = admin_test_common::sqlite_storage_with_clock(
            temp_dir.path(),
            "test.sqlite",
            clock.clone(),
        )
        .await;
        let aead = Arc::new(AeadService::from_master_key(MASTER_KEY));
        let config = test_config(oauth_addr, scopes);
        let state = AdminState {
            config_path: None,
            startup_config_overrides: Default::default(),
            storage: Some(storage.clone()),
            key_store: Some(admin_test_common::key_store(storage.clone())),
            aead: Arc::clone(&aead),
            limit_engine: admin_test_common::limit_engine_with_clock(clock.clone()),
            lifecycle: None,
            subscription_metadata_hook: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            dynamic_view: admin_test_common::dynamic_view_holder(&config),
            dynamic_view_rebinder: None,
            config: Arc::new(config),
            scheduler,
            admin_auth: crate::admin_test_common::static_token_auth("test-token"),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: cc_lb_admin::events::storage_tail_channel(),
            clock: clock.clone(),
        };

        Self {
            _temp_dir: temp_dir,
            app: router(state),
            storage,
            aead,
            clock,
            oauth_state,
        }
    }

    async fn create_upstream(
        &self,
        name: &str,
        kind: UpstreamKind,
    ) -> cc_lb_storage_api::UpstreamRecord {
        self.storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("create upstream")
    }

    async fn start(&self, upstream_id: Uuid) -> (StatusCode, Value) {
        self.post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/oauth/start"),
            json!({}),
        )
        .await
    }

    async fn start_draft(&self) -> (StatusCode, Value) {
        self.start_draft_with(json!({})).await
    }

    async fn start_draft_with(&self, body: Value) -> (StatusCode, Value) {
        self.post_json("/admin/v1/oauth/draft/start", body).await
    }

    async fn complete_draft(&self, state_token: &str, code: &str) -> (StatusCode, Value) {
        self.post_json(
            "/admin/v1/oauth/draft/complete",
            json!({ "state_token": state_token, "code": code }),
        )
        .await
    }

    async fn create_from_draft(&self, state_token: &str, name: &str) -> (StatusCode, Value) {
        self.post_json(
            "/admin/v1/upstreams/from-oauth-draft",
            json!({ "state_token": state_token, "name": name }),
        )
        .await
    }

    async fn create_from_draft_with_base_url(
        &self,
        state_token: &str,
        name: &str,
        base_url: &str,
    ) -> (StatusCode, Value) {
        self.post_json(
            "/admin/v1/upstreams/from-oauth-draft",
            json!({
                "state_token": state_token,
                "name": name,
                "base_url": base_url
            }),
        )
        .await
    }

    async fn complete(
        &self,
        upstream_id: Uuid,
        state_token: &str,
        code: &str,
    ) -> (StatusCode, Value) {
        self.post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/oauth/complete"),
            json!({ "state_token": state_token, "code": code }),
        )
        .await
    }

    async fn post_json(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("Authorization", "Bearer test-token")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        json_response(response).await
    }

    async fn get_oauth_status(&self, upstream_id: Uuid) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/admin/v1/upstreams/{upstream_id}/oauth/status"))
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        json_response(response).await
    }

    async fn seed_expired_credentials(
        &self,
        upstream: &cc_lb_storage_api::UpstreamRecord,
    ) -> cc_lb_storage_api::UpstreamRecord {
        let expired_at = now_unix_secs(self.clock.as_ref()).saturating_sub(3600);
        self.store_bundle(
            upstream,
            OAuthTokenBundle {
                access_token: "sk-ant-oat01-expired-seed".to_owned(),
                refresh_token: "sk-ant-ort01-expired-seed".to_owned(),
                expires_at_unix_secs: expired_at,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["org:profile".to_owned()],
                never_refresh: false,
            },
        )
        .await
    }

    async fn store_bundle(
        &self,
        upstream: &cc_lb_storage_api::UpstreamRecord,
        bundle: OAuthTokenBundle,
    ) -> cc_lb_storage_api::UpstreamRecord {
        let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
            self.aead.as_ref(),
            &bundle,
            upstream.id.as_bytes(),
        )
        .expect("encrypt bundle");
        self.storage
            .store_oauth_tokens(
                upstream.id,
                upstream.revision,
                encrypted,
                bundle.never_refresh,
            )
            .await
            .expect("store oauth tokens")
    }

    async fn decrypt_bundle(&self, upstream_id: Uuid) -> OAuthTokenBundle {
        let stored = self
            .storage
            .get_by_id(upstream_id)
            .await
            .expect("get upstream")
            .expect("stored upstream");
        stored
            .oauth_credentials
            .expect("oauth credentials")
            .decrypt(&self.aead, upstream_id.as_bytes())
            .expect("decrypt tokens")
    }

    async fn audit_entries(&self, needle: &str) -> Vec<AuditEntry> {
        let entries = self
            .storage
            .query_audit(None, 0, u64::MAX, 100)
            .await
            .expect("query audit");
        assert!(
            entries.iter().any(|entry| {
                entry
                    .admin_action
                    .as_deref()
                    .is_some_and(|action| action.contains(needle))
            }),
            "missing {needle} audit"
        );
        entries
    }
}

fn now_unix_secs(clock: &dyn cc_lb_clock::Clock) -> u64 {
    cc_lb_clock::unix_secs(clock.now())
}

#[tokio::test]
async fn happy_pkce_roundtrip_persists_encrypted_tokens_and_emits_redacted_audit() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("claude-sub", UpstreamKind::AnthropicOauth)
        .await;

    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");

    let (status, complete) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(complete["upstream_id"], upstream.id.to_string());
    assert_eq!(
        complete["access_token_fingerprint"]
            .as_str()
            .expect("fingerprint")
            .len(),
        8
    );

    let stored = fixture
        .storage
        .get_by_id(upstream.id)
        .await
        .expect("get upstream")
        .expect("stored upstream");
    let bundle: OAuthTokenBundle = stored
        .oauth_credentials
        .expect("oauth credentials")
        .decrypt(&fixture.aead, upstream.id.as_bytes())
        .expect("decrypt tokens");
    assert!(bundle.access_token.starts_with("sk-ant-oat01-"));
    assert!(bundle.refresh_token.starts_with("sk-ant-ort01-"));
    assert_eq!(
        complete["access_token_fingerprint"],
        fingerprint(&bundle.access_token)
    );

    let audit = fixture.audit_entries("upstream_oauth_complete").await;
    let start_route = format!("/admin/v1/upstreams/{}/oauth/start", upstream.id);
    let complete_route = format!("/admin/v1/upstreams/{}/oauth/complete", upstream.id);
    assert!(audit.iter().any(|entry| {
        entry.route == start_route
            && entry.upstream == upstream.name
            && entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.contains("upstream_oauth_start"))
    }));
    assert!(audit.iter().any(|entry| {
        entry.route == complete_route
            && entry.upstream == upstream.name
            && entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.contains("upstream_oauth_complete"))
    }));
    let serialized_audit = serde_json::to_string(&audit).expect("serialize audit");
    assert!(
        serialized_audit.contains(
            complete["access_token_fingerprint"]
                .as_str()
                .expect("fingerprint")
        )
    );
    assert!(!serialized_audit.contains(&bundle.access_token));
    assert!(!serialized_audit.contains(&bundle.refresh_token));
}

#[tokio::test]
async fn tampered_state_token_returns_400_invalid_state() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("tampered-state", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;

    let (status, body) = fixture.complete(upstream.id, "tampered", &code).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_state");
}

#[tokio::test]
async fn reusing_code_after_completion_returns_400() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("reuse-code", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, _) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = fixture.complete(upstream.id, state_token, &code).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_state");
}

#[tokio::test]
async fn upstream_wrong_kind_anthropic_api_key_returns_400() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("api-key-upstream", UpstreamKind::AnthropicApiKey)
        .await;

    let (status, body) = fixture.start(upstream.id).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "wrong_kind");
}

#[tokio::test]
async fn unknown_upstream_start_returns_json_404() {
    let fixture = Fixture::new().await;

    let (status, body) = fixture.start(Uuid::new_v4()).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "upstream_not_found");
}

#[tokio::test]
async fn audit_entries_contain_fingerprint_only_no_raw_tokens() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("audit-redaction", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, complete) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let stored = fixture
        .storage
        .get_by_id(upstream.id)
        .await
        .expect("get upstream")
        .expect("stored upstream");
    let bundle: OAuthTokenBundle = stored
        .oauth_credentials
        .expect("oauth credentials")
        .decrypt(&fixture.aead, upstream.id.as_bytes())
        .expect("decrypt tokens");
    let serialized_audit =
        serde_json::to_string(&fixture.audit_entries("upstream_oauth_complete").await)
            .expect("serialize audit");

    assert!(
        serialized_audit.contains(
            complete["access_token_fingerprint"]
                .as_str()
                .expect("fingerprint")
        )
    );
    assert!(!serialized_audit.contains(&bundle.access_token));
    assert!(!serialized_audit.contains(&bundle.refresh_token));
    assert!(!complete.to_string().contains(&bundle.access_token));
    assert!(!complete.to_string().contains(&bundle.refresh_token));
}

#[tokio::test]
async fn complete_with_stale_revision_returns_409() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("stale-revision", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    fixture
        .storage
        .set_enabled(upstream.id, upstream.revision, false)
        .await
        .expect("bump revision");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");

    let (status, body) = fixture.complete(upstream.id, state_token, &code).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "stale_revision");
    assert_eq!(body["current_revision"], upstream.revision + 1);
}

#[tokio::test]
async fn draft_start_returns_top_level_authorize_url_and_state() {
    let fixture = Fixture::new().await;

    let (status, start) = fixture.start_draft().await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        start["authorize_url"]
            .as_str()
            .is_some_and(|url| url.contains("/oauth/authorize") && url.contains("state="))
    );
    assert!(
        start["state_token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );
    assert!(start.get("revision").is_none());
}

#[tokio::test]
async fn draft_complete_invalid_code_reaches_endpoint_and_returns_structured_error() {
    let fixture = Fixture::new().await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");

    let (status, body) = fixture
        .complete_draft(state_token, "invalid-test-code")
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_grant");
}

#[tokio::test]
async fn create_from_incomplete_draft_returns_invalid_state() {
    let fixture = Fixture::new().await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");

    let (status, body) = fixture
        .create_from_draft(state_token, "draft-upstream")
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_state");
}

#[tokio::test]
async fn create_from_draft_with_invalid_name_returns_structured_bad_request() {
    let fixture = Fixture::new().await;

    let (status, body) = fixture
        .create_from_draft("unused-state-token", "system.blocked")
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_input");
    assert_eq!(body["field"], "upstream.name");
    assert!(
        body["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("system"))
    );
}

#[tokio::test]
async fn create_from_completed_draft_with_active_name_returns_name_conflict() {
    let fixture = Fixture::new().await;
    let existing = fixture
        .create_upstream("draft-name-conflict", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let (status, _) = fixture.complete_draft(state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = fixture
        .create_from_draft(state_token, "draft-name-conflict")
        .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "upstream_name_conflict");
    assert_eq!(body["name"], "draft-name-conflict");
    assert_eq!(body["existing_upstream_id"], existing.id.to_string());
}

#[tokio::test]
async fn create_from_oauth_draft_returns_the_stored_base_url_without_secrets() {
    let fixture = Fixture::new().await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let (status, _) = fixture.complete_draft(state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let base_url = "https://oauth-gateway.example.com/v1/";
    let (status, created) = fixture
        .create_from_draft_with_base_url(state_token, "draft-base-url", base_url)
        .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["base_url"], base_url);
    assert!(created.get("api_key_value").is_none());
    assert!(created.get("api_key_env").is_none());
    assert!(created.get("access_token").is_none());
    assert!(created.get("refresh_token").is_none());

    let upstream_id = created["id"]
        .as_str()
        .expect("created upstream id")
        .parse()
        .expect("upstream id parses");
    let stored = fixture
        .storage
        .get_by_id(upstream_id)
        .await
        .expect("load stored upstream")
        .expect("stored upstream");
    assert_eq!(stored.base_url.as_ref().map(Url::as_str), Some(base_url));
}

#[tokio::test]
async fn create_from_oauth_draft_audits_human_readable_upstream_name() {
    let fixture = Fixture::new().await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let (status, _) = fixture.complete_draft(state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let upstream_name = "draft-audit-name";
    let (status, created) = fixture.create_from_draft(state_token, upstream_name).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["name"], upstream_name);

    let audit = fixture
        .audit_entries("upstream_create_from_oauth_draft")
        .await;
    assert!(audit.iter().any(|entry| {
        entry.route == "/admin/v1/upstreams/from-oauth-draft"
            && entry.upstream == upstream_name
            && entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action == "upstream_create_from_oauth_draft")
    }));
}

#[tokio::test]
async fn oauth_status_reflects_completion_realtime() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("realtime-status", UpstreamKind::AnthropicOauth)
        .await;

    let (status, before) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(before["has_credentials"], false);
    assert_eq!(before["status"], "missing");
    assert!(before["expires_at_unix_secs"].is_null());

    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, _complete) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, after) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["has_credentials"], true);
    assert_eq!(after["status"], "active");
    let expires = after["expires_at_unix_secs"]
        .as_u64()
        .expect("expires_at_unix_secs present after complete");
    assert!(
        expires > now_unix_secs(fixture.clock.as_ref()),
        "expires_at_unix_secs {expires} should be in the future"
    );
    assert_eq!(after["refresh_token_present"], true);
}

#[tokio::test]
async fn oauth_status_unchanged_when_only_start_called() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("start-only-no-flip", UpstreamKind::AnthropicOauth)
        .await;

    let (status, before) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(before["has_credentials"], false);
    assert_eq!(before["status"], "missing");

    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert!(start["authorize_url"].as_str().is_some());

    let (status, after) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        before, after,
        "/oauth/status must be byte-identical when /oauth/complete has not run"
    );
}

#[tokio::test]
async fn oauth_status_flips_from_expired_to_active_after_reconnect() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("reconnect-expired", UpstreamKind::AnthropicOauth)
        .await;
    let seeded = fixture.seed_expired_credentials(&upstream).await;

    let (status, before) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(before["has_credentials"], true);
    assert_eq!(before["status"], "expired");
    let before_expires = before["expires_at_unix_secs"]
        .as_u64()
        .expect("seeded expiry");
    assert!(
        before_expires < now_unix_secs(fixture.clock.as_ref()),
        "seeded expiry {before_expires} must be in the past"
    );

    let (status, start) = fixture.start(seeded.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, _complete) = fixture.complete(seeded.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, after) = fixture.get_oauth_status(seeded.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["has_credentials"], true);
    assert_eq!(after["status"], "active");
    let after_expires = after["expires_at_unix_secs"]
        .as_u64()
        .expect("expiry after reconnect");
    assert!(
        after_expires > now_unix_secs(fixture.clock.as_ref()),
        "post-reconnect expiry {after_expires} must be in the future"
    );
    assert_ne!(
        before_expires, after_expires,
        "expiry must change once new tokens land"
    );
}

/// Drives draft/start → draft/complete → upstreams/from-oauth-draft and returns
/// the draft-complete body plus the upstream id of the created upstream.
async fn run_draft_flow(fixture: &Fixture, start_body: Value) -> (Value, Uuid) {
    let (status, start) = fixture.start_draft_with(start_body).await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;

    let (status, complete) = fixture.complete_draft(state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let name = format!("oauth-draft-{}", Uuid::new_v4().simple());
    let (status, created) = fixture.create_from_draft(state_token, &name).await;
    assert_eq!(status, StatusCode::CREATED);
    let upstream_id = created["id"]
        .as_str()
        .expect("created upstream id")
        .parse()
        .expect("upstream id parses");
    (complete, upstream_id)
}

#[tokio::test]
async fn long_lived_is_default_and_stored_as_non_refreshable() {
    let fixture = Fixture::new().await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "long_lived_365d");
    assert_eq!(complete["long_lived_fallback"], false);
    assert!(complete["fallback_reason"].is_null());
    assert_eq!(
        complete["granted_expires_in_secs"],
        cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS
    );
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    assert!(bundle.never_refresh);
    assert!(
        !bundle.refresh_token.is_empty(),
        "long-lived credentials still store the granted refresh token"
    );
    assert_eq!(
        bundle.expires_at_unix_secs,
        TEST_NOW_UNIX_SECS + cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS,
    );
}

#[tokio::test]
async fn default_connect_requests_long_lived_and_stores_it() {
    let fixture = Fixture::new().await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "long_lived_365d");
    assert_eq!(complete["long_lived_fallback"], false);
    assert!(complete["fallback_reason"].is_null());
    assert_eq!(
        complete["granted_expires_in_secs"],
        cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS
    );
    assert_eq!(
        fixture.oauth_state.requested_expires_in().as_slice(),
        &[Some(cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS)],
        "the default connect must request the 365-day lifetime"
    );
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    assert!(bundle.never_refresh);
    assert!(!bundle.refresh_token.is_empty());
    assert_eq!(
        bundle.expires_at_unix_secs,
        TEST_NOW_UNIX_SECS + cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS,
    );
}

async fn run_rejected_long_lived_draft(
    shape: RejectionShape,
) -> (Value, OAuthTokenBundle, Vec<Option<u64>>) {
    let oauth_state = MockOAuthState::default()
        .with_expires_in_rejection(ExpiresInRejection::AnyExpiresIn, shape);
    let fixture = Fixture::with_oauth_state(oauth_state).await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    let requested_expires_in = fixture.oauth_state.requested_expires_in();
    (complete, bundle, requested_expires_in)
}

#[tokio::test]
async fn long_lived_exchange_falls_back_on_invalid_expiry_for_scope() {
    let (complete, bundle, requested_expires_in) =
        run_rejected_long_lived_draft(RejectionShape::InvalidExpiryForScope).await;

    assert_eq!(complete["mode"], "refreshing");
    assert_eq!(complete["long_lived_fallback"], true);
    assert_eq!(complete["fallback_reason"], "rejected");
    assert_eq!(complete["granted_expires_in_secs"], 3600);
    assert_eq!(
        requested_expires_in.as_slice(),
        &[
            Some(cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS),
            None
        ],
        "the rejected exchange must be retried without expires_in"
    );
    // The same authorization code was re-exchanged without expires_in: the mock
    // only issues a token when the retry succeeds.
    assert!(!bundle.never_refresh);
    assert!(bundle.access_token.starts_with("sk-ant-oat01-"));
    assert_eq!(bundle.expires_at_unix_secs, TEST_NOW_UNIX_SECS + 3600);
}

#[tokio::test]
async fn long_lived_exchange_falls_back_on_custom_expires_in_not_allowed() {
    let (complete, bundle, requested_expires_in) = run_rejected_long_lived_draft(
        RejectionShape::CustomExpiresInNotAllowed("user:mcp_servers".to_owned()),
    )
    .await;

    assert_eq!(complete["mode"], "refreshing");
    assert_eq!(complete["long_lived_fallback"], true);
    assert_eq!(complete["fallback_reason"], "rejected");
    assert_eq!(complete["granted_expires_in_secs"], 3600);
    assert_eq!(
        requested_expires_in.as_slice(),
        &[
            Some(cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS),
            None
        ],
        "the rejected exchange must be retried without expires_in"
    );
    assert!(!bundle.never_refresh);
    assert!(bundle.access_token.starts_with("sk-ant-oat01-"));
}

#[tokio::test]
async fn long_lived_exchange_reports_scope_rejected_for_non_inference_scope() {
    // An operator who configures a scope Anthropic will not grant a year-long
    // token for (here `org:create_api_key`) must see that their own scope
    // choice caused the fallback, not a generic rejection.
    let oauth_state = MockOAuthState::default().with_expires_in_rejection(
        ExpiresInRejection::Scope("org:create_api_key".to_owned()),
        RejectionShape::CustomExpiresInNotAllowed("org:create_api_key".to_owned()),
    );
    let fixture = Fixture::with_oauth_state_and_scopes(
        oauth_state,
        vec![
            "user:profile".to_owned(),
            "user:inference".to_owned(),
            "org:create_api_key".to_owned(),
        ],
    )
    .await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "refreshing");
    assert_eq!(complete["long_lived_fallback"], true);
    assert_eq!(complete["fallback_reason"], "scope_rejected");
    assert_eq!(complete["granted_expires_in_secs"], 3600);
    assert_eq!(
        fixture.oauth_state.requested_expires_in().as_slice(),
        &[
            Some(cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS),
            None
        ],
        "the rejected exchange must be retried without expires_in"
    );
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    assert!(!bundle.never_refresh);
    assert!(bundle.access_token.starts_with("sk-ant-oat01-"));
}

#[tokio::test]
async fn long_lived_grant_clamped_below_floor_demotes_to_refreshing() {
    const CLAMPED_EXPIRES_IN: u64 = 28_800;
    let fixture = Fixture::with_oauth_state(
        MockOAuthState::default().with_expires_in_cap(CLAMPED_EXPIRES_IN),
    )
    .await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "refreshing");
    assert_eq!(complete["long_lived_fallback"], true);
    assert_eq!(complete["fallback_reason"], "clamped");
    assert_eq!(complete["granted_expires_in_secs"], CLAMPED_EXPIRES_IN);
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    assert!(!bundle.never_refresh);
    assert!(
        !bundle.refresh_token.is_empty(),
        "a demoted credential must keep the refresh token as its renewal path"
    );
    assert_eq!(
        bundle.expires_at_unix_secs,
        TEST_NOW_UNIX_SECS + CLAMPED_EXPIRES_IN,
        "stored expiry must reflect the granted expires_in, not the requested 365 days"
    );
    assert_eq!(
        fixture.oauth_state.requested_expires_in().as_slice(),
        &[Some(cc_lb_config::LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS)],
        "the long-lived exchange must request the 365-day lifetime"
    );
}

#[tokio::test]
async fn long_lived_grant_at_floor_demotes_to_refreshing() {
    let fixture = Fixture::with_oauth_state(
        MockOAuthState::default().with_expires_in_cap(cc_lb_config::LONG_LIVED_MIN_GRANT_SECS),
    )
    .await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "refreshing");
    assert_eq!(complete["long_lived_fallback"], true);
    assert_eq!(complete["fallback_reason"], "clamped");
    assert_eq!(
        complete["granted_expires_in_secs"],
        cc_lb_config::LONG_LIVED_MIN_GRANT_SECS
    );
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    assert!(!bundle.never_refresh);
    assert!(!bundle.refresh_token.is_empty());
    assert_eq!(
        bundle.expires_at_unix_secs,
        TEST_NOW_UNIX_SECS + cc_lb_config::LONG_LIVED_MIN_GRANT_SECS,
        "a grant at the floor is too short to give up renewal"
    );
}

#[tokio::test]
async fn long_lived_grant_above_floor_stays_long_lived() {
    const GRANTED_EXPIRES_IN: u64 = cc_lb_config::LONG_LIVED_MIN_GRANT_SECS + 1;
    let fixture = Fixture::with_oauth_state(
        MockOAuthState::default().with_expires_in_cap(GRANTED_EXPIRES_IN),
    )
    .await;

    let (complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "long_lived_365d");
    assert_eq!(complete["long_lived_fallback"], false);
    assert!(complete["fallback_reason"].is_null());
    assert_eq!(complete["granted_expires_in_secs"], GRANTED_EXPIRES_IN);
    let bundle = fixture.decrypt_bundle(upstream_id).await;
    assert!(bundle.never_refresh);
    assert_eq!(
        bundle.expires_at_unix_secs,
        TEST_NOW_UNIX_SECS + GRANTED_EXPIRES_IN,
        "stored expiry must reflect the granted expires_in, not the requested 365 days"
    );
}

#[tokio::test]
async fn authorize_always_requests_the_configured_scopes() {
    let fixture = Fixture::new().await;

    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;

    let (status, start) = fixture.start_draft_with(json!({})).await;
    assert_eq!(status, StatusCode::OK);
    authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;

    let scopes = fixture.oauth_state.requested_scopes();
    assert_eq!(
        scopes,
        vec![
            Some("user:profile user:inference".to_owned()),
            Some("user:profile user:inference".to_owned()),
        ],
        "both authorization attempts must request oauth.scopes"
    );
}

#[tokio::test]
async fn oauth_status_reports_long_lived_mode_without_refresh() {
    let fixture = Fixture::new().await;
    let (_complete, upstream_id) = run_draft_flow(&fixture, json!({})).await;

    let (status, body) = fixture.get_oauth_status(upstream_id).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["mode"], "long_lived_365d");
    assert_eq!(body["can_refresh"], false);
    assert_eq!(body["refresh_token_present"], true);
    assert!(body["refresh_token_expires_at_unix_secs"].is_null());
}

#[tokio::test]
async fn oauth_status_reports_refreshing_mode_and_refresh_token_expiry() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("refreshing-status", UpstreamKind::AnthropicOauth)
        .await;
    let refresh_expires_at = now_unix_secs(fixture.clock.as_ref()) + 2_592_000;
    fixture
        .store_bundle(
            &upstream,
            OAuthTokenBundle {
                access_token: "sk-ant-oat01-refreshing".to_owned(),
                refresh_token: "sk-ant-ort01-refreshing".to_owned(),
                expires_at_unix_secs: now_unix_secs(fixture.clock.as_ref()) + 3600,
                refresh_token_expires_at_unix_secs: Some(refresh_expires_at),
                scopes: vec!["org:profile".to_owned()],
                never_refresh: false,
            },
        )
        .await;

    let (status, body) = fixture.get_oauth_status(upstream.id).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["mode"], "refreshing");
    assert_eq!(body["can_refresh"], true);
    assert_eq!(body["refresh_token_present"], true);
    assert_eq!(
        body["refresh_token_expires_at_unix_secs"],
        refresh_expires_at
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn long_lived_upstream_seeds_no_oauth_refresh_task() {
    let (long_lived_fixture, long_lived_scheduler) = Fixture::new_with_scheduler().await;
    // A refreshing credential now only results from demotion, so the second
    // leg needs a mock that clamps the granted lifetime below the floor.
    let (refreshing_fixture, refreshing_scheduler) = Fixture::with_oauth_state_and_scheduler(
        MockOAuthState::default().with_expires_in_cap(28_800),
    )
    .await;

    let (_complete, long_lived_id) = run_draft_flow(&long_lived_fixture, json!({})).await;
    let (_complete, refreshing_id) = run_draft_flow(&refreshing_fixture, json!({})).await;

    assert_eq!(
        long_lived_scheduler
            .next_run_for_upstream(long_lived_id, "oauth_refresh")
            .await
            .expect("next run query succeeds"),
        None,
        "a long-lived credential must not seed an oauth refresh task"
    );
    assert_eq!(
        refreshing_scheduler
            .next_run_for_upstream(refreshing_id, "oauth_refresh")
            .await
            .expect("next run query succeeds"),
        Some(i64::try_from(TEST_NOW_UNIX_SECS).expect("test timestamp fits")),
        "a refreshing credential must seed its bootstrap oauth refresh task"
    );
    // Skipping the refresh bootstrap must not skip warmup: a long-lived
    // upstream is otherwise fully managed and gets its connect-time warmup
    // task exactly like a refreshing one.
    assert_eq!(
        long_lived_scheduler
            .next_run_for_upstream(long_lived_id, "warmup")
            .await
            .expect("next run query succeeds"),
        Some(i64::try_from(TEST_NOW_UNIX_SECS).expect("test timestamp fits")),
        "a long-lived credential must still seed its bootstrap warmup task"
    );
    assert_eq!(
        refreshing_scheduler
            .next_run_for_upstream(refreshing_id, "warmup")
            .await
            .expect("next run query succeeds"),
        Some(i64::try_from(TEST_NOW_UNIX_SECS).expect("test timestamp fits")),
        "a refreshing credential must seed its bootstrap warmup task"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn clamped_upstream_seeds_oauth_refresh_task() {
    let (fixture, scheduler) = Fixture::with_oauth_state_and_scheduler(
        MockOAuthState::default().with_expires_in_cap(28_800),
    )
    .await;

    let (complete, clamped_id) = run_draft_flow(&fixture, json!({})).await;

    assert_eq!(complete["mode"], "refreshing");
    assert_eq!(complete["fallback_reason"], "clamped");
    assert_eq!(
        scheduler
            .next_run_for_upstream(clamped_id, "oauth_refresh")
            .await
            .expect("next run query succeeds"),
        Some(i64::try_from(TEST_NOW_UNIX_SECS).expect("test timestamp fits")),
        "a clamped credential is refreshing and must seed its bootstrap oauth refresh task"
    );
}

fn test_config(oauth_addr: SocketAddr, scopes: Option<Vec<String>>) -> Config {
    let mut config = Config::default();
    config.oauth.anthropic = Some(AnthropicOAuthConfig {
        client_id: "client-test".to_owned(),
        auth_url: Url::parse(&format!("http://{oauth_addr}/oauth/authorize")).expect("auth url"),
        token_url: Url::parse(&format!("http://{oauth_addr}/oauth/token")).expect("token url"),
        redirect_uri: Url::parse("http://127.0.0.1/callback").expect("redirect url"),
        scopes: scopes
            .unwrap_or_else(|| vec!["user:profile".to_owned(), "user:inference".to_owned()]),
    });
    config
}

async fn spawn_mock_anthropic(state: MockOAuthState) -> (SocketAddr, MockOAuthState) {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind fixture");
    let addr = listener.local_addr().expect("local addr");
    let app = mock_anthropic_oauth_server::app_with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve mock anthropic oauth");
    });
    (addr, state)
}

async fn authorize_code(authorize_url: &str) -> String {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client: Client<_, Empty<bytes::Bytes>> =
        Client::builder(TokioExecutor::new()).build(connector);
    let request = http::Request::get(authorize_url)
        .body(Empty::<bytes::Bytes>::new())
        .expect("authorize request");
    let response = client.request(request).await.expect("authorize response");
    assert_eq!(response.status(), StatusCode::FOUND);
    let location = response
        .headers()
        .get(http::header::LOCATION)
        .expect("location")
        .to_str()
        .expect("location string");
    let redirect = Url::parse(location).expect("redirect location");
    redirect
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .expect("code")
}

async fn json_response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

fn fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    let mut out = String::with_capacity(8);
    for byte in &digest[..4] {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
