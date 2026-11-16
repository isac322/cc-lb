//! Admin bootstrap exercises real SQLite transactions and owner prerequisites.
use axum::http::{StatusCode, header};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use cc_lb_control::api_keys::{
    secret::compute_index_hash,
    session_token::{self, Purpose},
};
use cc_lb_storage_api::{
    AuditQueryScope, AuditStore,
    account_owner::{AccountOwnerBinding, AccountOwnerStore},
    codex_sessions::{CodexSessionStore, CredentialPurpose},
};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::admin_test_common::{self, SpawnedAdminServer};

struct Fixture {
    server: SpawnedAdminServer,
    principal: String,
    parent: String,
    binding: String,
}

impl Fixture {
    async fn new() -> Self {
        let server = admin_test_common::spawn_admin_server_with_clock(Arc::new(
            cc_lb_clock::TestClock::new_at_secs(100),
        ))
        .await;
        let (status, _, principal) = server.client.post_json("/admin/v1/principals", json!({
            "name": "session-owner", "kind": "human", "allowed_models": [], "default_limits": []
        })).await;
        assert_eq!(status, StatusCode::CREATED);
        let principal = principal["id"].as_str().unwrap().to_owned();
        let (status, _, key) = server
            .client
            .post_json(&format!("/admin/v1/principals/{principal}/keys"), json!({}))
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let parent = key["key_id"].as_str().unwrap().to_owned();
        let upstream_id = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO upstream_spec_v1 (id,name,kind,enabled,created_at,updated_at) VALUES (?,'session-owner','openai_oauth',1,100,100)")
            .bind(upstream_id.to_string()).execute(server.storage.pool()).await.unwrap();
        let binding = uuid::Uuid::new_v4().to_string();
        server
            .storage
            .create_owner_binding(AccountOwnerBinding {
                binding_id: binding.clone(),
                principal_id: principal.clone(),
                provider: "openai".into(),
                owner_id: "provider-owner".into(),
                upstream_id,
                owner_generation: 1,
                local_user_id: "local-user".into(),
                allowed_workspaces: vec!["local-workspace".into()],
                provider_workspace_id: "provider-workspace".into(),
                outbound_region: None,
                revision: 1,
                revoked_at_unix_secs: None,
            })
            .await
            .unwrap();
        Self {
            server,
            principal,
            parent,
            binding,
        }
    }

    fn route(&self) -> String {
        format!("/admin/v1/principals/{}/codex-sessions", self.principal)
    }

    fn request(&self) -> Value {
        json!({"parent_key_id": self.parent, "owner_binding_id": self.binding,
            "workspace_id": "local-workspace", "origin": "https://PROXY.example.com:443/",
            "access_ttl_seconds": 10, "session_ttl_seconds": 100})
    }

    async fn family_count(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM codex_session_families_v1")
            .fetch_one(self.server.storage.pool())
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn codex_session_issue_authorization_and_canonical_local_credentials() {
    let fixture = Fixture::new().await;
    let (status, headers, body) = fixture
        .server
        .client
        .post_json(&fixture.route(), fixture.request())
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    let mut fields: Vec<_> = body
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "access_token",
            "account_id",
            "expires_at",
            "origin",
            "renewal_token",
            "session_expires_at",
            "session_id",
            "token_type",
            "user_id",
            "workspace_generation"
        ]
    );
    assert_eq!(body["origin"], "https://proxy.example.com");
    assert_eq!(body["workspace_generation"], 1);
    assert_eq!(body["expires_at"], 110);
    assert_eq!(body["session_expires_at"], 200);
    assert_eq!(body["account_id"], "local-workspace");
    assert_eq!(body["user_id"], "local-user");
    assert_eq!(body["token_type"], "Bearer");
    let access = body["access_token"].as_str().unwrap();
    let renewal = body["renewal_token"].as_str().unwrap();
    for (token, purpose, stored_purpose) in [
        (access, Purpose::Access, CredentialPurpose::SessionAccess),
        (renewal, Purpose::Renewal, CredentialPurpose::SessionRenewal),
    ] {
        let stored = fixture
            .server
            .storage
            .lookup_session_credential(&compute_index_hash(token.as_bytes()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.credential.purpose, stored_purpose);
        assert!(session_token::verify(
            token,
            purpose,
            &stored.credential.verify_hash,
            &stored.credential.salt
        ));
        assert_eq!(stored.family.parent_key_id, fixture.parent);
        assert_eq!(stored.family.owner_binding_id, fixture.binding);
        assert_eq!(stored.family.principal_id, fixture.principal);
    }
    let segments: Vec<_> = access.split('.').collect();
    assert_eq!(segments.len(), 3);
    let payload: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(segments[1]).unwrap()).unwrap();
    assert_eq!(payload["iss"], "https://proxy.example.com");
    assert_eq!(payload["sub"], "local-user");
    assert_eq!(
        payload["https://api.openai.com/auth"]["chatgpt_plan_type"],
        "unknown"
    );
    assert!(payload.get("email").is_none());
    let rows = fixture
        .server
        .storage
        .query_recent_audit(
            AuditQueryScope::AdminActionRoute {
                action: "codex_session_issue",
                route: &fixture.route(),
            },
            0,
            u64::MAX,
            10,
            false,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].api_key_id.as_deref(), Some(fixture.parent.as_str()));
    assert!(rows[0].actor.is_some());
    let audit = serde_json::to_string(&rows[0]).unwrap();
    assert!(!audit.contains(access));
    assert!(!audit.contains(renewal));
    assert_eq!(
        rows[0].payload.as_ref().unwrap()["session_id"],
        body["session_id"]
    );
}

#[tokio::test]
async fn codex_session_rejects_invalid_origins_ttls_and_client_authority() {
    let fixture = Fixture::new().await;
    for origin in [
        "http://proxy.example.com",
        "https://user@proxy.example.com",
        "https://@proxy.example.com",
        "https://proxy.example.com/path",
        "https://proxy.example.com/a/..",
        "https://proxy.example.com//",
        "https://proxy.example.com?",
        "https://proxy.example.com#",
        "https://proxy.example.com\\",
    ] {
        let mut request = fixture.request();
        request["origin"] = json!(origin);
        let (status, _, _) = fixture
            .server
            .client
            .post_json(&fixture.route(), request)
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "origin {origin}");
    }
    for (access, session) in [
        (0, 100),
        (11, 10),
        (1, 0),
        (1, u64::MAX),
        (u64::MAX, u64::MAX),
        (1, i64::MAX as u64),
    ] {
        let mut request = fixture.request();
        request["access_ttl_seconds"] = json!(access);
        request["session_ttl_seconds"] = json!(session);
        let (status, _, _) = fixture
            .server
            .client
            .post_json(&fixture.route(), request)
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    for field in [
        "parent_key_id",
        "owner_binding_id",
        "workspace_id",
        "origin",
        "access_ttl_seconds",
        "session_ttl_seconds",
    ] {
        let mut request = fixture.request();
        request.as_object_mut().unwrap().remove(field);
        let (status, _, _) = fixture
            .server
            .client
            .post_json(&fixture.route(), request)
            .await;
        assert!(status.is_client_error());
    }
    let mut forged = fixture.request();
    forged["local_user_id"] = json!("forged-user");
    let (status, _, _) = fixture
        .server
        .client
        .post_json(&fixture.route(), forged)
        .await;
    assert!(status.is_client_error());
    assert_eq!(fixture.family_count().await, 0);
}

#[tokio::test]
async fn codex_session_requires_live_matching_owner_parent_and_principal() {
    let fixture = Fixture::new().await;
    for (field, value) in [
        ("owner_binding_id", "absent"),
        ("workspace_id", "unauthorized"),
        ("parent_key_id", "absent"),
    ] {
        let mut request = fixture.request();
        request[field] = json!(value);
        let (status, _, _) = fixture
            .server
            .client
            .post_json(&fixture.route(), request)
            .await;
        assert!(status.is_client_error());
    }
    let (status, _, _) = fixture
        .server
        .client
        .post_json(
            "/admin/v1/principals/other-principal/codex-sessions",
            fixture.request(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    sqlx::query("UPDATE principals_v1 SET enabled=0 WHERE id=?")
        .bind(&fixture.principal)
        .execute(fixture.server.storage.pool())
        .await
        .unwrap();
    let (status, _, _) = fixture
        .server
        .client
        .post_json(&fixture.route(), fixture.request())
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    sqlx::query("UPDATE principals_v1 SET enabled=1 WHERE id=?")
        .bind(&fixture.principal)
        .execute(fixture.server.storage.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE managed_keys_v1 SET status='revoked' WHERE key_id=?")
        .bind(&fixture.parent)
        .execute(fixture.server.storage.pool())
        .await
        .unwrap();
    let (status, _, _) = fixture
        .server
        .client
        .post_json(&fixture.route(), fixture.request())
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.family_count().await, 0);
}

#[tokio::test]
async fn codex_session_auth_rejection_precedes_json_validation() {
    let server = admin_test_common::spawn_admin_server_with_auth(
        admin_test_common::static_token_auth("different-test-token"),
    )
    .await;
    let (status, _, body) = server
        .client
        .post_json(
            "/admin/v1/principals/absent/codex-sessions",
            json!({"not_a_session_request": true}),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body.get("access_token").is_none());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM codex_session_families_v1")
        .fetch_one(server.storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn codex_session_audit_failure_compensates_and_returns_no_secret() {
    let fixture = Fixture::new().await;
    sqlx::query("CREATE TRIGGER fail_session_audit BEFORE INSERT ON audit_log_v1 BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END")
        .execute(fixture.server.storage.pool()).await.unwrap();
    let (status, _, body) = fixture
        .server
        .client
        .post_json(&fixture.route(), fixture.request())
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body, json!({"error": "audit_write_failed"}));
    assert_eq!(fixture.family_count().await, 1);
    let active_families: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM codex_session_families_v1 WHERE revoked_at IS NULL",
    )
    .fetch_one(fixture.server.storage.pool())
    .await
    .unwrap();
    let active_credentials: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM codex_session_credentials_v1 WHERE revoked_at IS NULL",
    )
    .fetch_one(fixture.server.storage.pool())
    .await
    .unwrap();
    assert_eq!(active_families, 0);
    assert_eq!(active_credentials, 0);
}

#[tokio::test]
async fn codex_session_owner_revocation_and_unavailable_upstream_do_not_issue() {
    let fixture = Fixture::new().await;
    let mut binding = fixture
        .server
        .storage
        .get_owner_binding(&fixture.binding)
        .await
        .unwrap()
        .unwrap();
    binding.revoked_at_unix_secs = Some(100);
    binding.revision += 1;
    fixture
        .server
        .storage
        .compare_and_replace_owner_binding(binding, 1)
        .await
        .unwrap();
    let (status, _, _) = fixture
        .server
        .client
        .post_json(&fixture.route(), fixture.request())
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(fixture.family_count().await, 0);

    let fixture = Fixture::new().await;
    sqlx::query("UPDATE upstream_spec_v1 SET enabled=0")
        .execute(fixture.server.storage.pool())
        .await
        .unwrap();
    let (status, _, body) = fixture
        .server
        .client
        .post_json(&fixture.route(), fixture.request())
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.get("access_token").is_none());
    assert_eq!(fixture.family_count().await, 0);
}

#[tokio::test]
async fn codex_session_accepts_explicit_nondefault_port_and_equal_ttls() {
    let fixture = Fixture::new().await;
    let mut request = fixture.request();
    request["origin"] = json!("https://proxy.example.com:8443");
    request["session_ttl_seconds"] = json!(10);
    let (status, _, body) = fixture
        .server
        .client
        .post_json(&fixture.route(), request)
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["origin"], "https://proxy.example.com:8443");
    assert_eq!(body["expires_at"], body["session_expires_at"]);
}
