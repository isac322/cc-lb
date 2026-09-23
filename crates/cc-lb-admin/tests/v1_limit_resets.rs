//! Route-level tests for the per-upstream limit-reset (cedar_ember) endpoints.
//! A wiremock server stands in for the Anthropic OAuth API; the upstream's
//! `base_url` points at it, exercising the same code path as production.

use axum::http::StatusCode;
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{AuditStore, UpstreamCreate, UpstreamStore};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::admin_test_common::{self, SpawnedAdminServer};

const ACCOUNT_UUID: &str = "9f0b1c2d-0000-4000-8000-0000000000aa";
const ORG_UUID: &str = "9f0b1c2d-0000-4000-8000-0000000000bb";
const ACCESS_TOKEN: &str = "sk-ant-oat01-limit-reset-test";
const REQUEST_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn profile_body() -> Value {
    json!({
        "account": {"uuid": ACCOUNT_UUID, "email": "admin@example.com"},
        "organization": {"uuid": ORG_UUID, "name": "Example Org"}
    })
}

fn usage_body(cedar_ember: Value) -> Value {
    json!({
        "five_hour": {"utilization": 95.0, "resets_at": "2026-09-24T20:00:00Z"},
        "cedar_ember": cedar_ember
    })
}

fn cedar_ember_body() -> Value {
    json!({
        "eligible": true,
        "ineligible_reason": null,
        "at_limit": true,
        "exhausted": ["five_hour"],
        "grants": [{
            "id": "grant_01",
            "label": "Weekly reset",
            "resets_total": 1,
            "resets_left": 1,
            "starts_at": "2026-09-20T00:00:00Z",
            "ends_at": "2026-09-27T00:00:00Z",
            "clears": ["five_hour", "seven_day"],
            "paused": false,
            "usable_now": true,
            "use_requires_limit": false,
            "percent_used": {"five_hour": 95, "seven_day": 40},
            "blocking": []
        }],
        "next_grant_id": "grant_01",
        "weekly_resets_at": "2026-09-28T00:00:00Z",
        "cooldown_until": null
    })
}

async fn mount_profile(provider: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/oauth/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(profile_body()))
        .mount(provider)
        .await;
}

async fn mount_usage(provider: &MockServer, body: Value) {
    Mock::given(method("GET"))
        .and(path("/api/oauth/usage"))
        .and(query_param("cedar_ember", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(provider)
        .await;
}

async fn seed_oauth_upstream(server: &SpawnedAdminServer, base_url: &str) -> Uuid {
    let upstream = UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            name: "coupon-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse(base_url).expect("base url parses")),
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("create upstream");
    let bundle = OAuthTokenBundle {
        access_token: ACCESS_TOKEN.to_owned(),
        refresh_token: "sk-ant-ort01-limit-reset-test".to_owned(),
        expires_at_unix_secs: 4_000_000_000,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["user:profile".to_owned()],
        never_refresh: true,
    };
    let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        &AeadService::from_master_key([0; 32]),
        &bundle,
        upstream.id.as_bytes(),
    )
    .expect("encrypt bundle");
    server
        .storage
        .store_oauth_tokens(upstream.id, upstream.revision, encrypted, true)
        .await
        .expect("store oauth tokens");
    upstream.id
}

fn claim_body(grant_id: &str) -> Value {
    json!({
        "account_id": ACCOUNT_UUID,
        "organization_id": ORG_UUID,
        "grant_id": grant_id,
        "request_id": REQUEST_ID,
    })
}

#[tokio::test]
async fn get_limit_resets_returns_provider_status() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    mount_usage(&provider, usage_body(cedar_ember_body())).await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["account_id"], ACCOUNT_UUID);
    assert_eq!(body["organization_id"], ORG_UUID);
    assert!(body.get("revision").is_none(), "no mock revision leaks");
    let cedar_ember = &body["cedar_ember"];
    assert_eq!(cedar_ember["eligible"], true);
    assert_eq!(cedar_ember["at_limit"], true);
    assert_eq!(cedar_ember["exhausted"], json!(["5h"]));
    assert_eq!(cedar_ember["next_grant_id"], "grant_01");
    assert_eq!(cedar_ember["weekly_resets_at"], "2026-09-28T00:00:00Z");
    let grant = &cedar_ember["grants"][0];
    assert_eq!(grant["id"], "grant_01");
    assert_eq!(grant["resets_left"], 1);
    assert_eq!(grant["clears"], json!(["5h", "7d"]));
    assert_eq!(grant["percent_used"], json!({"5h": 95, "7d": 40}));
    assert_eq!(grant["use_requires_limit"], false);
}

#[tokio::test]
async fn get_limit_resets_without_cedar_ember_returns_null() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    mount_usage(&provider, usage_body(Value::Null)).await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["account_id"], ACCOUNT_UUID);
    assert!(body["cedar_ember"].is_null());
}

#[tokio::test]
async fn get_limit_resets_malformed_cedar_ember_fails() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    // The sentinel value would appear verbatim in the serde error message;
    // the API must only surface the static category, never provider content.
    mount_usage(
        &provider,
        usage_body(json!({
            "eligible": true,
            "grants": "sentinel-provider-value-7f3a"
        })),
    )
    .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"], "provider_malformed_response");
    assert_eq!(body["detail"], "provider response failed schema validation");
    assert!(
        !body.to_string().contains("sentinel-provider-value-7f3a"),
        "provider content must not leak into the error response"
    );
}

#[tokio::test]
async fn get_limit_resets_missing_grants_is_malformed() {
    // A present cedar_ember block without a grants array is malformed, not
    // an empty grant list — the backend must not fabricate [].
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    mount_usage(
        &provider,
        usage_body(json!({"eligible": true, "at_limit": true})),
    )
    .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"], "provider_malformed_response");
}

#[tokio::test]
async fn get_limit_resets_empty_grants_is_valid() {
    // An explicit empty array is a legitimate provider answer.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    mount_usage(
        &provider,
        usage_body(json!({
            "eligible": false,
            "ineligible_reason": "no_grant",
            "at_limit": true,
            "grants": []
        })),
    )
    .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["cedar_ember"]["eligible"], false);
    assert_eq!(body["cedar_ember"]["grants"], json!([]));
}

#[tokio::test]
async fn get_limit_resets_forwards_null_resets_total() {
    // resets_total is optional in the provider schema; absence is forwarded
    // as null, never fabricated as 0.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    let mut block = cedar_ember_body();
    block["grants"][0]
        .as_object_mut()
        .expect("grant object")
        .remove("resets_total");
    mount_usage(&provider, usage_body(block)).await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::OK);
    let grant = &body["cedar_ember"]["grants"][0];
    assert!(grant["resets_total"].is_null());
    assert_eq!(grant["resets_left"], 1);
}

#[tokio::test]
async fn get_limit_resets_requires_auth() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, _) = server
        .client
        .get_without_auth(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_limit_resets_rejects_non_oauth_upstream() {
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            name: "api-key-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("create upstream");

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "not_oauth_upstream");
}

#[tokio::test]
async fn claim_dispatches_exact_grant_and_request_id_once() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    // The provider recommends grant_01; the user picked grant_02. The claim
    // must forward grant_02 verbatim — no next_grant_id substitution — and
    // exactly once (no backend retries).
    Mock::given(method("POST"))
        .and(path(format!(
            "/api/organizations/{ORG_UUID}/reset_rate_limits"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "reset",
            "reason": null,
            "resets_left": 0,
            "cleared": ["five_hour"],
            "weekly_resets_at": "2026-09-28T00:00:00Z",
            "cooldown_until": null
        })))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            claim_body("grant_02"),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "reset");
    assert_eq!(body["cleared"], json!(["5h"]));
    assert_eq!(body["resets_left"], 0);
    assert_eq!(body["weekly_resets_at"], "2026-09-28T00:00:00Z");
    assert!(body.get("revision").is_none());

    let requests = provider
        .received_requests()
        .await
        .expect("requests recorded");
    let claims: Vec<_> = requests
        .iter()
        .filter(|request| request.method.as_str() == "POST")
        .collect();
    assert_eq!(claims.len(), 1, "claim dispatched exactly once");
    let sent: Value = serde_json::from_slice(&claims[0].body).expect("claim body is json");
    assert_eq!(
        sent,
        json!({
            "program": "cedar_ember",
            "grant_id": "grant_02",
            "request_id": REQUEST_ID,
        })
    );
}

#[tokio::test]
async fn claim_rejects_stale_identity_without_dispatching() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "reset"})))
        .expect(0)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let mut body = claim_body("grant_01");
    body["account_id"] = json!("stale-account");
    let (status, _, response) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            body,
        )
        .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "stale_identity");
}

#[tokio::test]
async fn claim_rejects_malformed_ids_without_dispatching() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "reset"})))
        .expect(0)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let mut bad_grant = claim_body("Grant With Spaces");
    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            bad_grant.clone(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_grant_id");

    bad_grant["grant_id"] = json!("grant_01");
    bad_grant["request_id"] = json!("not-a-uuid");
    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            bad_grant,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_request_id");
}

#[tokio::test]
async fn claim_provider_5xx_reports_unknown_not_failure() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            claim_body("grant_01"),
        )
        .await;

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(body["error"], "claim_outcome_unknown");

    // An indeterminate claim still leaves a full audit trail: actor, target
    // upstream, submitted grant/request id, and the stable outcome code.
    let entries = server
        .storage
        .query_audit(None, 0, u64::MAX, 100)
        .await
        .expect("query audit");
    let entry = entries
        .iter()
        .find(|entry| entry.admin_action.as_deref() == Some("upstream_limit_reset_claim"))
        .expect("claim audit entry must exist even when the outcome is unknown");
    assert_eq!(entry.status, StatusCode::GATEWAY_TIMEOUT.as_u16());
    assert_eq!(entry.upstream, "coupon-upstream");
    assert_eq!(entry.actor_subject.as_deref(), Some("test-static-token"));
    let payload = entry.payload.as_ref().expect("claim audit payload");
    assert_eq!(payload["grant_id"], "grant_01");
    assert_eq!(payload["request_id"], REQUEST_ID);
    assert_eq!(payload["outcome"], "claim_outcome_unknown");
}

#[tokio::test]
async fn claim_provider_4xx_is_a_definite_rejection() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            claim_body("grant_01"),
        )
        .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], "provider_rejected");
}

#[tokio::test]
async fn claim_normalizes_unrecognized_provider_result() {
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "brand_new_outcome",
            "reason": "brand_new_reason",
            "resets_left": 1,
            "cleared": []
        })))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim"),
            claim_body("grant_01"),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "unknown");
    assert_eq!(body["reason"], "unknown");
    assert_eq!(body["resets_left"], 1);
}
