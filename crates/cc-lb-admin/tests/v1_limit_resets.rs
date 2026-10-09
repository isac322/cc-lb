//! Route-level tests for the per-upstream limit-reset (cedar_ember) endpoints.
//! The GET is backed by the durable MetaStore snapshot the scheduler's usage
//! poll writes, so the wiremock provider only stands in for the claim path's
//! live profile + reset POST. Malformed `cedar_ember` blocks are rejected at
//! poll time (see the scheduler usage tests) and can never reach this
//! endpoint.

use axum::http::StatusCode;
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_clock::{Clock, SystemClock};
use cc_lb_config::SubscriptionQuotaConfig;
use cc_lb_control::DynamicViewBuilder;
use cc_lb_control::anthropic_metadata::{
    CedarEmberIdentityRecord, CedarEmberPollRecord, CedarEmberStatus, cedar_ember_epoch_fence,
    cedar_ember_epoch_meta_key, cedar_ember_identity_meta_key, cedar_ember_meta_key,
};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditQueryScope, AuditStore, MetaStore, UpstreamCreate, UpstreamRecord, UpstreamStore,
};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;
use wiremock::matchers::{method, path};
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

fn now_unix_millis() -> u64 {
    cc_lb_clock::unix_millis(SystemClock.now()).min(u128::from(u64::MAX)) as u64
}

/// The test harness builds its view with a zero staleness window; production
/// wires `subscription_quota.routing_max_staleness_secs` (default 1800s).
/// Rebind with the production default so the GET's freshness gate behaves as
/// deployed.
fn use_production_staleness_window(server: &SpawnedAdminServer) {
    let current = server.dynamic_view.load();
    let next = DynamicViewBuilder::from_view(&current)
        .subscription_quota_routing_max_staleness_secs(
            SubscriptionQuotaConfig::default().routing_max_staleness_secs,
        )
        .build();
    server.dynamic_view.store(next);
}

/// Reads the current invalidation epoch exactly as the poll does before
/// issuing its provider request.
async fn current_epoch(server: &SpawnedAdminServer, upstream_id: Uuid) -> Option<Uuid> {
    server
        .storage
        .get_meta_value(&cedar_ember_epoch_meta_key(upstream_id))
        .await
        .expect("epoch read")
        .and_then(|raw| Uuid::parse_str(&raw).ok())
}

/// Seeds the coupon exactly as the usage poll persists it: bound to the
/// upstream's credential fingerprint and the current invalidation epoch.
async fn seed_coupon(server: &SpawnedAdminServer, upstream: &UpstreamRecord, cedar_ember: Value) {
    let epoch = current_epoch(server, upstream.id).await;
    write_coupon_at_epoch(server, upstream, cedar_ember, epoch).await;
}

/// Writes a poll snapshot stamped with an explicit epoch — the epoch a poll
/// captured before issuing its provider request.
async fn write_coupon_at_epoch(
    server: &SpawnedAdminServer,
    upstream: &UpstreamRecord,
    cedar_ember: Value,
    epoch: Option<Uuid>,
) {
    let status =
        serde_json::from_value::<CedarEmberStatus>(cedar_ember).expect("coupon block parses");
    let record = CedarEmberPollRecord {
        credential_fingerprint: upstream
            .oauth_credential_fingerprint()
            .expect("oauth upstream has credentials"),
        epoch,
        observed_at_unix_millis: now_unix_millis(),
        status: Some(status),
    };
    server
        .storage
        .put_meta_value(
            &cedar_ember_meta_key(upstream.id),
            &serde_json::to_string(&record).expect("coupon record serializes"),
        )
        .await
        .expect("coupon record stored");
}

/// Seeds the credential-bound identity the poll writes after its one-time
/// profile fetch.
async fn seed_identity(server: &SpawnedAdminServer, upstream: &UpstreamRecord) {
    let record = CedarEmberIdentityRecord {
        credential_fingerprint: upstream
            .oauth_credential_fingerprint()
            .expect("oauth upstream has credentials"),
        account_id: ACCOUNT_UUID.to_owned(),
        organization_id: ORG_UUID.to_owned(),
    };
    server
        .storage
        .put_meta_value(
            &cedar_ember_identity_meta_key(upstream.id),
            &serde_json::to_string(&record).expect("identity record serializes"),
        )
        .await
        .expect("identity record stored");
}

async fn seed_oauth_upstream(server: &SpawnedAdminServer, base_url: &str) -> UpstreamRecord {
    let upstream = UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "coupon-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse(base_url).expect("base url parses")),
            api_key_ciphertext: None,
            oauth_tokens: None,
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
        .expect("store oauth tokens")
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
async fn get_limit_resets_returns_cached_status_without_provider_calls() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
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

    let requests = provider
        .received_requests()
        .await
        .expect("requests recorded");
    assert!(requests.is_empty(), "GET must not call the provider");
}

#[tokio::test]
async fn get_limit_resets_repeated_polls_never_hit_provider() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;

    for _ in 0..2 {
        let (status, _, body) = server
            .client
            .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["cedar_ember"]["next_grant_id"], "grant_01");
    }

    let requests = provider
        .received_requests()
        .await
        .expect("requests recorded");
    assert!(
        requests.is_empty(),
        "repeated GETs must not call the provider"
    );
}

#[tokio::test]
async fn get_limit_resets_without_cedar_ember_returns_null() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_identity(&server, &upstream).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["account_id"], ACCOUNT_UUID);
    assert!(body["cedar_ember"].is_null());
}

#[tokio::test]
async fn get_limit_resets_without_identity_metadata_returns_null_ids() {
    // Until the poll's one-time profile fetch binds an identity to this
    // credential, the fields are null rather than fabricated, and the coupon
    // is gated off too — a coupon without identity could never be claimed
    // anyway. The claim endpoint still re-verifies the live identity.
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["account_id"].is_null());
    assert!(body["organization_id"].is_null());
    assert!(body["cedar_ember"].is_null());
}

#[tokio::test]
async fn get_limit_resets_hides_coupon_after_credential_rotation() {
    // A coupon bound to a previous credential must never surface: the account
    // it belonged to may be different. Rewriting the stored credential (as
    // reauthorization does) changes the fingerprint the snapshot is bound to.
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;

    let rotated = OAuthTokenBundle {
        access_token: "sk-ant-oat01-rotated".to_owned(),
        refresh_token: "sk-ant-ort01-rotated".to_owned(),
        expires_at_unix_secs: 4_000_000_000,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["user:profile".to_owned()],
        never_refresh: true,
    };
    let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        &AeadService::from_master_key([0; 32]),
        &rotated,
        upstream.id.as_bytes(),
    )
    .expect("encrypt rotated bundle");
    server
        .storage
        .store_oauth_tokens(upstream.id, upstream.revision, encrypted, true)
        .await
        .expect("rotate credentials");

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["cedar_ember"].is_null());
    // The identity was bound to the old credential too — another account may
    // now own this upstream, so it is not reused.
    assert!(body["account_id"].is_null());
    assert!(body["organization_id"].is_null());
}

#[tokio::test]
async fn get_limit_resets_empty_grants_is_valid() {
    // An explicit empty array is a legitimate provider answer.
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(
        &server,
        &upstream,
        json!({
            "eligible": false,
            "ineligible_reason": "no_grant",
            "at_limit": true,
            "grants": []
        }),
    )
    .await;
    seed_identity(&server, &upstream).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
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
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    let mut block = cedar_ember_body();
    block["grants"][0]
        .as_object_mut()
        .expect("grant object")
        .remove("resets_total");
    seed_coupon(&server, &upstream, block).await;
    seed_identity(&server, &upstream).await;

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, _) = server
        .client
        .get_without_auth(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_limit_resets_rejects_non_oauth_upstream() {
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "api-key-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
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
async fn claim_invalidates_cached_coupon_and_preclaim_poll_cannot_restore_it() {
    // A dispatched claim may have consumed the grant; the claim bumps the
    // durable invalidation epoch so the GET reports null until a poll issued
    // after the claim re-observes it. A poll issued before the claim — whose
    // response lands after it — carries the old epoch and stays unservable.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/api/organizations/{ORG_UUID}/reset_rate_limits"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "reset",
            "resets_left": 0,
            "cleared": ["five_hour"]
        })))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;
    // An in-flight poll captured this epoch before issuing its request.
    let preclaim_epoch = current_epoch(&server, upstream.id).await;

    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            claim_body("grant_01"),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(
        current_epoch(&server, upstream.id).await,
        preclaim_epoch,
        "claim bumps the invalidation epoch"
    );

    assert!(
        get_ok(&server, upstream.id).await["cedar_ember"].is_null(),
        "spent coupon must not be served"
    );

    // The pre-claim poll's response lands after the claim.
    write_coupon_at_epoch(&server, &upstream, cedar_ember_body(), preclaim_epoch).await;
    assert!(
        get_ok(&server, upstream.id).await["cedar_ember"].is_null(),
        "a pre-claim poll response must never resurrect the coupon"
    );

    // A poll issued after the claim restores the snapshot.
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    assert_eq!(
        get_ok(&server, upstream.id).await["cedar_ember"]["next_grant_id"],
        "grant_01"
    );
}

async fn get_ok(server: &SpawnedAdminServer, upstream_id: Uuid) -> Value {
    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/upstreams/{upstream_id}/limit-resets"))
        .await;
    assert_eq!(status, StatusCode::OK);
    body
}

/// Which claim-step epoch write the injected SQLite trigger rejects.
enum FailEpochWrite {
    /// The pre-POST per-attempt fence (`pending:…`).
    Fence,
    /// The post-claim settle write (any non-fence value).
    Settle,
}

/// Makes one step's epoch write fail inside SQLite, simulating a storage
/// failure at exactly that point of the claim. The SQL is static; the
/// literal `'pending:%'` prefix is pinned to the shared fence format below.
async fn fail_epoch_writes(server: &SpawnedAdminServer, step: FailEpochWrite) {
    assert!(cedar_ember_epoch_fence(Uuid::nil(), 0).starts_with("pending:"));
    let statements: [&'static str; 2] = match step {
        FailEpochWrite::Fence => [
            "CREATE TRIGGER fail_epoch_insert BEFORE INSERT ON meta_v1 \
             WHEN NEW.key LIKE 'cedar_ember_epoch:%' AND NEW.value LIKE 'pending:%' \
             BEGIN SELECT RAISE(ABORT, 'injected epoch write failure'); END",
            "CREATE TRIGGER fail_epoch_update BEFORE UPDATE ON meta_v1 \
             WHEN NEW.key LIKE 'cedar_ember_epoch:%' AND NEW.value LIKE 'pending:%' \
             BEGIN SELECT RAISE(ABORT, 'injected epoch write failure'); END",
        ],
        FailEpochWrite::Settle => [
            "CREATE TRIGGER fail_epoch_insert BEFORE INSERT ON meta_v1 \
             WHEN NEW.key LIKE 'cedar_ember_epoch:%' AND NEW.value NOT LIKE 'pending:%' \
             BEGIN SELECT RAISE(ABORT, 'injected epoch write failure'); END",
            "CREATE TRIGGER fail_epoch_update BEFORE UPDATE ON meta_v1 \
             WHEN NEW.key LIKE 'cedar_ember_epoch:%' AND NEW.value NOT LIKE 'pending:%' \
             BEGIN SELECT RAISE(ABORT, 'injected epoch write failure'); END",
        ],
    };
    for statement in statements {
        sqlx::query(statement)
            .execute(server.storage.pool())
            .await
            .expect("install failure trigger");
    }
}

#[tokio::test]
async fn claim_fence_write_failure_never_dispatches() {
    // The durable fence must land before the POST; if it cannot, nothing is
    // sent and nothing can be consumed.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "reset"})))
        .expect(0)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;
    fail_epoch_writes(&server, FailEpochWrite::Fence).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            claim_body("grant_01"),
        )
        .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "internal_error");
}

#[tokio::test]
async fn claim_settle_write_failure_keeps_coupon_fenced() {
    // The claim reached the provider but the post-claim epoch write failed:
    // the pending fence stays current, so neither the old snapshot nor a
    // late pre-claim poll response is ever served, and the failure surfaces.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/api/organizations/{ORG_UUID}/reset_rate_limits"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "reset",
            "resets_left": 0
        })))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;
    let preclaim_epoch = current_epoch(&server, upstream.id).await;
    fail_epoch_writes(&server, FailEpochWrite::Settle).await;

    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            claim_body("grant_01"),
        )
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    assert!(
        get_ok(&server, upstream.id).await["cedar_ember"].is_null(),
        "old snapshot must stay fenced"
    );
    write_coupon_at_epoch(&server, &upstream, cedar_ember_body(), preclaim_epoch).await;
    assert!(
        get_ok(&server, upstream.id).await["cedar_ember"].is_null(),
        "a late pre-claim poll response must stay fenced"
    );
}

#[tokio::test]
async fn get_limit_resets_fails_closed_on_unreadable_epoch() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;
    server
        .storage
        .put_meta_value(&cedar_ember_epoch_meta_key(upstream.id), "not-a-uuid")
        .await
        .expect("corrupt epoch");

    assert!(get_ok(&server, upstream.id).await["cedar_ember"].is_null());
}

#[tokio::test]
async fn claim_refuses_any_unsettled_epoch_without_dispatching() {
    // A live fence (another claim in flight), an abandoned fence awaiting
    // poll recovery, and an unreadable value all refuse the claim: a grant
    // is never claimed again before a fresh post-claim observation, and the
    // claim never takes over or rewrites someone else's fence.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "reset"})))
        .expect(0)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    let key = cedar_ember_epoch_meta_key(upstream.id);
    let now_unix_millis = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("millis fit u64");

    for unsettled in [
        cedar_ember_epoch_fence(Uuid::new_v4(), now_unix_millis),
        cedar_ember_epoch_fence(Uuid::new_v4(), 0),
        "not-a-uuid".to_owned(),
    ] {
        server
            .storage
            .put_meta_value(&key, &unsettled)
            .await
            .expect("seed unsettled epoch");
        let (status, _, body) = server
            .client
            .post_json(
                &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
                claim_body("grant_01"),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{unsettled}");
        assert_eq!(body["error"], "conflict");
        assert_eq!(
            server
                .storage
                .get_meta_value(&key)
                .await
                .expect("read epoch"),
            Some(unsettled),
            "a refused claim must leave the epoch untouched"
        );
    }
}

#[tokio::test]
async fn concurrent_claims_dispatch_exactly_once() {
    // Two claims for the same upstream race: the compare-and-put fence lets
    // exactly one dispatch; the other is refused before any POST, and the
    // winner's settle leaves a settled epoch behind.
    let provider = MockServer::start().await;
    mount_profile(&provider).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/api/organizations/{ORG_UUID}/reset_rate_limits"
        )))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result": "reset", "resets_left": 0}))
                .set_delay(std::time::Duration::from_millis(500)),
        )
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    seed_coupon(&server, &upstream, cedar_ember_body()).await;
    seed_identity(&server, &upstream).await;
    let route = format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id);

    let ((first, _, _), (second, _, _)) = tokio::join!(
        server.client.post_json(&route, claim_body("grant_01")),
        server.client.post_json(&route, claim_body("grant_01")),
    );

    let mut statuses = [first, second];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::OK, StatusCode::CONFLICT]);
    let epoch = server
        .storage
        .get_meta_value(&cedar_ember_epoch_meta_key(upstream.id))
        .await
        .expect("read epoch")
        .expect("claim settled an epoch");
    assert!(
        Uuid::parse_str(&epoch).is_ok(),
        "winner must settle: {epoch}"
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let mut body = claim_body("grant_01");
    body["account_id"] = json!("stale-account");
    let (status, _, response) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let mut bad_grant = claim_body("Grant With Spaces");
    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
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
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            claim_body("grant_01"),
        )
        .await;

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(body["error"], "claim_outcome_unknown");

    // An indeterminate claim still leaves a full audit trail: actor, target
    // upstream, submitted grant/request id, and the stable outcome code.
    let entries = server
        .storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
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
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            claim_body("grant_01"),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "unknown");
    assert_eq!(body["reason"], "unknown");
    assert_eq!(body["resets_left"], 1);
}
