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
use cc_lb_control::openai_limit_resets::{
    OpenAiLimitResetInventory, OpenAiLimitResetPollRecord, openai_limit_reset_epoch_meta_key,
    openai_limit_reset_meta_key,
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
            "type": "weekly",
            "gives_back_weekly": false,
            "unavailable_reason": null,
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
    seed_provider_oauth_upstream(server, base_url, UpstreamKind::AnthropicOauth).await
}

async fn seed_provider_oauth_upstream(
    server: &SpawnedAdminServer,
    base_url: &str,
    kind: UpstreamKind,
) -> UpstreamRecord {
    let upstream_id = Uuid::new_v4();
    let upstream = UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            id: upstream_id,
            name: format!("coupon-{upstream_id}"),
            kind,
            base_url: Some(Url::parse(base_url).expect("base url parses")),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create upstream");
    let bundle = OAuthTokenBundle {
        oauth_profile: Default::default(),
        access_token: ACCESS_TOKEN.to_owned(),
        refresh_token: "sk-ant-ort01-limit-reset-test".to_owned(),
        expires_at_unix_secs: 4_000_000_000,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["user:profile".to_owned()],
        account_id: (kind == UpstreamKind::OpenaiOauth).then(|| ACCOUNT_UUID.to_owned()),
        project_id: None,
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
    assert_eq!(grant["type"], "weekly");
    assert_eq!(grant["gives_back_weekly"], false);
    assert!(grant["unavailable_reason"].is_null());
    assert_eq!(grant["use_requires_limit"], false);

    let requests = provider
        .received_requests()
        .await
        .expect("requests recorded");
    assert!(requests.is_empty(), "GET must not call the provider");
}

async fn get_limit_resets_absent_five_hour_preserves_provider_at_limit() {
    for exhausted in [Some(json!([])), None] {
        let provider = MockServer::start().await;
        let server = admin_test_common::spawn_admin_server().await;
        let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
        use_production_staleness_window(&server);
        let mut coupon = cedar_ember_body();
        if let Some(exhausted) = &exhausted {
            coupon["exhausted"] = exhausted.clone();
        } else {
            coupon.as_object_mut().unwrap().remove("exhausted");
        }
        seed_coupon(&server, &upstream, coupon).await;
        seed_identity(&server, &upstream).await;

        let route = format!("/admin/v1/upstreams/{}/limit-resets", upstream.id);
        let now = now_unix_millis();
        let mut sample: cc_lb_storage_api::SubscriptionQuotaSample =
            serde_json::from_value(json!({
                "upstream_id": upstream.id,
                "window": "5h",
                "source": "api",
                "sample_kind": "sample",
                "observed_at_unix_millis": now,
                "sample_id": Uuid::new_v4(),
                "utilization": 0.40,
                "resets_at_unix_secs": now / 1_000 + 3_600,
                "ingested_at_unix_millis": now,
            }))
            .expect("present quota fixture");
        cc_lb_storage_api::UpstreamSubscriptionQuotaStore::record_subscription_quota_samples(
            server.storage.as_ref(),
            std::slice::from_ref(&sample),
        )
        .await
        .unwrap();
        let (status, _, body) = server.client.get(&route).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["cedar_ember"]["at_limit"], true,
            "a present 5h window must preserve the provider's at_limit"
        );

        sample.sample_kind = cc_lb_storage_api::SubscriptionQuotaSampleKind::Absent;
        sample.sample_id = Uuid::new_v4();
        sample.observed_at_unix_millis = now + 1;
        sample.ingested_at_unix_millis = now + 1;
        sample.utilization = None;
        sample.resets_at_unix_secs = None;
        cc_lb_storage_api::UpstreamSubscriptionQuotaStore::record_subscription_quota_samples(
            server.storage.as_ref(),
            &[sample],
        )
        .await
        .unwrap();
        let (status, _, body) = server.client.get(&route).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["cedar_ember"]["exhausted"],
            exhausted.unwrap_or(Value::Null)
        );
        assert_eq!(
            body["cedar_ember"]["at_limit"], true,
            "an absent 5h window must not rewrite the provider account-level gate"
        );
        assert!(provider.received_requests().await.unwrap().is_empty());
    }
}

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
        oauth_profile: Default::default(),
        account_id: None,
        project_id: None,
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

async fn get_limit_resets_requires_auth() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;

    let (status, _, _) = server
        .client
        .get_without_auth(&format!("/admin/v1/upstreams/{}/limit-resets", upstream.id))
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = server
        .client
        .get_without_auth("/admin/v1/upstreams/limit-resets")
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

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
            workspace_id: None,
            api_key_access: None,
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
    assert_eq!(entry.upstream, upstream.name);
    assert_eq!(entry.actor_subject.as_deref(), Some("test-static-token"));
    let payload = entry.payload.as_ref().expect("claim audit payload");
    assert_eq!(payload["grant_id"], "grant_01");
    assert_eq!(payload["request_id"], REQUEST_ID);
    assert_eq!(payload["outcome"], "claim_outcome_unknown");
}

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

async fn get_limit_resets_preserves_unknown_gate_and_fractional_percentages() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    use_production_staleness_window(&server);
    seed_identity(&server, &upstream).await;
    let mut coupon = cedar_ember_body();
    coupon.as_object_mut().unwrap().remove("at_limit");
    coupon.as_object_mut().unwrap().remove("exhausted");
    coupon["grants"][0]["type"] = json!("full");
    coupon["grants"][0]["gives_back_weekly"] = json!(true);
    coupon["grants"][0]["unavailable_reason"] = json!("not_at_limit");
    coupon["grants"][0]["percent_used"] = json!({
        "five_hour": 95.25,
        "seven_day": 0.5,
        "seven_day_overage_included": 100,
        "negative": -0.5,
        "over_limit": 100.1,
        "invalid": "40"
    });
    seed_coupon(&server, &upstream, coupon).await;
    let body = get_ok(&server, upstream.id).await;
    assert!(body["cedar_ember"]["at_limit"].is_null());
    assert!(body["cedar_ember"]["exhausted"].is_null());
    assert_eq!(
        body["cedar_ember"]["grants"][0]["percent_used"],
        json!({
            "5h": 95.25,
            "7d": 0.5,
            "7d_overage_included": 100
        })
    );
    assert_eq!(body["cedar_ember"]["grants"][0]["type"], "full");
    assert_eq!(body["cedar_ember"]["grants"][0]["gives_back_weekly"], true);
    assert_eq!(
        body["cedar_ember"]["grants"][0]["unavailable_reason"],
        "not_at_limit"
    );
    assert!(provider.received_requests().await.unwrap().is_empty());
}

async fn list_limit_resets_filters_upstreams_and_preserves_unknown_snapshots() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    use_production_staleness_window(&server);
    let ready = seed_oauth_upstream(&server, &provider.uri()).await;
    seed_identity(&server, &ready).await;
    let mut coupon = cedar_ember_body();
    coupon["grants"][0]["paused"] = json!(true);
    coupon["grants"][0]["usable_now"] = json!(false);
    coupon["cooldown_until"] = json!("2099-01-01T00:00:00Z");
    seed_coupon(&server, &ready, coupon).await;
    let unknown = seed_oauth_upstream(&server, &provider.uri()).await;
    let stale = seed_oauth_upstream(&server, &provider.uri()).await;
    seed_identity(&server, &stale).await;
    seed_coupon(&server, &stale, cedar_ember_body()).await;
    let key = cedar_ember_meta_key(stale.id);
    let mut record: CedarEmberPollRecord =
        serde_json::from_str(&server.storage.get_meta_value(&key).await.unwrap().unwrap()).unwrap();
    record.observed_at_unix_millis = 0;
    server
        .storage
        .put_meta_value(&key, &serde_json::to_string(&record).unwrap())
        .await
        .unwrap();
    let fenced = seed_oauth_upstream(&server, &provider.uri()).await;
    seed_identity(&server, &fenced).await;
    seed_coupon(&server, &fenced, cedar_ember_body()).await;
    server
        .storage
        .put_meta_value(
            &cedar_ember_epoch_meta_key(fenced.id),
            &cedar_ember_epoch_fence(Uuid::new_v4(), now_unix_millis()),
        )
        .await
        .unwrap();
    let disabled = seed_oauth_upstream(&server, &provider.uri()).await;
    UpstreamStore::set_enabled(
        server.storage.as_ref(),
        disabled.id,
        disabled.revision,
        false,
    )
    .await
    .unwrap();
    let deleted = seed_oauth_upstream(&server, &provider.uri()).await;
    UpstreamStore::soft_delete(server.storage.as_ref(), deleted.id, deleted.revision)
        .await
        .unwrap();
    for kind in [
        UpstreamKind::AnthropicOauth,
        UpstreamKind::AnthropicApiKey,
        UpstreamKind::OpenaiOauth,
        UpstreamKind::GeminiOauth,
    ] {
        let id = Uuid::new_v4();
        UpstreamStore::create(
            server.storage.as_ref(),
            UpstreamCreate {
                id,
                name: format!("excluded-{id}"),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .unwrap();
    }
    let (status, _, body) = server.client.get("/admin/v1/upstreams/limit-resets").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "static route must not enter the UUID detail route"
    );
    let upstreams = body["upstreams"].as_array().expect("fleet array");
    assert_eq!(upstreams.len(), 4);
    for upstream in [&ready, &unknown, &stale, &fenced] {
        let row = upstreams
            .iter()
            .find(|row| row["upstream_id"] == upstream.id.to_string())
            .expect("eligible upstream is retained");
        assert_eq!(row["provider"], "anthropic");
        if upstream.id == ready.id {
            assert_eq!(row["account_id"], ACCOUNT_UUID);
            assert_eq!(row["organization_id"], ORG_UUID);
            assert_eq!(row["cedar_ember"]["grants"][0]["resets_left"], 1);
            assert_eq!(row["cedar_ember"]["grants"][0]["paused"], true);
            assert_eq!(row["cedar_ember"]["grants"][0]["usable_now"], false);
        } else {
            assert!(
                row["cedar_ember"].is_null(),
                "unavailable is not known empty"
            );
        }
    }
    let unknown_row = upstreams
        .iter()
        .find(|row| row["upstream_id"] == unknown.id.to_string())
        .unwrap();
    assert!(unknown_row["account_id"].is_null());
    assert!(unknown_row["organization_id"].is_null());
    assert!(provider.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn v1_limit_resets_suite_part_1() {
    get_limit_resets_returns_cached_status_without_provider_calls().await;
    get_limit_resets_absent_five_hour_preserves_provider_at_limit().await;
    get_limit_resets_repeated_polls_never_hit_provider().await;
    get_limit_resets_without_cedar_ember_returns_null().await;
    get_limit_resets_without_identity_metadata_returns_null_ids().await;
    get_limit_resets_hides_coupon_after_credential_rotation().await;
    get_limit_resets_empty_grants_is_valid().await;
    get_limit_resets_forwards_null_resets_total().await;
    get_limit_resets_requires_auth().await;
    get_limit_resets_rejects_non_oauth_upstream().await;
    get_limit_resets_preserves_unknown_gate_and_fractional_percentages().await;
    list_limit_resets_filters_upstreams_and_preserves_unknown_snapshots().await;
    claim_dispatches_exact_grant_and_request_id_once().await;
}

#[tokio::test]
async fn v1_limit_resets_suite_part_2() {
    claim_invalidates_cached_coupon_and_preclaim_poll_cannot_restore_it().await;
    claim_fence_write_failure_never_dispatches().await;
    claim_settle_write_failure_keeps_coupon_fenced().await;
    get_limit_resets_fails_closed_on_unreadable_epoch().await;
    claim_refuses_any_unsettled_epoch_without_dispatching().await;
    concurrent_claims_dispatch_exactly_once().await;
    claim_rejects_stale_identity_without_dispatching().await;
    claim_rejects_malformed_ids_without_dispatching().await;
    claim_provider_5xx_reports_unknown_not_failure().await;
    claim_provider_4xx_is_a_definite_rejection().await;
    claim_normalizes_unrecognized_provider_result().await;
}

fn openai_inventory() -> Value {
    json!({
        "available_count": 3,
        "applicable_available_count": null,
        "credits": [
            {"id":"Credit:Chosen/02","reset_type":"codex_rate_limits","status":"available",
             "is_supported_by_plan":true,"granted_at":"2026-11-01T00:00:00Z",
             "expires_at":null,"title":"Full reset","description":null},
            {"id":"unsupported","reset_type":"future_type","status":"available",
             "is_supported_by_plan":false,"granted_at":"2026-11-01T00:00:00Z"},
            {"id":"unknown","reset_type":"future_type","status":"available",
             "granted_at":"2026-11-01T00:00:00Z"}
        ]
    })
}

async fn store_openai_inventory(
    server: &SpawnedAdminServer,
    upstream: &UpstreamRecord,
    record: &OpenAiLimitResetPollRecord,
) {
    server
        .storage
        .put_meta_value(
            &openai_limit_reset_meta_key(upstream.id),
            &serde_json::to_string(record).unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn openai_cached_inventory_preserves_gates_and_fails_closed() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    use_production_staleness_window(&server);
    let upstream =
        seed_provider_oauth_upstream(&server, &provider.uri(), UpstreamKind::OpenaiOauth).await;
    assert!(get_ok(&server, upstream.id).await["openai"].is_null());
    let fresh = OpenAiLimitResetPollRecord {
        credential_fingerprint: upstream.oauth_credential_fingerprint().unwrap(),
        oauth_token_generation: upstream.oauth_token_generation,
        account_id: ACCOUNT_UUID.to_owned(),
        epoch: None,
        observed_at_unix_millis: now_unix_millis(),
        inventory: Some(
            serde_json::from_value::<OpenAiLimitResetInventory>(openai_inventory()).unwrap(),
        ),
    };
    store_openai_inventory(&server, &upstream, &fresh).await;
    let response = get_ok(&server, upstream.id).await;
    assert_eq!(response["provider"], "openai");
    assert_eq!(response["account_id"], ACCOUNT_UUID);
    assert!(response["organization_id"].is_null());
    assert!(response["cedar_ember"].is_null());
    assert_eq!(response["openai"]["available_count"], 3);
    assert!(response["openai"]["applicable_available_count"].is_null());
    assert_eq!(
        response["openai"]["credits"][1]["is_supported_by_plan"],
        false
    );
    assert!(response["openai"]["credits"][2]["is_supported_by_plan"].is_null());
    let (status, _, fleet) = server.client.get("/admin/v1/upstreams/limit-resets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fleet["upstreams"][0]["openai"], response["openai"]);
    for invalidation in 0..6 {
        let mut record = fresh.clone();
        match invalidation {
            0 => record.observed_at_unix_millis = 0,
            1 => record.credential_fingerprint ^= 1,
            2 => record.oauth_token_generation += 1,
            3 => record.account_id = "different-account".to_owned(),
            4 => record.epoch = Some(Uuid::new_v4()),
            _ => record.inventory = None,
        }
        store_openai_inventory(&server, &upstream, &record).await;
        assert!(get_ok(&server, upstream.id).await["openai"].is_null());
    }
    store_openai_inventory(&server, &upstream, &fresh).await;
    for epoch in [
        "unreadable".to_owned(),
        cedar_ember_epoch_fence(Uuid::new_v4(), now_unix_millis()),
    ] {
        server
            .storage
            .put_meta_value(&openai_limit_reset_epoch_meta_key(upstream.id), &epoch)
            .await
            .unwrap();
        assert!(get_ok(&server, upstream.id).await["openai"].is_null());
    }
    assert!(provider.received_requests().await.unwrap().is_empty());
}

async fn mount_openai_inventory(provider: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/codex/rate-limit-reset-credits"))
        .and(wiremock::matchers::header(
            "chatgpt-account-id",
            ACCOUNT_UUID,
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_inventory()))
        .mount(provider)
        .await;
}

fn openai_claim_body(credit_id: &str) -> Value {
    json!({"account_id":ACCOUNT_UUID,"grant_id":credit_id,"request_id":REQUEST_ID})
}

#[tokio::test]
async fn openai_claim_binds_account_and_exact_credit_and_preserves_count() {
    let provider = MockServer::start().await;
    mount_openai_inventory(&provider).await;
    Mock::given(method("POST"))
        .and(path("/api/codex/rate-limit-reset-credits/consume"))
        .and(wiremock::matchers::header(
            "chatgpt-account-id",
            ACCOUNT_UUID,
        ))
        .and(wiremock::matchers::body_json(json!({
            "credit_id":"Credit:Chosen/02","redeem_request_id":REQUEST_ID
        })))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"code":"reset","windows_reset":2})),
        )
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream =
        seed_provider_oauth_upstream(&server, &provider.uri(), UpstreamKind::OpenaiOauth).await;
    struct ExistingRoutingQuota(std::sync::atomic::AtomicUsize);
    impl cc_lb_control::SubscriptionQuotaCacheLike for ExistingRoutingQuota {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        fn snapshot_for_upstream(
            &self,
            _upstream_id: Uuid,
            now_unix_millis: u64,
            max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            vec![
                serde_json::from_value(json!({
                    "window": "7d",
                    "state": "fresh",
                    "source": "api",
                    "utilization": 1.0,
                    "resets_at_unix_secs": now_unix_millis / 1_000 + 3_600,
                    "observed_at_unix_millis": now_unix_millis,
                    "max_staleness_secs": max_staleness_secs,
                }))
                .unwrap(),
            ]
        }
    }
    let cache = std::sync::Arc::new(ExistingRoutingQuota(0.into()));
    let next = DynamicViewBuilder::from_view(&server.dynamic_view.load())
        .subscription_quota_cache(cache.clone())
        .build();
    server.dynamic_view.store(next);
    let sample_time = now_unix_millis();
    let before = serde_json::to_value(
        server
            .dynamic_view
            .load()
            .subscription_quota_cache
            .snapshot_for_upstream(upstream.id, sample_time, 1_800),
    )
    .unwrap();
    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            openai_claim_body("Credit:Chosen/02"),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "reset");
    assert_eq!(body["windows_reset"], 2);
    assert_eq!(body["cleared"], json!([]));
    assert_eq!(body["usage_refresh"], "failed");
    assert_eq!(cache.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        serde_json::to_value(
            server
                .dynamic_view
                .load()
                .subscription_quota_cache
                .snapshot_for_upstream(upstream.id, sample_time, 1_800)
        )
        .unwrap(),
        before,
        "a missing refresh port must leave the existing routing quota unchanged",
    );
    assert!(
        server
            .storage
            .get_meta_value(&openai_limit_reset_epoch_meta_key(upstream.id))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn openai_claim_rejects_unknown_unsupported_foreign_and_invalid_requests() {
    let provider = MockServer::start().await;
    mount_openai_inventory(&provider).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream =
        seed_provider_oauth_upstream(&server, &provider.uri(), UpstreamKind::OpenaiOauth).await;
    let route = format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id);
    for credit in ["unsupported", "unknown", "not-in-inventory"] {
        let (status, _, _) = server
            .client
            .post_json(&route, openai_claim_body(credit))
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }
    let mut foreign = openai_claim_body("Credit:Chosen/02");
    foreign["account_id"] = json!("foreign-account");
    let (status, _, body) = server.client.post_json(&route, foreign).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "stale_identity");
    let mut malformed = openai_claim_body("Credit:Chosen/02");
    malformed["request_id"] = json!("not-a-uuid");
    assert_eq!(
        server.client.post_json(&route, malformed).await.0,
        StatusCode::BAD_REQUEST
    );
}

struct BlockingUsageRefresh {
    outcome: cc_lb_admin::LimitResetUsageRefresh,
    calls: std::sync::atomic::AtomicUsize,
    started: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    finished: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl cc_lb_admin::LimitResetUsageRefreshPort for BlockingUsageRefresh {
    async fn refresh_upstream_usage(
        &self,
        _upstream_id: Uuid,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> cc_lb_admin::LimitResetUsageRefresh {
        assert!(!cancel.is_cancelled(), "continuation has a fresh token");
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.started.notify_one();
        let permit = self.release.acquire().await.unwrap();
        permit.forget();
        assert!(!cancel.is_cancelled(), "disconnect must not cancel refresh");
        self.finished.notify_one();
        self.outcome
    }
}

fn refresh_test_router(
    server: &SpawnedAdminServer,
    refresh: std::sync::Arc<BlockingUsageRefresh>,
) -> axum::Router {
    let config = cc_lb_config::Config::default();
    cc_lb_admin::router(cc_lb_admin::AdminState {
        build: admin_test_common::test_build_info(),
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(server.storage.clone()),
        key_store: Some(admin_test_common::key_store(server.storage.clone())),
        aead: std::sync::Arc::new(AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        limit_reset_usage_refresh: Some(refresh),
        dynamic_view: server.dynamic_view.clone(),
        config: std::sync::Arc::new(config),
        dynamic_view_rebinder: None,
        scheduler: None,
        admin_auth: admin_test_common::static_token_auth("test-token"),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: std::sync::Arc::new(SystemClock),
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production().unwrap(),
    })
}

async fn router_claim(app: axum::Router, upstream_id: Uuid, body: Value) -> (StatusCode, Value) {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(format!(
            "/admin/v1/upstreams/{upstream_id}/limit-resets/claim"
        ))
        .header("authorization", "Bearer test-token")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn usage_refresh_completion_case(
    kind: UpstreamKind,
    outcome: cc_lb_admin::LimitResetUsageRefresh,
    disconnect: bool,
    provider_outcome: &str,
) {
    use std::sync::{Arc, atomic::Ordering};
    use std::time::Duration;
    let provider = MockServer::start().await;
    let provider_response = if kind == UpstreamKind::OpenaiOauth {
        mount_openai_inventory(&provider).await;
        json!({"code":provider_outcome})
    } else {
        mount_profile(&provider).await;
        json!({"result":provider_outcome})
    };
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(provider_response))
        .expect(1)
        .mount(&provider)
        .await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_provider_oauth_upstream(&server, &provider.uri(), kind).await;
    let refresh = Arc::new(BlockingUsageRefresh {
        outcome,
        calls: 0.into(),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
        finished: tokio::sync::Notify::new(),
    });
    let app = refresh_test_router(&server, refresh.clone());
    let claim = if kind == UpstreamKind::OpenaiOauth {
        openai_claim_body("Credit:Chosen/02")
    } else {
        claim_body("grant_01")
    };
    let task = tokio::spawn(router_claim(app, upstream.id, claim));
    tokio::time::timeout(Duration::from_secs(5), refresh.started.notified())
        .await
        .unwrap();
    assert!(
        !task.is_finished(),
        "HTTP success waits for refresh completion"
    );
    let epoch_key = if kind == UpstreamKind::OpenaiOauth {
        openai_limit_reset_epoch_meta_key(upstream.id)
    } else {
        cedar_ember_epoch_meta_key(upstream.id)
    };
    let epoch = server
        .storage
        .get_meta_value(&epoch_key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        Uuid::parse_str(&epoch).is_ok(),
        "fence settles before usage fetch"
    );
    if disconnect {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        refresh.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), refresh.finished.notified())
            .await
            .unwrap();
    } else {
        refresh.release.add_permits(1);
        let (status, response) = task.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response["result"],
            if provider_outcome == "already_redeemed" {
                "already_used"
            } else {
                "reset"
            }
        );
        assert_eq!(
            response["usage_refresh"],
            serde_json::to_value(outcome).unwrap()
        );
        assert!(
            response["windows_reset"].is_null(),
            "absent count is not zero"
        );
    }
    assert_eq!(refresh.calls.load(Ordering::SeqCst), 1);
    let requests = provider.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "POST")
            .count(),
        1,
        "refresh failure/disconnect never retries consumption"
    );
}

#[tokio::test]
async fn reset_awaits_usage_refresh_and_keeps_refresh_failure_distinct() {
    usage_refresh_completion_case(
        UpstreamKind::AnthropicOauth,
        cc_lb_admin::LimitResetUsageRefresh::Refreshed,
        false,
        "reset",
    )
    .await;
    usage_refresh_completion_case(
        UpstreamKind::OpenaiOauth,
        cc_lb_admin::LimitResetUsageRefresh::Failed,
        false,
        "reset",
    )
    .await;
    usage_refresh_completion_case(
        UpstreamKind::OpenaiOauth,
        cc_lb_admin::LimitResetUsageRefresh::Refreshed,
        false,
        "already_redeemed",
    )
    .await;
}

#[tokio::test]
async fn reset_usage_refresh_survives_client_disconnect() {
    usage_refresh_completion_case(
        UpstreamKind::AnthropicOauth,
        cc_lb_admin::LimitResetUsageRefresh::Refreshed,
        true,
        "reset",
    )
    .await;
    usage_refresh_completion_case(
        UpstreamKind::OpenaiOauth,
        cc_lb_admin::LimitResetUsageRefresh::Refreshed,
        true,
        "reset",
    )
    .await;
}

#[tokio::test]
async fn openai_claim_maps_non_reset_and_uncertain_outcomes_without_refresh() {
    for (provider_status, provider_body, expected_status, expected_result) in [
        (
            200,
            json!({"code":"nothing_to_reset","windows_reset":0}),
            StatusCode::OK,
            "not_limited",
        ),
        (
            200,
            json!({"code":"no_credit"}),
            StatusCode::OK,
            "unavailable",
        ),
        (
            200,
            json!({"code":"future_unknown_code"}),
            StatusCode::GATEWAY_TIMEOUT,
            "claim_outcome_unknown",
        ),
        (
            503,
            json!({}),
            StatusCode::GATEWAY_TIMEOUT,
            "claim_outcome_unknown",
        ),
        (
            429,
            json!({}),
            StatusCode::TOO_MANY_REQUESTS,
            "provider_rejected",
        ),
    ] {
        let provider = MockServer::start().await;
        mount_openai_inventory(&provider).await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(provider_status).set_body_json(provider_body))
            .expect(1)
            .mount(&provider)
            .await;
        let server = admin_test_common::spawn_admin_server().await;
        let upstream =
            seed_provider_oauth_upstream(&server, &provider.uri(), UpstreamKind::OpenaiOauth).await;
        let refresh = std::sync::Arc::new(BlockingUsageRefresh {
            outcome: cc_lb_admin::LimitResetUsageRefresh::Refreshed,
            calls: 0.into(),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(1),
            finished: tokio::sync::Notify::new(),
        });
        let app = refresh_test_router(&server, refresh.clone());
        let (status, response) =
            router_claim(app, upstream.id, openai_claim_body("Credit:Chosen/02")).await;
        assert_eq!(status, expected_status);
        assert_eq!(
            response[if status == StatusCode::OK {
                "result"
            } else {
                "error"
            }],
            expected_result
        );
        assert!(response.get("usage_refresh").is_none());
        assert_eq!(refresh.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        if expected_result == "not_limited" {
            assert_eq!(response["windows_reset"], 0);
        }
        let epoch = server
            .storage
            .get_meta_value(&openai_limit_reset_epoch_meta_key(upstream.id))
            .await
            .unwrap()
            .unwrap();
        assert!(Uuid::parse_str(&epoch).is_ok());
    }
}

#[tokio::test]
async fn anthropic_claim_requires_organization_before_provider_request() {
    let provider = MockServer::start().await;
    let server = admin_test_common::spawn_admin_server().await;
    let upstream = seed_oauth_upstream(&server, &provider.uri()).await;
    let mut body = claim_body("grant_01");
    body.as_object_mut().unwrap().remove("organization_id");
    let (status, _, response) = server
        .client
        .post_json(
            &format!("/admin/v1/upstreams/{}/limit-resets/claim", upstream.id),
            body,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"], "invalid_organization_id");
    assert!(provider.received_requests().await.unwrap().is_empty());
}
