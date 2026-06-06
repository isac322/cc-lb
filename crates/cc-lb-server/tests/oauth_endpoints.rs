use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode};
use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::organization_metadata::{
    OrganizationMetadataRecord, OrganizationMetadataStore,
};
use cc_lb_storage_api::types::{PrincipalKindLite, UpstreamKind as KeyUpstreamKind};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::upstream_subscription_metadata::{
    UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use cc_lb_storage_api::upstream_subscription_quota::{
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
};
use cc_lb_storage_api::{
    ManagedKeyStore, PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalStore,
    Storage as StorageTrait, UpstreamStore,
};
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage as RedbStorage};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn usage_returns_aggregated_max_utilization_min_future_resets() -> TestResult<()> {
    let harness = Harness::new(2, AllowedUpstreams::All).await?;
    let now = now_secs();
    harness
        .put_quota(quota(
            harness.upstreams[0],
            SubscriptionQuotaWindow::FiveHour,
            Some(0.4),
            Some(now + 120),
        ))
        .await?;
    harness
        .put_quota(quota(
            harness.upstreams[1],
            SubscriptionQuotaWindow::FiveHour,
            Some(0.8),
            Some(now + 60),
        ))
        .await?;

    let response = harness.auth_get_authorization("/api/oauth/usage").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_f64(response.json["five_hour"]["utilization"].as_f64(), 0.8);
    assert_eq!(
        response.json["five_hour"]["resets_at"],
        Value::String(iso8601(now + 60))
    );
    assert!(response.json["seven_day"].is_null());
    Ok(())
}

#[tokio::test]
async fn usage_extra_usage_is_or_sum_aggregated() -> TestResult<()> {
    let harness = Harness::new(2, AllowedUpstreams::All).await?;
    harness
        .put_quota(extra_quota(
            harness.upstreams[0],
            true,
            Some(100.0),
            Some(25.0),
        ))
        .await?;
    harness
        .put_quota(extra_quota(
            harness.upstreams[1],
            false,
            Some(50.0),
            Some(10.0),
        ))
        .await?;

    let response = harness.auth_get("/api/oauth/usage").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.json["extra_usage"]["is_enabled"], true);
    assert_f64(
        response.json["extra_usage"]["monthly_limit"].as_f64(),
        150.0,
    );
    assert_f64(response.json["extra_usage"]["used_credits"].as_f64(), 35.0);
    Ok(())
}

#[tokio::test]
async fn usage_extra_usage_utilization_computed() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;
    harness
        .put_quota(extra_quota(
            harness.upstreams[0],
            true,
            Some(100.0),
            Some(25.0),
        ))
        .await?;

    let response = harness.auth_get("/api/oauth/usage").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_f64(response.json["extra_usage"]["utilization"].as_f64(), 0.25);

    let zero_limit = Harness::new(1, AllowedUpstreams::All).await?;
    zero_limit
        .put_quota(extra_quota(
            zero_limit.upstreams[0],
            true,
            Some(0.0),
            Some(25.0),
        ))
        .await?;

    let response = zero_limit.auth_get("/api/oauth/usage").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(response.json["extra_usage"]["utilization"].is_null());
    Ok(())
}

#[tokio::test]
async fn usage_principal_with_no_upstreams_returns_nulls_not_500() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::None).await?;

    let response = harness.auth_get("/api/oauth/usage").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(response.json["five_hour"].is_null());
    assert!(response.json["seven_day"].is_null());
    assert!(response.json["seven_day_opus"].is_null());
    assert!(response.json["seven_day_sonnet"].is_null());
    assert_eq!(response.json["extra_usage"]["is_enabled"], false);
    assert!(response.json["extra_usage"]["monthly_limit"].is_null());
    assert!(response.json["extra_usage"]["used_credits"].is_null());
    assert!(response.json["extra_usage"]["utilization"].is_null());
    Ok(())
}

#[tokio::test]
async fn usage_unauthenticated_returns_401() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness.get_without_auth("/api/oauth/usage").await?;

    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_token");
    Ok(())
}

#[tokio::test]
async fn usage_clamps_utilization_to_unit_range() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;
    harness
        .put_quota(quota(
            harness.upstreams[0],
            SubscriptionQuotaWindow::SevenDay,
            Some(1.5),
            None,
        ))
        .await?;

    let response = harness.auth_get("/api/oauth/usage").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_f64(response.json["seven_day"]["utilization"].as_f64(), 1.0);
    Ok(())
}

#[tokio::test]
async fn roles_billing_type_max_maps_to_max() -> TestResult<()> {
    assert_role_mapping(Some("Claude Max"), Some("team"), "max").await
}

#[tokio::test]
async fn roles_billing_type_team_maps_to_team() -> TestResult<()> {
    assert_role_mapping(Some("Team Plan"), Some("enterprise"), "team").await
}

#[tokio::test]
async fn roles_organization_type_enterprise_maps_to_enterprise() -> TestResult<()> {
    assert_role_mapping(Some("pro"), Some("enterprise"), "enterprise").await
}

#[tokio::test]
async fn roles_default_falls_back_to_pro() -> TestResult<()> {
    assert_role_mapping(None, Some("workspace"), "pro").await
}

#[tokio::test]
async fn roles_principal_with_no_upstream_returns_200_with_nulls_and_pro_default() -> TestResult<()>
{
    let harness = Harness::new(1, AllowedUpstreams::None).await?;

    let response = harness.auth_get("/api/oauth/claude_cli/roles").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(response.json["organization_uuid"].is_null());
    assert!(response.json["organization_role"].is_null());
    assert_eq!(response.json["subscription_type"], "pro");
    Ok(())
}

#[tokio::test]
async fn roles_unauthenticated_returns_401() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness
        .get_without_auth("/api/oauth/claude_cli/roles")
        .await?;

    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_token");
    Ok(())
}

#[tokio::test]
async fn profile_returns_org_account_fields_when_org_metadata_present() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;
    harness
        .put_upstream_metadata(harness.upstreams[0], "org-profile", Some("admin"))
        .await?;
    harness
        .put_org_metadata(org_metadata(
            "org-profile",
            Some("Profile Org"),
            Some("team"),
            Some("max"),
            Some("account@example.com"),
            Some("Account Name"),
            Some("acct-1"),
        ))
        .await?;

    let response = harness.auth_get("/api/oauth/profile").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.json["account"]["uuid"], "acct-1");
    assert_eq!(response.json["account"]["email"], "account@example.com");
    assert_eq!(response.json["account"]["display_name"], "Account Name");
    assert_eq!(response.json["organization"]["uuid"], "org-profile");
    assert_eq!(response.json["organization"]["name"], "Profile Org");
    Ok(())
}

#[tokio::test]
async fn profile_falls_back_to_principal_id_when_no_org_metadata() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness.auth_get("/api/oauth/profile").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(
        response.json["account"]["uuid"],
        harness.principal.id.to_string()
    );
    assert!(response.json["account"]["email"].is_null());
    assert!(response.json["account"]["display_name"].is_null());
    assert!(response.json["organization"].is_null());
    Ok(())
}

#[tokio::test]
async fn profile_unauthenticated_returns_401() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness.get_without_auth("/api/oauth/profile").await?;

    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_token");
    Ok(())
}

#[tokio::test]
async fn account_settings_returns_empty_object_200() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness.auth_get("/api/oauth/account/settings").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.json, json!({}));
    Ok(())
}

#[tokio::test]
async fn account_settings_unauthenticated_returns_401() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness
        .get_without_auth("/api/oauth/account/settings")
        .await?;

    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_token");
    Ok(())
}

#[tokio::test]
async fn token_refresh_with_valid_token_returns_same_token_stub() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness
        .post_json_without_auth(
            "/v1/oauth/token",
            json!({"grant_type":"refresh_token","refresh_token":harness.api_key}),
        )
        .await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.json["access_token"], harness.api_key);
    assert_eq!(response.json["refresh_token"], harness.api_key);
    assert_eq!(response.json["token_type"], "Bearer");
    assert_eq!(response.json["expires_in"], 31_536_000);
    Ok(())
}

#[tokio::test]
async fn token_unsupported_grant_type_returns_400() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness
        .post_json_without_auth(
            "/v1/oauth/token",
            json!({"grant_type":"authorization_code","refresh_token":harness.api_key}),
        )
        .await?;

    assert_eq!(
        response.status,
        StatusCode::BAD_REQUEST,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "unsupported_grant_type");
    Ok(())
}

#[tokio::test]
async fn token_invalid_format_returns_400() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness
        .post_json_without_auth(
            "/v1/oauth/token",
            json!({"grant_type":"refresh_token","refresh_token":"not-a-cclb-key"}),
        )
        .await?;

    assert_eq!(
        response.status,
        StatusCode::BAD_REQUEST,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_grant");
    assert_eq!(
        response.json["error_description"],
        "invalid refresh_token format"
    );
    Ok(())
}

#[tokio::test]
async fn token_unknown_or_revoked_returns_401() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;
    let revoked = harness.issue_key("revoked").await?;
    let (_, secret_bytes) = cc_lb_core::api_keys::secret::parse(&revoked)?;
    let key_id = cc_lb_core::api_keys::secret::parse(&revoked)?.0;
    harness
        .key_store
        .revoke(&harness.principal.id.to_string(), &key_id)
        .await?;

    let response = harness
        .post_json_without_auth(
            "/v1/oauth/token",
            json!({"grant_type":"refresh_token","refresh_token":revoked}),
        )
        .await?;

    assert_eq!(secret_bytes.len(), 43);
    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_grant");
    assert_eq!(response.json["error_description"], "refresh_token rejected");
    Ok(())
}

#[tokio::test]
async fn token_endpoint_does_not_require_auth_header() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness
        .post_json_without_auth(
            "/v1/oauth/token",
            json!({"grant_type":"refresh_token","refresh_token":harness.api_key}),
        )
        .await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.json["access_token"], harness.api_key);
    Ok(())
}

#[tokio::test]
async fn precedence_oauth_usage_hits_synth_not_wildcard() -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;

    let response = harness.get_without_auth("/api/oauth/usage").await?;

    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.body
    );
    assert_eq!(response.json["error"], "invalid_token");
    assert!(response.json.get("error_description").is_some());
    Ok(())
}

async fn assert_role_mapping(
    billing_type: Option<&str>,
    organization_type: Option<&str>,
    expected: &str,
) -> TestResult<()> {
    let harness = Harness::new(1, AllowedUpstreams::All).await?;
    harness
        .put_upstream_metadata(harness.upstreams[0], "org-role", Some("admin"))
        .await?;
    harness
        .put_org_metadata(org_metadata(
            "org-role",
            Some("Role Org"),
            organization_type,
            billing_type,
            None,
            None,
            None,
        ))
        .await?;

    let response = harness.auth_get("/api/oauth/claude_cli/roles").await?;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.json["organization_uuid"], "org-role");
    assert_eq!(response.json["organization_role"], "admin");
    assert_eq!(response.json["subscription_type"], expected);
    Ok(())
}

enum AllowedUpstreams {
    All,
    None,
}

struct Harness {
    _dir: tempfile::TempDir,
    app: cc_lb_server::App,
    storage: Arc<RedbStorage>,
    key_store: Arc<KeyStore>,
    principal: PrincipalRecord,
    upstreams: Vec<Uuid>,
    api_key: String,
}

impl Harness {
    async fn new(upstream_count: usize, allowed: AllowedUpstreams) -> TestResult<Self> {
        let dir = tempfile::tempdir()?;
        let key = [41; 32];
        let storage_path = dir.path().join(format!("{}.redb", Uuid::new_v4()));
        let storage = Arc::new(RedbStorage::open(&storage_path, key)?);
        let mut upstreams = Vec::new();
        for index in 0..upstream_count {
            let record = UpstreamStore::create(
                storage.as_ref(),
                UpstreamCreate {
                    name: format!("upstream-{index}"),
                    kind: UpstreamKind::Custom,
                    base_url: Some(Url::parse("http://127.0.0.1:9")?),
                    api_key_ciphertext: None,
                    shape_plugin: None,
                },
            )
            .await?;
            upstreams.push(record.id);
        }
        let allowed_upstreams = match allowed {
            AllowedUpstreams::All => upstreams.clone(),
            AllowedUpstreams::None => Vec::new(),
        };
        let principal = PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: format!("principal-{}", Uuid::new_v4()),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams,
                default_limits: Vec::new(),
            },
            now_secs(),
        )
        .await?;
        let managed_store: Arc<dyn ManagedKeyStore> =
            Arc::new(RedbManagedKeyStore::new(storage.clone()));
        let key_store = Arc::new(KeyStore::new(managed_store.clone()));
        let api_key = issue_key(&key_store, &principal.id.to_string(), "primary").await?;
        let storage_trait: Arc<dyn StorageTrait> = storage.clone();
        let mut config = Config {
            storage: cc_lb_config::StorageConfig::Redb { path: storage_path },
            ..Default::default()
        };
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        config.aead.key_env = "__CC_LB_OAUTH_ENDPOINT_TEST_KEY__".to_owned();
        config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
        config.downstream_auth.none_mode = None;
        let app = build_app_with_storage(
            config,
            None,
            managed_store,
            storage_trait,
            Arc::new(AeadService::from_master_key(key)),
        )
        .await?;

        Ok(Self {
            _dir: dir,
            app,
            storage,
            key_store,
            principal,
            upstreams,
            api_key,
        })
    }

    async fn issue_key(&self, label: &str) -> TestResult<String> {
        issue_key(&self.key_store, &self.principal.id.to_string(), label).await
    }

    async fn put_quota(&self, record: SubscriptionQuotaObservationRecord) -> TestResult<()> {
        self.storage.put_subscription_quota(&record).await?;
        Ok(())
    }

    async fn put_upstream_metadata(
        &self,
        upstream_id: Uuid,
        organization_uuid: &str,
        organization_role: Option<&str>,
    ) -> TestResult<()> {
        self.storage
            .put_upstream_subscription_metadata(&UpstreamSubscriptionMetadataRecord {
                upstream_id,
                organization_uuid: Some(organization_uuid.to_owned()),
                organization_role: organization_role.map(ToOwned::to_owned),
                workspace_role: None,
                observed_at_unix_millis: now_millis() as i64,
                last_error: None,
                raw_roles: None,
                raw_bootstrap: None,
            })
            .await?;
        Ok(())
    }

    async fn put_org_metadata(&self, record: OrganizationMetadataRecord) -> TestResult<()> {
        self.storage.put_organization_metadata(&record).await?;
        Ok(())
    }

    async fn auth_get(&self, path: &str) -> TestResult<JsonResponse> {
        self.request(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("x-api-key", &self.api_key)
                .body(Body::empty())?,
        )
        .await
    }

    async fn auth_get_authorization(&self, path: &str) -> TestResult<JsonResponse> {
        self.request(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("Authorization", format!("Bearer {}", self.api_key))
                .body(Body::empty())?,
        )
        .await
    }

    async fn get_without_auth(&self, path: &str) -> TestResult<JsonResponse> {
        self.request(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())?,
        )
        .await
    }

    async fn post_json_without_auth(&self, path: &str, body: Value) -> TestResult<JsonResponse> {
        self.request(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await
    }

    async fn request(&self, request: Request<Body>) -> TestResult<JsonResponse> {
        let response = self.app.router.clone().oneshot(request).await?;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        let body = String::from_utf8(body.to_vec())?;
        let json = serde_json::from_str(&body).unwrap_or_else(|_| json!({"raw": body.clone()}));
        Ok(JsonResponse { status, body, json })
    }
}

struct JsonResponse {
    status: StatusCode,
    body: String,
    json: Value,
}

async fn issue_key(key_store: &KeyStore, principal_id: &str, label: &str) -> TestResult<String> {
    let (_, secret) = key_store
        .create(
            principal_id,
            CreateParams {
                upstream_kind: KeyUpstreamKind::AnthropicKey,
                label: label.to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
                principal_kind: PrincipalKindLite::Machine,
            },
        )
        .await?;
    Ok(secret.expose().to_owned())
}

fn quota(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: now_millis(),
        sample_id: Uuid::new_v4(),
        utilization,
        status: None,
        resets_at_unix_secs,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: now_millis(),
    }
}

fn extra_quota(
    upstream_id: Uuid,
    enabled: bool,
    monthly_limit: Option<f64>,
    used_credits: Option<f64>,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        extra_usage_enabled: Some(enabled),
        extra_usage_monthly_limit: monthly_limit,
        extra_usage_used_credits: used_credits,
        ..quota(upstream_id, SubscriptionQuotaWindow::Overage, None, None)
    }
}

fn org_metadata(
    organization_uuid: &str,
    organization_name: Option<&str>,
    organization_type: Option<&str>,
    billing_type: Option<&str>,
    account_email: Option<&str>,
    account_display_name: Option<&str>,
    account_uuid: Option<&str>,
) -> OrganizationMetadataRecord {
    OrganizationMetadataRecord {
        organization_uuid: organization_uuid.to_owned(),
        organization_name: organization_name.map(ToOwned::to_owned),
        organization_type: organization_type.map(ToOwned::to_owned),
        rate_limit_tier: None,
        has_extra_usage_enabled: None,
        billing_type: billing_type.map(ToOwned::to_owned),
        subscription_created_at_unix_secs: None,
        account_email: account_email.map(ToOwned::to_owned),
        account_display_name: account_display_name.map(ToOwned::to_owned),
        account_uuid: account_uuid.map(ToOwned::to_owned),
        overage_credit_amount_minor_units: None,
        overage_credit_currency: None,
        overage_credit_granted: None,
        overage_credit_eligible: None,
        observed_at_unix_millis: now_millis() as i64,
        last_error: None,
        raw_profile: None,
        raw_overage_grant: None,
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn iso8601(timestamp: u64) -> String {
    chrono::DateTime::from_timestamp(timestamp as i64, 0)
        .expect("valid timestamp")
        .to_rfc3339()
}

fn assert_f64(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("json number");
    assert!(
        (actual - expected).abs() < 0.000_001,
        "expected {expected}, got {actual}"
    );
}
