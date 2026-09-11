// tier-allow(silent-skip): storage trait optional-return signatures never skip assertions until=2027-03-31
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::{HeaderMap, Method};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackfillApplyOutcome, MetadataTierMappingOverrideRecord, OrganizationMetadataRecord,
    OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore, StorageResult, UpstreamCreate,
    UpstreamPlanTierRecord, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    UpstreamStore, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use cc_lb_testkit::{InMemoryStorage as Storage, fixed_clock};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, SignerError,
    SignerFactory, UpstreamDialect, shape_request, sign_request,
};
use url::Url;
use uuid::Uuid;

const NOW_UNIX_SECS: u64 = 1_800_000_000;

struct Fixture {
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    clock: cc_lb_engine::ClockHandle,
}

impl Fixture {
    async fn new() -> Self {
        let clock = fixed_clock(NOW_UNIX_SECS);
        let storage = Arc::new(Storage::with_clock(clock.clone()));
        let empty = Arc::new(EmptyDynamicStore);
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: empty.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: empty.clone(),
            organization_metadata: empty.clone(),
            plan_tiers: empty,
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: None,
        });
        let aead = Arc::new(AeadService::from_master_key([32; 32]));
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse("http://oauth.invalid/authorize").expect("auth url"),
            token_url: Url::parse("http://oauth.invalid/token").expect("token url"),
            redirect_uri: Url::parse("http://localhost/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        Self {
            storage,
            stores,
            aead,
            oauth_cfg,
            clock,
        }
    }

    async fn build_view(&self) -> Arc<cc_lb_engine::DynamicView> {
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
        build_dynamic_view(
            self.stores.as_ref(),
            self.oauth_cfg.as_ref(),
            self.aead.clone(),
            None,
            0,
            &runtime,
            Path::new("."),
            Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
            None,
            None,
            1800,
            self.clock.clone(),
        )
        .await
        .expect("dynamic view builds")
    }

    async fn create_oauth_upstream(&self, name: &str) -> Uuid {
        self.create_oauth_upstream_with_access_token(
            name,
            "sk-ant-oat01-test-access-token-123456789",
        )
        .await
    }

    async fn create_oauth_upstream_with_access_token(
        &self,
        name: &str,
        access_token: &str,
    ) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        let encrypted = encrypted(
            &self.aead,
            record.id,
            &OAuthTokenBundle {
                access_token: access_token.to_owned(),
                refresh_token: format!("sk-ant-ort01-{name}-refresh-token-123456789"),
                expires_at_unix_secs: NOW_UNIX_SECS + 3600,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["messages".to_owned()],
            },
        );
        self.storage
            .store_oauth_tokens(record.id, record.revision, encrypted)
            .await
            .expect("tokens stored");
        record.id
    }

    async fn create_apikey_upstream(&self, name: &str) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(b"test-key-ciphertext".to_vec()),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        record.id
    }

    async fn create_oauth_upstream_no_credentials(&self, name: &str) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        record.id
    }
}

#[tokio::test]
async fn t2__oauth_upstream_routes_to_oauth_signer() {
    let fixture = Fixture::new().await;
    let _upstream_id = fixture.create_oauth_upstream("oauth-test").await;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "oauth-test",
        fixture.clock.clone(),
    );

    let signer = factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await
        .expect("signer built for oauth upstream");

    let shaped = shaped_request();
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("signed request");

    let auth_header = signed
        .headers()
        .get("authorization")
        .expect("authorization header present");
    assert_eq!(
        auth_header.to_str().expect("header to str"),
        "Bearer sk-ant-oat01-test-access-token-123456789"
    );
}

#[tokio::test]
async fn apikey_upstream_routes_to_key_signer() {
    let fixture = Fixture::new().await;
    let _upstream_id = fixture.create_apikey_upstream("apikey-test").await;

    let factory = cc_lb_signer_anthropic_key::AnthropicKeySignerFactory::new(
        "sk-ant-test-api-key-12345".to_owned(),
    );

    let signer = factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await
        .expect("signer built for apikey upstream");

    let shaped = shaped_request();
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("signed request");

    let auth_header = signed
        .headers()
        .get("x-api-key")
        .expect("x-api-key header present");
    assert_eq!(
        auth_header.to_str().expect("header to str"),
        "sk-ant-test-api-key-12345"
    );
}

#[tokio::test]
async fn t2__router_choice_selects_matching_oauth_upstream() {
    let fixture = Fixture::new().await;
    fixture
        .create_oauth_upstream_with_access_token("oauth-alice", "sk-ant-oat01-alice-token")
        .await;
    fixture
        .create_oauth_upstream_with_access_token("oauth-bob", "sk-ant-oat01-bob-token")
        .await;
    let view = fixture.build_view().await;

    let signer_factory = view
        .signer_factory
        .with_router_choice("sk-ant-downstream".to_owned(), "oauth-bob".to_owned());
    let signer = signer_factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await
        .expect("signer built for router choice");
    let signed = sign_request(signer.as_ref(), shaped_request())
        .await
        .expect("signed request");

    let auth_header = signed
        .headers()
        .get("authorization")
        .expect("authorization header present");
    assert_eq!(
        auth_header.to_str().expect("header to str"),
        "Bearer sk-ant-oat01-bob-token"
    );
}

#[tokio::test]
async fn t2__empty_router_choice_errors() {
    let fixture = Fixture::new().await;
    fixture.create_oauth_upstream("oauth-only").await;
    let view = fixture.build_view().await;

    let signer_factory = view
        .signer_factory
        .with_router_choice("sk-ant-downstream".to_owned(), String::new());
    let result = signer_factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await;

    match result {
        Err(SignerError::MissingCredentials { reason }) => {
            assert!(
                reason.contains("upstream not present"),
                "error message should indicate no matching router choice: {}",
                reason
            );
        }
        Ok(_) => panic!("expected MissingCredentials error, got Ok"),
        Err(other) => panic!("expected MissingCredentials error, got: {:?}", other),
    }
}

#[tokio::test]
async fn t2__missing_oauth_credentials_returns_proper_signer_error() {
    let fixture = Fixture::new().await;
    let _upstream_id = fixture
        .create_oauth_upstream_no_credentials("oauth-missing")
        .await;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "oauth-missing",
        fixture.clock.clone(),
    );

    let result = factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await;

    match result {
        Err(SignerError::MissingCredentials { reason }) => {
            assert!(
                reason.contains("oauth")
                    || reason.contains("credentials")
                    || reason.contains("token"),
                "error message should indicate missing oauth credentials: {}",
                reason
            );
        }
        Ok(_) => panic!("expected MissingCredentials error, got Ok"),
        Err(other) => panic!("expected MissingCredentials error, got: {:?}", other),
    }
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

fn shaped_request() -> ShapedRequest {
    let ctx = DialectShapeContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    let principal = Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::OAuthSubject,
        claims: serde_json::Map::new(),
    };
    shape_request(
        &DirectDialect,
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .expect("shape")
}

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            Url::parse("https://api.anthropic.com/v1/messages").expect("url"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        ))
    }
}

pub(crate) struct EmptyDynamicStore;

#[async_trait]
impl UpstreamRateLimitStateStore for EmptyDynamicStore {
    async fn put_observation(
        &self,
        _record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        _upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl PlanTierStore for EmptyDynamicStore {
    async fn upsert_plan_tier_ratio(&self, _record: &PlanTierRatioRecord) -> StorageResult<()> {
        Ok(())
    }

    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        Ok(Vec::new())
    }

    async fn list_plan_tier_ratios_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<PlanTierRatioRecord>> {
        Ok(Vec::new())
    }

    async fn upsert_metadata_tier_override(
        &self,
        _record: &MetadataTierMappingOverrideRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        Ok(Vec::new())
    }

    async fn list_metadata_tier_overrides_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        Ok(Vec::new())
    }

    async fn append_upstream_plan_tier(
        &self,
        _record: &UpstreamPlanTierRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn backfill_upstream_plan_tier_intervals(
        &self,
        _upstream_id: Uuid,
        _intervals: &[UpstreamPlanTierRecord],
        _terminal_cap_unix_millis: i64,
        _provenance: &str,
    ) -> StorageResult<BackfillApplyOutcome> {
        Ok(BackfillApplyOutcome::Skipped)
    }

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        Ok(Vec::new())
    }

    async fn list_upstream_plan_tiers_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl UpstreamSubscriptionMetadataStore for EmptyDynamicStore {
    async fn put_upstream_subscription_metadata(
        &self,
        _record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn get_upstream_subscription_metadata(
        &self,
        _upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        Ok(None)
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl OrganizationMetadataStore for EmptyDynamicStore {
    async fn put_organization_metadata(
        &self,
        _record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn get_organization_metadata(
        &self,
        _organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        Ok(None)
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        Ok(Vec::new())
    }
}
