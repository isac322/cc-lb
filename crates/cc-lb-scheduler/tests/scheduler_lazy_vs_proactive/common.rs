use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_clock::{Clock, SystemClock, unix_secs};
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, AuditStore, OrganizationMetadataStore, PluginRegistryStore,
    PrincipalStore, PromptCacheObservationStore, UpstreamCreate, UpstreamRateLimitStateStore,
    UpstreamStore, UpstreamSubscriptionMetadataStore, UpstreamSubscriptionQuotaStore,
};
use uuid::Uuid;

use crate::fake::InitialTokens;

pub const LAZY_REQUEST_DELAY: Duration = Duration::from_millis(10);
pub const LOSER_SETTLE_DELAY: Duration = Duration::from_millis(50);
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(8);
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);
pub const NEAR_EXPIRY_OFFSET_SECS: u64 = 5;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn now_secs() -> u64 {
    unix_secs(SystemClock.now())
}

pub fn near_expiry_secs() -> u64 {
    now_secs().saturating_add(NEAR_EXPIRY_OFFSET_SECS)
}

pub fn stores_from_storage<Storage>(storage: Arc<Storage>) -> Arc<Stores>
where
    Storage: AnthropicCompatibilityKvStore
        + AuditStore
        + OrganizationMetadataStore
        + PluginRegistryStore
        + PrincipalStore
        + PromptCacheObservationStore
        + UpstreamRateLimitStateStore
        + UpstreamStore
        + UpstreamSubscriptionMetadataStore
        + UpstreamSubscriptionQuotaStore
        + 'static,
{
    let upstreams: Arc<dyn UpstreamStore> = storage.clone();
    let principals: Arc<dyn PrincipalStore> = storage.clone();
    let plugin_registry: Arc<dyn PluginRegistryStore> = storage.clone();
    let upstream_rate_limits: Arc<dyn UpstreamRateLimitStateStore> = storage.clone();
    let upstream_subscription_quotas: Arc<dyn UpstreamSubscriptionQuotaStore> = storage.clone();
    let upstream_subscription_metadata: Arc<dyn UpstreamSubscriptionMetadataStore> =
        storage.clone();
    let organization_metadata: Arc<dyn OrganizationMetadataStore> = storage.clone();
    let prompt_cache_observations: Arc<dyn PromptCacheObservationStore> = storage.clone();
    let anthropic_compatibility_kv: Arc<dyn AnthropicCompatibilityKvStore> = storage.clone();
    let audit: Arc<dyn AuditStore> = storage;

    Arc::new(Stores {
        upstreams,
        principals,
        plugin_registry,
        upstream_rate_limits,
        upstream_subscription_quotas,
        upstream_subscription_metadata,
        organization_metadata,
        prompt_cache_observations,
        anthropic_compatibility_kv,
        audit: Some(audit),
    })
}

pub async fn create_oauth_upstream<Storage>(
    storage: &Storage,
    aead: &AeadService,
    tokens: InitialTokens,
) -> TestResult<Uuid>
where
    Storage: UpstreamStore + ?Sized,
{
    let record = storage
        .create(UpstreamCreate {
            name: "lazy-proactive-race".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await?;
    let encrypted = EncryptedOAuthTokens::encrypt(
        aead,
        &OAuthTokenBundle {
            access_token: tokens.access_token,
            refresh_token: tokens.refresh_token,
            expires_at_unix_secs: near_expiry_secs(),
            scopes: vec!["messages".to_owned()],
        },
        record.id.as_bytes(),
    )?;
    storage
        .store_oauth_tokens(record.id, record.revision, encrypted)
        .await?;
    Ok(record.id)
}

pub async fn read_upstream_generation<Storage>(
    storage: &Storage,
    upstream_id: Uuid,
) -> TestResult<u64>
where
    Storage: UpstreamStore + ?Sized,
{
    let Some(record) = storage.get_by_id(upstream_id).await? else {
        return Err(format!("upstream {upstream_id} not found").into());
    };
    Ok(record.oauth_token_generation)
}

pub fn metadata_key_prefix(upstream_id: Uuid) -> String {
    format!("adaptive:metadata_refresh:{upstream_id}:%")
}
