use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use schemars::JsonSchema;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use url::Url;

mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(*duration).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        humantime::parse_duration(&value).map_err(serde::de::Error::custom)
    }
}

pub const DEFAULT_MESSAGES_CAP_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_FILES_CAP_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_OAUTH_AEAD_KEY_ENV: &str = "CC_LB_MASTER_KEY";
pub const DEFAULT_ADMIN_TOKEN_ENV: &str = "CC_LB_ADMIN_TOKEN";
pub const DEFAULT_SQLITE_PATH: &str = "/var/lib/cc-lb/storage.sqlite";
pub const DEFAULT_SCHEDULER_LEADER_LOCK_KEY: i64 = 0xCC1B_5CDE_0001_i64;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct Config {
    pub listener: ListenerConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
    pub body: BodyConfig,
    pub timeouts: TimeoutsConfig,
    pub downstream_auth: DownstreamAuthConfig,
    pub api_keys: ApiKeysConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    pub aead: AeadConfig,
    pub observability: ObservabilityConfig,
    pub admin: AdminConfig,
    pub oauth: OAuthConfig,
    #[serde(default)]
    pub subscription_quota: SubscriptionQuotaConfig,
    pub runtime: RuntimeConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub bulkhead: BulkheadConfig,
    pub dns: DnsConfig,
    pub egress: EgressConfig,
    #[serde(default)]
    pub prompt_cache_shadow: PromptCacheShadowConfig,
    #[serde(default)]
    pub lifecycle_shadow_writer: LifecycleShadowWriterConfig,
    #[serde(default)]
    pub lifecycle_hook_adapter: LifecycleHookAdapterConfig,
    #[serde(default)]
    pub lifecycle_pricing_subscriber: LifecyclePricingSubscriberConfig,
    #[serde(default)]
    pub request_event_writer_source: RequestEventWriterSource,
    #[serde(default)]
    pub limit_reservation_ttl: LimitReservationTtlConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RestartRequiredField {
    pub field: String,
    pub current: String,
    pub new: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ListenerConfig {
    #[serde(default = "default_proxy_addr")]
    pub proxy_addr: SocketAddr,
    #[serde(default = "default_admin_addr")]
    pub admin_addr: SocketAddr,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: SocketAddr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unix_socket: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            proxy_addr: default_proxy_addr(),
            admin_addr: default_admin_addr(),
            metrics_addr: default_metrics_addr(),
            unix_socket: None,
            tls: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct TlsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_path: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub reload_on_sighup: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            reload_on_sighup: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct BodyConfig {
    #[serde(default = "default_messages_cap_bytes")]
    pub messages_cap_bytes: u64,
    #[serde(default = "default_files_cap_bytes")]
    pub files_cap_bytes: u64,
    pub per_route_overrides: HashMap<String, u64>,
}

impl Default for BodyConfig {
    fn default() -> Self {
        Self {
            messages_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_cap_bytes: DEFAULT_FILES_CAP_BYTES,
            per_route_overrides: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct TimeoutsConfig {
    #[serde(default = "default_request_header_secs")]
    pub request_header_secs: u64,
    #[serde(default = "default_request_body_chunk_secs")]
    pub request_body_chunk_secs: u64,
    #[serde(default = "default_idle_secs")]
    pub idle_secs: u64,
    #[serde(default = "default_upstream_total_secs")]
    pub upstream_total_secs: u64,
    #[serde(default = "default_drain_secs")]
    pub drain_secs: u64,
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            request_header_secs: 10,
            request_body_chunk_secs: 30,
            idle_secs: 300,
            upstream_total_secs: 600,
            drain_secs: 60,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    Requests,
    InputTokens,
    OutputTokens,
    TotalTokens,
    CostUsd,
    Concurrent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Limit {
    pub kind: LimitKind,
    #[serde(
        serialize_with = "crate::types::humantime_serde::serialize",
        deserialize_with = "crate::types::humantime_serde::deserialize"
    )]
    pub window: Duration,
    pub cap_micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DownstreamAuthMode {
    None,
    ApiKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NoneModeUpstreamKind {
    AnthropicKey,
    AnthropicOAuth,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NoneModeConfig {
    pub principal_id: String,
    pub upstream_kind: NoneModeUpstreamKind,
}

impl Default for NoneModeConfig {
    fn default() -> Self {
        Self {
            principal_id: String::new(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DownstreamAuthConfig {
    pub mode: DownstreamAuthMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub none_mode: Option<NoneModeConfig>,
}

impl Default for DownstreamAuthConfig {
    fn default() -> Self {
        Self {
            mode: DownstreamAuthMode::ApiKey,
            none_mode: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PriceCatalogConfig {
    #[serde(default = "default_price_catalog_url")]
    pub url: String,
    #[serde(default = "default_price_catalog_refresh_interval")]
    #[serde(
        serialize_with = "crate::types::humantime_serde::serialize",
        deserialize_with = "crate::types::humantime_serde::deserialize"
    )]
    pub refresh_interval: Duration,
    #[serde(default = "default_price_catalog_cache_path")]
    pub cache_path: PathBuf,
}

impl Default for PriceCatalogConfig {
    fn default() -> Self {
        Self {
            url: default_price_catalog_url(),
            refresh_interval: default_price_catalog_refresh_interval(),
            cache_path: default_price_catalog_cache_path(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ApiKeysConfig {
    #[serde(default = "default_usage_retention_days")]
    pub usage_retention_days: u64,
    pub price_catalog: PriceCatalogConfig,
}

impl Default for ApiKeysConfig {
    fn default() -> Self {
        Self {
            usage_retention_days: default_usage_retention_days(),
            price_catalog: PriceCatalogConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageConfig {
    Postgres {
        url: String,
        #[serde(default)]
        pool: PostgresPoolConfig,
    },
    Sqlite {
        path: PathBuf,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Sqlite {
            path: PathBuf::from(DEFAULT_SQLITE_PATH),
        }
    }
}

impl<'de> Deserialize<'de> for StorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let has_kind = value.get("kind").is_some();

        if has_kind {
            let tagged = TaggedStorageConfig::deserialize(value).map_err(D::Error::custom)?;
            return Ok(tagged.into());
        }

        Ok(Self::default())
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TaggedStorageConfig {
    Postgres {
        url: String,
        #[serde(default)]
        pool: PostgresPoolConfig,
    },
    Sqlite {
        path: PathBuf,
    },
}

impl From<TaggedStorageConfig> for StorageConfig {
    fn from(value: TaggedStorageConfig) -> Self {
        match value {
            TaggedStorageConfig::Postgres { url, pool } => Self::Postgres { url, pool },
            TaggedStorageConfig::Sqlite { path } => Self::Sqlite { path },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AeadConfig {
    #[serde(default = "default_oauth_aead_key_env", alias = "oauth_aead_key_env")]
    pub key_env: String,
}

impl Default for AeadConfig {
    fn default() -> Self {
        Self {
            key_env: DEFAULT_OAUTH_AEAD_KEY_ENV.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PostgresPoolConfig {
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default)]
    pub min_connections: u32,
    #[serde(default = "default_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    #[serde(default = "default_sslmode")]
    pub sslmode: String,
}

impl Default for PostgresPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            min_connections: 0,
            acquire_timeout_secs: default_acquire_timeout_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            statement_timeout_secs: default_statement_timeout_secs(),
            sslmode: default_sslmode(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerConfig {
    #[serde(default)]
    pub separate_pool: SchedulerPoolConfig,
    #[serde(default = "default_scheduler_leader_lock_key")]
    pub leader_lock_key: i64,
    #[serde(default)]
    pub retry_classes: SchedulerRetryClasses,
    #[serde(
        default = "default_scheduler_recurring_jobs",
        serialize_with = "serialize_recurring_jobs"
    )]
    pub recurring_jobs: HashMap<String, RecurringJobConfig>,
    #[serde(default)]
    pub idempotency: SchedulerIdempotencyConfig,
    #[serde(default = "default_scheduler_dlq_retention_days")]
    pub dlq_retention_days: u32,
    #[serde(default = "default_scheduler_entity_concurrency")]
    pub entity_concurrency: usize,
    #[serde(default = "default_scheduler_singleton_concurrency")]
    pub singleton_concurrency: usize,
    #[serde(default)]
    pub staleness: SchedulerStalenessConfig,
    #[serde(default)]
    pub pgbouncer_transaction_mode: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            separate_pool: SchedulerPoolConfig::default(),
            leader_lock_key: DEFAULT_SCHEDULER_LEADER_LOCK_KEY,
            retry_classes: SchedulerRetryClasses::default(),
            recurring_jobs: default_scheduler_recurring_jobs(),
            idempotency: SchedulerIdempotencyConfig::default(),
            dlq_retention_days: 30,
            entity_concurrency: 8,
            singleton_concurrency: 2,
            staleness: SchedulerStalenessConfig::default(),
            pgbouncer_transaction_mode: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerPoolConfig {
    #[serde(default = "default_scheduler_pool_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_scheduler_pool_min_connections")]
    pub min_connections: u32,
    #[serde(default = "default_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    #[serde(default = "default_sslmode")]
    pub sslmode: String,
}

impl Default for SchedulerPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 5,
            min_connections: 1,
            acquire_timeout_secs: default_acquire_timeout_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            statement_timeout_secs: default_statement_timeout_secs(),
            sslmode: default_sslmode(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerRetryClasses {
    #[serde(default = "default_scheduler_retry_probe")]
    pub probe: SchedulerRetryConfig,
    #[serde(default = "default_scheduler_retry_adaptive")]
    pub adaptive: SchedulerRetryConfig,
    #[serde(default = "default_scheduler_retry_maintenance")]
    pub maintenance: SchedulerRetryConfig,
}

impl Default for SchedulerRetryClasses {
    fn default() -> Self {
        Self {
            probe: default_scheduler_retry_probe(),
            adaptive: default_scheduler_retry_adaptive(),
            maintenance: default_scheduler_retry_maintenance(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerRetryConfig {
    #[serde(default = "default_scheduler_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_scheduler_retry_base_secs")]
    pub base_secs: u64,
    #[serde(default = "default_scheduler_retry_max_secs")]
    pub max_secs: u64,
}

impl Default for SchedulerRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_secs: 1,
            max_secs: 5,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RecurringJobConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_recurring_job_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_recurring_job_jitter_secs")]
    pub jitter_secs: u64,
}

impl Default for RecurringJobConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 3600,
            jitter_secs: 30,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerIdempotencyConfig {
    #[serde(default = "default_scheduler_claim_ttl_secs")]
    pub claim_ttl_secs: u64,
}

impl Default for SchedulerIdempotencyConfig {
    fn default() -> Self {
        Self { claim_ttl_secs: 60 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerStalenessConfig {
    #[serde(default = "default_scheduler_warmup_effect_retention_days")]
    pub warmup_effect_retention_days: u32,
}

impl Default for SchedulerStalenessConfig {
    fn default() -> Self {
        Self {
            warmup_effect_retention_days: 30,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AnthropicOAuthConfig {
    pub client_id: String,
    pub auth_url: Url,
    pub token_url: Url,
    pub redirect_uri: Url,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl Default for AnthropicOAuthConfig {
    fn default() -> Self {
        Self {
            client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e".to_owned(),
            auth_url: Url::parse("https://claude.ai/oauth/authorize").expect("valid url"),
            token_url: Url::parse("https://console.anthropic.com/v1/oauth/token")
                .expect("valid url"),
            redirect_uri: Url::parse("https://console.anthropic.com/oauth/code/callback")
                .expect("valid url"),
            scopes: vec![
                "org:create_api_key".to_owned(),
                "user:profile".to_owned(),
                "user:inference".to_owned(),
            ],
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OAuthConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anthropic: Option<AnthropicOAuthConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct SubscriptionQuotaConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_subscription_quota_retention_days")]
    pub retention_days: u64,
    #[serde(default = "default_subscription_quota_gc_batch_size")]
    pub gc_batch_size: u32,
    #[serde(default = "default_subscription_quota_writer_batch_max_records")]
    pub writer_batch_max_records: u32,
    #[serde(default = "default_subscription_quota_writer_flush_ms")]
    pub writer_flush_ms: u64,
    #[serde(default = "default_subscription_quota_writer_channel_capacity")]
    pub writer_channel_capacity: u32,
    #[serde(default = "default_subscription_quota_dedup_elapsed_override_secs")]
    pub dedup_elapsed_override_secs: u64,
    #[serde(default = "default_subscription_quota_routing_max_staleness_secs")]
    pub routing_max_staleness_secs: u64,
}

impl Default for SubscriptionQuotaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 30,
            gc_batch_size: 10_000,
            writer_batch_max_records: 256,
            writer_flush_ms: 100,
            writer_channel_capacity: 4096,
            dedup_elapsed_override_secs: 30,
            routing_max_staleness_secs: 1800,
        }
    }
}

/// Prompt cache shadow mode configuration.
///
/// Controls the prompt cache observation cache behavior. When `enabled=false`, the cache layer
/// is not constructed and observation flow is gated off at the lifecycle level for byte-equivalent
/// pre-T22 behavior (no observation enqueue, no snapshot, no sweeper). When `enabled=true`,
/// the full cache pipeline activates: observations from successful responses are decoded,
/// upserted into the in-memory cache, and enqueued for persistent storage; `build_candidates`
/// snapshots cache state per upstream to compute cache scores.
///
/// Configuration keys and defaults:
/// - `enabled` (default: false) - gate all observation flow and sweeper spawn
/// - `grace_margin_secs` (default: 30) - minimum age before a cache hit is refreshed
/// - `refresh_debounce_secs` (default: 60) - debounce window for refresh-on-hit persistence
/// - `warm_set_cap` (default: 32) - max snapshot entries per upstream/model
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct PromptCacheShadowConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_prompt_cache_shadow_grace_margin_secs")]
    pub grace_margin_secs: u64,
    #[serde(default = "default_prompt_cache_shadow_refresh_debounce_secs")]
    pub refresh_debounce_secs: u64,
    #[serde(default = "default_prompt_cache_shadow_warm_set_cap")]
    pub warm_set_cap: usize,
}

impl Default for PromptCacheShadowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grace_margin_secs: 30,
            refresh_debounce_secs: 60,
            warm_set_cap: 32,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LifecycleShadowWriterConfig {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LifecycleHookAdapterConfig {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LifecyclePricingSubscriberConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// RFC-0002 Phase 7 background TTL sweeper for stale limit reservations.
///
/// When enabled, the LimitEngine periodically walks its reservation map and
/// full-refunds any reservation older than `ttl_secs`. This is opt-in in
/// Phase 7 and enabled unconditionally in Phase 8 once the subscriber path
/// is authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LimitReservationTtlConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_limit_reservation_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_limit_reservation_tick_secs")]
    pub tick_secs: u64,
}

impl Default for LimitReservationTtlConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl_secs: default_limit_reservation_ttl_secs(),
            tick_secs: default_limit_reservation_tick_secs(),
        }
    }
}

fn default_limit_reservation_ttl_secs() -> u64 {
    300
}

fn default_limit_reservation_tick_secs() -> u64 {
    30
}

/// RFC-0002 Phase 6 writer cutover switch.
///
/// - `Legacy` (default): only the pre-RFC-0002 writer persists rows. The
///   Phase-3 shadow-writer feature flag is ignored.
/// - `Both`: both writers persist rows. Rows carry distinct `event_id`s and
///   the shadow row also carries `shadow_event_id` pointing at the legacy
///   row's `event_id`. Diff via the comparison SQL in
///   `docs/runbook/lifecycle-shadow.md`.
/// - `Shadow`: only the assembler writer persists rows. Legacy path is
///   detached from storage. Do not use until the comparison diff is empty
///   for the target soak period.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequestEventWriterSource {
    #[default]
    Legacy,
    Both,
    Shadow,
}

impl RequestEventWriterSource {
    pub fn legacy_writer_enabled(self) -> bool {
        matches!(self, Self::Legacy | Self::Both)
    }

    pub fn shadow_writer_enabled(self) -> bool {
        matches!(self, Self::Shadow | Self::Both)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct RuntimeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
    pub startup_handshake: StartupHandshakeConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct StartupHandshakeConfig {
    #[serde(default = "default_true")]
    pub skip_if_fresh: bool,
    #[serde(default)]
    pub force: bool,
}

impl Default for StartupHandshakeConfig {
    fn default() -> Self {
        Self {
            skip_if_fresh: true,
            force: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ObservabilityConfig {
    #[serde(default = "default_tracing_level")]
    pub tracing_level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prometheus_endpoint: Option<String>,
    #[serde(default = "default_true")]
    pub log_redaction: bool,
    pub user_prompt_redaction: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            tracing_level: default_tracing_level(),
            otlp_endpoint: None,
            prometheus_endpoint: None,
            log_redaction: true,
            user_prompt_redaction: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AdminConfig {
    #[serde(default = "default_admin_token_env")]
    pub token_env: String,
    #[serde(skip)]
    pub token: Option<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            token_env: DEFAULT_ADMIN_TOKEN_ENV.to_owned(),
            token: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_failures_to_open")]
    pub failures_to_open: u32,
    #[serde(default = "default_breaker_window_secs")]
    pub window_secs: u64,
    #[serde(default = "default_half_open_after_secs")]
    pub half_open_after_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failures_to_open: 5,
            window_secs: 10,
            half_open_after_secs: 30,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct BulkheadConfig {
    #[serde(default = "default_max_conns_per_upstream")]
    pub max_conns_per_upstream: u32,
    #[serde(default = "default_semaphore_per_upstream")]
    pub semaphore_per_upstream: u32,
}

impl Default for BulkheadConfig {
    fn default() -> Self {
        Self {
            max_conns_per_upstream: 50,
            semaphore_per_upstream: 100,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct DnsConfig {
    #[serde(default = "default_dns_cache_ttl_floor_secs")]
    pub cache_ttl_floor_secs: u64,
    #[serde(default = "default_dns_cache_ttl_ceiling_secs")]
    pub cache_ttl_ceiling_secs: u64,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            cache_ttl_floor_secs: 30,
            cache_ttl_ceiling_secs: 300,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default)]
pub struct EgressConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_hosts: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ConfigOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listener: Option<ListenerOverrides>,
}

impl ConfigOverrides {
    pub fn from_listener(listener: ListenerOverrides) -> Self {
        if listener.is_empty() {
            Self::default()
        } else {
            Self {
                listener: Some(listener),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ListenerOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_addr: Option<SocketAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_addr: Option<SocketAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_addr: Option<SocketAddr>,
}

impl ListenerOverrides {
    fn is_empty(&self) -> bool {
        self.proxy_addr.is_none() && self.admin_addr.is_none() && self.metrics_addr.is_none()
    }
}

fn default_proxy_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8080)
}

fn default_admin_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9090)
}

fn default_metrics_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9091)
}

fn default_true() -> bool {
    true
}

fn default_messages_cap_bytes() -> u64 {
    DEFAULT_MESSAGES_CAP_BYTES
}

fn default_files_cap_bytes() -> u64 {
    DEFAULT_FILES_CAP_BYTES
}

fn default_request_header_secs() -> u64 {
    TimeoutsConfig::default().request_header_secs
}

fn default_request_body_chunk_secs() -> u64 {
    TimeoutsConfig::default().request_body_chunk_secs
}

fn default_idle_secs() -> u64 {
    TimeoutsConfig::default().idle_secs
}

fn default_upstream_total_secs() -> u64 {
    TimeoutsConfig::default().upstream_total_secs
}

fn default_drain_secs() -> u64 {
    TimeoutsConfig::default().drain_secs
}

fn default_oauth_aead_key_env() -> String {
    DEFAULT_OAUTH_AEAD_KEY_ENV.to_owned()
}

fn default_max_connections() -> u32 {
    10
}

fn default_acquire_timeout_secs() -> u64 {
    5
}

fn default_idle_timeout_secs() -> u64 {
    600
}

fn default_statement_timeout_secs() -> u64 {
    30
}

fn default_sslmode() -> String {
    "prefer".to_owned()
}

fn default_scheduler_pool_max_connections() -> u32 {
    SchedulerPoolConfig::default().max_connections
}

fn default_scheduler_pool_min_connections() -> u32 {
    SchedulerPoolConfig::default().min_connections
}

fn default_scheduler_leader_lock_key() -> i64 {
    DEFAULT_SCHEDULER_LEADER_LOCK_KEY
}

fn default_scheduler_recurring_jobs() -> HashMap<String, RecurringJobConfig> {
    HashMap::from([
        (
            "usage_rollup".to_owned(),
            recurring_job_config(30, scheduler_jitter_secs(30)),
        ),
        (
            "usage_prune".to_owned(),
            recurring_job_config(86_400, scheduler_jitter_secs(86_400)),
        ),
        (
            "quota_gc".to_owned(),
            recurring_job_config(3600, scheduler_jitter_secs(3600)),
        ),
        (
            "prompt_cache_purge".to_owned(),
            recurring_job_config(600, scheduler_jitter_secs(600)),
        ),
        (
            "price_catalog_refresh".to_owned(),
            recurring_job_config(3600, scheduler_jitter_secs(3600)),
        ),
        (
            "apalis_housekeeping".to_owned(),
            recurring_job_config(3600, scheduler_jitter_secs(3600)),
        ),
        (
            "anthropic_compat_refresh".to_owned(),
            recurring_job_config(86_400, scheduler_jitter_secs(86_400)),
        ),
        (
            "warmup_watchdog".to_owned(),
            recurring_job_config(6000, scheduler_jitter_secs(6000)),
        ),
        (
            "oauth_refresh_watchdog".to_owned(),
            recurring_job_config(9360, scheduler_jitter_secs(9360)),
        ),
        ("oauth_usage_poll".to_owned(), recurring_job_config(60, 0)),
        (
            "pool_quota_snapshot".to_owned(),
            recurring_job_config(60, 0),
        ),
    ])
}

fn serialize_recurring_jobs<S>(
    jobs: &HashMap<String, RecurringJobConfig>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let ordered = jobs
        .iter()
        .map(|(name, config)| (name.as_str(), config))
        .collect::<BTreeMap<_, _>>();
    ordered.serialize(serializer)
}

fn scheduler_jitter_secs(interval_secs: u64) -> u64 {
    (interval_secs / 10).min(30)
}

fn recurring_job_config(interval_secs: u64, jitter_secs: u64) -> RecurringJobConfig {
    RecurringJobConfig {
        enabled: true,
        interval_secs,
        jitter_secs,
    }
}

fn default_scheduler_retry_probe() -> SchedulerRetryConfig {
    SchedulerRetryConfig {
        max_attempts: 3,
        base_secs: 1,
        max_secs: 5,
    }
}

fn default_scheduler_retry_adaptive() -> SchedulerRetryConfig {
    SchedulerRetryConfig {
        max_attempts: 5,
        base_secs: 30,
        max_secs: 600,
    }
}

fn default_scheduler_retry_maintenance() -> SchedulerRetryConfig {
    SchedulerRetryConfig {
        max_attempts: 1,
        base_secs: 60,
        max_secs: 60,
    }
}

fn default_scheduler_retry_max_attempts() -> u32 {
    SchedulerRetryConfig::default().max_attempts
}

fn default_scheduler_retry_base_secs() -> u64 {
    SchedulerRetryConfig::default().base_secs
}

fn default_scheduler_retry_max_secs() -> u64 {
    SchedulerRetryConfig::default().max_secs
}

fn default_recurring_job_interval_secs() -> u64 {
    RecurringJobConfig::default().interval_secs
}

fn default_recurring_job_jitter_secs() -> u64 {
    RecurringJobConfig::default().jitter_secs
}

fn default_scheduler_claim_ttl_secs() -> u64 {
    SchedulerIdempotencyConfig::default().claim_ttl_secs
}

fn default_scheduler_dlq_retention_days() -> u32 {
    SchedulerConfig::default().dlq_retention_days
}

fn default_scheduler_entity_concurrency() -> usize {
    SchedulerConfig::default().entity_concurrency
}

fn default_scheduler_singleton_concurrency() -> usize {
    SchedulerConfig::default().singleton_concurrency
}

fn default_scheduler_warmup_effect_retention_days() -> u32 {
    SchedulerStalenessConfig::default().warmup_effect_retention_days
}

fn default_tracing_level() -> String {
    "info".to_owned()
}

fn default_usage_retention_days() -> u64 {
    90
}

fn default_price_catalog_url() -> String {
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
        .to_owned()
}

fn default_price_catalog_refresh_interval() -> Duration {
    Duration::from_secs(60 * 60)
}

fn default_price_catalog_cache_path() -> PathBuf {
    PathBuf::from("/var/lib/cc-lb/litellm.json")
}

fn default_admin_token_env() -> String {
    DEFAULT_ADMIN_TOKEN_ENV.to_owned()
}

fn default_failures_to_open() -> u32 {
    CircuitBreakerConfig::default().failures_to_open
}

fn default_breaker_window_secs() -> u64 {
    CircuitBreakerConfig::default().window_secs
}

fn default_half_open_after_secs() -> u64 {
    CircuitBreakerConfig::default().half_open_after_secs
}

fn default_max_conns_per_upstream() -> u32 {
    BulkheadConfig::default().max_conns_per_upstream
}

fn default_semaphore_per_upstream() -> u32 {
    BulkheadConfig::default().semaphore_per_upstream
}

fn default_dns_cache_ttl_floor_secs() -> u64 {
    DnsConfig::default().cache_ttl_floor_secs
}

fn default_dns_cache_ttl_ceiling_secs() -> u64 {
    DnsConfig::default().cache_ttl_ceiling_secs
}

fn default_subscription_quota_retention_days() -> u64 {
    SubscriptionQuotaConfig::default().retention_days
}

fn default_subscription_quota_gc_batch_size() -> u32 {
    SubscriptionQuotaConfig::default().gc_batch_size
}

fn default_subscription_quota_writer_batch_max_records() -> u32 {
    SubscriptionQuotaConfig::default().writer_batch_max_records
}

fn default_subscription_quota_writer_flush_ms() -> u64 {
    SubscriptionQuotaConfig::default().writer_flush_ms
}

fn default_subscription_quota_writer_channel_capacity() -> u32 {
    SubscriptionQuotaConfig::default().writer_channel_capacity
}

fn default_subscription_quota_dedup_elapsed_override_secs() -> u64 {
    SubscriptionQuotaConfig::default().dedup_elapsed_override_secs
}

fn default_subscription_quota_routing_max_staleness_secs() -> u64 {
    SubscriptionQuotaConfig::default().routing_max_staleness_secs
}

fn default_prompt_cache_shadow_grace_margin_secs() -> u64 {
    PromptCacheShadowConfig::default().grace_margin_secs
}

fn default_prompt_cache_shadow_refresh_debounce_secs() -> u64 {
    PromptCacheShadowConfig::default().refresh_debounce_secs
}

fn default_prompt_cache_shadow_warm_set_cap() -> usize {
    PromptCacheShadowConfig::default().warm_set_cap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, ConfigOverrides};
    use std::fs;

    fn load_config(toml: &str) -> Result<Config, crate::ConfigError> {
        let temp_file = tempfile::NamedTempFile::new().expect("create temp config");
        fs::write(temp_file.path(), toml).expect("write temp config");
        Config::load_with_overrides(temp_file.path(), ConfigOverrides::default())
    }

    #[test]
    fn valid_api_key_mode_deserializes() {
        let config = load_config(
            r#"
[downstream_auth]
mode = "api_key"

[api_keys]
"#,
        )
        .expect("config should load");

        assert_eq!(config.downstream_auth.mode, DownstreamAuthMode::ApiKey);
        assert!(config.downstream_auth.none_mode.is_none());
    }

    #[test]
    fn valid_none_mode_deserializes() {
        let config = load_config(
            r#"
[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "anon"
upstream_kind = "anthropic_key"

[api_keys]
"#,
        )
        .expect("config should load");

        assert_eq!(config.downstream_auth.mode, DownstreamAuthMode::None);
        let none_mode = config.downstream_auth.none_mode.expect("none mode config");
        assert_eq!(none_mode.principal_id, "anon");
        assert_eq!(none_mode.upstream_kind, NoneModeUpstreamKind::AnthropicKey);
    }

    #[test]
    fn none_mode_missing_fails_validation() {
        let error = load_config(
            r#"
[downstream_auth]
mode = "none"

[api_keys]
"#,
        )
        .expect_err("config should fail validation");

        assert!(
            error
                .to_string()
                .contains("downstream_auth.none_mode must be set iff mode=none")
        );
    }

    #[test]
    fn api_key_mode_rejects_none_mode() {
        let error = load_config(
            r#"
[downstream_auth]
mode = "api_key"

[downstream_auth.none_mode]
principal_id = "anon"
upstream_kind = "anthropic_key"

[api_keys]
"#,
        )
        .expect_err("config should fail validation");

        assert!(
            error
                .to_string()
                .contains("downstream_auth.none_mode must be set iff mode=none")
        );
    }

    fn legacy_removed_message() -> String {
        let legacy_plugin_section = ["authn", "_", "plugin"].concat();
        [
            "v2 removed `plugins.",
            &legacy_plugin_section,
            "` / `principals.*.quotas`; use `downstream_auth.mode` + `principals.*.default_limits` (sk-cclb-* API keys)",
        ]
        .concat()
    }

    #[test]
    fn legacy_plugin_rejected() {
        let legacy_plugin_section = ["authn", "_", "plugin"].concat();
        let config_toml = format!(
            "[plugins.{}]\nname = \"legacy\"\n\n[api_keys]\n",
            legacy_plugin_section
        );
        let error = load_config(&config_toml).expect_err("legacy plugin should fail");

        assert!(error.to_string().contains(&legacy_removed_message()));
    }

    #[test]
    fn legacy_principal_quotas_rejected() {
        let legacy_header = ["[", "principals", ".u1.quotas]"].concat();
        let config_toml = format!("\n{legacy_header}\ndefault_window_secs = 60\n\n[api_keys]\n");
        let error = load_config(&config_toml).expect_err("legacy quotas should fail");

        assert!(error.to_string().contains(&legacy_removed_message()));
    }
}
