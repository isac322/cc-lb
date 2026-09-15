use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

pub const DEFAULT_MESSAGES_CAP_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_FILES_CAP_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_OAUTH_AEAD_KEY_ENV: &str = "CC_LB_MASTER_KEY";
pub const DEFAULT_ADMIN_TOKEN_ENV: &str = "CC_LB_ADMIN_TOKEN";
pub const ADMIN_AUTH_PROVIDERS_JSON_ENV: &str = "CC_LB_ADMIN_AUTH_PROVIDERS_JSON";
pub const DEFAULT_SQLITE_PATH: &str = "/var/lib/cc-lb/storage.sqlite";
pub const DEFAULT_EVENT_BUS_BROADCAST_CAPACITY: usize = 4096;
pub const DEFAULT_CLUSTER_TOKEN_ENV: &str = "CC_LB_CLUSTER_TOKEN";
pub const DEFAULT_STORAGE_TAIL_POLL_INTERVAL_MS: u64 = 250;
pub const DEFAULT_PG_NOTIFY_CHANNEL: &str = "cc_lb_events_partial";
pub const DEFAULT_PARTIAL_RETENTION_TTL_SECS: u64 = 300;
pub const DEFAULT_PARTIAL_RETENTION_MAX_ENTRIES: usize = 10_000;
pub const DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS: u32 = 90;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listener: ListenerConfig,
    pub body: BodyConfig,
    pub timeouts: TimeoutsConfig,
    #[serde(default = "default_request_event_retention_days")]
    pub request_event_retention_days: u64,
    pub price_catalog: PriceCatalogConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub upstream_affinity: UpstreamAffinityConfig,
    pub aead: AeadConfig,
    pub observability: ObservabilityConfig,
    pub admin: AdminConfig,
    #[serde(default)]
    pub event_bus: EventBusConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
    pub oauth: OAuthConfig,
    #[serde(default)]
    pub subscription_quota: SubscriptionQuotaConfig,
    pub runtime: RuntimeConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub bulkhead: BulkheadConfig,
    #[serde(default)]
    pub prompt_cache_shadow: PromptCacheShadowConfig,
    #[serde(default)]
    pub limit_reservation_ttl: LimitReservationTtlConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listener: ListenerConfig::default(),
            body: BodyConfig::default(),
            timeouts: TimeoutsConfig::default(),
            request_event_retention_days: default_request_event_retention_days(),
            price_catalog: PriceCatalogConfig::default(),
            storage: StorageConfig::default(),
            scheduler: SchedulerConfig::default(),
            upstream_affinity: UpstreamAffinityConfig::default(),
            aead: AeadConfig::default(),
            observability: ObservabilityConfig::default(),
            admin: AdminConfig::default(),
            event_bus: EventBusConfig::default(),
            cluster: ClusterConfig::default(),
            oauth: OAuthConfig::default(),
            subscription_quota: SubscriptionQuotaConfig::default(),
            runtime: RuntimeConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            bulkhead: BulkheadConfig::default(),
            prompt_cache_shadow: PromptCacheShadowConfig::default(),
            limit_reservation_ttl: LimitReservationTtlConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamAffinityConfig {
    #[serde(default = "default_upstream_affinity_ttl_days")]
    #[schemars(range(min = 1))]
    pub ttl_days: u32,
}

impl UpstreamAffinityConfig {
    pub fn ttl_secs(&self) -> u64 {
        u64::from(self.ttl_days) * 86_400
    }
}

impl Default for UpstreamAffinityConfig {
    fn default() -> Self {
        Self {
            ttl_days: DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RestartRequiredField {
    pub field: String,
    pub current: String,
    pub new: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ListenerConfig {
    #[serde(default = "default_proxy_addr")]
    pub proxy_addr: SocketAddr,
    #[serde(default = "default_admin_addr")]
    pub admin_addr: SocketAddr,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: SocketAddr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            proxy_addr: default_proxy_addr(),
            admin_addr: default_admin_addr(),
            metrics_addr: default_metrics_addr(),
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
#[serde(default, deny_unknown_fields)]
pub struct BodyConfig {
    #[serde(default = "default_messages_cap_bytes")]
    pub messages_cap_bytes: u64,
    #[serde(default = "default_files_cap_bytes")]
    pub files_cap_bytes: u64,
}

impl Default for BodyConfig {
    fn default() -> Self {
        Self {
            messages_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_cap_bytes: DEFAULT_FILES_CAP_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TimeoutsConfig {
    #[serde(default = "default_upstream_total_secs")]
    pub upstream_total_secs: u64,
    #[serde(default = "default_drain_secs")]
    pub drain_secs: u64,
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            upstream_total_secs: 600,
            drain_secs: 60,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PriceCatalogConfig {
    #[serde(default = "default_price_catalog_url")]
    pub url: String,
    #[serde(default = "default_price_catalog_cache_path")]
    pub cache_path: PathBuf,
}

impl Default for PriceCatalogConfig {
    fn default() -> Self {
        Self {
            url: default_price_catalog_url(),
            cache_path: default_price_catalog_cache_path(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EventBusConfig {
    #[serde(default = "default_event_bus_broadcast_capacity")]
    pub broadcast_capacity: usize,
    #[serde(default = "default_storage_tail_poll_interval_ms")]
    pub storage_tail_poll_interval_ms: u64,
    #[serde(default = "default_pg_notify_channel")]
    pub pg_notify_channel: String,
    #[serde(default = "default_partial_retention_ttl_secs")]
    pub partial_retention_ttl_secs: u64,
    #[serde(default = "default_partial_retention_max_entries")]
    pub partial_retention_max_entries: usize,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            broadcast_capacity: DEFAULT_EVENT_BUS_BROADCAST_CAPACITY,
            storage_tail_poll_interval_ms: DEFAULT_STORAGE_TAIL_POLL_INTERVAL_MS,
            pg_notify_channel: DEFAULT_PG_NOTIFY_CHANNEL.to_owned(),
            partial_retention_ttl_secs: DEFAULT_PARTIAL_RETENTION_TTL_SECS,
            partial_retention_max_entries: DEFAULT_PARTIAL_RETENTION_MAX_ENTRIES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_url: Option<String>,
    #[serde(default = "default_cluster_token_env")]
    pub token_env: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            instance_url: None,
            token_env: DEFAULT_CLUSTER_TOKEN_ENV.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AeadConfig {
    #[serde(default = "default_aead_key_env")]
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
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,
    #[serde(default = "default_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_max_lifetime_secs")]
    pub max_lifetime_secs: u64,
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    #[serde(default = "default_test_before_acquire")]
    pub test_before_acquire: bool,
    #[serde(default = "default_sslmode")]
    pub sslmode: String,
}

impl Default for PostgresPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            acquire_timeout_secs: default_acquire_timeout_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            max_lifetime_secs: default_max_lifetime_secs(),
            statement_timeout_secs: default_statement_timeout_secs(),
            test_before_acquire: default_test_before_acquire(),
            sslmode: default_sslmode(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerConfig {
    #[serde(default)]
    pub separate_pool: SchedulerPoolConfig,
    #[serde(
        default = "default_scheduler_recurring_jobs",
        deserialize_with = "deserialize_recurring_jobs",
        serialize_with = "serialize_recurring_jobs"
    )]
    pub recurring_jobs: HashMap<String, RecurringJobConfig>,
    #[serde(default = "default_scheduler_dlq_retention_days")]
    pub dlq_retention_days: u32,
    #[serde(default = "default_scheduler_entity_concurrency")]
    pub entity_concurrency: usize,
    #[serde(default = "default_scheduler_singleton_concurrency")]
    pub singleton_concurrency: usize,
    #[serde(default = "default_scheduler_keepalive_concurrency")]
    pub keepalive_concurrency: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            separate_pool: SchedulerPoolConfig::default(),
            recurring_jobs: default_scheduler_recurring_jobs(),
            dlq_retention_days: 30,
            entity_concurrency: 8,
            singleton_concurrency: 2,
            keepalive_concurrency: 4,
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

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RecurringJobConfigOverride {
    enabled: Option<bool>,
    interval_secs: Option<u64>,
    jitter_secs: Option<u64>,
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
    #[serde(default = "default_subscription_quota_writer_batch_max_records")]
    pub writer_batch_max_records: u32,
    #[serde(default = "default_subscription_quota_writer_flush_ms")]
    pub writer_flush_ms: u64,
    #[serde(default = "default_subscription_quota_writer_channel_capacity")]
    pub writer_channel_capacity: u32,
    #[serde(default = "default_subscription_quota_routing_max_staleness_secs")]
    pub routing_max_staleness_secs: u64,
}

impl Default for SubscriptionQuotaConfig {
    fn default() -> Self {
        Self {
            writer_batch_max_records: 256,
            writer_flush_ms: 100,
            writer_channel_capacity: 4096,
            routing_max_staleness_secs: 1800,
        }
    }
}

/// Always-on prompt cache routing configuration.
///
/// Controls the prompt cache observation cache behavior. Observations from successful responses
/// are decoded, upserted into the in-memory cache, and enqueued for persistent storage;
/// `build_candidates` reads cache state per `(upstream, canonical model)` partition to compute
/// cache scores.
///
/// Configuration keys and defaults:
/// - `grace_margin_secs` (default: 30) - minimum age before a cache hit is refreshed
/// - `refresh_debounce_secs` (default: 60) - debounce window for refresh-on-hit persistence
/// - `max_live_entries_per_partition` (default: 50000) - cache-negative admission ceiling for one
///   `(upstream, canonical model)` partition
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PromptCacheShadowConfig {
    #[serde(default = "default_prompt_cache_shadow_grace_margin_secs")]
    pub grace_margin_secs: u64,
    #[serde(default = "default_prompt_cache_shadow_refresh_debounce_secs")]
    pub refresh_debounce_secs: u64,
    #[serde(default = "default_prompt_cache_shadow_max_live_entries_per_partition")]
    pub max_live_entries_per_partition: usize,
}

impl Default for PromptCacheShadowConfig {
    fn default() -> Self {
        Self {
            grace_margin_secs: 30,
            refresh_debounce_secs: 60,
            max_live_entries_per_partition: 50_000,
        }
    }
}

/// Background TTL sweeper for stale limit reservations. The LimitEngine
/// periodically walks its reservation map and full-refunds any reservation
/// older than `ttl_secs`; the sweeper runs unconditionally because the
/// subscriber path is authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LimitReservationTtlConfig {
    #[serde(default = "default_limit_reservation_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_limit_reservation_tick_secs")]
    pub tick_secs: u64,
}

impl Default for LimitReservationTtlConfig {
    fn default() -> Self {
        Self {
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct RuntimeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub wasmtime: WasmtimeConfig,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WasmtimeConfig {
    #[serde(default)]
    pub allocation_strategy: WasmtimeAllocationStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_max_pages: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_reservation_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_guard_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_total_memories: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_total_core_instances: Option<u32>,
    #[serde(default)]
    pub shape_origin_policy: ShapeOriginPolicy,
    #[serde(default)]
    pub wire_bounds: PluginWireBounds,
    #[serde(default)]
    pub cookie_redaction: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum WasmtimeAllocationStrategy {
    #[default]
    #[serde(rename = "ondemand")]
    OnDemand,
    #[serde(rename = "pooling")]
    Pooling,
}

/// Whether a shape plugin may return a URL whose origin
/// (scheme+host+port) differs from the selected upstream. Historical
/// behaviour is `Unrestricted`; deployments where a compromised or
/// buggy shape plugin misrouting to a wrong host is a concern can
/// opt into `SelectedUpstreamOrigin` to reject any URL that does not
/// match the selected upstream's `base_url` origin.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShapeOriginPolicy {
    #[default]
    Unrestricted,
    SelectedUpstreamOrigin,
}

/// Upper bounds enforced on plugin wire I/O. Defaults match current
/// unbounded-ish behaviour by tracking the request body cap already
/// enforced at the HTTP layer, so enabling this struct with defaults
/// is a no-op. Tightening any field is opt-in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct PluginWireBounds {
    pub output_body_bytes: u64,
    pub max_headers: u32,
    pub max_header_value_bytes: u32,
    pub reason_bytes: u32,
}

impl Default for PluginWireBounds {
    fn default() -> Self {
        Self {
            output_body_bytes: DEFAULT_FILES_CAP_BYTES,
            max_headers: 100,
            max_header_value_bytes: 8 * 1024,
            reason_bytes: 256,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default = "default_tracing_level")]
    pub tracing_level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    #[serde(default = "default_true")]
    pub log_redaction: bool,
    pub user_prompt_redaction: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            tracing_level: default_tracing_level(),
            otlp_endpoint: None,
            log_redaction: true,
            user_prompt_redaction: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    #[serde(default)]
    pub auth: AdminAuthConfig,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AdminAuthConfig {
    pub providers: Vec<AdminAuthProviderConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdminAuthProviderConfig {
    StaticToken {
        id: String,
        #[serde(default = "default_token_env")]
        token_env: String,
    },
    CloudflareAccess {
        id: String,
        /// Access team domain, used as the issuer and JWKS base URL.
        team_domain: String,
        /// Access application audience tags accepted for the admin surface.
        audiences: Vec<String>,
        #[serde(default = "default_cf_access_header")]
        header: String,
    },
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

fn default_event_bus_broadcast_capacity() -> usize {
    DEFAULT_EVENT_BUS_BROADCAST_CAPACITY
}

fn default_storage_tail_poll_interval_ms() -> u64 {
    DEFAULT_STORAGE_TAIL_POLL_INTERVAL_MS
}

fn default_pg_notify_channel() -> String {
    DEFAULT_PG_NOTIFY_CHANNEL.to_owned()
}

fn default_partial_retention_ttl_secs() -> u64 {
    DEFAULT_PARTIAL_RETENTION_TTL_SECS
}

fn default_partial_retention_max_entries() -> usize {
    DEFAULT_PARTIAL_RETENTION_MAX_ENTRIES
}

fn default_cluster_token_env() -> String {
    DEFAULT_CLUSTER_TOKEN_ENV.to_owned()
}

fn default_upstream_total_secs() -> u64 {
    TimeoutsConfig::default().upstream_total_secs
}

fn default_drain_secs() -> u64 {
    TimeoutsConfig::default().drain_secs
}

fn default_aead_key_env() -> String {
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

fn default_min_connections() -> u32 {
    1
}

fn default_max_lifetime_secs() -> u64 {
    1800
}

fn default_test_before_acquire() -> bool {
    true
}

fn default_scheduler_pool_max_connections() -> u32 {
    SchedulerPoolConfig::default().max_connections
}

fn default_scheduler_pool_min_connections() -> u32 {
    SchedulerPoolConfig::default().min_connections
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
            "prompt_cache_purge".to_owned(),
            recurring_job_config(600, scheduler_jitter_secs(600)),
        ),
        (
            "upstream_affinity_purge".to_owned(),
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

fn deserialize_recurring_jobs<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, RecurringJobConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let overrides = HashMap::<String, RecurringJobConfigOverride>::deserialize(deserializer)?;
    let mut jobs = default_scheduler_recurring_jobs();
    for (name, value) in overrides {
        let Some(config) = jobs.get_mut(&name) else {
            return Err(serde::de::Error::custom(format!(
                "unknown scheduler recurring job `{name}`"
            )));
        };
        if let Some(enabled) = value.enabled {
            config.enabled = enabled;
        }
        if let Some(interval_secs) = value.interval_secs {
            config.interval_secs = interval_secs;
        }
        if let Some(jitter_secs) = value.jitter_secs {
            config.jitter_secs = jitter_secs;
        }
    }
    Ok(jobs)
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

fn default_upstream_affinity_ttl_days() -> u32 {
    UpstreamAffinityConfig::default().ttl_days
}

fn default_recurring_job_interval_secs() -> u64 {
    RecurringJobConfig::default().interval_secs
}

fn default_recurring_job_jitter_secs() -> u64 {
    RecurringJobConfig::default().jitter_secs
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

fn default_scheduler_keepalive_concurrency() -> usize {
    SchedulerConfig::default().keepalive_concurrency
}

fn default_tracing_level() -> String {
    "info".to_owned()
}

fn default_request_event_retention_days() -> u64 {
    90
}

fn default_price_catalog_url() -> String {
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
        .to_owned()
}

fn default_price_catalog_cache_path() -> PathBuf {
    PathBuf::from("/var/lib/cc-lb/litellm.json")
}

fn default_token_env() -> String {
    DEFAULT_ADMIN_TOKEN_ENV.to_owned()
}

fn default_cf_access_header() -> String {
    "cf-access-jwt-assertion".to_owned()
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

fn default_subscription_quota_writer_batch_max_records() -> u32 {
    SubscriptionQuotaConfig::default().writer_batch_max_records
}

fn default_subscription_quota_writer_flush_ms() -> u64 {
    SubscriptionQuotaConfig::default().writer_flush_ms
}

fn default_subscription_quota_writer_channel_capacity() -> u32 {
    SubscriptionQuotaConfig::default().writer_channel_capacity
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

fn default_prompt_cache_shadow_max_live_entries_per_partition() -> usize {
    PromptCacheShadowConfig::default().max_live_entries_per_partition
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
    fn request_event_retention_and_price_catalog_load_at_top_level() {
        let config = load_config(
            r#"
request_event_retention_days = 14

[price_catalog]
url = "https://example.com/prices.json"
cache_path = "/tmp/prices.json"
"#,
        )
        .expect("config should load");

        assert_eq!(config.request_event_retention_days, 14);
        assert_eq!(config.price_catalog.url, "https://example.com/prices.json");
        assert_eq!(
            config.price_catalog.cache_path,
            PathBuf::from("/tmp/prices.json")
        );
    }
    #[test]
    fn removed_top_level_tables_are_rejected() {
        for field in [
            "api_keys",
            "dns",
            "downstream_auth",
            "egress",
            "lifecycle_api_key_metrics_subscriber",
            "lifecycle_cache_hit_miss_subscriber",
            "lifecycle_cache_observation_subscriber",
            "lifecycle_hook_adapter",
            "lifecycle_limit_reconcile_subscriber",
            "lifecycle_limit_rejection_audit_subscriber",
            "lifecycle_pricing_subscriber",
            "lifecycle_rate_limit_header_subscriber",
            "lifecycle_routing_tier_subscriber",
            "lifecycle_subscription_quota_subscriber",
            "tls",
        ] {
            let config_toml = format!("[{field}]\nremoved = true\n");
            let error = load_config(&config_toml).expect_err("removed table should be rejected");
            assert!(error.to_string().contains(field), "{field}: {error}");
        }
    }

    #[test]
    fn removed_nested_fields_and_aliases_are_rejected() {
        for (field, config_toml) in [
            (
                "listener.unix_socket",
                "[listener]\nunix_socket = \"/tmp/cc-lb.sock\"\n",
            ),
            (
                "body.per_route_overrides",
                "[body]\nper_route_overrides = {}\n",
            ),
            (
                "timeouts.request_header_secs",
                "[timeouts]\nrequest_header_secs = 1\n",
            ),
            (
                "timeouts.request_body_chunk_secs",
                "[timeouts]\nrequest_body_chunk_secs = 1\n",
            ),
            ("timeouts.idle_secs", "[timeouts]\nidle_secs = 1\n"),
            (
                "scheduler.retry_classes",
                "[scheduler.retry_classes]\nprobe = {}\n",
            ),
            (
                "scheduler.idempotency",
                "[scheduler.idempotency]\nclaim_ttl_secs = 60\n",
            ),
            (
                "scheduler.staleness",
                "[scheduler.staleness]\nwarmup_effect_retention_days = 30\n",
            ),
            (
                "scheduler.pgbouncer_transaction_mode",
                "[scheduler]\npgbouncer_transaction_mode = true\n",
            ),
            (
                "observability.prometheus_endpoint",
                "[observability]\nprometheus_endpoint = \"http://localhost:9091\"\n",
            ),
            (
                "admin.token_env",
                "[admin]\ntoken_env = \"CC_LB_ADMIN_TOKEN\"\n",
            ),
            ("admin.token", "[admin]\ntoken = \"secret\"\n"),
            (
                "cluster.token_env_optional",
                "[cluster]\ntoken_env_optional = true\n",
            ),
            (
                "runtime.wasmtime.plugin_failure_policy",
                "[runtime.wasmtime]\nplugin_failure_policy = \"pass_through\"\n",
            ),
            (
                "price_catalog.refresh_interval",
                "[price_catalog]\nrefresh_interval = \"1h\"\n",
            ),
            (
                "prompt_cache_shadow.enabled",
                "[prompt_cache_shadow]\nenabled = false\n",
            ),
            (
                "aead.oauth_aead_key_env",
                "[aead]\noauth_aead_key_env = \"CC_LB_MASTER_KEY\"\n",
            ),
            (
                "storage.storage_path",
                "[storage]\nkind = \"sqlite\"\npath = \"/tmp/current.sqlite\"\nstorage_path = \"/tmp/legacy.sqlite\"\n",
            ),
            (
                "runtime.wasmtime.allocation_strategy=on_demand",
                "[runtime.wasmtime]\nallocation_strategy = \"on_demand\"\n",
            ),
        ] {
            let error = load_config(config_toml).expect_err("removed field should be rejected");
            assert!(error.to_string().contains("unknown"), "{field}: {error}");
        }
    }

    #[test]
    fn event_bus_defaults_and_overrides_deserialize() {
        let default_config = Config::default();
        assert_eq!(
            default_config.event_bus.broadcast_capacity,
            DEFAULT_EVENT_BUS_BROADCAST_CAPACITY
        );
        assert_eq!(
            default_config.event_bus.storage_tail_poll_interval_ms,
            DEFAULT_STORAGE_TAIL_POLL_INTERVAL_MS
        );
        assert_eq!(
            default_config.event_bus.pg_notify_channel,
            DEFAULT_PG_NOTIFY_CHANNEL
        );

        let config = load_config(
            r#"
[storage]
kind = "postgres"
url = "postgres://localhost/cc_lb"

[event_bus]
broadcast_capacity = 8192
storage_tail_poll_interval_ms = 125
pg_notify_channel = "cc_lb_events_partial_custom"
partial_retention_ttl_secs = 60
partial_retention_max_entries = 256

[cluster]
instance_url = "http://127.0.0.1:9090"

"#,
        )
        .expect("config should load");

        assert_eq!(config.event_bus.broadcast_capacity, 8192);
        assert_eq!(config.event_bus.storage_tail_poll_interval_ms, 125);
        assert_eq!(
            config.event_bus.pg_notify_channel,
            "cc_lb_events_partial_custom"
        );
        assert_eq!(config.event_bus.partial_retention_ttl_secs, 60);
        assert_eq!(config.event_bus.partial_retention_max_entries, 256);
    }

    #[test]
    fn event_bus_transport_key_is_rejected() {
        let error = load_config(
            r#"
[event_bus]
transport = "pg_notify"
"#,
        )
        .expect_err("removed event_bus.transport key should fail to parse");
        assert!(error.to_string().contains("transport"));
    }

    #[test]
    fn postgres_storage_requires_cluster_instance_url() {
        load_config(
            r#"
[storage]
kind = "postgres"
url = "postgres://localhost/cc_lb"

[cluster]
instance_url = "http://127.0.0.1:9090"
"#,
        )
        .expect("postgres with instance_url should load");

        let missing_url = load_config(
            r#"
[storage]
kind = "postgres"
url = "postgres://localhost/cc_lb"
"#,
        )
        .expect_err("postgres without instance_url should fail validation");
        assert!(missing_url.to_string().contains("cluster.instance_url"));

        load_config("[listener]\n").expect("sqlite without instance_url should load");
    }

    #[test]
    fn database_owned_top_level_keys_are_rejected() {
        for key in [
            "principals",
            "upstreams",
            "plugins",
            "plugin_chains",
            "quotas",
        ] {
            let config_toml = format!("[{key}]\n");
            let error = load_config(&config_toml).expect_err("database-owned key should fail");
            let message = error.to_string();

            assert!(message.contains("unknown field"), "{message}");
            assert!(message.contains(key), "{message}");
        }
    }
}
