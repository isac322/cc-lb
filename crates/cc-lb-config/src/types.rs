use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use schemars::JsonSchema;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
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
pub const DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH: u32 = 32;
pub const DEFAULT_PLUGIN_BATCHED_FLUSH_MS: u64 = 100;
pub const DEFAULT_OAUTH_AEAD_KEY_ENV: &str = "CC_LB_MASTER_KEY";
pub const DEFAULT_ADMIN_TOKEN_ENV: &str = "CC_LB_ADMIN_TOKEN";
pub const DEFAULT_REDB_PATH: &str = "/var/lib/cc-lb/storage.redb";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct Config {
    pub listener: ListenerConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
    pub body: BodyConfig,
    pub timeouts: TimeoutsConfig,
    pub upstreams: HashMap<String, UpstreamSpec>,
    pub principals: HashMap<String, PrincipalSpec>,
    pub quotas: QuotasConfig,
    pub plugins: PluginsConfig,
    pub downstream_auth: DownstreamAuthConfig,
    pub api_keys: ApiKeysConfig,
    pub storage: StorageConfig,
    pub aead: AeadConfig,
    pub signers: SignersConfig,
    pub observability: ObservabilityConfig,
    pub admin: AdminConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub bulkhead: BulkheadConfig,
    pub dns: DnsConfig,
    pub egress: EgressConfig,
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
pub enum UpstreamKind {
    AnthropicDirect,
    BedrockRuntime,
    BedrockMantle,
    Vertex,
    Custom,
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
pub enum PrincipalType {
    Human,
    Machine,
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
    AwsSigV4,
    GcpOAuth,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NoneModeConfig {
    pub principal_id: String,
    pub upstream_kind: NoneModeUpstreamKind,
    pub upstream_credential_ref: String,
}

impl Default for NoneModeConfig {
    fn default() -> Self {
        Self {
            principal_id: String::new(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            upstream_credential_ref: String::new(),
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthStrategy {
    ApiKey,
    OAuth,
    AwsSigV4,
    GcpOAuth,
    InternalForwarded,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UpstreamSpec {
    pub kind: UpstreamKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<Url>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub auth_strategy: AuthStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct QuotasConfig {
    #[serde(default = "default_quota_window_secs")]
    pub default_window_secs: u64,
    #[serde(default = "default_quota_requests_per_window")]
    pub default_requests_per_window: u64,
    #[serde(default = "default_quota_input_tokens")]
    pub default_input_tokens: u64,
    #[serde(default = "default_quota_output_tokens")]
    pub default_output_tokens: u64,
}

impl Default for QuotasConfig {
    fn default() -> Self {
        Self {
            default_window_secs: default_quota_window_secs(),
            default_requests_per_window: default_quota_requests_per_window(),
            default_input_tokens: default_quota_input_tokens(),
            default_output_tokens: default_quota_output_tokens(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PrincipalSpec {
    #[serde(default = "default_machine")]
    pub principal_type: PrincipalType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quotas: Option<QuotasConfig>,
    #[serde(default)]
    pub default_limits: Vec<Limit>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub allowed_models: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<String>,
}

impl Default for PrincipalSpec {
    fn default() -> Self {
        Self {
            principal_type: PrincipalType::Machine,
            quotas: None,
            default_limits: Vec::new(),
            enabled: true,
            allowed_models: Vec::new(),
            credentials_ref: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default)]
pub struct PluginsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub router_plugin: Option<PluginRef>,
    pub observability_hooks: Vec<PluginRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct PluginRef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wasm_path: Option<PathBuf>,
    #[serde(default = "default_plugin_config")]
    pub config: Value,
    pub sse_per_event: bool,
    #[serde(default = "default_plugin_batched_events_per_flush")]
    pub batched_events_per_flush: u32,
    #[serde(default = "default_plugin_batched_flush_ms")]
    pub batched_flush_ms: u64,
}

impl Default for PluginRef {
    fn default() -> Self {
        Self {
            name: String::new(),
            wasm_path: None,
            config: default_plugin_config(),
            sse_per_event: false,
            batched_events_per_flush: DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH,
            batched_flush_ms: DEFAULT_PLUGIN_BATCHED_FLUSH_MS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageConfig {
    Redb {
        path: PathBuf,
    },
    Postgres {
        url: String,
        #[serde(default)]
        pool: PostgresPoolConfig,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Redb {
            path: PathBuf::from(DEFAULT_REDB_PATH),
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
        let has_legacy_redb_path = value.get("redb_path").is_some();
        let has_legacy_aead_env = value.get("oauth_aead_key_env").is_some();

        if has_kind {
            let tagged = TaggedStorageConfig::deserialize(value).map_err(D::Error::custom)?;
            return Ok(tagged.into());
        }

        if has_legacy_redb_path || has_legacy_aead_env {
            tracing::warn!(
                "[storage] redb_path/oauth_aead_key_env is deprecated; use kind = \"redb\" + path and [aead].key_env"
            );
            let legacy = LegacyStorageConfig::deserialize(value).map_err(D::Error::custom)?;
            return Ok(Self::Redb {
                path: legacy
                    .redb_path
                    .unwrap_or_else(|| PathBuf::from(DEFAULT_REDB_PATH)),
            });
        }

        Ok(Self::default())
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TaggedStorageConfig {
    Redb {
        path: PathBuf,
    },
    Postgres {
        url: String,
        #[serde(default)]
        pool: PostgresPoolConfig,
    },
}

impl From<TaggedStorageConfig> for StorageConfig {
    fn from(value: TaggedStorageConfig) -> Self {
        match value {
            TaggedStorageConfig::Redb { path } => Self::Redb { path },
            TaggedStorageConfig::Postgres { url, pool } => Self::Postgres { url, pool },
        }
    }
}

#[derive(Default, Deserialize)]
struct LegacyStorageConfig {
    redb_path: Option<PathBuf>,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct SignersConfig {
    pub anthropic_oauth: AnthropicOAuthSignerConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AnthropicOAuthSignerConfig {
    #[serde(default = "default_anthropic_oauth_issuer_base_url")]
    pub issuer_base_url: String,
    pub client_id: String,
    #[serde(default = "default_anthropic_oauth_redirect_uri")]
    pub redirect_uri: String,
    #[serde(default = "default_anthropic_oauth_scopes")]
    pub scopes: Vec<String>,
}

impl Default for AnthropicOAuthSignerConfig {
    fn default() -> Self {
        Self {
            issuer_base_url: default_anthropic_oauth_issuer_base_url(),
            client_id: String::new(),
            redirect_uri: default_anthropic_oauth_redirect_uri(),
            scopes: default_anthropic_oauth_scopes(),
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

fn default_quota_window_secs() -> u64 {
    60
}

fn default_quota_requests_per_window() -> u64 {
    1_000
}

fn default_quota_input_tokens() -> u64 {
    1_000_000
}

fn default_quota_output_tokens() -> u64 {
    1_000_000
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

fn default_plugin_config() -> Value {
    Value::Object(Map::new())
}

fn default_plugin_batched_events_per_flush() -> u32 {
    DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH
}

fn default_plugin_batched_flush_ms() -> u64 {
    DEFAULT_PLUGIN_BATCHED_FLUSH_MS
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

fn default_anthropic_oauth_issuer_base_url() -> String {
    "https://platform.claude.com".to_owned()
}

fn default_anthropic_oauth_redirect_uri() -> String {
    "http://127.0.0.1/admin/oauth/callback".to_owned()
}

fn default_anthropic_oauth_scopes() -> Vec<String> {
    vec!["messages".to_owned(), "files".to_owned()]
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

fn default_machine() -> PrincipalType {
    PrincipalType::Machine
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
upstream_credential_ref = "cred-1"

[api_keys]
"#,
        )
        .expect("config should load");

        assert_eq!(config.downstream_auth.mode, DownstreamAuthMode::None);
        let none_mode = config.downstream_auth.none_mode.expect("none mode config");
        assert_eq!(none_mode.principal_id, "anon");
        assert_eq!(none_mode.upstream_kind, NoneModeUpstreamKind::AnthropicKey);
        assert_eq!(none_mode.upstream_credential_ref, "cred-1");
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

        assert!(error
            .to_string()
            .contains("downstream_auth.none_mode must be set iff mode=none"));
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
upstream_credential_ref = "cred-1"

[api_keys]
"#,
        )
        .expect_err("config should fail validation");

        assert!(error
            .to_string()
            .contains("downstream_auth.none_mode must be set iff mode=none"));
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
        let error = load_config(&config_toml)
        .expect_err("legacy plugin should fail");

        assert!(error.to_string().contains(&legacy_removed_message()));
    }

    #[test]
    fn legacy_principal_quotas_rejected() {
        let error = load_config(
            r#"
[principals.u1.quotas]
default_window_secs = 60

[api_keys]
"#,
        )
        .expect_err("legacy quotas should fail");

        assert!(error.to_string().contains(&legacy_removed_message()));
    }

    #[test]
    fn principal_spec_defaults_to_machine() {
        let principal = toml::from_str::<PrincipalSpec>("allowed_models = []")
            .expect("principal should deserialize");

        assert_eq!(principal.principal_type, PrincipalType::Machine);
        assert!(principal.default_limits.is_empty());
        assert!(principal.enabled);
    }
}
