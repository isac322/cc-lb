use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use url::Url;

pub const DEFAULT_MESSAGES_CAP_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_FILES_CAP_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH: u32 = 32;
pub const DEFAULT_PLUGIN_BATCHED_FLUSH_MS: u64 = 100;
pub const DEFAULT_OAUTH_AEAD_KEY_ENV: &str = "CC_LB_MASTER_KEY";
pub const DEFAULT_ADMIN_TOKEN_ENV: &str = "CC_LB_ADMIN_TOKEN";

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
    pub plugins: PluginsConfig,
    pub storage: StorageConfig,
    pub signers: SignersConfig,
    pub observability: ObservabilityConfig,
    pub quotas: QuotasConfig,
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct PrincipalSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quotas: Option<QuotasConfig>,
    pub allowed_models: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default)]
pub struct PluginsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authn_plugin: Option<PluginRef>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct StorageConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redb_path: Option<PathBuf>,
    #[serde(default = "default_oauth_aead_key_env")]
    pub oauth_aead_key_env: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            redb_path: None,
            oauth_aead_key_env: DEFAULT_OAUTH_AEAD_KEY_ENV.to_owned(),
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
pub struct QuotasConfig {
    #[serde(default = "default_quota_window_secs")]
    pub default_window_secs: u64,
    #[serde(default = "default_requests_per_window")]
    pub default_requests_per_window: u64,
    #[serde(default = "default_input_tokens")]
    pub default_input_tokens: u64,
    #[serde(default = "default_output_tokens")]
    pub default_output_tokens: u64,
}

impl Default for QuotasConfig {
    fn default() -> Self {
        Self {
            default_window_secs: 60,
            default_requests_per_window: 1_000,
            default_input_tokens: 1_000_000,
            default_output_tokens: 1_000_000,
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

fn default_quota_window_secs() -> u64 {
    QuotasConfig::default().default_window_secs
}

fn default_requests_per_window() -> u64 {
    QuotasConfig::default().default_requests_per_window
}

fn default_input_tokens() -> u64 {
    QuotasConfig::default().default_input_tokens
}

fn default_output_tokens() -> u64 {
    QuotasConfig::default().default_output_tokens
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
