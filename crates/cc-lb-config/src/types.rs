use cc_lb_domain::OpenaiOauthCompatPolicy;
use cc_lb_oauth_protocol::{
    ANTIGRAVITY_OAUTH_AUTH_URL, ANTIGRAVITY_OAUTH_CODE_ASSIST_URL, ANTIGRAVITY_OAUTH_REDIRECT_URI,
    ANTIGRAVITY_OAUTH_SCOPES, ANTIGRAVITY_OAUTH_TOKEN_URL, ANTIGRAVITY_OAUTH_USER_AGENT,
    ANTIGRAVITY_OAUTH_USERINFO_URL, GEMINI_OAUTH_CODE_ASSIST_URL, OPENAI_CODEX_CLIENT_ID,
    OPENAI_OAUTH_AUTHORIZE_URL, OPENAI_OAUTH_BROWSER_REDIRECT_URI,
    OPENAI_OAUTH_DEVICE_REDIRECT_URI, OPENAI_OAUTH_DEVICE_TOKEN_URL,
    OPENAI_OAUTH_DEVICE_USERCODE_URL, OPENAI_OAUTH_DEVICE_VERIFICATION_URL, OPENAI_OAUTH_TOKEN_URL,
};
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

pub const DEFAULT_MESSAGES_CAP_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_FILES_CAP_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_MAX_BUFFERED_RESPONSE_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_UPSTREAM_CONNECT_SECS: u64 = 30;
pub const DEFAULT_OAUTH_AEAD_KEY_ENV: &str = "CC_LB_MASTER_KEY";
pub const DEFAULT_ADMIN_TOKEN_ENV: &str = "CC_LB_ADMIN_TOKEN";
pub const ADMIN_AUTH_PROVIDERS_JSON_ENV: &str = "CC_LB_ADMIN_AUTH_PROVIDERS_JSON";
pub const DEFAULT_SQLITE_PATH: &str = "/var/lib/cc-lb/storage.sqlite";
pub const DEFAULT_EVENT_BUS_BROADCAST_CAPACITY: usize = 4096;
pub const DEFAULT_STORAGE_TAIL_POLL_INTERVAL_MS: u64 = 250;
pub const DEFAULT_PG_NOTIFY_CHANNEL: &str = "cc_lb_events_partial";
pub const DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS: u32 = 7;

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
    pub warmup: WarmupConfig,
    #[serde(default)]
    pub upstream_affinity: UpstreamAffinityConfig,
    pub aead: AeadConfig,
    pub observability: ObservabilityConfig,
    pub admin: AdminConfig,
    #[serde(default)]
    pub event_bus: EventBusConfig,
    pub oauth: OAuthConfig,
    #[serde(default)]
    pub subscription_quota: SubscriptionQuotaConfig,
    pub runtime: RuntimeConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub bulkhead: BulkheadConfig,
    /// Restart-only defaults for public `/v1` requests served by
    /// ChatGPT-subscription (`openai_oauth`) upstreams. Each principal may
    /// override individual fields through the admin API.
    #[serde(default)]
    pub openai_oauth_compat: OpenaiOauthCompatPolicy,
    #[serde(default)]
    pub openai_public_api: OpenaiPublicApiConfig,
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
            warmup: WarmupConfig::default(),
            upstream_affinity: UpstreamAffinityConfig::default(),
            aead: AeadConfig::default(),
            observability: ObservabilityConfig::default(),
            admin: AdminConfig::default(),
            event_bus: EventBusConfig::default(),
            oauth: OAuthConfig::default(),
            subscription_quota: SubscriptionQuotaConfig::default(),
            runtime: RuntimeConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            bulkhead: BulkheadConfig::default(),
            openai_oauth_compat: OpenaiOauthCompatPolicy::default(),
            openai_public_api: OpenaiPublicApiConfig::default(),
            prompt_cache_shadow: PromptCacheShadowConfig::default(),
            limit_reservation_ttl: LimitReservationTtlConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OpenaiPublicApiConfig {
    pub realtime_origin: Url,
}

impl Default for OpenaiPublicApiConfig {
    fn default() -> Self {
        Self {
            realtime_origin: Url::parse("https://api.openai.com/").expect("valid URL"),
        }
    }
}

pub use cc_lb_domain::UnknownOpenaiReasoningPolicy;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamAffinityConfig {
    #[serde(default = "default_upstream_affinity_ttl_days")]
    #[schemars(range(min = 1))]
    pub ttl_days: u32,
    #[serde(default)]
    pub unknown_openai_reasoning: UnknownOpenaiReasoningPolicy,
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
            unknown_openai_reasoning: UnknownOpenaiReasoningPolicy::default(),
        }
    }
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
    #[serde(default = "default_tls_handshake_timeout_secs")]
    #[schemars(range(min = 1))]
    pub handshake_timeout_secs: u64,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            reload_on_sighup: true,
            handshake_timeout_secs: DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS,
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
    #[serde(default = "default_max_buffered_response_bytes")]
    #[schemars(range(min = 1))]
    pub max_buffered_response_bytes: u64,
}

impl Default for BodyConfig {
    fn default() -> Self {
        Self {
            messages_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_cap_bytes: DEFAULT_FILES_CAP_BYTES,
            max_buffered_response_bytes: DEFAULT_MAX_BUFFERED_RESPONSE_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TimeoutsConfig {
    #[schemars(default = "default_upstream_total_secs")]
    pub upstream_total_secs: u64,
    #[schemars(range(min = 1), default = "default_upstream_connect_secs")]
    pub upstream_connect_secs: u64,
    #[schemars(default = "default_drain_secs")]
    pub drain_secs: u64,
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            upstream_total_secs: 600,
            upstream_connect_secs: DEFAULT_UPSTREAM_CONNECT_SECS,
            drain_secs: 60,
        }
    }
}

impl<'de> Deserialize<'de> for TimeoutsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawTimeoutsConfig {
            upstream_total_secs: Option<u64>,
            upstream_connect_secs: Option<u64>,
            drain_secs: Option<u64>,
        }

        let defaults = Self::default();
        let raw = RawTimeoutsConfig::deserialize(deserializer)?;
        let upstream_total_secs = raw
            .upstream_total_secs
            .unwrap_or(defaults.upstream_total_secs);
        // Existing configurations with `upstream_total_secs < 30` (including legacy `0`,
        // which runs with a 1s effective total timeout) and an omitted connect timeout
        // stay valid on upgrade, while explicit overrides remain subject to `validate_timeouts`.
        let effective_total_secs = upstream_total_secs.max(1);
        let upstream_connect_secs = raw
            .upstream_connect_secs
            .unwrap_or_else(|| DEFAULT_UPSTREAM_CONNECT_SECS.min(effective_total_secs));
        let drain_secs = raw.drain_secs.unwrap_or(defaults.drain_secs);
        Ok(Self {
            upstream_total_secs,
            upstream_connect_secs,
            drain_secs,
        })
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
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            broadcast_capacity: DEFAULT_EVENT_BUS_BROADCAST_CAPACITY,
            storage_tail_poll_interval_ms: DEFAULT_STORAGE_TAIL_POLL_INTERVAL_MS,
            pg_notify_channel: DEFAULT_PG_NOTIFY_CHANNEL.to_owned(),
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

pub const DEFAULT_PGBOUNCER_STATEMENT_CACHE_CAPACITY: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
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
    /// Optional SSL mode override; when omitted, inherit `sslmode` or `ssl-mode` from the PostgreSQL URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
    /// Optional direct PostgreSQL URL for dedicated session-pinned listeners (`LISTEN/NOTIFY`), migrations, and advisory locks when `url` goes through a transaction pooler such as PgBouncer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listener_url: Option<String>,
    /// Optional SQLx prepared-statement cache capacity override (must be greater than `0` when `pgbouncer_transaction_pooling` is enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_cache_capacity: Option<usize>,
    /// Configure connections for PgBouncer transaction pooling (requires PgBouncer >= 1.21 with `max_prepared_statements > 0`, defaults `statement_cache_capacity` to `100` while rejecting `0`, and skips session-level `SET` statements in `after_connect`).
    #[serde(default, skip_serializing_if = "is_false_bool")]
    pub pgbouncer_transaction_pooling: bool,
}

fn is_false_bool(value: &bool) -> bool {
    !*value
}

impl PostgresPoolConfig {
    pub fn effective_listener_url<'a>(&'a self, primary_url: &'a str) -> &'a str {
        self.listener_url.as_deref().unwrap_or(primary_url)
    }

    pub fn effective_statement_cache_capacity(&self) -> Option<usize> {
        self.statement_cache_capacity.or_else(|| {
            self.pgbouncer_transaction_pooling
                .then_some(DEFAULT_PGBOUNCER_STATEMENT_CACHE_CAPACITY)
        })
    }
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
            sslmode: None,
            listener_url: None,
            statement_cache_capacity: None,
            pgbouncer_transaction_pooling: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WarmupConfig {
    pub openai_model: String,
    /// Primary-lane model for Gemini CLI OAuth credentials.
    pub gemini_model: String,
    /// Primary-lane model for Antigravity Gemini OAuth credentials.
    pub gemini_antigravity_model: String,
    /// Model for the Antigravity-only third-party lane.
    pub gemini_third_party_model: String,
}

impl Default for WarmupConfig {
    fn default() -> Self {
        Self {
            openai_model: "gpt-6.1-sol".to_owned(),
            gemini_model: "gemini-2.5-flash-lite".to_owned(),
            gemini_antigravity_model: "gemini-3.8-flash-low".to_owned(),
            gemini_third_party_model: "gpt-oss-120b-medium".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerConfig {
    #[serde(default)]
    pub separate_pool: SchedulerPoolConfig,
    /// Built-in recurring jobs by name. `api_key_usage_poll` (default every
    /// 300 s, 15 s jitter) requires `60 <= interval_secs <= 604800` and
    /// `jitter_secs < interval_secs`.
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
    /// Maximum time allowed for one keep-alive provider request. Must be below
    /// the scheduler's 60-second per-job timeout.
    #[serde(default = "default_scheduler_keepalive_dispatch_timeout_secs")]
    #[schemars(range(min = 1, max = 59))]
    pub keepalive_dispatch_timeout_secs: u64,
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
            keepalive_dispatch_timeout_secs: 30,
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
    /// Optional SSL mode override for scheduler connections; when omitted, inherit `sslmode` or `ssl-mode` from the scheduler PostgreSQL URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
}

impl Default for SchedulerPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 5,
            min_connections: 1,
            acquire_timeout_secs: default_acquire_timeout_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            statement_timeout_secs: default_statement_timeout_secs(),
            sslmode: None,
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

/// `expires_in` requested for a long-lived (365-day) Anthropic OAuth grant.
///
/// Anthropic honours this only on the `authorization_code` exchange and only
/// for a restricted scope set; see [`AnthropicOAuthConfig::scopes`].
pub const LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS: u64 = 31_536_000;

/// Shortest access-token lifetime that still justifies long-lived mode.
///
/// Anthropic anchors refresh tokens to a 30-day window, so a grant that does
/// not outlive that window buys nothing over the refreshing flow while giving
/// up the ability to renew. A server that silently clamps below this is
/// treated as refusing long-lived mode.
pub const LONG_LIVED_MIN_GRANT_SECS: u64 = 2_592_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AnthropicOAuthConfig {
    pub client_id: String,
    pub auth_url: Url,
    pub token_url: Url,
    pub redirect_uri: Url,
    /// Scopes requested on every Anthropic OAuth authorization.
    ///
    /// Connecting always asks Anthropic for a 365-day access token, which it
    /// only grants for inference-only scopes: `org:create_api_key`,
    /// `user:sessions:claude_code`, and `user:mcp_servers` would each force a
    /// shorter lifetime, and cc-lb uses none of them. If Anthropic still
    /// refuses or shortens the grant, the connect flow keeps the credential
    /// refreshable instead.
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
            scopes: vec!["user:profile".to_owned(), "user:inference".to_owned()],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OpenAiOAuthConfig {
    pub client_id: String,
    pub authorize_url: Url,
    pub token_url: Url,
    pub browser_redirect_uri: Url,
    pub device_usercode_url: Url,
    pub device_token_url: Url,
    pub device_verification_url: Url,
    pub device_redirect_uri: Url,
}

impl Default for OpenAiOAuthConfig {
    fn default() -> Self {
        Self {
            client_id: OPENAI_CODEX_CLIENT_ID.to_owned(),
            authorize_url: Url::parse(OPENAI_OAUTH_AUTHORIZE_URL).expect("valid url"),
            token_url: Url::parse(OPENAI_OAUTH_TOKEN_URL).expect("valid url"),
            browser_redirect_uri: Url::parse(OPENAI_OAUTH_BROWSER_REDIRECT_URI).expect("valid url"),
            device_usercode_url: Url::parse(OPENAI_OAUTH_DEVICE_USERCODE_URL).expect("valid url"),
            device_token_url: Url::parse(OPENAI_OAUTH_DEVICE_TOKEN_URL).expect("valid url"),
            device_verification_url: Url::parse(OPENAI_OAUTH_DEVICE_VERIFICATION_URL)
                .expect("valid url"),
            device_redirect_uri: Url::parse(OPENAI_OAUTH_DEVICE_REDIRECT_URI).expect("valid url"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct GeminiOAuthConfig {
    pub code_assist_url: Url,
    pub userinfo_url: Url,
}

impl Default for GeminiOAuthConfig {
    fn default() -> Self {
        Self {
            code_assist_url: Url::parse(GEMINI_OAUTH_CODE_ASSIST_URL).expect("valid url"),
            userinfo_url: Url::parse("https://www.googleapis.com/oauth2/v3/userinfo")
                .expect("valid url"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AntigravityOAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub authorize_url: Url,
    pub token_url: Url,
    pub redirect_uri: Url,
    pub scopes: Vec<String>,
    pub code_assist_url: Url,
    pub userinfo_url: Url,
    pub user_agent: String,
}

impl Default for AntigravityOAuthConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            authorize_url: Url::parse(ANTIGRAVITY_OAUTH_AUTH_URL).expect("valid url"),
            token_url: Url::parse(ANTIGRAVITY_OAUTH_TOKEN_URL).expect("valid url"),
            redirect_uri: Url::parse(ANTIGRAVITY_OAUTH_REDIRECT_URI).expect("valid url"),
            scopes: ANTIGRAVITY_OAUTH_SCOPES
                .iter()
                .map(|scope| (*scope).to_owned())
                .collect(),
            code_assist_url: Url::parse(ANTIGRAVITY_OAUTH_CODE_ASSIST_URL).expect("valid url"),
            userinfo_url: Url::parse(ANTIGRAVITY_OAUTH_USERINFO_URL).expect("valid url"),
            user_agent: ANTIGRAVITY_OAUTH_USER_AGENT.to_owned(),
        }
    }
}

impl AntigravityOAuthConfig {
    pub fn missing_client_credentials(&self) -> Option<&'static str> {
        match (
            self.client_id.trim().is_empty(),
            self.client_secret.trim().is_empty(),
        ) {
            (true, true) => Some("oauth.antigravity.client_id and oauth.antigravity.client_secret"),
            (true, false) => Some("oauth.antigravity.client_id"),
            (false, true) => Some("oauth.antigravity.client_secret"),
            (false, false) => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OAuthConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anthropic: Option<AnthropicOAuthConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai: Option<OpenAiOAuthConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gemini: Option<GeminiOAuthConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub antigravity: Option<AntigravityOAuthConfig>,
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
/// Controls prompt cache observation behavior. Observations decoded from successful
/// responses are persisted to the shared observation store, and `build_candidates`
/// reads that store per `(upstream, canonical model)` partition to compute cache
/// scores.
///
/// Configuration keys and defaults:
/// - `grace_margin_secs` (default: 0) - optional shortening of the
///   dispatch-anchored provider TTL; zero preserves the full TTL.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PromptCacheShadowConfig {
    #[serde(default = "default_prompt_cache_shadow_grace_margin_secs")]
    pub grace_margin_secs: u64,
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
    /// Per-call Wasmtime epoch preemption timeout in milliseconds (default 1000ms, valid range 10..=30000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_timeout_ms: Option<u64>,
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
/// (scheme+host+port) differs from the selected upstream. The default
/// is `SelectedUpstreamOrigin`, which rejects any URL that does not
/// match the selected upstream's `base_url` origin; deployments that
/// intentionally redirect requests to another host can opt into
/// `Unrestricted`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShapeOriginPolicy {
    Unrestricted,
    #[default]
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
    #[serde(default)]
    pub request_diagnostics_log: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            tracing_level: default_tracing_level(),
            otlp_endpoint: None,
            log_redaction: true,
            user_prompt_redaction: false,
            request_diagnostics_log: false,
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

fn default_max_buffered_response_bytes() -> u64 {
    DEFAULT_MAX_BUFFERED_RESPONSE_BYTES
}

fn default_tls_handshake_timeout_secs() -> u64 {
    DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS
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

fn default_upstream_total_secs() -> u64 {
    TimeoutsConfig::default().upstream_total_secs
}

fn default_upstream_connect_secs() -> u64 {
    DEFAULT_UPSTREAM_CONNECT_SECS
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
        ("oauth_usage_poll".to_owned(), recurring_job_config(60, 15)),
        (
            "api_key_usage_poll".to_owned(),
            recurring_job_config(300, 15),
        ),
        (
            "pool_quota_snapshot".to_owned(),
            recurring_job_config(60, 29),
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

fn default_scheduler_keepalive_dispatch_timeout_secs() -> u64 {
    SchedulerConfig::default().keepalive_dispatch_timeout_secs
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::fs;

    fn load_config(toml: &str) -> Result<Config, crate::ConfigError> {
        let temp_file = tempfile::NamedTempFile::new().expect("create temp config");
        fs::write(temp_file.path(), toml).expect("write temp config");
        Config::load(temp_file.path())
    }

    #[test]
    fn warmup_models_default_and_can_be_overridden_independently() {
        let defaults = load_config("").expect("default config");
        assert_eq!(defaults.warmup.openai_model, "gpt-6.1-sol");
        assert_eq!(defaults.warmup.gemini_model, "gemini-2.5-flash-lite");
        assert_eq!(
            defaults.warmup.gemini_antigravity_model,
            "gemini-3.8-flash-low"
        );

        let overridden =
            load_config("[warmup]\nopenai_model = \"custom-codex\"\n").expect("warmup config");
        assert_eq!(overridden.warmup.openai_model, "custom-codex");
        assert_eq!(overridden.warmup.gemini_model, "gemini-2.5-flash-lite");

        let overridden =
            load_config("[warmup]\ngemini_model = \"custom-gemini\"\n").expect("warmup config");
        assert_eq!(overridden.warmup.openai_model, "gpt-6.1-sol");
        assert_eq!(overridden.warmup.gemini_model, "custom-gemini");

        let overridden =
            load_config("[warmup]\ngemini_antigravity_model = \"custom-antigravity\"\n")
                .expect("warmup config");
        assert_eq!(
            overridden.warmup.gemini_antigravity_model,
            "custom-antigravity"
        );
        assert_eq!(overridden.warmup.gemini_model, "gemini-2.5-flash-lite");
    }

    #[test]
    fn warmup_gemini_third_party_model_defaults_and_accepts_claude_or_gpt_oss() {
        let defaults = load_config("").expect("default config");
        assert_eq!(
            defaults.warmup.gemini_third_party_model,
            "gpt-oss-120b-medium"
        );

        for model in ["claude-sonnet-5-5-low", "gpt-oss-20b"] {
            let config = load_config(&format!(
                "[warmup]\ngemini_third_party_model = \"{model}\"\n"
            ))
            .expect("third-party warmup model accepted");
            assert_eq!(config.warmup.gemini_third_party_model, model);
            assert_eq!(config.warmup.gemini_model, "gemini-2.5-flash-lite");
        }
    }

    #[test]
    fn warmup_gemini_third_party_model_rejects_models_outside_the_third_party_group() {
        for model in ["gemini-2.5-flash-lite", "gpt-6.1-sol", ""] {
            let error = load_config(&format!(
                "[warmup]\ngemini_third_party_model = \"{model}\"\n"
            ))
            .expect_err("non third-party model rejected");
            assert!(
                matches!(
                    &error,
                    crate::ConfigError::Validation(validation)
                        if validation.field == "warmup.gemini_third_party_model"
                ),
                "unexpected error for {model:?}: {error}"
            );
        }
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
    fn unknown_top_level_and_nested_fields_are_rejected() {
        let error = load_config("[unknown_table]\nkey = true\n")
            .expect_err("unknown top-level table should be rejected");
        assert!(error.to_string().contains("unknown"), "{error}");

        let error = load_config("[listener]\nunknown_field = true\n")
            .expect_err("unknown nested field should be rejected");
        assert!(error.to_string().contains("unknown"), "{error}");
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

"#,
        )
        .expect("config should load");

        assert_eq!(config.event_bus.broadcast_capacity, 8192);
        assert_eq!(config.event_bus.storage_tail_poll_interval_ms, 125);
        assert_eq!(
            config.event_bus.pg_notify_channel,
            "cc_lb_events_partial_custom"
        );
    }
}
