#![forbid(unsafe_code)]

mod source;
mod types;
mod validation;

pub use source::{BootEnv, BootEnvError, strip_boot_only_keys_for_seed};

use std::env;

use thiserror::Error;

pub use types::{
    AdminConfig, AnthropicOAuthConfig, ApiKeysConfig, BodyConfig, BulkheadConfig,
    CircuitBreakerConfig, ClusterConfig, Config, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES,
    DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_SQLITE_PATH,
    DownstreamAuthConfig, DownstreamAuthMode, EventBusConfig, EventBusTransport,
    LimitReservationTtlConfig, ListenerConfig, NoneModeConfig, NoneModeUpstreamKind,
    ObservabilityConfig, PluginFailurePolicy, PluginWireBounds, PostgresPoolConfig,
    PriceCatalogConfig, PromptCacheShadowConfig, RecurringJobConfig, RestartRequiredField,
    RuntimeConfig, SchedulerConfig, SchedulerPoolConfig, ShapeOriginPolicy, StorageConfig,
    SubscriptionQuotaConfig, TimeoutsConfig, TlsConfig, WasmtimeAllocationStrategy, WasmtimeConfig,
};
pub use validation::{ValidationError, validate_postgres_url, validate_runtime_overlay};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load config: {0}")]
    Figment(#[source] Box<figment::Error>),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("invalid postgres URL: {message}")]
    InvalidPostgresUrl { message: String },
    #[error("postgres statement timeout {statement}s must be less than request timeout {request}s")]
    StatementTimeoutExceedsRequestTimeout { statement: u64, request: u64 },
}

impl From<figment::Error> for ConfigError {
    fn from(error: figment::Error) -> Self {
        Self::Figment(Box::new(error))
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validation::validate_config(self)
    }

    pub fn json_schema() -> schemars::Schema {
        schemars::schema_for!(Config)
    }

    pub(crate) fn resolve_runtime_values(&mut self) {
        self.admin.token = if self.admin.token_env.trim().is_empty() {
            None
        } else {
            env::var(&self.admin.token_env)
                .ok()
                .filter(|token| !token.is_empty())
        };
    }
}
