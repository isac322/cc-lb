#![forbid(unsafe_code)]

#[cfg(not(loom))]
mod hot_reload;
mod types;
mod validation;

use std::{env, path::Path};

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use thiserror::Error;
#[cfg(not(loom))]
use tokio::sync::mpsc;
#[cfg(not(loom))]
use tokio::task::JoinHandle;

pub use types::{
    ADMIN_AUTH_PROVIDERS_JSON_ENV, AdminAuthConfig, AdminAuthProviderConfig, AdminConfig,
    AnthropicOAuthConfig, ApiKeysConfig, BodyConfig, BulkheadConfig, CircuitBreakerConfig,
    ClusterConfig, Config, ConfigOverrides, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES,
    DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_SQLITE_PATH,
    DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS, DnsConfig, DownstreamAuthConfig, DownstreamAuthMode,
    EgressConfig, EventBusConfig, ListenerConfig, ListenerOverrides, NoneModeConfig,
    NoneModeUpstreamKind, ObservabilityConfig, PluginFailurePolicy, PluginWireBounds,
    PostgresPoolConfig, PriceCatalogConfig, PromptCacheShadowConfig, RecurringJobConfig,
    RestartRequiredField, RuntimeConfig, SchedulerConfig, SchedulerIdempotencyConfig,
    SchedulerPoolConfig, SchedulerRetryClasses, SchedulerRetryConfig, SchedulerStalenessConfig,
    ShapeOriginPolicy, StorageConfig, SubscriptionQuotaConfig, TimeoutsConfig, TlsConfig,
    UpstreamAffinityConfig, WasmtimeAllocationStrategy, WasmtimeConfig,
};
pub use validation::{ValidationError, validate_postgres_url};

pub fn removed_prompt_cache_switch_warning(field: &str) -> String {
    format!(
        "removed prompt-cache disable switch `{field}` is ignored; cache-aware routing is always enabled"
    )
}

pub fn config_warning_message(warning: &str) -> String {
    if warning.starts_with("legacy [admin].token_env ") {
        warning.to_owned()
    } else {
        removed_prompt_cache_switch_warning(warning)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load config: {0}")]
    Figment(#[source] Box<figment::Error>),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("invalid postgres URL: {message}")]
    InvalidPostgresUrl { message: String },
    #[error("invalid {env}: {message}")]
    InvalidAdminAuthProvidersEnv { env: &'static str, message: String },
    #[error("postgres statement timeout {statement}s must be less than request timeout {request}s")]
    StatementTimeoutExceedsRequestTimeout { statement: u64, request: u64 },
}

impl From<figment::Error> for ConfigError {
    fn from(error: figment::Error) -> Self {
        Self::Figment(Box::new(error))
    }
}

impl Config {
    pub fn load(toml_path: &Path) -> Result<Self, ConfigError> {
        Self::load_with_warnings(toml_path).map(|(config, _warnings)| config)
    }

    pub fn load_with_warnings(toml_path: &Path) -> Result<(Self, Vec<String>), ConfigError> {
        Self::load_with_overrides_and_warnings(toml_path, ConfigOverrides::default())
    }

    pub fn load_with_overrides(
        toml_path: &Path,
        cli_overrides: ConfigOverrides,
    ) -> Result<Self, ConfigError> {
        Self::load_with_overrides_and_warnings(toml_path, cli_overrides)
            .map(|(config, _warnings)| config)
    }

    pub fn load_with_overrides_and_warnings(
        toml_path: &Path,
        cli_overrides: ConfigOverrides,
    ) -> Result<(Self, Vec<String>), ConfigError> {
        let process_env = Env::raw().lowercase(false);
        let raw_toml = std::fs::read_to_string(toml_path).ok();
        if let Some(raw_toml) = raw_toml.as_deref() {
            return Self::from_toml_str_with_overrides(raw_toml, &cli_overrides);
        }

        let mut warnings = removed_prompt_cache_env_switches(&process_env);
        let mut config: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Toml::file_exact(toml_path))
            .merge(Env::prefixed("CC_LB_").split("__"))
            .merge(Serialized::defaults(cli_overrides))
            .extract()?;

        let admin_auth_env_override = config.apply_admin_auth_env_override()?;
        config.resolve_runtime_values_from(&process_env);
        config.validate()?;
        if !admin_auth_env_override
            && config.admin.auth.providers.is_empty()
            && config.admin.token.is_some()
        {
            warnings.push(format!(
                "legacy [admin].token_env was automatically migrated in memory to the \
                 static-token/legacy provider; configure {ADMIN_AUTH_PROVIDERS_JSON_ENV} before \
                 legacy admin-token compatibility is removed"
            ));
        }
        Ok((config, warnings))
    }

    pub fn from_toml_str_with_overrides(
        toml: &str,
        overrides: &ConfigOverrides,
    ) -> Result<(Config, Vec<String>), ConfigError> {
        let process_env = Env::raw().lowercase(false);
        let mut warnings = validation::removed_prompt_cache_switches(toml);
        validation::validate_raw_toml(toml)?;
        let migrated_toml = validation::migrate_legacy_storage_toml(toml)?;
        warnings.extend(removed_prompt_cache_env_switches(&process_env));

        let mut config: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Toml::string(&migrated_toml))
            .merge(Env::prefixed("CC_LB_").split("__"))
            .merge(Serialized::defaults(overrides))
            .extract()?;

        let admin_auth_env_override = config.apply_admin_auth_env_override()?;
        config.resolve_runtime_values_from(&process_env);
        config.validate()?;
        if !admin_auth_env_override
            && config.admin.auth.providers.is_empty()
            && config.admin.token.is_some()
        {
            warnings.push(format!(
                "legacy [admin].token_env was automatically migrated in memory to the \
                 static-token/legacy provider; configure {ADMIN_AUTH_PROVIDERS_JSON_ENV} before \
                 legacy admin-token compatibility is removed"
            ));
        }
        Ok((config, warnings))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validation::validate_config(self)
    }

    #[cfg(not(loom))]
    pub fn watch_for_reload(path: &Path, tx: mpsc::Sender<Config>) -> JoinHandle<()> {
        hot_reload::watch_for_reload(path, tx)
    }

    pub fn json_schema() -> schemars::Schema {
        schemars::schema_for!(Config)
    }

    fn apply_admin_auth_env_override(&mut self) -> Result<bool, ConfigError> {
        let encoded = match env::var(ADMIN_AUTH_PROVIDERS_JSON_ENV) {
            Ok(encoded) => encoded,
            Err(env::VarError::NotPresent) => return Ok(false),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidAdminAuthProvidersEnv {
                    env: ADMIN_AUTH_PROVIDERS_JSON_ENV,
                    message: "value must be valid UTF-8 JSON".to_owned(),
                });
            }
        };
        let providers: Vec<AdminAuthProviderConfig> =
            serde_json::from_str(&encoded).map_err(|error| {
                ConfigError::InvalidAdminAuthProvidersEnv {
                    env: ADMIN_AUTH_PROVIDERS_JSON_ENV,
                    message: error.to_string(),
                }
            })?;
        if providers.is_empty() {
            return Err(ConfigError::InvalidAdminAuthProvidersEnv {
                env: ADMIN_AUTH_PROVIDERS_JSON_ENV,
                message: "provider list must not be empty".to_owned(),
            });
        }
        self.admin.auth.providers = providers;
        Ok(true)
    }

    fn resolve_runtime_values_from(&mut self, process_env: &Env) {
        self.admin.token = if self.admin.token_env.trim().is_empty() {
            None
        } else {
            environment_value(process_env, &self.admin.token_env).filter(|token| !token.is_empty())
        };
    }
}

fn removed_prompt_cache_env_switches(process_env: &Env) -> Vec<String> {
    validation::removed_prompt_cache_env_switches_in(|variable| {
        environment_value(process_env, variable).is_some()
    })
}

fn environment_value(process_env: &Env, name: &str) -> Option<String> {
    process_env
        .iter()
        .find_map(|(candidate, value)| (candidate.as_str() == name).then_some(value))
}
