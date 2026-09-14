#![forbid(unsafe_code)]

#[cfg(not(loom))]
mod hot_reload;
mod types;
mod validation;

use std::{env, ffi::OsString, path::Path};

use figment::providers::{Env, Format, Serialized, Toml};
use figment::value::{Dict, Map, Value};
use figment::{Figment, Metadata, Profile, Provider};
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
        Self::load_with_overrides_and_warnings_from(
            toml_path,
            cli_overrides,
            |path| std::fs::read_to_string(path).ok(),
            |figment| figment.merge(config_env_provider()),
            |name| env::var(name),
            |name| env::var_os(name),
        )
    }

    fn load_with_overrides_and_warnings_from(
        toml_path: &Path,
        cli_overrides: ConfigOverrides,
        read_toml: impl FnOnce(&Path) -> Option<String>,
        merge_env: impl FnOnce(Figment) -> Figment,
        lookup: impl FnMut(&str) -> Result<String, env::VarError>,
        lookup_os: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<(Self, Vec<String>), ConfigError> {
        if let Some(raw_toml) = read_toml(toml_path) {
            return Self::from_toml_str_with_overrides_and_env(
                &raw_toml,
                &cli_overrides,
                merge_env,
                lookup,
                lookup_os,
            );
        }

        let warnings = removed_prompt_cache_env_switches(lookup_os);
        let figment = merge_env(
            Figment::from(Serialized::defaults(Config::default()))
                .merge(Toml::file_exact(toml_path)),
        )
        .merge(Serialized::defaults(cli_overrides));
        finish_loaded_config(figment.extract()?, warnings, lookup)
    }

    pub fn from_toml_str_with_overrides(
        toml: &str,
        overrides: &ConfigOverrides,
    ) -> Result<(Config, Vec<String>), ConfigError> {
        Self::from_toml_str_with_overrides_and_env(
            toml,
            overrides,
            |figment| figment.merge(config_env_provider()),
            |name| env::var(name),
            |name| env::var_os(name),
        )
    }

    fn from_toml_str_with_overrides_and_env(
        toml: &str,
        overrides: &ConfigOverrides,
        merge_env: impl FnOnce(Figment) -> Figment,
        lookup: impl FnMut(&str) -> Result<String, env::VarError>,
        lookup_os: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<(Config, Vec<String>), ConfigError> {
        let mut warnings = validation::removed_prompt_cache_switches(toml);
        validation::validate_raw_toml(toml)?;
        let migrated_toml = validation::migrate_legacy_storage_toml(toml)?;
        warnings.extend(removed_prompt_cache_env_switches(lookup_os));

        let figment = merge_env(
            Figment::from(Serialized::defaults(Config::default()))
                .merge(Toml::string(&migrated_toml)),
        )
        .merge(Serialized::defaults(overrides));
        finish_loaded_config(figment.extract()?, warnings, lookup)
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

    fn apply_admin_auth_env_override_from(
        &mut self,
        lookup: &mut impl FnMut(&str) -> Result<String, env::VarError>,
    ) -> Result<bool, ConfigError> {
        let encoded = match lookup(ADMIN_AUTH_PROVIDERS_JSON_ENV) {
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

    fn resolve_runtime_values_from(
        &mut self,
        mut lookup: impl FnMut(&str) -> Result<String, env::VarError>,
    ) {
        self.admin.token = if self.admin.token_env.trim().is_empty() {
            None
        } else {
            lookup(&self.admin.token_env)
                .ok()
                .filter(|token| !token.is_empty())
        };
    }
}

const CONFIG_ENV_PREFIX: &str = "CC_LB_";

#[derive(Clone)]
struct ConfigEnvProvider {
    data: Map<Profile, Dict>,
}

impl Provider for ConfigEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named(format!("`{CONFIG_ENV_PREFIX}` environment variable(s)")).interpolater(
            |_: &Profile, keys: &[&str]| {
                keys.iter()
                    .map(|key| key.to_ascii_uppercase())
                    .collect::<Vec<_>>()
                    .join(".")
            },
        )
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        Ok(self.data.clone())
    }
}

fn config_env_provider() -> ConfigEnvProvider {
    config_env_provider_from_pairs(
        Env::raw()
            .iter()
            .map(|(key, value)| (key.as_str().to_owned(), value)),
    )
}

fn config_env_provider_from_pairs<I, K, V>(pairs: I) -> ConfigEnvProvider
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let figment = pairs
        .into_iter()
        .filter_map(|(key, value)| {
            config_env_key(key.as_ref()).map(|key| (key, value.as_ref().to_owned()))
        })
        .fold(Figment::new(), |figment, (key, value)| {
            let value = value
                .parse::<Value>()
                .expect("environment value parsing is infallible");
            figment.merge(Serialized::default(&key, value))
        });
    ConfigEnvProvider {
        data: figment
            .data()
            .expect("serialized environment values produce valid provider data"),
    }
}

fn config_env_key(key: &str) -> Option<String> {
    let key = key.trim();
    let prefix = key.get(..CONFIG_ENV_PREFIX.len())?;
    if !prefix.eq_ignore_ascii_case(CONFIG_ENV_PREFIX) {
        return None;
    }

    let key = split_config_env_key(key.get(CONFIG_ENV_PREFIX.len()..)?);
    let key = key.trim();
    if key.is_empty() || key.split('.').any(str::is_empty) {
        return None;
    }

    Some(key.to_ascii_lowercase())
}

fn split_config_env_key(key: &str) -> String {
    key.replace("__", ".")
}

fn finish_loaded_config(
    mut config: Config,
    mut warnings: Vec<String>,
    mut lookup: impl FnMut(&str) -> Result<String, env::VarError>,
) -> Result<(Config, Vec<String>), ConfigError> {
    let admin_auth_env_override = config.apply_admin_auth_env_override_from(&mut lookup)?;
    config.resolve_runtime_values_from(lookup);
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

fn removed_prompt_cache_env_switches(
    mut lookup: impl FnMut(&str) -> Option<OsString>,
) -> Vec<String> {
    validation::removed_prompt_cache_env_switches_in(|variable| lookup(variable).is_some())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    const TOKEN_ENV: &str = "ConfigTestExactAdminToken";

    #[derive(Clone, Copy)]
    enum TokenValue {
        Exact,
        Empty,
        Missing,
        NonUnicode,
    }

    #[test]
    fn config_environment_provider_strips_prefix_nests_and_coerces_numbers() {
        let toml = r#"
[listener]
proxy_addr = "[::1]:7101"

[body]
messages_cap_bytes = 456

[upstream_affinity]
ttl_days = 14
"#;
        let environment = config_env_provider_from_pairs([
            ("OTHER_BODY__MESSAGES_CAP_BYTES", "123"),
            ("CC_LB_LISTENER__PROXY_ADDR", "[::1]:7102"),
            ("CC_LB_UPSTREAM_AFFINITY__TTL_DAYS", "21"),
        ]);
        let (config, warnings) = Config::from_toml_str_with_overrides_and_env(
            toml,
            &ConfigOverrides::default(),
            |figment| figment.merge(environment.clone()),
            runtime_lookup(TokenValue::Missing),
            removed_switch_lookup(false),
        )
        .unwrap();

        assert_eq!(config.listener.proxy_addr, "[::1]:7102".parse().unwrap());
        assert_eq!(config.upstream_affinity.ttl_days, 21);
        assert_eq!(config.body.messages_cap_bytes, 456);
        assert!(warnings.is_empty(), "{warnings:?}");

        let overrides = ConfigOverrides::from_listener(ListenerOverrides {
            proxy_addr: Some("[::1]:7103".parse().unwrap()),
            ..ListenerOverrides::default()
        });
        let (config, warnings) = Config::from_toml_str_with_overrides_and_env(
            toml,
            &overrides,
            |figment| figment.merge(environment),
            runtime_lookup(TokenValue::Missing),
            removed_switch_lookup(false),
        )
        .unwrap();

        assert_eq!(config.listener.proxy_addr, "[::1]:7103".parse().unwrap());
        assert_eq!(config.upstream_affinity.ttl_days, 21);
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn injected_runtime_env_lookup_preserves_exact_missing_empty_and_non_unicode_semantics() {
        let toml = format!("[admin]\ntoken_env = \"{TOKEN_ENV}\"\n");
        let cases = [
            (TokenValue::Exact, Some("exact-token"), false),
            (TokenValue::Empty, None, true),
            (TokenValue::Missing, None, false),
            (TokenValue::NonUnicode, None, true),
        ];

        for (value, expected_token, removed_switch_present) in cases {
            let (config, warnings) = Config::from_toml_str_with_overrides_and_env(
                &toml,
                &ConfigOverrides::default(),
                |figment| figment,
                runtime_lookup(value),
                removed_switch_lookup(removed_switch_present),
            )
            .unwrap();

            assert_eq!(config.admin.token.as_deref(), expected_token);
            assert!(config.admin.auth.providers.is_empty());
            if expected_token.is_some() {
                assert_eq!(warnings.len(), 1, "{warnings:?}");
                assert!(warnings[0].contains("static-token/legacy"), "{warnings:?}");
                assert!(
                    warnings[0].contains(ADMIN_AUTH_PROVIDERS_JSON_ENV),
                    "{warnings:?}"
                );
            } else if removed_switch_present {
                assert_eq!(warnings, vec!["CC_LB_PROMPT_CACHE_SHADOW__ENABLED"]);
            } else {
                assert!(warnings.is_empty(), "{warnings:?}");
            }
        }
    }

    #[test]
    fn injected_admin_auth_provider_lookup_preserves_override_and_error_semantics() {
        let providers = r#"[{"kind":"cloudflare_access","id":"cf","team_domain":"https://team.cloudflareaccess.com","audiences":["admin-aud"]}]"#;
        let (config, warnings) = Config::from_toml_str_with_overrides_and_env(
            r#"
[[admin.auth.providers]]
kind = "static_token"
id = "toml"
token_env = "TOML_ADMIN_TOKEN"
"#,
            &ConfigOverrides::default(),
            |figment| figment,
            |name| match name {
                ADMIN_AUTH_PROVIDERS_JSON_ENV => Ok(providers.to_owned()),
                _ => Err(env::VarError::NotPresent),
            },
            |_| None,
        )
        .unwrap();

        assert!(warnings.is_empty(), "{warnings:?}");
        assert!(matches!(
            &config.admin.auth.providers[..],
            [AdminAuthProviderConfig::CloudflareAccess {
                id,
                team_domain,
                audiences,
                header,
            }] if id == "cf"
                && team_domain == "https://team.cloudflareaccess.com"
                && audiences == &["admin-aud"]
                && header == "cf-access-jwt-assertion"
        ));

        for invalid in [
            Err(env::VarError::NotUnicode(OsString::from("non-unicode"))),
            Ok("not-json".to_owned()),
        ] {
            let token_looked_up = Cell::new(false);
            let error = Config::from_toml_str_with_overrides_and_env(
                &format!("[admin]\ntoken_env = \"{TOKEN_ENV}\"\n"),
                &ConfigOverrides::default(),
                |figment| figment,
                |name| {
                    if name == ADMIN_AUTH_PROVIDERS_JSON_ENV {
                        match &invalid {
                            Ok(value) => Ok(value.clone()),
                            Err(env::VarError::NotPresent) => Err(env::VarError::NotPresent),
                            Err(env::VarError::NotUnicode(value)) => {
                                Err(env::VarError::NotUnicode(value.clone()))
                            }
                        }
                    } else {
                        token_looked_up.set(true);
                        Ok("legacy-token".to_owned())
                    }
                },
                |_| None,
            )
            .unwrap_err();

            assert!(!token_looked_up.get());
            let ConfigError::InvalidAdminAuthProvidersEnv { env, message } = error else {
                panic!("expected invalid admin auth providers error, got {error}");
            };
            assert_eq!(env, ADMIN_AUTH_PROVIDERS_JSON_ENV);
            match invalid {
                Err(env::VarError::NotUnicode(_)) => {
                    assert_eq!(message, "value must be valid UTF-8 JSON");
                }
                Ok(_) => {
                    assert!(message.contains("expected ident"), "{message}");
                }
                Err(env::VarError::NotPresent) => unreachable!(),
            }
        }
    }

    #[test]
    fn removed_switch_lookup_uses_exact_names_and_os_string_presence() {
        let mut probed = Vec::new();
        let warnings = removed_prompt_cache_env_switches(|name| {
            probed.push(name.to_owned());
            match name {
                "CC_LB_PROMPT_CACHE_SHADOW__ENABLED" => Some(OsString::new()),
                "CC_LB_LIFECYCLE_PROMPT_CACHE_OBSERVATION_SUBSCRIBER__ENABLED" => {
                    Some(OsString::from("present"))
                }
                _ => None,
            }
        });

        assert_eq!(
            warnings,
            vec![
                "CC_LB_PROMPT_CACHE_SHADOW__ENABLED",
                "CC_LB_LIFECYCLE_PROMPT_CACHE_OBSERVATION_SUBSCRIBER__ENABLED",
            ]
        );
        assert_eq!(
            probed,
            vec![
                "CC_LB_PROMPT_CACHE_SHADOW__ENABLED",
                "CC_LB_LIFECYCLE_PROMPT_CACHE_DRIFT_SUBSCRIBER__ENABLED",
                "CC_LB_LIFECYCLE_PROMPT_CACHE_OBSERVATION_SUBSCRIBER__ENABLED",
            ]
        );
    }

    fn runtime_lookup(value: TokenValue) -> impl FnMut(&str) -> Result<String, env::VarError> {
        move |name| match name {
            ADMIN_AUTH_PROVIDERS_JSON_ENV => Err(env::VarError::NotPresent),
            DEFAULT_ADMIN_TOKEN_ENV => Err(env::VarError::NotPresent),
            TOKEN_ENV => match value {
                TokenValue::Exact => Ok("exact-token".to_owned()),
                TokenValue::Empty => Ok(String::new()),
                TokenValue::Missing => Err(env::VarError::NotPresent),
                TokenValue::NonUnicode => {
                    Err(env::VarError::NotUnicode(OsString::from("non-unicode")))
                }
            },
            other => panic!("unexpected environment lookup: {other}"),
        }
    }

    fn removed_switch_lookup(present: bool) -> impl FnMut(&str) -> Option<OsString> {
        move |name| {
            if present && name == "CC_LB_PROMPT_CACHE_SHADOW__ENABLED" {
                Some(OsString::new())
            } else {
                None
            }
        }
    }
}
