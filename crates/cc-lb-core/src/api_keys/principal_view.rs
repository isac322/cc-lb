use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_lb_config::{Config, ConfigError, Limit as ConfigLimit, LimitKind as ConfigLimitKind};
use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::api_keys::types::{Limit, LimitKind, PrincipalType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalStatus {
    Active,
    Disabled,
    Missing,
}

#[derive(Debug)]
pub struct PrincipalView {
    specs: HashMap<String, PrincipalSpecCached>,
}

#[derive(Debug)]
pub struct PrincipalSpecCached {
    id: String,
    principal_type: PrincipalType,
    allowed_models: GlobSet,
    allowed_models_exact: HashSet<String>,
    default_limits: Vec<Limit>,
    enabled: bool,
}

impl PrincipalView {
    pub fn from_config(config: &Config) -> Result<Arc<PrincipalView>, ConfigError> {
        let specs = config
            .principals
            .iter()
            .map(|(principal_id, principal)| -> Result<_, ConfigError> {
                let mut exact = HashSet::new();
                let mut builder = GlobSetBuilder::new();

                for model in &principal.allowed_models {
                    if is_glob_pattern(model) {
                        builder.add(Glob::new(model).map_err(|source| {
                            invalid_allowed_models_glob(principal_id, model, source)
                        })?);
                    } else {
                        exact.insert(model.clone());
                    }
                }

                let allowed_models = builder.build().map_err(|source| {
                    invalid_allowed_models_glob(principal_id, "<compiled glob set>", source)
                })?;

                let cached = PrincipalSpecCached {
                    id: principal_id.clone(),
                    principal_type: principal.principal_type.clone().into(),
                    allowed_models,
                    allowed_models_exact: exact,
                    default_limits: principal
                        .default_limits
                        .iter()
                        .cloned()
                        .map(Into::into)
                        .collect(),
                    enabled: principal.enabled,
                };

                Ok((principal_id.clone(), cached))
            })
            .collect::<Result<HashMap<_, _>, ConfigError>>()?;

        Ok(Arc::new(PrincipalView { specs }))
    }

    pub fn get(&self, principal_id: &str) -> Option<&PrincipalSpecCached> {
        self.specs.get(principal_id)
    }

    pub fn is_model_allowed(&self, principal_id: &str, model: &str) -> bool {
        let Some(spec) = self.get(principal_id) else {
            return false;
        };

        if spec.allowed_models_exact.is_empty() && spec.allowed_models.is_empty() {
            return true;
        }

        spec.allowed_models_exact.contains(model) || spec.allowed_models.is_match(model)
    }

    pub fn principal_status(&self, principal_id: &str) -> PrincipalStatus {
        let Some(spec) = self.get(principal_id) else {
            return PrincipalStatus::Missing;
        };

        if spec.enabled {
            PrincipalStatus::Active
        } else {
            PrincipalStatus::Disabled
        }
    }

    pub fn default_limits(&self, principal_id: &str) -> &[Limit] {
        self.get(principal_id)
            .map(|spec| spec.default_limits.as_slice())
            .unwrap_or(&[])
    }
}

impl PrincipalSpecCached {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn principal_type(&self) -> PrincipalType {
        self.principal_type
    }
}

impl From<ConfigLimit> for Limit {
    fn from(value: ConfigLimit) -> Self {
        Self {
            kind: value.kind.into(),
            window: value.window,
            cap_micros: value.cap_micros,
        }
    }
}

impl From<ConfigLimitKind> for LimitKind {
    fn from(value: ConfigLimitKind) -> Self {
        match value {
            ConfigLimitKind::Requests => Self::Requests,
            ConfigLimitKind::InputTokens => Self::InputTokens,
            ConfigLimitKind::OutputTokens => Self::OutputTokens,
            ConfigLimitKind::TotalTokens => Self::TotalTokens,
            ConfigLimitKind::CostUsd => Self::CostUsd,
            ConfigLimitKind::Concurrent => Self::Concurrent,
        }
    }
}

impl From<cc_lb_config::PrincipalType> for PrincipalType {
    fn from(value: cc_lb_config::PrincipalType) -> Self {
        match value {
            cc_lb_config::PrincipalType::Human => Self::Human,
            cc_lb_config::PrincipalType::Machine => Self::Machine,
        }
    }
}

fn is_glob_pattern(model: &str) -> bool {
    model.chars().any(|ch| matches!(ch, '*' | '?' | '[' | ']'))
}

fn invalid_allowed_models_glob(
    principal_id: &str,
    pattern: &str,
    source: globset::Error,
) -> ConfigError {
    ConfigError::InvalidPrincipalAllowedModelsGlob {
        principal_id: principal_id.to_owned(),
        pattern: pattern.to_owned(),
        message: source.to_string(),
    }
}
