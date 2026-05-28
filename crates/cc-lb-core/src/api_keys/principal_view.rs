use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_lb_config::{Config, ConfigError, Limit as ConfigLimit, LimitKind as ConfigLimitKind};
use cc_lb_plugin_api::{ObservabilityHook, RouterPlugin};
use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::api_keys::types::{Limit, LimitKind, PrincipalType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalStatus {
    Active,
    Disabled,
    Missing,
}

#[derive(Clone)]
pub enum RouterPluginCache {
    Inherit,
    Explicit(Arc<dyn RouterPlugin>),
}

#[derive(Clone)]
pub enum ObservabilityHooksCache {
    Inherit,
    Explicit(Vec<Arc<dyn ObservabilityHook>>),
}

#[derive(Debug)]
pub struct PrincipalView {
    specs: HashMap<String, PrincipalSpecCached>,
}

pub struct PrincipalSpecCached {
    id: String,
    principal_type: PrincipalType,
    allowed_models: GlobSet,
    allowed_models_exact: HashSet<String>,
    default_limits: Vec<Limit>,
    enabled: bool,
    router_plugin: RouterPluginCache,
    observability_hooks: ObservabilityHooksCache,
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
                    router_plugin: RouterPluginCache::Inherit,
                    observability_hooks: ObservabilityHooksCache::Inherit,
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

impl std::fmt::Debug for PrincipalSpecCached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrincipalSpecCached")
            .field("id", &self.id)
            .field("principal_type", &self.principal_type)
            .field("allowed_models_exact", &self.allowed_models_exact)
            .field("default_limits", &self.default_limits)
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}

impl PrincipalSpecCached {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn principal_type(&self) -> PrincipalType {
        self.principal_type
    }

    pub fn resolved_router<'a>(
        &'a self,
        global: &'a Arc<dyn RouterPlugin>,
    ) -> &'a Arc<dyn RouterPlugin> {
        match &self.router_plugin {
            RouterPluginCache::Inherit => global,
            RouterPluginCache::Explicit(handle) => handle,
        }
    }

    pub fn resolved_hooks<'a>(
        &'a self,
        global: &'a [Arc<dyn ObservabilityHook>],
    ) -> &'a [Arc<dyn ObservabilityHook>] {
        match &self.observability_hooks {
            ObservabilityHooksCache::Inherit => global,
            ObservabilityHooksCache::Explicit(hooks) => hooks.as_slice(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_api::{
        ObservabilityError, ObserveEvent, Principal, RequestContext, RouteDecision, RouteError,
    };

    struct StubRouter(&'static str);
    impl RouterPlugin for StubRouter {
        fn route(&self, _: &RequestContext, _: &Principal) -> Result<RouteDecision, RouteError> {
            unimplemented!("StubRouter({}) is for identity comparison only", self.0)
        }
    }

    struct StubHook(&'static str);
    impl ObservabilityHook for StubHook {
        fn observe(&self, _: ObserveEvent) -> Result<(), ObservabilityError> {
            unimplemented!("StubHook({}) is for identity comparison only", self.0)
        }
    }

    fn cached(router: RouterPluginCache, hooks: ObservabilityHooksCache) -> PrincipalSpecCached {
        PrincipalSpecCached {
            id: "test".to_owned(),
            principal_type: PrincipalType::Machine,
            allowed_models: GlobSetBuilder::new().build().unwrap(),
            allowed_models_exact: HashSet::new(),
            default_limits: Vec::new(),
            enabled: true,
            router_plugin: router,
            observability_hooks: hooks,
        }
    }

    #[test]
    fn principal_spec_cached_inherit_resolves_to_global() {
        let global: Arc<dyn RouterPlugin> = Arc::new(StubRouter("global"));
        let spec = cached(RouterPluginCache::Inherit, ObservabilityHooksCache::Inherit);

        let resolved = spec.resolved_router(&global);

        assert!(
            Arc::ptr_eq(resolved, &global),
            "Inherit must yield the borrowed global handle by identity"
        );
    }

    #[test]
    fn principal_spec_cached_explicit_router_overrides_global() {
        let global: Arc<dyn RouterPlugin> = Arc::new(StubRouter("global"));
        let explicit: Arc<dyn RouterPlugin> = Arc::new(StubRouter("explicit"));
        let spec = cached(
            RouterPluginCache::Explicit(explicit.clone()),
            ObservabilityHooksCache::Inherit,
        );

        let resolved = spec.resolved_router(&global);

        assert!(Arc::ptr_eq(resolved, &explicit));
        assert!(!Arc::ptr_eq(resolved, &global));
    }

    #[test]
    fn principal_spec_cached_inherit_hooks_returns_global_slice() {
        let global: Vec<Arc<dyn ObservabilityHook>> =
            vec![Arc::new(StubHook("g1")), Arc::new(StubHook("g2"))];
        let spec = cached(RouterPluginCache::Inherit, ObservabilityHooksCache::Inherit);

        let resolved = spec.resolved_hooks(&global);

        assert_eq!(resolved.len(), 2);
        assert!(Arc::ptr_eq(&resolved[0], &global[0]));
        assert!(Arc::ptr_eq(&resolved[1], &global[1]));
    }

    #[test]
    fn principal_spec_cached_explicit_empty_hooks_returns_empty() {
        let global: Vec<Arc<dyn ObservabilityHook>> =
            vec![Arc::new(StubHook("g1")), Arc::new(StubHook("g2"))];
        let spec = cached(
            RouterPluginCache::Inherit,
            ObservabilityHooksCache::Explicit(Vec::new()),
        );

        let resolved = spec.resolved_hooks(&global);

        assert!(
            resolved.is_empty(),
            "Explicit(vec![]) must return an empty slice, NOT the global chain"
        );
    }
}
