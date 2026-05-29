use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_lb_plugin_api::{ObservabilityHook, RouterPlugin};
use cc_lb_storage_api::principal::{Limit as DbLimit, LimitKind as DbLimitKind};
use cc_lb_storage_api::{PrincipalKind as DbPrincipalKind, PrincipalRecord};
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

pub type PrincipalRoutingArtifacts = (RouterPluginCache, ObservabilityHooksCache);

#[derive(Debug)]
pub struct PrincipalView {
    specs: HashMap<String, PrincipalSpecCached>,
    name_aliases: HashMap<String, String>,
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
    pub fn for_tests(
        principal_id: &str,
        enabled: bool,
        allowed_models: Vec<String>,
        default_limits: Vec<DbLimit>,
        mut principal_chains: HashMap<String, PrincipalRoutingArtifacts>,
    ) -> Self {
        let principal = PrincipalRecord {
            id: uuid::Uuid::new_v4(),
            name: principal_id.to_owned(),
            kind: DbPrincipalKind::Machine,
            allowed_models,
            allowed_upstreams: vec![],
            default_limits,
            enabled,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
        };
        principal_chains
            .entry(principal_id.to_owned())
            .or_insert((RouterPluginCache::Inherit, ObservabilityHooksCache::Inherit));
        Self::from_db(&[principal], principal_chains)
    }

    pub fn from_db(
        principals: &[PrincipalRecord],
        mut principal_chains: HashMap<String, PrincipalRoutingArtifacts>,
    ) -> Self {
        let mut name_aliases = HashMap::new();
        let specs = principals
            .iter()
            .filter(|principal| principal.deleted_at_unix_secs.is_none())
            .map(|principal| {
                let mut exact = HashSet::new();
                let mut builder = GlobSetBuilder::new();

                for model in &principal.allowed_models {
                    if is_glob_pattern(model) {
                        match Glob::new(model) {
                            Ok(glob) => {
                                builder.add(glob);
                            }
                            Err(_) => {
                                exact.insert(model.clone());
                            }
                        }
                    } else {
                        exact.insert(model.clone());
                    }
                }

                let allowed_models = builder.build().unwrap_or_else(|_| {
                    GlobSetBuilder::new()
                        .build()
                        .unwrap_or_else(|_| unreachable!("empty glob set builds"))
                });
                let principal_id = principal.name.clone();
                name_aliases.insert(principal.id.to_string(), principal_id.clone());
                let (router_plugin, observability_hooks) = principal_chains
                    .remove(&principal_id)
                    .unwrap_or((RouterPluginCache::Inherit, ObservabilityHooksCache::Inherit));

                let cached = PrincipalSpecCached {
                    id: principal_id.clone(),
                    principal_type: principal.kind.into(),
                    allowed_models,
                    allowed_models_exact: exact,
                    default_limits: principal
                        .default_limits
                        .iter()
                        .cloned()
                        .map(Into::into)
                        .collect(),
                    enabled: principal.enabled,
                    router_plugin,
                    observability_hooks,
                };

                (principal_id, cached)
            })
            .collect();

        Self {
            specs,
            name_aliases,
        }
    }

    pub fn get(&self, principal_id: &str) -> Option<&PrincipalSpecCached> {
        self.specs.get(principal_id).or_else(|| {
            self.name_aliases
                .get(principal_id)
                .and_then(|canonical_id| self.specs.get(canonical_id))
        })
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

impl From<DbLimit> for Limit {
    fn from(value: DbLimit) -> Self {
        Self {
            kind: value.kind.into(),
            window: std::time::Duration::from_secs(value.window_secs),
            cap_micros: value.cap_micros,
        }
    }
}

impl From<DbPrincipalKind> for PrincipalType {
    fn from(value: DbPrincipalKind) -> Self {
        match value {
            DbPrincipalKind::Human => Self::Human,
            DbPrincipalKind::Machine | DbPrincipalKind::Admin => Self::Machine,
        }
    }
}

impl From<DbLimitKind> for LimitKind {
    fn from(value: DbLimitKind) -> Self {
        match value {
            DbLimitKind::Requests => Self::Requests,
            DbLimitKind::InputTokens => Self::InputTokens,
            DbLimitKind::OutputTokens => Self::OutputTokens,
            DbLimitKind::TotalTokens => Self::TotalTokens,
            DbLimitKind::CostUsd => Self::CostUsd,
            DbLimitKind::Concurrent => Self::Concurrent,
        }
    }
}

fn is_glob_pattern(model: &str) -> bool {
    model.chars().any(|ch| matches!(ch, '*' | '?' | '[' | ']'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_api::{
        ObservabilityError, ObserveEvent, Principal, RequestContext, RouteDecision, RouteError,
        UpstreamCandidate,
    };

    struct StubRouter(&'static str);
    impl RouterPlugin for StubRouter {
        fn route(
            &self,
            _: &RequestContext,
            _: &Principal,
            _: &[UpstreamCandidate],
        ) -> Result<RouteDecision, RouteError> {
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
