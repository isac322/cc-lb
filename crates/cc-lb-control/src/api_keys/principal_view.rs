use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_lb_domain::TerminalStrategy;
use cc_lb_observability::ObservabilityHook;
use cc_lb_routing::FilterPlugin;
use cc_lb_storage_api::principal::Limit as DbLimit;
use cc_lb_storage_api::{CacheKeepaliveConfig, PrincipalKind as DbPrincipalKind, PrincipalRecord};
use cc_lb_upstream::UpstreamDialect;
use globset::{Glob, GlobSet, GlobSetBuilder};
use uuid::Uuid;

use crate::api_keys::types::{Limit, PrincipalType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalStatus {
    Active,
    Disabled,
    Missing,
}

#[derive(Clone)]
pub struct RouterPipelineCache {
    pub user_filters: Vec<Arc<dyn FilterPlugin>>,
    pub terminal: TerminalStrategy,
    pub instantiation_error: Option<Arc<str>>,
}

impl RouterPipelineCache {
    pub fn empty(terminal: TerminalStrategy) -> Self {
        Self {
            user_filters: Vec::new(),
            terminal,
            instantiation_error: None,
        }
    }
}

#[derive(Clone)]
pub enum ObservabilityHooksCache {
    Inherit,
    Explicit(Vec<Arc<dyn ObservabilityHook>>),
}

#[derive(Clone)]
pub enum DialectCache {
    Inherit,
    Explicit(ShapePluginCache),
}

#[derive(Clone)]
pub struct ShapePluginCache {
    pub dialect: Arc<dyn UpstreamDialect>,
}

pub type PrincipalRoutingArtifacts = (
    Option<Arc<RouterPipelineCache>>,
    ObservabilityHooksCache,
    DialectCache,
);

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
    allowed_upstreams: Vec<Uuid>,
    default_limits: Vec<Limit>,
    enabled: bool,
    router_pipeline: Option<Arc<RouterPipelineCache>>,
    default_router_pipeline: Arc<RouterPipelineCache>,
    observability_hooks: ObservabilityHooksCache,
    dialect: DialectCache,
    cache_keepalive: Option<Arc<CacheKeepaliveConfig>>,
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
            router_terminal_strategy: Default::default(),
            cache_keepalive: None,
        };
        principal_chains.entry(principal_id.to_owned()).or_insert((
            None,
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ));
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
                let (router_pipeline, observability_hooks, dialect) =
                    principal_chains.remove(&principal_id).unwrap_or((
                        None,
                        ObservabilityHooksCache::Inherit,
                        DialectCache::Inherit,
                    ));
                let default_router_pipeline = Arc::new(RouterPipelineCache::empty(
                    principal.router_terminal_strategy.clone(),
                ));

                let cache_keepalive = principal
                    .cache_keepalive
                    .clone()
                    .filter(|cfg| cfg.enabled)
                    .map(Arc::new);
                let cached = PrincipalSpecCached {
                    id: principal_id.clone(),
                    principal_type: principal.kind.into(),
                    allowed_models,
                    allowed_models_exact: exact,
                    allowed_upstreams: principal.allowed_upstreams.clone(),
                    default_limits: principal.default_limits.clone(),
                    enabled: principal.enabled,
                    router_pipeline,
                    default_router_pipeline,
                    observability_hooks,
                    dialect,
                    cache_keepalive,
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

    pub fn has_any_active_principal(&self) -> bool {
        self.specs.values().any(|spec| spec.enabled)
    }

    pub fn default_limits(&self, principal_id: &str) -> &[Limit] {
        self.get(principal_id)
            .map(|spec| spec.default_limits.as_slice())
            .unwrap_or(&[])
    }

    pub fn allowed_upstreams(&self, principal_id: &str) -> Option<&[Uuid]> {
        self.get(principal_id)
            .map(PrincipalSpecCached::allowed_upstreams)
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

    pub fn allowed_upstreams(&self) -> &[Uuid] {
        &self.allowed_upstreams
    }

    pub fn resolved_pipeline(
        &self,
        global: Option<&Arc<RouterPipelineCache>>,
    ) -> Arc<RouterPipelineCache> {
        self.router_pipeline
            .clone()
            .or_else(|| global.cloned())
            .unwrap_or_else(|| Arc::clone(&self.default_router_pipeline))
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

    pub fn resolved_dialect<'a>(
        &'a self,
        fallback: &'a Arc<dyn UpstreamDialect>,
    ) -> &'a Arc<dyn UpstreamDialect> {
        match &self.dialect {
            DialectCache::Inherit => fallback,
            DialectCache::Explicit(cache) => &cache.dialect,
        }
    }

    pub fn cache_keepalive(&self) -> Option<&Arc<CacheKeepaliveConfig>> {
        self.cache_keepalive.as_ref()
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

fn is_glob_pattern(model: &str) -> bool {
    model.chars().any(|ch| matches!(ch, '*' | '?' | '[' | ']'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_observability::{ObservabilityError, ObserveEvent};
    use cc_lb_plugin_api::{Principal, UpstreamCandidate};
    use cc_lb_routing::{FilterError, FilterOutput, RoutingContext};

    struct StubFilter(&'static str);
    impl FilterPlugin for StubFilter {
        fn filter(
            &self,
            _: &RoutingContext,
            _: &Principal,
            _: &[UpstreamCandidate],
        ) -> Result<FilterOutput, FilterError> {
            unimplemented!("StubFilter({}) is for identity comparison only", self.0)
        }

        fn plugin_id(&self) -> Uuid {
            Uuid::nil()
        }

        fn plugin_name(&self) -> &str {
            self.0
        }
    }

    struct StubHook(&'static str);
    impl ObservabilityHook for StubHook {
        fn observe(&self, _: ObserveEvent) -> Result<(), ObservabilityError> {
            unimplemented!("StubHook({}) is for identity comparison only", self.0)
        }
    }

    fn cached(
        pipeline: Option<Arc<RouterPipelineCache>>,
        hooks: ObservabilityHooksCache,
    ) -> PrincipalSpecCached {
        PrincipalSpecCached {
            id: "test".to_owned(),
            principal_type: PrincipalType::Machine,
            allowed_models: GlobSetBuilder::new().build().unwrap(),
            allowed_models_exact: HashSet::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            enabled: true,
            router_pipeline: pipeline,
            default_router_pipeline: Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            )),
            observability_hooks: hooks,
            dialect: DialectCache::Inherit,
            cache_keepalive: None,
        }
    }

    #[test]
    fn principal_spec_cached_inherit_resolves_to_global_pipeline() {
        let global = Arc::new(RouterPipelineCache {
            user_filters: vec![Arc::new(StubFilter("global"))],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
        });
        let spec = cached(None, ObservabilityHooksCache::Inherit);

        let resolved = spec.resolved_pipeline(Some(&global));

        assert!(
            Arc::ptr_eq(&resolved, &global),
            "inherit must yield the global pipeline handle by identity"
        );
    }

    #[test]
    fn principal_spec_cached_explicit_pipeline_overrides_global() {
        let global = Arc::new(RouterPipelineCache::empty(TerminalStrategy::FirstPick));
        let explicit = Arc::new(RouterPipelineCache {
            user_filters: vec![Arc::new(StubFilter("explicit"))],
            terminal: TerminalStrategy::Random,
            instantiation_error: None,
        });
        let spec = cached(Some(explicit.clone()), ObservabilityHooksCache::Inherit);

        let resolved = spec.resolved_pipeline(Some(&global));

        assert!(Arc::ptr_eq(&resolved, &explicit));
        assert!(!Arc::ptr_eq(&resolved, &global));
    }

    #[test]
    fn principal_spec_cached_inherit_hooks_returns_global_slice() {
        let global: Vec<Arc<dyn ObservabilityHook>> =
            vec![Arc::new(StubHook("g1")), Arc::new(StubHook("g2"))];
        let spec = cached(None, ObservabilityHooksCache::Inherit);

        let resolved = spec.resolved_hooks(&global);

        assert_eq!(resolved.len(), 2);
        assert!(Arc::ptr_eq(&resolved[0], &global[0]));
        assert!(Arc::ptr_eq(&resolved[1], &global[1]));
    }

    #[test]
    fn principal_spec_cached_explicit_empty_hooks_returns_empty() {
        let global: Vec<Arc<dyn ObservabilityHook>> =
            vec![Arc::new(StubHook("g1")), Arc::new(StubHook("g2"))];
        let spec = cached(None, ObservabilityHooksCache::Explicit(Vec::new()));

        let resolved = spec.resolved_hooks(&global);

        assert!(
            resolved.is_empty(),
            "Explicit(vec![]) must return an empty slice, NOT the global chain"
        );
    }
}
