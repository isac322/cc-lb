use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView,
    RouterPipelineCache,
};
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, Principal, RequestContext, TerminalStrategy,
    UpstreamCandidate,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use uuid::Uuid;

const PRINCIPAL_ID: &str = "principal-a";

#[test]
fn explicit_pipeline_resolves_to_cached_reference() {
    let explicit = pipeline(
        vec![filter("first"), filter("second")],
        TerminalStrategy::Random,
        None,
    );
    let global = pipeline(vec![filter("global")], TerminalStrategy::FirstPick, None);
    let view = view_with_pipeline(Some(Arc::clone(&explicit)), TerminalStrategy::FirstPick);

    let resolved = view
        .get(PRINCIPAL_ID)
        .expect("principal exists")
        .resolved_pipeline(Some(&global));

    assert!(Arc::ptr_eq(&resolved, &explicit));
    assert_eq!(resolved.user_filters.len(), 2);
    assert_eq!(resolved.terminal, TerminalStrategy::Random);
    assert!(resolved.instantiation_error.is_none());
}

#[test]
fn inherited_pipeline_resolves_to_global_reference() {
    let global = pipeline(vec![filter("global")], TerminalStrategy::FirstPick, None);
    let view = view_with_pipeline(None, TerminalStrategy::Random);

    let resolved = view
        .get(PRINCIPAL_ID)
        .expect("principal exists")
        .resolved_pipeline(Some(&global));

    assert!(Arc::ptr_eq(&resolved, &global));
    assert_eq!(resolved.user_filters.len(), 1);
    assert_eq!(resolved.terminal, TerminalStrategy::FirstPick);
}

#[test]
fn inherited_pipeline_without_global_uses_default_terminal() {
    let view = view_with_pipeline(None, TerminalStrategy::FirstPick);

    let resolved = view
        .get(PRINCIPAL_ID)
        .expect("principal exists")
        .resolved_pipeline(None);

    assert!(resolved.user_filters.is_empty());
    assert_eq!(resolved.terminal, TerminalStrategy::FirstPick);
    assert!(resolved.instantiation_error.is_none());
}

#[test]
fn instantiation_error_is_exposed_by_resolved_pipeline() {
    let explicit = pipeline(
        vec![filter("first")],
        TerminalStrategy::Random,
        Some(Arc::<str>::from("filter plugin failed to instantiate")),
    );
    let global = pipeline(Vec::new(), TerminalStrategy::FirstPick, None);
    let view = view_with_pipeline(Some(Arc::clone(&explicit)), TerminalStrategy::FirstPick);

    let resolved = view
        .get(PRINCIPAL_ID)
        .expect("principal exists")
        .resolved_pipeline(Some(&global));

    assert!(Arc::ptr_eq(&resolved, &explicit));
    assert_eq!(
        resolved.instantiation_error.as_deref(),
        Some("filter plugin failed to instantiate")
    );
}

#[test]
fn arc_swap_returns_new_pipeline_reference_after_store() {
    let first = pipeline(vec![filter("first")], TerminalStrategy::FirstPick, None);
    let second = pipeline(vec![filter("second")], TerminalStrategy::Random, None);
    let holder = ArcSwap::from_pointee(view_with_pipeline(
        Some(Arc::clone(&first)),
        TerminalStrategy::FirstPick,
    ));

    let before = holder
        .load_full()
        .get(PRINCIPAL_ID)
        .expect("principal exists")
        .resolved_pipeline(None);
    assert!(Arc::ptr_eq(&before, &first));

    holder.store(Arc::new(view_with_pipeline(
        Some(Arc::clone(&second)),
        TerminalStrategy::FirstPick,
    )));

    let after = holder
        .load_full()
        .get(PRINCIPAL_ID)
        .expect("principal exists")
        .resolved_pipeline(None);
    assert!(Arc::ptr_eq(&after, &second));
    assert!(!Arc::ptr_eq(&after, &before));
}

fn view_with_pipeline(
    router_pipeline: Option<Arc<RouterPipelineCache>>,
    terminal: TerminalStrategy,
) -> PrincipalView {
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    if router_pipeline.is_some() {
        chains.insert(
            PRINCIPAL_ID.to_owned(),
            (
                router_pipeline,
                ObservabilityHooksCache::Inherit,
                DialectCache::Inherit,
            ),
        );
    }
    PrincipalView::from_db(&[principal(terminal)], chains)
}

fn principal(terminal: TerminalStrategy) -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: PRINCIPAL_ID.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: terminal,
    }
}

fn pipeline(
    user_filters: Vec<Arc<dyn FilterPlugin>>,
    terminal: TerminalStrategy,
    instantiation_error: Option<Arc<str>>,
) -> Arc<RouterPipelineCache> {
    Arc::new(RouterPipelineCache {
        user_filters,
        terminal,
        instantiation_error,
    })
}

fn filter(name: &'static str) -> Arc<dyn FilterPlugin> {
    Arc::new(StubFilter { name })
}

struct StubFilter {
    name: &'static str,
}

impl FilterPlugin for StubFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        unimplemented!("StubFilter({}) is for cache identity tests only", self.name)
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}
