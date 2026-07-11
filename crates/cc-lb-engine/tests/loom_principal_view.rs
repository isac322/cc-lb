#[cfg(loom)]
mod principal_view_swap {
    use std::collections::HashMap;
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    use arc_swap::ArcSwap;
    use cc_lb_engine::api_keys::principal_view::{
        DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
    };
    use cc_lb_plugin_api::{
        FilterError, FilterOutput, FilterPlugin, ObservabilityError, ObservabilityHook,
        ObserveEvent, Principal, TerminalStrategy, UpstreamCandidate,
    };
    use cc_lb_routing::RoutingContext;
    use cc_lb_storage_api::{PrincipalKind as DbPrincipalKind, PrincipalRecord};
    use loom::sync::Arc;

    const PRINCIPAL_ID: &str = "principal-a";

    #[test]
    fn principal_view_load_full_dispatch_survives_concurrent_store() {
        let initial_view = view_for_generation(1);
        let replacement_view = view_for_generation(2);

        loom::model(move || {
            let principal_view = Arc::new(ArcSwap::from(StdArc::clone(&initial_view)));
            let replacement_view = StdArc::clone(&replacement_view);

            let reader_view = Arc::clone(&principal_view);
            let reader = loom::thread::Builder::new()
                .stack_size(4 * 1024 * 1024)
                .spawn(move || {
                    let view = reader_view.load_full();
                    loom::thread::yield_now();

                    let global_pipeline =
                        StdArc::new(RouterPipelineCache::empty(TerminalStrategy::FirstPick));
                    let global_hooks: Vec<StdArc<dyn ObservabilityHook>> =
                        vec![StdArc::new(StubHook::new(0))];

                    let cached = view
                        .get(PRINCIPAL_ID)
                        .expect("principal exists in loaded view snapshot");
                    let pipeline = cached.resolved_pipeline(Some(&global_pipeline));
                    let hooks = cached.resolved_hooks(&global_hooks);

                    assert!(StdArc::strong_count(&pipeline) > 0);
                    assert_eq!(pipeline.user_filters.len(), 1);
                    assert_eq!(hooks.len(), 1);
                    assert!(StdArc::strong_count(&hooks[0]) > 0);

                    for hook in hooks {
                        hook.observe(ObserveEvent::AuthnComplete {
                            principal_id: PRINCIPAL_ID.to_owned(),
                            kind: cc_lb_domain::PrincipalKind::ApiKey,
                        })
                        .expect("stub hook accepts authn event");
                    }
                })
                .expect("reader thread spawns");

            let writer_view = Arc::clone(&principal_view);
            let writer = loom::thread::spawn(move || {
                loom::thread::yield_now();
                writer_view.store(replacement_view);
            });

            reader.join().unwrap();
            writer.join().unwrap();

            assert!(principal_view.load_full().get(PRINCIPAL_ID).is_some());
        });
    }

    fn view_for_generation(generation: u8) -> StdArc<PrincipalView> {
        let principals = vec![PrincipalRecord {
            id: uuid::Uuid::new_v4(),
            name: PRINCIPAL_ID.to_owned(),
            kind: DbPrincipalKind::Machine,
            allowed_models: vec!["claude-*".to_owned()],
            allowed_upstreams: vec![],
            default_limits: Vec::new(),
            enabled: true,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: Duration::ZERO.as_secs(),
            updated_at_unix_secs: Duration::ZERO.as_secs(),
            router_terminal_strategy: Default::default(),
            cache_keepalive: None,
        }];

        let mut principal_chains = HashMap::new();
        principal_chains.insert(
            PRINCIPAL_ID.to_owned(),
            (
                Some(StdArc::new(RouterPipelineCache {
                    user_filters: vec![StdArc::new(StubFilter::new(generation))],
                    terminal: TerminalStrategy::FirstPick,
                    instantiation_error: None,
                })),
                ObservabilityHooksCache::Explicit(vec![StdArc::new(StubHook::new(generation))]),
                DialectCache::Inherit,
            ),
        );

        StdArc::new(PrincipalView::from_db(&principals, principal_chains))
    }

    struct StubFilter {
        generation: u8,
    }

    impl StubFilter {
        fn new(generation: u8) -> Self {
            Self { generation }
        }
    }

    impl FilterPlugin for StubFilter {
        fn filter(
            &self,
            _ctx: &RoutingContext,
            _principal: &Principal,
            _candidates: &[UpstreamCandidate],
        ) -> Result<FilterOutput, FilterError> {
            unimplemented!(
                "StubFilter({}) is for identity comparison only",
                self.generation
            )
        }

        fn plugin_id(&self) -> uuid::Uuid {
            uuid::Uuid::nil()
        }

        fn plugin_name(&self) -> &str {
            "stub-filter"
        }
    }

    struct StubHook {
        generation: u8,
    }

    impl StubHook {
        fn new(generation: u8) -> Self {
            Self { generation }
        }
    }

    impl ObservabilityHook for StubHook {
        fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
            assert!(matches!(event, ObserveEvent::AuthnComplete { .. }));
            let _ = self.generation;
            Ok(())
        }
    }
}
