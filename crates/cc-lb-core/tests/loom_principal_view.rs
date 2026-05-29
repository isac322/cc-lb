#[cfg(loom)]
mod principal_view_swap {
    use std::collections::HashMap;
    use std::sync::Arc as StdArc;

    use arc_swap::ArcSwap;
    use bytes::Bytes;
    use cc_lb_storage_api::{PrincipalKind as DbPrincipalKind, PrincipalRecord};
    use cc_lb_core::api_keys::principal_view::{
        ObservabilityHooksCache, PrincipalView, RouterPluginCache,
    };
    use cc_lb_plugin_api::{
        DialectError, ObservabilityError, ObservabilityHook, ObserveEvent, Principal,
        PrincipalKind, RequestContext, RouteDecision, RouteError, RouterPlugin, ShapedRequest,
        ShapedRequestBuilder, Upstream, UpstreamDialect,
    };
    use http::{HeaderMap, Method, StatusCode};
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

                    let global_router: StdArc<dyn RouterPlugin> = StdArc::new(StubRouter::new(0));
                    let global_hooks: Vec<StdArc<dyn ObservabilityHook>> =
                        vec![StdArc::new(StubHook::new(0))];
                    let principal = principal();
                    let ctx = request_context();

                    let cached = view
                        .get(PRINCIPAL_ID)
                        .expect("principal exists in loaded view snapshot");
                    let router = cached.resolved_router(&global_router);
                    let hooks = cached.resolved_hooks(&global_hooks);

                    assert!(StdArc::strong_count(router) > 0);
                    assert_eq!(hooks.len(), 1);
                    assert!(StdArc::strong_count(&hooks[0]) > 0);

                    let route = router
                        .route(&ctx, &principal)
                        .expect("stub router always returns a route");
                    loom::thread::yield_now();

                    for hook in hooks {
                        hook.observe(ObserveEvent::UpstreamChosen {
                            upstream: route.upstream.clone(),
                        })
                        .expect("stub hook accepts matching route generation");
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

    // TODO(Task-35-followup): replace TOML config consumption with DB store read
    fn view_for_generation(generation: u8) -> StdArc<PrincipalView> {
        let mut config = Config::default();
        config.principals.insert(
            PRINCIPAL_ID.to_owned(),
            PrincipalSpec {
                allowed_models: vec!["claude-*".to_owned()],
                ..PrincipalSpec::default()
            },
        );

        let mut principal_chains = HashMap::new();
        principal_chains.insert(
            PRINCIPAL_ID.to_owned(),
            (
                RouterPluginCache::Explicit(StdArc::new(StubRouter::new(generation))),
                ObservabilityHooksCache::Explicit(vec![StdArc::new(StubHook::new(generation))]),
            ),
        );

        PrincipalView::from_config(&config, principal_chains)
            .expect("test config creates a principal view")
    }

    fn principal() -> Principal {
        Principal {
            id: PRINCIPAL_ID.to_owned(),
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        }
    }

    fn request_context() -> RequestContext {
        RequestContext {
            request_id: "req-loom".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(b"{}"),
        }
    }

    struct StubRouter {
        generation: u8,
    }

    impl StubRouter {
        fn new(generation: u8) -> Self {
            Self { generation }
        }
    }

    impl RouterPlugin for StubRouter {
        fn route(
            &self,
            _ctx: &RequestContext,
            principal: &Principal,
        ) -> Result<RouteDecision, RouteError> {
            assert_eq!(principal.id, PRINCIPAL_ID);
            assert!(matches!(self.generation, 1 | 2));

            Ok(RouteDecision {
                upstream: Upstream::CustomAnthropicSpec {
                    base_url: generation_url(self.generation),
                },
                dialect: StdArc::new(StubDialect),
            })
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
            let ObserveEvent::UpstreamChosen {
                upstream: Upstream::CustomAnthropicSpec { base_url },
            } = event
            else {
                panic!("stub hook only expects UpstreamChosen events");
            };

            assert_eq!(base_url, generation_url(self.generation));
            Ok(())
        }
    }

    struct StubDialect;

    impl UpstreamDialect for StubDialect {
        fn shape(
            &self,
            ctx: &RequestContext,
            upstream: &Upstream,
            _principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            let Upstream::CustomAnthropicSpec { base_url } = upstream else {
                unreachable!("stub router only returns custom upstreams");
            };

            Ok(builder.shaped_request(
                base_url.clone(),
                ctx.method.clone(),
                HeaderMap::new(),
                Bytes::new(),
            ))
        }

        fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
            None
        }
    }

    fn generation_url(generation: u8) -> url::Url {
        format!("https://generation-{generation}.example.test")
            .parse()
            .expect("generation URL is valid")
    }
}
