#[cfg(loom)]
mod principal_view_swap {
    use std::collections::HashMap;
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    use arc_swap::ArcSwap;
    use cc_lb_core::api_keys::principal_view::{
        DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
    };
    use cc_lb_plugin_api::{
        FilterError, FilterOutput, FilterPlugin, Principal, RequestContext, TerminalStrategy,
        UpstreamCandidate,
    };
    use cc_lb_storage_api::{PrincipalKind as DbPrincipalKind, PrincipalRecord};
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const PRINCIPAL_ID: &str = "principal-a";
    const STACK_SIZE: usize = 4 * 1024 * 1024;

    #[test]
    fn reader_holds_old_pipeline_ref_while_writer_swaps_view() {
        let (initial_view, initial_pipeline) = view_for_generation(1);
        let (replacement_view, replacement_pipeline) = view_for_generation(2);

        loom::model(move || {
            let principal_view = Arc::new(ArcSwap::from(StdArc::clone(&initial_view)));
            let replacement_view = StdArc::clone(&replacement_view);
            let initial_pipeline = StdArc::clone(&initial_pipeline);
            let replacement_pipeline = StdArc::clone(&replacement_pipeline);
            let reader_has_pipeline = Arc::new(AtomicBool::new(false));
            let writer_swapped = Arc::new(AtomicBool::new(false));

            let reader_view = Arc::clone(&principal_view);
            let reader_has_pipeline_signal = Arc::clone(&reader_has_pipeline);
            let writer_swapped_signal = Arc::clone(&writer_swapped);
            let reader = spawn_with_stack(move || {
                let view = reader_view.load_full();
                let pipeline = view
                    .get(PRINCIPAL_ID)
                    .expect("principal exists in loaded view snapshot")
                    .resolved_pipeline(None);
                assert!(StdArc::ptr_eq(&pipeline, &initial_pipeline));
                reader_has_pipeline_signal.store(true, Ordering::Release);

                while !writer_swapped_signal.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }

                assert_eq!(pipeline.user_filters.len(), 1);
                assert_eq!(filter_id(&pipeline, 0), filter_uuid(1, 0));
                assert!(StdArc::ptr_eq(&pipeline, &initial_pipeline));
            });

            let writer_view = Arc::clone(&principal_view);
            let writer_ready_signal = Arc::clone(&reader_has_pipeline);
            let writer_swapped_signal = Arc::clone(&writer_swapped);
            let writer = loom::thread::spawn(move || {
                while !writer_ready_signal.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }
                writer_view.store(replacement_view);
                writer_swapped_signal.store(true, Ordering::Release);
            });

            reader.join().unwrap();
            writer.join().unwrap();

            let current_pipeline = current_pipeline(&principal_view);
            assert!(StdArc::ptr_eq(&current_pipeline, &replacement_pipeline));
            assert!(!StdArc::ptr_eq(&current_pipeline, &initial_pipeline));
            assert_eq!(filter_id(&current_pipeline, 0), filter_uuid(2, 0));
        });
    }

    #[test]
    fn concurrent_readers_share_old_pipeline_and_new_loads_see_replacement() {
        let (initial_view, initial_pipeline) = view_for_generation(1);
        let (replacement_view, replacement_pipeline) = view_for_generation(2);

        loom::model(move || {
            let principal_view = Arc::new(ArcSwap::from(StdArc::clone(&initial_view)));
            let replacement_view = StdArc::clone(&replacement_view);
            let initial_pipeline = StdArc::clone(&initial_pipeline);
            let replacement_pipeline = StdArc::clone(&replacement_pipeline);
            let readers_with_old_pipeline = Arc::new(AtomicUsize::new(0));
            let writer_swapped = Arc::new(AtomicBool::new(false));

            let first_reader = reader_that_holds_old_pipeline(
                Arc::clone(&principal_view),
                StdArc::clone(&initial_pipeline),
                Arc::clone(&readers_with_old_pipeline),
                Arc::clone(&writer_swapped),
            );
            let second_reader = reader_that_holds_old_pipeline(
                Arc::clone(&principal_view),
                StdArc::clone(&initial_pipeline),
                Arc::clone(&readers_with_old_pipeline),
                Arc::clone(&writer_swapped),
            );

            let writer_view = Arc::clone(&principal_view);
            let writer_ready_signal = Arc::clone(&readers_with_old_pipeline);
            let writer_swapped_signal = Arc::clone(&writer_swapped);
            let writer = loom::thread::spawn(move || {
                while writer_ready_signal.load(Ordering::Acquire) < 2 {
                    loom::thread::yield_now();
                }
                writer_view.store(replacement_view);
                writer_swapped_signal.store(true, Ordering::Release);
            });

            first_reader.join().unwrap();
            second_reader.join().unwrap();
            writer.join().unwrap();

            let current_pipeline = current_pipeline(&principal_view);
            assert!(StdArc::ptr_eq(&current_pipeline, &replacement_pipeline));
            assert!(!StdArc::ptr_eq(&current_pipeline, &initial_pipeline));
            assert_eq!(filter_id(&current_pipeline, 0), filter_uuid(2, 0));
        });
    }

    #[test]
    fn swap_during_user_filters_vec_capacity_change_keeps_refs_valid() {
        let (initial_view, initial_pipeline) = view_with_filters(1, 1, 1);

        loom::model(move || {
            let principal_view = Arc::new(ArcSwap::from(StdArc::clone(&initial_view)));
            let initial_pipeline = StdArc::clone(&initial_pipeline);
            let reader_has_pipeline = Arc::new(AtomicBool::new(false));
            let writer_swapped = Arc::new(AtomicBool::new(false));

            let reader_view = Arc::clone(&principal_view);
            let reader_has_pipeline_signal = Arc::clone(&reader_has_pipeline);
            let writer_swapped_signal = Arc::clone(&writer_swapped);
            let reader = spawn_with_stack(move || {
                let view = reader_view.load_full();
                let pipeline = view
                    .get(PRINCIPAL_ID)
                    .expect("principal exists in loaded view snapshot")
                    .resolved_pipeline(None);
                assert!(StdArc::ptr_eq(&pipeline, &initial_pipeline));
                assert_eq!(pipeline.user_filters.len(), 1);
                assert_eq!(pipeline.user_filters.capacity(), 1);
                reader_has_pipeline_signal.store(true, Ordering::Release);

                while !writer_swapped_signal.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }

                assert_eq!(pipeline.user_filters.len(), 1);
                assert_eq!(pipeline.user_filters.capacity(), 1);
                assert_eq!(filter_id(&pipeline, 0), filter_uuid(1, 0));
            });

            let writer_view = Arc::clone(&principal_view);
            let writer_ready_signal = Arc::clone(&reader_has_pipeline);
            let writer_swapped_signal = Arc::clone(&writer_swapped);
            let writer = loom::thread::spawn(move || {
                while !writer_ready_signal.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }
                let (replacement_view, _) = view_with_filters(2, 4, 8);
                writer_view.store(replacement_view);
                writer_swapped_signal.store(true, Ordering::Release);
            });

            reader.join().unwrap();
            writer.join().unwrap();

            let current_pipeline = current_pipeline(&principal_view);
            assert_eq!(current_pipeline.user_filters.len(), 4);
            assert!(current_pipeline.user_filters.capacity() >= 8);
            assert_eq!(filter_id(&current_pipeline, 0), filter_uuid(2, 0));
            assert_eq!(filter_id(&current_pipeline, 3), filter_uuid(2, 3));
        });
    }

    fn reader_that_holds_old_pipeline(
        principal_view: Arc<ArcSwap<PrincipalView>>,
        expected_pipeline: StdArc<RouterPipelineCache>,
        readers_with_old_pipeline: Arc<AtomicUsize>,
        writer_swapped: Arc<AtomicBool>,
    ) -> loom::thread::JoinHandle<()> {
        spawn_with_stack(move || {
            let view = principal_view.load_full();
            let pipeline = view
                .get(PRINCIPAL_ID)
                .expect("principal exists in loaded view snapshot")
                .resolved_pipeline(None);
            assert!(StdArc::ptr_eq(&pipeline, &expected_pipeline));
            readers_with_old_pipeline.fetch_add(1, Ordering::AcqRel);

            while !writer_swapped.load(Ordering::Acquire) {
                loom::thread::yield_now();
            }

            assert_eq!(pipeline.user_filters.len(), 1);
            assert_eq!(filter_id(&pipeline, 0), filter_uuid(1, 0));
            assert!(StdArc::ptr_eq(&pipeline, &expected_pipeline));
        })
    }

    fn current_pipeline(
        principal_view: &Arc<ArcSwap<PrincipalView>>,
    ) -> StdArc<RouterPipelineCache> {
        principal_view
            .load_full()
            .get(PRINCIPAL_ID)
            .expect("principal exists after swap")
            .resolved_pipeline(None)
    }

    fn view_for_generation(generation: u8) -> (StdArc<PrincipalView>, StdArc<RouterPipelineCache>) {
        view_with_filters(generation, 1, 1)
    }

    fn view_with_filters(
        generation: u8,
        filter_count: usize,
        filter_capacity: usize,
    ) -> (StdArc<PrincipalView>, StdArc<RouterPipelineCache>) {
        assert!(filter_capacity >= filter_count);
        let pipeline = StdArc::new(RouterPipelineCache {
            user_filters: filters(generation, filter_count, filter_capacity),
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
        });

        let mut principal_chains = HashMap::new();
        principal_chains.insert(
            PRINCIPAL_ID.to_owned(),
            (
                Some(StdArc::clone(&pipeline)),
                ObservabilityHooksCache::Inherit,
                DialectCache::Inherit,
            ),
        );

        (
            StdArc::new(PrincipalView::from_db(
                &[principal_record()],
                principal_chains,
            )),
            pipeline,
        )
    }

    fn filters(
        generation: u8,
        filter_count: usize,
        filter_capacity: usize,
    ) -> Vec<StdArc<dyn FilterPlugin>> {
        let mut filters: Vec<StdArc<dyn FilterPlugin>> = Vec::with_capacity(filter_capacity);
        for index in 0..filter_count {
            filters.push(StdArc::new(StubFilter::new(generation, index as u8)));
        }
        filters
    }

    fn principal_record() -> PrincipalRecord {
        PrincipalRecord {
            id: uuid::Uuid::from_u128(1),
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
        }
    }

    fn filter_id(pipeline: &RouterPipelineCache, index: usize) -> uuid::Uuid {
        pipeline.user_filters[index].plugin_id()
    }

    fn filter_uuid(generation: u8, index: u8) -> uuid::Uuid {
        uuid::Uuid::from_u128((u128::from(generation) << 64) | u128::from(index))
    }

    fn spawn_with_stack<F>(f: F) -> loom::thread::JoinHandle<()>
    where
        F: FnOnce() + Send + 'static,
    {
        loom::thread::Builder::new()
            .stack_size(STACK_SIZE)
            .spawn(f)
            .expect("loom thread spawns")
    }

    struct StubFilter {
        generation: u8,
        index: u8,
    }

    impl StubFilter {
        fn new(generation: u8, index: u8) -> Self {
            Self { generation, index }
        }
    }

    impl FilterPlugin for StubFilter {
        fn filter(
            &self,
            _ctx: &RequestContext,
            _principal: &Principal,
            _candidates: &[UpstreamCandidate],
        ) -> Result<FilterOutput, FilterError> {
            unimplemented!(
                "StubFilter({}:{}) is for identity comparison only",
                self.generation,
                self.index
            )
        }

        fn plugin_id(&self) -> uuid::Uuid {
            filter_uuid(self.generation, self.index)
        }

        fn plugin_name(&self) -> &str {
            "stub-filter"
        }
    }
}
