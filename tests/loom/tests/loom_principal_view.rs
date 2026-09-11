#![cfg(loom)]

use std::collections::HashMap;
use std::sync::Arc as StdArc;

use arc_swap::ArcSwap;
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

                let cached = view
                    .get(PRINCIPAL_ID)
                    .expect("principal exists in loaded view snapshot");
                let pipeline = StdArc::clone(&cached.pipeline);
                let hooks = cached.hooks.clone();

                assert!(StdArc::strong_count(&pipeline) > 0);
                assert_eq!(pipeline.user_filters.len(), 1);
                assert_eq!(hooks.len(), 1);
                assert!(StdArc::strong_count(&hooks[0]) > 0);

                let generation = pipeline.user_filters[0].generation;
                for hook in hooks {
                    hook.observe(PRINCIPAL_ID);
                    assert_eq!(hook.generation, generation);
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

        let final_view = principal_view.load_full();
        let cached = final_view
            .get(PRINCIPAL_ID)
            .expect("principal exists after replacement");
        assert_eq!(cached.pipeline.user_filters[0].generation, 2);
        assert_eq!(cached.hooks[0].generation, 2);
    });
}

fn view_for_generation(generation: u8) -> StdArc<PrincipalView> {
    let mut entries = HashMap::new();
    entries.insert(
        PRINCIPAL_ID,
        CachedPrincipal {
            pipeline: StdArc::new(RouterPipelineCache {
                user_filters: vec![StdArc::new(StubFilter { generation })],
            }),
            hooks: vec![StdArc::new(StubHook { generation })],
        },
    );
    StdArc::new(PrincipalView { entries })
}

struct PrincipalView {
    entries: HashMap<&'static str, CachedPrincipal>,
}

impl PrincipalView {
    fn get(&self, principal_id: &str) -> Option<&CachedPrincipal> {
        self.entries.get(principal_id)
    }
}

struct CachedPrincipal {
    pipeline: StdArc<RouterPipelineCache>,
    hooks: Vec<StdArc<StubHook>>,
}

struct RouterPipelineCache {
    user_filters: Vec<StdArc<StubFilter>>,
}

struct StubFilter {
    generation: u8,
}

struct StubHook {
    generation: u8,
}

impl StubHook {
    fn observe(&self, principal_id: &str) {
        assert_eq!(principal_id, PRINCIPAL_ID);
    }
}
