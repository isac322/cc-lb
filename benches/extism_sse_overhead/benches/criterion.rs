use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use cc_lb_plugin_api::{ObservabilityHook, ObserveEvent, PluginManifest, PluginRuntime};
use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::{json, Value};
use tempfile::TempDir;

fn bench_extism_observe(c: &mut Criterion) {
    let per_event = ObserveFixture::new("criterion-per-event", 1);
    let per_event_hook = per_event.hook();
    c.bench_function("extism_observe_per_event", |b| {
        let mut batch_index = 0_u64;
        b.iter(|| {
            per_event_hook
                .observe(chunk(batch_index, 1, 256))
                .expect("per-event observe succeeds");
            batch_index = batch_index.saturating_add(1);
        });
    });

    let batched = ObserveFixture::new("criterion-batched-32", 32);
    let batched_hook = batched.hook();
    c.bench_function("extism_observe_batched_32_cumulative", |b| {
        let mut batch_index = 0_u64;
        b.iter(|| {
            for _ in 0..32 {
                batched_hook
                    .observe(chunk(batch_index, 1, 256))
                    .expect("batched observe succeeds");
                batch_index = batch_index.saturating_add(1);
            }
        });
    });
}

fn chunk(batch_index: u64, event_count: usize, total_bytes: usize) -> ObserveEvent {
    ObserveEvent::Chunk {
        batch_index,
        event_count,
        total_bytes,
    }
}

struct ObserveFixture {
    _dir: TempDir,
    runtime: cc_lb_runtime_extism::ExtismRuntime,
    manifest: PluginManifest,
}

impl ObserveFixture {
    fn new(name: &str, observe_batch: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir is created");
        let wasm = wat::parse_str(observe_module()).expect("observe module parses");
        let artifact = dir.path().join(format!("{name}.wasm"));
        fs::write(&artifact, wasm).expect("observe wasm is written");
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "observe_batch_count".to_owned(),
            Value::from(observe_batch as u64),
        );
        metadata.insert("observe_flush_ms".to_owned(), Value::from(60_000_u64));
        Self {
            _dir: dir,
            runtime: cc_lb_runtime_extism::ExtismRuntime::new(),
            manifest: PluginManifest {
                name: format!("observe-{name}"),
                artifact: artifact.display().to_string(),
                config: json!({}),
                metadata,
            },
        }
    }

    fn hook(&self) -> Arc<dyn ObservabilityHook> {
        self.runtime
            .instantiate_observability(&self.manifest)
            .expect("observability hook instantiates")
    }
}

fn observe_module() -> &'static str {
    r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (func $out (result i64)
    (local $ptr i64)
    (local.set $ptr (call $alloc (i64.const 14)))
    (call $store_u8 (local.get $ptr) (i32.const 123))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 1)) (i32.const 34))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 2)) (i32.const 95))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 3)) (i32.const 118))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 4)) (i32.const 101))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 5)) (i32.const 114))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 6)) (i32.const 115))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 7)) (i32.const 105))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 8)) (i32.const 111))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 9)) (i32.const 110))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 10)) (i32.const 34))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 11)) (i32.const 58))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 12)) (i32.const 49))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 13)) (i32.const 125))
    (local.get $ptr))
  (func (export "observe") (result i32)
    (call $output_set (call $out) (i64.const 14))
    (i32.const 0))
)
"#
}

criterion_group!(benches, bench_extism_observe);
criterion_main!(benches);
